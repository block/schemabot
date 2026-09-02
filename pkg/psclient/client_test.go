package psclient

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createDeployRequestServer serves the deploy-request create endpoint and
// captures the body SchemaBot actually sent.
func createDeployRequestServer(t *testing.T, captured *map[string]any) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/v1/organizations/block/databases/orders/deploy-requests", r.URL.Path)
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(body, captured))
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"dr1","number":132,"state":"open"}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// The cutover setting has to survive serialization: PlanetScale reads an absent
// auto_cutover as "use the database default", which on a database whose default
// is on hands the schema swap to the backend. A deploy request SchemaBot creates
// says false out loud.
func TestCreateDeployRequestSendsAutoCutoverFalse(t *testing.T) {
	var captured map[string]any
	srv := createDeployRequestServer(t, &captured)

	client, err := NewPSClientWithBaseURL("token-name", "token-value", srv.URL)
	require.NoError(t, err)

	dr, err := client.CreateDeployRequest(t.Context(), &ps.CreateDeployRequestRequest{
		Organization:     "block",
		Database:         "orders",
		Branch:           "schemabot-orders-02846775",
		IntoBranch:       "main",
		AutoCutover:      false,
		AutoDeleteBranch: false,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(132), dr.Number)

	require.Contains(t, captured, "auto_cutover", "the cutover setting must reach PlanetScale")
	assert.Equal(t, false, captured["auto_cutover"])
	require.Contains(t, captured, "auto_delete_branch", "branch teardown must not fall to the database default either")
	assert.Equal(t, false, captured["auto_delete_branch"])
	assert.Equal(t, "schemabot-orders-02846775", captured["branch"])
	assert.Equal(t, "main", captured["into_branch"])
}

// A true setting is transmitted the same way, so nothing about the explicit
// body depends on the value being the zero one.
func TestCreateDeployRequestSendsAutoDeleteBranchTrue(t *testing.T) {
	var captured map[string]any
	srv := createDeployRequestServer(t, &captured)

	client, err := NewPSClientWithBaseURL("token-name", "token-value", srv.URL)
	require.NoError(t, err)

	_, err = client.CreateDeployRequest(t.Context(), &ps.CreateDeployRequestRequest{
		Organization:     "block",
		Database:         "orders",
		Branch:           "schemabot-orders-02846775",
		AutoDeleteBranch: true,
	})
	require.NoError(t, err)

	assert.Equal(t, true, captured["auto_delete_branch"])
	assert.Equal(t, false, captured["auto_cutover"])
}

// Without a base URL the cutover setting cannot be expressed at all, and a
// deploy request created anyway would leave the backend free to cut over. The
// client refuses to create one rather than create a request it cannot govern.
func TestCreateDeployRequestRefusesWithoutBaseURL(t *testing.T) {
	client, err := NewPSClientWithBaseURL("token-name", "token-value", "")
	require.NoError(t, err)

	_, err = client.CreateDeployRequest(t.Context(), &ps.CreateDeployRequestRequest{
		Organization: "block",
		Database:     "orders",
		Branch:       "schemabot-orders-02846775",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "auto_cutover")
}

// A rejected create carries the API's own response text, so an operator reading
// the apply's failure does not have to reproduce the call to learn why.
func TestCreateDeployRequestSurfacesAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"message":"branch has no schema changes"}`))
	}))
	t.Cleanup(srv.Close)

	client, err := NewPSClientWithBaseURL("token-name", "token-value", srv.URL)
	require.NoError(t, err)

	_, err = client.CreateDeployRequest(t.Context(), &ps.CreateDeployRequestRequest{
		Organization: "block",
		Database:     "orders",
		Branch:       "schemabot-orders-02846775",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "branch has no schema changes")
	assert.Contains(t, err.Error(), "orders")
}

// The service token authenticates these calls the same way the SDK's do.
func TestCreateDeployRequestSendsServiceToken(t *testing.T) {
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"number":1}`))
	}))
	t.Cleanup(srv.Close)

	client, err := NewPSClientWithBaseURL("token-name", "token-value", srv.URL)
	require.NoError(t, err)

	_, err = client.CreateDeployRequest(t.Context(), &ps.CreateDeployRequestRequest{
		Organization: "block",
		Database:     "orders",
		Branch:       "schemabot-orders-02846775",
	})
	require.NoError(t, err)
	assert.Equal(t, "token-name:token-value", auth)
}

// autoCutoverServer serves the deploy-request read endpoint with a fixed body.
func autoCutoverServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/v1/organizations/block/databases/orders/deploy-requests/132", r.URL.Path)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// The cutover setting cannot be changed once a deploy request exists, so the
// only way to know the one SchemaBot asked for is the one being honoured is to
// read it back. PlanetScale carries it on the deployment rather than on the
// deploy request itself.
func TestDeployRequestAutoCutoverReadsTheDeploymentSetting(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want bool
	}{
		{"held for the operator", `{"number":132,"deployment":{"auto_cutover":false}}`, false},
		{"performed by the backend", `{"number":132,"deployment":{"auto_cutover":true}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := NewPSClientWithBaseURL("token-name", "token-value", autoCutoverServer(t, tc.body).URL)
			require.NoError(t, err)

			autoCutover, err := client.DeployRequestAutoCutover(t.Context(), "block", "orders", 132)

			require.NoError(t, err)
			assert.Equal(t, tc.want, autoCutover)
		})
	}
}

// A caller reads this setting precisely because it cannot assume one, so a
// response that omits it is an error rather than a default. Having no deployment
// to read and having a deployment that does not report the setting are separate
// causes: the first is a read with nothing in it yet, the second is the API
// answering in a shape this no longer matches, and an operator triaging a
// refused deploy needs to know which one they are looking at.
func TestDeployRequestAutoCutoverRefusesAnUnreportedSetting(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want error
	}{
		{"no deployment to read", `{"number":132}`, ErrDeploymentNotReported},
		{"deployment without the setting", `{"number":132,"deployment":{"state":"ready"}}`, ErrAutoCutoverNotReported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := NewPSClientWithBaseURL("token-name", "token-value", autoCutoverServer(t, tc.body).URL)
			require.NoError(t, err)

			_, err = client.DeployRequestAutoCutover(t.Context(), "block", "orders", 132)

			require.Error(t, err)
			assert.ErrorIs(t, err, tc.want)
			assert.Contains(t, err.Error(), "orders")
			assert.Contains(t, err.Error(), "#132")
		})
	}
}

// A failed call travels up as the apply's failure message and is rendered into a
// PR comment. Only the API's own message field is written for a human to read;
// the rest of a response body is text from whatever answered, so a response that
// is not the API's error shape contributes nothing to the message and is kept on
// the error for the server log instead.
func TestRawRequestFailureRendersOnlyTheAPIsOwnRefusal(t *testing.T) {
	for _, tc := range []struct {
		name        string
		body        string
		wantInError string
		wantOmitted []string
	}{
		{
			name:        "the API refuses in its own words",
			body:        `{"message":"deploy request is not deploying"}`,
			wantInError: "deploy request is not deploying",
		},
		{
			name:        "something else answered",
			body:        "upstream 10.4.19.7:3306 unreachable\n| broken | table |",
			wantOmitted: []string{"10.4.19.7", "|", "\n"},
		},
		{
			name:        "the refusal would break the markdown it lands in",
			body:        `{"message":"branch | main\nis behind"}`,
			wantInError: "branch / main is behind",
			wantOmitted: []string{"|", "\n"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write([]byte(tc.body))
			}))
			t.Cleanup(srv.Close)

			client, err := NewPSClientWithBaseURL("token-name", "token-value", srv.URL)
			require.NoError(t, err)

			_, err = client.DeployRequestAutoCutover(t.Context(), "block", "orders", 132)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "502 Bad Gateway")
			assert.Contains(t, err.Error(), "/v1/organizations/block/databases/orders/deploy-requests/132")
			assert.NotContains(t, err.Error(), srv.URL, "the endpoint host does not belong in an operator-facing message")
			if tc.wantInError != "" {
				assert.Contains(t, err.Error(), tc.wantInError)
			}
			for _, omitted := range tc.wantOmitted {
				assert.NotContains(t, err.Error(), omitted)
			}

			var apiErr *APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, http.StatusBadGateway, apiErr.StatusCode)
			assert.Equal(t, tc.body, apiErr.Body, "the whole response stays on the error for the server log")
		})
	}
}

func TestDeployRequestAutoCutoverRefusesWithoutBaseURL(t *testing.T) {
	client, err := NewPSClient("token-name", "token-value")
	require.NoError(t, err)
	client.(*psClientWrapper).baseURL = ""

	_, err = client.DeployRequestAutoCutover(t.Context(), "block", "orders", 132)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no PlanetScale API base URL")
}
