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
