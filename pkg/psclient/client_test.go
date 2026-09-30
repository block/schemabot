package psclient

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

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

// keyspacesServer serves the keyspace list endpoint for branch main of
// block/orders, answering each requested page with the body pages maps it to.
// A page with no entry is answered 404, so a request the test did not expect
// fails loudly. The pages requested, in order, are appended to requested.
func keyspacesServer(t *testing.T, pages map[string]string, requested *[]string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/v1/organizations/block/databases/orders/branches/main/keyspaces", r.URL.Path)
		assert.Equal(t, "token-name:token-value", r.Header.Get("Authorization"))
		assert.Equal(t, "100", r.URL.Query().Get("per_page"))
		page := r.URL.Query().Get("page")
		*requested = append(*requested, page)
		body, ok := pages[page]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"page not found"}`))
			return
		}
		if body == "" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"message":"keyspace listing unavailable"}`))
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func listOrdersKeyspaces(t *testing.T, baseURL string) ([]*ps.Keyspace, error) {
	t.Helper()
	client, err := NewPSClientWithBaseURL("token-name", "token-value", baseURL)
	require.NoError(t, err)
	return client.ListKeyspaces(t.Context(), &ps.ListKeyspacesRequest{
		Organization: "block",
		Database:     "orders",
		Branch:       "main",
	})
}

func keyspaceNames(keyspaces []*ps.Keyspace) []string {
	names := make([]string, 0, len(keyspaces))
	for _, ks := range keyspaces {
		names = append(names, ks.Name)
	}
	return names
}

// A branch with more keyspaces than fit on one page is listed in full: every
// page the API reports is read, in order, so progress and failure detail cover
// the keyspaces on the later pages too.
func TestListKeyspacesReadsEveryPage(t *testing.T) {
	var requested []string
	srv := keyspacesServer(t, map[string]string{
		"1": `{"type":"list","current_page":1,"next_page":2,"data":[{"name":"orders","shards":2},{"name":"orders_lookup","shards":1}]}`,
		"2": `{"type":"list","current_page":2,"next_page":3,"data":[{"name":"payments","shards":4}]}`,
		"3": `{"type":"list","current_page":3,"next_page":null,"data":[{"name":"refunds","shards":1}]}`,
	}, &requested)

	keyspaces, err := listOrdersKeyspaces(t, srv.URL)

	require.NoError(t, err)
	assert.Equal(t, []string{"orders", "orders_lookup", "payments", "refunds"}, keyspaceNames(keyspaces))
	assert.Equal(t, 4, keyspaces[2].Shards)
	assert.Equal(t, []string{"1", "2", "3"}, requested)
}

// A single page with no next page is the whole branch, and is read once.
func TestListKeyspacesStopsWhenNoNextPage(t *testing.T) {
	var requested []string
	srv := keyspacesServer(t, map[string]string{
		"1": `{"type":"list","current_page":1,"data":[{"name":"orders","shards":1}]}`,
	}, &requested)

	keyspaces, err := listOrdersKeyspaces(t, srv.URL)

	require.NoError(t, err)
	assert.Equal(t, []string{"orders"}, keyspaceNames(keyspaces))
	assert.Equal(t, []string{"1"}, requested)
}

func TestListKeyspacesStopsWhenNextPageIsZero(t *testing.T) {
	var requested []string
	srv := keyspacesServer(t, map[string]string{
		"1": `{"next_page":0,"data":[{"name":"orders","shards":1}]}`,
	}, &requested)

	keyspaces, err := listOrdersKeyspaces(t, srv.URL)

	require.NoError(t, err)
	assert.Equal(t, []string{"orders"}, keyspaceNames(keyspaces))
	assert.Equal(t, []string{"1"}, requested)
}

// A keyspace added while the pages are being read shifts the later ones onto
// the next page, so a name can be listed twice. The copy from the earlier page
// is kept, and the repeat is not counted as a second keyspace.
func TestListKeyspacesDeduplicatesNamesAcrossPages(t *testing.T) {
	var requested []string
	srv := keyspacesServer(t, map[string]string{
		"1": `{"next_page":2,"data":[{"name":"orders","shards":1}]}`,
		"2": `{"data":[{"name":"orders","shards":3},{"name":"payments","shards":2}]}`,
	}, &requested)

	keyspaces, err := listOrdersKeyspaces(t, srv.URL)

	require.NoError(t, err)
	assert.Equal(t, []string{"orders", "payments"}, keyspaceNames(keyspaces))
	assert.Equal(t, 1, keyspaces[0].Shards, "the copy from the earlier page is kept")
	assert.Equal(t, []string{"1", "2"}, requested)
}

func TestListKeyspacesFailsToDecodeALaterPage(t *testing.T) {
	var requested []string
	srv := keyspacesServer(t, map[string]string{
		"1": `{"next_page":2,"data":[{"name":"orders","shards":1}]}`,
		"2": `{not-json}`,
	}, &requested)

	keyspaces, err := listOrdersKeyspaces(t, srv.URL)

	require.Error(t, err)
	assert.Nil(t, keyspaces)
	assert.Contains(t, err.Error(), "decode keyspaces for block/orders branch main page 2")
	assert.Equal(t, []string{"1", "2"}, requested)
}

// Organization, database, and branch names are path segments. One that carries
// a character URL syntax gives meaning to is escaped, so it neither retargets
// the request nor swallows the page query that the whole listing depends on.
func TestListKeyspacesEscapesPathSegments(t *testing.T) {
	var gotPath, gotPage string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		gotPage = r.URL.Query().Get("page")
		_, _ = w.Write([]byte(`{"data":[{"name":"orders","shards":1}]}`))
	}))
	t.Cleanup(srv.Close)
	client, err := NewPSClientWithBaseURL("token-name", "token-value", srv.URL)
	require.NoError(t, err)

	keyspaces, err := client.ListKeyspaces(t.Context(), &ps.ListKeyspacesRequest{
		Organization: "block",
		Database:     "orders",
		Branch:       "feature/x?y",
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"orders"}, keyspaceNames(keyspaces))
	assert.Equal(t, "/v1/organizations/block/databases/orders/branches/feature%2Fx%3Fy/keyspaces", gotPath)
	assert.Equal(t, "1", gotPage)
}

// A failure on a later page fails the listing rather than returning the pages
// already read, and the error says which branch and page failed.
func TestListKeyspacesFailsOnALaterPage(t *testing.T) {
	var requested []string
	srv := keyspacesServer(t, map[string]string{
		"1": `{"type":"list","current_page":1,"next_page":2,"data":[{"name":"orders","shards":2}]}`,
		"2": "",
	}, &requested)

	keyspaces, err := listOrdersKeyspaces(t, srv.URL)

	require.Error(t, err)
	assert.Nil(t, keyspaces)
	assert.Contains(t, err.Error(), "list keyspaces for block/orders branch main page 2")
	assert.Contains(t, err.Error(), "keyspace listing unavailable")
	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusInternalServerError, apiErr.StatusCode)
	assert.Equal(t, []string{"1", "2"}, requested)
}

// An API that keeps reporting another page is not followed forever: the
// listing stops at the page bound and fails.
func TestListKeyspacesFailsPastThePageBound(t *testing.T) {
	var requested []string
	pages := make(map[string]string, maxKeyspacePages+1)
	for page := 1; page <= maxKeyspacePages+1; page++ {
		pages[strconv.Itoa(page)] = fmt.Sprintf(`{"current_page":%d,"next_page":%d,"data":[{"name":"ks%d"}]}`, page, page+1, page)
	}
	srv := keyspacesServer(t, pages, &requested)

	keyspaces, err := listOrdersKeyspaces(t, srv.URL)

	require.Error(t, err)
	assert.Nil(t, keyspaces)
	assert.Contains(t, err.Error(), fmt.Sprintf("API still reports page %d after %d pages and %d keyspaces", maxKeyspacePages+1, maxKeyspacePages, maxKeyspacePages))
	assert.Len(t, requested, maxKeyspacePages)
}

// A next page that does not move forward would re-read the same keyspaces
// forever, so it fails the listing at once.
func TestListKeyspacesFailsWhenNextPageDoesNotAdvance(t *testing.T) {
	var requested []string
	srv := keyspacesServer(t, map[string]string{
		"1": `{"current_page":1,"next_page":2,"data":[{"name":"orders"}]}`,
		"2": `{"current_page":2,"next_page":2,"data":[{"name":"payments"}]}`,
	}, &requested)

	keyspaces, err := listOrdersKeyspaces(t, srv.URL)

	require.Error(t, err)
	assert.Nil(t, keyspaces)
	assert.Contains(t, err.Error(), "page 2 reports next page 2, which does not advance")
	assert.Equal(t, []string{"1", "2"}, requested)
}

// Without a base URL the pages cannot be requested, and the listing refuses
// rather than return only what the SDK's first page would hold.
func TestListKeyspacesRefusesWithoutBaseURL(t *testing.T) {
	client, err := NewPSClient("token-name", "token-value")
	require.NoError(t, err)
	client.(*psClientWrapper).baseURL = ""

	_, err = client.ListKeyspaces(t.Context(), &ps.ListKeyspacesRequest{
		Organization: "block",
		Database:     "orders",
		Branch:       "main",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "list keyspaces for block/orders branch main: no PlanetScale API base URL")
}

// recordingTransport counts the requests routed through it, so a test can show
// which HTTP client actually carried a call.
type recordingTransport struct {
	calls int
}

func (rt *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.calls++
	return http.DefaultTransport.RoundTrip(req)
}

// Both constructors carry the request timeout on the client their raw-HTTP
// calls use, so a PlanetScale endpoint that stops answering cannot hold a
// driver forever.
func TestPSClientConstructorsBoundRawRequests(t *testing.T) {
	require.NotNil(t, newPlanetScaleHTTPClient().Transport,
		"the service-token option wraps the installed transport, so it must be non-nil")

	fromDefault, err := NewPSClient("token-name", "token-value")
	require.NoError(t, err)
	fromBaseURL, err := NewPSClientWithBaseURL("token-name", "token-value", "https://ps.example.com")
	require.NoError(t, err)

	for name, client := range map[string]PSClient{"NewPSClient": fromDefault, "NewPSClientWithBaseURL": fromBaseURL} {
		wrapper, ok := client.(*psClientWrapper)
		require.True(t, ok, name)
		require.NotNil(t, wrapper.httpClient, name)
		assert.Equal(t, planetScaleHTTPTimeout, wrapper.httpClient.Timeout, name)
	}
}

// A caller that passes its own HTTP client cannot displace the bounded client
// or the service token: SDK calls still go out on the bounded client, and they
// still authenticate.
func TestCallerHTTPClientCannotDisplaceTheBoundOrTheToken(t *testing.T) {
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"main"}`))
	}))
	t.Cleanup(srv.Close)

	callerTransport := &recordingTransport{}
	client, err := NewPSClient("token-name", "token-value",
		ps.WithBaseURL(srv.URL),
		ps.WithHTTPClient(&http.Client{Transport: callerTransport}),
	)
	require.NoError(t, err)

	_, err = client.GetBranch(t.Context(), &ps.GetDatabaseBranchRequest{
		Organization: "block",
		Database:     "orders",
		Branch:       "main",
	})
	require.NoError(t, err)
	assert.Zero(t, callerTransport.calls, "the SDK call must use the bounded client, not the caller's")
	assert.Equal(t, "token-name:token-value", auth)
}

// A raw-HTTP call to an endpoint that accepts the request and never answers
// returns an error once the client's timeout fires instead of blocking the
// driver.
func TestRawRequestReturnsWhenTheServerHangs(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-release
	}))
	// Cleanups run last-registered-first: release the handler before Close
	// waits for its connection.
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	wrapper := &psClientWrapper{
		httpClient: &http.Client{Timeout: 100 * time.Millisecond},
		baseURL:    srv.URL,
		tokenName:  "token-name",
		tokenValue: "token-value",
	}

	start := time.Now()
	_, err := wrapper.DeployRequestAutoCutover(t.Context(), "block", "orders", 132)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read auto_cutover for block/orders deploy request #132")
	assert.Less(t, time.Since(start), 10*time.Second, "the call must return at the client timeout")
}
