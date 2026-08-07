package psclient

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// throttleRequest captures what the client put on the wire so the test can
// assert against PlanetScale's published contract for the throttler endpoint.
type throttleRequest struct {
	method string
	path   string
	body   string
	auth   string
}

func newThrottleServer(t *testing.T, status int, responseBody string) (*httptest.Server, *throttleRequest) {
	t.Helper()
	captured := &throttleRequest{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		captured.method = r.Method
		captured.path = r.URL.Path
		captured.body = string(body)
		captured.auth = r.Header.Get("Authorization")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(responseBody))
	}))
	t.Cleanup(server.Close)
	return server, captured
}

func TestThrottleDeployRequestUsesThrottlerEndpoint(t *testing.T) {
	server, captured := newThrottleServer(t, http.StatusOK, `{"keyspaces":["commerce"]}`)

	client, err := NewPSClientWithBaseURL("token-name", "token-value", server.URL)
	require.NoError(t, err)

	require.NoError(t, client.ThrottleDeployRequest(t.Context(), &ThrottleDeployRequestRequest{
		Organization:  "acme",
		Database:      "orders",
		Number:        42,
		ThrottleRatio: 0.85,
	}))

	assert.Equal(t, http.MethodPatch, captured.method)
	assert.Equal(t, "/v1/organizations/acme/databases/orders/deploy-requests/42/throttler", captured.path)
	assert.JSONEq(t, `{"ratio":85}`, captured.body)
	assert.Equal(t, "token-name:token-value", captured.auth)
}

func TestThrottleDeployRequestConvertsRatioToWholePercent(t *testing.T) {
	tests := []struct {
		ratio float64
		want  string
	}{
		{0.0, `{"ratio":0}`},
		{0.05, `{"ratio":5}`},
		{0.85, `{"ratio":85}`},
		{0.95, `{"ratio":95}`},
	}
	for _, tc := range tests {
		server, captured := newThrottleServer(t, http.StatusOK, "{}")
		client, err := NewPSClientWithBaseURL("token-name", "token-value", server.URL)
		require.NoError(t, err)

		require.NoError(t, client.ThrottleDeployRequest(t.Context(), &ThrottleDeployRequestRequest{
			Organization:  "acme",
			Database:      "orders",
			Number:        7,
			ThrottleRatio: tc.ratio,
		}))
		assert.JSONEq(t, tc.want, captured.body, "ratio %v", tc.ratio)
	}
}

// A failing throttle call must carry the API's explanation, not just the status
// line, so an operator can tell a moved endpoint from a deleted deploy request.
func TestThrottleDeployRequestErrorCarriesResponseExcerpt(t *testing.T) {
	server, _ := newThrottleServer(t, http.StatusNotFound, "{\"error\":\"Not Found\",\n\"detail\":\"deploy request 42 does not exist\"}")

	client, err := NewPSClientWithBaseURL("token-name", "token-value", server.URL)
	require.NoError(t, err)

	err = client.ThrottleDeployRequest(t.Context(), &ThrottleDeployRequestRequest{
		Organization:  "acme",
		Database:      "orders",
		Number:        42,
		ThrottleRatio: 0.85,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "404")
	assert.Contains(t, err.Error(), "deploy request 42 does not exist")
	assert.Contains(t, err.Error(), "throttle deploy request 42 (acme/orders)")
	assert.NotContains(t, err.Error(), "\n", "the excerpt must not break log formatting")
}

func TestThrottleDeployRequestRequiresBaseURL(t *testing.T) {
	client := &psClientWrapper{}
	err := client.ThrottleDeployRequest(t.Context(), &ThrottleDeployRequestRequest{
		Organization: "acme",
		Database:     "orders",
		Number:       42,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "base URL")
}

func TestSanitizeResponseExcerpt(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{"empty", "", "<empty response body>"},
		{"whitespace only", "  \n\t ", "<empty response body>"},
		{"collapses newlines", "line one\nline two\r\nline three", "line one line two line three"},
		{"drops control runes", "before\x00\x07after", "beforeafter"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, sanitizeResponseExcerpt(strings.NewReader(tc.body)))
		})
	}

	long := sanitizeResponseExcerpt(strings.NewReader(strings.Repeat("x", 5000)))
	assert.Len(t, []rune(long), maxResponseExcerptLen)
	assert.True(t, strings.HasSuffix(long, "…"))
}
