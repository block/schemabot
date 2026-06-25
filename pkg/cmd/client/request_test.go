package client

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureAuthServer returns a test server that records the Authorization header
// of the request it receives and replies with an empty JSON object.
func captureAuthServer(t *testing.T, gotAuth *string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// setTokenForTest sets the shared auth token and restores it after the test so
// the package-level transport state does not leak between tests.
func setTokenForTest(t *testing.T, token string) {
	t.Helper()
	prev := authTransport.token
	t.Cleanup(func() { authTransport.token = prev })
	SetAuthToken(token)
}

// sendThroughTransport drives a request through the shared httpClient (and thus
// the bearer transport), tying it to the test lifecycle via t.Context(). The
// optional preset header is set before the transport runs, to verify the
// transport does not clobber caller-provided headers.
func sendThroughTransport(t *testing.T, rawURL string, presetAuth string) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, rawURL, nil)
	require.NoError(t, err)
	if presetAuth != "" {
		req.Header.Set("Authorization", presetAuth)
	}
	return httpClient.Do(req)
}

func TestAuthTokenAttachedAsBearer(t *testing.T) {
	var gotAuth string
	srv := captureAuthServer(t, &gotAuth)
	setTokenForTest(t, "tok-abc123")

	resp, err := sendThroughTransport(t, srv.URL+"/api/status", "")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, "Bearer tok-abc123", gotAuth)
}

func TestNoAuthTokenSendsNoHeader(t *testing.T) {
	var gotAuth string
	srv := captureAuthServer(t, &gotAuth)
	setTokenForTest(t, "")

	resp, err := sendThroughTransport(t, srv.URL+"/api/status", "")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Empty(t, gotAuth)
}

func TestAuthTokenTrimmed(t *testing.T) {
	var gotAuth string
	srv := captureAuthServer(t, &gotAuth)
	setTokenForTest(t, "  tok-padded\n")

	resp, err := sendThroughTransport(t, srv.URL+"/api/status", "")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, "Bearer tok-padded", gotAuth)
}

func TestExistingAuthorizationHeaderPreserved(t *testing.T) {
	var gotAuth string
	srv := captureAuthServer(t, &gotAuth)
	setTokenForTest(t, "tok-abc123")

	resp, err := sendThroughTransport(t, srv.URL+"/api/status", "Basic dXNlcjpwYXNz")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, "Basic dXNlcjpwYXNz", gotAuth)
}

func TestAuthTokenAllowedOverLoopbackHTTP(t *testing.T) {
	// httptest serves plaintext on 127.0.0.1, which is loopback and therefore
	// safe to send a token to during local development.
	var gotAuth string
	srv := captureAuthServer(t, &gotAuth)
	setTokenForTest(t, "tok-local")

	resp, err := sendThroughTransport(t, srv.URL+"/api/status", "")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, "Bearer tok-local", gotAuth)
}

func TestAuthTokenRefusedOverInsecureRemote(t *testing.T) {
	setTokenForTest(t, "tok-abc123")

	var out map[string]any
	err := doGetIntoCtx(t.Context(), "http://schemabot.example.com", "/api/status", &out)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInsecureTokenTransport)
}

func TestNoTokenAllowedOverInsecureRemote(t *testing.T) {
	// Without a token there is nothing to leak, so the insecure-transport guard
	// must not block ordinary unauthenticated requests: the request proceeds
	// past the guard and fails on connection, not on the token guard.
	setTokenForTest(t, "")

	var out map[string]any
	err := doGetIntoCtx(t.Context(), "http://schemabot.example.com", "/api/status", &out)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrInsecureTokenTransport)
}
