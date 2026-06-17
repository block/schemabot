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

func TestAuthTokenAttachedAsBearer(t *testing.T) {
	var gotAuth string
	srv := captureAuthServer(t, &gotAuth)
	setTokenForTest(t, "tok-abc123")

	var out map[string]any
	require.NoError(t, doGetInto(srv.URL, "/api/status", &out))
	assert.Equal(t, "Bearer tok-abc123", gotAuth)
}

func TestNoAuthTokenSendsNoHeader(t *testing.T) {
	var gotAuth string
	srv := captureAuthServer(t, &gotAuth)
	setTokenForTest(t, "")

	var out map[string]any
	require.NoError(t, doGetInto(srv.URL, "/api/status", &out))
	assert.Empty(t, gotAuth)
}

func TestAuthTokenTrimmed(t *testing.T) {
	var gotAuth string
	srv := captureAuthServer(t, &gotAuth)
	setTokenForTest(t, "  tok-padded\n")

	var out map[string]any
	require.NoError(t, doPostInto(srv.URL, "/api/plan", map[string]string{}, &out))
	assert.Equal(t, "Bearer tok-padded", gotAuth)
}
