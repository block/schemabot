package client

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A local credential cannot follow a redirect to another listener, including a
// different loopback port. No redirected request reaches the second server.
func TestLocalCredentialBoundToEndpoint(t *testing.T) {
	var forwarded atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { forwarded.Store(true) }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	transport := &bearerTransport{base: http.DefaultTransport, token: "private-runtime-token", origin: source.URL}
	client := &http.Client{Transport: transport}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, source.URL, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	require.ErrorContains(t, err, "refusing to forward")
	assert.False(t, forwarded.Load())
}

func TestLocalProfileExplicitSelection(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("SCHEMABOT_PROFILE", "")
	t.Setenv("SCHEMABOT_ENDPOINT", "")
	t.Setenv("SCHEMABOT_TOKEN", "")
	require.NoError(t, SaveConfig(&Config{DefaultProfile: "local", Profiles: map[string]Profile{"local": {LocalRuntime: "sample"}}}))
	connection, err := ResolveLocalConnection(t.Context(), "https://service.example", "local", "", "dev")
	require.NoError(t, err)
	assert.Nil(t, connection)
	_, err = ResolveLocalConnection(t.Context(), "", "local", "override", "dev")
	require.ErrorContains(t, err, "authentication overrides")
	require.NoError(t, SaveConfig(&Config{Profiles: map[string]Profile{"remote": {Endpoint: "http://127.0.0.1:1"}}}))
	connection, err = ResolveLocalConnection(t.Context(), "", "remote", "", "dev")
	require.NoError(t, err)
	assert.Nil(t, connection)
}
