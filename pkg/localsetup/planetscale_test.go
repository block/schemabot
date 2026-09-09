package localsetup

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// A Vitess target is verified and discovered through the PlanetScale API: the
// token travels only in the request header, the main branch's keyspaces are
// the namespaces offered, and reserved keyspaces are filtered the way pull does.
func TestPlanetScaleKeyspaceDiscoveryAndCheck(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		require.Equal(t, "/v1/organizations/acme/databases/shop/branches/main/keyspaces", r.URL.Path)
		require.Equal(t, "setup-token:secret-value", r.Header.Get("Authorization"))
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"name": "commerce"}, {"name": "_vt"}, {"name": "analytics"}}}))
	}))
	t.Cleanup(server.Close)
	target := Target{Engine: "vitess", Database: "shop", Organization: "acme", Token: "setup-token:secret-value", APIURL: server.URL}
	require.NoError(t, CheckPlanetScale(t.Context(), target))
	names, err := DiscoverNamespaces(t.Context(), target)
	require.NoError(t, err)
	require.Equal(t, []string{"commerce", "analytics"}, names)
	require.Equal(t, 2, requests)
}

// API failures and malformed inputs are reported without the token value.
func TestPlanetScaleErrorsNeverExposeToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"code": "unauthorized", "message": "bad token secret-value"}))
	}))
	t.Cleanup(server.Close)
	base := Target{Engine: "vitess", Database: "shop", Organization: "acme", Token: "setup-token:secret-value", APIURL: server.URL}
	err := CheckPlanetScale(t.Context(), base)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "secret-value")
	require.ErrorContains(t, err, "PlanetScale")

	for name, target := range map[string]Target{
		"missing organization": {Engine: "vitess", Database: "shop", Token: "setup-token:secret-value", APIURL: server.URL},
		"missing database":     {Engine: "vitess", Organization: "acme", Token: "setup-token:secret-value", APIURL: server.URL},
		"token without name":   {Engine: "vitess", Database: "shop", Organization: "acme", Token: "secret-value", APIURL: server.URL},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := DiscoverNamespaces(t.Context(), target)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "secret-value")
		})
	}
}

func TestParsePlanetScaleToken(t *testing.T) {
	name, value, err := parsePlanetScaleToken(" abc123 : s3cr3t ")
	require.NoError(t, err)
	require.Equal(t, "abc123", name)
	require.Equal(t, "s3cr3t", value)
	for _, raw := range []string{"", "abc123", ":value", "name:"} {
		_, _, err := parsePlanetScaleToken(raw)
		require.Error(t, err, raw)
	}
}
