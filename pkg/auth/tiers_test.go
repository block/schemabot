package auth

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTierForRequest(t *testing.T) {
	cases := []struct {
		method, path string
		want         Tier
	}{
		{http.MethodGet, "/api/status", TierRead},
		{http.MethodGet, "/api/logs/coffee", TierRead},
		{http.MethodGet, "/api/locks", TierRead},
		{http.MethodGet, "/api/settings", TierRead},
		{http.MethodPost, "/api/pull", TierRead},  // reading live schema is visibility, not a change
		{http.MethodPost, "/api/plan", TierWrite}, // planning stages a change
		{http.MethodPost, "/api/rollback/plan", TierWrite},
		{http.MethodPost, "/api/apply", TierWrite},
		{http.MethodPost, "/api/cutover", TierWrite},
		{http.MethodPost, "/api/webhooks/redrive", TierWrite},
		{http.MethodPost, "/api/checks/scan", TierWrite},
		{http.MethodPost, "/api/checks/synthesize", TierWrite},
		{http.MethodPost, "/api/settings", TierWrite},
		{http.MethodDelete, "/api/locks", TierWrite},
		// SchemaBot's own storage schema is admin territory on both halves:
		// the diff exposes the internal shape of its bookkeeping database, and
		// its sibling route converges it. On a deployment configured with only
		// read and write groups this tier is the whole admin decision, so the
		// GET must not sit at the read tier.
		{http.MethodGet, "/api/storage/schema/diff", TierWrite},
		{http.MethodPost, "/api/storage/schema/apply", TierWrite},
	}
	for _, c := range cases {
		assert.Equalf(t, c.want, TierForRequest(c.method, c.path), "%s %s", c.method, c.path)
	}
}

// Every write-tier GET is listed in writePaths, and a GET outside that list
// stays a read. The list is the only thing standing between a read-tier
// classification and an endpoint everyone with read access can call, so it has
// to be exact rather than approximate.
func TestWritePathsCoverEveryWriteTierGet(t *testing.T) {
	for path := range writePaths {
		assert.Equalf(t, TierWrite, TierForRequest(http.MethodGet, path), "GET %s", path)
		assert.Equalf(t, TierWrite, TierForRequest(http.MethodHead, path), "HEAD %s", path)
	}
	assert.Equal(t, TierRead, TierForRequest(http.MethodGet, "/api/storage/schema"),
		"a prefix of a write path is not itself a write path")
}

func TestMatchesAnyGroup(t *testing.T) {
	admin := []string{"octocat/schema-admins"}

	assert.True(t, matchesAnyGroup([]string{"schema-admins"}, admin), "bare slug should match org/slug")
	assert.True(t, matchesAnyGroup([]string{"octocat/schema-admins"}, admin), "exact match")
	assert.True(t, matchesAnyGroup([]string{"x", "schema-admins"}, admin), "any caller group matches")
	assert.False(t, matchesAnyGroup([]string{"other-team"}, admin))
	assert.False(t, matchesAnyGroup(nil, admin))
	assert.False(t, matchesAnyGroup([]string{"schema-admins"}, nil), "no admin groups configured")

	// Slug matching must not cross organization boundaries: two org-qualified
	// names with the same slug only match on an exact string.
	assert.False(t, matchesAnyGroup([]string{"other-org/schema-admins"}, admin),
		"same slug under a different org must not match")
}
