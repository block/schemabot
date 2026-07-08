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
		{http.MethodPost, "/api/settings", TierWrite},
		{http.MethodDelete, "/api/locks", TierWrite},
	}
	for _, c := range cases {
		assert.Equalf(t, c.want, tierForRequest(c.method, c.path), "%s %s", c.method, c.path)
	}
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
