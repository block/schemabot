package github

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A tree object never changes, so the listings and comparisons one delivery
// repeats read each tree level by SHA once, across calls. A level read by a
// branch name can name another tree later, so it is read every time.
func TestResolveGitTreeEntrySharesTreeLevelsAcrossCalls(t *testing.T) {
	headSHA := strings.Repeat("a", 40)
	schemaTreeSHA := strings.Repeat("b", 40)
	client, mux := setupRateLimitedTestGitHubServer(t)
	reads := map[string]*atomic.Int64{headSHA: {}, schemaTreeSHA: {}, "main": {}}
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/{sha}", func(w http.ResponseWriter, r *http.Request) {
		sha := r.PathValue("sha")
		reads[sha].Add(1)
		entries := []*gh.TreeEntry{{Path: new("schema"), Type: new("tree"), Mode: new(gitTreeMode), SHA: new(schemaTreeSHA)}}
		if sha == schemaTreeSHA {
			entries = []*gh.TreeEntry{{Path: new("staging"), Type: new("blob"), Mode: new(gitSymlinkMode), SHA: new("link-blob")}}
		}
		require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{Entries: entries}))
	})
	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))

	for range 3 {
		entry, found, err := ic.resolveGitTreeEntry(t.Context(), "octocat/hello-world", headSHA, "schema/staging", map[string][]TreeEntry{})
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, gitSymlinkMode, entry.Mode)
		_, found, err = ic.resolveGitTreeEntry(t.Context(), "octocat/hello-world", "main", "schema/staging", map[string][]TreeEntry{})
		require.NoError(t, err)
		require.True(t, found)
	}

	assert.Equal(t, int64(1), reads[headSHA].Load(), "the commit's root level is read once")
	assert.Equal(t, int64(1), reads[schemaTreeSHA].Load(), "the subtree is read once, whichever ref reached it")
	assert.Equal(t, int64(3), reads["main"].Load(), "a level read by branch name is read every time")
}

// The cache is bounded: a level that would push it past its entry limit
// starts it over empty rather than growing without bound.
func TestTreeLevelCacheStartsOverAtItsLimit(t *testing.T) {
	cache := newTreeLevelCache()
	first, second := strings.Repeat("c", 40), strings.Repeat("d", 40)
	cache.put("octocat/hello-world", first, make([]TreeEntry, maxCachedTreeEntries-1))
	_, ok := cache.get("octocat/hello-world", first)
	require.True(t, ok)

	cache.put("octocat/hello-world", second, make([]TreeEntry, 2))

	_, ok = cache.get("octocat/hello-world", first)
	assert.False(t, ok, "the cache started over")
	_, ok = cache.get("octocat/hello-world", second)
	assert.True(t, ok)
	_, ok = cache.get("octocat/other", second)
	assert.False(t, ok, "levels are kept per repository")
}
