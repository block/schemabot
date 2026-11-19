package github

import (
	"encoding/base64"
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

// schemaDirServer serves a two-file schema directory whose listing reports
// each file's blob SHA per ref, and counts every content read by path.
// users.sql has the same blob at every ref; orders.sql changes per ref.
func schemaDirServer(t *testing.T, refs map[string]string) (*InstallationClient, map[string]*atomic.Int64) {
	t.Helper()
	client, mux := setupRateLimitedTestGitHubServer(t)
	usersBlob := strings.Repeat("1", 40)
	reads := map[string]*atomic.Int64{"schema/users.sql": {}, "schema/orders.sql": {}}
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema", func(w http.ResponseWriter, r *http.Request) {
		ordersBlob, ok := refs[r.URL.Query().Get("ref")]
		require.True(t, ok, "unexpected ref %q", r.URL.Query().Get("ref"))
		require.NoError(t, json.NewEncoder(w).Encode([]gh.RepositoryContent{
			{Type: new("file"), Name: new("users.sql"), Path: new("schema/users.sql"), SHA: new(usersBlob)},
			{Type: new("file"), Name: new("orders.sql"), Path: new("schema/orders.sql"), SHA: new(ordersBlob)},
		}))
	})
	serveFile := func(name string, content func(ref string) string) {
		mux.HandleFunc("GET /repos/octocat/hello-world/contents/schema/"+name, func(w http.ResponseWriter, r *http.Request) {
			reads["schema/"+name].Add(1)
			require.NoError(t, json.NewEncoder(w).Encode(gh.RepositoryContent{
				Type: new("file"), Name: new(name), Path: new("schema/" + name),
				Encoding: new("base64"),
				Content:  new(base64.StdEncoding.EncodeToString([]byte(content(r.URL.Query().Get("ref"))))),
			}))
		})
	}
	serveFile("users.sql", func(string) string { return "CREATE TABLE users (id INT);" })
	serveFile("orders.sql", func(ref string) string { return "CREATE TABLE orders (id INT); -- " + ref })
	return NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil))), reads
}

func filesByPath(files []GitHubFile) map[string]string {
	out := make(map[string]string, len(files))
	for _, f := range files {
		out[f.Path] = f.Content
	}
	return out
}

// Plans of two pushes to a PR, and of each environment of one push, list the
// schema directory at a commit SHA. A file whose blob did not change between
// them is read from GitHub once; a file whose blob changed is read at each
// commit, and each plan sees its own commit's content.
func TestFetchSchemaFilesOptimizedReadsUnchangedBlobsOnce(t *testing.T) {
	firstPush, secondPush := strings.Repeat("a", 40), strings.Repeat("b", 40)
	ic, reads := schemaDirServer(t, map[string]string{
		firstPush:  strings.Repeat("2", 40),
		secondPush: strings.Repeat("3", 40),
	})

	for range 2 { // two environments planned from the first push
		files, err := ic.FetchSchemaFilesOptimized(t.Context(), "octocat/hello-world", firstPush, "schema", "mysql")
		require.NoError(t, err)
		assert.Equal(t, map[string]string{
			"schema/users.sql":  "CREATE TABLE users (id INT);",
			"schema/orders.sql": "CREATE TABLE orders (id INT); -- " + firstPush,
		}, filesByPath(files))
	}
	files, err := ic.FetchSchemaFilesOptimized(t.Context(), "octocat/hello-world", secondPush, "schema", "mysql")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"schema/users.sql":  "CREATE TABLE users (id INT);",
		"schema/orders.sql": "CREATE TABLE orders (id INT); -- " + secondPush,
	}, filesByPath(files))

	assert.Equal(t, int64(1), reads["schema/users.sql"].Load(), "the unchanged blob is read once across environments and pushes")
	assert.Equal(t, int64(2), reads["schema/orders.sql"].Load(), "the changed blob is read once per distinct blob")
}

// A listing taken at a branch name can name other content by the time each
// file is read, so files listed at a branch are read every time, never served
// from or added to the cache.
func TestFetchSchemaFilesOptimizedDoesNotCacheBranchReads(t *testing.T) {
	ic, reads := schemaDirServer(t, map[string]string{"main": strings.Repeat("2", 40)})

	for range 2 {
		_, err := ic.FetchSchemaFilesOptimized(t.Context(), "octocat/hello-world", "main", "schema", "mysql")
		require.NoError(t, err)
	}

	assert.Equal(t, int64(2), reads["schema/users.sql"].Load())
	assert.Equal(t, int64(2), reads["schema/orders.sql"].Load())
}

// The cache is bounded by content size: a blob that would push it past its
// limit starts it over empty rather than growing without bound, and a short
// or non-hex key is never cached.
func TestBlobContentCacheStartsOverAtItsLimit(t *testing.T) {
	cache := newBlobContentCache()
	first, second := strings.Repeat("c", 40), strings.Repeat("d", 40)
	cache.put("octocat/hello-world", first, strings.Repeat("x", maxCachedBlobBytes-1))
	_, ok := cache.get("octocat/hello-world", first)
	require.True(t, ok)

	cache.put("octocat/hello-world", second, "xy")
	_, ok = cache.get("octocat/hello-world", first)
	assert.False(t, ok, "the cache starts over instead of exceeding its limit")
	content, ok := cache.get("octocat/hello-world", second)
	assert.True(t, ok)
	assert.Equal(t, "xy", content)

	cache.put("octocat/hello-world", "abc123", "short")
	_, ok = cache.get("octocat/hello-world", "abc123")
	assert.False(t, ok, "only a full object name keys immutable content")
}
