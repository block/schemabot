package github

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A PR that only repoints an environment symlink, such as
// orders/schema/staging from production to base, changes the schema the
// environment plans. GitHub's file listing carries no file mode, so the
// listing reads the mode of every path that could be a symlink from the tree:
// at the head, and at the PR's merge base for what the change removed,
// replaced, or renamed away. Each counts as changing a schema input and is
// discovered from the symlink's config directory.
func TestFetchPRFilesMarksSymlinks(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"number": 1,
			"state":  "open",
			"head":   map[string]any{"sha": "head-sha", "ref": "feature"},
			"base":   map[string]any{"sha": "stale-base-sha", "ref": "main"},
			"user":   map[string]any{"login": "octocat"},
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]map[string]any{
			{"filename": "orders/schema/staging", "status": "modified", "additions": 1, "deletions": 1},
			{"filename": "orders/schema/NOTES", "status": "modified", "additions": 1, "deletions": 1},
			{"filename": "orders/schema/legacy", "status": "removed", "additions": 0, "deletions": 1},
			{"filename": "orders/schema/qa", "previous_filename": "orders/schema/test", "status": "renamed"},
			{"filename": "orders/schema/production", "status": "modified", "additions": 40, "deletions": 1},
			{"filename": "app/service.go", "status": "modified", "additions": 12, "deletions": 3},
		}))
	})
	registerBaseTipRef(t, mux, "tip-sha")
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/tip-sha...head-sha", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"merge_base_commit": map[string]any{"sha": "merge-base-sha"}}))
	})
	tree := func(entries ...map[string]any) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"sha": "root", "truncated": false, "tree": entries}))
		}
	}
	dir := func(name, sha string) map[string]any {
		return map[string]any{"path": name, "mode": gitTreeMode, "type": "tree", "sha": sha}
	}
	entry := func(name, mode string) map[string]any {
		return map[string]any{"path": name, "mode": mode, "type": "blob", "sha": name + "-blob"}
	}
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/head-sha", tree(dir("orders", "orders-head"), dir("app", "app-head")))
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/orders-head", tree(dir("schema", "schema-head")))
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/schema-head", tree(
		entry("schemabot.yaml", "100644"), entry("staging", gitSymlinkMode), entry("NOTES", "100644"), entry("qa", "100644"), entry("production", "100644")))
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/merge-base-sha", tree(dir("orders", "orders-base")))
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/orders-base", tree(dir("schema", "schema-base")))
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/schema-base", tree(
		entry("schemabot.yaml", "100644"), entry("NOTES", "100644"), entry("legacy", gitSymlinkMode), entry("test", gitSymlinkMode), entry("production", gitSymlinkMode)))
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/app-head", func(w http.ResponseWriter, _ *http.Request) {
		t.Errorf("a file with more than one line added and deleted cannot be a symlink, so its directory is never read")
		w.WriteHeader(http.StatusInternalServerError)
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	files, err := ic.FetchPRFiles(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)

	type symlinks struct{ current, previous bool }
	got := make(map[string]symlinks)
	byName := make(map[string]PRFile)
	for _, f := range files {
		got[f.Filename] = symlinks{f.Symlink, f.PreviousSymlink}
		byName[f.Filename] = f
	}
	assert.Equal(t, map[string]symlinks{
		"orders/schema/staging":    {current: true},  // a symlink at the head
		"orders/schema/NOTES":      {},               // a one-line regular file at both commits
		"orders/schema/legacy":     {current: true},  // a symlink the PR removes, read at the merge base
		"orders/schema/qa":         {previous: true}, // a regular file now, renamed from a symlink
		"orders/schema/production": {current: true},  // a symlink the PR replaces with a regular file
		"app/service.go":           {},
	}, got)

	onlyStaging := []PRFile{byName["orders/schema/staging"]}
	assert.True(t, HasSchemaInputFiles(onlyStaging), "a PR that only repoints an environment symlink changes a schema input")
	assert.Equal(t, []string{"orders/schema/staging"}, DiscoverySchemaPaths(onlyStaging))
	onlyRename := []PRFile{byName["orders/schema/qa"]}
	assert.True(t, HasSchemaInputFiles(onlyRename), "a PR that only renames a symlink away changes a schema input")
	assert.Equal(t, []string{"orders/schema/test"}, DiscoverySchemaPaths(onlyRename), "discovery walks up from the vacated symlink")
	assert.True(t, HasSchemaInputFiles([]PRFile{byName["orders/schema/production"]}), "replacing a symlink with a regular file changes a schema input")
	assert.False(t, HasSchemaInputFiles([]PRFile{byName["orders/schema/NOTES"]}), "a one-line regular file is not a schema input")
}

// A symlink holds its target on one line, so only a path with at most one
// line added can be a symlink after the change, and only one with at most one
// line deleted can have been one before it. Every other path skips the tree
// read.
func TestSymlinkCandidates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                                   string
		file                                   PRFile
		wantHead, wantBase, wantPreviousAtBase bool
	}{
		{name: "a repointed symlink", file: PRFile{Filename: "schema/staging", Status: "modified", Additions: 1, Deletions: 1}, wantHead: true, wantBase: true},
		{name: "an added symlink", file: PRFile{Filename: "schema/staging", Status: "added", Additions: 1}, wantHead: true},
		{name: "a regular file turned into a symlink", file: PRFile{Filename: "schema/staging", Status: "modified", Additions: 1, Deletions: 40}, wantHead: true},
		{name: "a symlink turned into a regular file", file: PRFile{Filename: "schema/staging", Status: "modified", Additions: 40, Deletions: 1}, wantBase: true},
		{name: "a mode change with the same bytes", file: PRFile{Filename: "schema/staging", Status: "changed"}, wantHead: true, wantBase: true},
		{name: "a removed symlink", file: PRFile{Filename: "schema/staging", Status: "removed", Deletions: 1}, wantBase: true},
		{name: "a removed multi-line file", file: PRFile{Filename: "schema/README", Status: "removed", Deletions: 40}},
		{name: "a multi-line edit", file: PRFile{Filename: "schema/README", Status: "modified", Additions: 3, Deletions: 3}},
		{name: "a renamed symlink", file: PRFile{Filename: "schema/qa", PreviousFilename: "schema/test", Status: "renamed"}, wantHead: true, wantPreviousAtBase: true},
		{name: "a renamed multi-line file", file: PRFile{Filename: "schema/qa.md", PreviousFilename: "schema/test.md", Status: "renamed", Additions: 5, Deletions: 5}},
		{name: "a schema file, an input by name", file: PRFile{Filename: "schema/orders.sql", Status: "modified", Additions: 1, Deletions: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			atHead, atBase, previousAtBase := symlinkCandidates(tt.file)
			assert.Equal(t, tt.wantHead, atHead, "current path at the head")
			assert.Equal(t, tt.wantBase, atBase, "current path at the base")
			assert.Equal(t, tt.wantPreviousAtBase, previousAtBase, "vacated path at the base")
		})
	}
}
