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
// listing reads the mode of every file that could be a symlink from the tree,
// and the PR counts as changing a schema input and is discovered from the
// symlink's config directory.
func TestFetchPRFilesMarksSymlinks(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"number": 1,
			"state":  "open",
			"head":   map[string]any{"sha": "head-sha", "ref": "feature"},
			"base":   map[string]any{"sha": "base-sha", "ref": "main"},
			"user":   map[string]any{"login": "octocat"},
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]map[string]any{
			{"filename": "orders/schema/staging", "status": "modified", "additions": 1, "deletions": 1},
			{"filename": "orders/schema/NOTES", "status": "modified", "additions": 1, "deletions": 1},
			{"filename": "orders/schema/legacy", "status": "removed", "additions": 0, "deletions": 1},
			{"filename": "orders/schema/qa", "previous_filename": "orders/schema/test", "status": "renamed"},
			{"filename": "app/service.go", "status": "modified", "additions": 12, "deletions": 3},
		}))
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
		entry("schemabot.yaml", "100644"), entry("staging", gitSymlinkMode), entry("NOTES", "100644"), entry("qa", "100644")))
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/main", tree(dir("orders", "orders-main")))
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/orders-main", tree(dir("schema", "schema-main")))
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/schema-main", tree(
		entry("schemabot.yaml", "100644"), entry("legacy", gitSymlinkMode), entry("test", gitSymlinkMode)))
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/app-head", func(w http.ResponseWriter, _ *http.Request) {
		t.Errorf("a file with more than one line added cannot be a symlink, so its directory is never read")
		w.WriteHeader(http.StatusInternalServerError)
	})

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	files, err := ic.FetchPRFiles(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)

	symlinks := make(map[string]bool)
	for _, f := range files {
		symlinks[f.Filename] = f.Symlink
	}
	assert.Equal(t, map[string]bool{
		"orders/schema/staging": true,  // a symlink at the head
		"orders/schema/NOTES":   false, // a one-line regular file
		"orders/schema/legacy":  true,  // a symlink the PR removes, read at the base
		"orders/schema/qa":      true,  // a regular file now, renamed from a symlink at the base
		"app/service.go":        false,
	}, symlinks)

	onlyStaging := []PRFile{files[0]}
	assert.True(t, HasSchemaInputFiles(onlyStaging), "a PR that only repoints an environment symlink changes a schema input")
	assert.Equal(t, []string{"orders/schema/staging"}, DiscoverySchemaPaths(onlyStaging))
	assert.False(t, HasSchemaInputFiles([]PRFile{files[1]}), "a one-line regular file is not a schema input")
}

// A symlink holds its target on one line, so only a file with at most one
// line added can be a symlink after the change, and only one with at most one
// line deleted can have been one before it. Every other file skips the tree
// read.
func TestSymlinkCandidates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                     string
		file                     PRFile
		wantCurrent, wantVacated bool
	}{
		{name: "a repointed symlink", file: PRFile{Filename: "schema/staging", Status: "modified", Additions: 1, Deletions: 1}, wantCurrent: true},
		{name: "an added symlink", file: PRFile{Filename: "schema/staging", Status: "added", Additions: 1}, wantCurrent: true},
		{name: "a regular file turned into a symlink", file: PRFile{Filename: "schema/staging", Status: "modified", Additions: 1, Deletions: 40}, wantCurrent: true},
		{name: "a removed symlink", file: PRFile{Filename: "schema/staging", Status: "removed", Deletions: 1}, wantCurrent: true},
		{name: "a removed multi-line file", file: PRFile{Filename: "schema/README", Status: "removed", Deletions: 40}},
		{name: "a multi-line edit", file: PRFile{Filename: "schema/README", Status: "modified", Additions: 3, Deletions: 1}},
		{name: "a renamed symlink", file: PRFile{Filename: "schema/qa", PreviousFilename: "schema/test", Status: "renamed"}, wantCurrent: true, wantVacated: true},
		{name: "a renamed multi-line file", file: PRFile{Filename: "schema/qa.md", PreviousFilename: "schema/test.md", Status: "renamed", Additions: 5, Deletions: 5}},
		{name: "a schema file, an input by name", file: PRFile{Filename: "schema/orders.sql", Status: "modified", Additions: 1, Deletions: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			current, vacated := symlinkCandidates(tt.file)
			assert.Equal(t, tt.wantCurrent, current, "current path")
			assert.Equal(t, tt.wantVacated, vacated, "vacated path")
		})
	}
}
