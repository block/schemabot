package github

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func registerLegacyAnchorTree(t *testing.T, mux *http.ServeMux, paths map[string]string) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/{sha}", func(w http.ResponseWriter, r *http.Request) {
		commit, dir := splitSyntheticTreeSHA(r.PathValue("sha"))
		assert.Equal(t, "anchor", commit, "path existence must be checked at the anchor")
		assert.Empty(t, r.URL.Query().Get("recursive"), "resolve exact paths without a whole-repo recursive listing")
		require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{Entries: treeLevel(commit, dir, paths)}))
	})
}

// Both identical refs and an empty path history require paths that exist at
// the anchor. A missing second path must fail even when the first is valid.
func TestLegacyPathChangesSinceAnchorValidatesEveryPath(t *testing.T) {
	for _, status := range []string{"identical", "ahead"} {
		for _, tc := range []struct {
			name  string
			paths []string
			want  string
		}{
			{name: "directory", paths: []string{"db/changes"}},
			{name: "file", paths: []string{"db/changes/001.sql"}},
			{name: "missing root", paths: []string{"never/existed/typo"}, want: "does not exist at anchor"},
			{name: "missing nested path", paths: []string{"db/typo"}, want: "does not exist at anchor"},
			{name: "missing second path", paths: []string{"db/changes", "db/typo"}, want: "does not exist at anchor"},
			{name: "file as directory", paths: []string{"db/changes/001.sql/child"}, want: "does not exist at anchor"},
		} {
			t.Run(status+"/"+tc.name, func(t *testing.T) {
				client, mux := setupRateLimitedTestGitHubServer(t)
				registerLegacyAnchorTree(t, mux, map[string]string{"db/changes/001.sql": "schema-blob"})
				base := "base"
				if status == "identical" {
					base = "anchor"
				}
				mux.HandleFunc("GET /repos/octocat/hello-world/compare/anchor..."+base, func(w http.ResponseWriter, _ *http.Request) {
					comparison := gh.CommitsComparison{Status: &status}
					if status == "ahead" {
						comparison.TotalCommits = new(1)
						comparison.Commits = []*gh.RepositoryCommit{{SHA: new("unrelated-change")}}
					}
					require.NoError(t, json.NewEncoder(w).Encode(comparison))
				})
				mux.HandleFunc("GET /repos/octocat/hello-world/commits", func(w http.ResponseWriter, r *http.Request) {
					assert.Equal(t, "ahead", status)
					assert.Equal(t, "base", r.URL.Query().Get("sha"))
					require.NoError(t, json.NewEncoder(w).Encode([]*gh.RepositoryCommit{}))
				})

				ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
				changes, err := ic.LegacyPathChangesSinceAnchor(t.Context(), "octocat/hello-world", "anchor", base, tc.paths)
				if tc.want != "" {
					require.ErrorContains(t, err, tc.want)
					return
				}
				require.NoError(t, err)
				assert.Empty(t, changes)
			})
		}
	}
}

func TestLegacyPathChangesSinceAnchorRejectsUnverifiablePath(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		tree gh.Tree
		want string
	}{
		{name: "read failure", code: http.StatusForbidden, want: "verify legacy path"},
		{name: "truncated tree", tree: gh.Tree{Truncated: new(true)}, want: "truncated"},
		{name: "missing object SHA", tree: gh.Tree{Entries: []*gh.TreeEntry{{Path: new("db"), Type: new("tree")}}}, want: "must resolve to a file or directory"},
		{name: "submodule", tree: gh.Tree{Entries: []*gh.TreeEntry{{Path: new("db"), Type: new("commit"), SHA: new("submodule-commit")}}}, want: "must resolve to a file or directory"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, mux := setupRateLimitedTestGitHubServer(t)
			mux.HandleFunc("GET /repos/octocat/hello-world/compare/anchor...anchor", func(w http.ResponseWriter, _ *http.Request) {
				require.NoError(t, json.NewEncoder(w).Encode(gh.CommitsComparison{Status: new("identical")}))
			})
			mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/anchor", func(w http.ResponseWriter, r *http.Request) {
				if tc.code != 0 {
					http.Error(w, "cannot read tree", tc.code)
					return
				}
				require.NoError(t, json.NewEncoder(w).Encode(tc.tree))
			})
			ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
			_, err := ic.LegacyPathChangesSinceAnchor(t.Context(), "octocat/hello-world", "anchor", "anchor", []string{"db"})
			require.ErrorContains(t, err, tc.want)
		})
	}
}
