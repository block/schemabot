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

// A PR that only repoints an environment symlink, such as
// orders/schema/staging from production to base, changes the schema the
// environment plans. GitHub's file listing carries no file mode, so the
// listing reads the mode of every path that could be a symlink from the tree:
// at the head, and at the PR's merge base for what the change removed,
// replaced, or renamed away. A symlink to a directory counts as changing a
// schema input and is discovered from the symlink's config directory; a link
// between two files outside a config directory, such as CLAUDE.md pointing
// at AGENTS.md, does not.
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
			{"filename": "CLAUDE.md", "status": "added", "additions": 1, "deletions": 0},
			{"filename": "assets/logo.png", "status": "added", "additions": 0, "deletions": 0},
			{"filename": "app/service.go", "status": "modified", "additions": 12, "deletions": 3},
		}))
	})
	registerBaseTipRef(t, mux, "tip-sha")
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/tip-sha...head-sha", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"merge_base_commit": map[string]any{"sha": "merge-base-sha"}}))
	})
	diffs := serveCompareDiff(t, mux, "merge-base-sha...head-sha", strings.Join([]string{
		"diff --git a/orders/schema/staging b/orders/schema/staging\nindex 1111111..2222222 120000\n--- a/orders/schema/staging\n+++ b/orders/schema/staging\n@@ -1 +1 @@\n-production\n+base",
		"diff --git a/orders/schema/NOTES b/orders/schema/NOTES\nindex 1111111..2222222 100644\n--- a/orders/schema/NOTES\n+++ b/orders/schema/NOTES\n@@ -1 +1 @@\n-old\n+new",
		"diff --git a/orders/schema/legacy b/orders/schema/legacy\ndeleted file mode 120000\nindex 1111111..0000000\n--- a/orders/schema/legacy\n+++ /dev/null\n@@ -1 +0,0 @@\n-base",
		"diff --git a/orders/schema/test b/orders/schema/qa\nold mode 120000\nnew mode 100644\nsimilarity index 100%\nrename from orders/schema/test\nrename to orders/schema/qa",
		"diff --git a/orders/schema/production b/orders/schema/production\ndeleted file mode 120000\nindex 1111111..0000000\n--- a/orders/schema/production\n+++ /dev/null\n@@ -1 +0,0 @@\n-base",
		"diff --git a/orders/schema/production b/orders/schema/production\nnew file mode 100644\nindex 0000000..3333333\n--- /dev/null\n+++ b/orders/schema/production\n@@ -0,0 +1,40 @@\n+CREATE TABLE ...",
		"diff --git a/CLAUDE.md b/CLAUDE.md\nnew file mode 120000\nindex 0000000..4444444\n--- /dev/null\n+++ b/CLAUDE.md\n@@ -0,0 +1 @@\n+AGENTS.md",
		"diff --git a/assets/logo.png b/assets/logo.png\nnew file mode 100644\nindex 0000000..5555555\nBinary files /dev/null and b/assets/logo.png differ",
		"diff --git a/app/service.go b/app/service.go\nindex 1111111..2222222 100644\n--- a/app/service.go\n+++ b/app/service.go\n@@ -1,3 +1,12 @@",
	}, "\n")+"\n")
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
	targets := map[string]string{"staging-blob": "base", "legacy-blob": "base", "test-blob": "base", "production-blob": "base", "CLAUDE.md-blob": "AGENTS.md"}
	mux.HandleFunc("GET /repos/octocat/hello-world/git/blobs/{sha}", func(w http.ResponseWriter, r *http.Request) {
		target, ok := targets[r.PathValue("sha")]
		require.True(t, ok, "only a symlink's target is read, not blob %s", r.PathValue("sha"))
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"content": target, "encoding": "utf-8"}))
	})
	unread := func(why string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			t.Errorf("%s, so its directory is never read", why)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/head-sha", tree(
		dir("orders", "orders-head"), dir("app", "app-head"), dir("assets", "assets-head"), entry("CLAUDE.md", gitSymlinkMode), entry("AGENTS.md", "100644")))
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/orders-head", tree(dir("schema", "schema-head")))
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/schema-head", tree(
		entry("schemabot.yaml", "100644"), dir("base", "base-head"), entry("staging", gitSymlinkMode), entry("NOTES", "100644"), entry("qa", "100644"), entry("production", "100644")))
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/merge-base-sha", tree(dir("orders", "orders-base")))
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/orders-base", tree(dir("schema", "schema-base")))
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/schema-base", tree(
		entry("schemabot.yaml", "100644"), dir("base", "base-base"), entry("NOTES", "100644"), entry("legacy", gitSymlinkMode), entry("test", gitSymlinkMode), entry("production", gitSymlinkMode)))
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/app-head", unread("a file with more than one line added and deleted cannot be a symlink"))
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/assets-head", unread("an added file with no line added is binary"))

	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	files, err := ic.FetchPRFiles(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)

	type symlinks struct{ current, previous, vacatedDirectory bool }
	got := make(map[string]symlinks)
	byName := make(map[string]PRFile)
	for _, f := range files {
		got[f.Filename] = symlinks{f.Symlink, f.PreviousSymlink, f.VacatedDirectoryLink}
		byName[f.Filename] = f
	}
	assert.Equal(t, map[string]symlinks{
		"orders/schema/staging":    {current: true},                          // a symlink to a directory at the head
		"orders/schema/NOTES":      {},                                       // a one-line regular file at both commits
		"orders/schema/legacy":     {current: true, vacatedDirectory: true},  // a directory symlink the PR removes, read at the merge base
		"orders/schema/qa":         {previous: true, vacatedDirectory: true}, // a regular file now, renamed from a directory symlink
		"orders/schema/production": {current: true},                          // a symlink the PR replaces with a regular file
		"CLAUDE.md":                {},                                       // a link to a file outside any config directory
		"assets/logo.png":          {},
		"app/service.go":           {},
	}, got)
	assert.Equal(t, int32(1), diffs.Load(), "one diff request reads every candidate's mode")

	onlyStaging := []PRFile{byName["orders/schema/staging"]}
	assert.True(t, HasSchemaInputFiles(onlyStaging), "a PR that only repoints an environment symlink changes a schema input")
	assert.Equal(t, []string{"orders/schema/staging"}, DiscoverySchemaPaths(onlyStaging))
	onlyRename := []PRFile{byName["orders/schema/qa"]}
	assert.True(t, HasSchemaInputFiles(onlyRename), "a PR that only renames a symlink away changes a schema input")
	assert.Equal(t, []string{"orders/schema/test"}, DiscoverySchemaPaths(onlyRename), "discovery walks up from the vacated symlink")
	assert.True(t, HasSchemaInputFiles([]PRFile{byName["orders/schema/production"]}), "replacing a symlink with a regular file changes a schema input")
	assert.False(t, HasSchemaInputFiles([]PRFile{byName["orders/schema/NOTES"]}), "a one-line regular file is not a schema input")
	assert.False(t, HasSchemaInputFiles([]PRFile{byName["CLAUDE.md"]}), "a link between two documentation files is not a schema input")
}

// serveCompareDiff serves diff as the compare diff of basehead and fails the
// test on any GraphQL query, so every mode has to come from the diff. It
// returns how many times the diff was read.
func serveCompareDiff(t *testing.T, mux *http.ServeMux, basehead, diff string) *atomic.Int32 {
	t.Helper()
	var reads atomic.Int32
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/"+basehead, func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.Header.Get("Accept"), "diff")
		reads.Add(1)
		_, _ = io.WriteString(w, diff)
	})
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, _ *http.Request) {
		t.Errorf("the diff names every candidate's mode, so no GraphQL query is sent")
		w.WriteHeader(http.StatusInternalServerError)
	})
	return &reads
}

// schemaSymlinkTree serves one commit of a repository for schemaSymlinkKind:
// each path maps to a symlink target, or to "" for a regular file. Parent
// directories are implied by the paths.
func schemaSymlinkTree(t *testing.T, mux *http.ServeMux, paths map[string]string) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/{sha}", func(w http.ResponseWriter, r *http.Request) {
		_, dir := splitSyntheticTreeSHA(r.PathValue("sha"))
		blobs := make(map[string]string, len(paths))
		modes := make(map[string]string)
		for p, target := range paths {
			blobs[p] = "blob~" + strings.ReplaceAll(p, "/", "~")
			if target != "" {
				modes[p] = gitSymlinkMode
			}
		}
		require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{Entries: treeLevel("commit", dir, blobs, modes)}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/blobs/{sha}", func(w http.ResponseWriter, r *http.Request) {
		p := strings.ReplaceAll(strings.TrimPrefix(r.PathValue("sha"), "blob~"), "~", "/")
		target := paths[p]
		require.NotEmpty(t, target, "only a symlink's target is read, not %s", p)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"content": target, "encoding": "utf-8"}))
	})
}

// Environment and namespace symlinks name directories, so a symlink counts
// as a schema input when its target is a directory. A symlink to a file
// counts only directly in a config directory, where planning reads it as an
// environment's schema root and fails. A target the read cannot resolve
// counts too, so an unreadable link is never mistaken for one that changes
// nothing.
func TestSchemaSymlinkKind(t *testing.T) {
	repoPaths := map[string]string{
		"orders/schema/schemabot.yaml":   "",
		"orders/schema/base/orders.sql":  "",
		"orders/schema/staging":          "base",
		"orders/schema/sandbox":          "README.md",
		"orders/schema/README.md":        "",
		"orders/schema/canary":           "staging",
		"orders/schema/missing":          "gone",
		"orders/schema/escape":           "../../../outside",
		"orders/schema/through/x":        "",
		"orders/schema/via":              "staging/orders.sql",
		"orders/schema/loop-a":           "loop-b",
		"orders/schema/loop-b":           "loop-a",
		"CLAUDE.md":                      "AGENTS.md",
		"AGENTS.md":                      "",
		"bin/go":                         ".go-1.26.pkg",
		"bin/.go-1.26.pkg":               "hermit",
		"bin/hermit":                     "",
		"docs/latest":                    "v2",
		"docs/v2/index.md":               "",
		"orders/schema/base/link-to-sql": "orders.sql",
	}
	client, mux := setupRateLimitedTestGitHubServer(t)
	schemaSymlinkTree(t, mux, repoPaths)
	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))

	tests := []struct {
		path string
		want schemaSymlinkKind
	}{
		{path: "orders/schema/staging", want: directorySchemaSymlink},
		{path: "orders/schema/canary", want: directorySchemaSymlink},      // a chain that ends at a directory
		{path: "orders/schema/sandbox", want: environmentSchemaSymlink},   // a file link directly in a config directory
		{path: "orders/schema/missing", want: directorySchemaSymlink},     // a dangling link
		{path: "orders/schema/escape", want: directorySchemaSymlink},      // a link out of the repository
		{path: "orders/schema/via", want: directorySchemaSymlink},         // a target reached through another symlink
		{path: "orders/schema/loop-a", want: directorySchemaSymlink},      // a chain past the hop limit
		{path: "docs/latest", want: directorySchemaSymlink},               // a directory link outside any config directory
		{path: "CLAUDE.md", want: notSchemaSymlink},                       // a file link outside any config directory
		{path: "bin/go", want: notSchemaSymlink},                          // a chain that ends at a file
		{path: "orders/schema/base/link-to-sql", want: notSchemaSymlink},  // a file link inside a namespace
		{path: "orders/schema/README.md", want: notSchemaSymlink},         // a regular file
		{path: "orders/schema/absent", want: notSchemaSymlink},            // no entry at all
		{path: "orders/schema/base/orders.sql/x", want: notSchemaSymlink}, // a path below a file
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got, err := ic.schemaSymlinkKind(t.Context(), "octocat/hello-world", "commit", tt.path, map[string][]TreeEntry{})
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// A symlink holds its target on one line, so only a path with at most one
// line added can be a symlink after the change, and only one with at most one
// line deleted can have been one before it, and an added or removed symlink
// always carries its target line. Every other path skips the tree read.
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
		{name: "an added binary file", file: PRFile{Filename: "assets/logo.png", Status: "added"}},
		{name: "a removed binary file", file: PRFile{Filename: "assets/logo.png", Status: "removed"}},
		{name: "a modified binary file, indistinguishable from a mode change", file: PRFile{Filename: "assets/logo.png", Status: "modified"}, wantHead: true, wantBase: true},
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

// A merged PR's head is on its base branch, so the branch tip's merge base
// with the head is the head itself, where nothing the PR removed exists. The
// listing reads the base side of a merged PR from the base commit GitHub
// recorded for it, so a symlink the PR removed is still found.
func TestFetchPRFilesReadsAMergedPRsRemovedSymlinkFromItsRecordedBase(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"number": 1,
			"state":  "closed",
			"merged": true,
			"head":   map[string]any{"sha": "head-sha", "ref": "feature"},
			"base":   map[string]any{"sha": "recorded-base-sha", "ref": "main"},
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]map[string]any{
			{"filename": "orders/schema/legacy", "status": "removed", "deletions": 1},
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/ref/heads/main", func(w http.ResponseWriter, _ *http.Request) {
		t.Errorf("a merged PR's base side is not read from the base branch tip")
		w.WriteHeader(http.StatusInternalServerError)
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/recorded-base-sha...head-sha", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"merge_base_commit": map[string]any{"sha": "merge-base-sha"}}))
	})
	serveCompareDiff(t, mux, "merge-base-sha...head-sha",
		"diff --git a/orders/schema/legacy b/orders/schema/legacy\ndeleted file mode 120000\nindex 1111111..0000000\n--- a/orders/schema/legacy\n+++ /dev/null\n@@ -1 +0,0 @@\n-base\n")
	schemaSymlinkTree(t, mux, map[string]string{
		"orders/schema/schemabot.yaml":  "",
		"orders/schema/base/orders.sql": "",
		"orders/schema/legacy":          "base",
	})
	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))

	files, err := ic.FetchPRFiles(t.Context(), "octocat/hello-world", 1)

	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.True(t, files[0].Symlink)
	assert.True(t, files[0].VacatedDirectoryLink)
}
