package github

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
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
// schema input and is discovered from the symlink's config directory; a
// symlink with no config file above it, such as CLAUDE.md pointing at
// AGENTS.md, does not, and its target is never read. A directory symlink the
// PR removes, renames away, or replaces with a regular file vacates the
// namespace it declared when its target held a schema file.
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
	targets := map[string]string{"staging-blob": "base", "legacy-blob": "base", "test-blob": "base", "production-blob": "base"}
	mux.HandleFunc("GET /repos/octocat/hello-world/git/blobs/{sha}", func(w http.ResponseWriter, r *http.Request) {
		target, ok := targets[r.PathValue("sha")]
		require.True(t, ok, "only the target of a symlink under a config directory is read, not blob %s", r.PathValue("sha"))
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
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/base-base", tree(entry("orders.sql", "100644")))
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
		"orders/schema/production": {current: true, vacatedDirectory: true},  // a directory symlink the PR replaces with a regular file
		"CLAUDE.md":                {},                                       // a link with no config file above it
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
	assert.Equal(t, []PRFile{{Filename: "orders/schema/production", Status: "removed", Symlink: true, VacatedDirectoryLink: true}},
		vacatedSchemaFiles([]PRFile{byName["orders/schema/production"]}, "orders/schema"), "replacing a directory symlink with a regular file vacates its namespace")
	assert.False(t, HasSchemaInputFiles([]PRFile{byName["orders/schema/NOTES"]}), "a one-line regular file is not a schema input")
	assert.False(t, HasSchemaInputFiles([]PRFile{byName["CLAUDE.md"]}), "a link between two documentation files is not a schema input")
}

// When the server's directory configuration provably covers every config
// location in a repository, a changed path outside those directories cannot
// be a schema symlink, so its mode is never read: it is left out of the diff
// probes, so a path the diff cannot show never sends the listing to GraphQL,
// and a PR whose only candidates sit outside them reads no mode at all. A
// candidate inside them is still read.
func TestFetchPRFilesReadsNoModeOutsideConfiguredSchemaDirs(t *testing.T) {
	tests := []struct {
		name       string
		files      []map[string]any
		readsModes bool
	}{
		{
			name: "a candidate in a schema directory beside one outside",
			files: []map[string]any{
				{"filename": "docs/current", "previous_filename": "docs/latest", "status": "renamed"},
				{"filename": "orders/schema/staging", "status": "modified", "additions": 1, "deletions": 1},
			},
			readsModes: true,
		},
		{
			name: "candidates outside every schema directory",
			files: []map[string]any{
				{"filename": "docs/current", "previous_filename": "docs/latest", "status": "renamed"},
				{"filename": "CLAUDE.md", "status": "added", "additions": 1},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux := setupRateLimitedTestGitHubServer(t)
			mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
				require.NoError(t, json.NewEncoder(w).Encode(tt.files))
			})
			mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
				if !tt.readsModes {
					t.Errorf("no changed path can be a schema symlink, so the PR is not read for its commits")
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
					"number": 1,
					"state":  "open",
					"head":   map[string]any{"sha": "head-sha", "ref": "feature"},
					"base":   map[string]any{"sha": "stale-base-sha", "ref": "main"},
				}))
			})
			registerBaseTipRef(t, mux, "tip-sha")
			mux.HandleFunc("GET /repos/octocat/hello-world/compare/tip-sha...head-sha", func(w http.ResponseWriter, _ *http.Request) {
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"merge_base_commit": map[string]any{"sha": "merge-base-sha"}}))
			})
			// The diff names only the schema directory's path, so a probe
			// for any other path could only be answered by GraphQL.
			diffs := serveCompareDiff(t, mux, "merge-base-sha...head-sha",
				"diff --git a/orders/schema/staging b/orders/schema/staging\nindex 1111111..2222222 120000\n--- a/orders/schema/staging\n+++ b/orders/schema/staging\n@@ -1 +1 @@\n-production\n+base\n")
			schemaSymlinkTree(t, mux, map[string]string{
				"orders/schema/schemabot.yaml":  "",
				"orders/schema/base/orders.sql": "",
				"orders/schema/staging":         "base",
				"docs/current":                  "v2",
				"docs/v2/index.md":              "",
				"CLAUDE.md":                     "AGENTS.md",
				"AGENTS.md":                     "",
			})
			ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
			ic.SetConfigDirHints(widgetsConfigDirHints(t, "orders/schema"))

			files, err := ic.FetchPRFiles(t.Context(), "octocat/hello-world", 1)

			require.NoError(t, err)
			for _, f := range files {
				if f.Filename == "orders/schema/staging" {
					assert.True(t, f.Symlink, "a candidate in a schema directory is read and marked")
					continue
				}
				assert.False(t, f.Symlink || f.PreviousSymlink || f.VacatedDirectoryLink, "%s is outside every schema directory, so it is never a schema symlink", f.Filename)
			}
			var wantDiffs int32
			if tt.readsModes {
				wantDiffs = 1
			}
			assert.Equal(t, wantDiffs, diffs.Load(), "diff reads")
		})
	}
}

// A push to a PR is listed as the comparison of the commit before it with the
// commit after it, and that listing marks symlinks the same way the PR's
// listing does: a repointed environment symlink is a schema input, and a
// removed directory symlink whose target held a schema file vacates its
// namespace. The modes come from the same comparison, read as a diff.
func TestFetchChangedFilesBetweenMarksSymlinks(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	var diffs atomic.Int32
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/base-sha...head-sha", func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.Header.Get("Accept"), "diff") {
			diffs.Add(1)
			_, _ = io.WriteString(w, strings.Join([]string{
				"diff --git a/orders/schema/staging b/orders/schema/staging\nindex 1111111..2222222 120000\n--- a/orders/schema/staging\n+++ b/orders/schema/staging\n@@ -1 +1 @@\n-production\n+base",
				"diff --git a/orders/schema/legacy b/orders/schema/legacy\ndeleted file mode 120000\nindex 1111111..0000000\n--- a/orders/schema/legacy\n+++ /dev/null\n@@ -1 +0,0 @@\n-base",
				"diff --git a/app/service.go b/app/service.go\nindex 1111111..2222222 100644\n--- a/app/service.go\n+++ b/app/service.go\n@@ -1,3 +1,12 @@",
			}, "\n")+"\n")
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(gh.CommitsComparison{
			Status: new("ahead"),
			Files: []*gh.CommitFile{
				{Filename: new("orders/schema/staging"), Status: new("modified"), Additions: new(1), Deletions: new(1)},
				{Filename: new("orders/schema/legacy"), Status: new("removed"), Deletions: new(1)},
				{Filename: new("app/service.go"), Status: new("modified"), Additions: new(12), Deletions: new(3)},
			},
		}))
	})
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, _ *http.Request) {
		t.Errorf("the diff names every candidate's mode, so no GraphQL query is sent")
		w.WriteHeader(http.StatusInternalServerError)
	})
	schemaSymlinkTree(t, mux, map[string]string{
		"orders/schema/schemabot.yaml":  "",
		"orders/schema/base/orders.sql": "",
		"orders/schema/staging":         "base",
		"orders/schema/legacy":          "base",
	})
	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))

	files, err := ic.FetchChangedFilesBetween(t.Context(), "octocat/hello-world", "base-sha", "head-sha")

	require.NoError(t, err)
	type symlinks struct{ current, vacatedDirectory bool }
	got := make(map[string]symlinks)
	for _, f := range files {
		got[f.Filename] = symlinks{f.Symlink, f.VacatedDirectoryLink}
	}
	assert.Equal(t, map[string]symlinks{
		"orders/schema/staging": {current: true},
		"orders/schema/legacy":  {current: true, vacatedDirectory: true},
		"app/service.go":        {},
	}, got)
	assert.Equal(t, int32(1), diffs.Load(), "one diff request reads every candidate's mode")
}

// Removing a directory symlink vacates a namespace only when the directory it
// pointed at held a schema file, because that is what discovery read through
// it. A link to a directory holding none declared no namespace, so its removal
// is a schema input but empties nothing, and planning never proposes dropping
// a namespace the base never declared. A link whose target cannot be resolved
// is treated as having declared one, so an unreadable link is never mistaken
// for one that changes nothing.
func TestFetchPRFilesVacatesOnlyTheNamespacesARemovedLinkDeclared(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"number": 1,
			"state":  "open",
			"head":   map[string]any{"sha": "head-sha", "ref": "feature"},
			"base":   map[string]any{"sha": "stale-base-sha", "ref": "main"},
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]map[string]any{
			{"filename": "orders/schema/legacy", "status": "removed", "deletions": 1},
			{"filename": "orders/schema/scratch", "status": "removed", "deletions": 1},
			{"filename": "orders/schema/ghost", "status": "removed", "deletions": 1},
		}))
	})
	registerBaseTipRef(t, mux, "tip-sha")
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/tip-sha...head-sha", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"merge_base_commit": map[string]any{"sha": "merge-base-sha"}}))
	})
	removed := func(name string) string {
		return "diff --git a/orders/schema/" + name + " b/orders/schema/" + name + "\ndeleted file mode 120000\nindex 1111111..0000000\n--- a/orders/schema/" + name + "\n+++ /dev/null\n@@ -1 +0,0 @@\n-target\n"
	}
	serveCompareDiff(t, mux, "merge-base-sha...head-sha", removed("legacy")+removed("scratch")+removed("ghost"))
	schemaSymlinkTree(t, mux, map[string]string{
		"orders/schema/schemabot.yaml":  "",
		"orders/schema/base/orders.sql": "",
		"orders/schema/empty/README.md": "",
		"orders/schema/legacy":          "base",
		"orders/schema/scratch":         "empty",
		"orders/schema/ghost":           "gone",
	})
	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))

	files, err := ic.FetchPRFiles(t.Context(), "octocat/hello-world", 1)

	require.NoError(t, err)
	type symlinks struct{ current, vacatedDirectory bool }
	got := make(map[string]symlinks)
	for _, f := range files {
		got[f.Filename] = symlinks{f.Symlink, f.VacatedDirectoryLink}
	}
	assert.Equal(t, map[string]symlinks{
		"orders/schema/legacy":  {current: true, vacatedDirectory: true}, // its target held a schema file
		"orders/schema/scratch": {current: true},                         // its target held none
		"orders/schema/ghost":   {current: true, vacatedDirectory: true}, // its target cannot be resolved
	}, got)
	assert.True(t, HasSchemaInputFiles(files), "removing a symlink under a config directory changes a schema input")
	assert.Equal(t, []PRFile{
		{Filename: "orders/schema/legacy", Status: "removed", Symlink: true, VacatedDirectoryLink: true},
		{Filename: "orders/schema/ghost", Status: "removed", Symlink: true, VacatedDirectoryLink: true},
	}, vacatedSchemaFiles(files, "orders/schema"), "only a link whose target held a schema file, or could not be read, empties a namespace")
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
// directories are implied by the paths. It returns the paths whose blobs have
// been read so far, in order.
func schemaSymlinkTree(t *testing.T, mux *http.ServeMux, paths map[string]string) func() []string {
	t.Helper()
	var mu sync.Mutex
	var reads []string
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
		mu.Lock()
		reads = append(reads, p)
		mu.Unlock()
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"content": target, "encoding": "utf-8"}))
	})
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(reads)
	}
}

// Environment and namespace symlinks name directories under a config
// directory, so a symlink counts as a schema input when it sits under one and
// its target is a directory. A symlink to a file counts only directly in a
// config directory, where planning reads it as an environment's schema root
// and fails. A target the read cannot resolve counts too, so an unreadable
// link is never mistaken for one that changes nothing. A symlink with no
// config file above it is not a schema input whatever it points at, and its
// target is never read, so a repository's documentation and tool links cost
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
		"orders/schema/absolute":         "/base/orders.sql",
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
	blobReads := schemaSymlinkTree(t, mux, repoPaths)
	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))

	tests := []struct {
		path       string
		want       schemaSymlinkKind
		wantTarget string
		readsBlob  bool
	}{
		{path: "orders/schema/staging", want: directorySchemaSymlink, wantTarget: "orders/schema/base", readsBlob: true},
		{path: "orders/schema/canary", want: directorySchemaSymlink, wantTarget: "orders/schema/base", readsBlob: true}, // a chain that ends at a directory
		{path: "orders/schema/sandbox", want: environmentSchemaSymlink, readsBlob: true},                                // a file link directly in a config directory
		{path: "orders/schema/missing", want: directorySchemaSymlink, readsBlob: true},                                  // a dangling link
		{path: "orders/schema/escape", want: directorySchemaSymlink, readsBlob: true},                                   // a link out of the repository
		{path: "orders/schema/absolute", want: directorySchemaSymlink, readsBlob: true},                                 // an absolute link is outside the repository, whatever it spells
		{path: "orders/schema/via", want: directorySchemaSymlink, readsBlob: true},                                      // a target reached through another symlink
		{path: "orders/schema/loop-a", want: directorySchemaSymlink, readsBlob: true},                                   // a chain past the hop limit
		{path: "orders/schema/base/link-to-sql", want: notSchemaSymlink, readsBlob: true},                               // a file link inside a namespace
		{path: "docs/latest", want: notSchemaSymlink},                                                                   // a directory link with no config file above it
		{path: "CLAUDE.md", want: notSchemaSymlink},                                                                     // a file link with no config file above it
		{path: "bin/go", want: notSchemaSymlink},                                                                        // a tool's link chain with no config file above it
		{path: "orders/schema/README.md", want: notSchemaSymlink},                                                       // a regular file
		{path: "orders/schema/absent", want: notSchemaSymlink},                                                          // no entry at all
		{path: "orders/schema/base/orders.sql/x", want: notSchemaSymlink},                                               // a path below a file
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			before := len(blobReads())

			got, targetDir, err := ic.schemaSymlinkKind(t.Context(), "octocat/hello-world", "commit", tt.path, map[string][]TreeEntry{})

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantTarget, targetDir)
			read := blobReads()[before:]
			if tt.readsBlob {
				assert.NotEmpty(t, read, "the symlink's target is read to classify it")
			} else {
				assert.Empty(t, read, "no blob is read for a path that is not a schema symlink by position")
			}
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
		{name: "a two-line addition, one too many for a symlink", file: PRFile{Filename: "schema/staging", Status: "modified", Additions: 2, Deletions: 1}, wantBase: true},
		{name: "a two-line deletion, one too many for a symlink", file: PRFile{Filename: "schema/staging", Status: "modified", Additions: 1, Deletions: 2}, wantHead: true},
		{name: "a renamed symlink", file: PRFile{Filename: "schema/qa", PreviousFilename: "schema/test", Status: "renamed"}, wantHead: true, wantPreviousAtBase: true},
		{name: "a renamed and repointed symlink", file: PRFile{Filename: "schema/qa", PreviousFilename: "schema/test", Status: "renamed", Additions: 1, Deletions: 1}, wantHead: true, wantPreviousAtBase: true},
		{name: "a renamed file with two lines deleted", file: PRFile{Filename: "schema/qa", PreviousFilename: "schema/test", Status: "renamed", Additions: 1, Deletions: 2}, wantHead: true},
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
