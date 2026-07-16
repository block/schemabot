package github

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// baseDriftFixture models the GitHub API responses SchemaDirChangedOnBase needs:
// merge-base resolution (compare baseRef...head), per-commit root trees (so a
// path can be resolved to a tree SHA), and the compare file list used for the
// operator-facing changed-file display.
type baseDriftFixture struct {
	// mergeBase is returned as merge_base_commit.sha for compare baseRef...head.
	mergeBase string
	// baseTip is returned as base_commit.sha for compare baseRef...head — the
	// live tip of the base ref, which the gate uses as the base SHA. When empty
	// it falls back to the compare's base path segment (the ref name).
	baseTip string
	// omitBaseCommit suppresses base_commit in the compare response so tests can
	// exercise the "no base commit" fail-closed path.
	omitBaseCommit bool
	// rootTrees maps a commit SHA to the entries of its root tree.
	rootTrees map[string][]driftEntry
	// compareFiles maps a "base...head" key to the files that compare returns.
	compareFiles map[string][]*gh.CommitFile
}

// driftEntry is a directory entry in a fixture root tree.
type driftEntry struct {
	path string
	sha  string
}

func newBaseDriftClient(t *testing.T, fx baseDriftFixture) *InstallationClient {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("GET /repos/octo/repo/compare/{basehead}", func(w http.ResponseWriter, r *http.Request) {
		basehead := r.PathValue("basehead")
		parts := strings.SplitN(basehead, "...", 2)
		require.Len(t, parts, 2, "compare basehead must be base...head")
		base, head := parts[0], parts[1]

		cmp := &gh.CommitsComparison{}
		// The merge-base query is always baseRef...head where head is the PR
		// head. For any compare we serve the recorded merge base, the live base
		// tip, and any recorded files.
		if base == "" || head == "" {
			http.Error(w, "bad compare", http.StatusBadRequest)
			return
		}
		if fx.mergeBase != "" {
			cmp.MergeBaseCommit = &gh.RepositoryCommit{SHA: new(fx.mergeBase)}
		}
		// base_commit.sha is the resolved live tip of the base ref. Default to
		// the base path segment so callers that pass a concrete SHA see it
		// echoed back; tests that model a moving base set baseTip explicitly.
		if !fx.omitBaseCommit {
			baseTip := fx.baseTip
			if baseTip == "" {
				baseTip = base
			}
			cmp.BaseCommit = &gh.RepositoryCommit{SHA: new(baseTip)}
		}
		if files, ok := fx.compareFiles[basehead]; ok {
			cmp.Files = files
		}
		writeJSON(t, w, cmp)
	})

	mux.HandleFunc("GET /repos/octo/repo/git/trees/{sha}", func(w http.ResponseWriter, r *http.Request) {
		sha := r.PathValue("sha")
		entries, ok := fx.rootTrees[sha]
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		tree := &gh.Tree{SHA: new(sha), Truncated: new(false)}
		for i := range entries {
			tree.Entries = append(tree.Entries, &gh.TreeEntry{
				Path: new(entries[i].path),
				Mode: new("040000"),
				Type: new("tree"),
				SHA:  new(entries[i].sha),
			})
		}
		writeJSON(t, w, tree)
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	ghc := gh.NewClient(nil)
	ghc.BaseURL, _ = url.Parse(server.URL + "/")
	return &InstallationClient{
		client: ghc,
		logger: slog.New(slog.NewTextHandler(prDiscardWriter{}, &slog.HandlerOptions{Level: slog.LevelError})),
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(v))
}

func treeEntry(pathName, sha string) driftEntry {
	return driftEntry{path: pathName, sha: sha}
}

func TestSchemaDirChangedOnBase(t *testing.T) {
	const (
		headSHA = "headsha"
		baseSHA = "basesha"
	)

	t.Run("merge base equals base means base has not advanced", func(t *testing.T) {
		ic := newBaseDriftClient(t, baseDriftFixture{mergeBase: baseSHA})
		changed, files, err := ic.SchemaDirChangedOnBase(t.Context(), "octo/repo", baseSHA, headSHA, "schemas")
		require.NoError(t, err)
		assert.False(t, changed)
		assert.Empty(t, files)
	})

	t.Run("base advanced but schema dir unchanged", func(t *testing.T) {
		ic := newBaseDriftClient(t, baseDriftFixture{
			mergeBase: "mergebase",
			rootTrees: map[string][]driftEntry{
				"mergebase": {treeEntry("schemas", "schematree"), treeEntry("docs", "docstree_old")},
				baseSHA:     {treeEntry("schemas", "schematree"), treeEntry("docs", "docstree_new")},
			},
		})
		changed, files, err := ic.SchemaDirChangedOnBase(t.Context(), "octo/repo", baseSHA, headSHA, "schemas")
		require.NoError(t, err)
		assert.False(t, changed, "only docs changed on base, not the schema dir")
		assert.Empty(t, files)
	})

	t.Run("resolves the live base tip so a stale snapshot does not mask drift", func(t *testing.T) {
		// GitHub's pull.base.sha snapshot for a fast-moving base is typically
		// the base tip at PR creation, which equals the merge base — so passing
		// it would short-circuit as "no drift". The gate passes the base ref and
		// resolves the live tip from base_commit, catching drift the snapshot
		// would hide. Here the merge base still has the old schema tree while the
		// live base tip has a new one.
		ic := newBaseDriftClient(t, baseDriftFixture{
			mergeBase: "mergebase",
			baseTip:   "livebasetip",
			rootTrees: map[string][]driftEntry{
				"mergebase":   {treeEntry("schemas", "schematree_old")},
				"livebasetip": {treeEntry("schemas", "schematree_new")},
			},
			compareFiles: map[string][]*gh.CommitFile{
				"mergebase...livebasetip": {
					{Filename: new("schemas/orders.sql"), Status: new("modified")},
				},
			},
		})
		changed, files, err := ic.SchemaDirChangedOnBase(t.Context(), "octo/repo", "main", headSHA, "schemas")
		require.NoError(t, err)
		assert.True(t, changed, "drift on the live base tip must be detected")
		assert.Equal(t, []string{"schemas/orders.sql"}, files)
	})

	t.Run("missing base commit is an error so callers fail closed", func(t *testing.T) {
		// A compare response with a merge base but no base_commit leaves the live
		// base tip unknown; the gate must fail closed rather than guess.
		ic := newBaseDriftClient(t, baseDriftFixture{mergeBase: "mergebase", omitBaseCommit: true})
		_, _, err := ic.SchemaDirChangedOnBase(t.Context(), "octo/repo", "main", headSHA, "schemas")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no base commit")
	})

	t.Run("schema dir changed on base lists changed files", func(t *testing.T) {
		ic := newBaseDriftClient(t, baseDriftFixture{
			mergeBase: "mergebase",
			rootTrees: map[string][]driftEntry{
				"mergebase": {treeEntry("schemas", "schematree_old")},
				baseSHA:     {treeEntry("schemas", "schematree_new")},
			},
			compareFiles: map[string][]*gh.CommitFile{
				"mergebase..." + baseSHA: {
					{Filename: new("schemas/orders.sql"), Status: new("modified")},
					{Filename: new("schemas/users.sql"), Status: new("added")},
					{Filename: new("docs/readme.md"), Status: new("modified")},
				},
			},
		})
		changed, files, err := ic.SchemaDirChangedOnBase(t.Context(), "octo/repo", baseSHA, headSHA, "schemas")
		require.NoError(t, err)
		assert.True(t, changed)
		assert.Equal(t, []string{"schemas/orders.sql", "schemas/users.sql"}, files,
			"only files under the schema dir are listed, sorted")
	})

	t.Run("schema dir added on base", func(t *testing.T) {
		ic := newBaseDriftClient(t, baseDriftFixture{
			mergeBase: "mergebase",
			rootTrees: map[string][]driftEntry{
				"mergebase": {treeEntry("docs", "docstree")},
				baseSHA:     {treeEntry("schemas", "schematree"), treeEntry("docs", "docstree")},
			},
		})
		changed, _, err := ic.SchemaDirChangedOnBase(t.Context(), "octo/repo", baseSHA, headSHA, "schemas")
		require.NoError(t, err)
		assert.True(t, changed)
	})

	t.Run("schema dir removed on base", func(t *testing.T) {
		ic := newBaseDriftClient(t, baseDriftFixture{
			mergeBase: "mergebase",
			rootTrees: map[string][]driftEntry{
				"mergebase": {treeEntry("schemas", "schematree")},
				baseSHA:     {treeEntry("docs", "docstree")},
			},
		})
		changed, _, err := ic.SchemaDirChangedOnBase(t.Context(), "octo/repo", baseSHA, headSHA, "schemas")
		require.NoError(t, err)
		assert.True(t, changed)
	})

	t.Run("schema dir absent on both base refs is a PR-introduced dir", func(t *testing.T) {
		ic := newBaseDriftClient(t, baseDriftFixture{
			mergeBase: "mergebase",
			rootTrees: map[string][]driftEntry{
				"mergebase": {treeEntry("docs", "docstree")},
				baseSHA:     {treeEntry("docs", "docstree")},
			},
		})
		changed, _, err := ic.SchemaDirChangedOnBase(t.Context(), "octo/repo", baseSHA, headSHA, "schemas")
		require.NoError(t, err)
		assert.False(t, changed, "base never had the schema dir, so base did not change it")
	})

	t.Run("missing merge base is an error so callers fail closed", func(t *testing.T) {
		ic := newBaseDriftClient(t, baseDriftFixture{mergeBase: ""})
		_, _, err := ic.SchemaDirChangedOnBase(t.Context(), "octo/repo", baseSHA, headSHA, "schemas")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "merge base")
	})

	t.Run("unavailable changed-file list still reports changed with no files", func(t *testing.T) {
		ic := newBaseDriftClient(t, baseDriftFixture{
			mergeBase: "mergebase",
			rootTrees: map[string][]driftEntry{
				"mergebase": {treeEntry("schemas", "schematree_old")},
				baseSHA:     {treeEntry("schemas", "schematree_new")},
			},
			// No compareFiles entry for mergebase...basesha, so the file list
			// compare returns nothing; the change decision still holds.
		})
		changed, files, err := ic.SchemaDirChangedOnBase(t.Context(), "octo/repo", baseSHA, headSHA, "schemas")
		require.NoError(t, err)
		assert.True(t, changed)
		assert.Empty(t, files)
	})
}

func TestPathWithinDir(t *testing.T) {
	assert.True(t, pathWithinDir("schema", "schema/foo.sql"))
	assert.True(t, pathWithinDir("schema", "schema"))
	assert.False(t, pathWithinDir("schema", "schemafoo/bar.sql"), "must match on segment boundary")
	assert.False(t, pathWithinDir("schema", "other/foo.sql"))
	assert.True(t, pathWithinDir("db/staging", "db/staging/orders.sql"))
	assert.False(t, pathWithinDir("db/staging", "db/prod/orders.sql"))
	assert.True(t, pathWithinDir(".", "anything/at/root.sql"), "root dir contains every path")
	assert.True(t, pathWithinDir("", "anything.sql"))
}
