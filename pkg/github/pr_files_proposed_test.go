package github

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// proposedFilesRepo lays out a repository at two commits for the narrowing
// comparison: a head and the default branch tip. Each commit is described as a
// flat map of path to blob SHA; a path absent from the map does not exist at
// that commit.
type proposedFilesRepo struct {
	head       map[string]string
	defaultTip map[string]string
}

// serve registers the default branch lookup, the ref resolution, and the tree
// reads the comparison walks, returning a counter of tree reads so a test can
// assert none happened. Trees are served one level at a time, which is how the
// comparison reads them.
func (r proposedFilesRepo) serve(t *testing.T, mux *http.ServeMux) *atomic.Int64 {
	t.Helper()

	mux.HandleFunc("GET /repos/octocat/hello-world", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.Repository{DefaultBranch: new("main")}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/ref/heads/main", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.Reference{
			Ref:    new("refs/heads/main"),
			Object: &gh.GitObject{Type: new("commit"), SHA: new("default-tip-sha")},
		}))
	})

	var treeReads atomic.Int64
	commits := map[string]map[string]string{
		"head-sha":        r.head,
		"default-tip-sha": r.defaultTip,
	}
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/{sha}", func(w http.ResponseWriter, req *http.Request) {
		treeReads.Add(1)
		commit, dir := splitSyntheticTreeSHA(req.PathValue("sha"))
		paths, ok := commits[commit]
		require.True(t, ok, "tree read for unknown commit %q", commit)
		require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{Entries: treeLevel(commit, dir, paths)}))
	})
	return &treeReads
}

// splitSyntheticTreeSHA decodes the tree identifiers treeLevel mints. A bare
// commit name is that commit's root tree. Directory separators are encoded so a
// tree identifier stays a single URL path segment, as a real tree SHA is.
func splitSyntheticTreeSHA(treeSHA string) (commit, dir string) {
	commit, encodedDir, found := strings.Cut(treeSHA, "~")
	if !found {
		return treeSHA, ""
	}
	return commit, strings.ReplaceAll(encodedDir, "~", "/")
}

// treeLevel returns the entries directly under dir at commit, given the flat
// path-to-blob map describing that commit. Subtree SHAs are synthesized from
// the commit and directory so each commit's directories resolve independently,
// which is what the comparison assumes when it caches by tree SHA.
func treeLevel(commit, dir string, paths map[string]string) []*gh.TreeEntry {
	prefix := ""
	if dir != "" {
		prefix = dir + "/"
	}
	var entries []*gh.TreeEntry
	seen := map[string]bool{}
	for full, blobSHA := range paths {
		rest, under := strings.CutPrefix(full, prefix)
		if !under || rest == "" {
			continue
		}
		segment, deeper, isDir := strings.Cut(rest, "/")
		if seen[segment] {
			continue
		}
		seen[segment] = true
		if isDir && deeper != "" {
			entries = append(entries, &gh.TreeEntry{
				Path: new(segment),
				Type: new("tree"),
				SHA:  new(commit + "~" + strings.ReplaceAll(prefix+segment, "/", "~")),
			})
			continue
		}
		entries = append(entries, &gh.TreeEntry{Path: new(segment), Type: new("blob"), SHA: new(blobSHA)})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].GetPath() < entries[j].GetPath() })
	return entries
}

func newProposedFilesClient(t *testing.T, repo proposedFilesRepo) (*InstallationClient, *atomic.Int64) {
	t.Helper()
	client, mux := setupRateLimitedTestGitHubServer(t)
	treeReads := repo.serve(t, mux)
	return NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil))), treeReads
}

func filenames(files []PRFile) []string {
	names := make([]string, len(files))
	for i, f := range files {
		names[i] = f.Filename
	}
	return names
}

// TestPRFilesProposedAgainstDefaultBranchDropsInheritedFiles covers the pull
// request whose changed-file list is wider than the change under review: a
// branch whose base lags, or briefly points elsewhere, carries files from
// history it has not caught up with. One inherited schema file is enough to
// make a pull request look like another database's, so a file whose content
// already matches the default branch is not something this pull request
// proposes.
func TestPRFilesProposedAgainstDefaultBranchDropsInheritedFiles(t *testing.T) {
	repo := proposedFilesRepo{
		head: map[string]string{
			"schema/orders/orders.sql":    "orders-blob",
			"schema/billing/invoices.sql": "shared-blob",
			"docs/readme.md":              "docs-blob",
		},
		defaultTip: map[string]string{
			"schema/billing/invoices.sql": "shared-blob",
		},
	}
	ic, _ := newProposedFilesClient(t, repo)

	proposed, defaultTipSHA, err := ic.PRFilesProposedAgainstDefaultBranch(t.Context(), "octocat/hello-world", "head-sha", []PRFile{
		{Filename: "schema/orders/orders.sql", Status: "added"},
		{Filename: "schema/billing/invoices.sql", Status: "modified"},
		{Filename: "docs/readme.md", Status: "modified"},
	})

	require.NoError(t, err)
	assert.Equal(t, "default-tip-sha", defaultTipSHA)
	assert.Equal(t, []string{"schema/orders/orders.sql", "docs/readme.md"}, filenames(proposed),
		"the inherited schema file is dropped; the proposed one and the non-schema file stay")
}

// TestPRFilesProposedAgainstDefaultBranchKeepsDeletionsAndConfigs verifies the
// two inputs discovery reads besides an edited schema file: a schema file this
// pull request deletes, and a schemabot.yaml it adds. Both differ from the
// default branch, so both are proposed.
func TestPRFilesProposedAgainstDefaultBranchKeepsDeletionsAndConfigs(t *testing.T) {
	repo := proposedFilesRepo{
		head: map[string]string{
			"schema/orders/schemabot.yaml": "config-blob",
		},
		defaultTip: map[string]string{
			"schema/orders/dropped.sql": "dropped-blob",
		},
	}
	ic, _ := newProposedFilesClient(t, repo)

	proposed, _, err := ic.PRFilesProposedAgainstDefaultBranch(t.Context(), "octocat/hello-world", "head-sha", []PRFile{
		{Filename: "schema/orders/dropped.sql", Status: "removed"},
		{Filename: "schema/orders/schemabot.yaml", Status: "added"},
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"schema/orders/dropped.sql", "schema/orders/schemabot.yaml"}, filenames(proposed))
}

// TestPRFilesProposedAgainstDefaultBranchSkipsPullRequestsWithoutSchema
// verifies that the narrowing costs nothing on the pull requests that never
// resolve a database — most of them, in a repository of any size. With no file
// discovery would read, there is nothing to compare and no reason to call
// GitHub at all.
func TestPRFilesProposedAgainstDefaultBranchSkipsPullRequestsWithoutSchema(t *testing.T) {
	ic, treeReads := newProposedFilesClient(t, proposedFilesRepo{})

	files := []PRFile{
		{Filename: "cmd/server/main.go", Status: "modified"},
		{Filename: "docs/readme.md", Status: "added"},
	}
	proposed, defaultTipSHA, err := ic.PRFilesProposedAgainstDefaultBranch(t.Context(), "octocat/hello-world", "head-sha", files)

	require.NoError(t, err)
	assert.Equal(t, files, proposed)
	assert.Empty(t, defaultTipSHA)
	assert.Zero(t, treeReads.Load(), "a pull request with no schema files reads no trees")
}

// TestPRFilesProposedAgainstDefaultBranchFailsWhenDefaultBranchUnresolved
// verifies that an unanswerable comparison is an error rather than a silent
// fallback to the wider list. Without the default branch there is no way to
// tell an inherited file from a proposed one, and guessing wrong is what plans
// a database the pull request never touched.
func TestPRFilesProposedAgainstDefaultBranchFailsWhenDefaultBranchUnresolved(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.Repository{}))
	})
	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))

	_, _, err := ic.PRFilesProposedAgainstDefaultBranch(t.Context(), "octocat/hello-world", "head-sha", []PRFile{
		{Filename: "schema/orders/orders.sql", Status: "added"},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no default branch")
}
