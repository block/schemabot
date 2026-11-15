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
// that commit. modes gives a path's file mode at a commit; any other path is
// a regular file.
type proposedFilesRepo struct {
	head       map[string]string
	defaultTip map[string]string
	modes      map[string]map[string]string
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
		require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{Entries: treeLevel(commit, dir, paths, r.modes[commit])}))
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
func treeLevel(commit, dir string, paths map[string]string, modes map[string]string) []*gh.TreeEntry {
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
		entries = append(entries, &gh.TreeEntry{Path: new(segment), Mode: new(fileMode(modes, full)), Type: new("blob"), SHA: new(blobSHA)})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].GetPath() < entries[j].GetPath() })
	return entries
}

// fileMode returns the mode modes gives filePath, or a regular file's mode.
func fileMode(modes map[string]string, filePath string) string {
	if mode, ok := modes[filePath]; ok {
		return mode
	}
	return "100644"
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

// TestPRFilesProposedAgainstDefaultBranchKeepsRenamesThatVacateSchema covers a
// rename that moves a schema file to a name discovery does not read. The
// rename the pull request proposes — the head lacks the old path and the
// default branch still declares it — is kept so discovery resolves the config
// that managed it. A rename the default branch already holds is inherited
// history and is dropped.
func TestPRFilesProposedAgainstDefaultBranchKeepsRenamesThatVacateSchema(t *testing.T) {
	repo := proposedFilesRepo{
		head: map[string]string{
			"schema/orders/users.sql.disabled": "users-blob",
			"schema/billing/invoices.md":       "invoices-blob",
		},
		defaultTip: map[string]string{
			"schema/orders/users.sql":    "users-blob",
			"schema/billing/invoices.md": "invoices-blob",
		},
	}
	ic, _ := newProposedFilesClient(t, repo)

	proposed, _, err := ic.PRFilesProposedAgainstDefaultBranch(t.Context(), "octocat/hello-world", "head-sha", []PRFile{
		{Filename: "schema/orders/users.sql.disabled", PreviousFilename: "schema/orders/users.sql", Status: "renamed"},
		{Filename: "schema/billing/invoices.md", PreviousFilename: "schema/billing/invoices.sql", Status: "renamed"},
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"schema/orders/users.sql.disabled"}, filenames(proposed))
}

// TestPRFilesProposedAgainstDefaultBranchKeepsConfigRenamesThatVacateAHeldConfig
// covers a pull request that moves a participant's config out of its directory
// while the default branch already holds an identical copy at the destination.
// When the default branch still declares the old config, merging removes it,
// so the rename is kept with its previous path and the deployment that managed
// it is still selected. When the default branch already absorbed the rename,
// the move is inherited history and the file is narrowed away.
func TestPRFilesProposedAgainstDefaultBranchKeepsConfigRenamesThatVacateAHeldConfig(t *testing.T) {
	renamed := PRFile{Filename: "archive/schemabot.yaml", PreviousFilename: "tenant-b/schema/schemabot.yaml", Status: "renamed"}
	tests := []struct {
		name         string
		defaultTip   map[string]string
		wantProposed []PRFile
	}{
		{
			name: "default branch still declares the old config",
			defaultTip: map[string]string{
				"archive/schemabot.yaml":         "config-blob",
				"tenant-b/schema/schemabot.yaml": "config-blob",
			},
			wantProposed: []PRFile{renamed},
		},
		{
			name: "default branch already absorbed the rename",
			defaultTip: map[string]string{
				"archive/schemabot.yaml": "config-blob",
			},
			wantProposed: []PRFile{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic, _ := newProposedFilesClient(t, proposedFilesRepo{
				head:       map[string]string{"archive/schemabot.yaml": "config-blob"},
				defaultTip: tt.defaultTip,
			})

			proposed, _, err := ic.PRFilesProposedAgainstDefaultBranch(t.Context(), "octocat/hello-world", "head-sha", []PRFile{renamed})

			require.NoError(t, err)
			assert.Equal(t, tt.wantProposed, proposed)
		})
	}
}

// TestPRFilesProposedAgainstDefaultBranchScopesCrossConfigRenames covers a
// rename that moves a schema file from one config directory to another, run
// through narrowing and then config discovery. When the pull request proposes
// the rename — the default branch still declares the old path — merging it
// stops declaring the table under billing, so both configs are planned. When
// the default branch already holds the rename and the pull request only edits
// the destination, the vacated path is inherited history: only orders is
// planned.
func TestPRFilesProposedAgainstDefaultBranchScopesCrossConfigRenames(t *testing.T) {
	renamed := PRFile{Filename: "schema/orders/users.sql", PreviousFilename: "schema/billing/users.sql", Status: "renamed"}
	tests := []struct {
		name          string
		repo          proposedFilesRepo
		wantPrevious  string
		wantDatabases []string
	}{
		{
			name: "proposed rename plans both configs",
			repo: proposedFilesRepo{
				head:       map[string]string{"schema/orders/users.sql": "users-blob"},
				defaultTip: map[string]string{"schema/billing/users.sql": "users-blob"},
			},
			wantPrevious:  "schema/billing/users.sql",
			wantDatabases: []string{"billing", "orders"},
		},
		{
			name: "inherited rename with an edited destination plans only orders",
			repo: proposedFilesRepo{
				head:       map[string]string{"schema/orders/users.sql": "users-edited-blob"},
				defaultTip: map[string]string{"schema/orders/users.sql": "users-blob"},
			},
			wantPrevious:  "",
			wantDatabases: []string{"orders"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux := setupRateLimitedTestGitHubServer(t)
			tt.repo.serve(t, mux)
			registerFileContent(t, mux, "/repos/octocat/hello-world/contents/schema/orders/schemabot.yaml", "database: orders\ntype: mysql\n")
			registerFileContent(t, mux, "/repos/octocat/hello-world/contents/schema/billing/schemabot.yaml", "database: billing\ntype: mysql\n")
			ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))

			proposed, _, err := ic.PRFilesProposedAgainstDefaultBranch(t.Context(), "octocat/hello-world", "head-sha", []PRFile{renamed})
			require.NoError(t, err)
			require.Len(t, proposed, 1)
			assert.Equal(t, "schema/orders/users.sql", proposed[0].Filename)
			assert.Equal(t, tt.wantPrevious, proposed[0].PreviousFilename)

			configs, err := ic.FindConfigsForPRFiles(t.Context(), "octocat/hello-world", "head-sha", proposed)
			require.NoError(t, err)
			databases := make([]string, len(configs))
			for i, dc := range configs {
				databases[i] = dc.Config.Database
			}
			sort.Strings(databases)
			assert.Equal(t, tt.wantDatabases, databases)
		})
	}
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

// A symlink and a regular file holding the same bytes share a blob SHA, so a
// pull request that turns an environment's regular file into a symlink with
// the same bytes changes only the file mode. That still changes what the
// environment plans, so the path is proposed. Making a schema file executable
// changes nothing a plan reads, so that file is inherited like an unchanged
// one.
func TestPRFilesProposedAgainstDefaultBranchKeepsSymlinkModeChanges(t *testing.T) {
	repo := proposedFilesRepo{
		head:       map[string]string{"schema/staging": "target-blob", "schema/production": "target-blob", "schema/users.sql": "users-blob"},
		defaultTip: map[string]string{"schema/staging": "target-blob", "schema/production": "target-blob", "schema/users.sql": "users-blob"},
		modes: map[string]map[string]string{
			"head-sha":        {"schema/staging": gitSymlinkMode, "schema/production": gitSymlinkMode, "schema/users.sql": "100755"},
			"default-tip-sha": {"schema/production": gitSymlinkMode},
		},
	}
	ic, _ := newProposedFilesClient(t, repo)

	proposed, _, err := ic.PRFilesProposedAgainstDefaultBranch(t.Context(), "octocat/hello-world", "head-sha", []PRFile{
		{Filename: "schema/staging", Status: "changed", Symlink: true},
		{Filename: "schema/production", Status: "modified", Additions: 1, Deletions: 1, Symlink: true},
		{Filename: "schema/users.sql", Status: "changed"},
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"schema/staging"}, filenames(proposed),
		"the file turned into a symlink is proposed; the symlink the default branch already holds and the executable schema file are inherited")
}
