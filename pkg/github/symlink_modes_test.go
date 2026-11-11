package github

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDiffModes(t *testing.T) {
	diff := strings.Join([]string{
		// A symlink added.
		"diff --git a/orders/schema/staging b/orders/schema/staging",
		"new file mode 120000",
		"index 0000000..1111111",
		"--- /dev/null",
		"+++ b/orders/schema/staging",
		"@@ -0,0 +1 @@",
		"+base",
		`\ No newline at end of file`,
		// A symlink repointed: the mode is on the index line.
		"diff --git a/orders/schema/production b/orders/schema/production",
		"index 2222222..3333333 120000",
		"--- a/orders/schema/production",
		"+++ b/orders/schema/production",
		"@@ -1 +1 @@",
		"-base",
		"+v2",
		// A regular file edited by one line.
		"diff --git a/svc/a/b/c/Main.java b/svc/a/b/c/Main.java",
		"index 4444444..5555555 100644",
		"--- a/svc/a/b/c/Main.java",
		"+++ b/svc/a/b/c/Main.java",
		"@@ -1 +1 @@",
		"-old",
		"+new",
		// A symlink removed.
		"diff --git a/orders/schema/sandbox b/orders/schema/sandbox",
		"deleted file mode 120000",
		"index 6666666..0000000",
		"--- a/orders/schema/sandbox",
		"+++ /dev/null",
		// A regular file turned into a symlink: git writes a type change as a
		// deletion block followed by a creation block for the same path.
		"diff --git a/docs/CLAUDE.md b/docs/CLAUDE.md",
		"deleted file mode 100644",
		"index aaaaaaa..0000000",
		"--- a/docs/CLAUDE.md",
		"+++ /dev/null",
		"@@ -1 +0,0 @@",
		"-guidance",
		"diff --git a/docs/CLAUDE.md b/docs/CLAUDE.md",
		"new file mode 120000",
		"index 0000000..bbbbbbb",
		"--- /dev/null",
		"+++ b/docs/CLAUDE.md",
		"@@ -0,0 +1 @@",
		"+AGENTS.md",
		`\ No newline at end of file`,
		// A pure rename: no mode anywhere in the headers.
		"diff --git a/old/link b/new/link",
		"similarity index 100%",
		"rename from old/link",
		"rename to new/link",
		// A quoted path with a space.
		`diff --git "a/dir with space/link" "b/dir with space/link"`,
		"new file mode 120000",
		"index 0000000..7777777",
		// An unquoted path with a space.
		"diff --git a/my dir/file.txt b/my dir/file.txt",
		"index 8888888..9999999 100644",
		"--- a/my dir/file.txt",
		"+++ b/my dir/file.txt",
	}, "\n")

	modes := parseDiffModes(diff)

	assert.Equal(t, map[string]bool{
		"orders/schema/staging":    false,
		"orders/schema/production": true,
		"svc/a/b/c/Main.java":      false,
		"orders/schema/sandbox":    true,
		"docs/CLAUDE.md":           false,
		"dir with space/link":      false,
		"my dir/file.txt":          false,
	}, modes.base)
	assert.Equal(t, map[string]bool{
		"orders/schema/staging":    true,
		"orders/schema/production": true,
		"svc/a/b/c/Main.java":      false,
		"orders/schema/sandbox":    false,
		"docs/CLAUDE.md":           true,
		"dir with space/link":      true,
		"my dir/file.txt":          false,
	}, modes.head)
	assert.NotContains(t, modes.base, "old/link", "a pure rename has no mode in its headers")
	assert.NotContains(t, modes.head, "new/link", "a pure rename has no mode in its headers")
}

// A symlink replaced by a regular file arrives as a deletion block followed by
// a creation block for the same path; the creation block's absent base must
// not erase the symlink mode the deletion block stated.
func TestParseDiffModesKeepsBothSidesOfATypeChange(t *testing.T) {
	diff := strings.Join([]string{
		"diff --git a/docs/was_symlink b/docs/was_symlink",
		"deleted file mode 120000",
		"index 237406b..0000000",
		"--- a/docs/was_symlink",
		"+++ /dev/null",
		"@@ -1 +0,0 @@",
		"-reg",
		`\ No newline at end of file`,
		"diff --git a/docs/was_symlink b/docs/was_symlink",
		"new file mode 100644",
		"index 0000000..b9a1f7f",
		"--- /dev/null",
		"+++ b/docs/was_symlink",
		"@@ -0,0 +1 @@",
		"+now regular",
	}, "\n")

	modes := parseDiffModes(diff)

	assert.True(t, modes.base["docs/was_symlink"], "the path was a symlink before the change")
	assert.False(t, modes.head["docs/was_symlink"], "the path is a regular file after the change")
}

// A rename or copy whose content also changed carries a mode on its index
// line, and its two paths come from the rename/copy headers because the
// "diff --git" line cannot be split when the paths differ.
func TestParseDiffModesReadsRenameAndCopyPathsFromTheirHeaders(t *testing.T) {
	diff := strings.Join([]string{
		"diff --git a/orders/schema/old env b/orders/schema/new env",
		"similarity index 80%",
		"rename from orders/schema/old env",
		"rename to orders/schema/new env",
		"index 1111111..2222222 120000",
		"--- a/orders/schema/old env",
		"+++ b/orders/schema/new env",
		"@@ -1 +1 @@",
		"-base",
		"+v2",
		"diff --git a/docs/src.md b/docs/copy.md",
		"similarity index 90%",
		"copy from docs/src.md",
		"copy to docs/copy.md",
		"index 3333333..4444444 100644",
		"--- a/docs/src.md",
		"+++ b/docs/copy.md",
		"@@ -1 +1 @@",
		"-x",
		"+y",
	}, "\n")

	modes := parseDiffModes(diff)

	assert.Equal(t, map[string]bool{"orders/schema/old env": true, "docs/src.md": false}, modes.base)
	assert.Equal(t, map[string]bool{"orders/schema/new env": true, "docs/copy.md": false}, modes.head)
}

// A file whose content is one very long line, such as a minified bundle, must
// not cut the files after it off from their modes.
func TestParseDiffModesReadsTheFilesAfterAnOverlongLine(t *testing.T) {
	var diff strings.Builder
	diff.WriteString("diff --git a/web/bundle.min.js b/web/bundle.min.js\nindex 1111111..2222222 100644\n--- a/web/bundle.min.js\n+++ b/web/bundle.min.js\n@@ -1 +1 @@\n-x\n+")
	diff.WriteString(strings.Repeat("a", 17<<20))
	diff.WriteString("\ndiff --git a/schema/db/staging b/schema/db/staging\nindex 3333333..4444444 120000\n")

	modes := parseDiffModes(diff.String())

	assert.Equal(t, map[string]bool{"web/bundle.min.js": false, "schema/db/staging": true}, modes.base)
	assert.Equal(t, map[string]bool{"web/bundle.min.js": false, "schema/db/staging": true}, modes.head)
}

// symlinkModesServer fakes the GitHub endpoints SymlinkModes uses and counts
// every request, failing the test on any other endpoint, such as a
// level-by-level tree read. A ref is resolvable when at least one tree is
// registered under it.
type symlinkModesServer struct {
	ic    *InstallationClient
	diff  func(w http.ResponseWriter)
	trees map[string][]graphQLTreeEntry // "<ref>:<dir>" → entries; absent → null
	// graphQL, when set, answers a GraphQL query itself and returns true, or
	// returns false to let the fake answer it from trees.
	graphQL func(w http.ResponseWriter, ref string, dirs []string) bool
	// recursiveTree, when set, answers a recursive tree read itself and
	// returns true, or returns false to let the fake list every entry of
	// trees registered under the ref.
	recursiveTree func(w http.ResponseWriter, ref string) bool
	mu            sync.Mutex
	compares      int
	queries       [][]string // the directories of each GraphQL query, in order
	treeReads     []string   // the ref of each recursive tree read, in order
}

func newSymlinkModesServer(t *testing.T) *symlinkModesServer {
	t.Helper()
	client, mux := setupRateLimitedTestGitHubServer(t)
	s := &symlinkModesServer{trees: map[string][]graphQLTreeEntry{}}
	s.ic = NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))

	mux.HandleFunc("GET /repos/octocat/hello-world/compare/{basehead}", func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.Header.Get("Accept"), "diff")
		assert.Equal(t, "base1...head1", r.PathValue("basehead"))
		s.mu.Lock()
		s.compares++
		s.mu.Unlock()
		s.diff(w)
	})
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Variables map[string]any `json:"variables"`
		}
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		ref, _ := req.Variables["ref"].(string)
		assert.NotEmpty(t, ref, "every directory listing names the ref it resolves")
		expressions := map[int]string{}
		var indexes []int
		for name, value := range req.Variables {
			if !strings.HasPrefix(name, "e") {
				continue
			}
			i, err := strconv.Atoi(strings.TrimPrefix(name, "e"))
			assert.NoError(t, err)
			expressions[i] = value.(string)
			indexes = append(indexes, i)
		}
		sort.Ints(indexes)
		dirs := make([]string, 0, len(indexes))
		for _, i := range indexes {
			dirs = append(dirs, strings.TrimPrefix(expressions[i], ref+":"))
		}
		s.mu.Lock()
		s.queries = append(s.queries, dirs)
		s.mu.Unlock()
		if s.graphQL != nil && s.graphQL(w, ref, dirs) {
			return
		}
		repository := map[string]any{"ref": nil}
		for key := range s.trees {
			if strings.HasPrefix(key, ref+":") {
				repository["ref"] = map[string]any{"oid": ref}
				break
			}
		}
		for _, i := range indexes {
			alias := "d" + strconv.Itoa(i)
			entries, ok := s.trees[expressions[i]]
			if !ok {
				repository[alias] = nil
				continue
			}
			repository[alias] = map[string]any{"entries": entries}
		}
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"repository": repository}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/{ref}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		if !assert.Equal(t, "1", r.URL.Query().Get("recursive"), "the tree is read whole, never level by level") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.treeReads = append(s.treeReads, ref)
		s.mu.Unlock()
		if s.recursiveTree != nil && s.recursiveTree(w, ref) {
			return
		}
		var entries []*gh.TreeEntry
		for key, dirEntries := range s.trees {
			dir, under := strings.CutPrefix(key, ref+":")
			if !under {
				continue
			}
			for _, entry := range dirEntries {
				entries = append(entries, &gh.TreeEntry{
					Path: new(path.Join(dir, entry.Name)),
					Mode: new(strconv.FormatInt(int64(entry.Mode), 8)),
					Type: new("blob"),
				})
			}
		}
		assert.NoError(t, json.NewEncoder(w).Encode(gh.Tree{SHA: new(ref), Entries: entries, Truncated: new(false)}))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected GitHub request %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	})
	return s
}

func (s *symlinkModesServer) compareCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.compares
}

// graphQLQueries returns the directories each GraphQL query listed, in order.
func (s *symlinkModesServer) graphQLQueries() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]string(nil), s.queries...)
}

// treeReadRefs returns the ref of each recursive tree read, in order.
func (s *symlinkModesServer) treeReadRefs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.treeReads...)
}

// graphQLBatchSizes returns how many directories each GraphQL query listed,
// in order.
func (s *symlinkModesServer) graphQLBatchSizes() []int {
	sizes := []int{}
	for _, dirs := range s.graphQLQueries() {
		sizes = append(sizes, len(dirs))
	}
	return sizes
}

func writeDiffTooLarge(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	w.WriteHeader(http.StatusNotAcceptable)
	assert.NoError(t, json.NewEncoder(w).Encode(gh.ErrorResponse{
		Message: "Sorry, the diff exceeded the maximum number of files (300).",
		Errors:  []gh.Error{{Resource: "PullRequest", Field: "diff", Code: "too_large"}},
	}))
}

func writeDiffTimedOut(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	w.WriteHeader(http.StatusUnprocessableEntity)
	assert.NoError(t, json.NewEncoder(w).Encode(gh.ErrorResponse{
		Message: "Sorry, this diff is taking too long to generate.",
		Errors:  []gh.Error{{Resource: "PullRequest", Field: "diff", Code: "timeout"}},
	}))
}

// A PR with fifty one-line edits in deeply nested files and one repointed
// environment symlink reads every mode from a single compare diff of its
// base and head commits: no tree is read and no GraphQL query is sent,
// however deep the files sit.
func TestSymlinkModesReadsEveryModeFromOneDiff(t *testing.T) {
	s := newSymlinkModesServer(t)
	var diff strings.Builder
	var probes []SymlinkProbe
	for i := range 50 {
		p := fmt.Sprintf("svc/a/b/c/d/e/f/g/h/i/j/k/File%d.java", i)
		fmt.Fprintf(&diff, "diff --git a/%s b/%s\nindex 1111111..2222222 100644\n--- a/%s\n+++ b/%s\n@@ -1 +1 @@\n-x\n+y\n", p, p, p, p)
		probes = append(probes, SymlinkProbe{Path: p, AtHead: true, AtBase: true})
	}
	diff.WriteString("diff --git a/orders/schema/staging b/orders/schema/staging\nindex 3333333..4444444 120000\n--- a/orders/schema/staging\n+++ b/orders/schema/staging\n@@ -1 +1 @@\n-production\n+base\n")
	probes = append(probes, SymlinkProbe{Path: "orders/schema/staging", AtHead: true, AtBase: true})
	s.diff = func(w http.ResponseWriter) { _, _ = io.WriteString(w, diff.String()) }

	modes, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, probes, false)
	require.NoError(t, err)

	assert.Equal(t, 1, s.compareCount(), "one diff request covers every changed file")
	assert.Empty(t, s.graphQLQueries(), "no fallback when the diff names every mode")
	assert.Equal(t, SymlinkMode{Base: true, Head: true}, modes["orders/schema/staging"])
	assert.Equal(t, SymlinkMode{}, modes["svc/a/b/c/d/e/f/g/h/i/j/k/File7.java"])
	assert.Len(t, modes, 51)
}

// When GitHub refuses the diff as too large, each side's modes come from one
// request. The head side's paths sit in more directories than one GraphQL
// query lists, so they come from one recursive listing of the tree; the base
// side's two directories fit in one GraphQL query. Nothing is read level by
// level.
func TestSymlinkModesReadsEachSideWithOneRequestWhenDiffTooLarge(t *testing.T) {
	s := newSymlinkModesServer(t)
	s.diff = func(w http.ResponseWriter) { writeDiffTooLarge(t, w) }
	var probes []SymlinkProbe
	for i := range 150 {
		dir := fmt.Sprintf("svc/pkg%d", i)
		probes = append(probes, SymlinkProbe{Path: dir + "/Main.java", AtHead: true})
		s.trees["head1:"+dir] = []graphQLTreeEntry{{Name: "Main.java", Mode: 0o100644}}
	}
	probes = append(probes,
		SymlinkProbe{Path: "orders/schema/staging", AtHead: true, AtBase: true},
		SymlinkProbe{Path: "gone/link", AtBase: true},
	)
	s.trees["head1:orders/schema"] = []graphQLTreeEntry{{Name: "staging", Mode: 40960}, {Name: "schemabot.yaml", Mode: 0o100644}}
	s.trees["base1:orders/schema"] = []graphQLTreeEntry{{Name: "staging", Mode: 0o100644}}
	// gone/ is absent at base, so its listing comes back null.

	modes, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, probes, false)
	require.NoError(t, err)

	assert.Equal(t, 1, s.compareCount())
	assert.Equal(t, []string{"head1"}, s.treeReadRefs(), "151 head directories are read from one tree listing")
	assert.Equal(t, [][]string{{"orders/schema", "gone"}}, s.graphQLQueries(), "2 base directories are read in one GraphQL query")
	assert.Equal(t, SymlinkMode{Head: true, Base: false}, modes["orders/schema/staging"], "the symlink is found in the tree listing")
	assert.Equal(t, SymlinkMode{}, modes["gone/link"])
	assert.Equal(t, SymlinkMode{}, modes["svc/pkg149/Main.java"])
}

// However many directories the unknown paths sit in, past one GraphQL batch
// each side costs exactly one recursive tree read: 150 directories and 1,500
// cost the same. Pure renames, which the diff gives no mode for, are read the
// same way as paths of a refused diff.
func TestSymlinkModesReadCostDoesNotGrowWithDirectories(t *testing.T) {
	for _, dirs := range []int{symlinkModeGraphQLBatch + 1, 1500} {
		t.Run(fmt.Sprintf("%d directories", dirs), func(t *testing.T) {
			s := newSymlinkModesServer(t)
			var diff strings.Builder
			var probes []SymlinkProbe
			for i := range dirs {
				from, to := fmt.Sprintf("old/pkg%04d/Main.java", i), fmt.Sprintf("new/pkg%04d/Main.java", i)
				fmt.Fprintf(&diff, "diff --git a/%s b/%s\nsimilarity index 100%%\nrename from %s\nrename to %s\n", from, to, from, to)
				probes = append(probes, SymlinkProbe{Path: to, AtHead: true}, SymlinkProbe{Path: from, AtBase: true})
				s.trees["head1:"+path.Dir(to)] = []graphQLTreeEntry{{Name: "Main.java", Mode: 0o100644}}
				s.trees["base1:"+path.Dir(from)] = []graphQLTreeEntry{{Name: "Main.java", Mode: 0o100644}}
			}
			s.trees["head1:new/pkg0007"][0].Mode = 40960
			s.diff = func(w http.ResponseWriter) { _, _ = io.WriteString(w, diff.String()) }

			modes, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, probes, false)
			require.NoError(t, err)

			assert.Equal(t, 1, s.compareCount())
			assert.Equal(t, []string{"head1", "base1"}, s.treeReadRefs(), "one tree listing per side")
			assert.Empty(t, s.graphQLQueries())
			assert.Equal(t, SymlinkMode{Head: true}, modes["new/pkg0007/Main.java"])
			assert.Equal(t, SymlinkMode{}, modes["old/pkg0007/Main.java"])
			assert.Len(t, modes, 2*dirs)
		})
	}
}

// writeTruncatedTree answers a recursive tree read the way GitHub answers one
// for a repository too large to list: truncated, with entries a caller must
// not trust.
func writeTruncatedTree(t *testing.T, w http.ResponseWriter, ref string) {
	t.Helper()
	assert.NoError(t, json.NewEncoder(w).Encode(gh.Tree{SHA: new(ref), Entries: []*gh.TreeEntry{}, Truncated: new(true)}))
}

// manyDirectoryProbes returns head probes for Main.java in dirs directories,
// registered as regular files at head1, with the one in pkg042 a symlink.
func manyDirectoryProbes(s *symlinkModesServer, dirs int) []SymlinkProbe {
	var probes []SymlinkProbe
	for i := range dirs {
		dir := fmt.Sprintf("schema/pkg%03d", i)
		probes = append(probes, SymlinkProbe{Path: dir + "/Main.java", AtHead: true})
		s.trees["head1:"+dir] = []graphQLTreeEntry{{Name: "Main.java", Mode: 0o100644}}
	}
	s.trees["head1:schema/pkg042"][0].Mode = 40960
	return probes
}

// When GitHub truncates the tree listing and the caller probed only paths
// inside the configured schema directories, the paths are read with batched
// GraphQL queries: their count is bounded by the schema directories the PR
// touches, not by the rest of the PR.
func TestSymlinkModesReadsScopedPathsWithGraphQLWhenTheTreeIsTruncated(t *testing.T) {
	s := newSymlinkModesServer(t)
	s.diff = func(w http.ResponseWriter) { writeDiffTooLarge(t, w) }
	s.recursiveTree = func(w http.ResponseWriter, ref string) bool {
		writeTruncatedTree(t, w, ref)
		return true
	}
	probes := manyDirectoryProbes(s, 151)

	modes, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, probes, true)
	require.NoError(t, err)

	assert.Equal(t, []string{"head1"}, s.treeReadRefs())
	assert.Equal(t, []int{100, 51}, s.graphQLBatchSizes())
	assert.Equal(t, SymlinkMode{Head: true}, modes["schema/pkg042/Main.java"])
	assert.Equal(t, SymlinkMode{}, modes["schema/pkg150/Main.java"])
}

// When GitHub truncates the tree listing and no configured schema directories
// bound the probed paths, reading them directory by directory would cost
// requests in proportion to the PR's other files. They are not read: every
// one is reported as not a symlink, without an error, so the PR is still
// planned.
func TestSymlinkModesLeavesUnscopedPathsUnreadWhenTheTreeIsTruncated(t *testing.T) {
	s := newSymlinkModesServer(t)
	s.diff = func(w http.ResponseWriter) { writeDiffTooLarge(t, w) }
	s.recursiveTree = func(w http.ResponseWriter, ref string) bool {
		writeTruncatedTree(t, w, ref)
		return true
	}
	probes := manyDirectoryProbes(s, 151)

	modes, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, probes, false)
	require.NoError(t, err)

	assert.Equal(t, []string{"head1"}, s.treeReadRefs())
	assert.Empty(t, s.graphQLQueries(), "no directory is listed")
	assert.Equal(t, SymlinkMode{}, modes["schema/pkg042/Main.java"], "an unread path is reported as not a symlink")
	assert.Len(t, modes, 151)
}

// A tree listing GitHub fails to serve is an error, never "not a symlink".
func TestSymlinkModesReturnsTreeError(t *testing.T) {
	s := newSymlinkModesServer(t)
	s.diff = func(w http.ResponseWriter) { writeDiffTooLarge(t, w) }
	s.recursiveTree = func(w http.ResponseWriter, _ string) bool {
		w.WriteHeader(http.StatusForbidden)
		return true
	}
	probes := manyDirectoryProbes(s, 151)

	_, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, probes, false)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "list the tree at head1")
	assert.Contains(t, err.Error(), "403")
	assert.Empty(t, s.graphQLQueries())
}

// GitHub answers a diff that takes too long to generate with 422 rather than
// 406. That says nothing about the commits, so the modes come from GraphQL
// the same way as for a diff that is too large.
func TestSymlinkModesFallsBackToGraphQLWhenTheDiffIsSlow(t *testing.T) {
	s := newSymlinkModesServer(t)
	s.diff = func(w http.ResponseWriter) { writeDiffTimedOut(t, w) }
	s.trees["head1:orders/schema"] = []graphQLTreeEntry{{Name: "staging", Mode: 40960}}
	s.trees["base1:orders/schema"] = []graphQLTreeEntry{{Name: "staging", Mode: 0o100644}}

	modes, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, []SymlinkProbe{
		{Path: "orders/schema/staging", AtHead: true, AtBase: true},
	}, false)
	require.NoError(t, err)

	assert.Equal(t, 1, s.compareCount())
	assert.Equal(t, [][]string{{"orders/schema"}, {"orders/schema"}}, s.graphQLQueries())
	assert.Equal(t, SymlinkMode{Head: true, Base: false}, modes["orders/schema/staging"])
}

// A pure rename carries no mode in the diff, so only its paths are read
// through GraphQL; the rest of the change still comes from the diff.
func TestSymlinkModesReadsOnlyModelessPathsThroughGraphQL(t *testing.T) {
	s := newSymlinkModesServer(t)
	s.diff = func(w http.ResponseWriter) {
		_, _ = io.WriteString(w, "diff --git a/a/README.md b/a/README.md\nindex 1111111..2222222 100644\n--- a/a/README.md\n+++ b/a/README.md\n@@ -1 +1 @@\n-x\n+y\n"+
			"diff --git a/old/link b/new/link\nsimilarity index 100%\nrename from old/link\nrename to new/link\n")
	}
	s.trees["head1:new"] = []graphQLTreeEntry{{Name: "link", Mode: 40960}}
	s.trees["base1:old"] = []graphQLTreeEntry{{Name: "link", Mode: 40960}}

	modes, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, []SymlinkProbe{
		{Path: "a/README.md", AtHead: true, AtBase: true},
		{Path: "new/link", AtHead: true},
		{Path: "old/link", AtBase: true},
	}, false)
	require.NoError(t, err)

	assert.Equal(t, [][]string{{"new"}, {"old"}}, s.graphQLQueries(), "one query per side, each for the renamed path's directory only")
	assert.Equal(t, SymlinkMode{}, modes["a/README.md"])
	assert.Equal(t, SymlinkMode{Head: true}, modes["new/link"])
	assert.Equal(t, SymlinkMode{Base: true}, modes["old/link"])
}

// A probed path at the repository root is listed from the root tree, whose
// GraphQL expression is the bare ref.
func TestSymlinkModesReadsRootDirectoryThroughGraphQL(t *testing.T) {
	s := newSymlinkModesServer(t)
	s.diff = func(w http.ResponseWriter) { writeDiffTooLarge(t, w) }
	s.trees["head1:"] = []graphQLTreeEntry{{Name: "staging", Mode: 40960}}

	modes, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, []SymlinkProbe{{Path: "staging", AtHead: true}}, false)
	require.NoError(t, err)

	assert.Equal(t, SymlinkMode{Head: true}, modes["staging"])
}

// No probes, no requests.
func TestSymlinkModesWithoutProbesSendsNoRequest(t *testing.T) {
	s := newSymlinkModesServer(t)

	modes, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, nil, false)
	require.NoError(t, err)

	assert.Empty(t, modes)
	assert.Equal(t, 0, s.compareCount())
}

// A diff GitHub fails to serve for any reason other than its size is an
// error, so discovery fails closed instead of treating modes as unknown.
func TestSymlinkModesReturnsDiffError(t *testing.T) {
	s := newSymlinkModesServer(t)
	s.diff = func(w http.ResponseWriter) { w.WriteHeader(http.StatusInternalServerError) }

	_, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, []SymlinkProbe{{Path: "x/link", AtHead: true}}, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read the diff of octocat/hello-world")
	assert.Empty(t, s.graphQLQueries())
}

func writeGraphQLErrors(t *testing.T, w http.ResponseWriter, message string) {
	t.Helper()
	assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"errors": []map[string]string{{"message": message}}}))
}

// A GraphQL error on the head side is returned rather than read as "not a
// symlink", and GitHub's stated refusal is not retried with a smaller batch.
func TestSymlinkModesReturnsGraphQLError(t *testing.T) {
	s := newSymlinkModesServer(t)
	s.diff = func(w http.ResponseWriter) { writeDiffTooLarge(t, w) }
	s.graphQL = func(w http.ResponseWriter, _ string, _ []string) bool {
		writeGraphQLErrors(t, w, "Something went wrong")
		return true
	}

	_, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, []SymlinkProbe{{Path: "x/link", AtHead: true}}, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Something went wrong")
	assert.Equal(t, []int{1}, s.graphQLBatchSizes(), "a GraphQL errors payload is not retried")
}

// A GraphQL error on the base side is returned too: a swallowed base error
// would turn a removed symlink into "not a symlink".
func TestSymlinkModesReturnsGraphQLErrorAtBase(t *testing.T) {
	s := newSymlinkModesServer(t)
	s.diff = func(w http.ResponseWriter) { writeDiffTooLarge(t, w) }
	s.trees["head1:x"] = []graphQLTreeEntry{{Name: "link", Mode: 0o100644}}
	s.graphQL = func(w http.ResponseWriter, ref string, _ []string) bool {
		if ref != "base1" {
			return false
		}
		writeGraphQLErrors(t, w, "Something went wrong at base")
		return true
	}

	_, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, []SymlinkProbe{{Path: "x/link", AtHead: true, AtBase: true}}, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at base base1")
	assert.Contains(t, err.Error(), "Something went wrong at base")
}

// GitHub answers null for a ref it cannot resolve just as it does for an
// absent directory; a ref that does not resolve is an error, never "no
// symlink".
func TestSymlinkModesRejectsAnUnresolvableRef(t *testing.T) {
	s := newSymlinkModesServer(t)
	s.diff = func(w http.ResponseWriter) { writeDiffTooLarge(t, w) }
	// No tree is registered for head1, as when the commit does not exist.

	_, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, []SymlinkProbe{{Path: "orders/schema/staging", AtHead: true}}, false)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "head1 does not resolve in octocat/hello-world")
}

// A GraphQL response without the repository is an error, not a listing in
// which every directory is absent.
func TestSymlinkModesRejectsANullRepository(t *testing.T) {
	s := newSymlinkModesServer(t)
	s.diff = func(w http.ResponseWriter) { writeDiffTooLarge(t, w) }
	s.graphQL = func(w http.ResponseWriter, _ string, _ []string) bool {
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"repository": nil}}))
		return true
	}

	_, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, []SymlinkProbe{{Path: "x/link", AtHead: true}}, false)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "repository octocat/hello-world not returned")
}

// A PR over GitHub's diff limits probes 100 paths in 100 directories of
// generated code. GitHub gives up on a GraphQL query whose response is too
// expensive to build, so the batch is split in half and each half retried
// until the queries are small enough, and the call succeeds instead of
// sending the same failing query on every delivery.
func TestSymlinkModesSplitsAGraphQLBatchGitHubGivesUpOn(t *testing.T) {
	const dirs, filesPerDir, maxEntriesPerResponse = 100, 20, 500

	s := newSymlinkModesServer(t)
	s.diff = func(w http.ResponseWriter) { writeDiffTooLarge(t, w) }
	var probes []SymlinkProbe
	for i := range dirs {
		dir := fmt.Sprintf("gen/pkg%03d", i)
		probes = append(probes, SymlinkProbe{Path: dir + "/Gen0.java", AtHead: true})
		entries := make([]graphQLTreeEntry, filesPerDir)
		for j := range entries {
			entries[j] = graphQLTreeEntry{Name: fmt.Sprintf("Gen%d.java", j), Mode: 0o100644}
		}
		s.trees["head1:"+dir] = entries
	}
	s.trees["head1:gen/pkg042"][0].Mode = 40960
	s.graphQL = func(w http.ResponseWriter, _ string, queried []string) bool {
		if len(queried)*filesPerDir <= maxEntriesPerResponse {
			return false
		}
		w.WriteHeader(http.StatusBadGateway) // GitHub's answer to a query that ran out of time
		return true
	}

	modes, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, probes, false)
	require.NoError(t, err)

	assert.Equal(t, []int{100, 50, 25, 25, 50, 25, 25}, s.graphQLBatchSizes(), "each failed batch is halved until GitHub answers")
	assert.Empty(t, s.treeReadRefs(), "paths in one batch of directories are read with GraphQL, not from the tree")
	assert.Equal(t, SymlinkMode{Head: true}, modes["gen/pkg042/Gen0.java"])
	assert.Equal(t, SymlinkMode{}, modes["gen/pkg099/Gen0.java"])
	assert.Len(t, modes, dirs)
}

// A refusal GitHub states with a 4xx is returned as is: a smaller batch would
// be refused the same way, so it is not retried.
func TestSymlinkModesDoesNotSplitAGraphQLBatchOnAClientError(t *testing.T) {
	s := newSymlinkModesServer(t)
	s.diff = func(w http.ResponseWriter) { writeDiffTooLarge(t, w) }
	s.graphQL = func(w http.ResponseWriter, _ string, _ []string) bool {
		w.WriteHeader(http.StatusForbidden)
		return true
	}
	probes := []SymlinkProbe{{Path: "a/one", AtHead: true}, {Path: "b/two", AtHead: true}}

	_, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, probes, false)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "403 Forbidden")
	assert.Equal(t, []int{2}, s.graphQLBatchSizes())
}
