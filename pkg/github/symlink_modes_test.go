package github

import (
	"encoding/json"
	"fmt"
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
		// A regular file turned into a symlink with the same bytes.
		"diff --git a/docs/CLAUDE.md b/docs/CLAUDE.md",
		"old mode 100644",
		"new mode 120000",
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

// symlinkModesServer fakes the GitHub endpoints SymlinkModes uses and counts
// every request, failing the test on any other endpoint, such as a tree read.
type symlinkModesServer struct {
	ic       *InstallationClient
	diff     func(w http.ResponseWriter)
	trees    map[string][]graphQLTreeEntry // "<ref>:<dir>" → entries; absent → null
	prDiffs  atomic.Int32
	compares atomic.Int32
	graphQL  atomic.Int32
}

func newSymlinkModesServer(t *testing.T) *symlinkModesServer {
	t.Helper()
	client, mux := setupRateLimitedTestGitHubServer(t)
	s := &symlinkModesServer{trees: map[string][]graphQLTreeEntry{}}
	s.ic = NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))

	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/7", func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.Header.Get("Accept"), "diff")
		s.prDiffs.Add(1)
		s.diff(w)
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/{basehead}", func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.Header.Get("Accept"), "diff")
		assert.Equal(t, "base1...head1", r.PathValue("basehead"))
		s.compares.Add(1)
		s.diff(w)
	})
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, r *http.Request) {
		s.graphQL.Add(1)
		var req struct {
			Variables map[string]any `json:"variables"`
		}
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		repository := map[string]any{}
		for name, value := range req.Variables {
			if !strings.HasPrefix(name, "e") {
				continue
			}
			alias := "d" + strings.TrimPrefix(name, "e")
			entries, ok := s.trees[value.(string)]
			if !ok {
				repository[alias] = nil
				continue
			}
			repository[alias] = map[string]any{"entries": entries}
		}
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"repository": repository}}))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected GitHub request %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	})
	return s
}

func writeDiffTooLarge(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	w.WriteHeader(http.StatusNotAcceptable)
	assert.NoError(t, json.NewEncoder(w).Encode(gh.ErrorResponse{
		Message: "Sorry, the diff exceeded the maximum number of files (300).",
		Errors:  []gh.Error{{Resource: "PullRequest", Field: "diff", Code: "too_large"}},
	}))
}

// A PR with fifty one-line edits in deeply nested files and one repointed
// environment symlink reads every mode from a single diff request: no tree is
// read and no GraphQL query is sent, however deep the files sit.
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

	modes, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, probes)
	require.NoError(t, err)

	assert.Equal(t, int32(1), s.prDiffs.Load(), "one diff request covers every changed file")
	assert.Equal(t, int32(0), s.graphQL.Load(), "no fallback when the diff names every mode")
	assert.Equal(t, SymlinkMode{Base: true, Head: true}, modes["orders/schema/staging"])
	assert.Equal(t, SymlinkMode{}, modes["svc/a/b/c/d/e/f/g/h/i/j/k/File7.java"])
	assert.Len(t, modes, 51)
}

// When GitHub refuses the diff as too large, the modes come from batched
// GraphQL directory listings, one query per side for up to a hundred
// directories, and never from a level-by-level tree walk.
func TestSymlinkModesFallsBackToBatchedGraphQLWhenDiffTooLarge(t *testing.T) {
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
	s.trees["head1:orders/schema"] = []graphQLTreeEntry{{Name: "staging", Mode: gitSymlinkModeDecimal}, {Name: "schemabot.yaml", Mode: 0o100644}}
	s.trees["base1:orders/schema"] = []graphQLTreeEntry{{Name: "staging", Mode: 0o100644}}
	// gone/ is absent at base, so its listing comes back null.

	modes, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, probes)
	require.NoError(t, err)

	assert.Equal(t, int32(1), s.prDiffs.Load())
	assert.Equal(t, int32(3), s.graphQL.Load(), "151 head directories in two batches, 2 base directories in one")
	assert.Equal(t, SymlinkMode{Head: true, Base: false}, modes["orders/schema/staging"])
	assert.Equal(t, SymlinkMode{}, modes["gone/link"])
	assert.Equal(t, SymlinkMode{}, modes["svc/pkg149/Main.java"])
}

// A pure rename carries no mode in the diff, so only its paths are read
// through GraphQL; the rest of the change still comes from the diff.
func TestSymlinkModesReadsOnlyModelessPathsThroughGraphQL(t *testing.T) {
	s := newSymlinkModesServer(t)
	s.diff = func(w http.ResponseWriter) {
		_, _ = io.WriteString(w, "diff --git a/a/README.md b/a/README.md\nindex 1111111..2222222 100644\n--- a/a/README.md\n+++ b/a/README.md\n@@ -1 +1 @@\n-x\n+y\n"+
			"diff --git a/old/link b/new/link\nsimilarity index 100%\nrename from old/link\nrename to new/link\n")
	}
	s.trees["head1:new"] = []graphQLTreeEntry{{Name: "link", Mode: gitSymlinkModeDecimal}}
	s.trees["base1:old"] = []graphQLTreeEntry{{Name: "link", Mode: gitSymlinkModeDecimal}}

	modes, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, []SymlinkProbe{
		{Path: "a/README.md", AtHead: true, AtBase: true},
		{Path: "new/link", AtHead: true},
		{Path: "old/link", AtBase: true},
	})
	require.NoError(t, err)

	assert.Equal(t, int32(2), s.graphQL.Load(), "one query per side, each for the renamed path only")
	assert.Equal(t, SymlinkMode{}, modes["a/README.md"])
	assert.Equal(t, SymlinkMode{Head: true}, modes["new/link"])
	assert.Equal(t, SymlinkMode{Base: true}, modes["old/link"])
}

// A comparison between two commits reads the compare diff instead of a PR's.
func TestSymlinkModesReadsCompareDiffWithoutPR(t *testing.T) {
	s := newSymlinkModesServer(t)
	s.diff = func(w http.ResponseWriter) {
		_, _ = io.WriteString(w, "diff --git a/x/link b/x/link\nnew file mode 120000\nindex 0000000..1111111\n")
	}

	modes, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{Base: "base1", Head: "head1"}, []SymlinkProbe{{Path: "x/link", AtHead: true, AtBase: true}})
	require.NoError(t, err)

	assert.Equal(t, int32(1), s.compares.Load())
	assert.Equal(t, int32(0), s.prDiffs.Load())
	assert.Equal(t, SymlinkMode{Head: true}, modes["x/link"])
}

// No probes, no requests.
func TestSymlinkModesWithoutProbesSendsNoRequest(t *testing.T) {
	s := newSymlinkModesServer(t)

	modes, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, nil)
	require.NoError(t, err)

	assert.Empty(t, modes)
	assert.Equal(t, int32(0), s.prDiffs.Load())
}

// A diff GitHub fails to serve for any reason other than its size is an
// error, so discovery fails closed instead of treating modes as unknown.
func TestSymlinkModesReturnsDiffError(t *testing.T) {
	s := newSymlinkModesServer(t)
	s.diff = func(w http.ResponseWriter) { w.WriteHeader(http.StatusInternalServerError) }

	_, err := s.ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, []SymlinkProbe{{Path: "x/link", AtHead: true}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read the diff of octocat/hello-world")
	assert.Equal(t, int32(0), s.graphQL.Load())
}

// A GraphQL error is returned rather than read as "not a symlink".
func TestSymlinkModesReturnsGraphQLError(t *testing.T) {
	client, mux := setupRateLimitedTestGitHubServer(t)
	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/7", func(w http.ResponseWriter, _ *http.Request) { writeDiffTooLarge(t, w) })
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, _ *http.Request) {
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"errors": []map[string]string{{"message": "Something went wrong"}}}))
	})

	_, err := ic.SymlinkModes(t.Context(), "octocat/hello-world", ChangeRange{PR: 7, Base: "base1", Head: "head1"}, []SymlinkProbe{{Path: "x/link", AtHead: true}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Something went wrong")
}
