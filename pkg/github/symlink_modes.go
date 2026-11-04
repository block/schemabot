package github

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"

	gh "github.com/google/go-github/v86/github"

	"github.com/block/schemabot/pkg/metrics"
)

// gitSymlinkModeOctal is the Git file mode of a symlink as it appears in diff
// headers. GraphQL reports the same mode as a decimal integer.
const gitSymlinkModeOctal = "120000"

// gitSymlinkModeDecimal is gitSymlinkModeOctal as GraphQL's TreeEntry.mode
// reports it.
const gitSymlinkModeDecimal = 0o120000

// symlinkModeGraphQLBatch is how many directories one GraphQL query lists.
// A query lists each directory's entries once, so the batch bounds the
// response size rather than the request budget.
const symlinkModeGraphQLBatch = 100

// ChangeRange identifies a change whose files' modes are read. PR is the pull
// request number, or 0 for a comparison between two commits. Base is the
// commit the change was made from: a PR's merge base, or the comparison's base
// commit. Head is the commit the change produced.
type ChangeRange struct {
	PR   int
	Base string
	Head string
}

// SymlinkProbe names a changed path and the side of the change, before it
// (AtBase) or after it (AtHead), on which the caller needs to know whether
// the path is a symlink.
type SymlinkProbe struct {
	Path   string
	AtHead bool
	AtBase bool
}

// SymlinkMode reports whether a path is a symlink before (Base) and after
// (Head) a change. A path absent on a side is not a symlink there. Only the
// sides a probe asked for are set.
type SymlinkMode struct {
	Base bool
	Head bool
}

// SymlinkModes reports, for each probed path, whether it is a symlink on the
// sides the probe asked for.
//
// GitHub's file listings carry no file mode, so the modes come from the
// change's diff, whose headers name each changed file's mode before and after
// (new file mode, deleted file mode, old mode / new mode, or the mode on the
// index line). One request covers every changed file however deep it sits.
// The diff omits a mode only for a pure rename, and GitHub refuses a diff
// larger than its limits; those paths are read from their parent directories
// in batched GraphQL queries instead, which spend GitHub's GraphQL budget
// rather than the REST budget webhook deliveries depend on. The tree is never
// walked level by level.
func (ic *InstallationClient) SymlinkModes(ctx context.Context, repo string, change ChangeRange, probes []SymlinkProbe) (map[string]SymlinkMode, error) {
	modes := make(map[string]SymlinkMode, len(probes))
	if len(probes) == 0 {
		return modes, nil
	}

	diffModes, err := ic.fetchChangeDiffModes(ctx, repo, change)
	if err != nil {
		return nil, err
	}

	var unknownAtHead, unknownAtBase []string
	for _, probe := range probes {
		mode := modes[probe.Path]
		if probe.AtHead {
			symlink, known := diffModes.head[probe.Path]
			if known {
				mode.Head = symlink
			} else {
				unknownAtHead = append(unknownAtHead, probe.Path)
			}
		}
		if probe.AtBase {
			symlink, known := diffModes.base[probe.Path]
			if known {
				mode.Base = symlink
			} else {
				unknownAtBase = append(unknownAtBase, probe.Path)
			}
		}
		modes[probe.Path] = mode
	}

	if len(unknownAtHead) > 0 {
		symlinks, err := ic.graphQLSymlinks(ctx, repo, change.Head, unknownAtHead)
		if err != nil {
			return nil, fmt.Errorf("read the modes of %d changed paths at head %s in %s: %w", len(unknownAtHead), change.Head, repo, err)
		}
		for _, p := range unknownAtHead {
			mode := modes[p]
			mode.Head = symlinks[p]
			modes[p] = mode
		}
	}
	if len(unknownAtBase) > 0 {
		symlinks, err := ic.graphQLSymlinks(ctx, repo, change.Base, unknownAtBase)
		if err != nil {
			return nil, fmt.Errorf("read the modes of %d changed paths at base %s in %s: %w", len(unknownAtBase), change.Base, repo, err)
		}
		for _, p := range unknownAtBase {
			mode := modes[p]
			mode.Base = symlinks[p]
			modes[p] = mode
		}
	}
	return modes, nil
}

// diffModes holds, per path, whether the change's diff shows it as a symlink
// before the change (base, keyed by the path before a rename) and after it
// (head, keyed by the path after a rename). A path missing from a map has no
// mode in the diff on that side.
type diffModes struct {
	base map[string]bool
	head map[string]bool
}

// fetchChangeDiffModes reads the change's diff and parses its file modes. A
// diff GitHub refuses as too large yields no modes, so every probe falls back
// to GraphQL.
func (ic *InstallationClient) fetchChangeDiffModes(ctx context.Context, repo string, change ChangeRange) (diffModes, error) {
	owner, repoName := splitRepo(repo)
	ctx = withGitHubRateLimitContext(ctx, metrics.GitHubOperationFetchDiff, repo)
	var (
		diff string
		err  error
	)
	if change.PR > 0 {
		diff, _, err = ic.client.PullRequests.GetRaw(ctx, owner, repoName, change.PR, gh.RawOptions{Type: gh.Diff})
	} else {
		diff, _, err = ic.client.Repositories.CompareCommitsRaw(ctx, owner, repoName, change.Base, change.Head, gh.RawOptions{Type: gh.Diff})
	}
	if isDiffTooLarge(err) {
		ic.logger.Info("GitHub refused the change's diff as too large, so changed file modes are read from their directories with GraphQL",
			"repo", repo, "pr", change.PR, "base_sha", change.Base, "head_sha", change.Head)
		return diffModes{}, nil
	}
	if err != nil {
		return diffModes{}, fmt.Errorf("read the diff of %s (pr %d, %s...%s) for changed file modes: %w", repo, change.PR, change.Base, change.Head, err)
	}
	return parseDiffModes(diff), nil
}

// isDiffTooLarge reports whether GitHub refused a diff for exceeding its size
// limits (files, lines, or bytes), which it answers with 406.
func isDiffTooLarge(err error) bool {
	var errResp *gh.ErrorResponse
	return errors.As(err, &errResp) && errResp.Response != nil && errResp.Response.StatusCode == http.StatusNotAcceptable
}

// parseDiffModes reads each file block's extended headers in a git diff and
// records whether the file is a symlink before and after the change.
//
//	new file mode M          absent before, mode M after
//	deleted file mode M      mode M before, absent after
//	old mode M / new mode N  mode M before, mode N after
//	index a..b M             mode M on both sides (mode unchanged)
//
// A pure rename has none of these, so its paths are left unknown. A block
// whose paths cannot be read is skipped, leaving its paths unknown too.
func parseDiffModes(diff string) diffModes {
	modes := diffModes{base: map[string]bool{}, head: map[string]bool{}}
	var block diffBlock
	flush := func() {
		block.record(modes)
		block = diffBlock{}
	}
	scanner := bufio.NewScanner(strings.NewReader(diff))
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	inHeader := false
	for scanner.Scan() {
		line := scanner.Text()
		if rest, ok := strings.CutPrefix(line, "diff --git "); ok {
			flush()
			block.basePath, block.headPath = parseDiffGitPaths(rest)
			inHeader = true
			continue
		}
		if !inHeader {
			continue
		}
		switch {
		case strings.HasPrefix(line, "@@"), strings.HasPrefix(line, "--- "), strings.HasPrefix(line, "Binary files "):
			inHeader = false
		case strings.HasPrefix(line, "new file mode "):
			block.headMode, block.baseAbsent = strings.TrimPrefix(line, "new file mode "), true
		case strings.HasPrefix(line, "deleted file mode "):
			block.baseMode, block.headAbsent = strings.TrimPrefix(line, "deleted file mode "), true
		case strings.HasPrefix(line, "old mode "):
			block.baseMode = strings.TrimPrefix(line, "old mode ")
		case strings.HasPrefix(line, "new mode "):
			block.headMode = strings.TrimPrefix(line, "new mode ")
		case strings.HasPrefix(line, "index "):
			if fields := strings.Fields(line); len(fields) == 3 {
				block.sharedMode = fields[2]
			}
		case strings.HasPrefix(line, "rename from "), strings.HasPrefix(line, "copy from "):
			block.basePath = unquoteDiffPath(line[strings.Index(line, "from ")+len("from "):])
		case strings.HasPrefix(line, "rename to "), strings.HasPrefix(line, "copy to "):
			block.headPath = unquoteDiffPath(line[strings.Index(line, "to ")+len("to "):])
		}
	}
	flush()
	return modes
}

// diffBlock accumulates one file's extended diff headers.
type diffBlock struct {
	basePath, headPath     string
	baseMode, headMode     string
	sharedMode             string
	baseAbsent, headAbsent bool
}

// record stores the block's modes, for the sides its headers determine.
func (b diffBlock) record(modes diffModes) {
	if b.basePath != "" {
		switch {
		case b.baseAbsent:
			modes.base[b.basePath] = false
		case b.baseMode != "":
			modes.base[b.basePath] = b.baseMode == gitSymlinkModeOctal
		case b.sharedMode != "":
			modes.base[b.basePath] = b.sharedMode == gitSymlinkModeOctal
		}
	}
	if b.headPath != "" {
		switch {
		case b.headAbsent:
			modes.head[b.headPath] = false
		case b.headMode != "":
			modes.head[b.headPath] = b.headMode == gitSymlinkModeOctal
		case b.sharedMode != "":
			modes.head[b.headPath] = b.sharedMode == gitSymlinkModeOctal
		}
	}
}

// parseDiffGitPaths reads the two paths of a "diff --git a/<path> b/<path>"
// line. Git quotes a path with unusual characters. Unquoted paths are
// ambiguous when they contain spaces, but outside a rename or copy both
// paths are equal, which fixes the split; a rename or copy names its paths
// again in its rename/copy lines, which override these.
func parseDiffGitPaths(rest string) (basePath, headPath string) {
	if strings.HasPrefix(rest, `"`) {
		first, err := strconv.QuotedPrefix(rest)
		if err != nil {
			return "", ""
		}
		basePath = strings.TrimPrefix(unquoteDiffPath(first), "a/")
		headPath = strings.TrimPrefix(unquoteDiffPath(strings.TrimSpace(rest[len(first):])), "b/")
		return basePath, headPath
	}
	if len(rest) < len("a/ b/") || (len(rest)-len("a/ b/"))%2 != 0 {
		return "", ""
	}
	n := (len(rest) - len("a/ b/")) / 2
	a, b := rest[:len("a/")+n], rest[len("a/")+n+1:]
	if !strings.HasPrefix(a, "a/") || !strings.HasPrefix(b, "b/") || a[2:] != b[2:] {
		return "", ""
	}
	return a[2:], b[2:]
}

// unquoteDiffPath undoes git's C-style quoting of a path, which Go's string
// unquoting covers (octal byte escapes included). An unquoted path is
// returned as is.
func unquoteDiffPath(s string) string {
	if !strings.HasPrefix(s, `"`) {
		return s
	}
	unquoted, err := strconv.Unquote(s)
	if err != nil {
		return s
	}
	return unquoted
}

// graphQLSymlinks reports which of paths are symlinks at ref by listing
// their parent directories, up to symlinkModeGraphQLBatch directories per
// query. A path absent at ref is not a symlink.
func (ic *InstallationClient) graphQLSymlinks(ctx context.Context, repo, ref string, paths []string) (map[string]bool, error) {
	var dirs []string
	seen := map[string]bool{}
	for _, p := range paths {
		dir := path.Dir(p)
		if !seen[dir] {
			seen[dir] = true
			dirs = append(dirs, dir)
		}
	}

	symlinkEntries := map[string]bool{}
	for start := 0; start < len(dirs); start += symlinkModeGraphQLBatch {
		batch := dirs[start:min(start+symlinkModeGraphQLBatch, len(dirs))]
		entries, err := ic.graphQLListDirectories(ctx, repo, ref, batch)
		if err != nil {
			return nil, err
		}
		for dir, dirEntries := range entries {
			for _, entry := range dirEntries {
				if entry.Mode == gitSymlinkModeDecimal {
					symlinkEntries[path.Join(dir, entry.Name)] = true
				}
			}
		}
	}

	symlinks := make(map[string]bool, len(paths))
	for _, p := range paths {
		symlinks[p] = symlinkEntries[path.Clean(p)]
	}
	return symlinks, nil
}

// graphQLTreeEntry is one entry of a directory listed through GraphQL.
type graphQLTreeEntry struct {
	Name string `json:"name"`
	Mode int    `json:"mode"`
}

// graphQLListDirectories lists the entries of dirs at ref in one GraphQL
// query. A directory absent at ref has no entries.
func (ic *InstallationClient) graphQLListDirectories(ctx context.Context, repo, ref string, dirs []string) (map[string][]graphQLTreeEntry, error) {
	owner, repoName := splitRepo(repo)
	var query strings.Builder
	query.WriteString("query($owner: String!, $name: String!")
	variables := map[string]any{"owner": owner, "name": repoName}
	for i := range dirs {
		fmt.Fprintf(&query, ", $e%d: String!", i)
	}
	query.WriteString(") { repository(owner: $owner, name: $name) {")
	for i, dir := range dirs {
		fmt.Fprintf(&query, " d%d: object(expression: $e%d) { ... on Tree { entries { name mode } } }", i, i)
		if dir == "." {
			dir = ""
		}
		variables[fmt.Sprintf("e%d", i)] = ref + ":" + dir
	}
	query.WriteString(" } }")

	payload, err := json.Marshal(map[string]any{"query": query.String(), "variables": variables})
	if err != nil {
		return nil, fmt.Errorf("marshal GraphQL directory listing: %w", err)
	}
	ctx = withGitHubRateLimitContext(ctx, metrics.GitHubOperationGraphQLTreeEntries, repo)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ic.graphQLURL(), bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build GraphQL directory listing: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := ic.client.Client().Do(req)
	if err != nil {
		return nil, fmt.Errorf("list %d directories at %s with GraphQL: %w", len(dirs), ref, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list %d directories at %s with GraphQL: endpoint returned %s", len(dirs), ref, resp.Status)
	}

	var result struct {
		Data struct {
			Repository map[string]*struct {
				Entries []graphQLTreeEntry `json:"entries"`
			} `json:"repository"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode GraphQL directory listing at %s: %w", ref, err)
	}
	if len(result.Errors) > 0 {
		return nil, fmt.Errorf("list %d directories at %s with GraphQL: %s", len(dirs), ref, result.Errors[0].Message)
	}
	if result.Data.Repository == nil {
		return nil, fmt.Errorf("list %d directories at %s with GraphQL: repository %s not returned", len(dirs), ref, repo)
	}

	listed := make(map[string][]graphQLTreeEntry, len(dirs))
	for i, dir := range dirs {
		tree := result.Data.Repository[fmt.Sprintf("d%d", i)]
		if tree == nil {
			ic.logger.Debug("directory is absent at ref, so none of its probed paths is a symlink",
				"repo", repo, "ref", ref, "dir", dir)
			continue
		}
		listed[dir] = tree.Entries
	}
	return listed, nil
}
