package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
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
// response size. Paths in more directories than one batch holds are read
// from the recursive tree instead, so the request count stays constant.
const symlinkModeGraphQLBatch = 100

// ChangeRange identifies a change whose files' modes are read. Base is the
// commit the change was made from: a PR's merge base, or a comparison's base
// commit. Head is the commit the change produced. The modes are read for
// exactly these two commits, so a push to the PR while they are read cannot
// move the answer. PR is the pull request number, or 0 for a comparison
// between two commits; it is only carried into logs and metrics.
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
// larger than its limits (or too slow to build). The paths it leaves unknown
// are read on each side with at most one request, however many there are:
//
//   - Paths in at most symlinkModeGraphQLBatch distinct directories are read
//     from those directories in one GraphQL query.
//   - More are read from one recursive listing of the whole tree.
//   - When GitHub truncates that listing, the paths are read with batched
//     GraphQL queries only if scopedToSchemaDirs says the caller probed only
//     paths inside the configured schema directories, so the queries grow
//     with the schema directories the PR touches rather than with the rest of
//     the PR. Otherwise they are not read: they are reported as not symlinks,
//     with a warning and a count in schemabot.github.symlink_modes_unread_total.
//
// The tree is never walked level by level, and a request GitHub fails is an
// error, never "not a symlink".
//
// Callers probe only the change's schema inputs: paths in or beside a schema
// directory, and paths that could be a symlink into one. Either way the read
// costs a bounded number of requests per side, so a PR's request count never
// grows with its non-schema files (AV-15).
func (ic *InstallationClient) SymlinkModes(ctx context.Context, repo string, change ChangeRange, probes []SymlinkProbe, scopedToSchemaDirs bool) (map[string]SymlinkMode, error) {
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
		symlinks, err := ic.symlinksAtRef(ctx, repo, change, change.Head, unknownAtHead, scopedToSchemaDirs)
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
		symlinks, err := ic.symlinksAtRef(ctx, repo, change, change.Base, unknownAtBase, scopedToSchemaDirs)
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

// symlinksAtRef reports which of paths, whose modes the diff did not give,
// are symlinks at ref (see SymlinkModes for how the read is chosen).
func (ic *InstallationClient) symlinksAtRef(ctx context.Context, repo string, change ChangeRange, ref string, paths []string, scopedToSchemaDirs bool) (map[string]bool, error) {
	dirs := parentDirs(paths)
	if len(dirs) <= symlinkModeGraphQLBatch {
		return ic.graphQLSymlinks(ctx, repo, ref, paths)
	}

	entries, truncated, err := ic.FetchGitTree(ctx, repo, ref)
	if err != nil {
		return nil, fmt.Errorf("list the tree at %s to read the modes of %d paths in %d directories: %w", ref, len(paths), len(dirs), err)
	}
	if !truncated {
		symlinkEntries := make(map[string]bool)
		for _, entry := range entries {
			if entry.Mode == gitSymlinkModeOctal {
				symlinkEntries[entry.Path] = true
			}
		}
		symlinks := make(map[string]bool, len(paths))
		for _, p := range paths {
			symlinks[p] = symlinkEntries[path.Clean(p)]
		}
		return symlinks, nil
	}

	if scopedToSchemaDirs {
		ic.logger.Info("GitHub truncated the tree listing, so the modes of changed paths inside the configured schema directories are read from their directories with GraphQL",
			"repo", repo, "pr", change.PR, "ref", ref, "paths", len(paths), "directories", len(dirs))
		return ic.graphQLSymlinks(ctx, repo, ref, paths)
	}

	// Without configured schema directories bounding the paths, reading them
	// directory by directory would cost requests in proportion to the PR's
	// other files, so they are left unread and reported as not symlinks.
	ic.logger.Warn("GitHub truncated the tree listing and no configured schema directories bound the changed paths, so these paths are treated as not symlinks: a symlink among them that names a schema root or namespace is not planned; set allowed_dirs on every database the repository serves so only paths inside them are read",
		"repo", repo, "pr", change.PR, "base_sha", change.Base, "head_sha", change.Head, "ref", ref, "paths", len(paths), "directories", len(dirs))
	metrics.RecordSymlinkModesUnread(ctx, repo, len(paths))
	return make(map[string]bool, len(paths)), nil
}

// parentDirs returns the distinct parent directories of paths, in first-seen
// order.
func parentDirs(paths []string) []string {
	var dirs []string
	seen := map[string]bool{}
	for _, p := range paths {
		dir := path.Dir(p)
		if !seen[dir] {
			seen[dir] = true
			dirs = append(dirs, dir)
		}
	}
	return dirs
}

// diffModes holds, per path, whether the change's diff shows it as a symlink
// before the change (base, keyed by the path before a rename) and after it
// (head, keyed by the path after a rename). A path missing from a map has no
// mode in the diff on that side.
type diffModes struct {
	base map[string]bool
	head map[string]bool
}

// fetchChangeDiffModes reads the diff between the change's two commits and
// parses its file modes. The diff is read as a comparison of the two commits
// rather than as the PR's diff, so a push to the PR while it is read cannot
// move the result away from the commits the GraphQL fallback reads. A diff
// GitHub will not produce yields no modes, so every probe falls back to
// GraphQL.
func (ic *InstallationClient) fetchChangeDiffModes(ctx context.Context, repo string, change ChangeRange) (diffModes, error) {
	owner, repoName := splitRepo(repo)
	ctx = withGitHubRateLimitContext(ctx, metrics.GitHubOperationFetchDiff, repo)
	diff, _, err := ic.client.Repositories.CompareCommitsRaw(ctx, owner, repoName, change.Base, change.Head, gh.RawOptions{Type: gh.Diff})
	if isDiffUnavailable(err) {
		ic.logger.Info("GitHub did not produce the change's diff, so changed file modes are read from their directories with GraphQL",
			"repo", repo, "pr", change.PR, "base_sha", change.Base, "head_sha", change.Head, "error", err)
		return diffModes{}, nil
	}
	if err != nil {
		return diffModes{}, fmt.Errorf("read the diff of %s (pr %d, %s...%s) for changed file modes: %w", repo, change.PR, change.Base, change.Head, err)
	}
	return parseDiffModes(diff), nil
}

// isDiffUnavailable reports whether GitHub declined to produce a diff: it
// answers one that exceeds its size limits (files, lines, or bytes) with 406,
// and one that takes too long to generate with 422. Neither says anything
// about the commits themselves, so the modes are read another way.
func isDiffUnavailable(err error) bool {
	var errResp *gh.ErrorResponse
	if !errors.As(err, &errResp) || errResp.Response == nil {
		return false
	}
	return errResp.Response.StatusCode == http.StatusNotAcceptable || errResp.Response.StatusCode == http.StatusUnprocessableEntity
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
// whose paths cannot be read is skipped, leaving its paths unknown too. Git
// writes a change of a file's type (a symlink replaced by a regular file, or
// the reverse) as a deletion block followed by a creation block for the same
// path, so a path's mode on a side is the one its headers state explicitly,
// and a block's inferred absence never overrides it.
//
// The diff is read line by line whatever the lines hold, so a file whose
// content is one very long line, such as a minified bundle, does not cut the
// files after it off from their modes.
func parseDiffModes(diff string) diffModes {
	modes := diffModes{base: map[string]bool{}, head: map[string]bool{}}
	var block diffBlock
	flush := func() {
		block.record(modes)
		block = diffBlock{}
	}
	inHeader := false
	for line := range strings.Lines(diff) {
		line = strings.TrimRight(line, "\r\n")
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

// record stores the block's modes, for the sides its headers determine. A
// mode the headers state explicitly always wins; absence of the file on a
// side is recorded only while no other block has stated a mode for the path
// there, since a type change splits one path across a deletion block and a
// creation block.
func (b diffBlock) record(modes diffModes) {
	if b.basePath != "" {
		recordSideMode(modes.base, b.basePath, b.baseMode, b.sharedMode, b.baseAbsent)
	}
	if b.headPath != "" {
		recordSideMode(modes.head, b.headPath, b.headMode, b.sharedMode, b.headAbsent)
	}
}

// recordSideMode stores whether p is a symlink on one side: from the side's
// own mode header, else from the index line's shared mode, else as absent
// when no other block has already stated a mode for p on that side.
func recordSideMode(side map[string]bool, p, sideMode, sharedMode string, absent bool) {
	switch {
	case sideMode != "":
		side[p] = sideMode == gitSymlinkModeOctal
	case sharedMode != "":
		side[p] = sharedMode == gitSymlinkModeOctal
	case absent:
		if _, known := side[p]; !known {
			side[p] = false
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
	dirs := parentDirs(paths)
	symlinkEntries := map[string]bool{}
	for start := 0; start < len(dirs); start += symlinkModeGraphQLBatch {
		batch := dirs[start:min(start+symlinkModeGraphQLBatch, len(dirs))]
		entries, err := ic.graphQLListDirectoriesSplitting(ctx, repo, ref, batch)
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

// graphQLStatusError is a GraphQL request GitHub answered with a non-200
// status, which carries no GraphQL errors payload.
type graphQLStatusError struct {
	status string
	code   int
}

func (e *graphQLStatusError) Error() string {
	return "endpoint returned " + e.status
}

// graphQLListDirectoriesSplitting lists dirs at ref, splitting the batch in
// half and retrying each half when GitHub fails to answer in time. A query
// that lists many large directories can be too expensive for GitHub to build
// before it gives up, and the same query would fail on every redelivery, so
// the batch shrinks down to one directory before the failure is returned. A
// refusal GitHub states (a 4xx or a GraphQL errors payload) is returned as
// is, since a smaller batch would be refused the same way.
func (ic *InstallationClient) graphQLListDirectoriesSplitting(ctx context.Context, repo, ref string, dirs []string) (map[string][]graphQLTreeEntry, error) {
	listed, err := ic.graphQLListDirectories(ctx, repo, ref, dirs)
	if err == nil {
		return listed, nil
	}
	if len(dirs) == 1 || ctx.Err() != nil || !isGraphQLTimeoutClassError(err) {
		return nil, err
	}
	ic.logger.Info("GraphQL directory listing timed out, so the batch is split in half and each half retried",
		"repo", repo, "ref", ref, "batch", len(dirs), "error", err)
	half := len(dirs) / 2
	listed, err = ic.graphQLListDirectoriesSplitting(ctx, repo, ref, dirs[:half])
	if err != nil {
		return nil, err
	}
	rest, err := ic.graphQLListDirectoriesSplitting(ctx, repo, ref, dirs[half:])
	if err != nil {
		return nil, err
	}
	maps.Copy(listed, rest)
	return listed, nil
}

// isGraphQLTimeoutClassError reports whether a GraphQL listing failed in a
// way a smaller batch can avoid: GitHub gave up on the query (a 5xx) or the
// request timed out before it answered.
func isGraphQLTimeoutClassError(err error) bool {
	var statusErr *graphQLStatusError
	if errors.As(err, &statusErr) {
		return statusErr.code >= http.StatusInternalServerError
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// graphQLListDirectories lists the entries of dirs at ref in one GraphQL
// query. The query also resolves ref itself: GitHub answers null for a
// directory that is absent at ref and for a ref it cannot resolve alike, so
// a null directory counts as absent only once ref is known to resolve.
func (ic *InstallationClient) graphQLListDirectories(ctx context.Context, repo, ref string, dirs []string) (map[string][]graphQLTreeEntry, error) {
	owner, repoName := splitRepo(repo)
	var query strings.Builder
	query.WriteString("query($owner: String!, $name: String!, $ref: String!")
	variables := map[string]any{"owner": owner, "name": repoName, "ref": ref}
	for i := range dirs {
		fmt.Fprintf(&query, ", $e%d: String!", i)
	}
	query.WriteString(") { repository(owner: $owner, name: $name) { ref: object(expression: $ref) { oid }")
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
		return nil, fmt.Errorf("list %d directories at %s with GraphQL: %w", len(dirs), ref, &graphQLStatusError{status: resp.Status, code: resp.StatusCode})
	}

	var result struct {
		Data struct {
			Repository map[string]*struct {
				OID     string             `json:"oid"`
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
	if result.Data.Repository["ref"] == nil {
		return nil, fmt.Errorf("list %d directories at %s with GraphQL: %s does not resolve in %s", len(dirs), ref, ref, repo)
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
