package github

import (
	"context"
	"fmt"
	"path"
	"slices"
	"strings"

	gh "github.com/google/go-github/v86/github"
)

// SchemaDirChangedOnBase reports whether the resolved schema directory
// schemaPath changed on the PR's base branch since the branch diverged. It
// compares the git tree SHA of schemaPath at the merge base of baseRef/headSHA
// against its tree SHA at the current tip of baseRef — so a base branch that
// advanced by thousands of commits without touching schemaPath reports no
// change, while any change under schemaPath (including the directory being added
// or removed on base) reports changed.
//
// baseRef must be the base branch *ref name* (e.g. "main"), not a snapshot SHA.
// The live base tip is resolved from the compare response's BaseCommit rather
// than from the caller's snapshot: GitHub's pull.base.sha is not guaranteed to
// track the live base tip and is typically the base tip at PR creation — which
// for most PRs equals the merge base, so passing it would make the
// mergeBase == base short-circuit fire (reporting "no drift") in exactly the
// fast-moving-base scenario this guard exists for. Resolving the tip from the
// ref keeps the comparison against the branch's current state.
//
// changed is the safety-relevant signal: callers downgrade automatic apply to
// manual confirmation when it is true. changedFiles is a best-effort list of the
// files under schemaPath that changed on base, for operator-facing display; it
// may be empty even when changed is true (for example when the compare file list
// is unavailable or truncated), and callers must not treat an empty list as
// "nothing changed". err is returned only for failures that leave the change
// decision unknown; callers must fail closed (downgrade) on a non-nil err.
//
// Comparison uses recursive git tree SHAs, which commit to entry names, modes,
// types, and child object SHAs, so equal tree SHAs prove identical directory
// snapshots. It does not follow schema-namespace symlinks whose targets live
// outside schemaPath, nor does it detect a base branch repointing the
// environment-root symlink itself; those are known gaps where the guard behaves
// as if no base change occurred.
func (ic *InstallationClient) SchemaDirChangedOnBase(ctx context.Context, repo, baseRef, headSHA, schemaPath string) (changed bool, changedFiles []string, err error) {
	owner, repoName := splitRepo(repo)

	comparison, err := retryGitHubUnavailableRead(ctx, ic.logger, "compare for merge base",
		[]any{"repo", repo, "base", baseRef, "head", headSHA},
		func(ctx context.Context) (*gh.CommitsComparison, error) {
			cmp, _, cmpErr := ic.client.Repositories.CompareCommits(ctx, owner, repoName, baseRef, headSHA, nil)
			if cmpErr != nil {
				return nil, fmt.Errorf("compare commits %s...%s: %w", baseRef, headSHA, classifyGitHubAPIError(cmpErr))
			}
			return cmp, nil
		})
	if err != nil {
		return false, nil, fmt.Errorf("resolve merge base %s...%s in %s: %w", baseRef, headSHA, repo, err)
	}
	if comparison == nil || comparison.MergeBaseCommit == nil || comparison.MergeBaseCommit.GetSHA() == "" {
		return false, nil, fmt.Errorf("resolve merge base %s...%s in %s: comparison returned no merge base", baseRef, headSHA, repo)
	}
	mergeBase := comparison.MergeBaseCommit.GetSHA()

	// Pin the live base tip from the compare response (the tip of baseRef at the
	// time of this call), not the caller's snapshot SHA. Fail closed if absent.
	if comparison.BaseCommit == nil || comparison.BaseCommit.GetSHA() == "" {
		return false, nil, fmt.Errorf("resolve base tip %s...%s in %s: comparison returned no base commit", baseRef, headSHA, repo)
	}
	baseSHA := comparison.BaseCommit.GetSHA()

	// The current base is already an ancestor of head: no base commits landed
	// since the branch diverged, so the schema directory cannot have changed on
	// base relative to what this PR is built on.
	if mergeBase == baseSHA {
		return false, nil, nil
	}

	mergeBaseTreeSHA, mergeBaseFound, err := ic.treeSHAForPath(ctx, repo, mergeBase, schemaPath)
	if err != nil {
		return false, nil, fmt.Errorf("resolve schema dir %q tree at merge base %s in %s: %w", schemaPath, mergeBase, repo, err)
	}
	baseTreeSHA, baseFound, err := ic.treeSHAForPath(ctx, repo, baseSHA, schemaPath)
	if err != nil {
		return false, nil, fmt.Errorf("resolve schema dir %q tree at base %s in %s: %w", schemaPath, baseSHA, repo, err)
	}

	// Base never had the directory (the PR introduces it): base did not change
	// a directory it does not contain.
	if !mergeBaseFound && !baseFound {
		return false, nil, nil
	}
	// Present on both sides with an identical snapshot: unchanged.
	if mergeBaseFound && baseFound && mergeBaseTreeSHA == baseTreeSHA {
		return false, nil, nil
	}

	// Differs, or the directory was added or removed on base since divergence.
	return true, ic.changedFilesUnderPath(ctx, repo, mergeBase, baseSHA, schemaPath), nil
}

// treeSHAForPath resolves the git tree SHA of the directory dir at ref. found is
// false when dir does not exist (or is not a directory) at ref. It walks the
// path one segment at a time with shallow tree fetches, so it works even when
// the repository's full recursive listing would be truncated. An empty or "."
// dir resolves to the ref's root tree SHA.
func (ic *InstallationClient) treeSHAForPath(ctx context.Context, repo, ref, dir string) (sha string, found bool, err error) {
	clean := strings.Trim(path.Clean(dir), "/")
	if clean == "" || clean == "." {
		root, rootErr := ic.rootTreeSHA(ctx, repo, ref)
		if rootErr != nil {
			return "", false, rootErr
		}
		return root, true, nil
	}

	treeSHA := ref
	for segment := range strings.SplitSeq(clean, "/") {
		entries, shallowErr := ic.fetchGitTreeShallow(ctx, repo, treeSHA)
		if shallowErr != nil {
			return "", false, fmt.Errorf("resolve %q under %q in repo %s ref %s: %w", segment, clean, repo, ref, shallowErr)
		}
		next := ""
		for _, entry := range entries {
			if entry.Path == segment && entry.Type == "tree" {
				next = entry.SHA
				break
			}
		}
		if next == "" {
			return "", false, nil
		}
		treeSHA = next
	}
	return treeSHA, true, nil
}

// rootTreeSHA returns the SHA of the root tree of the commit-ish ref.
func (ic *InstallationClient) rootTreeSHA(ctx context.Context, repo, ref string) (string, error) {
	owner, repoName := splitRepo(repo)
	tree, err := retryGitHubUnavailableRead(ctx, ic.logger, "fetch root tree",
		[]any{"repo", repo, "ref", ref},
		func(ctx context.Context) (*gh.Tree, error) {
			ghTree, _, treeErr := ic.client.Git.GetTree(ctx, owner, repoName, ref, false)
			if treeErr != nil {
				return nil, fmt.Errorf("fetch root tree: %w", classifyGitHubAPIError(treeErr))
			}
			return ghTree, nil
		})
	if err != nil {
		return "", fmt.Errorf("resolve root tree for repo %s ref %s: %w", repo, ref, err)
	}
	if tree.GetSHA() == "" {
		return "", fmt.Errorf("resolve root tree for repo %s ref %s: empty tree SHA", repo, ref)
	}
	return tree.GetSHA(), nil
}

// changedFilesUnderPath returns, best-effort, the files under schemaPath that
// changed between base and head, sorted and deduplicated. It is used only for
// operator-facing display, so any failure (including a truncated compare
// response) yields an empty list rather than an error: the change decision has
// already been made from tree SHAs by the caller.
func (ic *InstallationClient) changedFilesUnderPath(ctx context.Context, repo, base, head, schemaPath string) []string {
	files, err := ic.FetchChangedFilesBetween(ctx, repo, base, head)
	if err != nil {
		ic.logger.Warn("could not list changed schema files for base-drift downgrade; showing a generic message",
			"repo", repo, "base", base, "head", head, "schema_path", schemaPath, "error", err)
		return nil
	}

	seen := make(map[string]struct{}, len(files))
	var under []string
	for _, f := range files {
		for _, name := range []string{f.Filename, f.PreviousFilename} {
			if name == "" || !pathWithinDir(schemaPath, name) {
				continue
			}
			if _, dup := seen[name]; dup {
				continue
			}
			seen[name] = struct{}{}
			under = append(under, name)
		}
	}
	slices.Sort(under)
	return under
}

// pathWithinDir reports whether the file path p is dir itself or lives beneath
// dir, matching on path-segment boundaries so "schema/foo" does not match
// "schema/foobar". An empty or "." dir means the repository root, which contains
// every path.
func pathWithinDir(dir, p string) bool {
	cleanDir := strings.Trim(path.Clean(dir), "/")
	cleanPath := strings.Trim(path.Clean(p), "/")
	if cleanDir == "" || cleanDir == "." {
		return true
	}
	return cleanPath == cleanDir || strings.HasPrefix(cleanPath, cleanDir+"/")
}
