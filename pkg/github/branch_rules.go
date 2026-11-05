package github

import (
	"context"
	"fmt"

	gh "github.com/google/go-github/v86/github"

	"github.com/block/schemabot/pkg/metrics"
)

// BaseBranchDismissesStaleApprovals reports whether GitHub dismisses a PR's
// approvals when new commits are pushed to it, for PRs targeting branch. That
// holds when an active ruleset that applies to the branch, or the branch's
// classic protection, turns on dismissal of stale approvals.
//
// It returns false with a nil error only when GitHub answered both reads and
// neither turns dismissal on. An error means GitHub could not say, and the
// caller must not assume dismissal. Reading classic protection needs the
// App's Administration read permission; without it, the read fails unless a
// ruleset already turns dismissal on.
func (ic *InstallationClient) BaseBranchDismissesStaleApprovals(ctx context.Context, repo, branch string) (bool, error) {
	owner, repoName := splitRepo(repo)
	logAttrs := []any{"repo", repo, "branch", branch}

	rulesCtx := withGitHubRateLimitContext(ctx, metrics.GitHubOperationGetBranchRules, repo)
	opts := &gh.ListOptions{PerPage: 100}
	for {
		var resp *gh.Response
		rules, err := retryGitHubUnavailableRead(rulesCtx, ic.logger, "get branch rules", logAttrs, func(ctx context.Context) (*gh.BranchRules, error) {
			rules, rulesResp, rulesErr := ic.client.Repositories.GetRulesForBranch(ctx, owner, repoName, branch, opts)
			if rulesErr != nil {
				return nil, classifyGitHubAPIError(rulesErr)
			}
			resp = rulesResp
			return rules, nil
		})
		if err != nil {
			return false, fmt.Errorf("get rules for branch %q of %s: %w", branch, repo, err)
		}
		for _, rule := range rules.PullRequest {
			if rule.Parameters.DismissStaleReviewsOnPush {
				return true, nil
			}
		}
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}

	protectionCtx := withGitHubRateLimitContext(ctx, metrics.GitHubOperationGetPullRequestReviewProtection, repo)
	enforcement, err := retryGitHubUnavailableRead(protectionCtx, ic.logger, "get pull request review protection", logAttrs, func(ctx context.Context) (*gh.PullRequestReviewsEnforcement, error) {
		enforcement, _, enforcementErr := ic.client.Repositories.GetPullRequestReviewEnforcement(ctx, owner, repoName, branch)
		if enforcementErr != nil {
			return nil, classifyGitHubAPIError(enforcementErr)
		}
		return enforcement, nil
	})
	if err != nil {
		// GitHub answers 404 when the branch has no classic protection, or
		// its protection does not require reviews, so nothing dismisses.
		if IsNotFoundError(err) {
			return false, nil
		}
		return false, fmt.Errorf("get pull request review protection for branch %q of %s: %w", branch, repo, err)
	}
	return enforcement.DismissStaleReviews, nil
}
