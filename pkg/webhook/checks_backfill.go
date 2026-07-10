package webhook

import (
	"context"
	"fmt"
)

// BackfillPRCheckRuns recreates a PR's SchemaBot Check Runs by replaying the
// auto-plan flow against the PR's current head, exactly as the check-creating
// webhook delivery would have — a PR with no managed schema changes gets its
// passing aggregate, a PR with schema changes gets real auto-plans, and the
// leader/participant aggregate routing applies unchanged. Serves the checks
// operator endpoints for PRs whose delivery was never sent (for example PRs
// opened before check enablement) or is no longer retained by GitHub.
func (h *Handler) BackfillPRCheckRuns(ctx context.Context, repo string, pr int, installationID int64) (string, error) {
	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		return "", fmt.Errorf("create GitHub client to backfill Check Runs for %s#%d: %w", repo, pr, err)
	}
	prInfo, err := client.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		return "", fmt.Errorf("fetch PR to backfill Check Runs for %s#%d: %w", repo, pr, err)
	}
	if prInfo.IsClosed() {
		// Close-time cleanup owns a closed PR's check state; recreating Check
		// Runs there would resurrect what cleanup settled.
		h.logger.Info("Check Run backfill skipping closed PR", "repo", repo, "pr", pr)
		return "skipped: PR is closed", nil
	}
	h.logger.Info("backfilling Check Runs via the auto-plan flow",
		"repo", repo, "pr", pr, "head_sha", prInfo.HeadSHA)
	message := h.runAutoPlanForPR(ctx, client, repo, pr, prInfo.HeadSHA, installationID, "checks.backfill", "checks.backfill", "", "")
	return message, nil
}
