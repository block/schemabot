package webhook

import (
	"context"
	"fmt"

	"github.com/block/schemabot/pkg/state"
)

// BackfillPRCheckRuns recreates a PR's SchemaBot Check Runs by replaying the
// auto-plan flow against the PR's current head, exactly as the check-creating
// webhook delivery would have — a PR with no managed schema changes gets its
// passing aggregate, a PR with schema changes gets real auto-plans, and the
// leader/participant aggregate routing applies unchanged. Serves the checks
// operator endpoints for PRs whose delivery was never sent (for example PRs
// opened before check enablement) or is no longer retained by GitHub.
// A PR with a non-terminal apply is refused: the started apply remains
// authoritative for the PR's check state until it reaches a terminal state.
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
	// A started apply is authoritative for its PR's check state: replaying the
	// auto-plan flow over it could replace an apply-owned merge block with a
	// fresh passing plan. Refuse while any apply for the PR is non-terminal —
	// the operator reconciles the apply first, then backfills. Storage
	// uncertainty refuses too: without the apply rows there is no proof the PR
	// is safe to re-plan.
	applies, err := h.service.Storage().Applies().GetByPR(ctx, repo, pr)
	if err != nil {
		return "", fmt.Errorf("look up applies before backfilling Check Runs for %s#%d: %w", repo, pr, err)
	}
	for _, apply := range applies {
		if !state.IsTerminalApplyState(apply.State) {
			h.logger.Info("Check Run backfill refusing PR with a non-terminal apply", apply.LogAttrs()...)
			return fmt.Sprintf("skipped: apply %s is %s; backfill will not re-plan over a started apply", apply.ApplyIdentifier, apply.State), nil
		}
	}
	h.logger.Info("backfilling Check Runs via the auto-plan flow",
		"repo", repo, "pr", pr, "head_sha", prInfo.HeadSHA)
	message := h.runAutoPlanForPR(ctx, client, repo, pr, prInfo.HeadSHA, installationID, "checks.backfill", "checks.backfill", "", "")
	return message, nil
}
