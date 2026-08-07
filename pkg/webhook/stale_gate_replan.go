package webhook

import (
	"context"
	"time"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// replanWorkTimeout bounds the GitHub and discovery work a re-plan performs,
// matching the budget every other auto-plan entry point runs under.
const replanWorkTimeout = 30 * time.Second

// replanAfterTerminalApplyOnSupersededHead re-plans the PR on its current
// commit when a terminal apply's stored check row is pinned to a superseded
// one, and reports whether the re-plan ran.
//
// A row recorded for another commit contributes a blocking placeholder to the
// aggregate (normalizeStaleContributions): results computed for one commit say
// nothing about another, so the gate stays shut. Nothing else re-keys that row.
// The push that moved the head left it alone because an apply still owned it,
// and the fold that follows the apply reaching a terminal state re-publishes
// the same placeholder over the same stale row. A plan on the current commit is
// the one action that records results for it, so without this the gate stays
// shut on a PR that may already be mergeable and the only exit is an operator
// posting the command by hand.
//
// It refuses while any apply on the PR is non-terminal: a started apply is
// authoritative for its PR's check state, and replaying the auto-plan flow over
// one could replace an apply-owned merge block with a fresh passing plan.
// Storage uncertainty refuses for the same reason — without the apply rows
// there is no proof the PR is safe to re-plan. Both refusals leave the gate
// blocking, which is the safe direction.
func (h *Handler) replanAfterTerminalApplyOnSupersededHead(ctx context.Context, client *ghclient.InstallationClient, a *storage.Apply, storedHeadSHA string) bool {
	prInfo, err := client.FetchPullRequest(ctx, a.Repository, a.PullRequest)
	if err != nil {
		h.logger.Warn("merge gate may stay blocked on a superseded commit: cannot read the PR head after a terminal apply",
			append(a.LogAttrs(), "stored_head_sha", storedHeadSHA, "error", err)...)
		return false
	}
	if prInfo.HeadSHA == storedHeadSHA {
		return false
	}
	if prInfo.IsClosed() {
		// Close-time cleanup owns a closed PR's check state; re-planning here
		// would resurrect what cleanup settled.
		h.logger.Info("skipping re-plan for a terminal apply on a superseded commit: the PR is closed and close-time cleanup owns its check state",
			append(a.LogAttrs(), "stored_head_sha", storedHeadSHA, "current_head_sha", prInfo.HeadSHA)...)
		return false
	}

	applies, err := h.service.Storage().Applies().GetByPR(ctx, a.Repository, a.PullRequest)
	if err != nil {
		h.logger.Warn("merge gate may stay blocked on a superseded commit: cannot confirm no apply is still in flight before re-planning",
			append(a.LogAttrs(), "stored_head_sha", storedHeadSHA, "current_head_sha", prInfo.HeadSHA, "error", err)...)
		return false
	}
	for _, other := range applies {
		if !state.IsTerminalApplyState(other.State) {
			h.logger.Info("skipping re-plan for a terminal apply on a superseded commit: another apply on this PR is still in flight and owns its check state",
				append(other.LogAttrs(), "stored_head_sha", storedHeadSHA, "current_head_sha", prInfo.HeadSHA)...)
			return false
		}
	}

	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:    "aggregate_superseded_head_replan",
		Repository:   a.Repository,
		Database:     a.Database,
		DatabaseType: a.DatabaseType,
		Environment:  a.Environment,
		Status:       "success",
	})
	h.logger.Info("re-planning on the current commit so the merge gate re-opens: a terminal apply left its stored check state pinned to a superseded commit",
		append(a.LogAttrs(), "stored_head_sha", storedHeadSHA, "current_head_sha", prInfo.HeadSHA)...)

	// The apply that triggered this has already reached a terminal state, so the
	// re-plan must outlive whatever context settled it while keeping tracing
	// values.
	planCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), replanWorkTimeout)
	defer cancel()
	planCtx = ghclient.WithPRInfoCache(planCtx)

	// Discovery failures are already logged and posted as a failing check inside
	// runAutoPlanForPR, which leaves the gate blocking either way.
	message, _ := h.runAutoPlanForPR(planCtx, client, a.Repository, a.PullRequest, prInfo.HeadSHA,
		a.InstallationID, replanSourceSupersededHead, replanSourceSupersededHead, "", "")
	h.logger.Info("re-plan for a superseded commit finished",
		append(a.LogAttrs(), "current_head_sha", prInfo.HeadSHA, "result", message)...)
	return true
}

// replanSourceSupersededHead names this re-plan in auto-plan's source and action
// fields so its comments and logs are attributable to the gate re-opening rather
// than to a push or an operator command.
const replanSourceSupersededHead = "apply.superseded_head"
