package webhook

import (
	"context"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// assertPlanStillCurrent enforces the cross-delivery freshness invariant for
// apply-confirm: the stored plan that the user is about to confirm must have
// been rendered against the same commit that is currently the PR HEAD.
//
// PR #134's assertSchemaStillCurrent helper closes within-delivery races
// (HEAD advances between discovery and execution inside one webhook handler).
// It cannot see the cross-delivery race: HEAD advances while the user is
// reading the posted confirmation plan, before they click apply-confirm.
// At confirm time both ends of the within-delivery comparison see the new
// HEAD, so the within-delivery guard passes — but the plan the user reviewed
// was rendered for the older commit.
//
// This helper closes that gap by comparing the stored plan's HeadSHA (durable
// record of "which commit did the user actually review") against the fresh
// PR HEAD obtained via FetchPullRequestNoCache.
//
// Caller contract:
//   - plan must be the stored plan record loaded for this PR+env+database
//     (typically the most recent plan from PlanStore.GetByPR, filtered to the
//     target env+database).
//   - freshHeadSHA must come from FetchPullRequestNoCache — the cached fetch
//     would return the confirm-time discovery SHA and defeat the comparison.
//
// Returns true to mean "rejected — caller must release the lock and stop".
// Returns false when the plan is still current and execution may proceed.
//
// Skip semantics:
//   - plan == nil: no stored plan for this PR+env+database. The cross-delivery
//     invariant cannot be evaluated; let the existing handler logic surface
//     the missing-plan condition. Logged at debug level so the branch is not
//     silent (per AGENTS.md "no silent branch cases").
//   - plan.HeadSHA == "": plan row predates this column. Skip rather than
//     fail closed during the rollout window so in-flight plans created before
//     the deploy do not suddenly reject. Logged at debug level.
//
// Both skip cases are deliberately conservative; see the PR description for
// the trade-off and the case for switching to fail-closed in a follow-up.
func (h *Handler) assertPlanStillCurrent(
	ctx context.Context,
	repo string,
	pr int,
	installationID int64,
	plan *storage.Plan,
	freshHeadSHA string,
	environment string,
	requestedBy string,
) bool {
	if plan == nil {
		h.logger.Debug("cross-delivery plan-freshness check skipped: no stored plan",
			"repo", repo,
			"pr", pr,
			"environment", environment,
			"current_sha", freshHeadSHA,
		)
		return false
	}
	if plan.HeadSHA == "" {
		h.logger.Debug("cross-delivery plan-freshness check skipped: stored plan has no head_sha (legacy row)",
			"repo", repo,
			"pr", pr,
			"environment", environment,
			"database", plan.Database,
			"plan_identifier", plan.PlanIdentifier,
			"current_sha", freshHeadSHA,
		)
		return false
	}
	if plan.HeadSHA == freshHeadSHA {
		return false
	}

	h.logger.Warn("rejected: confirmation plan is stale, PR HEAD advanced since plan was posted",
		"repo", repo,
		"pr", pr,
		"environment", environment,
		"database", plan.Database,
		"database_type", plan.DatabaseType,
		"plan_identifier", plan.PlanIdentifier,
		"plan_sha", plan.HeadSHA,
		"current_sha", freshHeadSHA,
		"requested_by", requestedBy,
	)

	metrics.RecordStalePlanRejected(ctx, environment)

	h.postComment(repo, pr, installationID, templates.RenderStalePlanRejection(templates.StalePlanRejectionData{
		RequestedBy: requestedBy,
		Database:    plan.Database,
		Environment: environment,
		PlanSHA:     plan.HeadSHA,
		CurrentSHA:  freshHeadSHA,
	}))

	return true
}

// confirmationPlanForLock loads the plan that the active lock was acquired
// with — the apply-confirmation plan the human reviewed before clicking
// apply-confirm. Returns nil when the lock predates this column (empty
// PendingPlanID) or when the referenced plan row is gone; assertPlanStillCurrent
// then skips the freshness check rather than failing closed.
//
// Looking up the plan via lock.PendingPlanID (instead of "newest plan for
// repo+pr+env+database") avoids picking up a later plain `schemabot plan`
// that landed in the same plans table after the confirmation plan was posted,
// which would make a stale confirmation comment look current.
func (h *Handler) confirmationPlanForLock(ctx context.Context, lock *storage.Lock) (*storage.Plan, error) {
	if lock == nil || lock.PendingPlanID == "" {
		return nil, nil
	}
	return h.service.Storage().Plans().Get(ctx, lock.PendingPlanID)
}
