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
// assertSchemaStillCurrent closes within-delivery races (HEAD advances between
// discovery and execution inside one webhook handler). It cannot see the
// cross-delivery race: HEAD advances while the user is reading the posted
// confirmation plan, before they click apply-confirm. At confirm time both
// ends of the within-delivery comparison see the new HEAD, so the
// within-delivery guard passes — but the plan the user reviewed was rendered
// for the older commit.
//
// This helper closes that gap by comparing the stored plan's HeadSHA (durable
// record of "which commit did the user actually review") against the fresh
// PR HEAD obtained via FetchPullRequestNoCache.
//
// Caller contract:
//   - plan is the plan the pending confirmation pins (loaded by the lock's
//     PendingPlanID, never "the newest plan for this PR"). It must not be nil:
//     a pending confirmation with no loadable plan cannot attest what the
//     operator reviewed, and the caller rejects it before this check runs.
//   - freshHeadSHA must come from FetchPullRequestNoCache — the cached fetch
//     would return the confirm-time discovery SHA and defeat the comparison.
//
// Returns true to mean "rejected — caller must release the lock and stop".
// Returns false when the plan is still current and execution may proceed.
//
// A plan whose HeadSHA is empty predates the column and is allowed through
// rather than failed closed, so a confirmation that was pending when the
// column arrived does not suddenly reject. The skip is logged at debug level.
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

// confirmationPlanTargetsOtherEnvironment reports whether the pending
// confirmation was planned for an environment other than the one this
// apply-confirm names. The lock is keyed by database alone, so the plan it pins
// is the only record of which environment the operator reviewed and which
// environment passed the ordering gate when that plan was made. The caller
// rejects a confirmation with no loadable plan before asking this question.
func confirmationPlanTargetsOtherEnvironment(plan *storage.Plan, environment string) bool {
	return plan.Environment != environment
}

// confirmationPlanForLock loads the plan that the active lock was acquired
// with — the apply-confirmation plan the human reviewed before clicking
// apply-confirm. Returns nil when the lock predates this column (empty
// PendingPlanID) or when the referenced plan row is gone; the caller rejects
// that unverified confirmation while preserving its lock.
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
