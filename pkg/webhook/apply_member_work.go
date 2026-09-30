package webhook

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// rolloutStillPending reports whether a rollout whose reviewed primary is
// already at the desired schema still has something standing between it and
// done: a member that needs the change, or a member whose plan could not be
// confirmed.
//
// An empty primary plan speaks only for the primary. A member planned against a
// schema of its own can still need the change, and a member expected to mirror
// the primary can have drifted from it, so an apply cannot conclude there is
// nothing to do until the rollout round has planned every member.
func rolloutStillPending(outcome reviewDriftOutcome) bool {
	return outcome.blocks() || outcome.work.pending > 0
}

// rolloutRunsMemberWork reports whether a PR apply whose reviewed target is
// already at the desired schema runs the other targets' own plans: the rollout
// round passed its contract, some target still has work, and the comment the
// operator confirms renders every target's plan. A pending rollout that fails
// any of the three is refused instead, since the apply would otherwise run
// statements the comment never showed (RV-1).
func rolloutRunsMemberWork(outcome reviewDriftOutcome, preview *templates.DeploymentDriftData) bool {
	return !outcome.blocks() && outcome.work.pending > 0 && templates.RendersTargetPlans(preview)
}

// refusePendingRollout answers an apply whose reviewed target is already at the
// desired schema while the rest of the rollout is not known to be, and the apply
// cannot run what is pending: drift blocks, or the pending work is not what the
// operator was shown.
//
// The apply does not run, and the check records what is pending so the PR
// cannot merge as if every target were up to date (MG-12).
func (h *Handler) refusePendingRollout(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, schemaResult *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment, requestedBy string, actionName string, outcome reviewDriftOutcome) {
	h.refuseRollout(ctx, client, repo, pr, installationID, schemaResult, planResp, environment, requestedBy, actionName, outcome, pendingRolloutMessage(outcome))
}

// refuseRollout refuses an apply whose reviewed target is already at the
// desired schema, recording what is pending on the check before posting message.
func (h *Handler) refuseRollout(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, schemaResult *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment, requestedBy string, actionName string, outcome reviewDriftOutcome, message string) {
	h.logger.Info("apply refused: the reviewed target is already at the desired schema but the rest of the rollout is not",
		"repo", repo, "pr", pr, "database", schemaResult.Database, "database_type", schemaResult.Type,
		"environment", environment, "action", actionName, "plan_id", planResp.PlanID,
		"drift_blocked", outcome.blocks(), "targets_pending", outcome.work.pending, "targets", outcome.work.members,
		"pending_targets", outcome.work.names, "refusal", message)
	headSHA, err := h.storePlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, environment, outcome)
	switch {
	case err != nil:
		h.logger.Error("failed to record the pending rollout on the check; publishing a failing aggregate from the rollout round instead",
			"repo", repo, "pr", pr, "head_sha", schemaResult.HeadSHA, "database", schemaResult.Database, "database_type", schemaResult.Type,
			"environment", environment, "error", err)
		h.failClosedOnUnstoredRollout(ctx, client, repo, pr, schemaResult.HeadSHA, environment, outcome)
	case headSHA != "":
		h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
	}
	h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy, message)
}

// memberWorkRefusal returns why the other targets' work in a rollout cannot run
// from a PR apply whose reviewed target is already at the desired schema, or ""
// when it can. planID names the round's reviewed plan.
//
// The apply runs each member's stored plan, and the comment the operator
// confirms renders those plans' statements and nothing else. So work the apply
// cannot run as planned, and anything whose consent rests on a disclosure that
// comment does not carry, refuses here: an unsafe or direct-execution change, or
// an unfinished copy the apply would discard. It is asked before the apply
// pauses, so a refusal never pins a confirmation that could not succeed, and
// again at confirm against the rollout as it is then.
func (h *Handler) memberWorkRefusal(ctx context.Context, planID, environment string, rollout reviewDriftOutcome) (string, error) {
	if rollout.work.copyAtStake != "" {
		return rollout.work.copyAtStake, nil
	}
	plan, err := h.service.Storage().Plans().Get(ctx, planID)
	if err != nil {
		return "", fmt.Errorf("load reviewed plan %s: %w", planID, err)
	}
	if plan == nil {
		return "", fmt.Errorf("reviewed plan %s was not stored", planID)
	}
	members, err := h.service.MemberPlansForReviewRound(ctx, plan, environment)
	if err != nil {
		return "", fmt.Errorf("load member plans of round %s: %w", planID, err)
	}
	for _, member := range slices.Sorted(maps.Keys(members)) {
		memberPlan := members[member]
		if !memberPlan.HasWork() {
			continue
		}
		if reason := api.MemberWorkAConvergedReviewedPlanCannotRun(memberPlan); reason != "" {
			return fmt.Sprintf("target %s: its plan %s", member, reason), nil
		}
	}
	return "", nil
}

// memberWorkRefusalMessage tells the operator why the other targets' work was
// not run. The refusal names only targets, tables, and namespaces.
func memberWorkRefusalMessage(refusal string) string {
	return fmt.Sprintf("The reviewed target already has this schema, but %s. A PR apply whose reviewed target is already at the desired schema cannot run that or disclose it for confirmation, so nothing was applied. The schema check keeps blocking merge until every target has the change.", refusal)
}

// pauseForMemberWorkConfirmation holds the lock this apply acquired for an
// apply-confirm, when the reviewed target is already at the desired schema and
// other targets still have work.
//
// The check is stored before the comment is posted, from the rollout round, so
// the pending work keeps the merge gate blocked whatever the operator decides
// (MG-12). A check that cannot be stored releases the lock and publishes a
// failing aggregate from the round instead, and the command stays retryable:
// the pause is never acknowledged over check state that could still read as
// passing.
func (h *Handler) pauseForMemberWorkConfirmation(
	ctx context.Context, client *ghclient.InstallationClient,
	repo string, pr int, installationID int64, schemaResult *ghclient.SchemaRequestResult,
	planResp *apitypes.PlanResponse, environment, requestedBy string, result CommandResult,
	rollout reviewDriftOutcome, commentData templates.PlanCommentData,
) (bool, error) {
	database, dbType := schemaResult.Database, schemaResult.Type
	h.logger.Info("apply paused for confirmation: only targets other than the reviewed one have work",
		"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment,
		"plan_id", planResp.PlanID, "pending_targets", rollout.work.names)
	headSHA, checkErr := h.storePlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, environment, rollout)
	if checkErr != nil {
		h.logger.Error("failed to store check state for the member-work confirmation; releasing the lock and publishing a failing aggregate from the rollout round",
			"repo", repo, "pr", pr, "head_sha", schemaResult.HeadSHA, "database", database, "database_type", dbType,
			"environment", environment, "plan_id", planResp.PlanID, "error", checkErr)
		h.releaseApplyLockIfIntentUnchanged(ctx, repo, pr, database, dbType, environment, planResp.PlanID, "member-work confirmation check state store failure")
		h.failClosedOnUnstoredRollout(ctx, client, repo, pr, schemaResult.HeadSHA, environment, rollout)
		if !result.SuppressRetryComments {
			h.postCommandError(repo, pr, installationID, action.Apply, environment, requestedBy,
				"SchemaBot could not record the check state for this apply. Retry the command, and see server logs if it persists.")
		}
		return true, fmt.Errorf("apply command member-work confirmation check record %s#%d: %w", repo, pr, checkErr)
	}
	if headSHA != "" {
		h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
	}
	commentData.PendingManualConfirmation = true
	commentData.PausedApplyCause = &templates.PausedApplyCauseData{
		Heading: "The reviewed target already has this schema",
		Remedy: "Nothing has run. Confirming runs each target's own plan shown above; " +
			"a target already at the desired schema runs nothing.",
	}
	if postErr := h.postPendingConfirmation(ctx, repo, pr, installationID, database, dbType, environment, planResp.PlanID,
		templates.RenderPlanComment(commentData), "member-work confirmation disclosure post failure"); postErr != nil {
		return true, fmt.Errorf("apply command member-work confirmation disclosure %s#%d: %w", repo, pr, postErr)
	}
	return false, nil
}

// failClosedOnUnstoredRollout publishes a failing aggregate for an environment
// whose plan check record could not be stored after the rollout round proved
// its check must not pass: drift blocks, or some member has work, the primary
// included. Refreshing the aggregate instead would recompute it from the stored
// row, which can still be a pass recorded before the pending work was found
// (MG-1, MG-12).
func (h *Handler) failClosedOnUnstoredRollout(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA, environment string, outcome reviewDriftOutcome) {
	if headSHA == "" {
		h.logger.Warn("the pending rollout was not stored and no head SHA is known; the fallback failing aggregate was not posted, so an operator must re-run plan to re-establish the merge-gate block",
			"repo", repo, "pr", pr, "environment", environment)
		return
	}
	if outcome.blocks() {
		h.postFailingAggregatesWithBlock(ctx, client, repo, pr, headSHA, map[string]string{environment: outcome.summary}, reviewTimeDeploymentDriftBlock)
		return
	}
	h.postFailingAggregates(ctx, client, repo, pr, headSHA, map[string]string{environment: outcome.work.unstoredSummary()})
}

// pendingRolloutMessage explains a refused apply to the operator. The drift
// summary names only configured members and is already clamped for markdown,
// so it is safe to render; the raw causes stay in the server logs.
func pendingRolloutMessage(outcome reviewDriftOutcome) string {
	if outcome.blocks() {
		return fmt.Sprintf("The reviewed target already has this schema, but SchemaBot could not confirm that the other targets do (%s), so nothing was applied. The schema check stays failing until every target is confirmed.", outcome.summary)
	}
	return fmt.Sprintf("The reviewed target already has this schema, but %s: %s. The plans those targets would run were not on the comment this apply acts on, so nothing was applied. Run apply again for this environment to review and confirm each target's own plan.", outcome.work.summary(), strings.Join(outcome.work.names, ", "))
}

// confirmationCoversMemberWork reports whether the pending confirmation an
// apply-confirm acts on was given against the member work it is about to run,
// with a reason for the log when it was not.
//
// Only one comment pins an empty reviewed plan: the one the apply command posts
// when the reviewed target is already at the desired schema and other targets
// still have work, and that comment renders every target's plan. A pinned plan
// with work of its own was confirmed against a comment that showed the reviewed
// target's plan alone, so no other target's work was on it.
//
// The members are planned again at confirm, and a target's schema can change in
// between. So each member with work now must have been planned with the same
// statements in the confirmed round, and a statement the confirmed comment did
// not show never runs on the strength of that confirmation.
func (h *Handler) confirmationCoversMemberWork(ctx context.Context, pinnedPlanID, currentPlanID, environment string) (bool, string, error) {
	plans := h.service.Storage().Plans()
	pinned, err := plans.Get(ctx, pinnedPlanID)
	if err != nil {
		return false, "", fmt.Errorf("load confirmed plan %s: %w", pinnedPlanID, err)
	}
	if pinned == nil {
		return false, "the confirmed plan no longer exists", nil
	}
	if pinned.HasWork() {
		return false, "the confirmed plan has work on the reviewed target, so its comment showed no other target's plan", nil
	}
	current, err := plans.Get(ctx, currentPlanID)
	if err != nil {
		return false, "", fmt.Errorf("load confirm-time plan %s: %w", currentPlanID, err)
	}
	if current == nil {
		return false, "", fmt.Errorf("confirm-time plan %s was not stored", currentPlanID)
	}
	confirmed, err := h.service.MemberPlansForReviewRound(ctx, pinned, environment)
	if err != nil {
		return false, "", fmt.Errorf("load member plans of the confirmed round: %w", err)
	}
	now, err := h.service.MemberPlansForReviewRound(ctx, current, environment)
	if err != nil {
		return false, "", fmt.Errorf("load member plans of the confirm-time round: %w", err)
	}
	for _, member := range slices.Sorted(maps.Keys(now)) {
		plan := now[member]
		if !plan.HasWork() {
			continue
		}
		was, ok := confirmed[member]
		if !ok {
			return false, fmt.Sprintf("target %s has work the confirmed round did not plan", member), nil
		}
		if !sameMemberWork(was, plan) {
			return false, fmt.Sprintf("target %s would run statements the confirmed round did not plan", member), nil
		}
	}
	return true, "", nil
}

// confirmedPlanHasNoWork reports whether the plan a pending confirmation is
// pinned to has no work of its own, which only the confirmation of an apply
// whose reviewed target was already at the desired schema is. A pinned plan
// that no longer loads is an error: whether its comment showed the reviewed
// target's changes cannot be told.
func (h *Handler) confirmedPlanHasNoWork(ctx context.Context, pinnedPlanID string) (bool, error) {
	pinned, err := h.service.Storage().Plans().Get(ctx, pinnedPlanID)
	if err != nil {
		return false, fmt.Errorf("load confirmed plan %s: %w", pinnedPlanID, err)
	}
	if pinned == nil {
		return false, fmt.Errorf("confirmed plan %s no longer exists", pinnedPlanID)
	}
	return !pinned.HasWork(), nil
}

// sameMemberWork reports whether two plans for one member run the same
// statements. It compares what the operator reads on the comment, the table
// changes and each shard's own changes, byte for byte: a difference in spelling
// alone refuses too, which only ever sends the operator back to review.
func sameMemberWork(a, b *storage.Plan) bool {
	if !slices.EqualFunc(a.FlatDDLChanges(), b.FlatDDLChanges(), sameTableChange) {
		return false
	}
	if !slices.Equal(a.FinalizerNamespaces(), b.FinalizerNamespaces()) {
		return false
	}
	return slices.EqualFunc(a.Shards, b.Shards, func(x, y storage.ShardPlan) bool {
		return x.Namespace == y.Namespace && x.Shard == y.Shard && slices.EqualFunc(x.Changes, y.Changes, sameTableChange)
	})
}

func sameTableChange(a, b storage.TableChange) bool {
	return a.Namespace == b.Namespace && a.Table == b.Table && a.Operation == b.Operation && a.DDL == b.DDL && a.ExecutionMode == b.ExecutionMode
}
