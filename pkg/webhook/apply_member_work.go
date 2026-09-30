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

// rolloutRunsMemberWork reports whether a PR apply runs other targets' own
// plans alongside the reviewed one: the rollout round passed its contract, some
// target other than the reviewed one has work, and the comment the operator
// confirms renders every target's plan. Work on another target under any other
// shape is refused instead, since the apply would otherwise run statements the
// comment never showed (RV-1).
func rolloutRunsMemberWork(outcome reviewDriftOutcome, preview *templates.DeploymentDriftData) bool {
	return !outcome.blocks() && outcome.work.others > 0 && templates.RendersTargetPlans(preview)
}

// refusePendingRollout answers an apply that cannot run what the rollout has
// pending: drift blocks, the rest of the rollout is not known to be at the
// desired schema, or the other targets' work is not what the operator was shown.
// reviewedTargetConverged says whether the reviewed target itself was already at
// the desired schema, which decides what the operator is told.
//
// The apply does not run, and the check records what is pending so the PR
// cannot merge as if every target were up to date (MG-12).
func (h *Handler) refusePendingRollout(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, schemaResult *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment, requestedBy string, actionName string, outcome reviewDriftOutcome, reviewedTargetConverged bool) {
	h.refuseRollout(ctx, client, repo, pr, installationID, schemaResult, planResp, environment, requestedBy, actionName, outcome, reviewedTargetConverged, pendingRolloutMessage(outcome, reviewedTargetConverged))
}

// refuseRollout refuses an apply that cannot run what the rollout has pending,
// recording what is pending on the check before posting message.
func (h *Handler) refuseRollout(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, schemaResult *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment, requestedBy string, actionName string, outcome reviewDriftOutcome, reviewedTargetConverged bool, message string) {
	if err := h.recordPendingRollout(ctx, client, repo, pr, schemaResult, planResp, environment, outcome); err != nil {
		h.logger.Error("failed to record the pending rollout on the check; published a failing aggregate from the rollout round instead",
			"repo", repo, "pr", pr, "head_sha", schemaResult.HeadSHA, "database", schemaResult.Database, "database_type", schemaResult.Type,
			"environment", environment, "action", actionName, "reviewed_target_converged", reviewedTargetConverged, "error", err)
	}
	h.postRolloutRefusal(repo, pr, installationID, schemaResult, planResp, environment, requestedBy, actionName, outcome, reviewedTargetConverged, message)
}

// postRolloutRefusal tells the operator why an apply that cannot run what the
// rollout has pending did not run. The caller has already recorded the pending
// rollout on the check.
func (h *Handler) postRolloutRefusal(repo string, pr int, installationID int64, schemaResult *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment, requestedBy string, actionName string, outcome reviewDriftOutcome, reviewedTargetConverged bool, message string) {
	h.logger.Info("apply refused: the rollout has work this apply cannot run",
		"repo", repo, "pr", pr, "database", schemaResult.Database, "database_type", schemaResult.Type,
		"environment", environment, "action", actionName, "plan_id", planResp.PlanID,
		"reviewed_target_converged", reviewedTargetConverged,
		"drift_blocked", outcome.blocks(), "targets_pending", outcome.work.pending, "targets", outcome.work.members,
		"pending_targets", outcome.work.names, "refusal", message)
	h.postCommandError(repo, pr, installationID, actionName, environment, requestedBy, message)
}

// recordPendingRollout stores the check record for a rollout with work pending
// on some target, and refreshes the aggregate from it, so the PR cannot merge
// as if every target were up to date (MG-12). A record that cannot be stored
// publishes a failing aggregate from the rollout round instead, since
// refreshing would recompute from a stored row that can still be a pass
// (MG-1), and the error is returned.
func (h *Handler) recordPendingRollout(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, schemaResult *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment string, outcome reviewDriftOutcome) error {
	headSHA, err := h.storePlanCheckRecord(ctx, client, repo, pr, schemaResult, planResp, environment, outcome)
	if err != nil {
		h.failClosedOnUnstoredRollout(ctx, client, repo, pr, schemaResult.HeadSHA, environment, outcome)
		return fmt.Errorf("store the pending rollout's check record for %s#%d environment %s database %s: %w", repo, pr, environment, schemaResult.Database, err)
	}
	if headSHA != "" {
		h.updateAggregateCheck(ctx, client, repo, pr, headSHA)
	}
	return nil
}

// memberWorkRefusal returns why the other targets' work in a rollout cannot run
// from a PR apply, or "" when it can. planID names the round's reviewed plan,
// and reviewedTargetConverged says whether that plan is empty.
//
// The apply runs each member's stored plan, and the comment the operator
// confirms renders those plans' statements and nothing else. So work the apply
// cannot run as planned, and anything whose consent rests on a disclosure that
// comment does not carry, refuses here: an unsafe change the reviewed plan's
// disclosure does not name, a direct-execution change, or an unfinished copy
// the apply would discard. It is asked before the apply pauses, so a refusal
// never pins a confirmation that could not succeed, and again at confirm against
// the rollout as it is then.
//
// A copy at stake refuses whether or not the reviewed target has work, since
// the comment discloses only the reviewed plan's discarded copies. The rest is
// what apply creation asks of each member, which depends on the reviewed plan:
// an empty one can carry only per-member table work and discloses nothing
// (api.MemberWorkAConvergedReviewedPlanCannotRun), and one with work holds each
// member to its shape and to its disclosure (api.MemberWorkTheReviewedPlanCannotRun).
func (h *Handler) memberWorkRefusal(ctx context.Context, planID, environment string, rollout reviewDriftOutcome, reviewedTargetConverged bool) (string, error) {
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
		if reason := memberWorkReviewedPlanCannotRun(plan, memberPlan, reviewedTargetConverged); reason != "" {
			return fmt.Sprintf("target %s: its plan %s", member, reason), nil
		}
	}
	return "", nil
}

// memberWorkReviewedPlanCannotRun asks of one member's plan what apply creation
// asks of it under the reviewed plan the apply is created from.
func memberWorkReviewedPlanCannotRun(reviewed, member *storage.Plan, reviewedTargetConverged bool) string {
	if reviewedTargetConverged {
		return api.MemberWorkAConvergedReviewedPlanCannotRun(member)
	}
	return api.MemberWorkTheReviewedPlanCannotRun(reviewed, member)
}

// annotateMemberApplyRefusal records on a plan comment why a PR apply cannot
// run the other targets' plans it renders, when its reviewed target is already
// at the desired schema. Such an apply is refused whatever its flags, so the
// comment must not offer it. A refusal that cannot be computed leaves the apply
// offered: the apply asks again before it pauses and refuses on the same
// grounds, so the comment only ever misses a shortcut, never a gate.
func (h *Handler) annotateMemberApplyRefusal(ctx context.Context, data *templates.PlanCommentData, planResp *apitypes.PlanResponse, environment string, rollout reviewDriftOutcome, repo string, pr int) {
	if planResp.HasChanges() || !rolloutRunsMemberWork(rollout, data.DeploymentDrift) {
		h.logger.Debug("plan comment renders no other targets' plans for a converged reviewed target; no member-work refusal to disclose",
			"repo", repo, "pr", pr, "database", planResp.Database, "environment", environment, "plan_id", planResp.PlanID)
		return
	}
	refusal, err := h.memberWorkRefusal(ctx, planResp.PlanID, environment, rollout, true)
	if err != nil {
		h.logger.Warn("could not tell whether a PR apply can run the other targets' plans; the plan comment offers the apply, which re-checks before it pauses",
			"repo", repo, "pr", pr, "database", planResp.Database, "environment", environment, "plan_id", planResp.PlanID, "error", err)
		return
	}
	data.MemberApplyRefusal = refusal
}

// memberWorkRefusalMessage tells the operator why the other targets' work was
// not run. The refusal names only targets, tables, and namespaces.
func memberWorkRefusalMessage(refusal string, reviewedTargetConverged bool) string {
	if reviewedTargetConverged {
		return fmt.Sprintf("The reviewed target already has this schema, but %s. A PR apply whose reviewed target is already at the desired schema cannot run that or disclose it for confirmation, so nothing was applied. The schema check keeps blocking merge until every target has the change.", refusal)
	}
	return fmt.Sprintf("Targets other than the reviewed one have plans of their own, but %s. A PR apply cannot run that or disclose it for confirmation, so nothing was applied. The schema check keeps blocking merge until every target has the change.", refusal)
}

// pauseForMemberWorkConfirmation holds the lock this apply acquired for an
// apply-confirm, when targets other than the reviewed one have work, whether or
// not the reviewed target has work of its own.
//
// The caller recorded the pending work on the check straight after the rollout
// round, before taking the lock, so the merge gate is already blocked whatever
// the operator decides and on every exit between that round and this pause
// (MG-12).
func (h *Handler) pauseForMemberWorkConfirmation(
	ctx context.Context,
	repo string, pr int, installationID int64, schemaResult *ghclient.SchemaRequestResult,
	planResp *apitypes.PlanResponse, environment string,
	rollout reviewDriftOutcome, commentData templates.PlanCommentData, reviewedTargetConverged bool,
) (bool, error) {
	database, dbType := schemaResult.Database, schemaResult.Type
	h.logger.Info("apply paused for confirmation: targets other than the reviewed one have work",
		"repo", repo, "pr", pr, "database", database, "database_type", dbType, "environment", environment,
		"plan_id", planResp.PlanID, "reviewed_target_converged", reviewedTargetConverged, "pending_targets", rollout.work.names)
	commentData.PendingManualConfirmation = true
	commentData.PausedApplyCause = &templates.PausedApplyCauseData{
		Heading: "Each target runs its own plan",
		Remedy: "Nothing has run. Confirming runs each target's own plan shown above; " +
			"a target already at the desired schema runs nothing.",
	}
	if reviewedTargetConverged {
		commentData.PausedApplyCause.Heading = "The reviewed target already has this schema"
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
func pendingRolloutMessage(outcome reviewDriftOutcome, reviewedTargetConverged bool) string {
	const rerun = "Run apply again for this environment to review and confirm each target's own plan."
	switch {
	case outcome.blocks() && reviewedTargetConverged:
		return fmt.Sprintf("The reviewed target already has this schema, but SchemaBot could not confirm that the other targets do (%s), so nothing was applied. The schema check stays failing until every target is confirmed.", outcome.summary)
	case outcome.blocks():
		return fmt.Sprintf("SchemaBot could not confirm the plan of every target (%s), so nothing was applied. The schema check stays failing until every target is confirmed.", outcome.summary)
	case reviewedTargetConverged:
		return fmt.Sprintf("The reviewed target already has this schema, but %s: %s. The plans those targets would run were not on the comment this apply acts on, so nothing was applied. %s", outcome.work.summary(), strings.Join(outcome.work.names, ", "), rerun)
	default:
		return fmt.Sprintf("%s: %s. The plans of the targets other than the reviewed one were not on the comment this apply acts on, so nothing was applied. %s", outcome.work.summary(), strings.Join(outcome.work.names, ", "), rerun)
	}
}

// confirmationCoversMemberWork reports whether the pending confirmation an
// apply-confirm acts on was given against the member work it is about to run,
// with a reason for the log when it was not.
//
// A pending confirmation covers another target's work only through the
// rollout round stored with the pinned plan. The apply command runs that round
// before pinning, and whenever another target has work it pauses on a comment
// that renders every target's plan, so a round whose members have work is one
// whose plans the operator was shown. A pin without such a round covers no other
// target's work.
//
// The targets are planned again at confirm, and a target's schema can change in
// between. So the reviewed target, when it still has work, and each member with
// work now must have been planned with the same statements in the confirmed
// round, and a statement the confirmed comment did not show never runs on the
// strength of that confirmation.
func (h *Handler) confirmationCoversMemberWork(ctx context.Context, pinnedPlanID, currentPlanID, environment string) (bool, string, error) {
	plans := h.service.Storage().Plans()
	pinned, err := plans.Get(ctx, pinnedPlanID)
	if err != nil {
		return false, "", fmt.Errorf("load confirmed plan %s: %w", pinnedPlanID, err)
	}
	if pinned == nil {
		return false, "the confirmed plan no longer exists", nil
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
	covered, reason := roundCoversWork(pinned, current, confirmed, now)
	return covered, reason, nil
}

// roundCoversWork reports whether the confirm-time round runs only statements
// the confirmed round planned, with a reason for the log when it does not. A
// target with no work now runs nothing, so only targets with work are compared.
func roundCoversWork(pinned, current *storage.Plan, confirmed, now map[string]*storage.Plan) (bool, string) {
	if current.HasWork() && !sameMemberWork(pinned, current) {
		return false, "the reviewed target would run statements the confirmed plan did not show"
	}
	for _, member := range slices.Sorted(maps.Keys(now)) {
		plan := now[member]
		if !plan.HasWork() {
			continue
		}
		was, ok := confirmed[member]
		if !ok {
			return false, fmt.Sprintf("target %s has work the confirmed round did not plan", member)
		}
		if !sameMemberWork(was, plan) {
			return false, fmt.Sprintf("target %s would run statements the confirmed round did not plan", member)
		}
	}
	return true, ""
}

// confirmedConvergedTargetRound reports whether the pending confirmation an
// apply-confirm acts on was given against the comment an apply posts when the
// reviewed target is already at the desired schema and other targets still have
// work. That comment pins the reviewed target's empty plan and renders the plans
// the review round stored for the other targets, so both are read from storage:
// an empty pinned plan alone is not that comment, since a database with one
// target has no round of other targets' plans for it to have shown. A pinned
// plan that no longer loads is an error: what its comment showed cannot be told.
func (h *Handler) confirmedConvergedTargetRound(ctx context.Context, pinnedPlanID, environment string) (bool, error) {
	pinned, err := h.service.Storage().Plans().Get(ctx, pinnedPlanID)
	if err != nil {
		return false, fmt.Errorf("load confirmed plan %s: %w", pinnedPlanID, err)
	}
	if pinned == nil {
		return false, fmt.Errorf("confirmed plan %s no longer exists", pinnedPlanID)
	}
	if pinned.HasWork() {
		h.logger.Debug("apply-confirm: the confirmed plan has work on the reviewed target, so its comment was not a converged-target confirmation",
			"database", pinned.Database, "environment", environment, "pending_plan_id", pinnedPlanID)
		return false, nil
	}
	members, err := h.service.MemberPlansForReviewRound(ctx, pinned, environment)
	if err != nil {
		return false, fmt.Errorf("load member plans of the confirmed round %s: %w", pinnedPlanID, err)
	}
	for _, member := range members {
		if member.HasWork() {
			return true, nil
		}
	}
	h.logger.Debug("apply-confirm: the confirmed round planned no other target with work, so its comment was not a converged-target confirmation",
		"database", pinned.Database, "environment", environment, "pending_plan_id", pinnedPlanID, "member_plans", len(members))
	return false, nil
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
