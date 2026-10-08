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
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/ui"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// rolloutStillPending reports whether a rollout whose primary target is
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
// plans alongside the primary plan: the rollout round passed its contract, some
// target other than the primary has work, and the comment the operator
// confirms renders every target's plan. Work on another target under any other
// shape is refused instead, since the apply would otherwise run statements the
// comment never showed (RV-1).
func rolloutRunsMemberWork(outcome reviewDriftOutcome, preview *templates.DeploymentDriftData) bool {
	return !outcome.blocks() && outcome.work.others > 0 && templates.RendersTargetPlans(preview)
}

// refusePendingRollout answers an apply that cannot run what the rollout has
// pending: drift blocks, the rest of the rollout is not known to be at the
// desired schema, or the other targets' work is not what the operator was shown.
// primaryTargetConverged says whether the primary target itself was already at
// the desired schema, which decides what the operator is told, and the message
// names that target the way the comment does.
//
// The apply does not run, and the check records what is pending so the PR
// cannot merge as if every target were up to date (MG-12).
func (h *Handler) refusePendingRollout(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, schemaResult *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment, requestedBy string, actionName string, outcome reviewDriftOutcome, primaryTargetConverged bool) {
	h.refuseRollout(ctx, client, repo, pr, installationID, schemaResult, planResp, environment, requestedBy, actionName, outcome, primaryTargetConverged, pendingRolloutMessage(outcome, primaryTargetConverged, h.primaryTargetName(outcome.work, planResp, schemaResult.Database, environment)))
}

// primaryTargetName names the target the apply's own plan was planned against
// the way the plan comment does: as the rollout's first member. When the
// rollout round named no members, because it did not run or failed, the name
// is read against the environment's configured targets instead, so a
// deployment that addresses several targets still qualifies it. Targets that
// cannot be resolved leave the name qualified, which never reads as another
// target. A plan with no target is named by its deployment alone, the only
// name it has.
func (h *Handler) primaryTargetName(work memberWork, planResp *apitypes.PlanResponse, database, environment string) string {
	if work.primary != "" {
		return work.primary
	}
	primary := plannedPrimaryMember(planResp)
	if primary.Target == "" {
		return primary.Deployment
	}
	targets, err := h.service.Config().ResolveDatabaseTargets(database, environment)
	if err != nil {
		h.logger.Warn("could not resolve the environment's targets to name the primary target; naming it with its target",
			"database", database, "environment", environment, "deployment", primary.Deployment, "target", primary.Target, "error", err)
		return qualifiedTargetName(primary)
	}
	return routing.DisplayNames(append([]routing.ExecutionTarget{primary}, targets...))[0]
}

// qualifiedTargetName names a member by its deployment and target, or by its
// deployment alone when it carries no target.
func qualifiedTargetName(member routing.ExecutionTarget) string {
	if member.Target == "" {
		return member.Deployment
	}
	return member.MemberID()
}

// refuseRollout refuses an apply that cannot run what the rollout has pending,
// recording what is pending on the check before posting message.
func (h *Handler) refuseRollout(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, schemaResult *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment, requestedBy string, actionName string, outcome reviewDriftOutcome, primaryTargetConverged bool, message string) {
	if err := h.recordPendingRollout(ctx, client, repo, pr, schemaResult, planResp, environment, outcome); err != nil {
		h.logger.Error("failed to record the pending rollout on the check; published a failing aggregate from the rollout round instead",
			"repo", repo, "pr", pr, "head_sha", schemaResult.HeadSHA, "database", schemaResult.Database, "database_type", schemaResult.Type,
			"environment", environment, "action", actionName, "primary_target_converged", primaryTargetConverged, "error", err)
	}
	h.postRolloutRefusal(repo, pr, installationID, schemaResult, planResp, environment, requestedBy, actionName, outcome, primaryTargetConverged, message)
}

// postRolloutRefusal tells the operator why an apply that cannot run what the
// rollout has pending did not run. The caller has already recorded the pending
// rollout on the check.
func (h *Handler) postRolloutRefusal(repo string, pr int, installationID int64, schemaResult *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment, requestedBy string, actionName string, outcome reviewDriftOutcome, primaryTargetConverged bool, message string) {
	h.logger.Info("apply refused: the rollout has work this apply cannot run",
		"repo", repo, "pr", pr, "database", schemaResult.Database, "database_type", schemaResult.Type,
		"environment", environment, "action", actionName, "plan_id", planResp.PlanID,
		"primary_target_converged", primaryTargetConverged,
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
// from a PR apply, or "" when it can. planID names the round's primary plan,
// and primaryTargetConverged says whether that plan is empty.
//
// The apply runs each member's stored plan, and the comment behind the apply
// renders those plans' statements with each target's direct and unsafe changes
// under it. So work the apply cannot run as planned, and
// anything whose consent rests on a disclosure that comment does not carry,
// refuses here: an unfinished copy the apply would discard. A direct-execution
// or unsafe change is not refused: the comment discloses it under the target
// that runs it, and an unsafe one needs --allow-unsafe as the primary plan's
// own does. It is asked before the apply takes its lock, so a refusal never
// pins a confirmation that could not succeed, and again against the re-plan the
// apply runs from.
//
// A copy at stake refuses whether or not the primary target has work, since
// the comment discloses only the primary plan's discarded copies. The rest is
// what apply creation asks of each member, which depends on the primary plan:
// an empty one can carry only per-member table work
// (api.MemberWorkAConvergedPrimaryPlanCannotRun), and one with work holds each
// member to its shape (api.MemberWorkThePrimaryPlanCannotRun). Neither refuses
// a blocked change any less.
func (h *Handler) memberWorkRefusal(ctx context.Context, planID, environment string, rollout reviewDriftOutcome, primaryTargetConverged bool) (string, error) {
	if rollout.work.copyAtStake != "" {
		return rollout.work.copyAtStake, nil
	}
	plan, members, err := h.reviewRoundPlans(ctx, planID, environment)
	if err != nil {
		return "", err
	}
	for _, member := range slices.Sorted(maps.Keys(members)) {
		memberPlan := members[member]
		if !memberPlan.HasWork() {
			continue
		}
		if reason := memberWorkPrimaryPlanCannotRun(plan, memberPlan, primaryTargetConverged); reason != "" {
			return fmt.Sprintf("target %s: its plan %s", member, reason), nil
		}
	}
	return "", nil
}

// reviewRoundPlans loads the primary target's plan stored as planID and the
// plans its rollout round stored for the other targets, keyed by target.
func (h *Handler) reviewRoundPlans(ctx context.Context, planID, environment string) (*storage.Plan, map[string]*storage.Plan, error) {
	plan, err := h.service.Storage().Plans().Get(ctx, planID)
	if err != nil {
		return nil, nil, fmt.Errorf("load primary target's plan %s: %w", planID, err)
	}
	if plan == nil {
		return nil, nil, fmt.Errorf("primary target's plan %s was not stored", planID)
	}
	members, err := h.service.MemberPlansForReviewRound(ctx, plan, environment)
	if err != nil {
		return nil, nil, fmt.Errorf("load member plans of round %s: %w", planID, err)
	}
	return plan, members, nil
}

// deferCutoverHasNothingToDefer reports whether --defer-cutover has nothing to
// act on in an apply: every target it runs carries only direct-execution
// changes, which have no cutover, and at least one carries a change. When the
// apply runs other targets' own plans, their plans are read too, since the
// primary plan speaks only for the primary target. When it does not, the
// primary plan alone decides: runsMemberWork is the same answer that decides
// whether the apply runs the other targets' plans, so a plan this check skips
// is one the apply does not run.
func (h *Handler) deferCutoverHasNothingToDefer(ctx context.Context, planResp *apitypes.PlanResponse, environment string, runsMemberWork bool) (bool, error) {
	if !runsMemberWork {
		return planResp.AllChangesDirect(), nil
	}
	_, members, err := h.reviewRoundPlans(ctx, planResp.PlanID, environment)
	if err != nil {
		return false, err
	}
	return rolloutAllChangesDirect(planResp, members), nil
}

// confirmCommandHasNoCutoverToDefer reports whether the apply-confirm a paused
// apply's comment suggests leaves out --defer-cutover, because no target the
// apply runs has a cutover to defer. The primary plan alone cannot say so when
// other targets run engine-driven changes of their own. A target plan that
// cannot be read keeps the flag: apply-confirm refuses a flag with nothing to
// defer and keeps the pending confirmation, while a dropped flag would let a
// cutover run without the pause the operator asked for.
func (h *Handler) confirmCommandHasNoCutoverToDefer(ctx context.Context, repo string, pr int, planResp *apitypes.PlanResponse, environment string, runsMemberWork bool) bool {
	nothingToDefer, err := h.deferCutoverHasNothingToDefer(ctx, planResp, environment, runsMemberWork)
	if err != nil {
		h.logger.Warn("could not read every target's plan for the comment's apply-confirm command; it keeps --defer-cutover, which apply-confirm refuses if no target has a cutover to defer",
			"repo", repo, "pr", pr, "environment", environment, "plan_id", planResp.PlanID, "error", err)
		return false
	}
	return nothingToDefer
}

// rolloutAllChangesDirect reports whether every target with work, the
// primary's included, runs only direct-execution changes, and at least one
// target has work. A target already at the desired schema runs nothing, so it
// neither carries a cutover nor counts as direct.
func rolloutAllChangesDirect(primary *apitypes.PlanResponse, members map[string]*storage.Plan) bool {
	direct := false
	if primary.HasChanges() {
		if !primary.AllChangesDirect() {
			return false
		}
		direct = true
	}
	for _, member := range members {
		if !member.HasWork() {
			continue
		}
		if !member.AllChangesDirect() {
			return false
		}
		direct = true
	}
	return direct
}

// memberWorkPrimaryPlanCannotRun asks of one member's plan what apply creation
// asks of it under the primary plan the apply is created from.
func memberWorkPrimaryPlanCannotRun(primary, member *storage.Plan, primaryTargetConverged bool) string {
	if primaryTargetConverged {
		return api.MemberWorkAConvergedPrimaryPlanCannotRun(member)
	}
	return api.MemberWorkThePrimaryPlanCannotRun(primary, member)
}

// annotateMemberApplyRefusal records on a plan comment why a PR apply cannot
// run the other targets' plans it renders, whether or not the primary target
// has work of its own. Such an apply is refused whatever its flags, so the
// comment must not offer it. A refusal that cannot be computed leaves the apply
// offered: the apply asks again before it takes its lock and refuses on the
// same grounds, so the comment only ever misses a shortcut, never a gate.
func (h *Handler) annotateMemberApplyRefusal(ctx context.Context, data *templates.PlanCommentData, planResp *apitypes.PlanResponse, environment string, rollout reviewDriftOutcome, repo string, pr int) {
	if !rolloutRunsMemberWork(rollout, data.DeploymentDrift) {
		h.logger.Debug("plan comment renders no other targets' plans a PR apply would run; no member-work refusal to disclose",
			"repo", repo, "pr", pr, "database", planResp.Database, "environment", environment, "plan_id", planResp.PlanID)
		return
	}
	refusal, err := h.memberWorkRefusal(ctx, planResp.PlanID, environment, rollout, !planResp.HasChanges())
	if err != nil {
		h.logger.Warn("could not tell whether a PR apply can run the other targets' plans; the plan comment offers the apply, which re-checks before it takes its lock",
			"repo", repo, "pr", pr, "database", planResp.Database, "environment", environment, "plan_id", planResp.PlanID, "error", err)
		return
	}
	data.MemberApplyRefusal = refusal
}

// blockUnsafeWithoutOptIn posts the unsafe-changes refusal and reports true
// when the apply carries an unsafe change and was not given --allow-unsafe.
// The unsafe changes are the primary plan's and, when the apply runs other
// targets' own plans, theirs too, each named with the targets that carry it.
func (h *Handler) blockUnsafeWithoutOptIn(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, installationID int64, schemaResult *ghclient.SchemaRequestResult, planResp *apitypes.PlanResponse, environment, requestedBy string, result CommandResult, runsMemberWork bool, rolloutPreview *templates.DeploymentDriftData) bool {
	if result.AllowUnsafe {
		return false
	}
	var memberUnsafe []templates.UnsafeChangeData
	if runsMemberWork {
		memberUnsafe = templates.TargetPlanUnsafeChanges(rolloutPreview)
	}
	if len(planResp.UnsafeChanges()) == 0 && len(memberUnsafe) == 0 {
		return false
	}
	commentData := buildPlanCommentData(schemaResult, planResp, environment, result.Tenant, requestedBy, h.agentHint(), h.cliName())
	commentData.ScopedDatabase = result.Database
	commentData.UnsafeChanges = append(commentData.UnsafeChanges, memberUnsafe...)
	commentData.HasUnsafeChanges = true
	if runsMemberWork {
		// The refusal shows the plan each target would run, not only the
		// primary's, which can have nothing to run.
		commentData.DeploymentDrift = rolloutPreview
	}
	h.annotateAttributedChanges(ctx, client, &commentData, planResp, commentData.DeploymentDrift, repo, pr, environment)
	h.logger.Info("apply blocked by unsafe changes",
		"repo", repo, "pr", pr, "database", schemaResult.Database, "database_type", schemaResult.Type, "environment", environment,
		"plan_id", planResp.PlanID, "primary_unsafe", len(planResp.UnsafeChanges()), "other_targets_unsafe", len(memberUnsafe))
	h.postComment(repo, pr, installationID, templates.RenderUnsafeChangesBlocked(commentData))
	return true
}

// memberWorkRefusalMessage tells the operator why the other targets' work was
// not run. The refusal names only targets, tables, and namespaces.
func memberWorkRefusalMessage(refusal string) string {
	return fmt.Sprintf("This PR cannot apply every target's plan: %s, so nothing was applied. An apply runs every target or none. The schema check keeps blocking merge until every target has the change.", refusal)
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

// pendingRolloutMessage explains a refused apply to the operator, naming the
// primary target as the comment does. The drift summary names only configured
// members and is already clamped for markdown, so it is safe to render; the raw
// causes stay in the server logs.
func pendingRolloutMessage(outcome reviewDriftOutcome, primaryTargetConverged bool, primary string) string {
	const rerun = "Run apply again for this environment to review and confirm each target's own plan."
	switch {
	case outcome.blocks() && primaryTargetConverged:
		return fmt.Sprintf("Target %s already has this schema, but SchemaBot could not confirm that the other targets do (%s), so nothing was applied. The schema check stays failing until every target is confirmed.", primary, outcome.summary)
	case outcome.blocks():
		return fmt.Sprintf("SchemaBot could not confirm the plan of every target (%s), so nothing was applied. The schema check stays failing until every target is confirmed.", outcome.summary)
	case primaryTargetConverged:
		return fmt.Sprintf("Target %s already has this schema, but %s: %s. The plans those targets would run were not on the comment this apply acts on, so nothing was applied. %s", primary, outcome.work.summary(), strings.Join(outcome.work.names, ", "), rerun)
	default:
		return fmt.Sprintf("%s: %s. The comment this apply acts on showed only the plan of target %s, so nothing was applied. %s", outcome.work.summary(), strings.Join(outcome.work.names, ", "), primary, rerun)
	}
}

// unconfirmedWorkMessage tells the operator why an apply-confirm did not run:
// what the rollout would run now is not what the confirmation was given
// against. reason names only targets, and says whether it is the reviewed
// target or another one whose plan changed. The rollout's pending summary leads,
// so the operator sees which targets still need the change, as on every other
// refusal of a pending rollout.
func unconfirmedWorkMessage(work memberWork, reason string) string {
	message := fmt.Sprintf("This confirmation no longer covers what the apply would run: %s, so nothing was applied. Run apply again for this environment to review and confirm each target's own plan.", reason)
	if work.members > 1 && work.pending > 0 {
		return fmt.Sprintf("%s: %s. %s", work.summary(), strings.Join(work.names, ", "), message)
	}
	return message
}

// confirmationCoversMemberWork reports whether the comment behind an apply was
// posted with the member work it is about to run, with a reason for the log
// when it was not. pinnedPlanID is the plan the lock pins: on apply-confirm, the
// one the confirmation was given against, and on an automatic apply, the one the
// apply command just posted.
//
// A pinned plan covers another target's work only through the rollout round
// stored with it. The apply command runs that round before pinning, and
// whenever another target has work the comment it posts renders every target's
// plan, so a round whose members have work is one whose plans the operator was
// shown. A pin without such a round covers no other target's work.
//
// The targets are planned again before the apply runs, and a target's schema
// can change in between. So the primary target, when it still has work, and each member with
// work now must have been planned with the same work in the confirmed round,
// and work the confirmed comment did not show never runs on the strength of
// that confirmation.
//
// primary names the target current was planned against. It is called only to
// build a refusal, so naming the target costs nothing on a confirmation that
// covers the work.
func (h *Handler) confirmationCoversMemberWork(ctx context.Context, pinnedPlanID, currentPlanID, environment string, primary func() string) (bool, string, error) {
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
	// The two plans alone settle the primary member's identity, so a changed
	// primary is refused before the member plans are read: a read that fails
	// afterwards would keep a confirmation already known not to cover the work.
	if primaryTargetChanged(pinned, current) {
		h.logConfirmedPrimaryTargetChanged(pinned, current, environment)
		return false, primaryTargetDifferenceReason(primary(), workTarget), nil
	}
	confirmed, err := h.service.MemberPlansForReviewRound(ctx, pinned, environment)
	if err != nil {
		return false, "", fmt.Errorf("load member plans of the confirmed round: %w", err)
	}
	now, err := h.service.MemberPlansForReviewRound(ctx, current, environment)
	if err != nil {
		return false, "", fmt.Errorf("load member plans of the confirm-time round: %w", err)
	}
	covered, reason := roundCoversWork(primary, pinned, current, confirmed, now)
	return covered, reason, nil
}

// roundCoversWork reports whether the confirm-time round runs only work the
// confirmed round planned, with a reason naming the target and the part of its
// work that differs when it does not. primary names the target current was
// planned against, called only when the refusal names it. The primary member must still be the reviewed member, even
// when it converged while other targets still have work. With that identity
// fixed, only targets with work are compared.
//
// The identity check is part of what this comparison means, so it is made here
// as well as by confirmationCoversMemberWork, which refuses a changed primary
// before reading the member plans this function compares. That earlier refusal
// is the enforcement point; this one keeps the comparison complete on its own
// for any caller that has the plans already.
func roundCoversWork(primary func() string, pinned, current *storage.Plan, confirmed, now map[string]*storage.Plan) (bool, string) {
	if primaryTargetChanged(pinned, current) {
		return false, primaryTargetDifferenceReason(primary(), workTarget)
	}
	if current.HasWork() {
		if difference := memberWorkDifference(pinned, current); difference != workUnchanged {
			return false, primaryTargetDifferenceReason(primary(), difference)
		}
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
		if difference := memberWorkDifference(was, plan); difference != workUnchanged {
			return false, fmt.Sprintf("the plan of target %s differs from what the confirmed round planned, in %s", member, difference)
		}
	}
	return true, ""
}

// primaryTargetDifferenceReason is the refusal reason for a primary target,
// named as the comment names it, whose confirm-time re-plan differs from the
// plan the confirmation was given against, naming the part of its work that
// differs, or saying that the confirmed plan reviewed another target.
func primaryTargetDifferenceReason(primary string, difference workDifference) string {
	if difference == workTarget {
		return fmt.Sprintf("target %s is not the target the confirmed plan reviewed", primary)
	}
	return fmt.Sprintf("the re-plan of target %s differs from the confirmed plan in %s", primary, difference)
}

func primaryTargetChanged(pinned, current *storage.Plan) bool {
	return (routing.ExecutionTarget{Deployment: pinned.Deployment, Target: pinned.Target}).MemberID() !=
		(routing.ExecutionTarget{Deployment: current.Deployment, Target: current.Target}).MemberID()
}

func (h *Handler) logConfirmedPrimaryTargetChanged(pinned, current *storage.Plan, environment string) {
	h.logger.Info("apply-confirm refused: the primary member changed; a fresh apply must review the current targets",
		"repo", current.Repository, "pr", current.PullRequest, "head_sha", current.HeadSHA,
		"database", current.Database, "database_type", current.DatabaseType, "environment", environment,
		"pending_plan_id", pinned.PlanIdentifier, "plan_id", current.PlanIdentifier,
		"confirmed_deployment", pinned.Deployment, "confirmed_target", pinned.Target,
		"current_deployment", current.Deployment, "current_target", current.Target)
}

// convergedRoundRefusal is why an apply-confirm whose primary target has
// changes is refused against the comment it acts on, when that comment showed
// the primary target already at the desired schema.
type convergedRoundRefusal int

const (
	// convergedRoundAccepts means the confirmed comment showed the primary
	// target's own plan, so the gates that hold the re-plan to that plan decide.
	convergedRoundAccepts convergedRoundRefusal = iota
	// convergedRoundPrimaryMoved means the primary target is not the member the
	// confirmed comment showed as converged: the changes it has now are another
	// target's, which the comment may well have shown, but under that target.
	convergedRoundPrimaryMoved
	// convergedRoundPrimaryGainedWork means the primary target the comment
	// showed as converged has gained changes of its own since.
	convergedRoundPrimaryGainedWork
)

// confirmedConvergedTargetRound judges the pending confirmation an
// apply-confirm acts on when the primary target has changes, against the
// comment an apply posts when the primary target is already at the desired
// schema and other targets still have work. That comment pins the primary
// target's empty plan and renders the plans the review round stored for the
// other targets, so both are read from storage: an empty pinned plan alone is
// not that comment, since a database with one target has no round of other
// targets' plans for it to have shown. Having found that comment, the
// confirm-time plan, stored as currentPlanID, tells whether the primary target
// with changes is still the member the comment showed as converged, or another
// one that leads the rollout now. A pinned or confirm-time plan that no longer
// loads is an error: what the comment showed, or what the apply would run,
// cannot be told.
//
// A refusal comes with the name of the primary target it is about, as the
// comment named the targets of its round: the converged target when it gained
// changes, and the target that leads the rollout now when that moved.
func (h *Handler) confirmedConvergedTargetRound(ctx context.Context, pinnedPlanID, currentPlanID, environment string) (convergedRoundRefusal, string, error) {
	plans := h.service.Storage().Plans()
	pinned, err := plans.Get(ctx, pinnedPlanID)
	if err != nil {
		return convergedRoundAccepts, "", fmt.Errorf("load confirmed plan %s: %w", pinnedPlanID, err)
	}
	if pinned == nil {
		return convergedRoundAccepts, "", fmt.Errorf("confirmed plan %s no longer exists", pinnedPlanID)
	}
	if pinned.HasWork() {
		h.logger.Debug("apply-confirm: the confirmed plan has work on the primary target, so its comment was not a converged-target confirmation",
			"database", pinned.Database, "environment", environment, "pending_plan_id", pinnedPlanID)
		return convergedRoundAccepts, "", nil
	}
	members, err := h.service.MemberPlansForReviewRound(ctx, pinned, environment)
	if err != nil {
		return convergedRoundAccepts, "", fmt.Errorf("load member plans of the confirmed round %s: %w", pinnedPlanID, err)
	}
	if !anyMemberHasWork(members) {
		h.logger.Debug("apply-confirm: the confirmed round planned no other target with work, so its comment was not a converged-target confirmation",
			"database", pinned.Database, "environment", environment, "pending_plan_id", pinnedPlanID, "member_plans", len(members))
		return convergedRoundAccepts, "", nil
	}
	current, err := plans.Get(ctx, currentPlanID)
	if err != nil {
		return convergedRoundAccepts, "", fmt.Errorf("load confirm-time plan %s: %w", currentPlanID, err)
	}
	if current == nil {
		return convergedRoundAccepts, "", fmt.Errorf("confirm-time plan %s was not stored", currentPlanID)
	}
	if primaryTargetChanged(pinned, current) {
		h.logConfirmedPrimaryTargetChanged(pinned, current, environment)
		return convergedRoundPrimaryMoved, roundMemberName(current, members), nil
	}
	return convergedRoundPrimaryGainedWork, roundMemberName(pinned, members), nil
}

// anyMemberHasWork reports whether any plan of a review round's other targets
// has work for its target.
func anyMemberHasWork(members map[string]*storage.Plan) bool {
	for _, member := range members {
		if member.HasWork() {
			return true
		}
	}
	return false
}

// roundMemberName names target the way the comment of its review round named
// it: by its deployment, qualified with the target when another member of the
// round routes through the same deployment.
func roundMemberName(target *storage.Plan, round map[string]*storage.Plan) string {
	named := routing.ExecutionTarget{Deployment: target.Deployment, Target: target.Target}
	members := []routing.ExecutionTarget{named}
	for _, member := range round {
		other := routing.ExecutionTarget{Deployment: member.Deployment, Target: member.Target}
		if other.MemberID() == named.MemberID() {
			continue
		}
		members = append(members, other)
	}
	return routing.DisplayNames(members)[0]
}

// confirmationCoversPrimaryTarget reports how the primary target's
// confirm-time re-plan, stored as currentPlanID, differs from the plan the
// pending confirmation was given against, when that confirmation was given on a
// rollout's comment. workUnchanged means the confirmation covers the re-plan.
//
// Whether it was is read from the confirmed round as well as from the rollout
// at confirm: rolloutAtConfirm says the environment still has several targets,
// and a confirmed round that stored plans for other targets was a rollout's
// even when the topology has since shrunk to the primary target alone. A
// confirmation that neither marks as a rollout's was given for a single target,
// whose re-plan runs under the single-target gates only while it still addresses
// the reviewed member. A pinned or confirm-time
// plan that no longer loads is an error: what the comment showed, or what the
// apply would run, cannot be told.
func (h *Handler) confirmationCoversPrimaryTarget(ctx context.Context, pinnedPlanID, currentPlanID, environment string, rolloutAtConfirm bool) (workDifference, error) {
	plans := h.service.Storage().Plans()
	pinned, err := plans.Get(ctx, pinnedPlanID)
	if err != nil {
		return workUnchanged, fmt.Errorf("load confirmed plan %s: %w", pinnedPlanID, err)
	}
	if pinned == nil {
		return workUnchanged, fmt.Errorf("confirmed plan %s no longer exists", pinnedPlanID)
	}
	current, err := plans.Get(ctx, currentPlanID)
	if err != nil {
		return workUnchanged, fmt.Errorf("load confirm-time plan %s: %w", currentPlanID, err)
	}
	if current == nil {
		return workUnchanged, fmt.Errorf("confirm-time plan %s was not stored", currentPlanID)
	}
	if primaryTargetChanged(pinned, current) {
		h.logConfirmedPrimaryTargetChanged(pinned, current, environment)
		return workTarget, nil
	}
	if !rolloutAtConfirm {
		confirmedRound, err := h.service.MemberPlansForReviewRound(ctx, pinned, environment)
		if err != nil {
			return workUnchanged, fmt.Errorf("load member plans of the confirmed round %s: %w", pinnedPlanID, err)
		}
		if len(confirmedRound) == 0 {
			h.logger.Debug("apply-confirm: the confirmed plan was reviewed for a single target, so its re-plan runs under the single-target gates",
				"database", pinned.Database, "environment", environment, "pending_plan_id", pinnedPlanID, "plan_id", currentPlanID)
			return workUnchanged, nil
		}
		h.logger.Info("apply-confirm: the confirmed round planned other targets that the rollout no longer has; comparing the primary target's re-plan with the confirmed plan",
			"database", pinned.Database, "environment", environment, "pending_plan_id", pinnedPlanID, "plan_id", currentPlanID, "confirmed_member_plans", len(confirmedRound))
	}
	return memberWorkDifference(pinned, current), nil
}

// workDifference names the part of a target's work in which two plans for it
// differ, or that the plans address different primary members. The zero value,
// workUnchanged, means the plans run the same work.
type workDifference string

const (
	workUnchanged     workDifference = ""
	workTarget        workDifference = "which primary target it addresses"
	workStatements    workDifference = "its statements"
	workExecutionMode workDifference = "how its statements run"
	workUnsafe        workDifference = "which of its statements are unsafe"
	workFinalizer     workDifference = "which namespaces it finalizes"
	workVSchema       workDifference = "the VSchema it writes"
)

// memberWorkDifference reports the first part of the work in which two plans
// for one target differ, or workUnchanged when they run the same work. It
// compares everything an apply created from the plan executes and the comment
// shows: the table changes and each shard's own changes, byte for byte, then
// how each of those statements runs, then each statement's unsafe verdict and
// reason, which the target's schema decides and can change without changing
// the statement, then which namespaces end with a
// finalizer, and the VSchema each of those writes with the record of what that
// VSchema change does. A difference in spelling alone refuses too, which only
// ever sends the operator back to review.
func memberWorkDifference(a, b *storage.Plan) workDifference {
	if !slices.EqualFunc(a.FlatDDLChanges(), b.FlatDDLChanges(), sameStatement) || !sameShardChanges(a.Shards, b.Shards, sameStatement) {
		return workStatements
	}
	if !slices.EqualFunc(a.FlatDDLChanges(), b.FlatDDLChanges(), sameTableChange) || !sameShardChanges(a.Shards, b.Shards, sameTableChange) {
		return workExecutionMode
	}
	if !slices.EqualFunc(a.FlatDDLChanges(), b.FlatDDLChanges(), sameUnsafeVerdict) || !sameShardChanges(a.Shards, b.Shards, sameUnsafeVerdict) {
		return workUnsafe
	}
	if !slices.Equal(a.FinalizerNamespaces(), b.FinalizerNamespaces()) {
		return workFinalizer
	}
	for _, namespace := range a.FinalizerNamespaces() {
		if !sameVSchemaChange(a.Namespaces[namespace], b.Namespaces[namespace]) {
			return workVSchema
		}
	}
	return workUnchanged
}

// sameShardChanges reports whether two plans' per-shard changes target the
// same shards with changes that same reports equal.
func sameShardChanges(a, b []storage.ShardPlan, same func(x, y storage.TableChange) bool) bool {
	return slices.EqualFunc(a, b, func(x, y storage.ShardPlan) bool {
		return x.Namespace == y.Namespace && x.Shard == y.Shard && slices.EqualFunc(x.Changes, y.Changes, same)
	})
}

// sameVSchemaChange reports whether two plans of one namespace write the same
// VSchema with the same recorded effect: the document the finalizer applies,
// and the diff, removals and vindex mutations the comment rendered from it.
func sameVSchemaChange(a, b *storage.NamespacePlanData) bool {
	return a.Artifacts[storage.VSchemaArtifactName] == b.Artifacts[storage.VSchemaArtifactName] && maps.Equal(a.Metadata, b.Metadata)
}

// sameStatement reports whether two table changes run the same statement on
// the same table, however each is executed.
func sameStatement(a, b storage.TableChange) bool {
	return a.Namespace == b.Namespace && a.Table == b.Table && a.Operation == b.Operation && a.DDL == b.DDL
}

// sameTableChange reports whether two table changes run the same statement the
// same way.
func sameTableChange(a, b storage.TableChange) bool {
	return sameStatement(a, b) && a.ExecutionMode == b.ExecutionMode
}

// sameUnsafeVerdict reports whether two table changes run the same statement
// the same way and carry the same unsafe verdict and reason, so an opt-in given
// for one consents to exactly the consequences of the other.
func sameUnsafeVerdict(a, b storage.TableChange) bool {
	return sameTableChange(a, b) && a.IsUnsafe == b.IsUnsafe && sameUnsafeReason(a.UnsafeReason, b.UnsafeReason)
}

// sameUnsafeReason reports whether two unsafe reasons name the same findings,
// compared as the lists the comment renders from them, each on its own line,
// with order ignored. Engines that normalize their reasons already sort the
// findings, but a reason can also come from a plan stored before that, or from
// an engine that does not normalize, so the order of a reason's findings is
// never taken to mean anything. How many times a finding appears does mean
// something: the same message raised on two columns is two findings, and a
// plan that gains the second is not the plan the opt-in was given for.
func sameUnsafeReason(a, b string) bool {
	return slices.Equal(unsafeReasonFindings(a), unsafeReasonFindings(b))
}

// unsafeReasonFindings returns the findings an unsafe reason names, sorted.
func unsafeReasonFindings(reason string) []string {
	findings := ui.LintReasons(reason)
	slices.Sort(findings)
	return findings
}
