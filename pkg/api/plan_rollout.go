package api

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/storage"
)

// unplannedMemberDetail is the detail a plan response gives for a member that
// could not be planned, with what to do next. The raw error can carry dial
// failures and internal hostnames, so it is logged with the member rather
// than returned.
const unplannedMemberDetail = "could not be planned; see server logs for the cause, then plan again"

// divergedMemberDetail is the detail a plan response gives for a member whose
// own plan differs from the primary plan it is expected to mirror, with what
// to do next.
const divergedMemberDetail = "its live schema differs from the primary's, so the primary's plan does not describe it; bring it back in line with the primary, then plan again"

// planRollout plans the other members of a rollout for a plan requested
// through the API, so the response says what an apply would run on each
// member, and an apply created from the primary's plan has a stored plan for
// every member planned against its own schema. It returns nil for an
// environment with a single member, for a plan narrowed to one member, and
// for a primary plan that reported errors, which already fails the plan on its
// own.
//
// A narrowed plan speaks for its one member only. Planning the other members
// beside it would present that member's plan as the rollout's and store member
// plans bound to it, so the rollout is planned only beside the primary's plan.
func (s *Service) planRollout(ctx context.Context, req PlanRequest, primaryPlan *ternv1.PlanResponse, planResp *apitypes.PlanResponse) (*apitypes.PlanRolloutResponse, error) {
	if req.Target != "" {
		s.logger.Info("skipping rollout member plans: the plan is narrowed to one rollout member",
			"database", req.Database, "environment", req.Environment, "plan_id", primaryPlan.GetPlanId(),
			"selector", req.Target, "narrowed_to", planResp.NarrowedTo)
		return nil, nil
	}
	if len(primaryPlan.GetErrors()) > 0 {
		s.logger.Debug("skipping rollout member plans: the primary plan reported errors",
			"database", req.Database, "environment", req.Environment, "plan_id", primaryPlan.GetPlanId())
		return nil, nil
	}
	targets, err := s.config.ResolveDatabaseTargets(req.Database, req.Environment)
	if err != nil {
		return nil, fmt.Errorf("resolve rollout members for %s/%s: %w", req.Database, req.Environment, err)
	}
	if len(targets) <= 1 {
		s.logger.Debug("skipping rollout member plans: the environment has a single rollout member",
			"database", req.Database, "environment", req.Environment, "plan_id", primaryPlan.GetPlanId())
		return nil, nil
	}
	// The primary is held to the namespace selection it was planned under, so
	// the rollup can tell a placement change since the plan from the plan
	// itself.
	primary := routing.ExecutionTarget{Deployment: planResp.Deployment, Target: planResp.Target, Namespaces: planResp.SelectedNamespaces}
	if primary.MemberID() != targets[0].MemberID() {
		return nil, fmt.Errorf("plan %s for %s/%s was made for rollout member %s, not the rollout primary %s; the other members are planned only beside the primary's plan",
			primaryPlan.GetPlanId(), req.Database, req.Environment, primary.MemberID(), targets[0].MemberID())
	}
	rollup, err := s.planRolloutMembers(ctx, req, primaryPlan, primary, targets)
	if err != nil {
		return nil, err
	}
	rollout := planRolloutResponse(rollup)
	if len(rollout.Attention) == 0 {
		refused, err := s.rolloutApplyRefusals(ctx, req.Environment, primaryPlan.GetPlanId(), targets)
		if err != nil {
			return nil, err
		}
		rollout.Refused = refused
	} else {
		s.logger.Debug("skipping the rollout's apply refusals: members need attention, which refuses the apply first",
			"database", req.Database, "environment", req.Environment, "plan_id", primaryPlan.GetPlanId(), "members_needing_attention", len(rollout.Attention))
	}
	for i, entry := range rollup.Entries {
		if entry.Class != DeploymentErrored {
			continue
		}
		s.logger.Warn("rollout member could not be planned; an apply of this plan will be refused until it is",
			"database", req.Database,
			"environment", req.Environment,
			"plan_id", primaryPlan.GetPlanId(),
			"deployment", entry.Deployment,
			"target", entry.Target,
			"member_index", i,
			"error", entry.Err)
	}
	s.logger.Info("planned every rollout member",
		"database", req.Database,
		"environment", req.Environment,
		"plan_id", primaryPlan.GetPlanId(),
		"member_planning", rollup.Planning.String(),
		"members", rollout.Members,
		"distinct_plans", len(rollout.Groups),
		"members_needing_attention", len(rollout.Attention),
		"members_refused_rollout_wide", len(rollout.Refused))
	return rollout, nil
}

// planRolloutResponse groups a rollout's members by the plan each would run,
// one group per distinct plan with the primary's first, and lists the members
// an apply cannot run on as planned.
//
// Members are grouped on the plan fingerprint, which two members share exactly
// when their plans are the same work, together with the execution verdict each
// change runs under (see planGroupKey). A member that errored has no
// fingerprint and no plan, and a mirrored member that diverged would still run
// the primary's plan, so neither joins a group: each is listed for attention.
func planRolloutResponse(rollup PlanRollup) *apitypes.PlanRolloutResponse {
	members := make([]routing.ExecutionTarget, len(rollup.Entries))
	for i, e := range rollup.Entries {
		members[i] = routing.ExecutionTarget{Deployment: e.Deployment, Target: e.Target}
	}
	names := routing.DisplayNames(members)

	resp := &apitypes.PlanRolloutResponse{
		Members:     len(rollup.Entries),
		Independent: rollup.Planning == PlanIndependent,
	}
	byPlan := make(map[string]*apitypes.PlanMemberGroupResponse, len(rollup.Entries))
	for i, e := range rollup.Entries {
		switch e.Class {
		case DeploymentErrored:
			resp.Attention = append(resp.Attention, &apitypes.PlanMemberAttentionResponse{
				Member: names[i], Reason: apitypes.PlanMemberUnplanned, Detail: unplannedMemberDetail,
			})
			continue
		case DeploymentDiverged:
			resp.Attention = append(resp.Attention, &apitypes.PlanMemberAttentionResponse{
				Member: names[i], Reason: apitypes.PlanMemberDiverged, Detail: divergedMemberDetail,
			})
			continue
		case DeploymentMatch, DeploymentPlanned:
		}
		key := planGroupKey(e)
		group, ok := byPlan[key]
		if !ok {
			plan := planResponseFromProto(&ternv1.PlanResponse{Changes: e.ChangeSet.Changes, Shards: e.ChangeSet.Shards})
			group = &apitypes.PlanMemberGroupResponse{Primary: i == 0, Changes: plan.Changes, Shards: plan.Shards}
			byPlan[key] = group
			resp.Groups = append(resp.Groups, group)
		}
		group.Members = append(group.Members, names[i])
	}
	// The primary is the first member, so its group is already first unless
	// the primary itself needs attention. Ordering is stated as a property of
	// the result rather than left to that coincidence.
	slices.SortStableFunc(resp.Groups, func(a, b *apitypes.PlanMemberGroupResponse) int {
		switch {
		case a.Primary == b.Primary:
			return 0
		case a.Primary:
			return -1
		default:
			return 1
		}
	})
	return resp
}

// planGroupKey is what two members share when one group describes both: the
// same work, and the same execution verdict on every change. The fingerprint
// alone is the work, and the direct execution policy judges each target's own
// table, so two members can plan the same DDL while one runs it as native DDL
// that blocks writes and the other through the engine. A group's changes carry
// its first member's verdicts, so members whose verdicts differ are kept apart
// rather than shown under verdicts that are not theirs. A verdict's reason is
// not part of the key: it carries the member's own measurements, which differ
// between members that run the same statement the same way.
func planGroupKey(e DeploymentRollupEntry) string {
	var verdicts []string
	record := func(namespace, shard string, changes []*ternv1.TableChange) {
		for _, tc := range changes {
			if tc.GetExecutionMode() == "" {
				continue
			}
			verdicts = append(verdicts, strings.Join([]string{namespace, shard, tc.GetTableName(), tc.GetExecutionMode()}, "\x00"))
		}
	}
	for _, sc := range e.ChangeSet.Changes {
		record(sc.GetNamespace(), "", sc.GetTableChanges())
	}
	for _, sp := range e.ChangeSet.Shards {
		record(sp.GetNamespace(), sp.GetShard(), sp.GetChanges())
	}
	slices.Sort(verdicts)
	return e.PlanFingerprint + "\x1e" + strings.Join(verdicts, "\x1e")
}

// RolloutUnrenderedError refuses a rollout-wide plan or apply of an
// environment with more than one member from an HTTP caller that did not say
// it renders the plan of every member. Such a caller shows the primary's plan
// as the whole rollout's: "no changes" while another member still needs the
// change, or a confirmation for DDL that runs on members it never showed.
type RolloutUnrenderedError struct {
	Database    string
	Environment string
	Members     int
}

func (e *RolloutUnrenderedError) Error() string {
	return fmt.Sprintf("%s/%s has %d rollout members, and this client does not show the plan of each one; upgrade the schemabot CLI to plan or apply a multi-target environment, or plan and apply one member with its target",
		e.Database, e.Environment, e.Members)
}

// refusePlanRolloutUnrenderedByCaller refuses a rollout-wide plan request from
// a caller that does not render the rollout, for an environment with more than
// one member. A plan narrowed to one member speaks for that member alone and
// renders as one plan, so it is not refused.
//
// An environment the config cannot resolve is left to the planner, which
// refuses it with the error that names what is missing.
func (s *Service) refusePlanRolloutUnrenderedByCaller(req PlanRequest) error {
	if req.RendersRollout || req.Target != "" {
		return nil
	}
	targets, err := s.config.ResolveDatabaseTargets(req.Database, req.Environment)
	if err != nil {
		s.logger.Debug("rollout rendering check deferred to the planner: the environment's rollout members did not resolve",
			"database", req.Database, "environment", req.Environment, "error", err)
		return nil
	}
	if len(targets) <= 1 {
		return nil
	}
	return &RolloutUnrenderedError{Database: req.Database, Environment: req.Environment, Members: len(targets)}
}

// refuseApplyRolloutUnrenderedByCaller refuses a rollout-wide apply over HTTP
// of an environment with more than one member from a caller that does not
// render the rollout, by the rule refusePlanRolloutUnrenderedByCaller states.
// The webhook and the trusted enqueue path never come through HTTP: the
// webhook renders every member's plan on the comment it confirms.
func refuseApplyRolloutUnrenderedByCaller(plan *storage.Plan, req ApplyRequest, targets []routing.ExecutionTarget, narrowedTo string) error {
	if !req.viaHTTP || req.RendersRollout || narrowedTo != "" || len(targets) <= 1 {
		return nil
	}
	return &RolloutUnrenderedError{Database: plan.Database, Environment: req.Environment, Members: len(targets)}
}

// rolloutApplyRefusals lists the members whose own plans apply creation
// refuses when the primary's plan is applied rollout-wide through the API, so
// a caller can refuse before it takes a lock and prompts, and name the
// narrowed apply that runs each one.
//
// It pairs the members with the stored plans an apply would pair them with
// (resolveApplyMembers) and asks of each what apply creation asks of it for a
// caller other than a pull request apply-confirm
// (memberWorkARolloutWideAPIApplyCannotRun).
func (s *Service) rolloutApplyRefusals(ctx context.Context, environment, planID string, targets []routing.ExecutionTarget) ([]*apitypes.PlanMemberRefusalResponse, error) {
	plan, err := s.storage.Plans().Get(ctx, planID)
	if err != nil {
		return nil, fmt.Errorf("load plan %s to check what an apply of it refuses: %w", planID, err)
	}
	if plan == nil {
		return nil, fmt.Errorf("plan %s was stored, but no row carries its identifier", planID)
	}
	members, err := s.resolveApplyMembers(ctx, plan, environment, targets)
	if err != nil {
		return nil, fmt.Errorf("pair the rollout members of plan %s with their plans: %w", planID, err)
	}
	if len(members) != len(targets) {
		return nil, fmt.Errorf("pair the rollout members of plan %s with their plans: %d members for %d targets", planID, len(members), len(targets))
	}
	selectors := rolloutMemberSelectors(targets)
	names := applyMemberDisplayNames(members)
	var refusals []*apitypes.PlanMemberRefusalResponse
	for i, member := range members {
		if member.Target.MemberID() != targets[i].MemberID() {
			return nil, fmt.Errorf("pair the rollout members of plan %s with their plans: member %d is %s, not %s", planID, i, member.Target.MemberID(), targets[i].MemberID())
		}
		reason, detail := memberWorkARolloutWideAPIApplyCannotRun(plan, member.Plan)
		if reason == "" {
			continue
		}
		refusals = append(refusals, &apitypes.PlanMemberRefusalResponse{
			Member:      names[i],
			Target:      selectors[i],
			Reason:      reason,
			Detail:      detail,
			AllowUnsafe: len(member.Plan.UnsafeDDLChanges()) > 0 || len(member.Plan.UnsafeVSchemaChanges()) > 0,
		})
	}
	return refusals, nil
}

// memberWorkARolloutWideAPIApplyCannotRun asks of one member's plan what apply
// creation asks of it when the reviewed plan is applied rollout-wide by a
// caller other than a pull request apply-confirm, and returns the refusal
// reason and a description naming only tables and namespaces, or two empty
// strings when apply creation admits it.
//
// A member running the reviewed plan runs exactly what the caller was shown.
// A member with a plan of its own is held to what the reviewed plan's apply
// can run (MemberWorkTheReviewedPlanCannotRun, and
// MemberWorkAConvergedReviewedPlanCannotRun when the reviewed plan is empty),
// and its direct-execution change is refused as well: only a pull request
// comment disclosed it under the target that runs it
// (rejectUnconfirmedMemberDirectExecution). An apply narrowed to the member
// runs its own plan as the reviewed one, so everything but a blocked change
// runs there.
func memberWorkARolloutWideAPIApplyCannotRun(reviewed, member *storage.Plan) (reason, detail string) {
	if member == reviewed {
		return "", ""
	}
	if member.BlockedApplyError() != nil {
		return apitypes.PlanMemberBlocked, "carries changes its target's engine refuses"
	}
	if detail := MemberWorkTheReviewedPlanCannotRun(reviewed, member); detail != "" {
		return apitypes.PlanMemberNeedsTarget, detail
	}
	if !reviewed.HasWork() {
		if detail := MemberWorkAConvergedReviewedPlanCannotRun(member); detail != "" {
			return apitypes.PlanMemberNeedsTarget, detail
		}
	}
	if table := firstDirectExecutionTable(member); table != "" {
		return apitypes.PlanMemberNeedsTarget, fmt.Sprintf("runs table %q as direct-execution DDL, which a rollout-wide apply runs only from the pull request comment that discloses it under this target", table)
	}
	return "", ""
}
