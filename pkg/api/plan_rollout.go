package api

import (
	"context"
	"fmt"
	"slices"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
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
	primary := routing.ExecutionTarget{Deployment: planResp.Deployment, Target: planResp.Target}
	if primary.MemberID() != targets[0].MemberID() {
		return nil, fmt.Errorf("plan %s for %s/%s was made for rollout member %s, not the rollout primary %s; the other members are planned only beside the primary's plan",
			primaryPlan.GetPlanId(), req.Database, req.Environment, primary.MemberID(), targets[0].MemberID())
	}
	rollup, err := s.planRolloutMembers(ctx, req, primaryPlan, primary, targets)
	if err != nil {
		return nil, err
	}
	rollout := planRolloutResponse(rollup)
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
		"members_needing_attention", len(rollout.Attention))
	return rollout, nil
}

// planRolloutResponse groups a rollout's members by the plan each would run,
// one group per distinct plan with the primary's first, and lists the members
// an apply cannot run on as planned.
//
// Members are grouped on the plan fingerprint, which two members share exactly
// when their plans are the same work. A member that errored has no fingerprint
// and no plan, and a mirrored member that diverged would still run the
// primary's plan, so neither joins a group: each is listed for attention.
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
		group, ok := byPlan[e.PlanFingerprint]
		if !ok {
			plan := planResponseFromProto(&ternv1.PlanResponse{Changes: e.ChangeSet.Changes, Shards: e.ChangeSet.Shards})
			group = &apitypes.PlanMemberGroupResponse{Primary: i == 0, Changes: plan.Changes, Shards: plan.Shards}
			byPlan[e.PlanFingerprint] = group
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
