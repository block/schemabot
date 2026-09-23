package api

import (
	"context"
	"fmt"

	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/storage"
)

// applyMember is one rollout member of an apply together with the plan its work
// is built from. Members of an environment whose members hold the same schema
// all carry the apply's own plan; a member that was planned against its own live
// schema carries its own.
type applyMember struct {
	Target routing.ExecutionTarget
	Plan   *storage.Plan
}

// MemberID names the member for logs and errors.
func (m applyMember) MemberID() string {
	return m.Target.MemberID()
}

// resolveApplyMembers pairs each rollout member with the plan its operations
// must be built from.
//
// When the environment's members are expected to hold the same schema, every
// member runs the reviewed plan and the apply's own plan is used throughout.
//
// When the members were planned independently, each one has a plan of its own
// stored at review time, and running one member's DDL against another's target
// would apply a schema that target was never planned for. So each non-primary
// member is paired with its own stored plan from the same review round.
//
// The round takes precedence over current config. A round that planned its
// members independently stored a plan for each, stamped with the reviewed plan's
// identifier, and those plans are what the operator approved — so they are used
// even if the environment's routing has since been respelled as mirrored.
// Config decides only the case the round is silent about: no member plans stored
// means either that every member was verified against the reviewed plan, or that
// nothing planned the members at all, and only config can tell those apart.
//
// A member with no plan for an independently planned round fails apply creation.
// There is no safe fallback: the apply's plan describes a different target's
// schema, so substituting it would run DDL that was never planned for this
// member.
func (s *Service) resolveApplyMembers(ctx context.Context, plan *storage.Plan, environment string, targets []routing.ExecutionTarget) ([]applyMember, error) {
	// A single member is the plan's own primary, so it runs the apply's plan
	// under either contract and there is no sibling whose plan could be
	// substituted for it. Deciding the contract first would make apply creation
	// depend on database config that the trusted control-plane enqueue path is
	// not required to have — the same reason the caller falls back to the plan's
	// stored target when config does not resolve the environment.
	if len(targets) == 1 {
		return []applyMember{{Target: targets[0], Plan: plan}}, nil
	}

	memberPlans, err := s.memberPlansForReviewRound(ctx, plan, environment)
	if err != nil {
		return nil, err
	}

	members := make([]applyMember, 0, len(targets))
	if len(memberPlans) == 0 {
		planning, err := s.config.MemberPlanningFor(plan.Database, environment)
		if err != nil {
			// A database/environment the config no longer resolves cannot be shown
			// to have had its members verified against the reviewed plan, and that
			// is the only shape in which one plan runs everywhere. Fail rather than
			// assume it.
			return nil, fmt.Errorf("resolve member planning for %s/%s: %w", plan.Database, environment, err)
		}
		if planning == PlanMirrored {
			for _, target := range targets {
				members = append(members, applyMember{Target: target, Plan: plan})
			}
			return members, nil
		}
	}

	primary := routing.ExecutionTarget{Deployment: plan.Deployment, Target: plan.Target}
	for _, target := range targets {
		// The apply is created from the primary's plan, so the primary needs no
		// lookup — and must not take one, since it stores no plan of its own.
		if target.Deployment == primary.Deployment && target.Target == primary.Target {
			members = append(members, applyMember{Target: target, Plan: plan})
			continue
		}
		memberPlan, ok := memberPlans[target.MemberID()]
		if !ok {
			if plan.HeadSHA == "" {
				// Member plans are only ever written by a pull request review, so
				// an apply from a plan that had none is unplannable by
				// construction rather than by a missed round.
				return nil, fmt.Errorf("apply for %s/%s has no stored plan for rollout member %s, and its plan %s was not produced by a pull request review; plan from a pull request so every target is planned",
					plan.Database, environment, target.MemberID(), plan.PlanIdentifier)
			}
			return nil, fmt.Errorf("apply for %s/%s has no stored plan for rollout member %s in the reviewed round (plan %s); plan the environment again so every target is planned before applying",
				plan.Database, environment, target.MemberID(), plan.PlanIdentifier)
		}
		members = append(members, applyMember{Target: target, Plan: memberPlan})
	}
	return members, nil
}

// memberPlansForReviewRound loads the plans stored for the members of this
// apply's own review round, keyed by member id.
//
// The round is identified by the reviewed plan's identifier, which every member
// plan is stamped with at review time. A commit can be planned more than once —
// a re-plan, or two deliveries racing — and each round writes a plan per member
// carrying the same database, environment, repository, pull request, route, and
// head SHA. The stamp is the only thing that separates them, so selecting on it
// is what keeps an apply from dispatching a member plan the operator never saw.
//
// An empty result means the round planned no member on its own, which is how a
// mirrored round reads: it is the answer, not a lookup that came up short.
//
// The listing takes no row cap. Naming the round is what bounds it, and its
// members are exactly what this lookup must see: a cap could only cut members
// off the end, and a member the listing dropped is indistinguishable here from
// one the round never planned, which is reported to the operator as an
// unplanned target and sends them to re-plan a round that was planned fine.
func (s *Service) memberPlansForReviewRound(ctx context.Context, plan *storage.Plan, environment string) (map[string]*storage.Plan, error) {
	if plan.PlanIdentifier == "" {
		return nil, fmt.Errorf("apply for %s/%s addresses several targets but its plan has no identifier to match member plans against",
			plan.Database, environment)
	}
	stored, err := s.storage.Plans().List(ctx, storage.ListPlansOptions{
		Database:              plan.Database,
		Environment:           environment,
		Repository:            plan.Repository,
		PullRequest:           plan.PullRequest,
		PrimaryPlanIdentifier: plan.PlanIdentifier,
	})
	if err != nil {
		return nil, fmt.Errorf("list member plans for %s/%s round %s: %w", plan.Database, environment, plan.PlanIdentifier, err)
	}
	byMember := make(map[string]*storage.Plan, len(stored))
	for _, candidate := range stored {
		memberID := routing.ExecutionTarget{Deployment: candidate.Deployment, Target: candidate.Target}.MemberID()
		if _, seen := byMember[memberID]; seen {
			// One round stores one plan per member. A second row for the same
			// member means the round was written twice; the listing is newest
			// first, so the later write is the one the round ended with.
			continue
		}
		byMember[memberID] = candidate
	}
	return byMember, nil
}
