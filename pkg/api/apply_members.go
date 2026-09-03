package api

import (
	"context"
	"fmt"

	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/storage"
)

// memberPlanLookupLimit bounds the plan listing that resolves member plans. A
// review round stores one plan per member, and a PR is re-planned on every push,
// so the listing has to reach back far enough to cover several rounds of a
// wide environment while staying a bounded read.
const memberPlanLookupLimit = 200

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
// member is paired with its own stored plan, matched on the head SHA the apply's
// plan was created for, which binds the member plans to the same review round
// the operator approved.
//
// A member with no plan for that review round fails apply creation. There is no
// safe fallback: the apply's plan describes a different target's schema, so
// substituting it would run DDL that was never planned for this member.
func (s *Service) resolveApplyMembers(ctx context.Context, plan *storage.Plan, environment string, targets []routing.ExecutionTarget) ([]applyMember, error) {
	planning, err := s.config.MemberPlanningFor(plan.Database, environment)
	if err != nil {
		// A database/environment the config no longer resolves cannot be shown to
		// have independently planned members, and mirrored is the shape that reuses
		// one plan for every member. Fail rather than assume it.
		return nil, fmt.Errorf("resolve member planning for %s/%s: %w", plan.Database, environment, err)
	}

	members := make([]applyMember, 0, len(targets))
	if planning == PlanMirrored {
		for _, target := range targets {
			members = append(members, applyMember{Target: target, Plan: plan})
		}
		return members, nil
	}

	memberPlans, err := s.memberPlansForReviewRound(ctx, plan, environment)
	if err != nil {
		return nil, err
	}
	primary := routing.ExecutionTarget{Deployment: plan.Deployment, Target: plan.Target}
	for _, target := range targets {
		// The apply is created from the primary's plan, so the primary needs no
		// lookup — and must not take one, since a re-plan could have stored a newer
		// row for the same member than the plan the operator approved.
		if target.Deployment == primary.Deployment && target.Target == primary.Target {
			members = append(members, applyMember{Target: target, Plan: plan})
			continue
		}
		memberPlan, ok := memberPlans[target.MemberID()]
		if !ok {
			return nil, fmt.Errorf("apply for %s/%s has no stored plan for rollout member %s at head %q; plan the environment again so every target is planned before applying",
				plan.Database, environment, target.MemberID(), plan.HeadSHA)
		}
		members = append(members, applyMember{Target: target, Plan: memberPlan})
	}
	return members, nil
}

// memberPlansForReviewRound loads the member plans stored alongside plan, keyed
// by member id. Plans are listed newest first, so the first row seen for a
// member is that member's latest plan within the round.
//
// The round is identified by the apply plan's head SHA: every member of one
// review round is planned against the same commit, so a plan from an earlier
// push is a different round and must not be picked up. An apply plan with no
// head SHA was not produced by a PR review, which is the only place member plans
// are written, so there is nothing to match it against.
func (s *Service) memberPlansForReviewRound(ctx context.Context, plan *storage.Plan, environment string) (map[string]*storage.Plan, error) {
	if plan.HeadSHA == "" {
		return nil, fmt.Errorf("apply for %s/%s addresses several targets but its plan has no head SHA to match member plans against; plan from a pull request so every target is planned",
			plan.Database, environment)
	}
	stored, err := s.storage.Plans().List(ctx, storage.ListPlansOptions{
		Database:    plan.Database,
		Environment: environment,
		Repository:  plan.Repository,
		PullRequest: plan.PullRequest,
		Limit:       memberPlanLookupLimit,
	})
	if err != nil {
		return nil, fmt.Errorf("list member plans for %s/%s pr %d: %w", plan.Database, environment, plan.PullRequest, err)
	}
	byMember := make(map[string]*storage.Plan, len(stored))
	for _, candidate := range stored {
		if candidate.HeadSHA != plan.HeadSHA {
			continue
		}
		memberID := routing.ExecutionTarget{Deployment: candidate.Deployment, Target: candidate.Target}.MemberID()
		if _, seen := byMember[memberID]; seen {
			continue
		}
		byMember[memberID] = candidate
	}
	return byMember, nil
}
