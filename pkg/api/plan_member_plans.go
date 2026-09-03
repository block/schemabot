package api

import (
	"context"
	"fmt"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/routing"
)

// persistMemberPlans stores one plan row per non-primary rollout member that was
// planned against its own live schema, and records each stored plan's identifier
// on its rollup entry.
//
// It does work only under PlanIndependent. Under PlanMirrored every member is
// expected to run exactly the reviewed changes, which the primary's plan row
// already holds, so there is no second plan to store.
//
// The primary member is deliberately left without a plan identifier of its own.
// Its plan is the reviewed plan, already stored, and is the plan the apply is
// created from — so the primary's work runs its apply's plan, which is what an
// operation with no plan of its own already means.
//
// A member whose plan cannot be stored is reclassified as errored and blocks the
// review. An apply dispatches each member against its stored plan, so a member
// with no stored plan has nothing to run; letting the rollup stay clean would
// gate the PR on a member that could not have been applied.
func (s *Service) persistMemberPlans(ctx context.Context, req PlanRequest, planning MemberPlanning, diffs []DeploymentPlanDiff, rollup *PlanRollup) error {
	if planning != PlanIndependent {
		return nil
	}
	if len(diffs) != len(rollup.Entries) {
		return fmt.Errorf("persist member plans for %s/%s: %d member diffs for %d rollup entries", req.Database, req.Environment, len(diffs), len(rollup.Entries))
	}

	// Index 0 is the primary, whose reviewed plan is already stored.
	for i := 1; i < len(rollup.Entries); i++ {
		entry := &rollup.Entries[i]
		member := routing.ExecutionTarget{Deployment: entry.Deployment, Target: entry.Target}
		if entry.Class != DeploymentPlanned {
			s.logger.Debug("rollout member produced no usable plan to store; it already blocks the review",
				"database", req.Database,
				"environment", req.Environment,
				"member", member.MemberID(),
				"class", entry.Class.String())
			continue
		}

		// The plan came from the non-persisting diff RPC, so it arrived without an
		// identifier and gets one minted here.
		planIdentifier := engine.NewPlanID()
		route := storedPlanRoute{
			DatabaseType: entry.DatabaseType,
			Deployment:   entry.Deployment,
			Target:       entry.Target,
		}
		if _, err := s.storePlan(ctx, req, planIdentifier, diffs[i].Changes, diffs[i].Shards, route); err != nil {
			s.logger.Error("failed to store a rollout member's plan; the member will block the review because an apply would have no plan to run for it",
				"repository", req.Repository,
				"database", req.Database,
				"database_type", entry.DatabaseType,
				"environment", req.Environment,
				"deployment", entry.Deployment,
				"target", entry.Target,
				"plan_id", planIdentifier,
				"error", err)
			entry.Class = DeploymentErrored
			entry.Err = fmt.Errorf("store plan for rollout member %s: %w", member.MemberID(), err)
			rollup.Clean = false
			continue
		}

		entry.PlanIdentifier = planIdentifier
		s.logger.Info("stored plan for rollout member",
			"repository", req.Repository,
			"database", req.Database,
			"environment", req.Environment,
			"deployment", entry.Deployment,
			"target", entry.Target,
			"plan_id", planIdentifier)
	}
	return nil
}
