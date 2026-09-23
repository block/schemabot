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
//
// Every member plan is stamped with the reviewed plan's identifier, which is
// what durably binds it to this review round. A commit can be planned more than
// once — a re-plan, or two deliveries racing — and each round stores its own row
// per member with the same route and the same head SHA. Without the stamp an
// apply created from one round's reviewed plan could pair its members with
// another round's plans, dispatching DDL the operator never saw.
func (s *Service) persistMemberPlans(ctx context.Context, req PlanRequest, planning MemberPlanning, primaryPlanIdentifier string, diffs []DeploymentPlanDiff, rollup *PlanRollup) error {
	if planning != PlanIndependent {
		return nil
	}
	if len(diffs) != len(rollup.Entries) {
		return fmt.Errorf("persist member plans for %s/%s: %d member diffs for %d rollup entries", req.Database, req.Environment, len(diffs), len(rollup.Entries))
	}
	if primaryPlanIdentifier == "" {
		// A member plan that cannot be attributed to a review round is exactly
		// what the stamp exists to prevent, so it is not stored at all.
		return fmt.Errorf("persist member plans for %s/%s: the reviewed plan has no identifier to bind member plans to", req.Database, req.Environment)
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
			DatabaseType:          entry.DatabaseType,
			Deployment:            entry.Deployment,
			Target:                entry.Target,
			PrimaryPlanIdentifier: primaryPlanIdentifier,
			DirectExecution:       resolvedDirectExecution(diffs[i].DirectExecution),
		}
		if err := s.storePlan(ctx, req, planIdentifier, diffs[i].Changes, diffs[i].Shards, route); err != nil {
			s.logger.Error("failed to store a rollout member's plan; the member will block the review because an apply would have no plan to run for it",
				"repository", req.Repository,
				"database", req.Database,
				"database_type", entry.DatabaseType,
				"environment", req.Environment,
				"deployment", entry.Deployment,
				"target", entry.Target,
				"plan_id", planIdentifier,
				"error", err)
			entry.markErrored(fmt.Errorf("store plan for rollout member %s: %w", member.MemberID(), err))
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
