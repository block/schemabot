package api

import (
	"context"
	"fmt"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"

	"github.com/block/schemabot/pkg/metrics"
)

// RollupReviewTimeDrift computes the review-time drift rollup for a
// database/environment: it diffs every configured deployment against the
// reviewed primary plan and classifies each as match, diverged, or errored.
//
// primaryPlan is the just-reviewed primary plan proto, reused as the rollup's
// baseline so the comparison is against exactly what the user reviewed rather
// than a fresh read of the primary's live schema (which could have drifted and
// tripped a spurious primary-vs-primary mismatch).
//
// The expected deployment set is resolved independently from the producer's
// results so the rollup can enforce that the diffs cover every configured
// deployment in rollout order — a missing, extra, or reordered deployment fails
// closed rather than being mistaken for agreement. The returned rollup is Clean
// only when every deployment matches.
func (s *Service) RollupReviewTimeDrift(ctx context.Context, req PlanRequest, primaryPlan *ternv1.PlanResponse) (PlanRollup, error) {
	targets, err := s.config.ResolveDatabaseTargets(req.Database, req.Environment)
	if err != nil {
		return PlanRollup{}, fmt.Errorf("resolve deployment targets for %s/%s: %w", req.Database, req.Environment, err)
	}
	expectedDeployments := make([]string, len(targets))
	for i, t := range targets {
		expectedDeployments[i] = t.Deployment
	}

	diffs, err := s.PlanDeploymentDiffs(ctx, req, primaryPlan)
	if err != nil {
		return PlanRollup{}, fmt.Errorf("plan deployment diffs for %s/%s: %w", req.Database, req.Environment, err)
	}

	rollup, err := RollupDeploymentDiffs(diffs, expectedDeployments)
	if err != nil {
		return PlanRollup{}, fmt.Errorf("roll up deployment diffs for %s/%s: %w", req.Database, req.Environment, err)
	}

	for _, entry := range rollup.Entries {
		metrics.RecordReviewDrift(ctx, req.Database, req.Environment, entry.Deployment, entry.Class.String())
		switch entry.Class {
		case DeploymentDiverged:
			s.logger.Warn("review-time drift: deployment diverged from the reviewed plan; the plan check will block the PR until reconciled",
				"database", req.Database,
				"environment", req.Environment,
				"deployment", entry.Deployment,
				"target", entry.Target)
		case DeploymentErrored:
			s.logger.Warn("review-time drift: deployment could not be diffed or compared; the plan check will block the PR closed",
				"database", req.Database,
				"environment", req.Environment,
				"deployment", entry.Deployment,
				"target", entry.Target,
				"error", entry.Err)
		}
	}

	return rollup, nil
}
