package api

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/block/schemabot/pkg/metrics"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
)

// planDeploymentDiffConcurrency bounds how many deployments are diffed at once.
// Each deployment diff runs one live-schema read against a (often remote)
// deployment; a small cap rides out multi-deployment fan-out without opening an
// unbounded number of concurrent connections across regions.
const planDeploymentDiffConcurrency = 4

// DeploymentPlanDiff is one deployment's desired-vs-live diff computed at review
// time, or the error that prevented computing it. It is the per-deployment
// producer output the control-plane rollup compares across a database's
// deployments to surface drift before approval. The rollup must treat any Err —
// or a deployment missing from the results — as blocking, never as agreement.
type DeploymentPlanDiff struct {
	DatabaseType string
	Deployment   string
	Target       string

	Engine         ternv1.Engine
	Changes        []*ternv1.SchemaChange
	Shards         []*ternv1.ShardPlan
	LintViolations []*ternv1.LintViolation

	Err error
}

// PlanDeploymentDiffs computes every configured deployment's desired-vs-live
// diff for a database/environment at review time. Only the primary deployment
// plans locally today; the non-primary deployments never do, so drift on them is
// invisible until apply. This is the producer that closes that gap: each
// non-primary deployment is diffed with the non-persisting PlanDiff RPC, and the
// primary reuses the already-persisted reviewed plan (primaryPlan) so the rollup
// compares against exactly what the user reviewed rather than re-reading the
// primary's live schema — which could differ from the reviewed plan and trip a
// spurious primary-vs-primary mismatch.
//
// Per-deployment failures are captured in each result's Err so one unreachable
// deployment neither hides the others nor aborts the rollup; only a
// request-level failure (target resolution) returns an error. Results are
// returned in rollout order, primary first.
func (s *Service) PlanDeploymentDiffs(ctx context.Context, req PlanRequest, primaryPlan *ternv1.PlanResponse) ([]DeploymentPlanDiff, error) {
	targets, err := s.config.ResolveDatabaseTargets(req.Database, req.Environment)
	if err != nil {
		return nil, fmt.Errorf("resolve deployment targets for %s/%s: %w", req.Database, req.Environment, err)
	}

	results := make([]DeploymentPlanDiff, len(targets))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(planDeploymentDiffConcurrency)

	for i, target := range targets {
		results[i] = DeploymentPlanDiff{
			DatabaseType: target.DatabaseType,
			Deployment:   target.Deployment,
			Target:       target.Target,
		}

		// Rollout index 0 is the primary. ResolveDatabaseTargets returns targets
		// in rollout order, primary first, so the primary is positionally index 0;
		// a future change to that ordering would misattribute the primary here and
		// must keep primary-first. When its reviewed plan is provided, reuse it so
		// the rollup's primary member is exactly what was reviewed and no redundant
		// live-schema read runs.
		if i == 0 && primaryPlan != nil {
			results[i].Engine = primaryPlan.Engine
			results[i].Changes = primaryPlan.Changes
			results[i].Shards = primaryPlan.Shards
			results[i].LintViolations = primaryPlan.LintViolations
			// A reviewed plan that reported planning errors is not a trustworthy
			// baseline; record the error so the rollup fails closed rather than
			// comparing deployments against a broken primary.
			if len(primaryPlan.Errors) > 0 {
				results[i].Err = fmt.Errorf("reviewed primary plan reported errors: %v", primaryPlan.Errors)
			}
			continue
		}

		g.Go(func() error {
			resp, err := s.planDeploymentDiff(gctx, req, target)
			if err != nil {
				s.logger.Warn("plan deployment diff failed; deployment will block the review rollup",
					"database", req.Database,
					"environment", req.Environment,
					"deployment", target.Deployment,
					"target", target.Target,
					"error", err)
				metrics.RecordDeploymentDiff(gctx, req.Database, target.Deployment, req.Environment, "errored")
				results[i].Err = err
				return nil
			}
			results[i].Engine = resp.Engine
			results[i].Changes = resp.Changes
			results[i].Shards = resp.Shards
			results[i].LintViolations = resp.LintViolations
			// A diff that succeeded at the RPC layer but reported planning errors
			// is not a trustworthy comparison input; block on it so the rollup
			// never mistakes an incomplete diff for agreement.
			if len(resp.Errors) > 0 {
				diffErr := fmt.Errorf("plan diff on deployment %q target %q reported errors: %v", target.Deployment, target.Target, resp.Errors)
				s.logger.Warn("plan deployment diff reported errors; deployment will block the review rollup",
					"database", req.Database,
					"environment", req.Environment,
					"deployment", target.Deployment,
					"target", target.Target,
					"error", diffErr)
				metrics.RecordDeploymentDiff(gctx, req.Database, target.Deployment, req.Environment, "errored")
				results[i].Err = diffErr
				return nil
			}
			metrics.RecordDeploymentDiff(gctx, req.Database, target.Deployment, req.Environment, "ok")
			return nil
		})
	}

	// Deployment failures are captured per result, so Wait only returns non-nil
	// if the parent context was cancelled.
	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("plan deployment diffs for %s/%s: %w", req.Database, req.Environment, err)
	}
	return results, nil
}

// planDeploymentDiff runs the non-persisting PlanDiff RPC against a single
// deployment, building the per-target request the same way ExecutePlan builds
// the primary's plan request.
func (s *Service) planDeploymentDiff(ctx context.Context, req PlanRequest, target routing.ExecutionTarget) (*ternv1.PlanDiffResponse, error) {
	client, err := s.TernClient(target.Deployment, req.Environment)
	if err != nil {
		return nil, fmt.Errorf("tern client for deployment %q environment %q: %w", target.Deployment, req.Environment, err)
	}

	trustedSchemaPath := ""
	if req.SourceTrusted {
		trustedSchemaPath = req.SchemaPath
	}
	ternReq := &ternv1.PlanRequest{
		Database:    req.Database,
		Type:        target.DatabaseType,
		SchemaFiles: req.SchemaFiles,
		Repository:  req.Repository,
		Environment: req.Environment,
		Target:      target.Target,
		SchemaPath:  trustedSchemaPath,
	}
	if req.PullRequest != nil {
		ternReq.PullRequest = *req.PullRequest
	}
	if req.HeadSHA != nil {
		ternReq.HeadSha = *req.HeadSHA
	}

	resp, err := client.PlanDiff(ctx, ternReq)
	if err != nil {
		return nil, fmt.Errorf("plan diff on deployment %q target %q: %w", target.Deployment, target.Target, err)
	}
	return resp, nil
}
