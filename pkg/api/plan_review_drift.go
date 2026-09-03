package api

import (
	"context"
	"fmt"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"

	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/tern"
)

// RollupReviewTimeDrift computes the review-time drift rollup for a
// database/environment: it diffs every configured deployment against the
// reviewed primary plan and classifies each as match, diverged, or errored.
//
// primaryPlan is the just-reviewed primary plan proto, reused as the rollup's
// baseline so the comparison is against exactly what the user reviewed rather
// than a fresh read of the primary's live schema (which could have drifted and
// tripped a spurious primary-vs-primary mismatch). primaryMember is the
// deployment and target that plan was created against; the producer fails
// closed if that pair no longer maps to rollout index 0 at rollup time.
//
// The database/environment is resolved once here to the configured member set
// in rollout order, then shared with the producer. The resolved order is also
// passed to RollupDeploymentDiffs as the expected set so the rollup can enforce
// that the producer returned one diff per member in that order — a missing,
// extra, or reordered result fails closed rather than being mistaken for
// agreement. The returned rollup is Clean only when every member matches.
func (s *Service) RollupReviewTimeDrift(ctx context.Context, req PlanRequest, primaryPlan *ternv1.PlanResponse, primaryMember routing.ExecutionTarget) (PlanRollup, error) {
	targets, err := s.config.ResolveDatabaseTargets(req.Database, req.Environment)
	if err != nil {
		return PlanRollup{}, fmt.Errorf("resolve deployment targets for %s/%s: %w", req.Database, req.Environment, err)
	}

	planning, err := s.config.MemberPlanningFor(req.Database, req.Environment)
	if err != nil {
		return PlanRollup{}, fmt.Errorf("resolve member planning for %s/%s: %w", req.Database, req.Environment, err)
	}

	diffs, err := s.PlanDeploymentDiffs(ctx, req, primaryPlan, primaryMember, targets)
	if err != nil {
		return PlanRollup{}, fmt.Errorf("plan deployment diffs for %s/%s: %w", req.Database, req.Environment, err)
	}

	rollup, err := RollupDeploymentDiffs(diffs, targets, planning)
	if err != nil {
		return PlanRollup{}, fmt.Errorf("roll up deployment diffs for %s/%s: %w", req.Database, req.Environment, err)
	}

	// Members planned on their own each need a plan row of their own, since an
	// apply has no single plan that covers them. Storing them here, before the
	// rollup is reported, keeps "the review says this member is fine" and "this
	// member has a plan to run" from being separately true.
	if err := s.persistMemberPlans(ctx, req, planning, diffs, &rollup); err != nil {
		return PlanRollup{}, fmt.Errorf("persist member plans for %s/%s: %w", req.Database, req.Environment, err)
	}

	// Include repo/pr/head SHA so an operator can tell which PR is blocked from
	// the drift warn log alone.
	var pr int32
	if req.PullRequest != nil {
		pr = *req.PullRequest
	}
	var headSHA string
	if req.HeadSHA != nil {
		headSHA = *req.HeadSHA
	}
	for _, entry := range rollup.Entries {
		metrics.RecordReviewDrift(ctx, req.Database, req.Environment, entry.Deployment, entry.Class.String())
		switch entry.Class {
		case DeploymentDiverged:
			s.logger.Warn("review-time drift: deployment diverged from the reviewed plan; the plan check will block the PR until reconciled",
				append([]any{
					"repository", req.Repository,
					"pr", pr,
					"head_sha", headSHA,
					"database", req.Database,
					"environment", req.Environment,
					"deployment", entry.Deployment,
					"target", entry.Target,
				}, driftDiffLogAttrs(entry.Diff)...)...)
		case DeploymentErrored:
			s.logger.Warn("review-time drift: deployment could not be diffed or compared; the plan check will block the PR closed",
				"repository", req.Repository,
				"pr", pr,
				"head_sha", headSHA,
				"database", req.Database,
				"environment", req.Environment,
				"deployment", entry.Deployment,
				"target", entry.Target,
				"member_planning", planning.String(),
				"error", entry.Err)
		}
	}

	return rollup, nil
}

// maxDriftDiffLogItems caps how many diff items a diverged deployment's warn log
// renders per list, so a badly drifted deployment cannot flood the log line.
// The omitted remainder is summarized as a "+N more" tail.
const maxDriftDiffLogItems = 5

// driftDiffLogAttrs renders a diverged deployment's change-set diff as log
// attributes so an operator can tell from the warn log alone what the
// deployment would run that the reviewed plan does not say (and vice versa).
// Each item names the namespace, shard when set, table, and operation — never
// the DDL body, which can be long and belongs on the producer's own logs.
// Empty lists are omitted.
func driftDiffLogAttrs(diff tern.ChangeSetDiff) []any {
	var attrs []any
	if items := formatDriftDiffItems(diff.MissingFromCandidate); len(items) > 0 {
		attrs = append(attrs, "diff_missing", items)
	}
	if items := formatDriftDiffItems(diff.UnexpectedInCandidate); len(items) > 0 {
		attrs = append(attrs, "diff_unexpected", items)
	}
	if ns := capDriftList(diff.MissingVSchema); len(ns) > 0 {
		attrs = append(attrs, "diff_missing_vschema", ns)
	}
	if ns := capDriftList(diff.UnexpectedVSchema); len(ns) > 0 {
		attrs = append(attrs, "diff_unexpected_vschema", ns)
	}
	return attrs
}

// formatDriftDiffItems renders diff items as "namespace[/shard].table operation"
// strings, capped to maxDriftDiffLogItems with a "+N more" overflow entry.
func formatDriftDiffItems(items []tern.ChangeSetDiffItem) []string {
	formatted := make([]string, 0, min(len(items), maxDriftDiffLogItems+1))
	for _, item := range items[:min(len(items), maxDriftDiffLogItems)] {
		loc := item.Namespace
		if item.Shard != "" {
			loc += "/" + item.Shard
		}
		formatted = append(formatted, fmt.Sprintf("%s.%s %s", loc, item.Table, item.Operation))
	}
	if overflow := len(items) - maxDriftDiffLogItems; overflow > 0 {
		formatted = append(formatted, fmt.Sprintf("+%d more", overflow))
	}
	return formatted
}

// capDriftList caps a namespace list to maxDriftDiffLogItems with a "+N more"
// overflow entry.
func capDriftList(values []string) []string {
	if len(values) <= maxDriftDiffLogItems {
		return values
	}
	capped := make([]string, 0, maxDriftDiffLogItems+1)
	capped = append(capped, values[:maxDriftDiffLogItems]...)
	capped = append(capped, fmt.Sprintf("+%d more", len(values)-maxDriftDiffLogItems))
	return capped
}
