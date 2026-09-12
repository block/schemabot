package webhook

import (
	"context"
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/api"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// reviewTimeDrift computes the review-time drift rollup for a database and
// environment and turns it into the check-record outcome plus the preview
// rendering of that rollup.
//
// It returns a clean outcome when every deployment matches the reviewed plan,
// and a blocked outcome on any divergence, a deployment that could not be diffed
// or compared, or a failure to compute the rollup at all: without a trustworthy
// comparison SchemaBot cannot confirm the reviewed change is safe to apply
// everywhere, so the plan check fails closed.
//
// The primary plan reporting errors is not a drift signal — that generic plan
// failure already fails the check on its own — so the rollup is skipped and the
// outcome is not-evaluated, avoiding N-1 live per-deployment diffs whose result
// the rollup would classify entirely from the unusable baseline anyway.
//
// On a successful rollup the preview data is nil for a single-deployment
// database (nothing to compare) and non-nil otherwise, so the PR comment can
// show a uniform "same plan everywhere" line or a per-deployment breakdown even
// when the reviewed primary plan is a clean no-op. On a rollup failure the
// preview is always non-nil (Computed:false), regardless of deployment count,
// so the PR comment explains why the check is failing closed.
//
// primaryPlan is the reviewed primary plan proto returned by
// executePlanProtoWithTransientRetry, reused as the rollup baseline so the
// comparison is against exactly what was reviewed. primaryMember is the
// deployment and target that plan was created against.
func (h *Handler) reviewTimeDrift(ctx context.Context, planReq api.PlanRequest, primaryPlan *ternv1.PlanResponse, primaryMember routing.ExecutionTarget, repo string, pr int) (reviewDriftOutcome, *templates.DeploymentDriftData) {
	if len(primaryPlan.GetErrors()) > 0 {
		h.logger.Debug("skipping review-time drift rollup: primary plan reported errors",
			"repo", repo, "pr", pr, "database", planReq.Database, "environment", planReq.Environment)
		return reviewDriftOutcome{state: driftNotEvaluated}, nil
	}

	rollup, err := h.service.RollupReviewTimeDrift(ctx, planReq, primaryPlan, primaryMember)
	if err != nil {
		h.logger.Error("review-time drift rollup failed; the plan check will block the PR closed",
			"repo", repo,
			"pr", pr,
			"database", planReq.Database,
			"environment", planReq.Environment,
			"error", err)
		// Keep the stored Change column short and stable; the root cause is in the
		// log above, and the raw error is untrusted for the aggregate's markdown.
		// The preview says drift could not be verified so reviewers see why the
		// check is failing closed.
		return reviewDriftOutcome{
			state:   driftBlocked,
			summary: "drift check failed; see logs",
		}, &templates.DeploymentDriftData{Computed: false}
	}
	preview := deploymentDriftPreview(rollup)
	if rollup.Clean {
		return reviewDriftOutcome{state: driftClean}, preview
	}
	return reviewDriftOutcome{
		state:   driftBlocked,
		summary: summarizeReviewDrift(rollup),
	}, preview
}

// erroredDriftDetail is the sanitized detail shown in the PR preview for a
// deployment whose diff could not be computed. The raw error is untrusted for
// PR markdown (it can carry internal hostnames, IPs, or DSN fragments) and is
// logged server-side instead.
const erroredDriftDetail = "diff failed; see server logs"

// deploymentDriftPreview turns a computed rollup into the PR-preview rendering
// data. It returns nil for a single-deployment database: with one deployment
// there is nothing to compare, so a "same plan everywhere" line would be noise.
func deploymentDriftPreview(rollup api.PlanRollup) *templates.DeploymentDriftData {
	if len(rollup.Entries) <= 1 {
		return nil
	}
	entries := make([]templates.DeploymentDriftEntry, len(rollup.Entries))
	for i, e := range rollup.Entries {
		entry := templates.DeploymentDriftEntry{
			Deployment: e.Deployment,
			Target:     e.Target,
			Primary:    i == 0,
			Class:      e.Class.String(),
		}
		switch e.Class {
		case api.DeploymentDiverged:
			entry.Detail = describeDriftDiff(e.Diff)
		case api.DeploymentErrored:
			// The raw diff error can wrap dial failures carrying internal
			// hostnames, IPs, or DSN fragments, so it stays out of the PR
			// markdown. The root cause is logged with the deployment in
			// RollupReviewTimeDrift; the preview shows only a sanitized line.
			entry.Detail = erroredDriftDetail
		}
		entries[i] = entry
	}
	return &templates.DeploymentDriftData{
		Deployments: entries,
		Clean:       rollup.Clean,
		Computed:    true,
		Independent: rollup.Planning == api.PlanIndependent,
	}
}

// describeDriftDiff renders a short, count-based summary of how a diverged
// deployment differs from the reviewed plan, e.g. "1 unexpected, 2 missing
// changes". The full DDL is intentionally omitted to keep the preview compact;
// the operator reconciles by replanning the diverged deployment.
func describeDriftDiff(diff tern.ChangeSetDiff) string {
	var parts []string
	if n := len(diff.UnexpectedInCandidate); n > 0 {
		parts = append(parts, fmt.Sprintf("%d unexpected", n))
	}
	if n := len(diff.MissingFromCandidate); n > 0 {
		parts = append(parts, fmt.Sprintf("%d missing", n))
	}
	if n := len(diff.UnexpectedVSchema); n > 0 {
		parts = append(parts, fmt.Sprintf("%d unexpected vschema", n))
	}
	if n := len(diff.MissingVSchema); n > 0 {
		parts = append(parts, fmt.Sprintf("%d missing vschema", n))
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, ", ") + " change(s) vs the reviewed plan"
}

// rollupMemberNames renders each rollup entry the way an operator addresses it,
// index-parallel to rollup.Entries.
func rollupMemberNames(rollup api.PlanRollup) []string {
	members := make([]routing.ExecutionTarget, len(rollup.Entries))
	for i, e := range rollup.Entries {
		members[i] = routing.ExecutionTarget{Deployment: e.Deployment, Target: e.Target}
	}
	return routing.DisplayNames(members)
}

// maxDriftSummaryLen bounds the stored drift summary to the checks table's
// change_summary column width. The summary is truncated on a rune boundary so it
// never exceeds the column or splits a multibyte character.
const maxDriftSummaryLen = 255

// summarizeReviewDrift builds the concise operator-facing reason a review-time
// drift rollup blocked the plan check. It names the deployments that diverged
// from the reviewed plan and those that could not be diffed or compared, so the
// check's Change column tells an operator exactly which deployment to reconcile.
func summarizeReviewDrift(rollup api.PlanRollup) string {
	independent := rollup.Planning == api.PlanIndependent
	// One deployment can address several targets, so the deployment name alone
	// does not always say which member failed. The shared naming rule adds the
	// target only where it disambiguates.
	names := rollupMemberNames(rollup)
	var diverged, errored []string
	for i, entry := range rollup.Entries {
		switch entry.Class {
		case api.DeploymentDiverged:
			diverged = append(diverged, names[i])
		case api.DeploymentErrored:
			errored = append(errored, names[i])
		}
	}

	var parts []string
	if len(diverged) > 0 {
		parts = append(parts, fmt.Sprintf("diverged: %s", strings.Join(diverged, ", ")))
	}
	if len(errored) > 0 {
		// An errored member means different things under the two contracts: a
		// mirrored member's diff could not be confirmed against the reviewed plan,
		// while an independent member has no plan of its own at all.
		reason := "could not verify"
		if independent {
			reason = "could not plan"
		}
		parts = append(parts, fmt.Sprintf("%s: %s", reason, strings.Join(errored, ", ")))
	}
	if len(parts) == 0 {
		// A not-clean rollup always has at least one non-passing entry; guard
		// anyway so the check never records an empty, uninformative reason.
		if independent {
			return "blocks apply: not every target could be planned"
		}
		return "drift blocks apply: deployments differ from the reviewed plan"
	}
	// Targets that hold their own schemas are never expected to agree, so their
	// failure is an unplanned target, not drift between them.
	if independent {
		return clampDriftSummary("blocks apply — " + strings.Join(parts, "; "))
	}
	return clampDriftSummary("drift blocks apply — " + strings.Join(parts, "; "))
}

// clampDriftSummary makes a drift summary safe to store in the checks table's
// change_summary column and to render in the aggregate check's markdown: it
// collapses newlines, neutralizes the table cell separator, and truncates on a
// rune boundary to the column width.
func clampDriftSummary(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "|", "/")
	runes := []rune(s)
	if len(runes) > maxDriftSummaryLen {
		return string(runes[:maxDriftSummaryLen-1]) + "…"
	}
	return s
}
