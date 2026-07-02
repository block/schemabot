package webhook

import (
	"context"
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/api"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// reviewTimeDrift computes the review-time drift rollup for a database and
// environment and turns it into the check-record outcome. It returns a clean
// outcome when every deployment matches the reviewed plan, and a blocked outcome
// on any divergence, a deployment that could not be diffed or compared, or a
// failure to compute the rollup at all: without a trustworthy comparison
// SchemaBot cannot confirm the reviewed change is safe to apply everywhere, so
// the plan check fails closed.
//
// The primary plan reporting errors is not a drift signal — that generic plan
// failure already fails the check on its own — so the rollup is skipped and the
// outcome is not-evaluated, avoiding N-1 live per-deployment diffs whose result
// the rollup would classify entirely from the unusable baseline anyway.
//
// primaryPlan is the reviewed primary plan proto returned by
// executePlanProtoWithTransientRetry, reused as the rollup baseline so the
// comparison is against exactly what was reviewed.
func (h *Handler) reviewTimeDrift(ctx context.Context, planReq api.PlanRequest, primaryPlan *ternv1.PlanResponse, repo string, pr int) reviewDriftOutcome {
	if len(primaryPlan.GetErrors()) > 0 {
		h.logger.Debug("skipping review-time drift rollup: primary plan reported errors",
			"repo", repo, "pr", pr, "database", planReq.Database, "environment", planReq.Environment)
		return reviewDriftOutcome{state: driftNotEvaluated}
	}

	rollup, err := h.service.RollupReviewTimeDrift(ctx, planReq, primaryPlan)
	if err != nil {
		h.logger.Error("review-time drift rollup failed; the plan check will block the PR closed",
			"repo", repo,
			"pr", pr,
			"database", planReq.Database,
			"environment", planReq.Environment,
			"error", err)
		// Keep the stored Change column short and stable; the root cause is in the
		// log above, and the raw error is untrusted for the aggregate's markdown.
		return reviewDriftOutcome{
			state:   driftBlocked,
			summary: "drift check failed; see logs",
		}
	}
	if rollup.Clean {
		return reviewDriftOutcome{state: driftClean}
	}
	return reviewDriftOutcome{
		state:   driftBlocked,
		summary: summarizeReviewDrift(rollup),
	}
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
	var diverged, errored []string
	for _, entry := range rollup.Entries {
		switch entry.Class {
		case api.DeploymentDiverged:
			diverged = append(diverged, entry.Deployment)
		case api.DeploymentErrored:
			errored = append(errored, entry.Deployment)
		}
	}

	var parts []string
	if len(diverged) > 0 {
		parts = append(parts, fmt.Sprintf("diverged: %s", strings.Join(diverged, ", ")))
	}
	if len(errored) > 0 {
		parts = append(parts, fmt.Sprintf("could not verify: %s", strings.Join(errored, ", ")))
	}
	if len(parts) == 0 {
		// A not-clean rollup always has at least one non-matching entry; guard
		// anyway so the check never records an empty, uninformative reason.
		return "drift blocks apply: deployments differ from the reviewed plan"
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
