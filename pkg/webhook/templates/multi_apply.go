package templates

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/presentation"
)

// MultiDeploymentApplyData is the input to the multi-deployment apply comment.
// It bundles the surface-agnostic rollup (pkg/presentation) with each
// deployment's existing single-deployment comment data, so the per-deployment
// detail is rendered by exactly the same code path a single-deployment apply
// uses today — the multi-deployment comment only adds the aggregate header and
// the per-deployment hierarchy on top.
type MultiDeploymentApplyData struct {
	// Model is the derived rollup: aggregate state/label/counts/next-action and
	// the per-deployment presentations in resolved deployment order.
	Model presentation.Apply

	// ApplyID is the parent apply's identifier, shown once in the aggregate header.
	ApplyID string

	// Environment is the rollout environment, shown in the header and used to
	// format the next-action command.
	Environment string

	// RequestedBy is the operator who requested the apply.
	RequestedBy string

	// StartedAt / CompletedAt are RFC3339 timestamps for the aggregate elapsed time.
	StartedAt   string
	CompletedAt string

	// Details maps a deployment name to that deployment's single-deployment
	// comment data (its tables, error, timing, database). Each deployment's
	// <details> body is rendered from its entry via RenderApplyStatusComment.
	Details map[string]ApplyStatusCommentData
}

// RenderMultiDeploymentApplyComment renders the PR comment for an apply that
// fans out across more than one deployment. The layout, per the multi-deployment
// UX: an aggregate header (state title, metadata, per-status counts, and the
// single next operator action), then a flat per-deployment summary so rollout
// health and any failure are visible without expanding, then a <details> section
// per deployment carrying today's per-table UX scoped to that deployment.
//
// The single-deployment case is intentionally not handled here: callers render
// it with RenderApplyStatusComment so that UX stays byte-for-byte unchanged.
func RenderMultiDeploymentApplyComment(data MultiDeploymentApplyData) string {
	var sb strings.Builder

	// Aggregate header: reuse the single-deployment title map keyed on the
	// derived aggregate state so the headline vocabulary is identical.
	writeApplyHeader(&sb, ApplyStatusCommentData{State: data.Model.State})
	writeAggregateMetadata(&sb, data)
	writeDeploymentCounts(&sb, data.Model.Counts)
	writeAggregateNextAction(&sb, data)

	// Flat per-deployment summary (always visible — survives any later size
	// trimming of the detail sections).
	writeDeploymentSummaryList(&sb, data.Model.Deployments)

	// Expandable per-deployment detail, in resolved order.
	writeDeploymentSections(&sb, data)

	return sb.String()
}

// writeAggregateMetadata writes the apply-level metadata line. The database is
// intentionally omitted — it is per-deployment and shown in each deployment's
// section — so the aggregate carries only the environment, apply ID, elapsed
// time, and requester.
func writeAggregateMetadata(sb *strings.Builder, data MultiDeploymentApplyData) {
	var parts []string
	if data.Environment != "" {
		parts = append(parts, fmt.Sprintf("**Environment**: `%s`", data.Environment))
	}
	if data.ApplyID != "" {
		parts = append(parts, fmt.Sprintf("**Apply ID**: `%s`", data.ApplyID))
	}
	if elapsed := applyElapsed(ApplyStatusCommentData{StartedAt: data.StartedAt, CompletedAt: data.CompletedAt}); elapsed != "" {
		parts = append(parts, fmt.Sprintf("**Elapsed**: %s", elapsed))
	}
	if len(parts) > 0 {
		fmt.Fprintf(sb, "%s\n", strings.Join(parts, " | "))
	}
	writeAppliedByOrTimestamp(sb, data.RequestedBy)
}

// writeDeploymentCounts writes the per-status histogram so an operator sees
// rollout health at a glance without expanding anything.
func writeDeploymentCounts(sb *strings.Builder, counts []presentation.StateCount) {
	if len(counts) == 0 {
		return
	}
	parts := make([]string, 0, len(counts))
	for _, c := range counts {
		parts = append(parts, fmt.Sprintf("%d %s", c.Count, c.Label))
	}
	fmt.Fprintf(sb, "\n**Deployments**: %s\n", strings.Join(parts, ", "))
}

// writeAggregateNextAction renders the single suggested operator action derived
// for the rollup, if any. The verb set mirrors the per-deployment commands the
// CLI exposes; an empty action (NextActionNone) writes nothing.
func writeAggregateNextAction(sb *strings.Builder, data MultiDeploymentApplyData) {
	na := data.Model.NextAction
	switch na.Kind {
	case presentation.NextActionCutover:
		writeFooterAction(sb,
			fmt.Sprintf("To cut over `%s`:", na.Deployment),
			fmt.Sprintf("schemabot cutover -e %s --deployment %s", data.Environment, na.Deployment))
	case presentation.NextActionResume:
		writeFooterAction(sb, "To resume:", fmt.Sprintf("schemabot start -e %s", data.Environment))
	case presentation.NextActionReviewFailure:
		if na.Deployment != "" {
			writeFooterAction(sb,
				fmt.Sprintf("Review the failure in `%s`, then revert:", na.Deployment),
				fmt.Sprintf("schemabot revert -e %s --deployment %s", data.Environment, na.Deployment))
		} else {
			writeFooterAction(sb, "Review the failed deployment, then revert:",
				fmt.Sprintf("schemabot revert -e %s", data.Environment))
		}
	case presentation.NextActionNone:
		// No operator action is pending; nothing to render.
	}
}

// writeDeploymentSummaryList writes one line per deployment (status glyph, name,
// derived label) in resolved order. This is the at-a-glance rollout view and the
// part that must always remain even if detail sections are later trimmed for size.
func writeDeploymentSummaryList(sb *strings.Builder, deps []presentation.Deployment) {
	if len(deps) == 0 {
		return
	}
	sb.WriteString("\n")
	for _, d := range deps {
		fmt.Fprintf(sb, "- %s — %s\n", deploymentTag(d), d.Label)
	}
}

// writeDeploymentSections writes a <details> block per deployment in resolved
// order. Active and problematic deployments default open; completed and queued
// ones default collapsed (the model's Open flag). The body reuses the
// single-deployment renderer so per-table progress, errors, and DDL keep today's
// fidelity scoped to one deployment.
func writeDeploymentSections(sb *strings.Builder, data MultiDeploymentApplyData) {
	for _, d := range data.Model.Deployments {
		openAttr := ""
		if d.Open {
			openAttr = " open"
		}
		fmt.Fprintf(sb, "\n<details%s>\n<summary>%s — %s</summary>\n\n", openAttr, deploymentTag(d), d.Label)
		if detail, ok := data.Details[d.Deployment]; ok {
			sb.WriteString(RenderApplyStatusComment(detail))
		}
		sb.WriteString("\n</details>\n")
	}
}

// deploymentTag renders the "<emoji> <deployment>" prefix, omitting the leading
// space when a state has no glyph.
func deploymentTag(d presentation.Deployment) string {
	if d.Emoji == "" {
		return d.Deployment
	}
	return fmt.Sprintf("%s %s", d.Emoji, d.Deployment)
}
