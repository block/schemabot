package templates

import (
	"fmt"
	"html"
	"strings"

	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
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

	// ApplyID is the parent apply's identifier, shown once in the aggregate
	// header and used to format the next-action commands. Always set: every
	// apply is created with a server-generated identifier.
	ApplyID string

	// Environment is the rollout environment, shown in the header and used to
	// format the next-action commands. Always set: a non-empty environment is
	// enforced when the apply is created.
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
// it with RenderApplyStatusComment so the title and detail vocabulary stay shared.
func RenderMultiDeploymentApplyComment(data MultiDeploymentApplyData) string {
	var sb strings.Builder
	renderedAt := currentTimestamp()

	// Aggregate header: reuse the single-deployment title map keyed on the
	// derived aggregate state so the headline vocabulary is identical.
	writeApplyHeader(&sb, ApplyStatusCommentData{State: data.Model.State, Environment: data.Environment})
	writeAggregateMetadata(&sb, data, renderedAt)
	writeDeploymentCounts(&sb, data.Model.Counts)
	writeAggregateFirstFailure(&sb, data.Model.FirstFailure)
	writeAggregateNextAction(&sb, data)

	// Flat per-deployment summary (always visible — survives any later size
	// trimming of the detail sections).
	writeDeploymentSummaryList(&sb, data.Model.Deployments)

	// Expandable per-deployment detail, in resolved order.
	writeDeploymentSections(&sb, data, renderedAt)
	if !state.IsTerminalApplyState(data.Model.State) {
		writeLastUpdatedFooter(&sb, renderedAt)
	}

	return sb.String()
}

// RenderMultiDeploymentApplySummaryComment renders the final summary PR comment
// for a terminal apply that fans out across more than one deployment. It mirrors
// RenderMultiDeploymentApplyComment's aggregate layout — terminal header,
// metadata, per-status counts, the single next operator action, and the flat
// per-deployment summary — but each <details> body is the deployment's terminal
// summary (RenderApplySummaryComment) rather than its in-progress status.
//
// The single-deployment case is intentionally not handled here: callers render
// it with RenderApplySummaryComment so the title and detail vocabulary stay shared.
func RenderMultiDeploymentApplySummaryComment(data MultiDeploymentApplyData) string {
	var sb strings.Builder

	writeApplyHeader(&sb, ApplyStatusCommentData{State: data.Model.State, Environment: data.Environment})
	writeAggregateMetadata(&sb, data, currentTimestamp())
	writeDeploymentCounts(&sb, data.Model.Counts)
	writeAggregateFirstFailure(&sb, data.Model.FirstFailure)
	writeAggregateNextAction(&sb, data)

	writeDeploymentSummaryList(&sb, data.Model.Deployments)

	// Expandable per-deployment terminal summary, in resolved order.
	writeDeploymentSummarySections(&sb, data)

	return sb.String()
}

// writeAggregateMetadata writes the apply-level metadata line. The database is
// intentionally omitted — it is per-deployment and shown in each deployment's
// section — so the aggregate carries only the apply ID and requester.
func writeAggregateMetadata(sb *strings.Builder, data MultiDeploymentApplyData, renderedAt string) {
	// ApplyID is always populated from the persisted apply, so it is rendered
	// unconditionally. Environment is already in the title.
	parts := []string{
		fmt.Sprintf("**Apply ID**: `%s`", data.ApplyID),
	}
	fmt.Fprintf(sb, "%s\n", strings.Join(parts, " | "))
	writeAppliedByOrTimestampAt(sb, data.RequestedBy, renderedAt)
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

// writeAggregateFirstFailure lifts the first failed deployment's error to the
// aggregate header so an operator sees what failed without expanding that
// deployment's section. It renders nothing when no deployment has failed. The
// reason is the first failed operation's error — the same operation the
// persisted aggregate ErrorMessage is stamped from — and falls back to naming
// the deployment when that operation carried no error detail.
//
// The deployment name is wrapped in a <code> element rather than Markdown
// backticks: a name may contain HTML-significant characters (and backticks
// themselves), and HTML-escaped text inside a code span would render its
// entities literally.
func writeAggregateFirstFailure(sb *strings.Builder, failure *presentation.Deployment) {
	if failure == nil {
		return
	}
	name := html.EscapeString(failure.Deployment)
	if failure.Error == "" {
		fmt.Fprintf(sb, "\n> ⚠️ **First failure:** <code>%s</code>\n", name)
		return
	}
	fmt.Fprintf(sb, "\n> ⚠️ **First failure:** <code>%s</code> — %s\n", name, html.EscapeString(failure.Error))
}

// writeAggregateNextAction renders the single suggested operator action derived
// for the rollup, if any. An empty action (NextActionNone) writes nothing.
func writeAggregateNextAction(sb *strings.Builder, data MultiDeploymentApplyData) {
	// The CLI today addresses an apply by its identifier and has no
	// --deployment flag, so the suggested commands mirror the executable forms
	// the single-deployment footer already ships. Deployment-targeted commands
	// arrive with the per-deployment CLI surface.
	na := data.Model.NextAction
	switch na.Kind {
	case presentation.NextActionCutover:
		writeFooterAction(sb,
			fmt.Sprintf("To cut over `%s`:", na.Deployment),
			fmt.Sprintf("schemabot cutover %s -e %s", data.ApplyID, data.Environment))
	case presentation.NextActionResume:
		writeFooterAction(sb, "Paused — to resume from where it stopped:", fmt.Sprintf("schemabot start %s -e %s", data.ApplyID, data.Environment))
	case presentation.NextActionReviewFailure:
		// revert applies only to a deployment still in its post-cutover revert
		// window, not to a failure; the recovery path for a failed apply is a
		// retry, matching the single-deployment failed footer.
		writeFooterAction(sb, "To retry:", fmt.Sprintf("schemabot apply -e %s", data.Environment))
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
		fmt.Fprintf(sb, "- %s — %s\n", deploymentTag(d), html.EscapeString(d.Label))
	}
}

// writeDeploymentSections writes the in-progress status detail per deployment,
// reusing the single-deployment status renderer for each <details> body.
func writeDeploymentSections(sb *strings.Builder, data MultiDeploymentApplyData, renderedAt string) {
	writeDeploymentDetailSections(sb, data, func(detail ApplyStatusCommentData) string {
		return renderApplyStatusComment(detail, false, renderedAt)
	})
}

// writeDeploymentSummarySections writes the terminal summary detail per
// deployment, reusing the single-deployment summary renderer for each <details>
// body.
func writeDeploymentSummarySections(sb *strings.Builder, data MultiDeploymentApplyData) {
	writeDeploymentDetailSections(sb, data, RenderApplySummaryComment)
}

// writeDeploymentDetailSections writes a <details> block per deployment in
// resolved order, rendering each body with renderDetail (the status renderer for
// an in-progress comment, the summary renderer for a terminal comment). Active
// and problematic deployments default open; completed and queued ones default
// collapsed (the model's Open flag). The per-deployment body keeps today's
// single-deployment fidelity — per-table progress, errors, and DDL — scoped to
// one deployment.
func writeDeploymentDetailSections(sb *strings.Builder, data MultiDeploymentApplyData, renderDetail func(ApplyStatusCommentData) string) {
	for _, d := range data.Model.Deployments {
		openAttr := ""
		if d.Open {
			openAttr = " open"
		}
		fmt.Fprintf(sb, "\n<details%s>\n<summary>%s — %s</summary>\n\n", openAttr, deploymentTag(d), html.EscapeString(d.Label))
		if detail, ok := data.Details[d.Deployment]; ok {
			sb.WriteString(renderDetail(detail))
		} else {
			sb.WriteString("_No details available yet._\n")
		}
		sb.WriteString("\n</details>\n")
	}
}

// deploymentTag renders the "<emoji> <deployment>" prefix, omitting the leading
// space when a state has no glyph.
func deploymentTag(d presentation.Deployment) string {
	name := html.EscapeString(d.Deployment)
	if d.Emoji == "" {
		return name
	}
	return fmt.Sprintf("%s %s", d.Emoji, name)
}
