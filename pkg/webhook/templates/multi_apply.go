package templates

import (
	"fmt"
	"html"
	"strings"

	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
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

	// Details is each member's single-deployment comment data (its tables,
	// error, timing, database), index-parallel to Model.Deployments. Each
	// member's <details> body is rendered from its entry via
	// RenderApplyStatusComment; a nil entry, or an index past the end, renders
	// the no-detail placeholder instead. It is positional rather than keyed by
	// deployment name because a deployment can own several operations —
	// different targets of one deployment, or the several operations of a keyed
	// apply — and a name-keyed lookup would render one member's tables under
	// every one of them.
	Details []*ApplyStatusCommentData

	// Tenant is the deployment's tenant identity, appended as --tenant to every
	// pasteable command hint so copied commands address this deployment in
	// tenant mode. Empty on single-tenant deployments, leaving hints unchanged.
	Tenant string

	// Rollback reports whether this apply reverts a previously applied schema
	// change (the apply's durable rollback option). It switches the aggregate
	// headline vocabulary from "apply" to "rollback", matching the
	// single-deployment comment.
	Rollback bool
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
	renderedAt := currentTimestamp()
	return renderWithinCommentLimit(countDeploymentTablesWithDDL(data), applyCommentAppendReserve, func(budget *ddlBlockBudget) string {
		return renderMultiDeploymentApplyComment(data, renderedAt, budget)
	})
}

// countDeploymentTablesWithDDL counts the DDL blocks the per-deployment detail
// sections render between them, so one comment's DDL budget is shared across
// every deployment rather than granted to each. A rolled-up deployment renders
// each distinct change once, however many targets run it.
func countDeploymentTablesWithDDL(data MultiDeploymentApplyData) int {
	count := 0
	for _, g := range data.Model.Groups() {
		if len(g.Members) > 1 {
			for _, work := range targetWorkGroups(data, g) {
				count += countTablesWithDDL(work.tables)
			}
			continue
		}
		if detail := memberDetail(data.Details, g.Members[0]); detail != nil {
			count += countTablesWithDDL(detail.Tables)
		}
	}
	return count
}

func renderMultiDeploymentApplyComment(data MultiDeploymentApplyData, renderedAt string, budget *ddlBlockBudget) string {
	var sb strings.Builder

	// Aggregate header: the stable in-place status title, identical to the
	// single-deployment comment so the headline vocabulary stays shared.
	writeApplyStatusHeader(&sb, ApplyStatusCommentData{State: data.Model.State, Environment: data.Environment, Rollback: data.Rollback})
	writeAggregateMetadata(&sb, data, renderedAt)
	groups := data.Model.Groups()
	writeDeploymentCounts(&sb, data.Model.Counts, groups)
	writeAggregateFirstFailure(&sb, data.Model.FirstFailure)

	// Flat per-deployment summary (always visible — survives any later size
	// trimming of the detail sections).
	writeDeploymentSummaryList(&sb, data.Model, groups)

	// Expandable per-deployment detail, in resolved order.
	writeDeploymentSections(&sb, data, groups, renderedAt, budget)
	writeRolloutFooter(&sb, data)
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
	return renderWithinCommentLimit(countDeploymentTablesWithDDL(data), applyCommentAppendReserve, func(budget *ddlBlockBudget) string {
		return renderMultiDeploymentApplySummaryComment(data, budget)
	})
}

func renderMultiDeploymentApplySummaryComment(data MultiDeploymentApplyData, budget *ddlBlockBudget) string {
	var sb strings.Builder

	writeApplyHeader(&sb, ApplyStatusCommentData{State: data.Model.State, Environment: data.Environment, Rollback: data.Rollback})
	writeAggregateMetadata(&sb, data, currentTimestamp())
	groups := data.Model.Groups()
	writeDeploymentCounts(&sb, data.Model.Counts, groups)
	writeAggregateFirstFailure(&sb, data.Model.FirstFailure)

	writeDeploymentSummaryList(&sb, data.Model, groups)

	// Expandable per-deployment terminal summary, in resolved order.
	writeDeploymentSummarySections(&sb, data, groups, budget)
	writeRolloutFooter(&sb, data)

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
	attributionAt := renderedAt
	if data.RequestedBy == "" {
		attributionAt = startedAtDisplay(data.StartedAt, renderedAt)
	}
	writeAppliedByOrTimestampAt(sb, data.RequestedBy, attributionAt)
}

// writeDeploymentCounts writes the per-status histogram so an operator sees
// rollout health at a glance without expanding anything. The histogram counts
// members, so once a deployment addresses several targets it counts targets.
func writeDeploymentCounts(sb *strings.Builder, counts []presentation.StateCount, groups []presentation.Group) {
	if len(counts) == 0 {
		return
	}
	unit := "Deployments"
	if hasMultiTargetGroup(groups) {
		unit = "Targets"
	}
	fmt.Fprintf(sb, "\n**%s**: %s\n", unit, countsPhrase(counts))
}

// countsPhrase joins a histogram into "3 completed, 1 running".
func countsPhrase(counts []presentation.StateCount) string {
	parts := make([]string, 0, len(counts))
	for _, c := range counts {
		parts = append(parts, fmt.Sprintf("%d %s", c.Count, c.Label))
	}
	return strings.Join(parts, ", ")
}

// hasMultiTargetGroup reports whether any deployment addresses several targets.
func hasMultiTargetGroup(groups []presentation.Group) bool {
	for _, g := range groups {
		if len(g.Members) > 1 {
			return true
		}
	}
	return false
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
	name := html.EscapeString(failure.Name)
	msg := SanitizeInlineError(failure.Error)
	if msg == "" {
		fmt.Fprintf(sb, "\n> "+glyph.Failed+" **First failure:** <code>%s</code>\n", name)
		return
	}
	fmt.Fprintf(sb, "\n> "+glyph.Failed+" **First failure:** <code>%s</code> — %s\n", name, html.EscapeString(msg))
}

// writeRolloutFooter writes the rollout's footer at the bottom, where the
// single-deployment comment keeps its footer: every control command addresses
// the whole apply. A pending rollup action leads; otherwise the footer is the
// one the aggregate state would carry, such as stop while running. Whenever a
// member is still writing to its target, stop follows in the same footer, so
// neither a pending action nor the aggregate state can take stop away from
// live work.
func writeRolloutFooter(sb *strings.Builder, data MultiDeploymentApplyData) {
	footer := rolloutFooterData(data)
	footerStart := sb.Len()
	actionPending := data.Model.NextAction.Kind != presentation.NextActionNone
	if actionPending {
		writeAggregateNextAction(sb, data)
	}
	// A pause-held rollout waits for a human to choose: release lets the held
	// deployments proceed, and stop parks the whole apply instead.
	paused := state.IsState(data.Model.State, state.Apply.Paused)
	if paused {
		writeFooterAction(sb, "Paused after a failure — to let the held deployments proceed:",
			appendTenantFlag(fmt.Sprintf("schemabot release %s -e %s", data.ApplyID, data.Environment), data.Tenant))
	}
	if !actionPending && !paused {
		writeApplyFooter(sb, footer)
		if applyFooterOffersStop(footer.State) {
			return
		}
	}
	// A paused rollout always offers stop beside release; otherwise stop
	// follows only a member that is still writing to its target.
	if !paused && !hasStoppableLiveWork(data.Model.Deployments) {
		return
	}
	label, command := rolloutStopAction(footer)
	if sb.Len() == footerStart {
		writeFooterAction(sb, label, command)
		return
	}
	// The footer already began above; stop joins it rather than opening a
	// second one.
	fmt.Fprintf(sb, "\n%s\n```\n%s\n```\n", label, command)
}

// rolloutStopAction is the label and command that stop the whole apply, or
// cancel it on an engine whose control command is cancel.
func rolloutStopAction(footer ApplyStatusCommentData) (string, string) {
	command := stopOrCancelCommand(footer)
	label := "To stop this schema change:"
	if command == "cancel" {
		label = "To cancel this schema change:"
	}
	return label, appendTenantFlag(fmt.Sprintf("schemabot %s %s -e %s", command, footer.ApplyID, footer.Environment), footer.Tenant)
}

// rolloutFooterData is the apply-wide comment data the rollout footer renders
// its commands from. It is not a member section, so its footer actions render.
func rolloutFooterData(data MultiDeploymentApplyData) ApplyStatusCommentData {
	footer := ApplyStatusCommentData{State: data.Model.State, ApplyID: data.ApplyID, Environment: data.Environment, Tenant: data.Tenant}
	// The members of one apply change one database, so they share its engine
	// and its cutover option; the first member with detail speaks for all.
	// Every member's tables feed the footer, so a table retrying on any target
	// gets the retry guidance.
	for _, detail := range data.Details {
		if detail == nil {
			continue
		}
		if footer.Engine == "" {
			footer.Engine = detail.Engine
			footer.DeferCutover = detail.DeferCutover
		}
		footer.Tables = append(footer.Tables, detail.Tables...)
	}
	return footer
}

// applyFooterOffersStop reports whether writeApplyFooter writes the stop (or
// cancel) command for an apply in state s: the running family, the PlanetScale
// setup phases, and an apply retrying a failed table.
func applyFooterOffersStop(s string) bool {
	return state.IsRunningApplyState(s) || state.IsState(s,
		state.Apply.FailedRetryable,
		state.Apply.PreparingBranch,
		state.Apply.ApplyingBranchChanges,
		state.Apply.ValidatingBranch,
		state.Apply.CreatingDeployRequest,
		state.Apply.ValidatingDeployRequest)
}

// hasStoppableLiveWork reports whether any member is still writing to its
// target in a state where the single-deployment footer offers stop. A member
// waiting for cutover is left out, as it is from that footer, so a rollout
// whose members only wait for cutover keeps the cutover as its one command.
func hasStoppableLiveWork(deployments []presentation.Deployment) bool {
	for _, d := range deployments {
		if applyFooterOffersStop(d.State) {
			return true
		}
	}
	return false
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
			fmt.Sprintf("To cut over %s:", inlineCode(na.Name)),
			appendTenantFlag(fmt.Sprintf("schemabot cutover %s -e %s", data.ApplyID, data.Environment), data.Tenant))
	case presentation.NextActionResume:
		writeFooterAction(sb, "Paused — to resume from where it stopped:", appendTenantFlag(fmt.Sprintf("schemabot start %s -e %s", data.ApplyID, data.Environment), data.Tenant))
	case presentation.NextActionReviewFailure:
		// revert applies only to a deployment still in its post-cutover revert
		// window, not to a failure; the recovery path for a failed apply is a
		// retry, matching the single-deployment failed footer.
		writeFooterAction(sb, "To retry:", appendTenantFlag(fmt.Sprintf("schemabot apply -e %s", data.Environment), data.Tenant))
	case presentation.NextActionNone:
		// No operator action is pending; nothing to render.
	}
}

// writeDeploymentSummaryList writes one line per deployment (status glyph,
// name, and label or rolled-up target counts) in resolved order. This is the
// at-a-glance rollout view and must survive any trimming of detail sections.
func writeDeploymentSummaryList(sb *strings.Builder, model presentation.Apply, groups []presentation.Group) {
	if len(groups) == 0 {
		return
	}
	sb.WriteString("\n")
	for _, g := range groups {
		if len(g.Members) > 1 {
			fmt.Fprintf(sb, "- %s — %s\n", glyphTag(g.Lead.Emoji, inlineCode(g.Deployment)), groupCountsLabel(g))
			continue
		}
		d := model.Deployments[g.Members[0]]
		fmt.Fprintf(sb, "- %s — %s\n", deploymentTag(d), html.EscapeString(d.Label))
	}
}

// groupCountsLabel is a multi-target deployment's status: its own histogram
// and how many targets it addresses.
func groupCountsLabel(g presentation.Group) string {
	return fmt.Sprintf("%s (%d targets)", countsPhrase(g.Counts), len(g.Members))
}

// writeDeploymentSections writes the in-progress status detail per deployment,
// reusing the single-deployment status renderer for each single-target body.
func writeDeploymentSections(sb *strings.Builder, data MultiDeploymentApplyData, groups []presentation.Group, renderedAt string, budget *ddlBlockBudget) {
	writeDeploymentDetailSections(sb, data, groups, budget, func(detail ApplyStatusCommentData) string {
		return renderApplyStatusCommentBody(detail, false, renderedAt, budget)
	})
}

// writeDeploymentSummarySections writes the terminal summary detail per
// deployment, reusing the single-deployment summary renderer for each
// single-target body.
func writeDeploymentSummarySections(sb *strings.Builder, data MultiDeploymentApplyData, groups []presentation.Group, budget *ddlBlockBudget) {
	writeDeploymentDetailSections(sb, data, groups, budget, func(detail ApplyStatusCommentData) string {
		return renderApplySummaryComment(detail, budget)
	})
}

// writeDeploymentDetailSections writes a <details> block per deployment in
// resolved order, open per the model's Open flag. The body sits in a <dd>,
// the one indent GitHub keeps, so it reads as nested under its <summary>.
//
// A single-target body is rendered with renderDetail (status or summary)
// minus the headline, apply ID, attribution, and footer the rollout carries
// once. A multi-target deployment rolls its targets up (writeTargetRollup).
func writeDeploymentDetailSections(sb *strings.Builder, data MultiDeploymentApplyData, groups []presentation.Group, budget *ddlBlockBudget, renderDetail func(ApplyStatusCommentData) string) {
	for _, g := range groups {
		openAttr := ""
		if g.Open {
			openAttr = " open"
		}
		if len(g.Members) > 1 {
			fmt.Fprintf(sb, "\n<details%s>\n<summary>%s — %s</summary>\n<dl><dd>\n\n", openAttr,
				glyphTag(g.Lead.Emoji, html.EscapeString(flattenIdentifier(g.Deployment))), groupCountsLabel(g))
			writeTargetRollup(sb, data, g, budget)
			sb.WriteString("\n</dd></dl>\n</details>\n")
			continue
		}
		i := g.Members[0]
		d := data.Model.Deployments[i]
		fmt.Fprintf(sb, "\n<details%s>\n<summary>%s — %s</summary>\n<dl><dd>\n\n", openAttr, deploymentTagHTML(d), html.EscapeString(d.Label))
		if detail := memberDetail(data.Details, i); detail != nil {
			body := *detail
			body.DerivedStatus = siblingDerivedStatus(d)
			body.InRolloutSection = true
			sb.WriteString(stripLeadingHeading(renderDetail(body)))
		} else {
			sb.WriteString("_No details available yet._\n")
		}
		sb.WriteString("\n</dd></dl>\n</details>\n")
	}
}

// memberDetail returns member i's comment data, or nil when the caller supplied
// no detail for it — either a nil entry or a details slice that stops short of
// the member set, both of which mean the member has nothing to show yet.
func memberDetail(details []*ApplyStatusCommentData, i int) *ApplyStatusCommentData {
	if i >= len(details) {
		return nil
	}
	return details[i]
}

// siblingDerivedStatus returns the <details> body status for a deployment whose
// presentation is derived from its earlier siblings: queued, waiting, halted,
// or paused. The raw operation state for all four is pending, which the
// single-deployment renderer glosses as "Starting" — contradicting the
// <summary> line for a deployment that is in fact held by an earlier failure.
// Every other presentation returns "" and keeps the raw-state gloss.
func siblingDerivedStatus(d presentation.Deployment) string {
	switch d.Presentation {
	case presentation.StateQueuedNext, presentation.StateWaiting, presentation.StateHalted, presentation.StatePaused:
		// The label interpolates deployment names; escape it like the
		// <summary> line does so a name cannot inject markup into the body.
		label := html.EscapeString(ui.CapitalizeFirst(d.Label))
		if d.Emoji == "" {
			return label
		}
		return d.Emoji + " " + label
	default:
		return ""
	}
}

// stripLeadingHeading removes the headline a single-deployment renderer writes
// as its first line ("## <title>[ — <env>]") plus the blank lines that follow
// it, so a body embedded in a per-deployment <details> section does not repeat
// the aggregate comment's header. A body that does not start with a heading is
// returned unchanged.
func stripLeadingHeading(body string) string {
	if !strings.HasPrefix(body, "## ") {
		return body
	}
	_, rest, found := strings.Cut(body, "\n")
	if !found {
		return ""
	}
	return strings.TrimLeft(rest, "\n")
}

// deploymentTag renders the "<emoji> <member>" prefix for a markdown line. The
// member is named by the derivation's resolved name, so two targets of one
// deployment are labelled distinctly.
//
// A member name is assembled from server config, so it reaches this comment as
// text SchemaBot did not choose. Rendering it as a code span keeps a name
// carrying a backtick or a line break from closing the span it sits in and
// writing markdown of its own into a comment operators act on.
func deploymentTag(d presentation.Deployment) string {
	return glyphTag(d.Emoji, inlineCode(d.Name))
}

// deploymentTagHTML is deploymentTag for a <summary>, which GitHub reads as
// HTML: the name is escaped rather than fenced, and flattened first so it
// cannot carry a line break out of the tag it sits in.
func deploymentTagHTML(d presentation.Deployment) string {
	return glyphTag(d.Emoji, html.EscapeString(flattenIdentifier(d.Name)))
}

// glyphTag joins a state's glyph to an already-rendered name, omitting the
// leading space when the state has no glyph.
func glyphTag(emoji, name string) string {
	if emoji == "" {
		return name
	}
	return fmt.Sprintf("%s %s", emoji, name)
}
