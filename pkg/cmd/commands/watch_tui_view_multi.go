package commands

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"

	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func (m WatchModel) multiDeploymentProgressView() string {
	model := presentation.Derive(tuiOperationsForPresentation(m.operations, m.released))

	var b strings.Builder
	m.writeMultiDeploymentHeader(&b, model)

	// Derive returns one Deployment per input operation, in input order, so
	// model.Deployments[i] projects m.operations[i]. Pairing by index lets each
	// section render its own operation's identifiers; a deployment can own
	// several operations, so a name-based lookup cannot tell them apart.
	for i, deployment := range model.Deployments {
		m.writeDeploymentSection(&b, deployment, m.operations[i])
	}

	m.writeMultiDeploymentFooter(&b, model)
	return b.String()
}

// tuiOperationsForPresentation maps the watch model's progress operations to the
// surface-neutral presentation inputs. released is the apply-level release latch:
// a released pause behaves like continue, so the held siblings proceed and the
// aggregate runs degraded instead of paused.
func tuiOperationsForPresentation(ops []templates.ProgressOperation, released bool) []presentation.Operation {
	presentationOps := make([]presentation.Operation, 0, len(ops))
	for _, op := range ops {
		presentationOps = append(presentationOps, presentation.Operation{
			Deployment:        op.Deployment,
			Target:            op.Target,
			State:             op.State,
			Barrier:           op.CutoverPolicy == storage.CutoverPolicyBarrier,
			Parallel:          op.CutoverPolicy == storage.CutoverPolicyParallel,
			ContinueOnFailure: op.OnFailure == storage.OnFailureContinue,
			PauseOnFailure:    op.OnFailure == storage.OnFailurePause,
			Released:          released,
			Error:             op.ErrorMessage,
		})
	}
	return presentationOps
}

func (m WatchModel) writeMultiDeploymentHeader(b *strings.Builder, model presentation.Apply) {
	if state.IsRunningApplyState(model.State) || state.IsState(model.State, state.Apply.Pending, state.Apply.WaitingForCutover, state.Apply.CuttingOver, state.Apply.Recovering) {
		b.WriteString(m.spinner.View() + model.Label + m.elapsed() + "\n")
	} else {
		b.WriteString(model.Label + "\n")
	}
	if counts := formatTUIDeploymentCounts(model.Counts); counts != "" {
		b.WriteString(counts + "\n")
	}
	if model.FirstFailure != nil {
		errStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
		if model.FirstFailure.Error != "" {
			fmt.Fprintf(b, "%s\n", errStyle.Render(fmt.Sprintf(glyph.Failed+" First failure: %s — %s", model.FirstFailure.Name, model.FirstFailure.Error)))
		} else {
			fmt.Fprintf(b, "%s\n", errStyle.Render(fmt.Sprintf(glyph.Failed+" First failure: %s", model.FirstFailure.Name)))
		}
	}
	if m.applyID != "" {
		fmt.Fprintf(b, "Apply ID: %s\n", m.applyID)
	}
	if m.environment != "" {
		fmt.Fprintf(b, "Environment: %s\n", m.environment)
	}
	b.WriteString("\n")
}

func formatTUIDeploymentCounts(counts []presentation.StateCount) string {
	parts := make([]string, 0, len(counts))
	for _, count := range counts {
		parts = append(parts, fmt.Sprintf("%d %s", count.Count, count.Label))
	}
	return strings.Join(parts, " · ")
}

func (m WatchModel) writeDeploymentSection(b *strings.Builder, deployment presentation.Deployment, op templates.ProgressOperation) {
	fmt.Fprintf(b, "%s %s — %s", deployment.Emoji, deployment.Name, deployment.Label)
	// A member whose name already carries its target does not repeat it in the
	// trailing parenthetical.
	if op.Target != "" && deployment.Name == deployment.Deployment {
		fmt.Fprintf(b, " (%s)", op.Target)
	}
	b.WriteString("\n")
	// The external operation ID identifies this operation's own data-plane row,
	// so it never falls back to a sibling's value.
	if op.ExternalOperationID != "" {
		fmt.Fprintf(b, "  External operation ID: %s\n", op.ExternalOperationID)
	}
	if externalID := externalIDForTUIMember(m.operations, op); externalID != "" {
		fmt.Fprintf(b, "  External apply ID: %s\n", externalID)
	}

	if deployment.Error != "" {
		errStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
		fmt.Fprintf(b, "  %s\n", errStyle.Render(deployment.Error))
	}

	tables := tablesForMember(m.tables, deployment.Deployment, deployment.Target)
	if len(tables) > 0 && !state.IsSetupPhase(m.state) {
		sortTablesByProgress(tables)
		m.renderTables(b, tables)
	}
	b.WriteString("\n")
}

// externalIDForTUIMember resolves the external apply ID shown in a section: the
// section's own operation when set, falling back to a sibling's — a keyed
// apply's operations share one data-plane apply, so an operation that has not
// dispatched yet still shows it. The fallback applies only while the deployment
// addresses a single target, so it never shows one member the ID of another.
func externalIDForTUIMember(ops []templates.ProgressOperation, op templates.ProgressOperation) string {
	if op.ExternalID != "" {
		return op.ExternalID
	}
	target := ""
	for _, sibling := range ops {
		if sibling.Deployment != op.Deployment || sibling.Target == "" {
			continue
		}
		if target != "" && target != sibling.Target {
			return ""
		}
		target = sibling.Target
	}
	for _, sibling := range ops {
		if sibling.Deployment == op.Deployment && sibling.ExternalID != "" {
			return sibling.ExternalID
		}
	}
	return ""
}

// tablesForMember selects the tables copied by one rollout member. Both halves
// of the routing pair are matched: two targets of one deployment each copy the
// same tables, and matching the deployment alone would list both members'
// copies under each of them.
func tablesForMember(tables []templates.TableProgress, deployment, target string) []templates.TableProgress {
	memberTables := make([]templates.TableProgress, 0, len(tables))
	for _, table := range tables {
		if table.Deployment == deployment && table.Target == target && table.TableName != "" {
			memberTables = append(memberTables, table)
		}
	}
	return memberTables
}

func (m WatchModel) writeMultiDeploymentFooter(b *strings.Builder, model presentation.Apply) {
	switch {
	case state.IsState(model.State, state.Apply.Completed):
		b.WriteString("\n")
		b.WriteString(templates.FormatApplyCompleteWithSummary(countTableProgressChanges(m.tables).summary(), m.applyID))
		b.WriteString("\n")
	case state.IsState(model.State, state.Apply.Failed):
		b.WriteString("\n")
		b.WriteString(templates.FormatApplyFailed())
		b.WriteString("\n")
	case state.IsState(model.State, state.Apply.Stopped):
		b.WriteString("\n")
		b.WriteString(templates.FormatApplyStopped())
		b.WriteString("\n")
	default:
		dimStyle := lipgloss.NewStyle().Faint(true)
		b.WriteString(dimStyle.Render("ESC to detach"))
		b.WriteString("\n")
	}
}
