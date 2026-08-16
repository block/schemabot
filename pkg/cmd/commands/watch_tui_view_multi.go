package commands

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"

	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func (m WatchModel) multiDeploymentProgressView() string {
	model := presentation.Derive(tuiOperationsForPresentation(m.operations, m.released))

	var b strings.Builder
	m.writeMultiDeploymentHeader(&b, model)

	for _, group := range templates.GroupOperationIndicesByDeployment(m.operations) {
		if len(group) == 1 {
			m.writeDeploymentSection(&b, model.Deployments[group[0]], m.operations[group[0]])
			continue
		}
		m.writeGroupedDeploymentSection(&b, group, model)
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
			fmt.Fprintf(b, "%s\n", errStyle.Render(fmt.Sprintf("⚠ First failure: %s — %s", model.FirstFailure.Deployment, model.FirstFailure.Error)))
		} else {
			fmt.Fprintf(b, "%s\n", errStyle.Render(fmt.Sprintf("⚠ First failure: %s", model.FirstFailure.Deployment)))
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
	fmt.Fprintf(b, "%s %s — %s", deployment.Emoji, deployment.Deployment, deployment.Label)
	if op.Target != "" {
		fmt.Fprintf(b, " (%s)", op.Target)
	}
	b.WriteString("\n")
	if op.ExternalOperationID != "" {
		fmt.Fprintf(b, "  External operation ID: %s\n", op.ExternalOperationID)
	}
	if op.ExternalID != "" {
		fmt.Fprintf(b, "  External apply ID: %s\n", op.ExternalID)
	}

	if deployment.Error != "" {
		errStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
		fmt.Fprintf(b, "  %s\n", errStyle.Render(deployment.Error))
	}

	m.writeDeploymentSectionTables(b, deployment.Deployment)
	b.WriteString("\n")
}

// writeGroupedDeploymentSection renders one section for a deployment that owns
// several operations. The header aggregates the group; each operation gets one
// line with its own kind, status, and external identifiers, and the
// deployment's tables render once — task rows are deployment-scoped, not
// operation-scoped.
func (m WatchModel) writeGroupedDeploymentSection(b *strings.Builder, group []int, model presentation.Apply) {
	ops := make([]templates.ProgressOperation, 0, len(group))
	for _, i := range group {
		ops = append(ops, m.operations[i])
	}
	sub := presentation.Derive(tuiOperationsForPresentation(ops, m.released))

	fmt.Fprintf(b, "%s %s — %s", templates.AggregateStateEmoji(sub.State), ops[0].Deployment, formatTUIDeploymentCounts(sub.Counts))
	if ops[0].Target != "" {
		fmt.Fprintf(b, " (%s)", ops[0].Target)
	}
	b.WriteString("\n")

	for n, i := range group {
		deployment := model.Deployments[i]
		op := m.operations[i]
		fmt.Fprintf(b, "  %s %s — %s%s\n",
			deployment.Emoji, templates.OperationKindLabel(op, n), deployment.Label, templates.OperationIdentifierSuffix(op))
		if deployment.Error != "" {
			errStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
			fmt.Fprintf(b, "      %s\n", errStyle.Render(deployment.Error))
		}
	}

	m.writeDeploymentSectionTables(b, ops[0].Deployment)
	b.WriteString("\n")
}

// writeDeploymentSectionTables renders the deployment's table progress once
// per deployment section, outside the setup phase.
func (m WatchModel) writeDeploymentSectionTables(b *strings.Builder, deployment string) {
	tables := tablesForDeployment(m.tables, deployment)
	if len(tables) > 0 && !state.IsSetupPhase(m.state) {
		sortTablesByProgress(tables)
		m.renderTables(b, tables)
	}
}

func tablesForDeployment(tables []tableProgress, deployment string) []tableProgress {
	deploymentTables := make([]tableProgress, 0, len(tables))
	for _, table := range tables {
		if table.Deployment == deployment && table.Name != "" {
			deploymentTables = append(deploymentTables, table)
		}
	}
	return deploymentTables
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
