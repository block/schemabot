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
	model := presentation.Derive(tuiOperationsForPresentation(m.operations))

	var b strings.Builder
	m.writeMultiDeploymentHeader(&b, model)

	for _, deployment := range model.Deployments {
		m.writeDeploymentSection(&b, deployment)
	}

	m.writeMultiDeploymentFooter(&b, model)
	return b.String()
}

func tuiOperationsForPresentation(ops []templates.ProgressOperation) []presentation.Operation {
	presentationOps := make([]presentation.Operation, 0, len(ops))
	for _, op := range ops {
		presentationOps = append(presentationOps, presentation.Operation{
			Deployment:        op.Deployment,
			State:             op.State,
			Barrier:           op.CutoverPolicy == storage.CutoverPolicyBarrier,
			Parallel:          op.CutoverPolicy == storage.CutoverPolicyParallel,
			HaltOnFailure:     op.OnFailure != storage.OnFailureContinue,
			ContinueOnFailure: op.OnFailure == storage.OnFailureContinue,
			Error:             op.ErrorMessage,
		})
	}
	return presentationOps
}

func (m WatchModel) writeMultiDeploymentHeader(b *strings.Builder, model presentation.Apply) {
	if state.IsState(model.State, state.Apply.Running, state.Apply.RunningDegraded, state.Apply.Pending, state.Apply.WaitingForCutover, state.Apply.CuttingOver, state.Apply.Recovering) {
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

func (m WatchModel) writeDeploymentSection(b *strings.Builder, deployment presentation.Deployment) {
	fmt.Fprintf(b, "%s %s — %s", deployment.Emoji, deployment.Deployment, deployment.Label)
	if target := targetForTUIDeployment(m.operations, deployment.Deployment); target != "" {
		fmt.Fprintf(b, " (%s)", target)
	}
	b.WriteString("\n")

	if deployment.Error != "" {
		errStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
		fmt.Fprintf(b, "  %s\n", errStyle.Render(deployment.Error))
	}

	tables := tablesForDeployment(m.tables, deployment.Deployment)
	if len(tables) > 0 && !state.IsSetupPhase(m.state) {
		sortTablesByProgress(tables)
		m.renderTables(b, tables)
	}
	b.WriteString("\n")
}

func targetForTUIDeployment(ops []templates.ProgressOperation, deployment string) string {
	for _, op := range ops {
		if op.Deployment == deployment {
			return op.Target
		}
	}
	return ""
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
