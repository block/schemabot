package commands

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"

	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
)

func (m WatchModel) multiDeploymentProgressView() string {
	model := presentation.Derive(templates.ProgressOperationsForPresentation(m.operations, m.released))
	groups := model.Groups()
	view := templates.RolloutView{
		ApplyID:     m.applyID,
		Environment: m.environment,
		Engine:      m.engine,
		Operations:  m.operations,
		Model:       model,
		Tables:      m.tables,
		SetupPhase:  state.IsSetupPhase(m.state),
	}

	var b strings.Builder
	m.writeMultiDeploymentHeader(&b, model, groups)

	// A deployment that addresses several targets renders as one rollup
	// section, shared with the progress output; any other member keeps a
	// section of its own. Group members index model.Deployments, which Derive
	// returns index-parallel to m.operations, so each section renders its own
	// operation's identifiers; a deployment can own several operations, so a
	// name-based lookup cannot tell them apart.
	for _, g := range groups {
		if len(g.Members) > 1 {
			b.WriteString(templates.FormatTargetRollup(view, g))
			continue
		}
		i := g.Members[0]
		m.writeDeploymentSection(&b, model.Deployments[i], m.operations[i])
	}

	b.WriteString(templates.FormatThrottleReference(m.tables))
	if footer := templates.FormatRolloutFooter(view); footer != "" {
		b.WriteString(footer + "\n")
	}
	m.writeMultiDeploymentFooter(&b, model)
	return b.String()
}

func (m WatchModel) writeMultiDeploymentHeader(b *strings.Builder, model presentation.Apply, groups []presentation.Group) {
	if state.IsRunningApplyState(model.State) || state.IsState(model.State, state.Apply.Pending, state.Apply.WaitingForCutover, state.Apply.CuttingOver, state.Apply.Recovering) {
		b.WriteString(m.spinner.View() + model.Label + m.elapsed() + "\n")
	} else {
		b.WriteString(model.Label + "\n")
	}
	if counts := templates.FormatStateCounts(model.Counts); counts != "" {
		fmt.Fprintf(b, "%s: %s\n", templates.RolloutCountsUnit(groups), counts)
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
	if externalID := templates.SectionExternalID(op, m.operations); externalID != "" {
		fmt.Fprintf(b, "  External apply ID: %s\n", externalID)
	}

	if deployment.Error != "" {
		errStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
		fmt.Fprintf(b, "  %s\n", errStyle.Render(deployment.Error))
	}

	// The selector takes the member's recorded target, not the resolved one the
	// header shows: a table row carries whatever its own operation carried, so
	// matching an inherited value would look for a target the rows do not have.
	tables := tablesForMember(m.tables, deployment.Deployment, deployment.Target)
	if len(tables) > 0 && !state.IsSetupPhase(m.state) {
		sortTablesByProgress(tables)
		m.renderTables(b, tables)
	}
	b.WriteString("\n")
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
