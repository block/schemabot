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

// multiDeploymentProgressSections renders the rollout as its body (the header
// and each deployment's section) and its footer (the rollout's next command
// and the keys the operator can press).
func (m WatchModel) multiDeploymentProgressSections() (string, string) {
	model := presentation.Derive(templates.ProgressOperationsForPresentation(m.operations, m.tables, m.released))
	groups := model.Groups()
	view := templates.RolloutView{
		ApplyID:      m.applyID,
		Environment:  m.environment,
		Engine:       m.engine,
		Model:        model,
		Tables:       m.tables,
		SetupPhase:   state.IsSetupPhase(m.state),
		DeferCutover: m.deferCutover,
		StopByKey:    m.rolloutStopByKey(model),
	}

	var header, b strings.Builder
	m.writeMultiDeploymentHeader(&header, model, groups)

	// A deployment that addresses several targets renders as one rollup
	// section, shared with the progress output; any other member keeps a
	// section of its own. Each member names the row it reads its identifiers
	// from, so each section renders its own operation's; a deployment can own
	// several operations, so a name-based lookup cannot tell them apart.
	for _, g := range groups {
		if len(g.Members) > 1 {
			b.WriteString(templates.FormatTargetRollup(view, g))
			continue
		}
		d := model.Deployments[g.Members[0]]
		m.writeDeploymentSection(&b, d, m.operations[d.Row])
	}

	b.WriteString(templates.FormatThrottleReference(m.tables))
	body := b.String()
	// Table blocks open on a blank line of their own; a deployment's section
	// does not, so the header gives it one.
	if header.Len() > 0 && !strings.HasPrefix(body, "\n") {
		header.WriteString("\n")
	}
	var f strings.Builder
	m.writeMultiDeploymentOutcome(&f, model, groups)
	if footer := templates.FormatRolloutFooter(view); footer != "" {
		f.WriteString(footer + "\n")
	}
	m.writeMultiDeploymentFooter(&f, model, view.StopByKey)
	return header.String() + body, f.String()
}

// rolloutStopByKey reports whether the watcher offers stop as a key for this
// rollout: the rollout footer would offer a stop, and the key acts in the
// apply's state. Elsewhere the footer keeps the stop command, since the key
// would do nothing.
func (m WatchModel) rolloutStopByKey(model presentation.Apply) bool {
	return model.OffersRolloutStop() && m.stopKeyAvailable()
}

// writeMultiDeploymentHeader writes the rollout's state line, the way the
// single-target watcher writes its own: a spinner and the state while the
// apply is active, followed by the rollout's counts. A finished apply has no
// state line; its outcome closes the view instead (writeMultiDeploymentFooter).
func (m WatchModel) writeMultiDeploymentHeader(b *strings.Builder, model presentation.Apply, groups []presentation.Group) {
	switch {
	case m.stopAwaitingConfirmation():
		// Until the server reports the stop, the targets still read as
		// running, so the state line acknowledges the key press instead.
		b.WriteString(m.spinner.View() + m.stopPendingLabel() + "\n")
	case state.IsTerminalApplyState(model.State):
	case rolloutStateIsActive(model.State):
		b.WriteString(m.spinner.View() + rolloutStateLine(model, groups, "...") + "\n")
	default:
		b.WriteString(rolloutStateLine(model, groups, "") + "\n")
	}
	if model.FirstFailure != nil {
		errStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
		if model.FirstFailure.Error != "" {
			fmt.Fprintf(b, "%s\n", errStyle.Render(fmt.Sprintf(glyph.Failed+" First failure: %s — %s", model.FirstFailure.Name, model.FirstFailure.Error)))
		} else {
			fmt.Fprintf(b, "%s\n", errStyle.Render(fmt.Sprintf(glyph.Failed+" First failure: %s", model.FirstFailure.Name)))
		}
	}
}

// rolloutStateIsActive reports whether the rollout is still working toward its
// outcome, so its state line carries the spinner.
func rolloutStateIsActive(s string) bool {
	return state.IsRunningApplyState(s) || state.IsState(s, state.Apply.Pending, state.Apply.WaitingForCutover, state.Apply.CuttingOver, state.Apply.Recovering)
}

// rolloutStateLine is the rollout's state, capitalized as the single-target
// watcher writes it, and how its members stand: "Running on all 4
// targets..." while they agree, "Running... 1 completed · 3 running (4
// targets)" once they differ. suffix follows the state, "..." while the apply
// is active.
func rolloutStateLine(model presentation.Apply, groups []presentation.Group, suffix string) string {
	label := model.Label
	if label != "" {
		label = strings.ToUpper(label[:1]) + label[1:]
	}
	if coverage := templates.RolloutCoverage(model, groups); coverage != "" {
		return label + " on " + coverage + suffix
	}
	if counts := templates.RolloutCountsLine(model, groups); counts != "" {
		return label + suffix + " " + counts
	}
	return label + suffix
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

// writeMultiDeploymentOutcome states how a finished rollout ended, above the
// rollout footer's command, the way the single-target watcher closes: what
// happened, then what to run next.
func (m WatchModel) writeMultiDeploymentOutcome(b *strings.Builder, model presentation.Apply, groups []presentation.Group) {
	switch {
	case state.IsState(model.State, state.Apply.Completed):
		summary := countRolloutTableChanges(m.tables).summary()
		if targets := len(model.Deployments); targets > 1 && summary != "" {
			on := templates.RolloutCoverage(model, groups)
			if on == "" {
				on = rolloutMemberNoun(groups).Count(targets)
			}
			summary = strings.TrimSuffix(summary, ".") + " on " + on + "."
		}
		b.WriteString(templates.FormatApplyCompleteWithSummary(summary, m.applyID))
		b.WriteString("\n")
	case state.IsState(model.State, state.Apply.Stopped):
		b.WriteString(templates.FormatApplyStopped() + outcomeCounts(model, groups) + "\n")
	case state.IsState(model.State, state.Apply.Failed):
		b.WriteString(templates.FormatApplyFailedHeadline() + outcomeCounts(model, groups) + "\n")
	case state.IsState(model.State, state.Apply.Cancelled):
		b.WriteString("🚫 Schema change cancelled" + outcomeCounts(model, groups) + "\n")
	}
}

// outcomeCounts follows a finished rollout's outcome with how its members
// ended: " on all 4 targets" when they agree, ": 1 completed · 3 stopped (4
// targets)" when they differ.
func outcomeCounts(model presentation.Apply, groups []presentation.Group) string {
	if coverage := templates.RolloutCoverage(model, groups); coverage != "" {
		return " on " + coverage
	}
	if counts := templates.RolloutCountsLine(model, groups); counts != "" {
		return ": " + counts
	}
	return ""
}

// rolloutMemberNoun is what the rollout's counts count (templates.RolloutCountsUnit).
func rolloutMemberNoun(groups []presentation.Group) presentation.Noun {
	if templates.RolloutCountsUnit(groups) == "Targets" {
		return presentation.TargetNoun
	}
	return presentation.DeploymentNoun
}

// writeMultiDeploymentFooter closes the view with the keys the operator can
// press. A finished rollout has none: its outcome and the rollout footer's
// command already close it.
func (m WatchModel) writeMultiDeploymentFooter(b *strings.Builder, model presentation.Apply, stopByKey bool) {
	switch {
	case state.IsTerminalApplyState(model.State):
		// Nothing follows the outcome and the rollout footer's command.
	case m.cuttingOver():
		// The watcher ignores ESC and stop until the cutover lands.
		b.WriteString(lipgloss.NewStyle().Faint(true).Render(cutoverInProgressFooter))
		b.WriteString("\n")
	case stopByKey:
		b.WriteString(m.formatFooter())
		b.WriteString("\n")
	default:
		dimStyle := lipgloss.NewStyle().Faint(true)
		b.WriteString(dimStyle.Render("ESC to detach"))
		b.WriteString("\n")
	}
}
