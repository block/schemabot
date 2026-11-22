package templates

import (
	"fmt"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/ui"
)

// Freeze text comes from whoever set the freeze or asked for the override, so
// every free-text field is printed through ui.TerminalSafeLine.

func freezeTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

func freezeKind(kind string) string {
	return strings.ReplaceAll(kind, "_", " ")
}

// freezeScope says what a freeze covers.
func freezeScope(f apitypes.Freeze) string {
	var parts []string
	if len(f.Deployments) > 0 {
		parts = append(parts, "deployments "+ui.TerminalSafeLine(strings.Join(f.Deployments, ", ")))
	}
	if len(f.Databases) > 0 {
		parts = append(parts, "databases "+ui.TerminalSafeLine(strings.Join(f.Databases, ", ")))
	}
	if len(parts) == 0 {
		return "whole environment"
	}
	return strings.Join(parts, "; ")
}

// freezeUntil is the end of a freeze's window; one with no end lasts until
// someone lifts it.
func freezeUntil(f apitypes.Freeze) string {
	return ui.FreezeUntil(f.EndsAt)
}

// WriteFreezeCreated writes the confirmation of a new freeze.
func WriteFreezeCreated(f apitypes.Freeze) {
	state := "frozen"
	if !f.InEffect {
		state = "freeze scheduled"
	}
	fmt.Printf("%s %s created: %s %s (%s, %s) by %s. Reason: %s\n", glyph.Attention,
		ui.TerminalSafeLine(f.ID), ui.TerminalSafeLine(f.Environment), state,
		freezeKind(f.Kind), freezeUntil(f), ui.TerminalSafeLine(f.CreatedBy), ui.TerminalSafeLine(f.Reason))
	fmt.Printf("   Scope:           %s\n", freezeScope(f))
	fmt.Printf("   Starts:          %s\n", freezeTime(f.StartsAt))
	fmt.Printf("   Running applies: %s\n", freezeRunning(f.Running))
}

// freezeRunning shows the recorded running policy. SchemaBot does not act on
// stop yet, so a responder must not read it as applies having stopped.
func freezeRunning(policy string) string {
	if policy == "stop" {
		return ui.TerminalSafeLine(policy) + " (recorded; running applies are not stopped yet, use 'schemabot stop')"
	}
	return ui.TerminalSafeLine(policy)
}

// WriteFreezeLifted writes the confirmation of a lifted freeze.
func WriteFreezeLifted(f apitypes.Freeze) {
	fmt.Printf("✓ %s lifted.\n", ui.TerminalSafeLine(f.ID))
}

// writeFreezeRows prints one freeze under a heading. detail adds the fields
// a listing leaves out.
func writeFreezeRows(f apitypes.Freeze, heading string, detail bool) {
	status := ""
	switch {
	case f.LiftedAt != nil:
		status = ", lifted"
	case f.InEffect:
		status = ", in effect"
	}
	fmt.Printf("%s%s  %s (%s%s)\n", heading, ui.TerminalSafeLine(f.ID), ui.TerminalSafeLine(f.Environment), freezeKind(f.Kind), status)
	row := func(label, value string) { fmt.Printf("     %-9s %s\n", label+":", value) }
	row("Scope", freezeScope(f))
	row("Window", freezeTime(f.StartsAt)+" "+freezeUntil(f))
	row("Reason", ui.TerminalSafeLine(f.Reason))
	row("By", ui.TerminalSafeLine(f.CreatedBy))
	if f.LiftedAt != nil {
		lifted := fmt.Sprintf("by %s at %s", ui.TerminalSafeLine(f.LiftedBy), freezeTime(*f.LiftedAt))
		if f.LiftReason != "" {
			lifted += ": " + ui.TerminalSafeLine(f.LiftReason)
		}
		row("Lifted", lifted)
	}
	if detail {
		row("Running", ui.TerminalSafeLine(f.Running)+" (applies already running in scope)")
		row("Source", ui.TerminalSafeLine(f.Source))
		if f.ExternalID != "" {
			row("External", ui.TerminalSafeLine(f.ExternalID))
		}
		row("Created", freezeTime(f.CreatedAt))
	}
}

// WriteFreezeShow writes one freeze in detail.
func WriteFreezeShow(f apitypes.Freeze) {
	writeFreezeRows(f, "Freeze ", true)
}

// WriteFreezeList writes the three freeze groups the API returns.
func WriteFreezeList(list apitypes.FreezeListResponse) {
	groups := []struct {
		title   string
		freezes []apitypes.Freeze
	}{
		{"In effect", list.InEffect},
		{"Scheduled", list.Scheduled},
		{"Recent", list.Recent},
	}
	for i, group := range groups {
		if i > 0 {
			fmt.Println()
		}
		fmt.Printf("%s (%d)\n", group.title, len(group.freezes))
		if len(group.freezes) == 0 {
			fmt.Println("  None.")
		}
		for j, f := range group.freezes {
			writeFreezeRows(f, fmt.Sprintf("  %d. ", j+1), false)
		}
	}
}

// WriteFreezeOverrideApproved writes the confirmation of an approval.
func WriteFreezeOverrideApproved(r apitypes.FreezeOverrideRequest) {
	requester := ui.TerminalSafeLine(r.RequestedBy)
	fmt.Printf("✓ Approved %s for %s on %s in %s. Requested by %s: %s. It expires at %s. %s can now re-run the command.\n",
		ui.TerminalSafeLine(r.ID), ui.TerminalSafeLine(r.Action), ui.TerminalSafeLine(r.Database),
		ui.TerminalSafeLine(r.Environment), requester, ui.TerminalSafeLine(r.Reason),
		freezeTime(r.ExpiresAt), requester)
}

// WriteFreezeOverride writes one freeze override request.
func WriteFreezeOverride(r apitypes.FreezeOverrideRequest) {
	status := "waiting for approval"
	if r.ApprovedAt != nil {
		status = fmt.Sprintf("approved by %s at %s", ui.TerminalSafeLine(r.ApprovedBy), freezeTime(*r.ApprovedAt))
	}
	fmt.Printf("Override request %s: %s\n", ui.TerminalSafeLine(r.ID), status)
	row := func(label, value string) { fmt.Printf("  %-13s %s\n", label+":", value) }
	row("Action", fmt.Sprintf("%s on %s in %s", ui.TerminalSafeLine(r.Action), ui.TerminalSafeLine(r.Database), ui.TerminalSafeLine(r.Environment)))
	row("Target", ui.TerminalSafeLine(r.TargetID))
	row("Blockers", ui.TerminalSafeLine(strings.Join(r.Blockers, ", ")))
	row("Requested by", ui.TerminalSafeLine(r.RequestedBy))
	row("Reason", ui.TerminalSafeLine(r.Reason))
	row("Expires", freezeTime(r.ExpiresAt))
}

// ChangeGateRefusedData is what a freeze refusal of an apply, start or cutover
// tells the operator.
type ChangeGateRefusedData struct {
	Environment string
	// Operation is "apply", "start", "cutover" or "rollback".
	Operation string
	Blockers  []apitypes.ChangeGateBlocker
	// OverrideRequest is the request SchemaBot recorded for this caller, nil
	// when it recorded none.
	OverrideRequest *apitypes.FreezeOverrideRequest
	// OverrideAsked says the operator passed --override.
	OverrideAsked bool
	// Message is the server's refusal, which says why no override request
	// was recorded when one was asked for.
	Message string
	// OverrideCommand is the CLI command that asks for an override, for a
	// caller whose own request cannot carry --override: the watch TUI's keys
	// send none. Empty when the operator ran a command they can rerun with it.
	OverrideCommand string
}

// ChangeGateRefusedText is the refusal as lines, so a caller that cannot print
// it to stdout can carry the same words in an error.
func ChangeGateRefusedText(d ChangeGateRefusedData) []string {
	outcome := "This change did not start."
	if d.Operation == "cutover" {
		outcome = "This cutover did not start."
	}
	lines := []string{fmt.Sprintf("%s %s is frozen. %s", glyph.Frozen, ui.TerminalSafeLine(d.Environment), outcome)}
	for _, b := range d.Blockers {
		lines = append(lines, fmt.Sprintf("   %s  %s  (%s)", ui.TerminalSafeLine(b.ID), ui.TerminalSafeLine(b.Reason), ui.FreezeUntil(b.EndsAt)))
	}
	switch {
	case d.OverrideRequest != nil:
		id := ui.TerminalSafeLine(d.OverrideRequest.ID)
		lines = append(lines, fmt.Sprintf("Override requested: %s. A freeze approver other than you must run '%s', then run this command again.",
			id, cliname.Command("freeze", "approve", id)))
		// Asking again while a request is pending keeps its first reason, so
		// say which one the approver sees.
		if d.OverrideRequest.Reason != "" {
			lines = append(lines, "Reason the approver sees: "+ui.TerminalSafeLine(d.OverrideRequest.Reason))
		}
	case d.Operation == "rollback":
		// rollback has no --override; only the freezes ending lets it run.
		lines = append(lines, "Run the rollback again once these freezes end or are lifted.")
	case !d.OverrideAsked && d.OverrideCommand != "":
		// The command gets a line of its own so a narrow watch, which cuts
		// wide lines rather than wrapping them, still shows it whole.
		lines = append(lines, "To ask for an override, run:",
			fmt.Sprintf(`  %s --override "<reason>"`, ui.TerminalSafeLine(d.OverrideCommand)))
	case !d.OverrideAsked:
		lines = append(lines, `To ask for an override, run the same command with --override "<reason>".`)
	default:
		lines = append(lines, "No override request was recorded. SchemaBot said: "+ui.TerminalSafeLine(d.Message))
	}
	return lines
}

// WriteChangeGateRefused writes the refusal of an apply, start or cutover by a
// freeze.
func WriteChangeGateRefused(d ChangeGateRefusedData) {
	for _, line := range ChangeGateRefusedText(d) {
		fmt.Println(line)
	}
}
