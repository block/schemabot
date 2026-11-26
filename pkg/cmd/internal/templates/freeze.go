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
	fmt.Printf("   Running applies: %s\n", freezeRunning(f))
	writeFreezeStopSweep(f.StopSweep)
}

// freezeRunning shows the running policy, and for stop, what setting the
// freeze did to the applies already running, so a responder never reads it
// as more than happened.
func freezeRunning(f apitypes.Freeze) string {
	policy := ui.TerminalSafeLine(f.Running)
	if f.Running != "stop" {
		return policy
	}
	if !f.InEffect {
		return policy + " (applies running when it starts are not stopped; use 'schemabot stop')"
	}
	return policy
}

// writeFreezeStopSweep lists the applies a stop freeze asked to stop when it
// was set, and the ones it did not, each with the reason. A stop is a durable
// request its driver delivers, so the applies listed may still be writing as
// this prints; status confirms each one has stopped. An apply not stopped
// keeps running, and its reason says what to do, since the remedy differs:
// one engine has no stop at all.
func writeFreezeStopSweep(sweep *apitypes.FreezeStopSweep) {
	if sweep == nil {
		return
	}
	if sweep.Error != "" {
		fmt.Printf("   Stop requested:  none. %s\n", ui.TerminalSafeLine(sweep.Error))
		return
	}
	switch {
	case len(sweep.Stopped) > 0:
		fmt.Printf("   Stop requested:  %s\n", ui.TerminalSafeLine(strings.Join(sweep.Stopped, ", ")))
		fmt.Printf("                    Confirm each one has stopped with '%s'.\n", cliname.Command("status", "<apply-id>"))
	case len(sweep.NotStopped) == 0:
		fmt.Println("   Stop requested:  none; no schema change in scope was pending or running")
	default:
		fmt.Println("   Stop requested:  none")
	}
	for _, unstopped := range sweep.NotStopped {
		fmt.Printf("   Not stopped:     %s keeps running: %s\n", ui.TerminalSafeLine(unstopped.ApplyID), ui.TerminalSafeLine(unstopped.Reason))
	}
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
	switch {
	case r.ApprovedAt != nil && strings.EqualFold(strings.TrimSpace(r.ApprovedBy), strings.TrimSpace(r.RequestedBy)):
		// Only the reason override policy stores a request approved by its
		// own requester.
		status = fmt.Sprintf("admitted on its requester's reason at %s", freezeTime(*r.ApprovedAt))
	case r.ApprovedAt != nil:
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
		lines = append(lines, "If this change can't wait, override the freeze with a reason:",
			fmt.Sprintf(`  %s --override "<reason>"`, ui.TerminalSafeLine(d.OverrideCommand)))
	case !d.OverrideAsked:
		lines = append(lines, `If this change can't wait, run the same command with --override "<reason>".`)
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

// StoppedByFreezeText is what status says about an apply a freeze with
// running "stop" stopped (AZ-11): which freeze, and how the apply resumes:
// after the freeze ends or with an override while it is in effect, and with a
// plain start once it has ended. It is empty when no freeze stopped the
// apply. StoppedByFreezeNote in pkg/webhook/templates tells the PR the same.
func StoppedByFreezeText(freeze *apitypes.FreezeStopRef, applyID, environment string) string {
	if freeze == nil || freeze.ID == "" {
		return ""
	}
	text := "Stopped by freeze " + ui.TerminalSafeLine(freeze.ID)
	if freeze.Reason != "" {
		text += " (" + ui.TerminalSafeLine(freeze.Reason) + ")"
	}
	start := cliname.Command("start", ui.TerminalSafeLine(applyID), "-e", ui.TerminalSafeLine(environment))
	if freeze.Ended {
		return text + fmt.Sprintf(", which has ended. Run '%s' to resume it.", start)
	}
	return text + fmt.Sprintf(". Run '%s' after the freeze ends or is lifted, or add --override \"<reason>\" if it can't wait.", start)
}

// writeStoppedByFreeze writes StoppedByFreezeText below the status box.
func writeStoppedByFreeze(data ProgressData) {
	if text := StoppedByFreezeText(data.StoppedByFreeze, data.ApplyID, data.Environment); text != "" {
		fmt.Printf("\n  ⏹️ %s\n", text)
	}
}

// PlanFreezesText is what a plan says about the freezes that cover its change
// in environment: those in effect, which refuse its apply now, then those
// scheduled, which will once they start. It says nothing when none covers it.
// It only informs: the apply checks the freezes again, and its refusal is the
// one that counts.
func PlanFreezesText(environment string, freezes []apitypes.Freeze) []string {
	var inEffect, scheduled []apitypes.Freeze
	for _, f := range freezes {
		if f.InEffect {
			inEffect = append(inEffect, f)
		} else {
			scheduled = append(scheduled, f)
		}
	}
	env := ui.TerminalSafeLine(environment)
	var lines []string
	if len(inEffect) > 0 {
		lines = append(lines, fmt.Sprintf("%s %s is frozen, so applying this plan would be refused now.", glyph.Frozen, env))
		for _, f := range inEffect {
			lines = append(lines, fmt.Sprintf("   %s  %s  (%s)", ui.TerminalSafeLine(f.ID), ui.TerminalSafeLine(f.Reason), freezeUntil(f)))
		}
		subject := "the freeze"
		if len(inEffect) > 1 {
			subject = "the freezes"
		}
		// The PR comment words the override by the server's policy. The plan
		// response carries no policy, so this says what --override does under
		// either one, as the --override help does. The CLI refusal offers
		// --override under every policy too.
		lines = append(lines, fmt.Sprintf(`Wait for %s to end before you apply. If this change can't wait, apply with --override "<reason>": as the server's override policy says, SchemaBot lets it through on your reason, or asks a freeze approver to approve it first.`, subject))
	}
	if len(scheduled) > 0 {
		if len(lines) > 0 {
			lines = append(lines, "")
		}
		subject := "A freeze is scheduled"
		if len(scheduled) > 1 {
			subject = "Freezes are scheduled"
		}
		lines = append(lines, fmt.Sprintf("%s %s in %s. Apply before it starts, and let the change finish: while it holds, the apply is refused, and so is a start or cutover the change still needs.", glyph.Frozen, subject, env))
		for _, f := range scheduled {
			lines = append(lines, fmt.Sprintf("   %s  %s  (starts %s, %s)", ui.TerminalSafeLine(f.ID), ui.TerminalSafeLine(f.Reason), freezeTime(f.StartsAt), freezeUntil(f)))
		}
	}
	return lines
}

// WritePlanFreezes writes PlanFreezesText, then a blank line when it wrote
// any.
func WritePlanFreezes(environment string, freezes []apitypes.Freeze) {
	lines := PlanFreezesText(environment, freezes)
	for _, line := range lines {
		fmt.Println(line)
	}
	if len(lines) > 0 {
		fmt.Println()
	}
}
