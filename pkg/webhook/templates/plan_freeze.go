package templates

import (
	"fmt"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/ui"
)

// PlanFreezeNotice is the freezes that cover an environment's change when it
// was planned, shown so the author knows before applying that a freeze
// refuses the apply, or will once it starts. It only informs: the apply reads
// the freezes again, and its refusal is the one that counts.
type PlanFreezeNotice struct {
	// InEffect are the freezes that would refuse the apply now.
	InEffect []RefusingFreeze
	// Scheduled are the freezes that have not started yet, oldest start
	// first.
	Scheduled []ScheduledFreeze
	// OverrideOnReason says the server's override policy admits a change on
	// its author's own reason, with no approver.
	OverrideOnReason bool
	// Approvers are the freeze approver teams (freeze.github_teams). Under the
	// approval policy, a pull request's override needs a member of one, so
	// with none the notice offers no override, as the refusal does not.
	Approvers []string
}

// offersOverride reports whether an override can get the change through from
// the pull request: on its reason, or with an approver team to approve it.
func (n *PlanFreezeNotice) offersOverride() bool {
	return n.OverrideOnReason || len(n.Approvers) > 0
}

// ScheduledFreeze is a freeze that covers the change once it starts.
type ScheduledFreeze struct {
	ID       string
	Reason   string
	StartsAt time.Time
	// EndsAt is nil for a freeze that lasts until someone lifts it.
	EndsAt *time.Time
}

// HasFreezes reports whether the notice names any freeze.
func (n *PlanFreezeNotice) HasFreezes() bool {
	return n != nil && (len(n.InEffect) > 0 || len(n.Scheduled) > 0)
}

// writePlanFreezes writes the freezes that cover environment's change: those
// in effect, which refuse its apply now, then those scheduled, which will.
// Every reason is a person's text, so it is redacted and clamped, as in a
// refusal.
func writePlanFreezes(sb *strings.Builder, environment string, notice *PlanFreezeNotice) {
	if !notice.HasFreezes() {
		return
	}
	env := inlineCode(environment)
	if len(notice.InEffect) > 0 {
		fmt.Fprintf(sb, "%s **%s is frozen, so applying this plan would be refused now.**\n\n", glyph.Frozen, env)
		writeRefusingFreezes(sb, notice.InEffect)
		switch {
		case notice.OverrideOnReason:
			fmt.Fprintf(sb, "Wait for %s to end before you apply. If this change can't wait, apply with `--override` and your reason, as the command below does: SchemaBot lets it through and records your reason.\n\n",
				theFreezes(len(notice.InEffect)))
		case len(notice.Approvers) > 0:
			teams := make([]string, 0, len(notice.Approvers))
			for _, team := range notice.Approvers {
				teams = append(teams, inlineCode(team))
			}
			fmt.Fprintf(sb, "Wait for %s to end before you apply. If this change can't wait, apply with `--override` and your reason, as the command below does: a freeze approver other than you and the PR author must approve it before it runs, a member of %s.\n\n",
				theFreezes(len(notice.InEffect)), joinWithOr(teams))
		default:
			fmt.Fprintf(sb, "Wait for %s to end before you apply.\n\n", theFreezes(len(notice.InEffect)))
		}
	}
	if len(notice.Scheduled) > 0 {
		subject := "A freeze is scheduled"
		if len(notice.Scheduled) > 1 {
			subject = "Freezes are scheduled"
		}
		fmt.Fprintf(sb, "%s **%s in %s.** Apply before it starts, or the apply is refused while it holds.\n\n", glyph.Frozen, subject, env)
		writeScheduledFreezes(sb, notice.Scheduled)
	}
}

// appendFreezeOverride adds the --override placeholder to an apply command
// when a freeze in effect would refuse it, so the command the author copies is
// the one that gets an urgent change through. A scheduled freeze adds nothing:
// an apply before it starts needs no override.
func appendFreezeOverride(command string, notice *PlanFreezeNotice) string {
	if notice == nil || len(notice.InEffect) == 0 || !notice.offersOverride() {
		return command
	}
	return command + " " + overridePlaceholder
}

// appendRetryOverride adds --override to the command a refusal asks the
// author to run again, when a freeze in effect would refuse it. It carries
// the reason the refused command gave, so the author does not type it again,
// or the placeholder when the command gave none or its reason cannot be
// quoted back.
func appendRetryOverride(command string, notice *PlanFreezeNotice, reason string) string {
	if notice == nil || len(notice.InEffect) == 0 || !notice.offersOverride() {
		return command
	}
	if quoted, ok := quoteOverrideReason(reason); ok {
		return command + " --override " + quoted
	}
	return command + " " + overridePlaceholder
}

// quoteOverrideReason quotes reason so the command parser reads it back as one
// value. The parser has no escapes: a reason is wrapped in the quote it does
// not contain, and one with both, a line break, or a code fence, which would
// end the block the command renders in, is not quoted back at all.
func quoteOverrideReason(reason string) (string, bool) {
	if strings.TrimSpace(reason) == "" || strings.ContainsAny(reason, "\r\n") || strings.Contains(reason, "```") {
		return "", false
	}
	switch {
	case !strings.Contains(reason, `"`):
		return `"` + reason + `"`, true
	case !strings.Contains(reason, "'"):
		return "'" + reason + "'", true
	default:
		return "", false
	}
}

// writeScheduledFreezes writes the scheduled freezes as a table, at most
// maxListedFreezes of them, and counts the rest.
func writeScheduledFreezes(sb *strings.Builder, freezes []ScheduledFreeze) {
	sb.WriteString("| Freeze | Starts | Holds | Reason |\n| :-- | :-- | :-- | :-- |\n")
	shown := freezes
	if len(shown) > maxListedFreezes {
		shown = shown[:maxListedFreezes]
	}
	for _, freeze := range shown {
		reason := "—"
		if shownReason := RedactedReason(freeze.Reason, maxListedFreezeReason); shownReason != "" {
			reason = tableCellText(markdownReason(shownReason))
		}
		fmt.Fprintf(sb, "| %s | %s | %s | %s |\n", inlineCodeCell(freeze.ID),
			freeze.StartsAt.UTC().Format(time.RFC3339), ui.FreezeUntil(freeze.EndsAt), reason)
	}
	switch unlisted := len(freezes) - len(shown); {
	case unlisted == 1:
		sb.WriteString("\n1 more freeze is scheduled.\n")
	case unlisted > 1:
		fmt.Fprintf(sb, "\n%d more freezes are scheduled.\n", unlisted)
	}
	sb.WriteString("\n")
}
