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
			fmt.Fprintf(sb, "Wait for %s to end before you apply. If this change can't wait, apply with `--override` and your reason, as the command below does. A freeze approver other than you and the PR author, a member of %s, must approve it, and then you run the command again.\n\n",
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
		fmt.Fprintf(sb, "%s **%s in %s.** Apply before it starts, and let the change finish: while it holds, the apply is refused, and so is a `start` or `cutover` the change still needs.\n\n", glyph.Frozen, subject, env)
		writeScheduledFreezes(sb, notice.Scheduled)
	}
}

// writePlanFreezesIfApplyOffered writes the plan's freeze notice only when
// its footer offers the apply. The notice points at that command, so a plan
// whose apply is withheld, by a member apply refusal or behind an earlier
// environment's refused apply, shows none: it would send the author to a
// command the comment does not give.
func writePlanFreezesIfApplyOffered(sb *strings.Builder, data PlanCommentData) {
	if !data.OffersApply() || data.MemberApplyRefusal != "" || data.applyWithheld {
		return
	}
	writePlanFreezes(sb, data.Environment, data.Freezes)
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
