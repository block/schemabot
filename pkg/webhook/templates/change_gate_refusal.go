package templates

import (
	"fmt"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/ui"
	"github.com/block/schemabot/pkg/webhook/action"
)

// ChangeGateRefusalData is a PR command that a freeze refused.
type ChangeGateRefusalData struct {
	// CommandName is the refused PR command, such as action.Apply.
	CommandName string
	// TakesOverride says the command accepts `--override "<reason>"`, from
	// the command's spec, so a new command that takes one is offered it.
	TakesOverride bool
	Environment   string
	// Tenant is the deployment's own, carried by the approve command the
	// comment names, so a pasted command reaches the deployment that stores
	// the request.
	Tenant string
	// RequestedBy is the commenter's GitHub login.
	RequestedBy string
	Timestamp   string
	Freezes     []RefusingFreeze
	// Override is the request SchemaBot recorded for the commenter, nil when
	// it recorded none.
	Override *RefusalOverrideRequest
	// OverrideUnrecorded says the commenter asked for an override and
	// SchemaBot could not record one.
	OverrideUnrecorded bool
	// Approvers are the GitHub teams, as org/team, whose members may approve
	// an override from this pull request. Empty means nobody can.
	Approvers []string
}

// RefusingFreeze is one freeze that holds the environment.
type RefusingFreeze struct {
	ID     string
	Reason string
	// EndsAt is nil for a freeze that lasts until someone lifts it.
	EndsAt *time.Time
}

// RefusalOverrideRequest is a recorded request to let the change through.
type RefusalOverrideRequest struct {
	ID string
	// Requester and Author are GitHub logins without any prefix.
	Requester string
	Author    string
	Reason    string
	// ReasonKept says the commenter asked before, so the request keeps the
	// reason given then.
	ReasonKept bool
}

const (
	// maxListedFreezes is how many freezes a refusal lists; the rest are
	// counted.
	maxListedFreezes = 10
	// maxListedFreezeReason and maxShownOverrideReason bound, in runes, the
	// reasons a refusal quotes. The full reasons are on the freeze and the
	// request.
	maxListedFreezeReason  = 150
	maxShownOverrideReason = 300
	// overridePlaceholder is the reason the comment shows in an --override
	// example. It sits in a code span, so it reads as written.
	overridePlaceholder = `--override "<why it can't wait>"`
)

// RenderChangeGateRefusal renders a freeze refusal of a PR command: what is
// frozen, each freeze with when it ends and why, and what the commenter can
// do next. Every reason is a person's text, so it is redacted and clamped,
// and a mention in it notifies nobody.
//
// An override is approved by someone other than the commenter and the pull
// request's author, and then the commenter runs the command again: an
// approved override admits only the person who asked for it. A refused
// apply-confirm has released its pending confirmation, so its retry starts
// with apply.
func RenderChangeGateRefusal(data ChangeGateRefusalData) string {
	var b strings.Builder
	blocked := "a Freeze"
	if len(data.Freezes) > 1 {
		blocked = "Freezes"
	}
	fmt.Fprintf(&b, "## %s %s Blocked by %s\n\n", glyph.Frozen, capitalizeFirst(data.CommandName), blocked)
	fmt.Fprintf(&b, "**Environment**: %s\n\n", inlineCode(data.Environment))
	b.WriteString(SchemaErrorData{RequestedBy: data.RequestedBy, Timestamp: data.Timestamp}.Attribution() + "\n\n")

	outcome := "Nothing changed."
	if data.CommandName == action.ApplyConfirm {
		outcome = "Nothing was applied, and the pending confirmation was released."
	}
	fmt.Fprintf(&b, "%s is frozen, so this command did not run. %s\n\n", inlineCode(data.Environment), outcome)
	writeRefusingFreezes(&b, data.Freezes)

	switch request := data.Override; {
	case request != nil:
		writeOverrideRequested(&b, data, request)
	case data.OverrideUnrecorded:
		retry := "Run the command again"
		if data.CommandName == action.ApplyConfirm {
			retry = "Run `apply` again, then `apply-confirm` with `--override`"
		}
		b.WriteString("### Override not recorded\n\n")
		b.WriteString("SchemaBot could not confirm who is asking and who wrote this pull request, so it recorded no override request. " +
			retry + ". If this keeps happening, the SchemaBot server logs have the cause.\n")
	default:
		writeRefusalNextSteps(&b, data)
	}
	return offerSupportChannel(b.String())
}

// writeRefusingFreezes writes the freezes as a table, at most
// maxListedFreezes of them, and counts the rest.
func writeRefusingFreezes(b *strings.Builder, freezes []RefusingFreeze) {
	if len(freezes) == 0 {
		return
	}
	b.WriteString("| Freeze | Holds | Reason |\n| :-- | :-- | :-- |\n")
	shown := freezes
	if len(shown) > maxListedFreezes {
		shown = shown[:maxListedFreezes]
	}
	for _, freeze := range shown {
		reason := "—"
		if shownReason := RedactedReason(freeze.Reason, maxListedFreezeReason); shownReason != "" {
			reason = tableCellText(markdownReason(shownReason))
		}
		fmt.Fprintf(b, "| %s | %s | %s |\n", inlineCodeCell(freeze.ID), ui.FreezeUntil(freeze.EndsAt), reason)
	}
	switch unlisted := len(freezes) - len(shown); {
	case unlisted == 1:
		b.WriteString("\n1 more freeze also holds it.\n")
	case unlisted > 1:
		fmt.Fprintf(b, "\n%d more freezes also hold it.\n", unlisted)
	}
	b.WriteString("\n")
}

// writeRefusalNextSteps writes what the commenter can do when no override was
// asked for: wait, and, for a command that takes --override when a team can
// approve one, ask for one. The approver teams are named, not mentioned:
// nobody has asked them for anything yet, and a freeze can refuse many pull
// requests at once.
func writeRefusalNextSteps(b *strings.Builder, data ChangeGateRefusalData) {
	b.WriteString("### What you can do\n\n")
	fmt.Fprintf(b, "- **Wait for %s to end**, then %s.\n", theFreezes(len(data.Freezes)), rerun(data.CommandName))
	// The CLI always names --override, because an API request can be
	// approved through the API. A pull request's request can be approved only
	// from the pull request, by a member of an approver team, so with no team
	// the comment offers no override nobody could approve.
	if !data.TakesOverride || len(data.Approvers) == 0 {
		return
	}
	ask := "Run the same command with `" + overridePlaceholder + "`."
	if data.CommandName == action.ApplyConfirm {
		ask = "Run `apply` again, then run `apply-confirm` with `" + overridePlaceholder + "`."
	}
	teams := make([]string, 0, len(data.Approvers))
	for _, team := range data.Approvers {
		teams = append(teams, inlineCode(team))
	}
	fmt.Fprintf(b, "- **Ask for an override** if this change can't wait. %s "+
		"A freeze approver other than you and the PR author must approve it: a member of %s.\n", ask, joinWithOr(teams))
}

// theFreezes names the refusing freezes as the subject of a sentence.
func theFreezes(count int) string {
	if count > 1 {
		return "the freezes"
	}
	return "the freeze"
}

// rerun says how to run a refused command again. A refused apply-confirm has
// released its pending confirmation, so its retry starts with apply.
func rerun(command string) string {
	if command == action.ApplyConfirm {
		return "run `apply` again, then `apply-confirm`"
	}
	return "run the same command again"
}

// writeOverrideRequested writes the recorded request and the two steps that
// let the change through: another approver approves it, then the requester
// runs the command again.
func writeOverrideRequested(b *strings.Builder, data ChangeGateRefusalData, request *RefusalOverrideRequest) {
	b.WriteString("### Override requested\n\n")
	// With no approver team, the request can never be approved here, so the
	// steps would send the reader to wait on nobody.
	if len(data.Approvers) == 0 {
		fmt.Fprintf(b, "SchemaBot recorded override request %s, but no team can approve an override from a pull request, so nobody can approve it here. "+
			"Wait for %s to end, then %s.\n", inlineCode(request.ID), theFreezes(len(data.Freezes)), rerun(data.CommandName))
		return
	}
	fmt.Fprintf(b, "Override request %s is waiting for a freeze approver.\n\n", inlineCode(request.ID))
	if reason := RedactedReason(request.Reason, maxShownOverrideReason); reason != "" {
		b.WriteString("**Reason the approver sees:** " + markdownReason(reason))
		if !endsSentence(reason) {
			b.WriteString(".")
		}
		if request.ReasonKept {
			b.WriteString(" You asked for it earlier, so it keeps the reason you gave then.")
		}
		b.WriteString("\n\n")
	}

	// The request waits on these teams, so they are mentioned, the same as the
	// reviewers a review-required comment waits on.
	b.WriteString("**Freeze approvers**:\n")
	writeMentionList(b, data.Approvers)
	b.WriteString("\n")

	approver := fmt.Sprintf("A freeze approver other than @%s and other than the PR author (@%s)", request.Requester, request.Author)
	if strings.EqualFold(request.Requester, request.Author) {
		approver = fmt.Sprintf("A freeze approver other than @%s, who wrote this pull request,", request.Requester)
	}
	again := "runs this command again"
	if data.CommandName == action.ApplyConfirm {
		again = "runs `apply` again, then `apply-confirm`"
	}
	fmt.Fprintf(b, "1. %s comments:\n   ```\n   %s\n   ```\n", approver, AppendTenantFlag("schemabot freeze approve "+request.ID, data.Tenant))
	fmt.Fprintf(b, "2. Then @%s %s.\n", request.Requester, again)
}

// tableCellText keeps Markdown text on one table row: a cell separator would
// split the row, so every pipe not already escaped is escaped, which GFM
// honours inside a code span too. A pipe is already escaped when an odd
// number of backslashes precede it.
func tableCellText(text string) string {
	var b strings.Builder
	backslashes := 0
	for _, r := range text {
		if r == '|' && backslashes%2 == 0 {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
		if r == '\\' {
			backslashes++
		} else {
			backslashes = 0
		}
	}
	return b.String()
}

// endsSentence reports whether text already ends a sentence, so no period is
// added after it. A clamped reason ends in "…".
func endsSentence(text string) bool {
	return strings.HasSuffix(text, ".") || strings.HasSuffix(text, "!") ||
		strings.HasSuffix(text, "?") || strings.HasSuffix(text, "…")
}
