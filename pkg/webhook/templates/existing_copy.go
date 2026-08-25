package templates

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/ui"
)

// ExistingCopyData is an unfinished copy of one or more tables sitting on the
// target when the plan was made, and what applying the plan does to it. The
// disposition is carried by which slice on PlanCommentData holds the entry.
type ExistingCopyData struct {
	// Namespace is the schema or keyspace holding the copy.
	Namespace string
	// Tables are the tables the copy covers.
	Tables []string
	// Reason is the engine's identifier for why the copy cannot be resumed.
	// Empty for an adopted copy, which needs no explanation.
	Reason string
	// Age is how long ago the copy last made progress, already humanized. It
	// dates the copy's last checkpoint, not how long it has been copying: a copy
	// still running checkpoints continuously, so its age stays small however
	// many hours of work it holds. It is rendered as staleness for that reason,
	// and never as the cost of discarding.
	//
	// It is rendered only for a copy that is over. On a running copy the same
	// number means the opposite thing — the interval between checkpoints, so
	// small is healthy — and there is no reading of "last progress 4 seconds
	// ago" that carries that.
	//
	// Empty for a copy with no recorded progress to date it by.
	Age string
	// Running reports that the copy is still being made right now. It travels
	// with the entry rather than with the section holding it, because a copy
	// can be running and still be one applying throws away: another PR's copy
	// of the same table, live this second, is discarded by a plan whose
	// statement differs from the one that started it. That entry belongs in the
	// destructive warning — the work really is destroyed — but its Age is a
	// heartbeat rather than staleness, so rendering it there would report the
	// liveliest copy on the target as the most abandoned.
	Running bool
	// Statement is the schema change the copy was started for. Empty when the
	// engine has no record of it, which is itself a reason it cannot be
	// resumed. Rendered only where it explains the cause: told the schema
	// change differs from the one that started the copy, the next thing an
	// operator needs is what it differs from.
	Statement string
}

// writeDiscardedCopies writes the section for unfinished copies the apply
// throws away. The work already done on the target is lost and every table in
// the copy is read again from the start, which on a large table is hours.
//
// What is lost is named as the work itself, never as a duration. The only
// duration a copy carries is how long ago it last checkpointed, which is
// staleness rather than elapsed copying — a live copy checkpoints continuously,
// so hours of work show up as seconds of age. Each entry reports it as the
// staleness it is, and the headline carries none.
//
// alreadyApplying selects between the two things this section can be, and the
// difference is whether the reader has a move.
//
// While the copy is still there, applying is a choice with a cost, and the cost
// can be avoided: applying the schema change the copy was made for resumes it
// instead. That is a warning with a remedy, so it takes ⚠️ and closes on what
// confirming spends and how to spend less.
//
// On a comment announcing an apply already under way there is no move. The copy
// is dropped as that comment is posted, and nothing brings it back: not stopping
// the apply, not restoring the earlier schema change. So the section states what
// went and stops. It takes ℹ️, because a reader who cannot act on something is
// being informed, not warned, and it closes after the entries rather than
// offering a remedy that is out of reach or restating the headline as advice.
//
// Both keep the same verb so the two read as one disclosure rather than two.
func writeDiscardedCopies(sb *strings.Builder, copies []ExistingCopyData, alreadyApplying bool) {
	n := len(copies)
	marker, subject := glyph.Attention, "Applying"
	if alreadyApplying {
		marker, subject = glyph.Info, "This apply"
	}
	fmt.Fprintf(sb, "%s **%s destroys work in progress**: **%d** unfinished %s on the target\n",
		marker, subject, n, copyNoun(n))
	writeExistingCopyEntries(sb, copies)
	if alreadyApplying {
		sb.WriteString("\n")
		return
	}
	fmt.Fprintf(sb, "\nApplying restarts the %s from zero rows. To keep the work already done, "+
		"apply the schema %s that started %s.\n\n",
		copyNoun(n), ui.PluralizeLabel("change", "changes", n), ui.PluralizeLabel("it", "them", n))
}

// writeAdoptedCopies writes the section for unfinished copies the apply
// resumes: work an apply that is over left behind on the target. It reassures
// rather than warns: nothing is destroyed and the copy picks up where it
// stopped, which is why an operator seeing a long-running apply reappear should
// not expect it to restart.
//
// alreadyApplying moves the closing line from what applying would do to what
// the apply already under way is doing, matching the discard section on the
// same comment: the two sections can render together, and one describing a
// decision while the other describes an event reads as two comments spliced.
func writeAdoptedCopies(sb *strings.Builder, copies []ExistingCopyData, alreadyApplying bool) {
	n := len(copies)
	subject := "Applying picks"
	if alreadyApplying {
		subject = "This apply picks"
	}
	fmt.Fprintf(sb, "♻️ **Resuming work in progress**: **%d** unfinished %s on the target will be continued\n",
		n, copyNoun(n))
	writeExistingCopyEntries(sb, copies)
	fmt.Fprintf(sb, "\n%s up where the existing %s stopped rather than starting over.\n\n", subject, copyNoun(n))
}

// writeRunningCopies writes the section for copies that are still being made on
// the target right now. It is separate from the resumed section because the
// promise is a different one: there is nothing to pick back up, because nothing
// stopped. Applying resolves the operator to the copy already in flight, which
// keeps every row of it and does not start a second one.
//
// Saying "resumes" here would misdescribe both ends of it — a copy that was
// never interrupted, and an apply that starts nothing — and it is the sentence
// an operator reads before deciding whether a copy they can see progressing is
// about to be disturbed. Every entry here reads "(still copying)" rather than
// an age, which each entry decides for itself: the number behind an age is the
// interval between a live copy's checkpoints, so rendering it as how long ago
// the copy last progressed reports healthy work as nearly stalled — wherever
// the entry lands, including the destructive section, save the one entry whose
// age is its discard reason's own evidence (see writeExistingCopyEntries).
func writeRunningCopies(sb *strings.Builder, copies []ExistingCopyData, alreadyApplying bool) {
	n := len(copies)
	subject := "Applying joins"
	if alreadyApplying {
		subject = "This apply joined"
	}
	fmt.Fprintf(sb, "♻️ **Work already in progress**: **%d** unfinished %s still running on the target\n",
		n, copyNoun(n))
	writeExistingCopyEntries(sb, copies)
	fmt.Fprintf(sb, "\n%s the %s already running rather than starting %s: every row copied so far is kept, and no second %s is made.\n\n",
		subject, copyNoun(n), ui.PluralizeLabel("a new one", "new ones", n), copyNoun(1))
}

// copyNoun is the count-appropriate noun for a row copy.
func copyNoun(count int) string {
	return ui.PluralizeLabel("copy", "copies", count)
}

// writeExistingCopyEntries writes one entry per copy: the tables it covers,
// where they live, whether it is still being made or how stale it is, and why
// it cannot be resumed.
//
// Each entry's Running field selects between the two things the copy's timing
// can mean. A copy that is over is dated by how long ago it last progressed,
// which is what an operator weighs when deciding whether it is worth keeping. A
// copy still being made has no such number to give — its last checkpoint is
// seconds old however many hours of rows it holds — so it says it is running
// and leaves the timing to the progress comment, which reports the copy's
// actual position.
//
// The one exception is a running copy discarded because its checkpoint expired:
// live but unresumable, wedged past the engine's resume bound. There the age is
// not a heartbeat misread as staleness, it is the reason's own justification —
// how far behind the checkpoint has fallen — so the entry renders both.
func writeExistingCopyEntries(sb *strings.Builder, copies []ExistingCopyData) {
	for _, c := range copies {
		line := existingCopyTableList(c.Tables)
		if c.Namespace != "" {
			line += " in " + markdownInlineCode(c.Namespace)
		}
		switch {
		case c.Running && c.Age != "" && c.Reason == engine.DiscardCheckpointExpired:
			line += fmt.Sprintf(" (still copying, last checkpoint %s ago)", c.Age)
		case c.Running:
			line += " (still copying)"
		case c.Age != "":
			line += fmt.Sprintf(" (last progress %s ago)", c.Age)
		}
		if reason := existingCopyReason(c); reason != "" {
			line += ": " + reason
		}
		fmt.Fprintf(sb, "- %s\n", line)
	}
}

// existingCopyTableList renders the copy's tables as a comma-separated list of
// inline code spans. The names are identifiers read off a live target, so they
// are rendered through the shared sanitizer: a quoted identifier may legally
// carry a backtick or a newline, and either one would end the code span early
// or split this entry across lines in a section an operator reads to decide
// whether hours of copying are expendable.
func existingCopyTableList(tables []string) string {
	if len(tables) == 0 {
		return "unnamed tables"
	}
	quoted := make([]string, len(tables))
	for i, t := range tables {
		quoted[i] = markdownInlineCode(t)
	}
	return strings.Join(quoted, ", ")
}

// existingCopyReason renders the engine's discard reason as operator-facing
// copy. An identifier this build does not translate is still shown, in code
// ticks, because naming a cause the operator can search for beats dropping it:
// the reason arrives over the wire from a server that may be a version ahead,
// so an unknown value means a missing translation, not a copy that discards for
// no reason. It goes through the shared sanitizer for the same reason the table
// names do, since this build cannot know what a future value contains.
func existingCopyReason(c ExistingCopyData) string {
	switch c.Reason {
	case "":
		return ""
	case engine.DiscardStatementDiffers:
		// The cause is a comparison, so naming only one side of it leaves the
		// operator to guess the other. The plan's own statements are already
		// in the comment above; this supplies the one they are being compared
		// against, which is also the change the closing line tells them to
		// re-apply to keep the copy.
		differs := "the schema change differs from the one that started it"
		if started := existingCopyStatement(c.Statement); started != "" {
			return differs + ", " + started
		}
		return differs
	case engine.DiscardCheckpointExpired:
		return "it is too old to resume"
	case engine.DiscardCopyIncomplete:
		return "it covers only some of the tables this schema change alters"
	default:
		return markdownInlineCode(c.Reason)
	}
}

// maxExistingCopyStatementLen clamps the statement a copy was started for by
// rune count. A disclosure entry has to stay scannable next to the others in
// its section, and the statement is read off a live target, so its length is
// not this build's to assume.
const maxExistingCopyStatementLen = 200

// existingCopyStatement renders the schema change a copy was started for as a
// single-line inline code span, introduced so the entry reads as a sentence.
//
// The statement comes off a live target, so it is stripped of control text and
// clamped before it is wrapped: an entry in this section has to survive a value
// that is long, multi-line, or hostile. The shared inline-code helper drops the
// backticks around identifiers, which costs nothing here because the statement
// is being shown for comparison rather than to be copied.
func existingCopyStatement(statement string) string {
	s := strings.Join(strings.Fields(stripControlText(statement)), " ")
	if s == "" {
		return ""
	}
	if runes := []rune(s); len(runes) > maxExistingCopyStatementLen {
		s = string(runes[:maxExistingCopyStatementLen-1]) + "…"
	}
	return "which was " + markdownInlineCode(s)
}
