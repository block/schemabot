package templates

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/engine"
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
	// Age is how long ago the copy last made progress, already humanized.
	// Empty for a copy with no recorded progress to date it by.
	Age string
}

// writeDiscardedCopies writes the section for unfinished copies the apply
// throws away. The work already done on the target is lost and every table in
// the copy is read again from the start, which on a large table is hours.
//
// How long the copy has been running is what makes an operator stop, so it
// leads the section rather than sitting in a parenthetical below the headline.
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
	headlineAge := soleCopyAge(copies)
	marker, subject := "⚠️", "Applying"
	if alreadyApplying {
		marker, subject = "ℹ️", "This apply"
	}
	fmt.Fprintf(sb, "%s **%s destroys work in progress**: **%d** unfinished %s on the target",
		marker, subject, n, copyNoun(n))
	if headlineAge != "" {
		fmt.Fprintf(sb, ", %s of copying", headlineAge)
	}
	sb.WriteString("\n")
	writeExistingCopyEntries(sb, copies, headlineAge == "")
	if alreadyApplying {
		sb.WriteString("\n")
		return
	}
	lost := "the work already done"
	if headlineAge != "" {
		lost = "the " + headlineAge + " already spent"
	}
	fmt.Fprintf(sb, "\nApplying copies the tables above again from zero rows, so it runs as long as a first copy would; "+
		"%s is lost and cannot be recovered. "+
		"To continue the existing copy instead, apply the same schema change that started it.\n\n", lost)
}

// soleCopyAge is the age to put in the headline: the copy's own, when exactly
// one copy is being reported and its age is known. Empty otherwise — several
// copies have several ages and no one of them describes what applying costs, so
// each keeps its age on its own entry.
func soleCopyAge(copies []ExistingCopyData) string {
	if len(copies) != 1 {
		return ""
	}
	return copies[0].Age
}

// writeAdoptedCopies writes the section for unfinished copies the apply
// resumes. It reassures rather than warns: nothing is destroyed and the copy
// picks up where it stopped, which is why an operator seeing a long-running
// apply reappear should not expect it to restart.
func writeAdoptedCopies(sb *strings.Builder, copies []ExistingCopyData) {
	n := len(copies)
	fmt.Fprintf(sb, "♻️ **Resuming work in progress**: **%d** unfinished %s on the target will be continued\n",
		n, copyNoun(n))
	writeExistingCopyEntries(sb, copies, true)
	sb.WriteString("\nApplying picks up where the existing copy stopped rather than starting over.\n\n")
}

// copyNoun is the irregular plural the shared pluralize helper cannot form.
func copyNoun(count int) string {
	if count == 1 {
		return "copy"
	}
	return "copies"
}

// writeExistingCopyEntries writes one entry per copy. withAge is false when the
// section headline already carries the age, so the entry does not repeat it.
func writeExistingCopyEntries(sb *strings.Builder, copies []ExistingCopyData, withAge bool) {
	for _, c := range copies {
		line := existingCopyTableList(c.Tables)
		if c.Namespace != "" {
			line += " in " + markdownInlineCode(c.Namespace)
		}
		if withAge && c.Age != "" {
			line += fmt.Sprintf(" (last progress %s ago)", c.Age)
		}
		if reason := existingCopyReason(c.Reason); reason != "" {
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
func existingCopyReason(reason string) string {
	switch reason {
	case "":
		return ""
	case engine.DiscardStatementDiffers:
		return "the schema change differs from the one that started it"
	case engine.DiscardCheckpointExpired:
		return "it is too old to resume"
	case engine.DiscardCopyIncomplete:
		return "it covers only some of the tables this schema change alters"
	default:
		return markdownInlineCode(reason)
	}
}
