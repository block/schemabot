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
// throws away. This is the disclosure the operator most needs before
// confirming: the work already done on the target is lost and every table in
// the copy is read again from the start, which on a large table is hours.
func writeDiscardedCopies(sb *strings.Builder, copies []ExistingCopyData) {
	n := len(copies)
	fmt.Fprintf(sb, "🗑️ **Discarding work in progress**: **%d** unfinished %s on the target will be thrown away\n",
		n, copyNoun(n))
	writeExistingCopyEntries(sb, copies)
	sb.WriteString("\nApplying copies each of these tables again from the start; the work already done is lost. " +
		"To continue the existing copy instead, apply the same schema change that started it.\n\n")
}

// writeAdoptedCopies writes the section for unfinished copies the apply
// resumes. It reassures rather than warns: nothing is destroyed and the copy
// picks up where it stopped, which is why an operator seeing a long-running
// apply reappear should not expect it to restart.
func writeAdoptedCopies(sb *strings.Builder, copies []ExistingCopyData) {
	n := len(copies)
	fmt.Fprintf(sb, "♻️ **Resuming work in progress**: **%d** unfinished %s on the target will be continued\n",
		n, copyNoun(n))
	writeExistingCopyEntries(sb, copies)
	sb.WriteString("\nApplying picks up where the existing copy stopped rather than starting over.\n\n")
}

// copyNoun is the irregular plural the shared pluralize helper cannot form.
func copyNoun(count int) string {
	if count == 1 {
		return "copy"
	}
	return "copies"
}

func writeExistingCopyEntries(sb *strings.Builder, copies []ExistingCopyData) {
	for _, c := range copies {
		line := existingCopyTableList(c.Tables)
		if c.Namespace != "" {
			line += fmt.Sprintf(" in `%s`", c.Namespace)
		}
		if c.Age != "" {
			line += fmt.Sprintf(" (last progress %s ago)", c.Age)
		}
		if reason := existingCopyReason(c.Reason); reason != "" {
			line += ": " + reason
		}
		fmt.Fprintf(sb, "- %s\n", line)
	}
}

func existingCopyTableList(tables []string) string {
	if len(tables) == 0 {
		return "unnamed tables"
	}
	quoted := make([]string, len(tables))
	for i, t := range tables {
		quoted[i] = "`" + t + "`"
	}
	return strings.Join(quoted, ", ")
}

// existingCopyReason renders the engine's discard reason as operator-facing
// copy. The engine emits a fixed set of identifiers, so an unrecognized one is
// a missing translation rather than untrusted text: it is shown verbatim in
// code ticks, because naming a cause the operator can search for beats dropping
// it.
func existingCopyReason(reason string) string {
	switch reason {
	case "":
		return ""
	case engine.DiscardStatementDiffers:
		return "the schema change differs from the one that started it"
	case engine.DiscardCheckpointExpired:
		return "it is too old to resume"
	default:
		return "`" + reason + "`"
	}
}
