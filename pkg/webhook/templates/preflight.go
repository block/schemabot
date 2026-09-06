package templates

import (
	"strings"

	"github.com/block/schemabot/pkg/glyph"
)

// PreflightStatus is what an apply gate reported when it was read for the
// checklist. It is deliberately three-valued: a gate whose inputs could not be
// read is Unknown and says so, never Ready. The checklist is advisory and
// gates nothing, so an Unknown costs the operator one uncertain line, while a
// wrong Ready would send them to fix one thing and hit another.
type PreflightStatus string

const (
	PreflightReady   PreflightStatus = "ready"
	PreflightBlocked PreflightStatus = "blocked"
	PreflightUnknown PreflightStatus = "unknown"
)

// PreflightRow is one gate's line in the checklist.
type PreflightRow struct {
	// Gate is the operator-facing name of the gate, as its own rejection
	// comment would name it.
	Gate   string
	Status PreflightStatus
	// Detail is a short phrase naming what is not satisfied, for a blocked or
	// unknown gate. It is never raw error text.
	Detail string
}

// preflightStatusCell renders a row's status column.
func preflightStatusCell(row PreflightRow) string {
	switch row.Status {
	case PreflightReady:
		return "✅ Ready"
	case PreflightBlocked:
		return glyph.Attention + " " + row.Detail
	case PreflightUnknown:
		return glyph.Info + " " + row.Detail
	default:
		// A status this renderer does not recognize is uncertainty, and
		// uncertainty renders as uncertainty. Reading it as ready is the one
		// wrong answer here: it would send an operator to re-run against a gate
		// nobody established anything about. The detail is fixed rather than the
		// row's, which an unrecognized status has no reason to have set.
		return glyph.Info + " Status could not be read"
	}
}

// AppendPreflightChecklist adds the remaining-gates checklist to an apply
// rejection comment.
//
// The apply gates run in order and the first one that blocks answers the
// command, so an operator who clears it can land on the next one and learn
// about it only by re-running. Reporting the gates behind the one that blocked
// turns that sequence into a single reading: fix everything named here, then
// re-run once.
//
// When every remaining gate is ready the checklist collapses to one line
// saying so, which is the more common case and the more useful one — it tells
// the operator the retry will get through.
//
// The section is inserted above the support-channel offer when the body
// carries one, so the offer stays the last thing in the comment.
func AppendPreflightChecklist(body string, rows []PreflightRow) string {
	if len(rows) == 0 {
		return body
	}

	var sb strings.Builder
	sb.WriteString("\n### Remaining before this apply can run\n\n")
	if !preflightHasUncleared(rows) {
		sb.WriteString("Nothing else blocks it. Clear the item above and run the apply command again.\n")
	} else {
		sb.WriteString("| Gate | Status |\n| --- | --- |\n")
		for _, row := range rows {
			sb.WriteString("| " + row.Gate + " | " + preflightStatusCell(row) + " |\n")
		}
		sb.WriteString("\nRead when this comment was posted, so clear them together and run the apply command once.\n")
	}

	return insertBeforeSupportChannelOffer(body, sb.String())
}

// preflightHasUncleared reports whether any gate in the checklist is something
// other than ready.
func preflightHasUncleared(rows []PreflightRow) bool {
	for _, row := range rows {
		if row.Status != PreflightReady {
			return true
		}
	}
	return false
}

// insertBeforeSupportChannelOffer splices section into body just above the
// support-channel offer line, or appends it when the body carries no offer.
func insertBeforeSupportChannelOffer(body, section string) string {
	idx := strings.Index(body, supportChannelOfferMarker)
	if idx < 0 {
		return strings.TrimRight(body, "\n") + "\n" + section
	}
	return strings.TrimRight(body[:idx], "\n") + "\n" + section + "\n" + body[idx:]
}
