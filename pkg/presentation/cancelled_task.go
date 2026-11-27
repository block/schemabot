package presentation

import "github.com/block/schemabot/pkg/ui"

// CancelledTask is how a cancelled table reads, on every surface that lists
// it.
type CancelledTask struct {
	// Label is the outcome text, such as "Cancelled at 40%" or "Not run".
	Label string
	// NotRun marks a table its apply's failure cancelled before it began. No
	// one cancelled it, so a surface marks it as skipped rather than with the
	// cancel glyph.
	NotRun bool
}

// ReadCancelledTask reads a cancelled table. A row copy the cancel stopped
// names how far it had come. A table that began reads as cancelled. One that
// never began reads as not run when its apply failed, since the failure and
// not an operator stopped it, and as cancelled before it started otherwise.
// began reports whether the engine started the table's change; a surface
// with no start signal passes false.
func ReadCancelledTask(f TableFacts, began, applyFailed bool) CancelledTask {
	switch {
	case f.PercentComplete > 0 || f.RowsCopied > 0:
		return CancelledTask{Label: "Cancelled at " + ui.FormatRowCopyPercent(f.PercentComplete, f.RowsCopied, f.RowsTotal) + ShardCoverageSuffix(f)}
	case began:
		return CancelledTask{Label: "Cancelled"}
	case applyFailed:
		return CancelledTask{Label: "Not run", NotRun: true}
	default:
		return CancelledTask{Label: "Cancelled (not started)"}
	}
}
