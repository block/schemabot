package presentation

import (
	"fmt"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

// TableFacts is what a surface knows about one table's change while the
// engine works on it. Each surface fills it from its own data, and
// ReadInFlightTable turns it into the words every surface shows, whichever
// engine runs the change.
type TableFacts struct {
	Status          string
	ChangeType      string // create, alter, drop
	IsInstant       bool
	PercentComplete int
	RowsCopied      int64
	RowsTotal       int64
	// RowsTotalPlanned marks a row total taken from the plan's estimate
	// because some shards or targets have not reported one yet.
	RowsTotalPlanned bool
	ETASeconds       int64
	// EstimatedRows is the table's row count when it was planned, nil when
	// the plan could not count it.
	EstimatedRows       *int64
	EstimatedBytes      *int64
	ChecksumRowsChecked int64
	ChecksumRowsTotal   int64
	Throttled           bool
	ThrottleReason      string
	BuildWork           apitypes.BuildWork
	// ShardsReporting and ShardsTotal describe a sharded table whose row
	// figures cover only the shards that have reported a total. Both are zero
	// when the figures cover the whole table.
	ShardsReporting int
	ShardsTotal     int
	// PlannedShards is how many shards the plan sized the table across.
	PlannedShards int
}

// partialShards reports whether the table's row figures cover only some of
// its shards.
func (f TableFacts) partialShards() bool {
	return f.ShardsReporting > 0 && f.ShardsReporting < f.ShardsTotal
}

// WithShardCoverage returns the facts with the shards their row figures cover,
// for a table that lists its shards: listed is how many it lists and reporting
// how many of those have reported a row total. The table spans the plan's
// shard count, or the listed shards when more are listed or the plan recorded
// none. While later shards have not reported, the figures cover the reporting
// ones alone, so the reading says so rather than passing their fraction off
// as the table's. A table with one shard or none listed is left as it is, and
// so is one whose total already counts the plan's estimate for the shards that
// have not reported, since that total spans the whole table.
func (f TableFacts) WithShardCoverage(listed, reporting int) TableFacts {
	if listed < 2 || f.RowsTotalPlanned {
		return f
	}
	if total := max(listed, f.PlannedShards); reporting > 0 && reporting < total {
		f.ShardsReporting, f.ShardsTotal = reporting, total
	}
	return f
}

// TableReading is how one table's change reads on every surface: a bar and a
// label beside the table name, then detail lines under its statement. The
// surfaces differ only in markup.
type TableReading struct {
	// Bar is the progress bar that leads the label, or empty when the
	// change has no measure of its progress to draw.
	Bar   string
	Label string
	// Activity marks a copy that has passed its row estimate. Its label
	// reads ActivityLabel then ShardCoverageSuffix, and a surface that
	// animates may draw its own bar and label in place of Bar and
	// ActivityLabel.
	Activity bool
	// Throttled marks a change the engine's throttler is holding back. The
	// headline says so after the label.
	Throttled bool
	Details   []TableDetail
}

// ActivityLabel is the label of a copy that has passed its row estimate.
const ActivityLabel = "Finalizing copy"

// Headline is the bar, label, and throttle annotation as one line, without
// the table name.
func (r TableReading) Headline() string {
	if r.Bar == "" {
		return r.Label + ui.ThrottledSuffix(r.Throttled)
	}
	return r.Bar + " " + r.Label + ui.ThrottledSuffix(r.Throttled)
}

// TableDetail is one line under a table's statement.
type TableDetail struct {
	Text string
	// Note marks an explanation rather than a figure. Surfaces set notes
	// apart from figures, with the info glyph and a muted style.
	Note bool
	// DocsURL is the page a note's surface may link for more.
	DocsURL string
}

// NoteText is a note's text after the info glyph.
func (d TableDetail) NoteText() string {
	return glyph.Info + " " + d.Text
}

// ReadInFlightTable reads a table the engine is working on: copying or
// building, verifying the copy, or applying the changes that accumulated
// while it ran. A status outside those phases reads as running.
func ReadInFlightTable(f TableFacts) TableReading {
	// An engine's raw phase name reads as the phase it belongs to.
	status := state.NormalizeTaskStatus(f.Status)
	var r TableReading
	switch {
	case state.IsState(status, state.Task.CatchingUp):
		// Row copy is done; the engine is applying the changes that
		// accumulated from live traffic during the copy. On a busy source
		// this can run for a long time, so the label names the phase rather
		// than leaving a full bar that looks finished.
		r = TableReading{Bar: ui.ProgressBarRowCopy(100), Label: "⏩ Catching up on accumulated changes...", Details: rowsCopiedDetail(f)}
	case state.IsState(status, state.Task.PostChecksum):
		// The verification passed and the engine is applying the changes
		// that accumulated while it ran, named apart from the earlier
		// catch-up so the line does not rewind to an earlier phase.
		r = TableReading{Bar: ui.ProgressBarRowCopy(100), Label: "⏩ Data verified, applying final changes...", Details: rowsCopiedDetail(f)}
	case state.IsState(status, state.Task.Checksumming):
		r = readChecksum(f)
	default:
		r = readRunning(f)
	}
	r.Throttled = f.Throttled
	if note, ok := ThrottleNote(f); ok {
		r.Details = append(r.Details, note)
	}
	return r
}

// readChecksum reads a table whose copy the engine is verifying against the
// source. On a large table this can run for hours, so it shows how far the
// verification has gone once the engine reports a total.
func readChecksum(f TableFacts) TableReading {
	if f.ChecksumRowsTotal <= 0 {
		return TableReading{Bar: ui.ProgressBarRowCopy(100), Label: "🔍 Checksumming to verify data..."}
	}
	pct := ui.ChecksumDisplayPercent(f.ChecksumRowsChecked, f.ChecksumRowsTotal)
	return TableReading{
		Bar:   ui.ProgressBarRowCopy(pct),
		Label: fmt.Sprintf("🔍 Checksumming to verify data (%s%s)", ui.FormatRowCopyPercent(pct, f.ChecksumRowsChecked, f.ChecksumRowsTotal), ShardCoverageSuffix(f)),
		Details: []TableDetail{{Text: fmt.Sprintf("Rows verified: %s / %s",
			ui.FormatNumber(ui.ClampRows(f.ChecksumRowsChecked, f.ChecksumRowsTotal)), ui.FormatNumber(f.ChecksumRowsTotal))}},
	}
}

// readRunning reads a table the engine is copying, building, or applying.
func readRunning(f TableFacts) TableReading {
	switch {
	case f.RowsTotal > 0 && ui.EstimateExceeded(f.RowsCopied, f.RowsTotal):
		// The copy has passed the engine's estimate, so a percentage would
		// read as done while rows are still moving. While some shards have
		// not reported, the label names the ones the copy covers, so it does
		// not read as the whole table wrapping up.
		return TableReading{Bar: ui.ProgressBarActivity(), Label: ActivityLabel + ShardCoverageSuffix(f), Activity: true, Details: []TableDetail{
			{Text: fmt.Sprintf("Rows copied: %s so far", ui.FormatNumber(f.RowsCopied))},
			{Text: ui.EstimateExceededTooltip, Note: true},
		}}
	case ui.CopyFinishedShortOfEstimate(f.PercentComplete, f.RowsCopied, f.RowsTotal):
		// The engine reports the copy done, but it inserted fewer rows than
		// the estimated total, as when the estimate ran high or the copy
		// skipped rows. It can insert none at all, when the source emptied
		// after the estimate or the binlog applier moved every row first, so
		// this comes before the starting-copy case. A percent of the estimate
		// would sit beside a full bar, so the table reads as fully copied,
		// with the rows it copied, as the post-copy phases do.
		return TableReading{Bar: ui.ProgressBarRowCopy(100), Label: ui.FormatRowCopyPercent(100, f.RowsTotal, f.RowsTotal) + ShardCoverageSuffix(f), Details: rowsCopiedDetail(f)}
	case f.RowsTotal > 0 && f.RowsCopied == 0:
		// The row total is known but no rows have moved yet, which can take a
		// while on a large table. A 0% bar reads as stuck, and a percent the
		// engine reports before its first rows would sit above a rows line
		// saying none have moved, so the label says the copy is starting and
		// the rows line gives the total.
		return TableReading{Label: "⏳ Starting copy...", Details: []TableDetail{{Text: RowsLine(f)}}}
	case f.RowsTotal > 0:
		return TableReading{
			Bar:     ui.ProgressBarRowCopy(ui.RowCopyDisplayPercent(f.PercentComplete, f.RowsCopied)),
			Label:   ui.FormatRowCopyPercent(f.PercentComplete, f.RowsCopied, f.RowsTotal) + ShardCoverageSuffix(f),
			Details: []TableDetail{{Text: RowsLine(f)}},
		}
	case f.BuildWork.IsConcurrentIndexBuild():
		// The engine's whole-build percent draws the bar. The current phase's
		// counters sit under the statement and carry no percent of their own,
		// so the table never shows two. The build reads every row, so the
		// table's planned rows and size stay under it, as they read while the
		// build was queued: the phase counters say how far it is, and the size
		// says how long the rest will take.
		r := TableReading{Label: ui.IndexBuildProgress(f.PercentComplete, f.BuildWork)}
		if line := ui.FormatBuildWork(f.BuildWork); line != "" {
			r.Details = append(r.Details, TableDetail{Text: line})
		}
		if line := PlannedRowsText(f.EstimatedRows, f.EstimatedBytes, ""); line != "" {
			r.Details = append(r.Details, TableDetail{Text: line})
		}
		return r
	default:
		// The change reports no progress to measure: a CREATE or DROP, an
		// instant ALTER, or a statement whose engine reports none. No bar is
		// drawn, since a bar would claim a measure the engine never gave.
		return TableReading{Label: applyingLabel(f)}
	}
}

// applyingLabel names a change that reports no progress by what it does.
func applyingLabel(f TableFacts) string {
	op := ddl.OpToStatementType(f.ChangeType)
	switch {
	case f.IsInstant && op == ddl.StatementAlterTable:
		return "Applying instantly..."
	case op == ddl.StatementCreateTable || op == ddl.StatementDropTable:
		return "Applying..."
	default:
		return "Running..."
	}
}

// rowsCopiedDetail reports the rows a finished copy moved, once it moved any.
func rowsCopiedDetail(f TableFacts) []TableDetail {
	if line := FinishedCopyRowsText(f.RowsCopied); line != "" {
		return []TableDetail{{Text: line}}
	}
	return nil
}

// FinishedCopyRowsText is the rows line of a copy the engine reports finished
// short of its estimate: the rows it copied, since a line against the
// estimate would contradict the full bar beside it. It is empty when the copy
// inserted none.
func FinishedCopyRowsText(rowsCopied int64) string {
	if rowsCopied <= 0 {
		return ""
	}
	return "Rows copied: " + ui.FormatNumber(rowsCopied)
}

// RowsLine is a copy's rows line: rows copied of the total, the shards the
// counts cover when only some have reported, the table's planned size, and
// the time left. A total from the plan is approximate, and the time left is a
// floor while some shards or targets have not reported, since they can only
// add to it.
func RowsLine(f TableFacts) string {
	total := ui.FormatNumber(f.RowsTotal)
	if f.RowsTotalPlanned {
		total = ui.FormatApproxRows(f.RowsTotal)
	}
	coverage, size := "", ui.FormatTableSizeClause(f.EstimatedBytes)
	if f.partialShards() {
		coverage = fmt.Sprintf(" across %d of %d shards", f.ShardsReporting, f.ShardsTotal)
		size = ui.FormatShardedTableSizeClause(f.EstimatedBytes, f.PlannedShards)
	}
	text := fmt.Sprintf("Rows: %s / %s%s%s", ui.FormatNumber(ui.ClampRows(f.RowsCopied, f.RowsTotal)), total, coverage, size)
	if f.ETASeconds > 0 {
		eta := ui.FormatETA(f.ETASeconds)
		if f.partialShards() || f.RowsTotalPlanned {
			eta = "≥ " + eta
		}
		text += " · ETA: " + eta
	}
	return text
}

// ShardCoverageSuffix names the shards a table's figures cover when only some
// have reported.
func ShardCoverageSuffix(f TableFacts) string {
	if !f.partialShards() {
		return ""
	}
	return fmt.Sprintf(" (%d of %d shards)", f.ShardsReporting, f.ShardsTotal)
}

// ThrottleNote explains a throttled label with the engine's reason. The raw
// reason names the engine signal; the tip says what the pause protects. A
// reason whose signal has no tip reads alone, so a new engine signal shows as
// raw text rather than a wrong explanation. A throttle with no reason has no
// note, and the label's annotation stands alone.
func ThrottleNote(f TableFacts) (TableDetail, bool) {
	if !f.Throttled || f.ThrottleReason == "" {
		return TableDetail{}, false
	}
	if tip := ui.ThrottleTip(f.ThrottleReason); tip != "" {
		return TableDetail{Text: "Throttled: " + f.ThrottleReason + " · " + tip, Note: true, DocsURL: ui.MySQLThrottleDocURL}, true
	}
	return TableDetail{Text: "Throttled: " + f.ThrottleReason, Note: true}, true
}
