package templates

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

// Shard indentation constants (derived from indentContent in progress.go).
var (
	indentShardHeader = indentContent + "• "   // Shards: header with bullet
	indentShardLine   = indentContent + "    " // individual shard lines
	indentShardMore   = indentContent + "    " // "... N more" lines
)

// FormatShardProgress returns per-shard progress for a Vitess table as a string.
func FormatShardProgress(shards []ShardProgress) string {
	return formatPartProgress(shards, presentation.ShardNoun, presentation.PendingQueued)
}

// formatTableParts renders a table's per-part progress: its shards, or its
// targets when the table stands for one change across a rollout's targets.
// Its targets are listed only while the change is in flight on one of them or
// one of them stopped, as the PR comment lists them (listsTargets): otherwise
// the table's line counts them.
func formatTableParts(t TableProgress) string {
	if t.AcrossTargets {
		if !listsTargets(t) {
			return ""
		}
		return formatPartProgress(t.Shards, presentation.TargetNoun, pendingTargetsWord(t))
	}
	return formatPartProgress(t.Shards, presentation.ShardNoun, presentation.PendingQueued)
}

// keepsShardedBar reports whether a sharded table past its row copy still
// leads with its bar across its shards (presentation.ShardsKeepTableBar), once
// its shards have reported rows, as the PR comment's does.
func keepsShardedBar(t TableProgress) bool {
	if t.AcrossTargets || t.RowsTotal <= 0 {
		return false
	}
	statuses := make([]string, len(t.Shards))
	for i, s := range t.Shards {
		statuses[i] = s.Status
	}
	return presentation.ShardsKeepTableBar(t.Status, statuses)
}

// formatShardedPhaseProgress renders a sharded table that keeps its bar
// (keepsShardedBar): the bar across every shard the table spans
// (presentation.PercentAcrossTargets), counting a shard that has not reported
// as nothing, then the rows line and each shard's line.
func formatShardedPhaseProgress(t TableProgress) string {
	parts := make([]presentation.Part, len(t.Shards))
	for i, s := range t.Shards {
		parts[i] = shardPart(s)
	}
	pct := presentation.PercentAcrossTargets(parts, shardTotal(t)-len(t.Shards))
	var b strings.Builder
	writeTableLine(&b, t, "%s %d%%%s", ui.ProgressBarRowCopy(pct), pct, throttledSuffix(t))
	if t.DDL != "" {
		b.WriteString(formatTableDDL(t))
	}
	writeStructuredRowsAndETA(&b, t)
	writeThrottleTooltip(&b, t)
	b.WriteString("\n")
	b.WriteString(formatTableParts(t))
	return b.String()
}

// listsTargets reports whether a table across a rollout's targets lists them
// one per line under it (presentation.ListsTargets), decided by where each
// target stands rather than by the table's rolled-up status.
func listsTargets(t TableProgress) bool {
	statuses := make([]string, len(t.Shards))
	for i, s := range t.Shards {
		statuses[i] = s.Status
	}
	return presentation.ListsTargets(statuses)
}

// formatPartProgress renders the parts one table's change runs across — the
// shards of a keyspace or the targets of a rollout — named by noun: a heading
// counting them by state, then the lines presentation.ListParts picks, the
// listing the PR comment shows too. pendingWord names the parts still
// pending (presentation.PendingWord).
func formatPartProgress(shards []ShardProgress, noun presentation.Noun, pendingWord string) string {
	if len(shards) == 0 {
		return ""
	}
	var b strings.Builder
	c := presentation.CountParts(len(shards), func(i int) string { return shards[i].Status })
	c.PendingLabel = pendingWord
	fmt.Fprintf(&b, indentShardHeader+"%s%s: %d (%s)%s\n", ANSIDim, ui.CapitalizeFirst(noun.Plural), len(shards), strings.Join(c.Phrases(), ", "), ANSIReset)
	for _, line := range presentation.ListParts(len(shards), func(i int) presentation.Part { return shardPart(shards[i]) }, noun) {
		if line.Summary != "" {
			fmt.Fprintf(&b, indentShardMore+"%s... %s%s\n", ANSIDim, line.Summary, ANSIReset)
			continue
		}
		b.WriteString(formatShardLine(shards[line.Part], pendingWord))
	}
	return b.String()
}

// shardPart is a shard's or target's progress as the shared part listing
// reads it.
func shardPart(s ShardProgress) presentation.Part {
	return presentation.Part{
		Name: s.Shard, Status: s.Status, PercentComplete: s.PercentComplete, RowsCopied: s.RowsCopied, RowsTotal: s.RowsTotal,
		ChecksumRowsChecked: s.ChecksumRowsChecked, ChecksumRowsTotal: s.ChecksumRowsTotal,
		PlannedRows: s.PlannedRows,
	}
}

// formatShardLine renders one part's line: its mark and name in the color of
// its state, then its state in words, with a copying part's ETA. Every phase
// an engine works through is colored as working; only a part that is not
// working on the change is dimmed. pendingWord names a part still pending.
func formatShardLine(s ShardProgress, pendingWord string) string {
	detail := presentation.PartDetail(shardPart(s), pendingWord)
	glyph := presentation.PartGlyph(s.Status)
	switch s.Status {
	case state.Task.Completed:
		return fmt.Sprintf(indentShardLine+"%s%s %s%s: %s\n", ANSIGreen, glyph, s.Shard, ANSIReset, detail)
	case state.Task.Running:
		if s.ETASeconds > 0 {
			detail += " · ETA: " + FormatDurationSeconds(s.ETASeconds)
		}
		return fmt.Sprintf(indentShardLine+"%s%s %s%s: %s\n", ANSICyan, glyph, s.Shard, ANSIReset, detail)
	case state.Task.WaitingForCutover, state.Task.CuttingOver:
		return fmt.Sprintf(indentShardLine+"%s%s %s%s: %s\n", ANSIYellow, glyph, s.Shard, ANSIReset, detail)
	case state.Task.Failed:
		return fmt.Sprintf(indentShardLine+"%s%s %s%s: %s\n", ANSIRed, glyph, s.Shard, ANSIReset, detail)
	}
	switch {
	case state.IsInFlightTaskState(s.Status):
		// Every other phase an engine works through reads as working, like a copy.
		return fmt.Sprintf(indentShardLine+"%s%s %s%s: %s\n", ANSICyan, glyph, s.Shard, ANSIReset, detail)
	default:
		return fmt.Sprintf(indentShardLine+"%s%s %s: %s%s\n", ANSIDim, glyph, s.Shard, detail, ANSIReset)
	}
}

// FormatDurationSeconds formats seconds into a human-readable duration.
func FormatDurationSeconds(seconds int64) string {
	if seconds <= 0 {
		return "< 1s"
	}
	if seconds < 60 {
		return fmt.Sprintf("%ds", seconds)
	}
	if seconds < 3600 {
		return fmt.Sprintf("%dm %ds", seconds/60, seconds%60)
	}
	return fmt.Sprintf("%dh %dm", seconds/3600, (seconds%3600)/60)
}
