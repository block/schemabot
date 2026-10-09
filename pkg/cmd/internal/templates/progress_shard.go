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
	return formatPartProgress(shards, presentation.ShardNoun, "queued")
}

// formatTableParts renders a table's per-part progress: its shards, or its
// targets when the table stands for one change across a rollout's targets.
func formatTableParts(t TableProgress) string {
	if t.AcrossTargets {
		return formatPartProgress(t.Shards, presentation.TargetNoun, pendingTargetsWord(t))
	}
	return formatPartProgress(t.Shards, presentation.ShardNoun, "queued")
}

// formatPartProgress renders the parts one table's change runs across — the
// shards of a keyspace or the targets of a rollout — named by noun: a heading
// counting them by state, then the lines presentation.ListParts picks, the
// listing the PR comment shows too. pendingWord names the parts still
// pending: "queued", or "not started" once a settled rollout will not run them.
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
	return presentation.Part{Name: s.Shard, Status: s.Status, PercentComplete: s.PercentComplete, RowsCopied: s.RowsCopied, RowsTotal: s.RowsTotal}
}

// formatShardLine renders one part's line: its mark and name in the color of
// its state, then its state in words, with a copying part's ETA. pendingWord
// names a part still pending.
func formatShardLine(s ShardProgress, pendingWord string) string {
	detail := presentation.PartDetail(shardPart(s))
	if s.Status == state.Task.Pending {
		detail = pendingWord
	}
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
