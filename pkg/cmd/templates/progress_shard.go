package templates

import (
	"fmt"
	"sort"
	"strings"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

// Shard indentation constants (derived from indentContent in progress.go).
var (
	indentShardHeader = indentContent + "• "   // Shards: header with bullet
	indentShardLine   = indentContent + "    " // individual shard lines
	indentShardMore   = indentContent + "    " // "... N more" lines
)

// maxShardDetail is the maximum number of individual shard lines to render.
// Beyond this, only non-terminal (copying/queued/failed) shards are shown
// with a collapsed summary for complete shards.
const maxShardDetail = 8

// FormatShardProgress returns per-shard progress for a Vitess table as a string.
// For large shard counts (>maxShardDetail), only shows non-terminal shards
// plus a collapsed count for complete/queued shards.
func FormatShardProgress(shards []ShardProgress) string {
	if len(shards) == 0 {
		return ""
	}

	var b strings.Builder

	c := CountShardsByStatus(shards)
	parts := FormatShardSummaryParts(c, false)
	fmt.Fprintf(&b, indentShardHeader+"%sShards: %d (%s)%s\n", ANSIDim, len(shards), strings.Join(parts, ", "), ANSIReset)

	// For small shard counts, show all shards
	if len(shards) <= maxShardDetail {
		for _, s := range shards {
			b.WriteString(formatShardLine(s))
		}
		return b.String()
	}

	// For large shard counts: show failed first (always), then a sample
	// of copying shards, then collapse the rest into a summary.
	const maxCopyingShown = 5

	// Always show failed shards (they need attention). Limit other non-copying
	// shards to avoid a wall of identical "ready for cutover" lines.
	const maxNonCopyingShown = 3
	for _, s := range shards {
		if s.Status == state.Task.Failed {
			b.WriteString(formatShardLine(s))
		}
	}
	var waitingCount, cuttingCount int
	for _, s := range shards {
		switch s.Status {
		case state.Task.WaitingForCutover:
			if waitingCount < maxNonCopyingShown {
				b.WriteString(formatShardLine(s))
			}
			waitingCount++
		case state.Task.CuttingOver:
			if cuttingCount < maxNonCopyingShown {
				b.WriteString(formatShardLine(s))
			}
			cuttingCount++
		}
	}
	if waitingCount > maxNonCopyingShown {
		fmt.Fprintf(&b, indentShardMore+"%s... %d more ready for cutover%s\n",
			ANSIDim, waitingCount-maxNonCopyingShown, ANSIReset)
	}
	if cuttingCount > maxNonCopyingShown {
		fmt.Fprintf(&b, indentShardMore+"%s... %d more cutting over%s\n",
			ANSIDim, cuttingCount-maxNonCopyingShown, ANSIReset)
	}

	// Collect copying shards, sorted by percent complete (lowest first)
	// so the most behind shards are always visible.
	var copying []ShardProgress
	for _, s := range shards {
		if s.Status == state.Task.Running {
			copying = append(copying, s)
		}
	}
	sort.Slice(copying, func(i, j int) bool {
		return copying[i].PercentComplete < copying[j].PercentComplete
	})
	for i, s := range copying {
		if i >= maxCopyingShown {
			break
		}
		b.WriteString(formatShardLine(s))
	}
	if len(copying) > maxCopyingShown {
		fmt.Fprintf(&b, indentShardMore+"%s... %d more copying shards%s\n",
			ANSIDim, len(copying)-maxCopyingShown, ANSIReset)
	}

	// Summarize remaining shards not individually shown
	if c.Complete > 0 || c.Queued > 0 {
		var remainParts []string
		if c.Complete > 0 {
			remainParts = append(remainParts, fmt.Sprintf("%d complete", c.Complete))
		}
		if c.Queued > 0 {
			remainParts = append(remainParts, fmt.Sprintf("%d queued", c.Queued))
		}
		fmt.Fprintf(&b, indentShardMore+"%s... %s%s\n", ANSIDim, strings.Join(remainParts, ", "), ANSIReset)
	}

	return b.String()
}

func formatShardLine(s ShardProgress) string {
	switch s.Status {
	case state.Task.Completed:
		return fmt.Sprintf(indentShardLine+"%s✓ %s%s: %s rows\n", ANSIGreen, s.Shard, ANSIReset, ui.FormatNumber(s.RowsTotal))
	case state.Task.Running:
		pct := s.PercentComplete
		if pct == 0 && s.RowsTotal > 0 {
			pct = int(s.RowsCopied * 100 / s.RowsTotal)
		}
		detail := fmt.Sprintf("%d%% (%s/%s rows)", pct, ui.FormatNumber(ui.ClampRows(s.RowsCopied, s.RowsTotal)), ui.FormatNumber(s.RowsTotal))
		if s.ETASeconds > 0 {
			detail += fmt.Sprintf(" ETA %s", FormatDurationSeconds(s.ETASeconds))
		}
		return fmt.Sprintf(indentShardLine+"%s◉ %s%s: %s\n", ANSICyan, s.Shard, ANSIReset, detail)
	case state.Task.WaitingForCutover:
		return fmt.Sprintf(indentShardLine+"%s● %s%s: ready for cutover\n", ANSIYellow, s.Shard, ANSIReset)
	case state.Task.CuttingOver:
		return fmt.Sprintf(indentShardLine+"%s● %s%s: cutting over\n", ANSIYellow, s.Shard, ANSIReset)
	case state.Task.Pending:
		return fmt.Sprintf(indentShardLine+"%s○ %s: queued%s\n", ANSIDim, s.Shard, ANSIReset)
	case state.Task.Failed:
		return fmt.Sprintf(indentShardLine+"%s✗ %s%s: failed\n", ANSIRed, s.Shard, ANSIReset)
	default:
		return fmt.Sprintf(indentShardLine+"%s○ %s: %s%s\n", ANSIDim, s.Shard, s.Status, ANSIReset)
	}
}

// CountShardsByStatus aggregates shard progress into status counts.
func CountShardsByStatus(shards []ShardProgress) ShardCounts {
	var c ShardCounts
	c.Total = len(shards)
	for _, s := range shards {
		switch s.Status {
		case state.Task.Completed:
			c.Complete++
		case state.Task.Running:
			c.Running++
		case state.Task.WaitingForCutover:
			c.WaitingForCutover++
		case state.Task.CuttingOver:
			c.CuttingOver++
		case state.Task.Pending:
			c.Queued++
		case state.Task.Failed:
			c.Failed++
		case state.Task.Cancelled:
			c.Cancelled++
		}
	}
	return c
}

// FormatShardSummaryParts formats shard counts into human-readable parts.
func FormatShardSummaryParts(c ShardCounts, compact bool) []string {
	var parts []string
	if c.Complete > 0 {
		parts = append(parts, fmt.Sprintf("%d complete", c.Complete))
	}
	if c.WaitingForCutover > 0 {
		parts = append(parts, fmt.Sprintf("%d ready for cutover", c.WaitingForCutover))
	}
	if c.CuttingOver > 0 {
		parts = append(parts, fmt.Sprintf("%d cutting over", c.CuttingOver))
	}
	if c.Running > 0 {
		parts = append(parts, fmt.Sprintf("%d copying", c.Running))
	}
	if c.Queued > 0 {
		parts = append(parts, fmt.Sprintf("%d queued", c.Queued))
	}
	if c.Failed > 0 {
		parts = append(parts, fmt.Sprintf("%d failed", c.Failed))
	}
	if c.Cancelled > 0 {
		parts = append(parts, fmt.Sprintf("%d cancelled", c.Cancelled))
	}
	if len(parts) == 0 {
		return []string{"none"}
	}
	return parts
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
