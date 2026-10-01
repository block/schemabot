package templates

import (
	"fmt"
	"slices"
	"sort"
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

// maxShardDetail is the maximum number of individual shard lines to render.
// Beyond this, only non-terminal (copying/queued/failed) shards are shown
// with a collapsed summary for complete shards.
const maxShardDetail = 8

// maxFailedShown is the most failed parts a wide table names one per line:
// enough that a handful of failures are each named, few enough that a change
// that failed on every part does not print a line per part.
const maxFailedShown = 10

// FormatShardProgress returns per-shard progress for a Vitess table as a string.
// For large shard counts (>maxShardDetail), only shows non-terminal shards
// plus a collapsed count for complete/queued shards.
func FormatShardProgress(shards []ShardProgress) string {
	return formatPartProgress(shards, presentation.ShardNoun)
}

// formatTableParts renders a table's per-part progress: its shards, or its
// targets when the table stands for one change across a rollout's targets.
func formatTableParts(t TableProgress) string {
	if t.AcrossTargets {
		return formatPartProgress(t.Shards, presentation.TargetNoun)
	}
	return formatPartProgress(t.Shards, presentation.ShardNoun)
}

// formatPartProgress renders the parts one table's change runs across — the
// shards of a keyspace or the targets of a rollout — named by noun.
func formatPartProgress(shards []ShardProgress, noun presentation.Noun) string {
	if len(shards) == 0 {
		return ""
	}

	var b strings.Builder

	c := CountShardsByStatus(shards)
	parts := FormatShardSummaryParts(c, false)
	fmt.Fprintf(&b, indentShardHeader+"%s%s: %d (%s)%s\n", ANSIDim, ui.CapitalizeFirst(noun.Plural), len(shards), strings.Join(parts, ", "), ANSIReset)

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

	// Failed shards come first (they need attention), up to maxFailedShown
	// with the rest counted, so a change that failed everywhere still fits on
	// one screen; the header carries the total. Every other status that is
	// neither copying, complete nor queued is sampled a few lines per status
	// with the rest counted, so a wall of identical "waiting for cutover"
	// lines stays short and no part in any phase goes unmentioned.
	const maxNonCopyingShown = 3
	failed := 0
	for _, s := range shards {
		if s.Status != state.Task.Failed {
			continue
		}
		if failed < maxFailedShown {
			b.WriteString(formatShardLine(s))
		}
		failed++
	}
	if more := failed - maxFailedShown; more > 0 {
		fmt.Fprintf(&b, indentShardMore+"%s... %d more failed %s%s\n", ANSIDim, more, noun.Plural, ANSIReset)
	}
	sampled := make(map[string]int)
	for _, s := range shards {
		if !isSampledPartStatus(s.Status) {
			continue
		}
		if sampled[s.Status] < maxNonCopyingShown {
			b.WriteString(formatShardLine(s))
		}
		sampled[s.Status]++
	}
	for _, status := range orderedPartStatuses(sampled) {
		if more := sampled[status] - maxNonCopyingShown; more > 0 {
			fmt.Fprintf(&b, indentShardMore+"%s... %d more %s%s\n",
				ANSIDim, more, partStatusLabel(status), ANSIReset)
		}
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
		fmt.Fprintf(&b, indentShardMore+"%s... %d more copying %s%s\n",
			ANSIDim, len(copying)-maxCopyingShown, noun.Plural, ANSIReset)
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
		detail := fmt.Sprintf("%s (%s/%s rows)", ui.FormatRowCopyPercent(s.PercentComplete, s.RowsCopied, s.RowsTotal),
			ui.FormatNumber(ui.ClampRows(s.RowsCopied, s.RowsTotal)), ui.FormatNumber(s.RowsTotal))
		if s.ETASeconds > 0 {
			detail += fmt.Sprintf(" ETA %s", FormatDurationSeconds(s.ETASeconds))
		}
		return fmt.Sprintf(indentShardLine+"%s◉ %s%s: %s\n", ANSICyan, s.Shard, ANSIReset, detail)
	case state.Task.WaitingForCutover:
		return fmt.Sprintf(indentShardLine+"%s● %s%s: waiting for cutover\n", ANSIYellow, s.Shard, ANSIReset)
	case state.Task.CuttingOver:
		return fmt.Sprintf(indentShardLine+"%s● %s%s: cutting over\n", ANSIYellow, s.Shard, ANSIReset)
	case state.Task.Pending:
		return fmt.Sprintf(indentShardLine+"%s○ %s: queued%s\n", ANSIDim, s.Shard, ANSIReset)
	case state.Task.Failed:
		return fmt.Sprintf(indentShardLine+"%s✗ %s%s: failed\n", ANSIRed, s.Shard, ANSIReset)
	default:
		return fmt.Sprintf(indentShardLine+"%s○ %s: %s%s\n", ANSIDim, s.Shard, partStatusLabel(s.Status), ANSIReset)
	}
}

// sampledPartStatusOrder is the order the summary and the sampled lines list
// the statuses ShardCounts has no field of its own for, alongside waiting
// for and cutting over. A status outside it still counts, after these.
var sampledPartStatusOrder = []string{
	state.Task.WaitingForCutover,
	state.Task.CuttingOver,
	state.Task.CatchingUp,
	state.Task.Checksumming,
	state.Task.PostChecksum,
	state.Task.WaitingForDeploy,
	state.Task.Recovering,
	state.Task.FailedRetryable,
	state.Task.Stopped,
	state.Task.Reverting,
	state.Task.RevertWindow,
	state.Task.Reverted,
	state.Task.Cancelled,
}

// isSampledPartStatus reports whether a part in a wide table is shown by
// sampling its status: every status except copying (sampled by how far
// behind), failed (always shown), and complete or queued (counted).
func isSampledPartStatus(status string) bool {
	switch status {
	case state.Task.Running, state.Task.Failed, state.Task.Completed, state.Task.Pending:
		return false
	default:
		return true
	}
}

// orderedPartStatuses is the statuses counted in counts, in
// sampledPartStatusOrder and then any others alphabetically.
func orderedPartStatuses(counts map[string]int) []string {
	ordered := make([]string, 0, len(counts))
	for _, status := range sampledPartStatusOrder {
		if counts[status] > 0 {
			ordered = append(ordered, status)
		}
	}
	var unlisted []string
	for status, n := range counts {
		if n > 0 && !slices.Contains(sampledPartStatusOrder, status) {
			unlisted = append(unlisted, status)
		}
	}
	slices.Sort(unlisted)
	return append(ordered, unlisted...)
}

// partStatusLabel names a part's status in its line and in the summary.
func partStatusLabel(status string) string {
	switch status {
	case state.Task.WaitingForCutover:
		return "waiting for cutover"
	case state.Task.CuttingOver:
		return "cutting over"
	case state.Task.CatchingUp:
		return "catching up"
	case state.Task.PostChecksum:
		return "applying final changes"
	case state.Task.WaitingForDeploy:
		return "waiting for deploy"
	case state.Task.FailedRetryable:
		return "retrying"
	case state.Task.RevertWindow:
		return "revert window open"
	default:
		return status
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
		default:
			if c.Other == nil {
				c.Other = make(map[string]int)
			}
			c.Other[s.Status]++
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
		parts = append(parts, fmt.Sprintf("%d waiting for cutover", c.WaitingForCutover))
	}
	if c.CuttingOver > 0 {
		parts = append(parts, fmt.Sprintf("%d cutting over", c.CuttingOver))
	}
	if c.Running > 0 {
		parts = append(parts, fmt.Sprintf("%d copying", c.Running))
	}
	for _, status := range orderedPartStatuses(c.Other) {
		parts = append(parts, fmt.Sprintf("%d %s", c.Other[status], partStatusLabel(status)))
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
