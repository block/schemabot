package presentation

import (
	"cmp"
	"fmt"
	"slices"
	"sort"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

// Part is one member's progress on a table: a shard of a keyspace, or a target
// of a rollout. The CLI and the PR comment list a table's parts the same way,
// one per line with its state in words, so a rollout's targets read the way a
// keyspace's shards do.
type Part struct {
	Name            string
	Status          string
	PercentComplete int
	RowsCopied      int64
	RowsTotal       int64
}

// PendingQueued and PendingNotStarted name a part still to run: queued while
// its rollout can still run it, not started once the rollout has settled and
// never will.
const (
	PendingQueued     = "queued"
	PendingNotStarted = "not started"
)

// PendingWord names the parts still to run of a rollout that has or has not
// settled.
func PendingWord(settled bool) string {
	if settled {
		return PendingNotStarted
	}
	return PendingQueued
}

// partListInlineLimit is the most parts a listing names one per line. Past it,
// the listing names only the parts that need attention or set the pace, and
// the heading's counts cover the rest.
const partListInlineLimit = 8

// The caps on how many parts a wide listing names. Enough failed parts are
// named that a handful of failures are each visible, few enough that a change
// that failed everywhere does not print a line per part. The slowest few
// copying parts are named because they set the ETA.
const (
	maxFailedParts  = 5
	maxCopyingParts = 3
)

// PartListLine is one line of a table's part listing: the part at index Part,
// or, when Summary is set, a count of parts the listing does not name.
type PartListLine struct {
	Part    int
	Summary string
}

// ListParts is the lines a table's part listing shows, given its n parts, in
// the order the heading counts them (PartCounts.Phrases): failed parts first,
// then copying parts furthest behind first, then the other phases, then queued
// parts in order, so the next to run leads them, and complete parts last. Up
// to the inline limit every part is named. Past it, only the failed parts and
// the slowest copying parts are named, each followed by a count of the ones of
// its kind left unnamed; the heading already counts every other state, so the
// listing does not repeat the rest.
func ListParts(n int, part func(int) Part, noun Noun) []PartListLine {
	if n <= partListInlineLimit {
		return listEveryPart(n, part)
	}
	var lines []PartListLine
	failed := 0
	for i := range n {
		if state.NormalizeTaskStatus(part(i).Status) != state.Task.Failed {
			continue
		}
		if failed < maxFailedParts {
			lines = append(lines, PartListLine{Part: i})
		}
		failed++
	}
	if more := failed - maxFailedParts; more > 0 {
		lines = append(lines, PartListLine{Part: -1, Summary: fmt.Sprintf("%d more failed %s", more, noun.Plural)})
	}
	copying := copyingFurthestBehindFirst(n, part)
	for _, i := range copying[:min(len(copying), maxCopyingParts)] {
		lines = append(lines, PartListLine{Part: i})
	}
	if more := len(copying) - maxCopyingParts; more > 0 {
		lines = append(lines, PartListLine{Part: -1, Summary: fmt.Sprintf("%d more copying %s", more, noun.Plural)})
	}
	return lines
}

// listEveryPart names each of n parts in the listing's order: failed, copying
// furthest behind first, the other phases, queued, then complete.
func listEveryPart(n int, part func(int) Part) []PartListLine {
	var failed, phases, queued, complete []int
	for i := range n {
		switch status := state.NormalizeTaskStatus(part(i).Status); {
		case status == state.Task.Failed:
			failed = append(failed, i)
		case status == state.Task.Pending:
			queued = append(queued, i)
		case status == state.Task.Completed:
			complete = append(complete, i)
		case status != state.Task.Running:
			phases = append(phases, i)
		}
	}
	// The other phases follow the order the heading counts them in.
	sort.SliceStable(phases, func(a, b int) bool {
		return comparePartStatuses(state.NormalizeTaskStatus(part(phases[a]).Status), state.NormalizeTaskStatus(part(phases[b]).Status)) < 0
	})
	lines := make([]PartListLine, 0, n)
	for _, group := range [][]int{failed, copyingFurthestBehindFirst(n, part), phases, queued, complete} {
		for _, i := range group {
			lines = append(lines, PartListLine{Part: i})
		}
	}
	return lines
}

// copyingFurthestBehindFirst is the copying parts among n, the one furthest
// behind first, since that is the one a reader watches. A part that has not
// reported progress yet is not known to be behind, so the parts that have
// reported come first and the rest follow in order: a wide listing names the
// parts that set the pace rather than parts with nothing to show.
func copyingFurthestBehindFirst(n int, part func(int) Part) []int {
	var copying []int
	for i := range n {
		if state.NormalizeTaskStatus(part(i).Status) == state.Task.Running {
			copying = append(copying, i)
		}
	}
	sort.SliceStable(copying, func(a, b int) bool {
		pa, pb := part(copying[a]), part(copying[b])
		if ra, rb := partReported(pa), partReported(pb); ra != rb {
			return ra
		}
		return ui.RowCopyFraction(pa.PercentComplete, pa.RowsCopied, pa.RowsTotal) < ui.RowCopyFraction(pb.PercentComplete, pb.RowsCopied, pb.RowsTotal)
	})
	return copying
}

// partReported reports whether a copying part has reported any progress yet.
func partReported(p Part) bool {
	return p.PercentComplete != 0 || p.RowsCopied != 0
}

// PartGlyph is the mark a part's line leads with: ✓ complete, ◉ copying,
// ● waiting for or cutting over, ✗ failed, and ○ for every other state.
func PartGlyph(status string) string {
	switch state.NormalizeTaskStatus(status) {
	case state.Task.Completed:
		return "✓"
	case state.Task.Running:
		return "◉"
	case state.Task.WaitingForCutover, state.Task.CuttingOver:
		return "●"
	case state.Task.Failed:
		return "✗"
	default:
		return "○"
	}
}

// PartDetail is a part's state in words, for the text after its name: the
// rows a complete part copied, how far a copying part is, pendingWord for a
// part still to run, how far a stopped or cancelled part got, or its phase.
// Progress reads the way a table's own does, the percent and then its rows:
// "62.00% · 620 / 1,000 rows".
func PartDetail(p Part, pendingWord string) string {
	status := state.NormalizeTaskStatus(p.Status)
	switch status {
	case state.Task.Completed:
		if p.RowsTotal > 0 {
			return ui.FormatNumber(p.RowsTotal) + " rows"
		}
		return "complete"
	case state.Task.Running:
		// A part that has not reported progress yet reads as copying rather
		// than a misleading 0%.
		if !partReported(p) {
			return "copying"
		}
		return partProgress(p)
	case state.Task.Pending:
		return pendingWord
	case state.Task.Stopped, state.Task.Cancelled:
		// A part that had copied rows says how far it got before it halted.
		if p.RowsCopied > 0 && p.RowsTotal > 0 {
			return status + " at " + partProgress(p)
		}
		return status
	default:
		return PartStatusLabel(status)
	}
}

// partProgress is how far a part's copy is: its percent, then its rows once
// it knows its total.
func partProgress(p Part) string {
	pct := ui.FormatRowCopyPercent(p.PercentComplete, p.RowsCopied, p.RowsTotal)
	if p.RowsTotal <= 0 {
		return pct
	}
	return fmt.Sprintf("%s · %s / %s rows", pct, ui.FormatNumber(ui.ClampRows(p.RowsCopied, p.RowsTotal)), ui.FormatNumber(p.RowsTotal))
}

// PartCounts is a table's parts counted by state.
type PartCounts struct {
	Total             int
	Complete          int
	Running           int
	WaitingForCutover int
	CuttingOver       int
	Queued            int
	Failed            int
	Cancelled         int
	// PendingLabel is the word Phrases counts Queued parts by, PendingQueued
	// when empty (PendingWord).
	PendingLabel string
	// Other counts every status the fields above do not name, keyed by
	// status, so a part in any phase stays in the summary.
	Other map[string]int
}

// CountParts counts n parts by the status each reports.
func CountParts(n int, status func(int) string) PartCounts {
	c := PartCounts{Total: n}
	for i := range n {
		switch s := state.NormalizeTaskStatus(status(i)); s {
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
			c.Other[s]++
		}
	}
	return c
}

// Phrases is the counts in words, in the order a listing names its parts:
// "2 failed", "40 copying", "3 waiting for cutover", "60 complete".
func (c PartCounts) Phrases() []string {
	var parts []string
	if c.Failed > 0 {
		parts = append(parts, fmt.Sprintf("%d failed", c.Failed))
	}
	if c.Running > 0 {
		parts = append(parts, fmt.Sprintf("%d copying", c.Running))
	}
	if c.WaitingForCutover > 0 {
		parts = append(parts, fmt.Sprintf("%d waiting for cutover", c.WaitingForCutover))
	}
	if c.CuttingOver > 0 {
		parts = append(parts, fmt.Sprintf("%d cutting over", c.CuttingOver))
	}
	for _, status := range orderedPartStatuses(c.Other) {
		parts = append(parts, fmt.Sprintf("%d %s", c.Other[status], PartStatusLabel(status)))
	}
	if c.Cancelled > 0 {
		parts = append(parts, fmt.Sprintf("%d cancelled", c.Cancelled))
	}
	if c.Queued > 0 {
		label := c.PendingLabel
		if label == "" {
			label = PendingQueued
		}
		parts = append(parts, fmt.Sprintf("%d %s", c.Queued, label))
	}
	if c.Complete > 0 {
		parts = append(parts, fmt.Sprintf("%d complete", c.Complete))
	}
	if len(parts) == 0 {
		return []string{"none"}
	}
	return parts
}

// otherPartStatusOrder is the order the counts list the statuses PartCounts
// has no field of its own for. A status outside it still counts, after these
// and before cancelled.
var otherPartStatusOrder = []string{
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
}

// comparePartStatuses orders two statuses the way Phrases counts them, so a
// listing's lines follow its heading: otherPartStatusOrder, then any status
// outside it alphabetically, then cancelled, which PartCounts counts on its
// own after the rest. Both statuses are normalized.
func comparePartStatuses(a, b string) int {
	if c := cmp.Compare(partStatusRank(a), partStatusRank(b)); c != 0 {
		return c
	}
	return cmp.Compare(a, b)
}

// partStatusRank is where a status falls in the heading's order: its index in
// otherPartStatusOrder, one past it for any status outside it, and last for
// cancelled.
func partStatusRank(status string) int {
	if status == state.Task.Cancelled {
		return len(otherPartStatusOrder) + 1
	}
	if i := slices.Index(otherPartStatusOrder, status); i >= 0 {
		return i
	}
	return len(otherPartStatusOrder)
}

// orderedPartStatuses is the statuses counted in counts, in
// otherPartStatusOrder and then any others alphabetically.
func orderedPartStatuses(counts map[string]int) []string {
	ordered := make([]string, 0, len(counts))
	for _, status := range otherPartStatusOrder {
		if counts[status] > 0 {
			ordered = append(ordered, status)
		}
	}
	var unlisted []string
	for status, n := range counts {
		if n > 0 && !slices.Contains(otherPartStatusOrder, status) {
			unlisted = append(unlisted, status)
		}
	}
	slices.Sort(unlisted)
	return append(ordered, unlisted...)
}

// PartStatusLabel names a part's status in its line and in the counts.
func PartStatusLabel(status string) string {
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
