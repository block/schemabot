package presentation

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
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
	// ChecksumRowsChecked and ChecksumRowsTotal are how far a checksumming
	// part's verify has got, once its engine reports a total.
	ChecksumRowsChecked int64
	ChecksumRowsTotal   int64
	// ETASeconds is the part's own estimate of the time its copy has left,
	// zero when its engine reports none.
	ETASeconds int64
	// PlannedRows is the plan's row estimate for a part whose copy is still
	// ahead (CopyStillAhead) and whose engine has reported no row total yet,
	// standing in for the rows it will copy. Nil once the part reports a
	// total, or when the plan had no estimate for it.
	PlannedRows *int64
	// RunningLabel is the word for the part while it works on its change,
	// RunningCopying when empty (RunningWord).
	RunningLabel string
}

// runningLabel is the word p is named by while it works on its change.
func (p Part) runningLabel() string {
	if p.RunningLabel == "" {
		return RunningCopying
	}
	return p.RunningLabel
}

// PendingQueued and PendingNotStarted name a part still to run: queued while
// its rollout can still run it, not started once the rollout has settled and
// never will.
const (
	PendingQueued     = "queued"
	PendingNotStarted = "not started"
)

// RunningCopying and RunningApplying name a part working on its change:
// copying while the change copies the table's rows, and applying while it runs
// a change that copies none, such as a CREATE TABLE, a DROP TABLE or an
// instant ALTER, where "copying" would describe work that is not happening.
const (
	RunningCopying  = "copying"
	RunningApplying = "applying"
)

// RunningWord names a part working on a change that does or does not copy
// the table's rows.
func RunningWord(copiesRows bool) string {
	if copiesRows {
		return RunningCopying
	}
	return RunningApplying
}

// ChangeCopiesRows reports whether a table change copies the table's rows,
// given its DDL action ("create", "alter" or "drop") and whether its engine
// runs it as instant DDL. A CREATE TABLE, a DROP TABLE and an instant ALTER
// copy none. An action it does not recognize reads as copying, the label a
// change that may copy has always carried.
func ChangeCopiesRows(ddlAction string, instant bool) bool {
	switch ddl.OpToStatementType(ddlAction) {
	case ddl.StatementCreateTable, ddl.StatementDropTable:
		return false
	case ddl.StatementAlterTable:
		return !instant
	}
	return true
}

// TableChange is what decides whether one table change copies rows: its DDL
// action ("create", "alter" or "drop") and whether its engine runs it as
// instant DDL.
type TableChange struct {
	DDLAction string
	Instant   bool
}

// ChangesCopyNoRows reports whether none of a shard operation's table
// changes copies rows (ChangeCopiesRows), so the shard reads as applying
// rather than running a table copy. An operation with no changes recorded yet
// reads as copying, since nothing says otherwise.
func ChangesCopyNoRows(changes []TableChange) bool {
	if len(changes) == 0 {
		return false
	}
	for _, c := range changes {
		if ChangeCopiesRows(c.DDLAction, c.Instant) {
			return false
		}
	}
	return true
}

// TasksCopyNoRows reports whether a shard operation's stored tasks are all
// table changes that copy no rows (ChangesCopyNoRows). The progress API and
// the PR comment both read a shard's tasks through it.
func TasksCopyNoRows(tasks []*storage.Task) bool {
	return ChangesCopyNoRows(TaskTableChanges(tasks))
}

// TaskTableChanges is the table change each of a shard operation's stored
// tasks runs.
func TaskTableChanges(tasks []*storage.Task) []TableChange {
	changes := make([]TableChange, 0, len(tasks))
	for _, t := range tasks {
		changes = append(changes, TableChange{DDLAction: t.DDLAction, Instant: t.IsInstant})
	}
	return changes
}

// ShardedTableCopiesNoRows reports whether a table rolled up across shards
// copies no rows, so it reads as applying rather than copying. The CLI and
// the PR comment both read a sharded table by this rule. shardChanges holds
// each shard's recorded changes to the table. The shards that have recorded
// any decide: the table copies no rows when at least one has and none of
// those changes copies rows. A shard that has recorded nothing yet does not
// decide. When no shard has recorded a change, the table reads from planned,
// its change in the stored plan, which carries no instant flag. With no
// planned change either, it reads as copying, since nothing says otherwise.
func ShardedTableCopiesNoRows(shardChanges [][]TableChange, planned *TableChange) bool {
	recorded := false
	for _, changes := range shardChanges {
		if len(changes) == 0 {
			continue
		}
		recorded = true
		if !ChangesCopyNoRows(changes) {
			return false
		}
	}
	if recorded {
		return true
	}
	if planned == nil {
		return false
	}
	return !ChangeCopiesRows(planned.DDLAction, planned.Instant)
}

// PendingWord names the parts still to run of a rollout that has or has not
// settled.
func PendingWord(settled bool) string {
	if settled {
		return PendingNotStarted
	}
	return PendingQueued
}

// UnreportedTargetStatus is where a rollout member that has reported no table
// progress stands on each table it runs, given its operation's state: its own
// halt or outcome once it has one, since that is all there is to say about it,
// and queued while it is still to run. A stopped member reports its tables
// once the apply resumes.
func UnreportedTargetStatus(memberState string) string {
	switch {
	case state.IsState(memberState, state.ApplyOperation.Stopped):
		return state.Task.Stopped
	case state.IsState(memberState, state.ApplyOperation.Failed):
		return state.Task.Failed
	case state.IsState(memberState, state.ApplyOperation.FailedRetryable):
		return state.Task.FailedRetryable
	case state.IsState(memberState, state.ApplyOperation.Cancelled):
		return state.Task.Cancelled
	case state.IsState(memberState, state.ApplyOperation.Reverted):
		return state.Task.Reverted
	case state.IsState(memberState, state.ApplyOperation.Completed):
		return state.Task.Completed
	default:
		return state.Task.Pending
	}
}

// EveryTargetStopped reports whether a table stopped on every one of its
// targets. A table that stopped there before any target began it reads as
// stopped once, with no target list, since each target would only repeat it;
// a table with a target in any other state keeps its list, so that state is
// never hidden.
func EveryTargetStopped(statuses []string) bool {
	return len(statuses) > 0 && !slices.ContainsFunc(statuses, func(status string) bool {
		return !state.IsState(status, state.Task.Stopped)
	})
}

// RolloutTableStatus is where a target stands on one table of a rollout in
// applyState, given the table's own task status. A table still pending on a
// target of a stopped apply is not waiting for its turn: nothing runs until
// the apply is started again, so it reads as stopped, the way a single
// deployment's stop records the tables it had not begun.
func RolloutTableStatus(applyState, taskStatus string) string {
	if state.IsState(applyState, state.Apply.Stopped) && state.IsState(taskStatus, state.Task.Pending) {
		return state.Task.Stopped
	}
	return taskStatus
}

// partListInlineLimit is the most parts a listing names one per line. Past it,
// the listing names only the parts that need attention or set the pace, and
// the heading's counts cover the rest.
const partListInlineLimit = 8

// The caps on how many parts a wide listing names. Enough failed parts are
// named that a handful of failures are each visible, few enough that a change
// that failed everywhere does not print a line per part. The slowest few
// copying parts are named because they set the ETA, and the first few parts
// working past the copy are named because they are as live as a copying one.
const (
	maxFailedParts  = 5
	maxCopyingParts = 4
	maxWorkingParts = 4
)

// ListsParts reports whether a table in status lists its parts one per line
// under it: only while the change is in flight on it (state.IsInFlightTaskState).
// A table that has not started, or has settled, says where it stands on its
// own line, so the PR comment and the CLI both leave the listing out then.
func ListsParts(status string) bool {
	return state.IsInFlightTaskState(state.NormalizeTaskStatus(status))
}

// ListsTargets reports whether a table across a rollout's targets, each in
// the status at the same index of statuses, lists them one per line under it:
// while the change is in flight on any of them (ListsParts), and while any of
// them is stopped, since a stopped target resumes from where it stopped and
// the listing is what says where that is. It reads the targets rather than the
// table's rolled-up status, because a failed target outranks a stopped or
// copying one in the rollup and would otherwise hide where they stand.
func ListsTargets(statuses []string) bool {
	return slices.ContainsFunc(statuses, func(status string) bool {
		return ListsParts(status) || state.NormalizeTaskStatus(status) == state.Task.Stopped
	})
}

// ShardsKeepTableBar reports whether a sharded table past its row copy still
// leads with its bar across its shards rather than its phase: it is still in
// flight, its shards are in different phases (OnePhaseSpeaksForParts), and
// there is more than one of them. The table's status is its most pressing
// shard's, so one shard cutting over would otherwise speak for shards still
// copying or verifying. A table copying rows, or resuming its copy, already
// leads with its copy bar, so it is left to it.
func ShardsKeepTableBar(tableStatus string, shardStatuses []string) bool {
	if len(shardStatuses) <= 1 || !state.IsInFlightTaskState(tableStatus) {
		return false
	}
	switch state.NormalizeTaskStatus(tableStatus) {
	case state.Task.Running, state.Task.Recovering:
		return false
	}
	return !OnePhaseSpeaksForParts(shardStatuses)
}

// RevertSpeaksForParts reports whether any part is reverting the change or
// has reverted it. A revert undoes the change rather than progressing it, so
// its phase speaks for the table instead of a bar that would count the rows
// the copy once moved as progress.
func RevertSpeaksForParts(statuses []string) bool {
	return slices.ContainsFunc(statuses, func(status string) bool {
		switch state.NormalizeTaskStatus(status) {
		case state.Task.Reverting, state.Task.Reverted:
			return true
		}
		return false
	})
}

// OnePhaseSpeaksForParts reports whether every part is in the same phase past
// the row copy (all checksumming, all waiting for cutover), so that phase
// speaks for the table as a whole. Parts in different phases leave no one
// phase that does, so the table reads its progress across them and the list
// under it says where each one stands; parts all copying read their progress
// too, since the copy is what a bar measures.
func OnePhaseSpeaksForParts(statuses []string) bool {
	if len(statuses) == 0 {
		return false
	}
	phase := state.NormalizeTaskStatus(statuses[0])
	if phase == state.Task.Running {
		return false
	}
	for _, status := range statuses[1:] {
		if state.NormalizeTaskStatus(status) != phase {
			return false
		}
	}
	return true
}

// TargetCoverage is the " · 40 complete, 4 copying, 19 queued, 1 failed,
// 1 retrying" suffix of a table's line across a rollout's targets, naming only
// the states some target is in, or "" when there are none. Queued targets are
// waiting on the apply's driver cap or on their turn in order; pendingWord
// names them, "not started" once the rollout has settled (PendingWord).
// runningWord names the targets working on the change (RunningWord).
func TargetCoverage(done, running, queued, failed, retrying int, runningWord, pendingWord string) string {
	var parts []string
	if done > 0 {
		parts = append(parts, fmt.Sprintf("%d complete", done))
	}
	if running > 0 {
		parts = append(parts, fmt.Sprintf("%d %s", running, runningWord))
	}
	if queued > 0 {
		parts = append(parts, fmt.Sprintf("%d %s", queued, pendingWord))
	}
	if failed > 0 {
		parts = append(parts, fmt.Sprintf("%d failed", failed))
	}
	if retrying > 0 {
		parts = append(parts, fmt.Sprintf("%d retrying", retrying))
	}
	if len(parts) == 0 {
		return ""
	}
	return " · " + strings.Join(parts, ", ")
}

// PartListLine is one line of a table's part listing: the part at index Part,
// or, when Summary is set, a count of parts the listing does not name.
type PartListLine struct {
	Part    int
	Summary string
}

// ListParts is the lines a table's part listing shows, given its n parts. Up
// to the inline limit every part is named, in the parts' own order, so each
// part keeps its line from one update to the next and a reader follows it by
// where it sits. Past it, only the failed parts, the slowest copying parts and
// the parts working past the copy are named, each followed by a count of the
// ones of its kind left unnamed; the heading already counts every other
// state, so the listing does not repeat the rest.
func ListParts(n int, part func(int) Part, noun Noun) []PartListLine {
	if n <= partListInlineLimit {
		lines := make([]PartListLine, n)
		for i := range n {
			lines[i] = PartListLine{Part: i}
		}
		return lines
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
		lines = append(lines, PartListLine{Part: -1, Summary: fmt.Sprintf("%d more failed %s", more, noun.Label(more))})
	}
	copying := copyingFurthestBehindFirst(n, part)
	for _, i := range copying[:min(len(copying), maxCopyingParts)] {
		lines = append(lines, PartListLine{Part: i})
	}
	if more := len(copying) - maxCopyingParts; more > 0 {
		lines = append(lines, PartListLine{Part: -1, Summary: fmt.Sprintf("%d more %s %s", more, part(copying[0]).runningLabel(), noun.Label(more))})
	}
	// A part working through any later phase is as live as a copying one, so
	// it is named too, in its own order, rather than hidden in the heading.
	working := workingPastCopy(n, part)
	for _, i := range working[:min(len(working), maxWorkingParts)] {
		lines = append(lines, PartListLine{Part: i})
	}
	if more := len(working) - maxWorkingParts; more > 0 {
		lines = append(lines, PartListLine{Part: -1, Summary: fmt.Sprintf("%d more %s past copying", more, noun.Label(more))})
	}
	return lines
}

// workingPastCopy is the parts among n an engine is working on past the row
// copy (checksumming, waiting for or cutting over, recovering, reverting), in
// order.
func workingPastCopy(n int, part func(int) Part) []int {
	var working []int
	for i := range n {
		status := state.NormalizeTaskStatus(part(i).Status)
		if status != state.Task.Running && state.IsInFlightTaskState(status) {
			working = append(working, i)
		}
	}
	return working
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

// PartGlyph is the mark a part's line leads with: ✓ complete, ● at the
// cutover (waiting for it, cutting over, or in its revert window), ◉ working
// through any other phase of the change, ✗ failed, and ○ for a part that is
// not working on it: queued, not started, or halted. A part in its revert
// window has the change live but not final, so it never reads as done.
func PartGlyph(status string) string {
	switch status := state.NormalizeTaskStatus(status); {
	case status == state.Task.Completed:
		return "✓"
	case status == state.Task.WaitingForCutover, status == state.Task.CuttingOver, status == state.Task.RevertWindow:
		return "●"
	case status == state.Task.Failed:
		return "✗"
	case state.IsInFlightTaskState(status):
		return "◉"
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
		// A part that has not reported progress yet reads as working on its
		// change rather than a misleading 0%.
		if !partReported(p) {
			return p.runningLabel()
		}
		return partProgress(p)
	case state.Task.Pending:
		// A part still to run keeps its queued word and says how much it
		// will copy, once the plan sized it.
		if p.PlannedRows != nil {
			return pendingWord + " · " + ui.FormatApproxRows(*p.PlannedRows) + " rows"
		}
		return pendingWord
	case state.Task.Checksumming:
		if p.ChecksumRowsTotal <= 0 {
			return PartStatusLabel(status)
		}
		// A checksum reads like a copy, the percent and then its rows, the
		// way the single-deployment comment reads it.
		checked := ui.ClampRows(p.ChecksumRowsChecked, p.ChecksumRowsTotal)
		pct := ui.ClampPercent(int(checked * 100 / p.ChecksumRowsTotal))
		return fmt.Sprintf("checksumming %s · %s / %s rows verified", ui.FormatRowCopyPercent(pct, checked, p.ChecksumRowsTotal), ui.FormatNumber(checked), ui.FormatNumber(p.ChecksumRowsTotal))
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
	if ui.CopyFinishedShortOfEstimate(p.PercentComplete, p.RowsCopied, p.RowsTotal) {
		// The part's copy is done short of its estimate, so its rows read as
		// the rows it copied rather than a fraction of the estimate.
		return fmt.Sprintf("%s · %s %s copied", pct, ui.FormatNumber(p.RowsCopied), ui.Pluralize("row", int(min(p.RowsCopied, 2))))
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
	// RunningLabel is the word Phrases counts Running parts by,
	// RunningCopying when empty (RunningWord).
	RunningLabel string
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
		label := c.RunningLabel
		if label == "" {
			label = RunningCopying
		}
		parts = append(parts, fmt.Sprintf("%d %s", c.Running, label))
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

// TargetRows is a table's rows summed across the targets that run it, for the
// rows line under its bar and the percent beside it. Every surface derives it
// with SumTargetRows, so the PR comment and the CLI count the same rows.
type TargetRows struct {
	// Copied and Total are the rows copied of the rows to copy across the
	// targets in Counted.
	Copied, Total int64
	// Counted is how many targets the sum covers. A target whose rows are not
	// known, or whose rows are not progressing, is left out.
	Counted int
	// Estimated reports whether a plan's estimate stands in for the rows of a
	// target still to copy, which makes Total approximate.
	Estimated bool
	// ETASeconds is the slowest copying target's ETA, zero when none reports
	// one.
	ETASeconds int64
	// ETAFloor reports whether a target still has time to add that no target's
	// ETA covers: it has not started, or it is working without having reported
	// its rows yet. The table's ETA is then a floor.
	ETAFloor bool
}

// SumTargetRows sums a table's rows across its targets the way
// PercentAcrossTargets weighs them, so the rows line agrees with the bar. A
// completed target counts in full, since it has every row; its engine's row
// estimate may have run ahead of the rows it copied. A working target counts
// the rows it reports, capped at its own total, so one target copying past its
// estimate never covers another's unfinished copy. A target still to copy
// counts the plan's estimate (Part.PlannedRows) as nothing copied yet, and
// without one it is left out. A target past its copy that reported no rows
// copied a table with none, so it counts as nothing to copy. A target that
// failed, is retrying, halted, or is undoing the change is left out, since its
// rows are not progressing.
func SumTargetRows(parts []Part) TargetRows {
	var r TargetRows
	for _, p := range parts {
		status := state.NormalizeTaskStatus(p.Status)
		if status == state.Task.Pending || (p.RowsTotal <= 0 && CopyStillAhead(status)) {
			r.ETAFloor = true
		}
		if status == state.Task.Running {
			r.ETASeconds = max(r.ETASeconds, p.ETASeconds)
		}
		switch status {
		case state.Task.Completed, state.Task.RevertWindow:
			r.Counted++
			r.Copied += max(p.RowsTotal, 0)
			r.Total += max(p.RowsTotal, 0)
		case state.Task.Failed, state.Task.FailedRetryable, state.Task.Stopped, state.Task.Cancelled,
			state.Task.Reverting, state.Task.Reverted:
			continue
		case state.Task.Pending:
			// A queued target's rows are the plan's estimate until it starts:
			// PercentAcrossTargets weighs it no other way.
			if p.PlannedRows != nil {
				r.Counted++
				r.Total += *p.PlannedRows
				r.Estimated = true
			}
		default:
			switch {
			case p.RowsTotal > 0:
				r.Counted++
				r.Copied += ui.CopyCoveredRows(p.PercentComplete, p.RowsCopied, p.RowsTotal)
				r.Total += p.RowsTotal
			case p.PlannedRows != nil:
				r.Counted++
				r.Total += *p.PlannedRows
				r.Estimated = true
			case !CopyStillAhead(status):
				r.Counted++
			}
		}
	}
	return r
}

// Exact reports whether every one of targets is in the sum by its engine's own
// count, so the percent beside the bar can carry a single target's precision.
func (r TargetRows) Exact(targets int) bool {
	return r.Counted == targets && !r.Estimated && r.Total > 0
}

// Partial reports whether the sum leaves some of targets out, so the rows line
// names the targets it covers.
func (r TargetRows) Partial(targets int) bool {
	return r.Counted < targets
}

// RowsLine is the rows line under a table's bar across targets, after its
// bullet: "Rows: 1,400 / 2,000 across 2 of 3 targets · ~3.5 GB · ETA: ≥ 2m".
// The total is approximate while a plan's estimate stands in for a target. The
// line names the targets it covers when it leaves some out, and then the
// planned size, which is every target's, names them all (TargetsTableSizeClause).
// estimatedBytes is the plan's size summed across every target, nil unless each
// has one. It is empty while no target's rows are known.
func (r TargetRows) RowsLine(targets int, estimatedBytes *int64) string {
	if r.Total <= 0 {
		return ""
	}
	total := ui.FormatNumber(r.Total)
	if r.Estimated {
		total = ui.FormatApproxRows(r.Total)
	}
	line := fmt.Sprintf("Rows: %s / %s", ui.FormatNumber(r.Copied), total)
	partial := r.Partial(targets)
	if partial {
		line += fmt.Sprintf(" across %d of %d targets", r.Counted, targets)
	}
	line += TargetsTableSizeClause(estimatedBytes, partial, targets)
	if r.ETASeconds > 0 {
		floor := ""
		if r.ETAFloor {
			floor = "≥ "
		}
		line += " · ETA: " + floor + ui.FormatETA(r.ETASeconds)
	}
	return line
}

// AcrossTargetsPercent renders a table's percent across its targets beside
// its bar. When exact, every target's rows are its engine's own count, so the
// percent is the rows copied of the rows to copy at the precision a single
// target's percent has (ui.FormatRowCopyPercent). Otherwise it is an estimate
// from each target's share, or counts a plan's estimate, and stays whole. It
// never reads complete before pct (PercentAcrossTargets), which waits for
// every target to complete, does.
func AcrossTargetsPercent(pct int, exact bool, copied, total int64) string {
	if !exact || total <= 0 {
		return fmt.Sprintf("%d%%", pct)
	}
	if pct < 100 && copied >= total {
		// Every row is copied but a target has yet to complete the change.
		return "99.99%"
	}
	return ui.FormatRowCopyPercent(pct, copied, total)
}

// IndexBuildPercentAcrossTargets is how far a concurrent index build is across
// every target that runs it. A build has no rows to count, and each target
// reports its own whole-build percent instead, so the table's percent is their
// mean: a completed target counts in full, a running target by its percent,
// and every other target as nothing yet. Like PercentAcrossTargets, it stops
// short of complete until every target has completed. Once any target has
// made progress it reads at least 1%, the way a row copy that has moved rows
// does (ui.RowCopyDisplayPercent): across many targets the mean can round
// down to 0, which would read as a build that has not started.
func IndexBuildPercentAcrossTargets(parts []Part) int {
	if len(parts) == 0 {
		return 0
	}
	sum, done := 0, 0
	for _, p := range parts {
		switch state.NormalizeTaskStatus(p.Status) {
		case state.Task.Completed, state.Task.RevertWindow:
			sum += 100
			done++
		case state.Task.Running:
			sum += ui.ClampPercent(p.PercentComplete)
		}
	}
	percent := sum / len(parts)
	if percent == 0 && sum > 0 {
		percent = 1
	}
	if done < len(parts) {
		percent = min(percent, 99)
	}
	return percent
}

// ShardedPhasePercent is the bar of a sharded table that keeps its bar past
// its row copy (ShardsKeepTableBar). When the table's row total counts the
// plan's estimate for the shards still to copy (RollUpShardedTable), the bar
// is the rows copied over that total, the same fraction its rows line and its
// copy bar show, so it does not jump when a shard moves from copying to
// verifying. It stops short of complete, since the table is still in flight.
// Otherwise it is the table's share across its shards
// (PercentAcrossTargets).
func ShardedPhasePercent(parts []Part, silent int, rowsCopied, rowsTotal int64, rowsTotalPlanned bool) int {
	if rowsTotalPlanned && rowsTotal > 0 {
		return min(int(ui.ClampRows(rowsCopied, rowsTotal)*100/rowsTotal), 99)
	}
	return PercentAcrossTargets(parts, silent)
}

// PercentAcrossTargets is how far a table is across every target that runs it,
// for the bar that leads its line until every target completes. While every
// target's rows are known, it is the rows copied over the rows to copy across
// all of them. A target's rows are known once it has completed, once its
// engine reports a row total, once it is past its copy (a table with no rows
// to copy), or, while its copy is still ahead, from the plan's estimate
// (Part.PlannedRows), which counts as nothing copied yet. Otherwise it is the
// table's share across all of its targets: a completed target counts in full,
// a working target by the rows its engine reports, and every other target as
// nothing yet. A target that failed, is retrying, halted, or is undoing the
// change leaves the table to its share and counts as nothing, since it has not
// landed the change and its rows are not progressing, so one copy beside many
// failures never reads as half done. silent is the targets with no progress
// reported at all. The percent stops short of complete until every target has
// completed, since rows copied on a target still verifying or waiting to cut
// over are not the change landing there.
func PercentAcrossTargets(parts []Part, silent int) int {
	targets := len(parts) + silent
	if targets == 0 {
		return 0
	}
	var share float64
	var copied, total int64
	done := 0
	everyReported := silent == 0
	for _, p := range parts {
		switch state.NormalizeTaskStatus(p.Status) {
		case state.Task.Completed, state.Task.RevertWindow:
			// A target in its revert window has completed the change, and may
			// have run it without copying any rows.
			share++
			done++
			if p.RowsTotal > 0 {
				copied += p.RowsTotal
				total += p.RowsTotal
			}
		case state.Task.Failed, state.Task.FailedRetryable, state.Task.Stopped, state.Task.Cancelled:
			// Halted or waiting on a retry: its rows are not progressing.
			everyReported = false
		case state.Task.Reverting, state.Task.Reverted:
			// Undoing the change: the rows its copy moved are not progress.
			everyReported = false
		case state.Task.Pending:
			if p.PlannedRows == nil {
				everyReported = false
				continue
			}
			total += *p.PlannedRows
		default:
			if p.RowsTotal <= 0 {
				switch {
				case p.PlannedRows != nil:
					total += *p.PlannedRows
				case CopyStillAhead(p.Status):
					everyReported = false
				default:
					// Past its copy with no rows reported: the table had
					// none to copy there.
				}
				continue
			}
			rows := ui.CopyCoveredRows(p.PercentComplete, p.RowsCopied, p.RowsTotal)
			share += float64(rows) / float64(p.RowsTotal)
			copied += rows
			total += p.RowsTotal
		}
	}
	percent := int(share * 100 / float64(targets))
	if everyReported && total > 0 {
		percent = int(copied * 100 / total)
	}
	if done < targets {
		percent = min(percent, 99)
	}
	return percent
}
