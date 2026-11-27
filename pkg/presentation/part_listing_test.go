package presentation

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
)

func statusesOf(statuses ...string) func(int) string {
	return func(i int) string { return statuses[i] }
}

func TestCountParts(t *testing.T) {
	task := state.Task
	tests := []struct {
		name     string
		statuses []string
		want     PartCounts
	}{
		{"copying and waiting", []string{task.Running, task.Running, task.WaitingForCutover}, PartCounts{Total: 3, Running: 2, WaitingForCutover: 1}},
		{"all complete", []string{task.Completed, task.Completed}, PartCounts{Total: 2, Complete: 2}},
		{"cutting over is not complete", []string{task.CuttingOver, task.CuttingOver}, PartCounts{Total: 2, CuttingOver: 2}},
		{"cancelled is not failed", []string{task.Cancelled, task.Cancelled}, PartCounts{Total: 2, Cancelled: 2}},
		{"other phases are kept", []string{task.Checksumming, "STATE_PENDING"}, PartCounts{Total: 2, Queued: 1, Other: map[string]int{task.Checksumming: 1}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, CountParts(len(tt.statuses), statusesOf(tt.statuses...)))
		})
	}
}

func TestPartCountsPhrases(t *testing.T) {
	assert.Equal(t, []string{"20 copying", "2 waiting for cutover", "10 complete"}, PartCounts{Complete: 10, Running: 20, WaitingForCutover: 2}.Phrases())
	assert.Equal(t, []string{"1 failed", "2 cutting over", "1 queued"}, PartCounts{CuttingOver: 2, Queued: 1, Failed: 1}.Phrases())
	assert.Equal(t, []string{"none"}, PartCounts{}.Phrases())
	// A CREATE TABLE, DROP TABLE or instant ALTER copies no rows, so its
	// running parts read as applying.
	assert.Equal(t, []string{"1 applying", "3 complete"}, PartCounts{Complete: 3, Running: 1, RunningLabel: RunningApplying}.Phrases())
}

func TestRunningWord(t *testing.T) {
	assert.Equal(t, RunningCopying, RunningWord(true))
	assert.Equal(t, RunningApplying, RunningWord(false))
}

func TestTargetCoverageNamesTheRunningWord(t *testing.T) {
	assert.Equal(t, " · 2 complete, 1 applying", TargetCoverage(2, 1, 0, 0, 0, RunningApplying, PendingQueued))
	assert.Equal(t, " · 2 copying, 1 queued", TargetCoverage(0, 2, 1, 0, 0, RunningCopying, PendingQueued))
}

func TestPartDetail(t *testing.T) {
	task := state.Task
	tests := []struct {
		part Part
		want string
	}{
		{Part{Status: task.Completed, RowsTotal: 12129068}, "12,129,068 rows"},
		{Part{Status: task.Completed}, "complete"},
		{Part{Status: task.Running, PercentComplete: 62, RowsCopied: 620, RowsTotal: 1000}, "62.00% · 620 / 1,000 rows"},
		{Part{Status: task.Running, PercentComplete: 45}, "45%"},
		{Part{Status: task.Running}, "copying"},
		{Part{Status: task.Pending}, "queued"},
		{Part{Status: task.Pending, PlannedRows: new(int64(1_200))}, "queued · ~1.2k rows"},
		{Part{Status: task.WaitingForCutover}, "waiting for cutover"},
		{Part{Status: task.Checksumming}, "checksumming"},
		{Part{Status: task.Checksumming, ChecksumRowsChecked: 450, ChecksumRowsTotal: 1000}, "checksumming 45.00% · 450 / 1,000 rows verified"},
		{Part{Status: task.Failed}, "failed"},
		{Part{Status: task.Cancelled, RowsCopied: 800000, RowsTotal: 2100000}, "cancelled at 38.10% · 800,000 / 2,100,000 rows"},
		{Part{Status: task.Running, PercentComplete: 100, RowsCopied: 1, RowsTotal: 200_000}, "100.00% · 1 row copied"},
		{Part{Status: task.Running, PercentComplete: 100, RowsCopied: 3, RowsTotal: 200_000}, "100.00% · 3 rows copied"},
		{Part{Status: task.Stopped}, "stopped"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, PartDetail(tt.part, PendingQueued))
		})
	}
	// A part that copies no rows reads as applying until it reports.
	assert.Equal(t, "applying", PartDetail(Part{Status: task.Running, RunningLabel: RunningApplying}, PendingQueued))
	// A part a settled rollout will never run reads as not started.
	assert.Equal(t, "not started", PartDetail(Part{Status: task.Pending}, PendingWord(true)))
	assert.Equal(t, "not started · ~1.2k rows", PartDetail(Part{Status: task.Pending, PlannedRows: new(int64(1_200))}, PendingWord(true)))
}

// Up to the inline limit a listing names every part in the parts' own order,
// so a target keeps its line as it moves from copying to checksumming to
// waiting for cutover. Past it, it names only the failures and the slowest
// copying parts, since the heading already counts every state, so a wide
// table stays short.
func TestListParts(t *testing.T) {
	task := state.Task
	t.Run("inline keeps the parts' order whatever their phase", func(t *testing.T) {
		parts := []Part{
			{Name: "001", Status: task.Checksumming},
			{Name: "002", Status: task.WaitingForCutover},
			{Name: "003", Status: task.Running, RowsCopied: 200, RowsTotal: 1000},
			{Name: "004", Status: task.Pending},
			{Name: "005", Status: task.Failed},
			{Name: "006", Status: task.Completed},
		}
		var names []string
		for _, line := range ListParts(len(parts), func(i int) Part { return parts[i] }, TargetNoun) {
			names = append(names, parts[line.Part].Name)
		}
		assert.Equal(t, []string{"001", "002", "003", "004", "005", "006"}, names)
	})
	t.Run("wide", func(t *testing.T) {
		var parts []Part
		add := func(n int, p Part) {
			for range n {
				p.Name = fmt.Sprintf("t%02d", len(parts))
				parts = append(parts, p)
			}
		}
		add(12, Part{Status: task.Failed})
		add(5, Part{Status: task.WaitingForCutover})
		for i := range 7 {
			add(1, Part{Status: task.Running, RowsCopied: int64(100 * (7 - i)), RowsTotal: 1000})
		}
		add(3, Part{Status: task.Completed})
		add(2, Part{Status: task.Pending})
		got := ListParts(len(parts), func(i int) Part { return parts[i] }, TargetNoun)

		var want []PartListLine
		for i := range 5 {
			want = append(want, PartListLine{Part: i})
		}
		want = append(want, PartListLine{Part: -1, Summary: "7 more failed targets"})
		// The slowest copying targets, furthest behind first.
		want = append(want, PartListLine{Part: 23}, PartListLine{Part: 22}, PartListLine{Part: 21}, PartListLine{Part: 20})
		want = append(want, PartListLine{Part: -1, Summary: "3 more copying targets"})
		// The targets working past the copy, in order, each as live as a copier.
		want = append(want, PartListLine{Part: 12}, PartListLine{Part: 13}, PartListLine{Part: 14}, PartListLine{Part: 15})
		want = append(want, PartListLine{Part: -1, Summary: "1 more target past copying"})
		assert.Equal(t, want, got)
	})
	t.Run("one past each cap reads singular", func(t *testing.T) {
		var parts []Part
		for i := range 6 {
			parts = append(parts, Part{Name: fmt.Sprintf("failed%d", i), Status: task.Failed})
		}
		for i := range 5 {
			parts = append(parts, Part{Name: fmt.Sprintf("copying%d", i), Status: task.Running, RowsCopied: int64(100 * (i + 1)), RowsTotal: 1000})
		}
		var summaries []string
		for _, line := range ListParts(len(parts), func(i int) Part { return parts[i] }, ShardNoun) {
			if line.Summary != "" {
				summaries = append(summaries, line.Summary)
			}
		}
		assert.Equal(t, []string{"1 more failed shard", "1 more copying shard"}, summaries)
	})
	t.Run("copiers that have not reported follow the ones that have", func(t *testing.T) {
		var parts []Part
		for i := range 6 {
			parts = append(parts, Part{Name: fmt.Sprintf("quiet%d", i), Status: task.Running})
		}
		for i := range 4 {
			parts = append(parts, Part{Name: fmt.Sprintf("measured%d", i), Status: task.Running, RowsCopied: int64(100 * (i + 1)), RowsTotal: 1000})
		}
		var names []string
		for _, line := range ListParts(len(parts), func(i int) Part { return parts[i] }, ShardNoun) {
			if line.Summary != "" {
				names = append(names, line.Summary)
				continue
			}
			names = append(names, parts[line.Part].Name)
		}
		assert.Equal(t, []string{"measured0", "measured1", "measured2", "measured3", "6 more copying shards"}, names,
			"the slowest measured copiers set the pace; ones with nothing reported are not named ahead of them")
	})
	t.Run("the heading counts an unlisted phase before cancelled", func(t *testing.T) {
		// A phase outside otherPartStatusOrder, as a new task state would be
		// until it is added there, sits after the listed phases and before
		// cancelled.
		const unlisted = "zz_new_phase"
		c := PartCounts{Other: map[string]int{state.Task.Stopped: 1, unlisted: 1}, Cancelled: 1}
		assert.Equal(t, []string{"1 stopped", "1 " + unlisted, "1 cancelled"}, c.Phrases())
	})
}

// A table lists its parts in every state in which an engine is still working
// on the change, waiting on a deploy request and reverting included, and in
// none in which it has not started or has settled. A rollout's targets are
// listed once stopped too, since each resumes from the row the listing names.
func TestListsParts(t *testing.T) {
	for _, s := range []string{
		state.Task.Running, state.Task.CatchingUp, state.Task.Checksumming, state.Task.PostChecksum,
		state.Task.WaitingForCutover, state.Task.CuttingOver, state.Task.WaitingForDeploy, state.Task.Recovering, state.Task.Reverting,
	} {
		assert.Truef(t, ListsParts(s), "a table %s lists its parts", s)
		assert.Truef(t, ListsTargets([]string{state.Task.Completed, s}), "a table with a target %s lists its targets", s)
	}
	for _, s := range []string{
		state.Task.Pending, state.Task.Completed, state.Task.RevertWindow, state.Task.Failed,
		state.Task.FailedRetryable, state.Task.Cancelled, state.Task.Reverted, state.Task.Stopped,
	} {
		assert.Falsef(t, ListsParts(s), "a table %s counts its parts on its line", s)
	}
}

// TestListsTargets pins that a table across targets decides its listing from
// each target, not from its rolled-up status: a stopped or copying target is
// listed even when a failed sibling makes the table read as failed, since the
// stopped target resumes from the row it reached and only the listing says
// which row that is.
func TestListsTargets(t *testing.T) {
	assert.True(t, ListsTargets([]string{state.Task.Stopped, state.Task.Stopped}), "a stopped table lists where each target stopped")
	assert.True(t, ListsTargets([]string{state.Task.Completed, state.Task.Failed, state.Task.Stopped, state.Task.Stopped}),
		"a failed table lists the targets that stopped beside the failure")
	assert.True(t, ListsTargets([]string{state.Task.Failed, state.Task.Running}), "a failed table lists a target still copying")
	assert.True(t, ListsTargets([]string{"STATE_STOPPED"}), "a target's raw status is normalized before it is read")
	assert.False(t, ListsTargets([]string{state.Task.Completed, state.Task.Failed}), "a settled table counts its targets on its line")
	assert.False(t, ListsTargets([]string{state.Task.Cancelled, state.Task.Completed}), "a cancelled table counts its targets on its line")
	assert.False(t, ListsTargets(nil), "a table with no targets lists nothing")
}

// A part's mark says whether it is working: every phase an engine is working
// through reads as working, not only the row copy, and only a part that is not
// working on the change reads as idle.
func TestPartGlyph(t *testing.T) {
	task := state.Task
	for _, s := range []string{task.Running, task.CatchingUp, task.Checksumming, task.PostChecksum, task.WaitingForDeploy, task.Recovering, task.Reverting} {
		assert.Equalf(t, "◉", PartGlyph(s), "a part %s is working", s)
	}
	for _, s := range []string{task.WaitingForCutover, task.CuttingOver, task.RevertWindow} {
		assert.Equalf(t, "●", PartGlyph(s), "a part %s is at its cutover", s)
	}
	for _, s := range []string{task.Pending, task.Stopped, task.Cancelled, task.FailedRetryable} {
		assert.Equalf(t, "○", PartGlyph(s), "a part %s is not working", s)
	}
	assert.Equal(t, "✓", PartGlyph(task.Completed))
	assert.Equal(t, "✗", PartGlyph(task.Failed))
}

// A table's bar across its targets counts a target in its revert window as
// done, weighs the rows of every target once all have reported, and stops
// short of complete until every target has completed.
func TestPercentAcrossTargets(t *testing.T) {
	task := state.Task
	// orders-001 finished a direct ALTER and is in its revert window with no
	// rows copied, orders-002 has copied half its rows, and orders-003 has
	// reported nothing: half the table is done.
	assert.Equal(t, 50, PercentAcrossTargets([]Part{
		{Status: task.RevertWindow},
		{Status: task.Running, RowsCopied: 500, RowsTotal: 1000},
	}, 1))
	// Two targets parked at cutover and two queued are half the table.
	parked := Part{Status: task.WaitingForCutover, RowsCopied: 1000, RowsTotal: 1000}
	queued := Part{Status: task.Pending}
	assert.Equal(t, 50, PercentAcrossTargets([]Part{parked, parked, queued, queued}, 0))
	// Every target parked has copied every row, but none has the change yet.
	assert.Equal(t, 99, PercentAcrossTargets([]Part{parked, parked}, 0))
	// With every target reporting, the rows weigh in: 900 + 100 of 2,000.
	assert.Equal(t, 50, PercentAcrossTargets([]Part{
		{Status: task.Running, RowsCopied: 900, RowsTotal: 1000},
		{Status: task.Checksumming, RowsCopied: 100, RowsTotal: 1000},
	}, 0))
	// A target whose copy finished short of its estimate and is now verifying
	// counts in full: 1,000 + 500 of 2,000.
	assert.Equal(t, 75, PercentAcrossTargets([]Part{
		{Status: task.Checksumming, PercentComplete: 100, RowsCopied: 1, RowsTotal: 1000},
		{Status: task.Running, RowsCopied: 500, RowsTotal: 1000},
	}, 0))
	// A failed target has not landed the change, so the table reads its share
	// across all three rather than the rows of the two still in play.
	assert.Equal(t, 50, PercentAcrossTargets([]Part{
		{Status: task.Completed, RowsCopied: 1000, RowsTotal: 1000},
		{Status: task.Running, RowsCopied: 500, RowsTotal: 1000},
		{Status: task.Failed, RowsCopied: 300, RowsTotal: 1000},
	}, 0))
	// A reverted target has undone the change, so its copied rows are not
	// progress: the table reads the other target's half across both, the way
	// its rows line counts only the target still copying.
	assert.Equal(t, 25, PercentAcrossTargets([]Part{
		{Status: task.Reverted, RowsCopied: 1000, RowsTotal: 1000},
		{Status: task.Running, RowsCopied: 500, RowsTotal: 1000},
	}, 0))
	assert.Equal(t, 100, PercentAcrossTargets([]Part{{Status: task.Completed}, {Status: task.RevertWindow}}, 0))
	assert.Equal(t, 0, PercentAcrossTargets(nil, 0))
}

// A target whose copy is still ahead counts by the plan's row estimate, so
// the bar is the rows copied over the rows to copy across every target from
// the start. A target without an estimate, or one with no progress at all,
// leaves the table to its share.
func TestPercentAcrossTargets_PlannedRows(t *testing.T) {
	task := state.Task
	planned := func(rows int64) *int64 { return &rows }
	copying := Part{Status: task.Running, RowsCopied: 500, RowsTotal: 1000}
	// 500 of 1,000 + 1,200 + 1,800 planned is 12%, where the share across
	// three targets would read 16%.
	assert.Equal(t, 12, PercentAcrossTargets([]Part{
		copying,
		{Status: task.Pending, PlannedRows: planned(1200)},
		{Status: task.Pending, PlannedRows: planned(1800)},
	}, 0))
	// A target that started but has not reported its total counts by its
	// estimate too.
	assert.Equal(t, 12, PercentAcrossTargets([]Part{
		copying,
		{Status: task.Running, PlannedRows: planned(1200)},
		{Status: task.Pending, PlannedRows: planned(1800)},
	}, 0))
	// A target past its copy with no rows had none to copy, so the rows of
	// the others carry the bar.
	assert.Equal(t, 50, PercentAcrossTargets([]Part{copying, {Status: task.WaitingForCutover}}, 0))
	// Without an estimate for every target, the table reads its share.
	assert.Equal(t, 16, PercentAcrossTargets([]Part{
		copying,
		{Status: task.Pending, PlannedRows: planned(1200)},
		{Status: task.Pending},
	}, 0))
	assert.Equal(t, 16, PercentAcrossTargets([]Part{
		copying,
		{Status: task.Pending, PlannedRows: planned(1200)},
	}, 1))
}

// A target undoing the change speaks for the table whatever its siblings are
// doing, since a bar would count the rows its copy once moved as progress.
func TestRevertSpeaksForParts(t *testing.T) {
	task := state.Task
	assert.True(t, RevertSpeaksForParts([]string{task.Reverting, task.Reverted}))
	assert.True(t, RevertSpeaksForParts([]string{task.Completed, "STATE_REVERTING"}))
	assert.False(t, RevertSpeaksForParts([]string{task.RevertWindow, task.Running}))
	assert.False(t, RevertSpeaksForParts(nil))
}

// A phase past the copy that every part shares speaks for the table; parts in
// different phases, or all still copying, leave the table to its bar.
func TestOnePhaseSpeaksForParts(t *testing.T) {
	task := state.Task
	assert.True(t, OnePhaseSpeaksForParts([]string{task.WaitingForCutover, "STATE_WAITING_FOR_CUTOVER"}))
	assert.True(t, OnePhaseSpeaksForParts([]string{task.Checksumming}))
	assert.False(t, OnePhaseSpeaksForParts([]string{task.Checksumming, task.WaitingForCutover}))
	assert.False(t, OnePhaseSpeaksForParts([]string{task.Running, task.Running}))
	assert.False(t, OnePhaseSpeaksForParts([]string{task.WaitingForCutover, task.Pending}))
	assert.False(t, OnePhaseSpeaksForParts(nil))
}

// A table's rows across targets weigh each target the way its bar does: a
// target that completed counts in full whatever its last report read, a
// copying target counts its rows up to its own total, and a queued target
// counts only by the plan's estimate. A halted target, or a queued one with no
// estimate, is left out, so the rows cover only some of the targets.
func TestSumTargetRows(t *testing.T) {
	task := state.Task
	planned := func(rows int64) *int64 { return &rows }
	copying := Part{Status: task.Running, RowsCopied: 420, RowsTotal: 1000, ETASeconds: 120}

	rows := SumTargetRows([]Part{copying, {Status: task.Completed, RowsCopied: 990, RowsTotal: 1000}})
	assert.Equal(t, TargetRows{Copied: 1420, Total: 2000, Counted: 2, ETASeconds: 120}, rows)
	assert.True(t, rows.Exact(2))
	assert.False(t, rows.Partial(2))

	// Rows a queued target reported before its copy started are not its
	// progress, so they stay out of the sum.
	rows = SumTargetRows([]Part{copying, {Status: task.Pending, RowsTotal: 9000}})
	assert.Equal(t, TargetRows{Copied: 420, Total: 1000, Counted: 1, ETASeconds: 120, ETAFloor: true}, rows)
	assert.False(t, rows.Exact(2))
	assert.True(t, rows.Partial(2))

	rows = SumTargetRows([]Part{copying, {Status: task.Pending, PlannedRows: planned(1000)}})
	assert.Equal(t, TargetRows{Copied: 420, Total: 2000, Counted: 2, Estimated: true, ETASeconds: 120, ETAFloor: true}, rows)
	assert.False(t, rows.Exact(2), "an estimate keeps the percent whole")

	rows = SumTargetRows([]Part{copying, {Status: task.Failed, RowsCopied: 300, RowsTotal: 1000}})
	assert.Equal(t, TargetRows{Copied: 420, Total: 1000, Counted: 1, ETASeconds: 120}, rows)

	// A target whose copy finished short of its estimate counts its whole
	// total, so it does not pull the rows across targets down.
	rows = SumTargetRows([]Part{copying, {Status: task.Running, PercentComplete: 100, RowsCopied: 1, RowsTotal: 1000}})
	assert.Equal(t, TargetRows{Copied: 1420, Total: 2000, Counted: 2, ETASeconds: 120}, rows)

	// A target past its copy with no rows had none to copy.
	rows = SumTargetRows([]Part{copying, {Status: task.WaitingForCutover}})
	assert.Equal(t, TargetRows{Copied: 420, Total: 1000, Counted: 2, ETASeconds: 120}, rows)
	assert.True(t, rows.Exact(2))
}

// The rows line names how many targets the rows cover when they cover only
// some, marks an estimated total as approximate, and floors the ETA when a
// target still has time to add that no ETA covers.
func TestTargetRowsLine(t *testing.T) {
	size := int64(4_000_000_000)
	exact := TargetRows{Copied: 3420, Total: 4000, Counted: 4, ETASeconds: 120}
	assert.Equal(t, "Rows: 3,420 / 4,000 · ~4.0 GB · ETA: 2m", exact.RowsLine(4, &size))

	partial := TargetRows{Copied: 1420, Total: 2000, Counted: 2, ETASeconds: 120, ETAFloor: true}
	assert.Equal(t, "Rows: 1,420 / 2,000 across 2 of 3 targets · ~4.0 GB across all 3 targets · ETA: ≥ 2m", partial.RowsLine(3, &size))

	estimated := TargetRows{Copied: 500, Total: 2000, Counted: 2, Estimated: true, ETAFloor: true}
	assert.Equal(t, "Rows: 500 / ~2k", estimated.RowsLine(2, nil))

	assert.Empty(t, TargetRows{Counted: 1}.RowsLine(2, nil), "no rows known")
}

// The percent beside a table's bar across targets reads at one target's
// precision only when every target's rows are its own count, and never reads
// as done while a target is still short of its total.
func TestAcrossTargetsPercent(t *testing.T) {
	assert.Equal(t, "85.50%", AcrossTargetsPercent(85, true, 3420, 4000))
	assert.Equal(t, "85%", AcrossTargetsPercent(85, false, 3420, 4000))
	assert.Equal(t, "99.99%", AcrossTargetsPercent(99, true, 4000, 4000))
	assert.Equal(t, "0%", AcrossTargetsPercent(0, true, 0, 0))
}

// A CREATE TABLE, a DROP TABLE and an instant ALTER copy no rows; any other
// ALTER does, and an action the rule does not recognize reads as copying.
func TestChangeCopiesRows(t *testing.T) {
	assert.False(t, ChangeCopiesRows("create", false))
	assert.False(t, ChangeCopiesRows("drop", false))
	assert.False(t, ChangeCopiesRows("alter", true))
	assert.True(t, ChangeCopiesRows("alter", false))
	assert.True(t, ChangeCopiesRows("", false))
	assert.True(t, ChangeCopiesRows("", true))
}

// A table rolled up across shards reads as applying only when the shards that
// have recorded their changes all copy no rows. A shard with nothing recorded
// yet does not decide, and the plan's change decides only when no shard has
// recorded one.
func TestShardedTableCopiesNoRows(t *testing.T) {
	instant := []TableChange{{DDLAction: "alter", Instant: true}}
	copying := []TableChange{{DDLAction: "alter"}}
	plannedAlter := &TableChange{DDLAction: "alter"}
	plannedCreate := &TableChange{DDLAction: "create"}

	assert.True(t, ShardedTableCopiesNoRows([][]TableChange{nil, instant}, plannedAlter),
		"a shard with nothing recorded does not outvote one running an instant ALTER")
	assert.False(t, ShardedTableCopiesNoRows([][]TableChange{instant, copying}, nil),
		"one shard copying rows makes the table a table copy")
	assert.True(t, ShardedTableCopiesNoRows([][]TableChange{nil, nil}, plannedCreate),
		"with nothing recorded, a planned CREATE TABLE copies no rows")
	assert.False(t, ShardedTableCopiesNoRows([][]TableChange{nil, nil}, plannedAlter),
		"a planned ALTER carries no instant flag, so it reads as a table copy")
	assert.False(t, ShardedTableCopiesNoRows([][]TableChange{nil}, nil),
		"with nothing recorded or planned, the table reads as copying")
}

// An index build across targets reads as the mean of each target's whole-build
// percent. A completed target counts in full, a target that is queued or
// halted counts as nothing, and the table never reads complete until every
// target has completed.
func TestIndexBuildPercentAcrossTargets(t *testing.T) {
	running := func(pct int) Part { return Part{Status: state.Task.Running, PercentComplete: pct} }
	tests := []struct {
		name  string
		parts []Part
		want  int
	}{
		{name: "no targets", want: 0},
		{name: "two builds at a quarter", parts: []Part{running(25), running(25)}, want: 25},
		{name: "one done, one halfway", parts: []Part{{Status: state.Task.Completed}, running(50)}, want: 75},
		{name: "a queued target counts as nothing", parts: []Part{running(60), {Status: state.Task.Pending}}, want: 30},
		{name: "a failed target counts as nothing", parts: []Part{running(80), {Status: state.Task.Failed, PercentComplete: 90}}, want: 40},
		{name: "a percent past complete is clamped", parts: []Part{running(140)}, want: 99},
		{name: "every target done", parts: []Part{{Status: state.Task.Completed}, {Status: state.Task.RevertWindow}}, want: 100},
		{name: "progress on a few of many targets reads as begun", parts: manyTargets(300, Part{Status: state.Task.Completed}, Part{Status: state.Task.Completed}, running(10)), want: 1},
		{name: "many targets, none started", parts: manyTargets(300), want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IndexBuildPercentAcrossTargets(tt.parts))
		})
	}
}

// manyTargets is a rollout of n targets: the given ones first, then queued
// targets for the rest.
func manyTargets(n int, first ...Part) []Part {
	parts := make([]Part, n)
	for i := range parts {
		parts[i] = Part{Status: state.Task.Pending}
	}
	copy(parts, first)
	return parts
}
