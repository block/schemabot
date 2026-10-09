package presentation

import (
	"fmt"
	"slices"
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
		{Part{Status: task.WaitingForCutover}, "waiting for cutover"},
		{Part{Status: task.Failed}, "failed"},
		{Part{Status: task.Cancelled, RowsCopied: 800000, RowsTotal: 2100000}, "cancelled at 38.10% · 800,000 / 2,100,000 rows"},
		{Part{Status: task.Stopped}, "stopped"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, PartDetail(tt.part))
		})
	}
}

// A listing puts where the change is at the top and settled parts last, in
// the order its heading counts them. Up to the inline limit it names every
// part. Past it, it names only the failures and the slowest copying parts,
// since the heading already counts every state, so a wide table stays short.
func TestListParts(t *testing.T) {
	task := state.Task
	t.Run("inline", func(t *testing.T) {
		parts := []Part{{Name: "a", Status: task.Completed}, {Name: "b", Status: task.Running}, {Name: "c", Status: task.Pending}}
		got := ListParts(len(parts), func(i int) Part { return parts[i] }, TargetNoun)
		assert.Equal(t, []PartListLine{{Part: 1}, {Part: 2}, {Part: 0}}, got, "copying, then queued, then complete")
	})
	t.Run("inline order", func(t *testing.T) {
		parts := []Part{
			{Name: "done", Status: task.Completed},
			{Name: "ahead", Status: task.Running, RowsCopied: 800, RowsTotal: 1000},
			{Name: "next", Status: task.Pending},
			{Name: "behind", Status: task.Running, RowsCopied: 200, RowsTotal: 1000},
			{Name: "ready", Status: task.WaitingForCutover},
			{Name: "broken", Status: task.Failed},
			{Name: "last", Status: task.Pending},
		}
		var names []string
		for _, line := range ListParts(len(parts), func(i int) Part { return parts[i] }, TargetNoun) {
			names = append(names, parts[line.Part].Name)
		}
		assert.Equal(t, []string{"broken", "behind", "ahead", "ready", "next", "last", "done"}, names)
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
		add(4, Part{Status: task.WaitingForCutover})
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
		want = append(want, PartListLine{Part: 22}, PartListLine{Part: 21}, PartListLine{Part: 20})
		want = append(want, PartListLine{Part: -1, Summary: "4 more copying targets"})
		assert.Equal(t, want, got)
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
		assert.Equal(t, []string{"measured0", "measured1", "measured2", "7 more copying shards"}, names,
			"the slowest measured copiers set the pace; ones with nothing reported are not named ahead of them")
	})
	t.Run("lines follow the heading's order", func(t *testing.T) {
		// A phase outside otherPartStatusOrder, as a new task state would be
		// until it is added there, sits after the listed phases and before
		// cancelled in both the heading and the lines.
		const unlisted = "zz_new_phase"
		c := PartCounts{Other: map[string]int{state.Task.Stopped: 1, unlisted: 1}, Cancelled: 1}
		assert.Equal(t, []string{"1 stopped", "1 " + unlisted, "1 cancelled"}, c.Phrases())
		statuses := []string{task.Cancelled, unlisted, task.Stopped}
		slices.SortStableFunc(statuses, comparePartStatuses)
		assert.Equal(t, []string{task.Stopped, unlisted, task.Cancelled}, statuses)
	})
}
