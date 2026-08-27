package templates

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
)

func TestCountShardsByStatus(t *testing.T) {
	shards := []ShardProgress{
		{Shard: "-80", Status: state.Task.Running},
		{Shard: "80-c0", Status: state.Task.Running},
		{Shard: "c0-", Status: state.Task.WaitingForCutover},
	}
	c := CountShardsByStatus(shards)
	assert.Equal(t, 3, c.Total)
	assert.Equal(t, 2, c.Running)
	assert.Equal(t, 1, c.WaitingForCutover)
	assert.Equal(t, 0, c.Complete)
	assert.Equal(t, 0, c.CuttingOver)
}

func TestCountShardsByStatus_AllComplete(t *testing.T) {
	shards := []ShardProgress{
		{Shard: "-80", Status: state.Task.Completed},
		{Shard: "80-", Status: state.Task.Completed},
	}
	c := CountShardsByStatus(shards)
	assert.Equal(t, 2, c.Complete)
	assert.Equal(t, 0, c.Running)
	assert.Equal(t, 0, c.WaitingForCutover)
}

func TestCountShardsByStatus_CuttingOverSeparateFromComplete(t *testing.T) {
	shards := []ShardProgress{
		{Shard: "-80", Status: state.Task.CuttingOver},
		{Shard: "80-", Status: state.Task.CuttingOver},
	}
	c := CountShardsByStatus(shards)
	assert.Equal(t, 2, c.CuttingOver)
	assert.Equal(t, 0, c.Complete)
}

func TestCountShardsByStatus_WaitingForCutoverSeparateFromComplete(t *testing.T) {
	shards := []ShardProgress{
		{Shard: "-80", Status: state.Task.WaitingForCutover},
		{Shard: "80-", Status: state.Task.WaitingForCutover},
	}
	c := CountShardsByStatus(shards)
	assert.Equal(t, 2, c.WaitingForCutover)
	assert.Equal(t, 0, c.Complete)
}

func TestCountShardsByStatus_Cancelled(t *testing.T) {
	shards := []ShardProgress{
		{Shard: "-80", Status: state.Task.Cancelled},
		{Shard: "80-", Status: state.Task.Cancelled},
	}
	c := CountShardsByStatus(shards)
	assert.Equal(t, 2, c.Cancelled)
	assert.Equal(t, 0, c.Failed)
}

func TestFormatShardSummaryParts_CopyingNotRunning(t *testing.T) {
	c := ShardCounts{Running: 3}
	parts := FormatShardSummaryParts(c, false)
	assert.Contains(t, parts, "3 copying")
	for _, p := range parts {
		assert.NotContains(t, p, "running")
	}
}

func TestFormatShardSummaryParts_WaitingForCutover(t *testing.T) {
	c := ShardCounts{WaitingForCutover: 5}
	parts := FormatShardSummaryParts(c, false)
	assert.Contains(t, parts, "5 waiting for cutover")
}

func TestFormatShardSummaryParts_CuttingOver(t *testing.T) {
	c := ShardCounts{CuttingOver: 2}
	parts := FormatShardSummaryParts(c, false)
	assert.Contains(t, parts, "2 cutting over")
}

func TestFormatShardSummaryParts_Mixed(t *testing.T) {
	c := ShardCounts{Complete: 10, Running: 20, WaitingForCutover: 2}
	parts := FormatShardSummaryParts(c, false)
	assert.Equal(t, 3, len(parts))
	assert.Equal(t, "10 complete", parts[0])
	assert.Equal(t, "2 waiting for cutover", parts[1])
	assert.Equal(t, "20 copying", parts[2])
}

func TestFormatShardSummaryParts_Empty(t *testing.T) {
	c := ShardCounts{}
	parts := FormatShardSummaryParts(c, false)
	assert.Equal(t, []string{"none"}, parts)
}

func TestFormatDurationSeconds(t *testing.T) {
	tests := []struct {
		seconds  int64
		expected string
	}{
		{0, "< 1s"},
		{-1, "< 1s"},
		{30, "30s"},
		{60, "1m 0s"},
		{90, "1m 30s"},
		{3600, "1h 0m"},
		{3661, "1h 1m"},
		{7200, "2h 0m"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.expected, FormatDurationSeconds(tt.seconds), "seconds=%d", tt.seconds)
	}
}

func TestFormatShardLineDisplaysOnePercentAfterCopyStarts(t *testing.T) {
	line := formatShardLine(ShardProgress{
		Shard:           "-80",
		Status:          state.Task.Running,
		RowsCopied:      3_000,
		RowsTotal:       1_604_159,
		PercentComplete: 0,
	})

	assert.Contains(t, line, "1% (3,000/1,604,159 rows)")
	assert.NotContains(t, line, "0%")
}

func TestIsPlanetScaleEngine(t *testing.T) {
	assert.True(t, state.IsPlanetScaleEngine("planetscale"))
	assert.True(t, state.IsPlanetScaleEngine("PlanetScale"))
	assert.True(t, state.IsPlanetScaleEngine("PLANETSCALE"))
	assert.True(t, state.IsPlanetScaleEngine("ENGINE_PLANETSCALE"))
	assert.False(t, state.IsPlanetScaleEngine("spirit"))
	assert.False(t, state.IsPlanetScaleEngine("Spirit"))
	assert.False(t, state.IsPlanetScaleEngine(""))
}

// With more copying shards than the detail view can show, the furthest-behind
// shards are the ones rendered, lowest percent first, so an operator always
// sees the laggards that gate cutover rather than the shards about to finish.
func TestFormatShardProgressShowsMostBehindCopyingShardsFirst(t *testing.T) {
	shards := []ShardProgress{
		{Shard: "s90", Status: state.Task.Running, PercentComplete: 90, RowsCopied: 900, RowsTotal: 1000},
		{Shard: "s10", Status: state.Task.Running, PercentComplete: 10, RowsCopied: 100, RowsTotal: 1000},
		{Shard: "s70", Status: state.Task.Running, PercentComplete: 70, RowsCopied: 700, RowsTotal: 1000},
		{Shard: "s30", Status: state.Task.Running, PercentComplete: 30, RowsCopied: 300, RowsTotal: 1000},
		{Shard: "s80", Status: state.Task.Running, PercentComplete: 80, RowsCopied: 800, RowsTotal: 1000},
		{Shard: "s50", Status: state.Task.Running, PercentComplete: 50, RowsCopied: 500, RowsTotal: 1000},
		{Shard: "s20", Status: state.Task.Running, PercentComplete: 20, RowsCopied: 200, RowsTotal: 1000},
		{Shard: "c1", Status: state.Task.Completed, RowsTotal: 1000},
		{Shard: "c2", Status: state.Task.Completed, RowsTotal: 1000},
	}
	out := FormatShardProgress(shards)

	// The five furthest-behind copying shards render individually, in
	// ascending percent order.
	shown := []string{"s10", "s20", "s30", "s50", "s70"}
	lastIdx := -1
	for _, shard := range shown {
		idx := strings.Index(out, shard)
		assert.Greater(t, idx, lastIdx, "shard %s should render after its slower neighbors", shard)
		lastIdx = idx
	}

	// The two furthest-ahead copying shards collapse into the summary line.
	assert.NotContains(t, out, "s80")
	assert.NotContains(t, out, "s90")
	assert.Contains(t, out, "... 2 more copying shards")
}
