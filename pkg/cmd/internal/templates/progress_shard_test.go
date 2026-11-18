package templates

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
)

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

// A copying shard that hasn't reached 1% shows the true fraction computed
// from its row counts, so a shard's early progress on a huge table reads as
// the small fraction it is instead of a rounded-up 1%.
func TestFormatShardLineShowsSubPercentFraction(t *testing.T) {
	line := formatShardLine(ShardProgress{
		Shard:           "-80",
		Status:          state.Task.Running,
		RowsCopied:      3_000,
		RowsTotal:       1_604_159,
		PercentComplete: 0,
	}, "queued")

	assert.Contains(t, line, "0.19% · 3,000 / 1,604,159 rows")
	assert.NotContains(t, line, " 0%")
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

	// The three furthest-behind copying shards render individually, in
	// ascending percent order.
	shown := []string{"s10", "s20", "s30"}
	lastIdx := -1
	for _, shard := range shown {
		idx := strings.Index(out, shard)
		assert.Greater(t, idx, lastIdx, "shard %s should render after its slower neighbors", shard)
		lastIdx = idx
	}

	// The rest are counted in the heading.
	assert.Contains(t, out, "Shards: 9 (7 copying, 2 complete)")
	assert.NotContains(t, out, "s50")
}
