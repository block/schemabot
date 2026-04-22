package templates

import (
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

func TestFormatShardSummaryParts_ReadyForCutover(t *testing.T) {
	c := ShardCounts{WaitingForCutover: 5}
	parts := FormatShardSummaryParts(c, false)
	assert.Contains(t, parts, "5 ready for cutover")
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
	assert.Equal(t, "2 ready for cutover", parts[1])
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

func TestIsVSchemaTask(t *testing.T) {
	assert.True(t, isVSchemaTask(TableProgress{TableName: "VSchema"}))
	assert.True(t, isVSchemaTask(TableProgress{TableName: "vschema:myapp_sharded"}))
	assert.True(t, isVSchemaTask(TableProgress{TableName: "VSchema: myapp_sharded"}))
	assert.False(t, isVSchemaTask(TableProgress{TableName: "users"}))
	assert.False(t, isVSchemaTask(TableProgress{TableName: "orders"}))
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
