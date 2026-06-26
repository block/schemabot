package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
)

const mutesDDL = "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`);"

func mutesCell(shard string) ShardCell { return ShardCell{Shard: shard, Table: "mutes", DDL: mutesDDL} }

// A uniform sharded apply (every shard the same single change) renders one
// status table and shows the DDL once — no per-shard grouping.
func TestRenderShardedApplyComment_UniformSingleTable(t *testing.T) {
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "cdb_resolute",
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x", RequestedBy: "morgo",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "🔄", Label: "running table copy", State: state.ApplyOperation.Running},
			{Shard: "80-", Emoji: "⏳", Label: "queued — next in order", State: state.ApplyOperation.Pending},
		},
		Cells: []ShardCell{mutesCell("-40"), mutesCell("80-")},
	})

	assert.Contains(t, out, "**Shards**: 1 running table copy, 1 queued")
	assert.Contains(t, out, "| Shard | Status |")
	assert.NotContains(t, out, "grouped by change", "a uniform apply is not grouped")
	assert.Equal(t, 1, strings.Count(out, "```sql"), "the shared DDL is shown exactly once")
}

// A failed shard's error is lifted to the top and shown in its status row.
func TestRenderShardedApplyComment_FailedSurfacesError(t *testing.T) {
	const failErr = "resolve shard primary for `-40`: context deadline exceeded"
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "staging", Database: "cdb_resolute",
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "❌", Label: "failed", State: state.ApplyOperation.Failed, Error: failErr},
			{Shard: "80-", Emoji: "⏸", Label: "halted — -40 failed", State: state.ApplyOperation.Pending},
		},
		Cells: []ShardCell{mutesCell("-40"), mutesCell("80-")},
	})

	assert.Contains(t, out, "❌ Schema Change Failed")
	assert.Contains(t, out, "> ⚠️ **First failure:** shard <code>-40</code> — "+failErr)
	assert.Contains(t, out, failErr, "the error also appears in the failed shard's row")
	assert.Contains(t, out, "To retry:")
}

// An auto-retrying (failed_retryable) shard surfaces its error and the apply
// offers the stop-retrying action, matching the single-deployment footer.
func TestRenderShardedApplyComment_FailedRetryableSurfacesErrorAndStop(t *testing.T) {
	const retryErr = "lost connection to shard primary; retrying"
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.FailedRetryable, Environment: "staging", Database: "cdb_resolute",
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "🔁", Label: "retrying", State: state.ApplyOperation.FailedRetryable, Error: retryErr},
			{Shard: "80-", Emoji: "⏳", Label: "queued — next in order", State: state.ApplyOperation.Pending},
		},
		Cells: []ShardCell{mutesCell("-40"), mutesCell("80-")},
	})

	assert.Contains(t, out, "First failure:", "a retrying shard's error is still lifted")
	assert.Contains(t, out, retryErr, "the retrying shard's error is shown, not dropped")
	assert.Contains(t, out, "To stop retrying:")
	assert.Contains(t, out, "schemabot stop apply-x")
}

// When shards diverge, they are grouped by change signature: each group lists
// its shards' statuses next to the exact DDL it runs. The same table computing
// to different DDL across shards yields two groups.
func TestRenderShardedApplyComment_DivergentGroupsByVariant(t *testing.T) {
	const driftDDL = "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`), ADD COLUMN `reason` varchar(255);"
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "cdb_resolute",
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "🔄", Label: "running table copy", State: state.ApplyOperation.Running},
			{Shard: "40-80", Emoji: "⏳", Label: "queued — next in order", State: state.ApplyOperation.Pending},
			{Shard: "80-c0", Emoji: "⏳", Label: "waiting for -40", State: state.ApplyOperation.Pending},
		},
		Cells: []ShardCell{
			mutesCell("-40"),
			{Shard: "40-80", Table: "mutes", DDL: driftDDL},
			mutesCell("80-c0"),
		},
	})

	assert.Contains(t, out, "Shards diverge — grouped by change:")
	assert.Contains(t, out, "**shards `-40`, `80-c0`**", "shards sharing the standard change are one group")
	assert.Contains(t, out, "**shard `40-80`**", "the drifted shard is its own group")
	assert.Contains(t, out, driftDDL)
	assert.Equal(t, 2, strings.Count(out, "```sql"), "one DDL block per group")
}

// A single-shard divergent group degrades cleanly: one group, no spurious
// grouping header for a uniform apply (covered above) — here every shard shares
// the same multi-table change set, so it is still one group.
func TestRenderShardedApplyComment_UniformMultiTableIsOneGroup(t *testing.T) {
	blocks := func(shard string) ShardCell {
		return ShardCell{Shard: shard, Table: "blocks", DDL: "ALTER TABLE `blocks` ADD INDEX `created_at`(`created_at`);"}
	}
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "cdb_resolute",
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "🔄", Label: "running table copy", State: state.ApplyOperation.Running},
			{Shard: "80-", Emoji: "⏳", Label: "queued — next in order", State: state.ApplyOperation.Pending},
		},
		Cells: []ShardCell{mutesCell("-40"), blocks("-40"), mutesCell("80-"), blocks("80-")},
	})

	assert.NotContains(t, out, "grouped by change", "identical multi-table change sets are one group")
	assert.Contains(t, out, "| Shard | Status |")
	require.Equal(t, 2, strings.Count(out, "```sql"), "both tables' DDL shown once")
}
