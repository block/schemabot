package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// singleShardStrataProgress is a completed apply on a keyspace's only shard:
// one work row for the table and the finalizer the engine runs beside it.
func singleShardStrataProgress(sharded bool) ProgressData {
	return ProgressData{
		ApplyID:     "apply-single-shard",
		Database:    "shop",
		Environment: "staging",
		State:       state.Apply.Completed,
		StartedAt:   "2026-06-16T10:00:00Z",
		CompletedAt: "2026-06-16T10:00:06Z",
		Sharded:     sharded,
		Operations: []ProgressOperation{
			{Deployment: "data-plane", OperationKey: "shop_001/-/orders", OperationKind: storage.ApplyOperationKindWork, ExternalOperationID: "11", State: state.ApplyOperation.Completed},
			{Deployment: "data-plane", OperationKey: "shop_001/group_finalizer", OperationKind: storage.ApplyOperationKindGroupFinalizer, ExternalOperationID: "12", State: state.ApplyOperation.Completed},
		},
		Tables: []TableProgress{
			{Deployment: "data-plane", Namespace: "shop_001", TableName: "orders", ChangeType: "alter", Dialect: schema.DialectMySQL, DDL: "ALTER TABLE `orders` ADD COLUMN `note` text", Status: state.Task.Completed, PercentComplete: 100},
		},
	}
}

// A sharded apply on a keyspace's only shard renders like an apply with one
// operation, as its PR comments do: the table once under its keyspace, with
// no section for the finalizer and no count of operation rows.
func TestWriteProgressSingleShardRendersAsOneChange(t *testing.T) {
	output := captureStdout(t, func() { WriteProgress(singleShardStrataProgress(true)) })

	assert.Contains(t, output, "Database:")
	assert.Contains(t, output, "── shop_001 ──")
	assert.Equal(t, 1, strings.Count(output, "~ orders:"), "the table renders once:\n%s", output)
	assert.NotContains(t, output, "group_finalizer")
	assert.NotContains(t, output, "Deployments:")
	assert.NotContains(t, output, "External operation ID")
}

// Without the flag the same rows keep a section per operation, so a server
// that does not report it leaves the layout as it was.
func TestWriteProgressShardRowsWithoutShardedKeepTheirSections(t *testing.T) {
	output := captureStdout(t, func() { WriteProgress(singleShardStrataProgress(false)) })

	assert.Contains(t, output, "shop_001/group_finalizer")
	assert.Contains(t, output, "Deployments:")
}

// A table copying across four shards, of which only the first has started,
// renders as one table with its shards under it, as the PR comment does. Its
// row figures cover the one started shard, so the bar and the rows line say
// so, and the ETA reads as a floor, rather than passing that shard's fraction
// off as the table's, while the planned size names the shards it covers. The
// header counts the shards by status, the finalizer's VSchema change shows
// under the tables, and no line counts the operation rows as deployments.
func TestWriteProgressShardedApplyRollsShardsUpUnderTheTable(t *testing.T) {
	data := ParseProgressResponse(multiShardRunningResponse())
	output := captureStdout(t, func() { WriteProgress(data) })

	assert.Equal(t, 1, strings.Count(output, "~ orders:"), "the table renders once:\n%s", output)
	assert.Contains(t, output, "62.00% (1 of 4 shards)")
	assert.Contains(t, output, "Shards:       1 running table copy, 3 waiting for -40")
	assert.Contains(t, output, "Rows: 620 / 1,000 across 1 of 4 shards · ~23.4 GB across all 4 shards · ETA: ≥ ")
	assert.Contains(t, output, "Shards: 4 (1 copying, 3 queued)")
	assert.Contains(t, output, "◉ -40"+ANSIReset+": 62.00% · 620 / 1,000 rows")
	assert.Contains(t, output, "○ 40-80: queued")
	assert.Contains(t, output, "~ VSchema (shop_001): Pending")
	assert.Contains(t, output, `"orders": {}`)
	assert.NotContains(t, output, "Deployments:")
	assert.NotContains(t, output, "group_finalizer")
}

// Every state that shows a sharded table's copy percentage says which shards
// it covers while only some have started, so a stopped, cancelled, or
// recovering copy of the first shard never reads as the whole table's.
func TestFormatTableProgressNamesThePartialShardCoverageInEveryCopyState(t *testing.T) {
	table := ParseProgressResponse(multiShardRunningResponse()).Tables[0]
	for status, want := range map[string]string{
		state.Apply.Stopped:    "Stopped at 62.00% (1 of 4 shards)",
		state.Apply.Cancelled:  "Cancelled at 62.00% (1 of 4 shards)",
		state.Apply.Recovering: "Row copy in progress (62.00% (1 of 4 shards))",
	} {
		t.Run(status, func(t *testing.T) {
			table.Status = status
			output := FormatTableProgress(table)
			assert.Contains(t, output, want)
			if status == state.Apply.Stopped {
				assert.Contains(t, output, "Rows: 620 / 1,000 across 1 of 4 shards · ~23.4 GB across all 4 shards\n")
			}
		})
	}
}

// A sharded apply shows no operation sections, so when only a failed
// operation recorded why the apply failed, its error stands in for the
// apply's, naming the shard when a shard failed.
func TestParseProgressResponseShardedTakesAFailedOperationsError(t *testing.T) {
	const finalizeErr = "apply VSchema for shop_001: keyspace not found"
	response := func(sharded bool, ops ...*apitypes.ProgressOperationResponse) *apitypes.ProgressResponse {
		return &apitypes.ProgressResponse{State: state.Apply.Failed, Sharded: sharded, Operations: ops}
	}
	completedShard := &apitypes.ProgressOperationResponse{Deployment: "data-plane", OperationKey: "shop_001/-/orders", State: state.ApplyOperation.Completed}
	failedFinalizer := &apitypes.ProgressOperationResponse{Deployment: "data-plane", OperationKey: "shop_001/group_finalizer", State: state.ApplyOperation.Failed, ErrorMessage: finalizeErr}
	failedShard := &apitypes.ProgressOperationResponse{Deployment: "data-plane", OperationKey: "shop_001/-40/orders", State: state.ApplyOperation.Failed, ErrorMessage: "resolve shard primary: context deadline exceeded"}

	assert.Equal(t, finalizeErr, ParseProgressResponse(response(true, completedShard, failedFinalizer)).ErrorMessage)
	assert.Equal(t, "shard -40: resolve shard primary: context deadline exceeded",
		ParseProgressResponse(response(true, failedShard, failedFinalizer)).ErrorMessage)
	assert.Empty(t, ParseProgressResponse(response(false, completedShard, failedFinalizer)).ErrorMessage,
		"an apply with operation sections shows the error on the failed section")
}

// multiShardRunningResponse is a sharded apply copying `orders` across a
// keyspace's four shards, as the server reports it: one table row rolled up
// across the shards, of which only -40 has started, with the table's planned
// size across all four, and the finalizer's pending VSchema change in the
// display metadata.
func multiShardRunningResponse() *apitypes.ProgressResponse {
	ops := []*apitypes.ProgressOperationResponse{
		{Deployment: "data-plane", OperationKey: "shop_001/-40/orders", OperationKind: storage.ApplyOperationKindWork, State: state.ApplyOperation.Running},
		{Deployment: "data-plane", OperationKey: "shop_001/40-80/orders", OperationKind: storage.ApplyOperationKindWork, State: state.ApplyOperation.Pending},
		{Deployment: "data-plane", OperationKey: "shop_001/80-c0/orders", OperationKind: storage.ApplyOperationKindWork, State: state.ApplyOperation.Pending},
		{Deployment: "data-plane", OperationKey: "shop_001/c0-/orders", OperationKind: storage.ApplyOperationKindWork, State: state.ApplyOperation.Pending},
		{Deployment: "data-plane", OperationKey: "shop_001/group_finalizer", OperationKind: storage.ApplyOperationKindGroupFinalizer, State: state.ApplyOperation.Pending},
	}
	vschema, err := apitypes.EncodeVSchemaChanges([]apitypes.VSchemaChange{{Namespace: "shop_001", Diff: "+  \"orders\": {}"}})
	if err != nil {
		panic(err)
	}
	return &apitypes.ProgressResponse{
		ApplyID:      "apply-sharded-a1b2c3d4",
		Database:     "shop",
		DatabaseType: storage.DatabaseTypeMySQL,
		Environment:  "production",
		State:        state.Apply.Running,
		Sharded:      true,
		Operations:   ops,
		Metadata:     map[string]string{apitypes.VSchemaChangesMetadataKey: vschema},
		Tables: []*apitypes.TableProgressResponse{{
			TableName: "orders", Keyspace: "shop_001", Deployment: "data-plane", ChangeType: "alter",
			DDL:    "ALTER TABLE `orders` ADD INDEX `idx_created_at`(`created_at`)",
			Status: state.Task.Running, RowsCopied: 620, RowsTotal: 1000, PercentComplete: 62, ETASeconds: 195,
			EstimatedBytes: new(int64(23_400_000_000)), PlannedShards: 4,
			Shards: []*apitypes.ShardProgressResponse{
				{Shard: "-40", Status: state.Task.Running, RowsCopied: 620, RowsTotal: 1000, PercentComplete: 62, ETASeconds: 195},
				{Shard: "40-80", Status: state.Task.Pending},
				{Shard: "80-c0", Status: state.Task.Pending},
				{Shard: "c0-", Status: state.Task.Pending},
			},
		}},
	}
}
