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
func singleShardStrataProgress(singleShard bool) ProgressData {
	return ProgressData{
		ApplyID:     "apply-single-shard",
		Database:    "shop",
		Environment: "staging",
		State:       state.Apply.Completed,
		StartedAt:   "2026-06-16T10:00:00Z",
		CompletedAt: "2026-06-16T10:00:06Z",
		SingleShard: singleShard,
		Operations: []ProgressOperation{
			{Deployment: "data-plane", OperationKey: "shop_001/-/orders", OperationKind: storage.ApplyOperationKindWork, ExternalOperationID: "11", State: state.ApplyOperation.Completed},
			{Deployment: "data-plane", OperationKey: "shop_001/group_finalizer", OperationKind: storage.ApplyOperationKindGroupFinalizer, ExternalOperationID: "12", State: state.ApplyOperation.Completed},
		},
		Tables: []TableProgress{
			{Deployment: "data-plane", Namespace: "shop_001", TableName: "orders", ChangeType: "alter", Dialect: schema.DialectMySQL, DDL: "ALTER TABLE `orders` ADD COLUMN `note` text", Status: state.Task.Completed, PercentComplete: 100},
		},
	}
}

// A sharded apply on a keyspace's only shard, with no VSchema change to show,
// renders like an apply with one operation, as its PR comments do: the table
// once under its keyspace, with no section for the finalizer and no count of
// deployments.
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
func TestWriteProgressShardRowsWithoutSingleShardKeepTheirSections(t *testing.T) {
	output := captureStdout(t, func() { WriteProgress(singleShardStrataProgress(false)) })

	assert.Contains(t, output, "shop_001/group_finalizer")
	assert.Contains(t, output, "Deployments:")
}

// A single-shard apply shows no operation sections, so when only a failed
// finalize recorded why the apply failed, its error stands in for the apply's.
func TestParseProgressResponseSingleShardTakesAFailedOperationsError(t *testing.T) {
	const finalizeErr = "apply VSchema for shop_001: keyspace not found"
	response := func(singleShard bool) *apitypes.ProgressResponse {
		return &apitypes.ProgressResponse{
			State:       state.Apply.Failed,
			SingleShard: singleShard,
			Operations: []*apitypes.ProgressOperationResponse{
				{Deployment: "data-plane", OperationKey: "shop_001/-/orders", State: state.ApplyOperation.Completed},
				{Deployment: "data-plane", OperationKey: "shop_001/group_finalizer", State: state.ApplyOperation.Failed, ErrorMessage: finalizeErr},
			},
		}
	}

	assert.Equal(t, finalizeErr, ParseProgressResponse(response(true)).ErrorMessage)
	assert.Empty(t, ParseProgressResponse(response(false)).ErrorMessage,
		"an apply with operation sections shows the error on the failed section")
}
