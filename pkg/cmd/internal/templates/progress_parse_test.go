package templates

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
)

func TestParseProgressResponseIncludesOperationsAndTableDeployments(t *testing.T) {
	result := &apitypes.ProgressResponse{
		State:       state.Apply.Running,
		Caller:      "github:octocat@acme/shop#412",
		PullRequest: "https://github.com/acme/shop/pull/412",
		Operations: []*apitypes.ProgressOperationResponse{
			{
				Deployment:          "deploy-a",
				OperationKey:        "commerce/-80/users",
				ExternalID:          "remote-apply-a",
				ExternalOperationID: "remote-operation-a",
				Target:              "target-a",
				State:               "STATE_RUNNING",
				CutoverPolicy:       "barrier",
				OnFailure:           "continue",
				ErrorCode:           "engine_error_retryable",
				ErrorMessage:        "retryable failure",
				StartedAt:           "2026-06-16T10:00:00Z",
				CompletedAt:         "2026-06-16T10:05:00Z",
				RolloutStep:         2,
			},
			{
				Deployment:       "deploy-a",
				Target:           "target-b",
				State:            "STATE_COMPLETED",
				AlreadyConverged: true,
			},
		},
		Tables: []*apitypes.TableProgressResponse{
			{
				TableName:  "users",
				Deployment: "deploy-a",
				Keyspace:   "testdb",
				ChangeType: "alter",
				DDL:        "ALTER TABLE users ADD COLUMN email varchar(255)",
				Status:     "STATE_RUNNING",
			},
		},
	}

	data := ParseProgressResponse(result)

	require.Len(t, data.Operations, 2)
	assert.False(t, data.Operations[0].AlreadyConverged)
	assert.True(t, data.Operations[1].AlreadyConverged, "the target that already had the change keeps its mark")
	assert.Equal(t, "deploy-a", data.Operations[0].Deployment)
	assert.Equal(t, "commerce/-80/users", data.Operations[0].OperationKey)
	assert.Equal(t, "remote-apply-a", data.Operations[0].ExternalID)
	assert.Equal(t, "remote-operation-a", data.Operations[0].ExternalOperationID)
	assert.Equal(t, "target-a", data.Operations[0].Target)
	assert.Equal(t, state.Apply.Running, data.Operations[0].State)
	assert.Equal(t, "barrier", data.Operations[0].CutoverPolicy)
	assert.Equal(t, "continue", data.Operations[0].OnFailure)
	assert.Equal(t, "engine_error_retryable", data.Operations[0].ErrorCode)
	assert.Equal(t, "retryable failure", data.Operations[0].ErrorMessage)
	assert.Equal(t, "2026-06-16T10:00:00Z", data.Operations[0].StartedAt)
	assert.Equal(t, "2026-06-16T10:05:00Z", data.Operations[0].CompletedAt)
	assert.Equal(t, 2, data.Operations[0].RolloutStep)
	assert.Equal(t, 0, data.Operations[1].RolloutStep)
	require.Len(t, data.Tables, 1)
	assert.Equal(t, "deploy-a", data.Tables[0].Deployment)
	assert.Equal(t, state.Task.Running, data.Tables[0].Status)
	assert.Equal(t, "github:octocat@acme/shop#412", data.Caller)
	assert.Equal(t, "https://github.com/acme/shop/pull/412", data.PullRequestURL)
}

func TestParseProgressResponseWithoutOperationsKeepsDeploymentEmpty(t *testing.T) {
	result := &apitypes.ProgressResponse{
		State: state.Apply.Completed,
		Tables: []*apitypes.TableProgressResponse{
			{
				TableName:  "users",
				Keyspace:   "testdb",
				ChangeType: "alter",
				DDL:        "ALTER TABLE users ADD COLUMN email varchar(255)",
				Status:     state.Task.Completed,
			},
		},
	}

	data := ParseProgressResponse(result)

	assert.Empty(t, data.Operations)
	require.Len(t, data.Tables, 1)
	assert.Empty(t, data.Tables[0].Deployment)
	assert.Equal(t, "users", data.Tables[0].TableName)
	assert.Equal(t, state.Task.Completed, data.Tables[0].Status)
}

// A table's headline figures — including its ETA and any per-shard ETAs —
// survive the JSON-response-to-render-data mapping, so a renderer's decision
// to show or hide an ETA is a status-gating choice, never a lost value.
func TestParseProgressResponseCarriesTableAndShardETA(t *testing.T) {
	result := &apitypes.ProgressResponse{
		State: state.Apply.Running,
		Tables: []*apitypes.TableProgressResponse{
			{
				TableName:       "users",
				Keyspace:        "testdb",
				ChangeType:      "alter",
				Status:          state.Task.Running,
				RowsCopied:      250000,
				RowsTotal:       500000,
				PercentComplete: 50,
				ETASeconds:      540,
				Shards: []*apitypes.ShardProgressResponse{
					{
						Shard:      "-80",
						Status:     state.Task.Running,
						RowsCopied: 125000,
						RowsTotal:  250000,
						ETASeconds: 480,
					},
				},
			},
		},
	}

	data := ParseProgressResponse(result)

	require.Len(t, data.Tables, 1)
	assert.Equal(t, int64(250000), data.Tables[0].RowsCopied)
	assert.Equal(t, int64(500000), data.Tables[0].RowsTotal)
	assert.Equal(t, 50, data.Tables[0].PercentComplete)
	assert.Equal(t, int64(540), data.Tables[0].ETASeconds)
	require.Len(t, data.Tables[0].Shards, 1)
	assert.Equal(t, "-80", data.Tables[0].Shards[0].Shard)
	assert.Equal(t, int64(480), data.Tables[0].Shards[0].ETASeconds)
}

// A sharded table without its own estimate takes the slowest shard's ETA, and
// a shard reporting rows but no percent gets its percent derived from them —
// clamped to 100, since row totals are estimates a copy can exceed.
func TestParseProgressResponsePromotesSlowestShardETAAndDerivesShardPercent(t *testing.T) {
	result := &apitypes.ProgressResponse{
		State: state.Apply.Running,
		Tables: []*apitypes.TableProgressResponse{
			{
				TableName: "users",
				Keyspace:  "testdb",
				Status:    state.Task.Running,
				Shards: []*apitypes.ShardProgressResponse{
					{Shard: "-80", Status: state.Task.Running, RowsCopied: 125000, RowsTotal: 250000, ETASeconds: 480},
					{Shard: "80-", Status: state.Task.Running, RowsCopied: 60000, RowsTotal: 240000, ETASeconds: 900},
					{Shard: "c0-", Status: state.Task.Running, RowsCopied: 300000, RowsTotal: 250000, ETASeconds: 120},
					{Shard: "40-80", Status: state.Task.Running, RowsCopied: 300000, RowsTotal: 250000, PercentComplete: 120, ETASeconds: 60},
				},
			},
		},
	}

	data := ParseProgressResponse(result)

	require.Len(t, data.Tables, 1)
	assert.Equal(t, int64(900), data.Tables[0].ETASeconds)
	require.Len(t, data.Tables[0].Shards, 4)
	assert.Equal(t, 50, data.Tables[0].Shards[0].PercentComplete)
	assert.Equal(t, 25, data.Tables[0].Shards[1].PercentComplete)
	assert.Equal(t, 100, data.Tables[0].Shards[2].PercentComplete)
	// A server-sent percent that exceeds 100 is clamped the same way as a
	// derived one.
	assert.Equal(t, 100, data.Tables[0].Shards[3].PercentComplete)
}

// Spirit internal tables (shadow, checkpoint, sentinel) are working artifacts
// of the copy, not schema changes: they never reach the rendered table list.
func TestParseProgressResponseFiltersSpiritInternalTables(t *testing.T) {
	result := &apitypes.ProgressResponse{
		State: state.Apply.Running,
		Tables: []*apitypes.TableProgressResponse{
			{TableName: "users", Status: state.Task.Running},
			{TableName: "_users_new", Status: state.Task.Running},
			{TableName: "_spirit_sentinel", Status: state.Task.Running},
		},
	}

	data := ParseProgressResponse(result)

	require.Len(t, data.Tables, 1)
	assert.Equal(t, "users", data.Tables[0].TableName)
}

// The structured copy fields pass through untouched, and a raw engine phase
// string normalizes to its canonical task state.
func TestParseProgressResponseCarriesCopyFieldsAndNormalizesStatus(t *testing.T) {
	result := &apitypes.ProgressResponse{
		State: state.Apply.Running,
		Tables: []*apitypes.TableProgressResponse{
			{
				TableName:       "users",
				Status:          "copyRows",
				RowsCopied:      71436,
				RowsTotal:       221193,
				PercentComplete: 32,
			},
		},
	}

	data := ParseProgressResponse(result)

	require.Len(t, data.Tables, 1)
	assert.Equal(t, state.Task.Running, data.Tables[0].Status)
	assert.Equal(t, int64(71436), data.Tables[0].RowsCopied)
	assert.Equal(t, int64(221193), data.Tables[0].RowsTotal)
	assert.Equal(t, 32, data.Tables[0].PercentComplete)
}

// A sharded apply deferred its cutover and shard -40 failed, so SchemaBot
// stopped 40-80 and 80- at the cutover and failed them for -40 (ended_by).
// Parsed from the progress response's JSON, the CLI counts those shards as
// stopped at cutover rather than failed and notes that their copies are
// saved. A reference to a row the apply no longer lists arrives as an empty
// ended_by ({}): the shard still reads stopped at cutover, without naming the
// shard that failed.
func TestParseProgressResponseCountsShardsStoppedAtCutover(t *testing.T) {
	const response = `{
		"state": "failed",
		"options": {"defer_cutover": "true"},
		"operations": [
			{"deployment": "default", "operation_key": "commerce/-40/orders", "state": "failed", "cutover_policy": "parallel", "error_message": "lock wait timeout exceeded"},
			{"deployment": "default", "operation_key": "commerce/40-80/orders", "state": "failed", "cutover_policy": "parallel", "ended_by": {"deployment": "default", "operation_key": "commerce/-40/orders"}},
			{"deployment": "default", "operation_key": "commerce/80-/orders", "state": "failed", "cutover_policy": "parallel", "ended_by": %s}
		],
		"tables": [{"table_name": "orders", "keyspace": "commerce", "status": "failed", "shards": [
			{"shard": "-40", "status": "failed", "rows_copied": 400, "rows_total": 1000},
			{"shard": "40-80", "status": "stopped", "rows_copied": 1000, "rows_total": 1000},
			{"shard": "80-", "status": "stopped", "rows_copied": 1000, "rows_total": 1000}
		]}]
	}`
	for _, tc := range []struct {
		name          string
		lastEndedBy   string
		wantRef       presentation.OperationRef
		wantCounts    string
		wantNoteCause string
	}{
		{
			name:          "names the failed shard",
			lastEndedBy:   `{"deployment": "default", "operation_key": "commerce/-40/orders"}`,
			wantRef:       presentation.OperationRef{Deployment: "default", OperationKey: "commerce/-40/orders"},
			wantCounts:    "1 failed, 2 stopped at cutover",
			wantNoteCause: "2 shards stopped at the cutover because -40 failed.",
		},
		{
			name:          "an unplaced reference names no shard",
			lastEndedBy:   `{}`,
			wantRef:       presentation.OperationRef{},
			wantCounts:    "1 failed, 2 stopped at cutover",
			wantNoteCause: "2 shards stopped at the cutover because another shard failed.",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var result apitypes.ProgressResponse
			require.NoError(t, json.Unmarshal(fmt.Appendf(nil, response, tc.lastEndedBy), &result))

			data := ParseProgressResponse(&result)

			require.Len(t, data.Operations, 3)
			assert.Nil(t, data.Operations[0].EndedBy)
			require.NotNil(t, data.Operations[2].EndedBy)
			assert.Equal(t, tc.wantRef, *data.Operations[2].EndedBy)
			assert.Equal(t, tc.wantCounts, ShardCounts(data.State, data.Operations, data.Tables, data.Released))
			assert.Equal(t, tc.wantNoteCause+" Their copies are saved, so applying again resumes them.",
				ShardedStoppedAtCutoverNote(data.State, data.Operations, data.Tables, data.Released))
		})
	}
}

// A deferred apply changes orders and users on a keyspace that runs on its
// only shard, and orders failed, so SchemaBot stopped users at the cutover and
// failed it for orders (ended_by). With no shard to count, the CLI counts the
// table instead, and notes that its copy is saved.
func TestParseProgressResponseCountsTablesStoppedAtCutoverOnTheOnlyShard(t *testing.T) {
	const response = `{
		"state": "failed",
		"sharded": true,
		"options": {"defer_cutover": "true"},
		"operations": [
			{"deployment": "default", "operation_key": "commerce/-/orders", "state": "failed", "cutover_policy": "parallel", "error_message": "lock wait timeout exceeded"},
			{"deployment": "default", "operation_key": "commerce/-/users", "state": "failed", "cutover_policy": "parallel", "ended_by": {"deployment": "default", "operation_key": "commerce/-/orders"}}
		],
		"tables": [
			{"table_name": "orders", "keyspace": "commerce", "status": "failed", "rows_copied": 400, "rows_total": 1000},
			{"table_name": "users", "keyspace": "commerce", "status": "stopped", "rows_copied": 1000, "rows_total": 1000}
		]
	}`
	var result apitypes.ProgressResponse
	require.NoError(t, json.Unmarshal([]byte(response), &result))

	data := ParseProgressResponse(&result)

	require.True(t, data.Sharded)
	require.Len(t, data.Operations, 2)
	assert.Empty(t, ShardCounts(data.State, data.Operations, data.Tables, data.Released), "the only shard needs no count")
	assert.Equal(t, "1 table stopped at the cutover because orders failed. Its copy is saved, so applying again resumes it.",
		ShardedStoppedAtCutoverNote(data.State, data.Operations, data.Tables, data.Released))
}
