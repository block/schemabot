package templates

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
)

func TestParseProgressResponseIncludesOperationsAndTableDeployments(t *testing.T) {
	result := &apitypes.ProgressResponse{
		State:  state.Apply.Running,
		Volume: 3,
		Operations: []*apitypes.ProgressOperationResponse{
			{
				Deployment:          "deploy-a",
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

	require.Len(t, data.Operations, 1)
	assert.Equal(t, "deploy-a", data.Operations[0].Deployment)
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
	require.Len(t, data.Tables, 1)
	assert.Equal(t, "deploy-a", data.Tables[0].Deployment)
	assert.Equal(t, state.Task.Running, data.Tables[0].Status)
	assert.Equal(t, 3, data.Volume)
}

// The detail box names the operator-set volume level only while the engine is
// actively working — copying, draining, or verifying, where volume remains
// adjustable — and stays quiet when no volume was ever set or the apply is in
// a state where the level carries no signal.
func TestVolumeBoxRow(t *testing.T) {
	cases := []struct {
		name   string
		volume int
		state  string
		want   string
	}{
		{"running with volume", 3, state.Apply.Running, "3/11"},
		{"running degraded with volume", 5, state.Apply.RunningDegraded, "5/11"},
		{"catching up with volume", 2, state.Apply.CatchingUp, "2/11"},
		{"checksumming with volume", 7, state.Apply.Checksumming, "7/11"},
		{"post checksum with volume", 11, state.Apply.PostChecksum, "11/11"},
		{"running without volume", 0, state.Apply.Running, ""},
		{"stopped with volume", 3, state.Apply.Stopped, ""},
		{"waiting for cutover with volume", 3, state.Apply.WaitingForCutover, ""},
		{"completed with volume", 3, state.Apply.Completed, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row, ok := volumeBoxRow(tc.volume, tc.state)
			if tc.want == "" {
				assert.False(t, ok)
				return
			}
			require.True(t, ok)
			assert.Equal(t, "Volume", row.Label)
			assert.Equal(t, tc.want, row.Value)
		})
	}
}

// The retry row reports how much of the budget the next attempt spends and when
// it becomes eligible, so an operator can tell an apply that is waiting from one
// that is wedged. It appears only while the apply is retrying, and names a time
// only while one is still ahead.
func TestRetryBoxRow(t *testing.T) {
	now := time.Date(2026, 1, 15, 14, 30, 0, 0, time.UTC)
	originalNow, originalZone := nowFunc, localZone
	t.Cleanup(func() { nowFunc, localZone = originalNow, originalZone })
	nowFunc = func() time.Time { return now }
	localZone = time.UTC

	row, ok := retryBoxRow(ProgressData{
		State:      state.Apply.FailedRetryable,
		Attempt:    3,
		RetryAfter: now.Add(90 * time.Second).Format(time.RFC3339),
	})
	require.True(t, ok)
	assert.Equal(t, "Retry", row.Label)
	assert.Equal(t, "attempt 4/10 · next 14:31:30 UTC", row.Value)

	due, ok := retryBoxRow(ProgressData{
		State:      state.Apply.FailedRetryable,
		Attempt:    3,
		RetryAfter: now.Add(-time.Second).Format(time.RFC3339),
	})
	require.True(t, ok)
	assert.Equal(t, "attempt 4/10", due.Value, "an elapsed wait names no time — the retry is due")

	_, ok = retryBoxRow(ProgressData{State: state.Apply.Running, Attempt: 3})
	assert.False(t, ok, "a spent attempt counter carries no signal outside the retrying state")
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
