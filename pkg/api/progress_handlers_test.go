package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// A table's ETA travels from the progress proto through the HTTP response and
// its JSON encoding unchanged, so CLI and API consumers see the same figure
// the driver stored.
func TestProgressResponseFromProtoCarriesTableETA(t *testing.T) {
	resp := progressResponseFromProto(&ternv1.ProgressResponse{
		State:  ternv1.State_STATE_RUNNING,
		Engine: ternv1.Engine_ENGINE_SPIRIT,
		Tables: []*ternv1.TableProgress{
			{
				TableName:       "users",
				Namespace:       "testdb",
				Status:          "running",
				RowsCopied:      250000,
				RowsTotal:       500000,
				PercentComplete: 50,
				EtaSeconds:      540,
			},
		},
	})

	require.Len(t, resp.Tables, 1)
	assert.Equal(t, int64(540), resp.Tables[0].ETASeconds)

	encoded, err := json.Marshal(resp)
	require.NoError(t, err)
	var decoded apitypes.ProgressResponse
	require.NoError(t, json.Unmarshal(encoded, &decoded))

	require.Len(t, decoded.Tables, 1)
	assert.Equal(t, "users", decoded.Tables[0].TableName)
	assert.Equal(t, state.Apply.Running, decoded.State)
	assert.Equal(t, int64(250000), decoded.Tables[0].RowsCopied)
	assert.Equal(t, int64(500000), decoded.Tables[0].RowsTotal)
	assert.Equal(t, int32(50), decoded.Tables[0].PercentComplete)
	assert.Equal(t, int64(540), decoded.Tables[0].ETASeconds)
}

// A progress read against a gRPC-mode apply with stale stored task state syncs
// the remote per-table snapshot into local task records. The failed table's own
// engine error must be mirrored onto its stored task row — the record the PR
// comment and CLI render a failure reason from — while an empty remote error
// never wipes a stored one: the stored message may have been stamped from the
// apply-level error, or the data plane may run an older proto that omits the
// field entirely.
func TestSyncTasksFromTernMirrorsPerTableError(t *testing.T) {
	apply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-1",
		ExternalID:      "remote-1",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		Deployment:      DefaultDeployment,
	}
	failedTask := &storage.Task{
		ID:             1,
		TaskIdentifier: "task-1",
		ApplyID:        apply.ID,
		Namespace:      "testdb",
		TableName:      "users",
		State:          state.Task.Running,
	}
	stampedTask := &storage.Task{
		ID:             2,
		TaskIdentifier: "task-2",
		ApplyID:        apply.ID,
		Namespace:      "testdb",
		TableName:      "orders",
		State:          state.Task.Running,
		ErrorMessage:   "stamped from apply-level error",
	}
	client := &mockTernClient{
		isRemote: true,
		progressResp: &ternv1.ProgressResponse{
			State: ternv1.State_STATE_FAILED,
			Tables: []*ternv1.TableProgress{
				{Namespace: "testdb", TableName: "users", Status: state.Task.Failed, ErrorMessage: "Duplicate entry 'a' for key 'uniq_name'"},
				{Namespace: "testdb", TableName: "orders", Status: state.Task.Running},
			},
		},
	}
	svc := New(&mockStorageWithApplyStores{
		tasks: &capturingTaskStore{tasks: []*storage.Task{failedTask, stampedTask}},
	}, testServerConfig(), map[string]tern.Client{DefaultDeployment + "/staging": client}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	require.NoError(t, svc.syncTasksFromTern(t.Context(), apply, []*storage.Task{failedTask, stampedTask}))

	assert.Equal(t, state.Task.Failed, failedTask.State)
	assert.Equal(t, "Duplicate entry 'a' for key 'uniq_name'", failedTask.ErrorMessage,
		"failed task should carry its own engine error mirrored from the remote per-table progress")
	assert.Equal(t, state.Task.Running, stampedTask.State)
	assert.Equal(t, "stamped from apply-level error", stampedTask.ErrorMessage,
		"an empty remote per-table error must not wipe a stored one")
}
