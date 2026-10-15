package tern

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A sharded task must reach the engine with exactly one target shard, or a
// sharded engine (Strata) rejects it with "expected exactly one target shard,
// got 0". The shard is also placed on the SchemaChange.
func TestSequentialEngineApplyRequest_PropagatesShard(t *testing.T) {
	task := &storage.Task{
		TaskIdentifier: "task-1", Database: "cdb_resolute", Namespace: "cdb_resolute_sharded",
		TableName: "mutes", Shard: "-40", DDL: "ALTER TABLE `mutes` ADD INDEX `created_at` (`created_at`)",
	}

	req := sequentialEngineApplyRequest("apply-1", task, nil, nil, nil)

	assert.Equal(t, "apply-1", req.ApplyIdentifier, "the engine learns which apply the work belongs to")
	require.Equal(t, []string{"-40"}, req.TargetShards, "the task's shard must reach the engine as a single target shard")
	require.Len(t, req.Changes, 1)
	assert.Equal(t, "-40", req.Changes[0].Shard.Name, "the shard is also set on the SchemaChange")
	require.Len(t, req.Changes[0].TableChanges, 1)
	assert.Equal(t, "mutes", req.Changes[0].TableChanges[0].Table)
	assert.Equal(t, task.DDL, req.Changes[0].TableChanges[0].DDL)
}

// requestRecordingEngine accepts every Apply, keeps the request it was handed,
// and reports the work completed on the first poll.
type requestRecordingEngine struct {
	engine.Engine
	requests []*engine.ApplyRequest
}

func (e *requestRecordingEngine) Name() string { return "request-recording" }

func (e *requestRecordingEngine) Apply(_ context.Context, req *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.requests = append(e.requests, req)
	return &engine.ApplyResult{Accepted: true}, nil
}

func (e *requestRecordingEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	return &engine.ProgressResult{State: engine.StateCompleted, Progress: 100}, nil
}

// A sequential drive hands the engine the apply's own identifier with each
// task, not the task's. The engine keys the target's quarantine log by it, so
// a DROP replayed by a later drive of the same apply, in another process, can
// recognize the copies this drive moved into pending drops.
func TestExecuteApplySequential_EngineRequestNamesTheApply(t *testing.T) {
	task := &storage.Task{
		ID: 1, ApplyID: 1, TaskIdentifier: "task-drop-orders",
		Database: "shop", Namespace: "shop", TableName: "orders", State: state.Task.Pending,
		DDL: "DROP TABLE `orders`",
	}
	apply := &storage.Apply{
		ID: 1, ApplyIdentifier: "apply-shop-7", Database: "shop",
		DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging", State: state.Apply.Pending,
	}
	eng := &requestRecordingEngine{}
	client := &LocalClient{
		config:            LocalConfig{Database: "shop", Type: storage.DatabaseTypeMySQL},
		spiritEngine:      eng,
		heartbeatInterval: time.Hour,
		storage: &exactProgressStorage{
			applies:         &snapshotApplyStore{stored: *apply},
			tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
			controlRequests: &testControlRequestStore{},
			logs:            &mockApplyLogStore{},
		},
		logger: slog.Default(),
	}

	require.NoError(t, client.executeApplySequential(t.Context(), apply, []*storage.Task{task}, &storage.Plan{}, nil))

	require.Len(t, eng.requests, 1, "one engine call per task")
	assert.Equal(t, "apply-shop-7", eng.requests[0].ApplyIdentifier, "the engine learns which apply the task belongs to")
	require.NotNil(t, eng.requests[0].ResumeState)
	assert.Equal(t, "task-drop-orders", eng.requests[0].ResumeState.MigrationContext, "the task still keys the engine's resume state")
}

// A non-sharded task leaves the shard fields unset, so the non-sharded engine
// path is unchanged.
func TestSequentialEngineApplyRequest_NonShardedUnchanged(t *testing.T) {
	task := &storage.Task{
		TaskIdentifier: "task-1", Database: "testdb", Namespace: "testdb",
		TableName: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
	}

	req := sequentialEngineApplyRequest("apply-1", task, nil, nil, nil)

	assert.Empty(t, req.TargetShards, "a non-sharded task targets no shard")
	require.Len(t, req.Changes, 1)
	assert.Equal(t, engine.Shard{}, req.Changes[0].Shard, "no shard on the SchemaChange")
}
