//go:build integration

package tern

import (
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A cancel can land after the engine's schema change has already completed on
// its backend: the drive that consumes the durable cancel request gets an
// already-completed rejection from the engine. The drive must adopt the
// engine's authoritative outcome — settle the apply and its tasks to completed,
// not cancelled — and complete the durable cancel request. Without the settle
// the rejection would surface as a retryable drive error, the request would
// stay pending, and the operator would re-claim the apply and re-run the
// doomed cancel on every poll forever.
func TestLocalClient_CancelAfterEngineChangeCompletedSettlesCompleted(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	client, eng := newTasklessControlClient(t, dsn, stor)
	eng.cancelErr = engine.NewAlreadyCompletedError("cancel deploy request #121 rejected: the deploy request completed before the cancel arrived")

	apply := dispatchQueuedApply(t, stor, client, []storage.TableChange{{
		Namespace: "testdb",
		Table:     "users",
		DDL:       "ALTER TABLE `users` ADD COLUMN completed_note VARCHAR(255)",
		Operation: "alter",
	}})

	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 1, "the dispatched apply must carry its task")
	tasks[0].State = state.Task.WaitingForDeploy
	require.NoError(t, stor.Tasks().Update(ctx, tasks[0]),
		"the task must hold live engine work so the cancel reaches the engine")

	cancelResp, err := client.Cancel(ctx, &ternv1.CancelRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.True(t, cancelResp.Accepted)
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationCancel, storage.ControlRequestPending)

	driveNextQueuedApply(t, stor, client)

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Completed, settled.State,
		"the apply must adopt the engine's completed outcome, not cancelled")
	assert.NotNil(t, settled.CompletedAt, "a completed apply must carry its completion time")

	settledTasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, settledTasks, 1)
	assert.Equal(t, state.Task.Completed, settledTasks[0].State,
		"the task must adopt the engine's completed outcome, not cancelled")
	assert.Equal(t, 100, settledTasks[0].ProgressPercent, "a completed task reports full progress")
	assert.NotNil(t, settledTasks[0].CompletedAt, "a completed task must carry its completion time")

	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationCancel, storage.ControlRequestCompleted)
	assert.Contains(t, eng.recorded(), "Cancel", "the drive must consult the engine before settling")

	reclaimed, err := stor.Applies().FindNextApply(ctx, "test-reclaim-"+t.Name())
	require.NoError(t, err)
	assert.Nil(t, reclaimed, "a completed apply must not be claimable again")
}
