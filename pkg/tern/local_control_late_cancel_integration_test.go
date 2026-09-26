//go:build integration

package tern

import (
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// dispatchApplyWithCompletedTask dispatches a one-table apply and records its
// task as completed, the shape a driver leaves when its schema change landed.
func dispatchApplyWithCompletedTask(t *testing.T, stor storage.Storage, client *LocalClient, column string) (*storage.Apply, *storage.Task) {
	t.Helper()
	ctx := t.Context()
	apply := dispatchQueuedApply(t, stor, client, []storage.TableChange{{
		Namespace: "testdb",
		Table:     "users",
		DDL:       "ALTER TABLE `users` ADD COLUMN " + column + " VARCHAR(255)",
		Operation: "alter",
	}})

	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 1, "the dispatched apply must carry its task")
	finishedAt := time.Now()
	tasks[0].State = state.Task.Completed
	tasks[0].CompletedAt = &finishedAt
	require.NoError(t, stor.Tasks().Update(ctx, tasks[0]))
	return apply, tasks[0]
}

// An operator cancels an apply whose schema change already landed: its task
// completed and the apply row records completed. The cancel is accepted and
// reports the outcome, and the apply stays completed with its original
// completion time, since the column it added is live on the target and a
// cancelled row would tell the operator otherwise.
func TestLocalClient_LateCancelKeepsCompletedApply(t *testing.T) {
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

	apply, _ := dispatchApplyWithCompletedTask(t, stor, client, "late_cancel_note")
	completedAt := time.Now().Add(-time.Minute).Truncate(time.Second)
	apply.State = state.Apply.Completed
	apply.CompletedAt = &completedAt
	require.NoError(t, stor.Applies().Update(ctx, apply))

	resp, err := client.cancelOwnedApply(ctx, &ternv1.CancelRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	}, "operator")
	require.NoError(t, err)
	assert.True(t, resp.Accepted, "a cancel over a settled apply resolves instead of retrying")
	assert.Equal(t, int64(0), resp.CancelledCount)
	assert.Equal(t, int64(1), resp.SkippedCount)
	assert.Equal(t, "Schema change already completed", resp.ErrorMessage)

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Completed, settled.State, "a late cancel must not rewrite a completed outcome")
	require.NotNil(t, settled.CompletedAt)
	assert.True(t, completedAt.Equal(*settled.CompletedAt), "the completed apply keeps its completion time")
	assert.NotContains(t, eng.recorded(), "Cancel", "a completed task has no engine work to cancel")
}

// A driver finishes its only task and is about to record the apply completed
// when an operator's cancel arrives. The cancel stopped no work, so it records
// the outcome the task reached rather than cancelled, and the driver's own
// completed write that follows still lands. Recording cancelled here would
// settle the apply with an outcome the database never had and refuse the
// driver's completed write.
func TestLocalClient_CancelAfterTasksFinishedRecordsTheirOutcome(t *testing.T) {
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
	client, _ := newTasklessControlClient(t, dsn, stor)

	apply, _ := dispatchApplyWithCompletedTask(t, stor, client, "finished_cancel_note")
	apply.State = state.Apply.Running
	require.NoError(t, stor.Applies().Update(ctx, apply))
	driverCopy := *apply

	resp, err := client.cancelOwnedApply(ctx, &ternv1.CancelRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	}, "operator")
	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(0), resp.CancelledCount)
	assert.Equal(t, int64(1), resp.SkippedCount)
	assert.Equal(t, "Schema change already completed", resp.ErrorMessage)

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Completed, settled.State, "the apply records what its finished task did")
	assert.NotNil(t, settled.CompletedAt)

	finishedAt := time.Now()
	driverCopy.State = state.Apply.Completed
	driverCopy.CompletedAt = &finishedAt
	require.NoError(t, stor.Applies().Update(ctx, &driverCopy), "the driver's completed write agrees with the recorded outcome")
}
