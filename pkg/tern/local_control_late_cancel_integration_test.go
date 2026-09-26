//go:build integration

package tern

import (
	"context"
	"fmt"
	"sync"
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

// A driver records its only task failed and exits before it writes the apply
// row, and an operator's cancel arrives. The cancel stopped no work, so the
// apply records the failure its task reached, carrying the task's error so the
// operator can triage from the apply record, rather than a cancelled outcome
// that would hide the failure.
func TestLocalClient_CancelAfterTaskFailedRecordsTheFailure(t *testing.T) {
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

	apply, task := dispatchApplyWithCompletedTask(t, stor, client, "failed_cancel_note")
	task.State = state.Task.Failed
	task.ErrorMessage = "Duplicate column name 'failed_cancel_note'"
	require.NoError(t, stor.Tasks().Update(ctx, task))
	apply.State = state.Apply.Running
	require.NoError(t, stor.Applies().Update(ctx, apply))

	resp, err := client.cancelOwnedApply(ctx, &ternv1.CancelRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	}, "operator")
	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(0), resp.CancelledCount)
	assert.Equal(t, int64(1), resp.SkippedCount)
	assert.Equal(t, "Schema change already failed", resp.ErrorMessage)

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Failed, settled.State, "the apply records the failure its task reached")
	assert.Equal(t, "table users failed: Duplicate column name 'failed_cancel_note'", settled.ErrorMessage,
		"the apply carries its failed task's error")
	assert.NotNil(t, settled.CompletedAt)
}

// An operator cancels an apply whose tasks on this database have all finished,
// while the same apply still has a task running on another database that this
// client cannot see. Settling the apply from the finished tasks alone would
// record an outcome over live work, so the cancel fails closed: the apply stays
// running and the running task is left alone.
func TestLocalClient_CancelFailsClosedWhenTheApplyStillHasUnfinishedTasks(t *testing.T) {
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

	apply, task := dispatchApplyWithCompletedTask(t, stor, client, "unfinished_cancel_note")
	elsewhere := *task
	elsewhere.ID = 0
	elsewhere.TaskIdentifier = task.TaskIdentifier + "-elsewhere"
	elsewhere.Database = "otherdb"
	elsewhere.State = state.Task.Running
	elsewhere.CompletedAt = nil
	_, err := stor.Tasks().Create(ctx, &elsewhere)
	require.NoError(t, err)
	apply.State = state.Apply.Running
	require.NoError(t, stor.Applies().Update(ctx, apply))

	resp, err := client.cancelOwnedApply(ctx, &ternv1.CancelRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	}, "operator")
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "cancel cannot settle apply "+apply.ApplyIdentifier)
	assert.Contains(t, err.Error(), "its tasks derive running")

	current, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, current)
	assert.Equal(t, state.Apply.Running, current.State, "the apply must not settle over a task still running elsewhere")
	assert.Nil(t, current.CompletedAt)
	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	for _, got := range tasks {
		if got.Database == "otherdb" {
			assert.Equal(t, state.Task.Running, got.State, "the cancel must not touch a task it cannot see")
		}
	}
	assert.NotContains(t, eng.recorded(), "Cancel", "the finished task has no engine work to cancel")
}

// settleBeforeDeriveTaskStore settles the apply through the underlying store the
// first time the apply's tasks are loaded after it is armed, standing in for
// another writer that records the outcome between the cancel's read of the
// apply and its write.
type settleBeforeDeriveTaskStore struct {
	storage.TaskStore
	mu     sync.Mutex
	settle func(ctx context.Context, applyID int64) error
}

func (s *settleBeforeDeriveTaskStore) arm(settle func(ctx context.Context, applyID int64) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settle = settle
}

func (s *settleBeforeDeriveTaskStore) GetByApplyID(ctx context.Context, applyID int64) ([]*storage.Task, error) {
	s.mu.Lock()
	settle := s.settle
	s.settle = nil
	s.mu.Unlock()
	if settle != nil {
		if err := settle(ctx, applyID); err != nil {
			return nil, fmt.Errorf("settle apply %d before its tasks are read: %w", applyID, err)
		}
	}
	return s.TaskStore.GetByApplyID(ctx, applyID)
}

type settleBeforeDeriveStorage struct {
	storage.Storage
	tasks *settleBeforeDeriveTaskStore
}

func (s *settleBeforeDeriveStorage) Tasks() storage.TaskStore { return s.tasks }

// An operator's cancel finds the apply's only task completed and derives a
// completed outcome, but another writer settles the apply as failed between the
// cancel's read of the apply and its write. The storage guard refuses the
// cancel's different outcome, and the cancel is accepted and reports the
// failure that was recorded, so its durable request completes instead of
// retrying against an outcome that can no longer change.
func TestLocalClient_CancelThatLosesTheSettleRaceReportsTheRecordedOutcome(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	inner := createStorage(t, dsn)
	defer utils.CloseAndLog(inner)
	stor := &settleBeforeDeriveStorage{Storage: inner, tasks: &settleBeforeDeriveTaskStore{TaskStore: inner.Tasks()}}
	client, _ := newTasklessControlClient(t, dsn, stor)

	apply, _ := dispatchApplyWithCompletedTask(t, stor, client, "race_cancel_note")
	apply.State = state.Apply.Running
	require.NoError(t, stor.Applies().Update(ctx, apply))

	stor.tasks.arm(func(ctx context.Context, applyID int64) error {
		other, err := inner.Applies().Get(ctx, applyID)
		if err != nil {
			return fmt.Errorf("load apply %d: %w", applyID, err)
		}
		if other == nil {
			return fmt.Errorf("load apply %d: %w", applyID, storage.ErrApplyNotFound)
		}
		settledAt := time.Now()
		other.State = state.Apply.Failed
		other.ErrorMessage = "rollout failed on another target"
		other.CompletedAt = &settledAt
		return inner.Applies().Update(ctx, other)
	})

	resp, err := client.cancelOwnedApply(ctx, &ternv1.CancelRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	}, "operator")
	require.NoError(t, err, "a cancel that loses the race to settle the apply resolves instead of retrying")
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(0), resp.CancelledCount)
	assert.Equal(t, int64(1), resp.SkippedCount)
	assert.Equal(t, "Schema change already failed", resp.ErrorMessage, "the cancel reports the outcome that was recorded")

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Failed, settled.State, "the first recorded outcome stands")
	assert.Equal(t, "rollout failed on another target", settled.ErrorMessage)
}

// An operator cancels a task-less apply that already completed. The cancel is
// accepted and reports the recorded outcome, the same answer a late cancel over
// an apply with tasks gets, and the apply keeps its completed outcome.
func TestLocalClient_LateCancelOnTasklessApplyReportsItsOutcome(t *testing.T) {
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

	apply := dispatchQueuedApply(t, stor, client, nil)
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
	assert.Equal(t, int64(0), resp.SkippedCount)
	assert.Equal(t, "Schema change already completed", resp.ErrorMessage)

	settled, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, settled)
	assert.Equal(t, state.Apply.Completed, settled.State, "a late cancel must not rewrite a completed outcome")
	assert.Empty(t, eng.recorded(), "a task-less apply has no engine work to cancel")
}

// A multi-operation drive holds only its operation lease when it consumes an
// operator's cancel, and every task it would cancel has already finished. The
// parent apply row belongs to the operator's projection, so the drive accepts
// the cancel without writing the parent and leaves the durable request pending
// for the projection to complete once it resolves the stored apply. Writing the
// parent here would be refused under the operation-only lease and turn the
// cancel into a drive error that the claim loop re-runs forever.
func TestLocalClient_CancelAfterTasksFinishedUnderOperationLeaseLeavesParentToProjection(t *testing.T) {
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

	apply, _ := dispatchApplyWithCompletedTask(t, stor, client, "operation_cancel_note")
	op, err := stor.ApplyOperations().FindNextApplyOperation(ctx, "op-driver-"+t.Name())
	require.NoError(t, err)
	require.NotNil(t, op, "the queued apply's operation row must be claimable")
	require.Equal(t, apply.ID, op.ApplyID, "the claimed operation row must belong to the dispatched apply")
	apply.State = state.Apply.Running
	require.NoError(t, stor.Applies().Update(ctx, apply))

	cancelResp, err := client.Cancel(ctx, &ternv1.CancelRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.True(t, cancelResp.Accepted)
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationCancel, storage.ControlRequestPending)

	opCtx := storage.WithOperationLease(ctx, op.Lease())
	reloaded, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, reloaded)
	standDown, err := client.processPendingCancelControlRequest(opCtx, reloaded)
	require.NoError(t, err, "a cancel over finished tasks must not fail the operation drive")
	assert.True(t, standDown, "the drive must consume the pending cancel")

	parent, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, parent)
	assert.Equal(t, state.Apply.Running, parent.State,
		"the apply row belongs to the operator's projection and must not be written under the operation lease alone")
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationCancel, storage.ControlRequestPending)
	assert.NotContains(t, eng.recorded(), "Cancel", "finished tasks have no engine work to cancel")
}
