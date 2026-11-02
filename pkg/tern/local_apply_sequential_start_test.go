package tern

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// runningRefusingTaskStore keeps task rows by value, the way a real store does,
// and refuses every write that records a task running with err, accepting every
// other write.
type runningRefusingTaskStore struct {
	storage.TaskStore
	stored map[string]storage.Task
	err    error
}

func newRunningRefusingTaskStore(err error, tasks ...*storage.Task) *runningRefusingTaskStore {
	s := &runningRefusingTaskStore{stored: map[string]storage.Task{}, err: err}
	for _, task := range tasks {
		s.stored[task.TaskIdentifier] = *task
	}
	return s
}

func (s *runningRefusingTaskStore) Get(_ context.Context, taskIdentifier string) (*storage.Task, error) {
	stored, ok := s.stored[taskIdentifier]
	if !ok {
		return nil, nil
	}
	return &stored, nil
}

func (s *runningRefusingTaskStore) GetByApplyID(context.Context, int64) ([]*storage.Task, error) {
	tasks := make([]*storage.Task, 0, len(s.stored))
	for _, stored := range s.stored {
		tasks = append(tasks, &stored)
	}
	return tasks, nil
}

func (s *runningRefusingTaskStore) Update(_ context.Context, task *storage.Task) error {
	if state.IsState(task.State, state.Task.Running) {
		return s.err
	}
	s.stored[task.TaskIdentifier] = *task
	return nil
}

// startCountingEngine accepts every Apply, counting the calls, and records the
// owner of every halt.
type startCountingEngine struct {
	*haltRecordingControlEngine
	applyCalls int
}

func (e *startCountingEngine) Apply(ctx context.Context, req *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.applyCalls++
	return e.fakeControlEngine.Apply(ctx, req)
}

// sequentialStartFixture builds a fresh sequential Spirit drive over two tables
// whose engine accepts every task and reports completed on its first poll, so a
// drive that goes on to poll settles the first table and starts the second.
func sequentialStartFixture(taskWriteErr error) (*LocalClient, *storage.Apply, []*storage.Task, *snapshotApplyStore, *runningRefusingTaskStore, *startCountingEngine) {
	apply := &storage.Apply{
		ID: 1, ApplyIdentifier: "apply-sequential-start", Database: "orders",
		DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging", State: state.Apply.Pending,
	}
	tasks := []*storage.Task{
		{ID: 1, ApplyID: apply.ID, TaskIdentifier: "task-line-items", Database: "orders", Namespace: "orders", TableName: "line_items",
			DDL: "ALTER TABLE `line_items` ADD COLUMN `sku` varchar(64)", DDLAction: "alter", State: state.Task.Pending},
		{ID: 2, ApplyID: apply.ID, TaskIdentifier: "task-shipments", Database: "orders", Namespace: "orders", TableName: "shipments",
			DDL: "ALTER TABLE `shipments` ADD COLUMN `carrier` varchar(64)", DDLAction: "alter", State: state.Task.Pending},
	}
	applies := &snapshotApplyStore{stored: *apply}
	taskStore := newRunningRefusingTaskStore(taskWriteErr, tasks...)
	eng := &startCountingEngine{haltRecordingControlEngine: &haltRecordingControlEngine{
		fakeControlEngine: &fakeControlEngine{progressResult: &engine.ProgressResult{State: engine.StateCompleted, Message: "Complete"}},
	}}
	client := &LocalClient{
		config:       LocalConfig{Database: "orders", Type: storage.DatabaseTypeMySQL},
		spiritEngine: eng,
		storage: &exactProgressStorage{
			applies:         applies,
			tasks:           taskStore,
			controlRequests: &testControlRequestStore{},
			applyOperations: &exactProgressApplyOperationStore{},
			logs:            &mockApplyLogStore{},
		},
		heartbeatInterval:        time.Hour,
		taskPollIntervalOverride: time.Millisecond,
		logger:                   slog.Default(),
	}
	return client, apply, tasks, applies, taskStore, eng
}

// The engine has accepted the first table's work, so a storage failure writing
// the task as running is storage uncertainty, not a failed schema change. The
// drive stops before polling, rests the task failed_retryable so the apply
// pauses for operator retry rather than driving on over a task storage does not
// hold as running, starts no further table, and halts the accepted engine work
// under its own claim as it returns.
func TestExecuteApplySequential_TaskStartWriteFailurePausesForRetry(t *testing.T) {
	client, apply, tasks, applies, taskStore, eng := sequentialStartFixture(errors.New("storage unavailable"))
	drive := driveContext(t.Context(), "token-a")

	require.NoError(t, client.executeApplySequential(drive, apply, tasks, &storage.Plan{}, nil))

	storedApply, err := applies.Get(t.Context(), apply.ID)
	require.NoError(t, err)
	storedFirst, err := taskStore.Get(t.Context(), tasks[0].TaskIdentifier)
	require.NoError(t, err)

	assert.True(t, state.IsState(storedFirst.State, state.Task.FailedRetryable),
		"the first table must pause for operator retry, got %s", storedFirst.State)
	assert.Contains(t, storedFirst.ErrorMessage, "failed to record task running")
	assert.Contains(t, storedFirst.ErrorMessage, "mark task task-line-items running for apply apply-sequential-start")
	assert.True(t, state.IsState(storedApply.State, state.Apply.FailedRetryable),
		"the apply must pause for operator retry, got %s", storedApply.State)
	assert.Equal(t, 1, eng.applyCalls, "no further table's DDL starts after the refused start write")
	assert.Nil(t, eng.progressReq, "a drive that could not record the start never polls the engine")
	assert.Equal(t, []string{driveWorkOwner(drive)}, eng.haltOwners, "the accepted work is halted under the drive's own owner")
}

// A task start write refused because another driver holds the lease is not the
// schema change's failure and not this driver's to settle: the drive stands
// down, leaves the apply and its tasks as stored for the lease holder, and
// halts the engine work it started under its own claim.
func TestExecuteApplySequential_TaskStartWriteLeaseLostStandsDown(t *testing.T) {
	leaseLost := fmt.Errorf("apply apply-sequential-start lease taken by another driver: %w", storage.ErrApplyLeaseLost)
	client, apply, tasks, applies, taskStore, eng := sequentialStartFixture(leaseLost)
	drive := driveContext(t.Context(), "token-a")

	require.NoError(t, client.executeApplySequential(drive, apply, tasks, &storage.Plan{}, nil))

	storedApply, err := applies.Get(t.Context(), apply.ID)
	require.NoError(t, err)
	assert.True(t, state.IsState(storedApply.State, state.Apply.Running),
		"the apply stays as stored for the lease holder, got %s", storedApply.State)
	for _, task := range tasks {
		stored, err := taskStore.Get(t.Context(), task.TaskIdentifier)
		require.NoError(t, err)
		assert.True(t, state.IsState(stored.State, state.Task.Pending),
			"task %s is not recorded running or settled, got %s", task.TableName, stored.State)
		assert.Empty(t, stored.ErrorMessage)
	}
	assert.True(t, state.IsState(tasks[0].State, state.Task.Pending), "the drive's own task does not claim a start storage refused")
	assert.Nil(t, tasks[0].StartedAt)
	assert.Equal(t, 1, eng.applyCalls, "a displaced drive starts no further table")
	assert.Nil(t, eng.progressReq, "a displaced drive never polls the engine")
	assert.Equal(t, []string{driveWorkOwner(drive)}, eng.haltOwners, "the accepted work is halted under the drive's own owner")
}
