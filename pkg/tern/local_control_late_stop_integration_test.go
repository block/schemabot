//go:build integration

package tern

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// addTaskOnAnotherDatabase records another task for the apply on a database
// this client does not serve, so the client's stop cannot see or target it.
func addTaskOnAnotherDatabase(t *testing.T, stor storage.Storage, task *storage.Task, database, taskState string) {
	t.Helper()
	elsewhere := *task
	elsewhere.ID = 0
	elsewhere.TaskIdentifier = task.TaskIdentifier + "-" + database
	elsewhere.Database = database
	elsewhere.State = taskState
	elsewhere.CompletedAt = nil
	_, err := stor.Tasks().Create(t.Context(), &elsewhere)
	require.NoError(t, err)
}

func stopAsOperator(t *testing.T, client *LocalClient, apply *storage.Apply) (*ternv1.StopResponse, error) {
	t.Helper()
	return client.stopOwnedApply(t.Context(), &ternv1.StopRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	}, "operator")
}

// An operator stops an apply whose task on this database has finished, while
// the same apply still has a task running on another database that this client
// cannot see. Recording an outcome from the finished task alone would leave
// that task running under a finished apply that no later stop can reach, so the
// stop fails closed with an error, as cancel does, instead of reporting the
// apply finished: the apply stays running and the running task is left alone.
// The second case adds a stopped task on a third database: the tasks then
// derive stopped, which looks finished, and the running task must still keep
// the stop from recording it.
func TestLocalClient_StopFailsClosedWhenTheApplyStillHasActiveTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testCases := []struct {
		name            string
		withStoppedTask bool
	}{
		{name: "running task elsewhere"},
		{name: "running task elsewhere behind a stopped one", withStoppedTask: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, dsn := setupMySQLContainer(t)
			setupStorageSchema(t, dsn)
			cleanupTasks(t, dsn)
			cleanupTestTables(t, dsn)

			ctx := t.Context()
			stor := createStorage(t, dsn)
			defer utils.CloseAndLog(stor)
			client, eng := newTasklessControlClient(t, dsn, stor)

			apply, task := dispatchApplyWithCompletedTask(t, stor, client, "unfinished_stop_note")
			addTaskOnAnotherDatabase(t, stor, task, "otherdb", state.Task.Running)
			if tc.withStoppedTask {
				addTaskOnAnotherDatabase(t, stor, task, "stoppeddb", state.Task.Stopped)
			}
			apply.State = state.Apply.Running
			require.NoError(t, stor.Applies().Update(ctx, apply))

			resp, err := stopAsOperator(t, client, apply)
			require.Error(t, err)
			assert.Nil(t, resp)
			assert.Contains(t, err.Error(), "stop cannot settle apply "+apply.ApplyIdentifier)
			assert.Contains(t, err.Error(), "on database otherdb is running")

			current, err := stor.Applies().Get(ctx, apply.ID)
			require.NoError(t, err)
			require.NotNil(t, current)
			assert.Equal(t, state.Apply.Running, current.State, "the apply must not finish over a task still running elsewhere")
			assert.Nil(t, current.CompletedAt)
			tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
			require.NoError(t, err)
			for _, got := range tasks {
				if got.Database == "otherdb" {
					assert.Equal(t, state.Task.Running, got.State, "the stop must not touch a task it cannot see")
				}
			}
			assert.NotContains(t, eng.recorded(), "Stop", "the finished task has no engine work to stop")
		})
	}
}

// An operator stops an apply whose task on this database has finished, and
// whose only other task, on another database, is already stopped. Nothing is
// running, so the stop records the apply as stopped and is accepted.
func TestLocalClient_StopRecordsStoppedWhenTheOnlyUnfinishedTaskIsStopped(t *testing.T) {
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

	apply, task := dispatchApplyWithCompletedTask(t, stor, client, "stopped_sibling_note")
	addTaskOnAnotherDatabase(t, stor, task, "otherdb", state.Task.Stopped)
	apply.State = state.Apply.Running
	require.NoError(t, stor.Applies().Update(ctx, apply))

	resp, err := stopAsOperator(t, client, apply)
	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(0), resp.StoppedCount)
	assert.Equal(t, int64(1), resp.SkippedCount)
	assert.Equal(t, "Schema change already stopped", resp.ErrorMessage)

	current, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, current)
	assert.Equal(t, state.Apply.Stopped, current.State)
}

// An operator's stop finds the apply's task on this database finished and
// derives an outcome, but another writer finishes the apply between the stop's
// read of the apply and its write. Storage refuses the stop's write, and the
// stop is accepted and reports the outcome that was recorded, so its durable
// request completes instead of retrying against an outcome that can no longer
// change. The first case derives completed and loses to a failed outcome; the
// second derives stopped, because its other task is stopped, and loses to a
// completed one.
func TestLocalClient_StopThatLosesTheRaceToFinishTheApplyReportsTheRecordedOutcome(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	testCases := []struct {
		name             string
		siblingState     string
		recordedState    string
		recordedError    string
		wantErrorMessage string
	}{
		{
			name:             "derived completed loses to failed",
			recordedState:    state.Apply.Failed,
			recordedError:    "rollout failed on another target",
			wantErrorMessage: "Schema change already failed",
		},
		{
			name:             "derived stopped loses to completed",
			siblingState:     state.Task.Stopped,
			recordedState:    state.Apply.Completed,
			wantErrorMessage: "Schema change already completed",
		},
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cleanupTasks(t, dsn)
			cleanupTestTables(t, dsn)

			ctx := t.Context()
			inner := createStorage(t, dsn)
			defer utils.CloseAndLog(inner)
			stor := &settleBeforeDeriveStorage{Storage: inner, tasks: &settleBeforeDeriveTaskStore{TaskStore: inner.Tasks()}}
			client, _ := newTasklessControlClient(t, dsn, stor)

			apply, task := dispatchApplyWithCompletedTask(t, stor, client, "race_stop_note")
			if tc.siblingState != "" {
				addTaskOnAnotherDatabase(t, inner, task, "otherdb", tc.siblingState)
			}
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
				finishedAt := time.Now()
				other.State = tc.recordedState
				other.ErrorMessage = tc.recordedError
				other.CompletedAt = &finishedAt
				return inner.Applies().Update(ctx, other)
			})

			resp, err := stopAsOperator(t, client, apply)
			require.NoError(t, err, "a stop that loses the race to finish the apply resolves instead of retrying")
			assert.True(t, resp.Accepted)
			assert.Equal(t, int64(0), resp.StoppedCount)
			assert.Equal(t, int64(1), resp.SkippedCount)
			assert.Equal(t, tc.wantErrorMessage, resp.ErrorMessage, "the stop reports the outcome that was recorded")

			current, err := stor.Applies().Get(ctx, apply.ID)
			require.NoError(t, err)
			require.NotNil(t, current)
			assert.Equal(t, tc.recordedState, current.State, "the first recorded outcome stands")
			assert.Equal(t, tc.recordedError, current.ErrorMessage)
		})
	}
}
