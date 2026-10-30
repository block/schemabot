package tern

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A task whose start write is refused keeps the values it arrived with, so
// nothing downstream reads a running task storage does not hold. Tasks written
// before it stay running, and the error names the refused task and its apply.
func TestMarkTasksRunningRestoresRefusedTask(t *testing.T) {
	storeErr := errors.New("storage unavailable")
	apply := &storage.Apply{ID: 42, ApplyIdentifier: "apply-mark-running", Database: "testdb"}
	tasks := []*storage.Task{
		{ID: 1, ApplyID: apply.ID, TaskIdentifier: "task-orders", TableName: "orders", State: state.Task.Pending},
		{ID: 2, ApplyID: apply.ID, TaskIdentifier: "task-users", TableName: "users", State: state.Task.Pending},
	}
	client := &LocalClient{
		storage: &exactProgressStorage{tasks: &exactProgressTaskStore{updateErr: func(_ context.Context, task *storage.Task) error {
			if task.TaskIdentifier == "task-users" {
				return storeErr
			}
			return nil
		}}},
		logger: slog.Default(),
	}

	err := client.markTasksRunning(t.Context(), apply, tasks)

	require.ErrorIs(t, err, storeErr)
	assert.Contains(t, err.Error(), "task-users")
	assert.Contains(t, err.Error(), "apply-mark-running")
	assert.Equal(t, state.Task.Running, tasks[0].State)
	assert.NotNil(t, tasks[0].StartedAt)
	assert.Equal(t, state.Task.Pending, tasks[1].State, "the refused task is not left running")
	assert.Nil(t, tasks[1].StartedAt)
}
