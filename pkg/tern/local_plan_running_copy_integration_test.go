//go:build integration

package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
)

// A plan tells an operator whether an unfinished copy on the target is still
// being made, and it reads that from the deployment's own task rows rather than
// from the engine's checkpoint. A task in flight is work happening on the
// target now; a task that is pending, stopped, or finished is not.
func TestRunningCopyTablesReportsOnlyWorkInFlight(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newAdoptTestFixture(t, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})

	first := f.dispatch(t, "schemabot:v1:running-copy")
	require.True(t, first.Accepted, "first dispatch must be accepted: %s", first.ErrorMessage)

	inFlight := runningCopyKey{"testdb", "users"}

	for _, tc := range []struct {
		taskState string
		running   bool
		why       string
	}{
		{state.Task.Running, true, "a running task is a copy being made right now"},
		{state.Task.CuttingOver, true, "work past the copy phase is still work on the target"},
		{state.Task.Pending, false, "a task that has not started is copying nothing"},
		{state.Task.Stopped, false, "a stopped copy is one applying really does pick up where it stopped"},
		{state.Task.Completed, false, "finished work is not in flight"},
		{state.Task.Cancelled, false, "cancelled work is not in flight"},
	} {
		t.Run(tc.taskState, func(t *testing.T) {
			_, err := f.db.ExecContext(t.Context(), "UPDATE tasks SET state = ?", tc.taskState)
			require.NoError(t, err)

			running, err := f.client.runningCopyTables(t.Context(), "testdb")
			require.NoError(t, err)
			assert.Equal(t, tc.running, running[inFlight], tc.why)
		})
	}
}
