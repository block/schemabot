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

			running, err := f.client.runningCopyTables(t.Context(), "testdb", localClientTestEnvironment)
			require.NoError(t, err)
			assert.Equal(t, tc.running, running[inFlight], tc.why)
		})
	}
}

// Task rows are keyed by database name, which is not a target: the same logical
// name can belong to this deployment's staging and production databases. A plan
// only describes the target it was asked about, so a copy running in one
// environment must never be reported as live work on another's.
func TestRunningCopyTablesReportsOnlyThePlannedTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newAdoptTestFixture(t, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})

	first := f.dispatch(t, "schemabot:v1:running-copy-target")
	require.True(t, first.Accepted, "first dispatch must be accepted: %s", first.ErrorMessage)
	_, err := f.db.ExecContext(t.Context(), "UPDATE tasks SET state = ?", state.Task.Running)
	require.NoError(t, err)

	inFlight := runningCopyKey{"testdb", "users"}

	running, err := f.client.runningCopyTables(t.Context(), "testdb", localClientTestEnvironment)
	require.NoError(t, err)
	require.True(t, running[inFlight], "the copy is running on the environment that was planned")

	for _, tc := range []struct {
		name        string
		environment string
		why         string
	}{
		{"other environment", "some-other-environment",
			"a copy running in one environment is not work on another's target"},
		{"unnamed environment", "",
			"a request naming no environment marks nothing rather than borrowing another's"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			running, err := f.client.runningCopyTables(t.Context(), "testdb", tc.environment)
			require.NoError(t, err)
			assert.False(t, running[inFlight], tc.why)
		})
	}

	t.Run("other database type", func(t *testing.T) {
		_, err := f.db.ExecContext(t.Context(), "UPDATE tasks SET database_type = ?", "vitess")
		require.NoError(t, err)

		running, err := f.client.runningCopyTables(t.Context(), "testdb", localClientTestEnvironment)
		require.NoError(t, err)
		assert.False(t, running[inFlight],
			"a namesake of another database type is not this target")
	})
}
