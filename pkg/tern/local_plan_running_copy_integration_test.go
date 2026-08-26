//go:build integration

package tern

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/block/spirit/pkg/checkpoint"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// localClientTestTarget is the target the fixture's plans and dispatches
// resolve to: a plan request naming no target falls back to the database name.
const localClientTestTarget = "testdb"

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

			running, err := f.client.runningCopyTables(t.Context(), "testdb", localClientTestEnvironment, localClientTestTarget)
			require.NoError(t, err)
			assert.Equal(t, tc.running, running[inFlight], tc.why)
		})
	}
}

// Task rows are keyed by database name, which is not a target: the same logical
// name can belong to this deployment's staging and production databases, and
// two targets can share the name, type, and environment outright. A plan only
// describes the target it was asked about, so a copy running on any other
// target — another environment's, a namesake type's, or a sibling target's —
// must never be reported as live work on this one's.
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

	running, err := f.client.runningCopyTables(t.Context(), "testdb", localClientTestEnvironment, localClientTestTarget)
	require.NoError(t, err)
	require.True(t, running[inFlight], "the copy is running on the target that was planned")

	for _, tc := range []struct {
		name        string
		environment string
		target      string
		why         string
	}{
		{"other environment", "some-other-environment", localClientTestTarget,
			"a copy running in one environment is not work on another's target"},
		{"unnamed environment", "", localClientTestTarget,
			"a request naming no environment marks nothing rather than borrowing another's"},
		{"other target", localClientTestEnvironment, "sibling-target",
			"a sibling target sharing the database name, type, and environment is still not the planned target"},
		{"unnamed target", localClientTestEnvironment, "",
			"a plan naming no target marks nothing rather than claiming every namesake's work"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			running, err := f.client.runningCopyTables(t.Context(), "testdb", tc.environment, tc.target)
			require.NoError(t, err)
			assert.False(t, running[inFlight], tc.why)
		})
	}

	t.Run("unnamed environment on both sides", func(t *testing.T) {
		_, err := f.db.ExecContext(t.Context(), "UPDATE tasks SET environment = ''")
		require.NoError(t, err)
		t.Cleanup(func() {
			_, err := f.db.ExecContext(context.WithoutCancel(t.Context()), "UPDATE tasks SET environment = ?", localClientTestEnvironment)
			require.NoError(t, err)
		})

		running, err := f.client.runningCopyTables(t.Context(), "testdb", "", localClientTestTarget)
		require.NoError(t, err)
		assert.False(t, running[inFlight],
			"a request naming no environment marks nothing even when a task row names none either")
	})

	t.Run("task with no operation row", func(t *testing.T) {
		restoreTaskOperationIDs(t, f)
		_, err := f.db.ExecContext(t.Context(), "UPDATE tasks SET apply_operation_id = NULL")
		require.NoError(t, err)

		running, err := f.client.runningCopyTables(t.Context(), "testdb", localClientTestEnvironment, localClientTestTarget)
		require.NoError(t, err)
		assert.False(t, running[inFlight],
			"a task with no operation row cannot be attributed to a target, so it marks nothing")
	})

	t.Run("other database type", func(t *testing.T) {
		_, err := f.db.ExecContext(t.Context(), "UPDATE tasks SET database_type = ?", "vitess")
		require.NoError(t, err)

		running, err := f.client.runningCopyTables(t.Context(), "testdb", localClientTestEnvironment, localClientTestTarget)
		require.NoError(t, err)
		assert.False(t, running[inFlight],
			"a namesake of another database type is not this target")
	})
}

// restoreTaskOperationIDs snapshots every task's operation reference and puts
// it back when the subtest ends, so a subtest that severs the attribution join
// leaves the fixture attributable for the next one.
func restoreTaskOperationIDs(t *testing.T, f *adoptTestFixture) {
	t.Helper()
	rows, err := f.db.QueryContext(t.Context(), "SELECT id, apply_operation_id FROM tasks")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)

	type taskOp struct {
		id          int64
		operationID *int64
	}
	var saved []taskOp
	for rows.Next() {
		var to taskOp
		require.NoError(t, rows.Scan(&to.id, &to.operationID))
		saved = append(saved, to)
	}
	require.NoError(t, rows.Err())

	t.Cleanup(func() {
		for _, to := range saved {
			_, err := f.db.ExecContext(context.WithoutCancel(t.Context()), "UPDATE tasks SET apply_operation_id = ? WHERE id = ?", to.operationID, to.id)
			require.NoError(t, err)
		}
	})
}

// The disclosure an operator decides on is the one the production plan path
// produces, so the marker is exercised end to end: a plan meets a copy on the
// target, reads the deployment's own task rows, and tells the operator whether
// that copy is still being made or was left behind. The same seeded copy reads
// both ways depending only on whether its task is in flight, which is the
// distinction the marker exists to draw.
func TestPlanDisclosesWhetherACopyIsStillRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newAdoptTestFixture(t, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})

	first := f.dispatch(t, "schemabot:v1:plan-running-copy")
	require.True(t, first.Accepted, "first dispatch must be accepted: %s", first.ErrorMessage)
	_, err := f.db.ExecContext(t.Context(), "UPDATE tasks SET state = ?", state.Task.Running)
	require.NoError(t, err)

	// The desired schema is read off the target before the copy artifacts are
	// seeded, so the plan's change set covers only the fixture's own tables.
	schemaFiles := buildSchemaWithAllTables(t, f.dsn, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})
	seedTargetCopy(t, f, "users", "ALTER TABLE `users` ADD INDEX `idx_email` (`email`)")

	planExistingCopy := func(t *testing.T) *ternv1.ExistingCopy {
		t.Helper()
		resp, err := f.client.Plan(t.Context(), &ternv1.PlanRequest{
			Type:        storage.DatabaseTypeMySQL,
			Database:    "testdb",
			Environment: localClientTestEnvironment,
			SchemaFiles: map[string]*ternv1.SchemaFiles{
				"testdb": {Files: schemaFiles},
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.ExistingCopies, 1, "the plan meets the copy seeded on the target")
		return resp.ExistingCopies[0]
	}

	assert.True(t, planExistingCopy(t).Running,
		"a copy whose task is in flight is disclosed as still running")

	_, err = f.db.ExecContext(t.Context(), "UPDATE tasks SET state = ?", state.Task.Stopped)
	require.NoError(t, err)
	assert.False(t, planExistingCopy(t).Running,
		"a copy whose task stopped is disclosed as left behind, dated by its age")
}

// seedTargetCopy puts a Spirit copy of table on the fixture's target: the
// shadow table holding the copied rows and a checkpoint recording the statement
// the copy was started for. The artifacts are dropped when the test ends so a
// later plan against the shared container meets a clean target.
func seedTargetCopy(t *testing.T, f *adoptTestFixture, table, statement string) {
	t.Helper()

	shadowTable := utils.NewTableName(table)
	checkpointTable := utils.CheckpointTableName(table)
	t.Cleanup(func() {
		_, _ = f.db.ExecContext(context.WithoutCancel(t.Context()), fmt.Sprintf("DROP TABLE IF EXISTS `%s`", shadowTable))
		_, _ = f.db.ExecContext(context.WithoutCancel(t.Context()), fmt.Sprintf("DROP TABLE IF EXISTS `%s`", checkpointTable))
	})

	_, err := f.db.ExecContext(t.Context(),
		fmt.Sprintf("CREATE TABLE `%s` (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)", shadowTable))
	require.NoError(t, err, "create shadow table for %s", table)

	cp := checkpoint.NewTable(f.db, checkpointTable, checkpoint.Transient)
	require.NoError(t, cp.Create(t.Context()), "create checkpoint table %s", checkpointTable)
	require.NoError(t, cp.Write(t.Context(), checkpoint.Record{
		Statement:       statement,
		CopierWatermark: `{"Key":["id"],"LowerBound":100}`,
		Position:        "mysql-bin.000001:4",
	}), "write checkpoint row")
}

// A plan is a read: it describes the target and must never fail because of it.
// When the task rows cannot be read, the plan still answers — it marks nothing,
// disclosing every copy as left behind, which is the disclosure exactly as it
// reads without the running distinction, and logs the failure for the operator.
func TestRunningCopiesForPlanMarksNothingWhenTheTaskReadFails(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)

	stor := createStorage(t, dsn)
	require.NoError(t, stor.Close(), "closing the storage makes every task read fail")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(client) })

	running := client.runningCopiesForPlan(t.Context(), &engine.PlanResult{
		ExistingCopies: []*engine.ExistingCopy{{Namespace: "testdb", Tables: []string{"users"}}},
	}, localClientTestEnvironment, localClientTestTarget)

	assert.Nil(t, running, "a failed read marks nothing rather than failing the plan")
}
