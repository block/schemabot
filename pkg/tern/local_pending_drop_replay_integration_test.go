//go:build integration

package tern

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	spiritutils "github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	waitutil "github.com/block/schemabot/e2e/testutil"
	"github.com/block/schemabot/pkg/pendingdrops"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// pendingDropReplayDeadline bounds each wait in the pending drops replay
// scenario: a drive reaching the blocked RENAME, a stop settling, and a
// server-side RENAME landing once its lock is released.
const pendingDropReplayDeadline = 30 * time.Second

// pendingDropReplayTables are the tables the scenario's plan drops, named so
// the plan orders their DROP statements the way the scenario needs: the first
// is quarantined before the stop, the second is the one whose RENAME the stop
// abandons, and the third is never reached before the stop.
var pendingDropReplayTables = []string{"replay_a_users", "replay_b_sessions", "replay_c_tokens"}

// seedPendingDropReplayTables creates the scenario's tables with rows on the
// target and removes them, and any copy of them in the pending drops
// quarantine, when the test ends.
func seedPendingDropReplayTables(t *testing.T, db *sql.DB, rows int) {
	t.Helper()
	// The quarantine is shared with everything else running against this
	// target, so only this scenario's own copies are removed from it.
	cleanup := func(ctx context.Context) {
		for _, table := range pendingDropReplayTables {
			_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS `"+table+"`")
			assert.NoError(t, err, "drop %s", table)
			copies, err := db.QueryContext(ctx,
				"SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name LIKE ?",
				pendingdrops.Database, "%"+table)
			if !assert.NoError(t, err, "list pending drops copies of %s", table) {
				return
			}
			var names []string
			for copies.Next() {
				var name string
				assert.NoError(t, copies.Scan(&name), "read a pending drops copy of %s", table)
				names = append(names, name)
			}
			assert.NoError(t, copies.Err(), "list pending drops copies of %s", table)
			spiritutils.CloseAndLog(copies)
			for _, name := range names {
				_, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", pendingdrops.Database, name))
				assert.NoError(t, err, "drop pending drops copy %s", name)
			}
		}
	}
	cleanup(t.Context())
	cleanupCtx := context.WithoutCancel(t.Context())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(cleanupCtx, pendingDropReplayDeadline)
		defer cancel()
		cleanup(ctx)
	})

	for _, table := range pendingDropReplayTables {
		_, err := db.ExecContext(t.Context(),
			fmt.Sprintf("CREATE TABLE `%s` (id INT PRIMARY KEY AUTO_INCREMENT, note VARCHAR(32))", table))
		require.NoError(t, err, "create %s", table)
		for range rows {
			_, err := db.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO `%s` (note) VALUES ('kept')", table))
			require.NoError(t, err, "seed %s", table)
		}
	}
}

// requireQuarantinedOnce asserts the table has left the target and that the
// pending drops quarantine holds exactly one copy of it, with every row.
func requireQuarantinedOnce(t *testing.T, dsn, table string, rows int) {
	t.Helper()
	assert.False(t, targetTableExists(t, dsn, table), "%s must have left the target", table)
	copies := quarantinedCopies(t, dsn, table)
	require.Len(t, copies, 1, "%s must be quarantined exactly once, found %v", table, copies)
	for name, count := range copies {
		assert.Equal(t, rows, count, "the pending drops copy %s must keep every row of %s", name, table)
	}
}

// renameBlockedOnLock reports whether a RENAME of table into pending drops is
// waiting on the server.
func renameBlockedOnLock(t *testing.T, db *sql.DB, table string) bool {
	t.Helper()
	var waiting int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.processlist WHERE info LIKE ?",
		"RENAME TABLE%"+table+"%").Scan(&waiting))
	return waiting > 0
}

// requireApplyState asserts the state a drive left the stored apply in once it
// returned.
func requireApplyState(t *testing.T, stor storage.Storage, applyID int64, want string) {
	t.Helper()
	apply, err := stor.Applies().Get(t.Context(), applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	require.True(t, state.IsState(apply.State, want),
		"apply is %s, want %s (error: %s)", apply.State, want, apply.ErrorMessage)
}

// An apply's plan drops three tables with the pending drops quarantine
// enabled. The first table is renamed into pending drops and its task
// completes. A long transaction holds the second, so its RENAME waits on a
// metadata lock, and the operator stops the apply while it waits: the drive
// abandons the RENAME, but the server completes it once the transaction ends.
// The third table is never reached. A driver on another server, whose engine
// has no memory of the first attempt, then takes the operator's start and
// resumes the apply.
//
// The resume plans again against the live schema before it runs anything, so
// the second table has already left the diff and its task settles as
// completed without its DROP being handed to the engine, which would fail on
// a table it never quarantined. Only the third table's DROP runs. Every table
// ends up quarantined exactly once with all of its rows, none is dropped
// outright or brought back, the apply completes, and planning again finds
// nothing left to drop.
func TestLocalClient_ResumeAfterStopMidQuarantineQuarantinesEachTableOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { spiritutils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(ctx))

	const rows = 3
	seedPendingDropReplayTables(t, db, rows)
	schemaFiles := buildSchemaWithAllTables(t, dsn, nil)
	for _, table := range pendingDropReplayTables {
		delete(schemaFiles, table+".sql")
	}

	stor := createStorage(t, dsn)
	defer spiritutils.CloseAndLog(stor)
	first := newSpiritControlClient(t, dsn, stor)
	first.taskPollIntervalOverride = 50 * time.Millisecond

	planResp, err := first.Plan(ctx, &ternv1.PlanRequest{
		Type:        storage.DatabaseTypeMySQL,
		Database:    "testdb",
		SchemaFiles: map[string]*ternv1.SchemaFiles{"testdb": {Files: schemaFiles}},
	})
	require.NoError(t, err)
	var planned []string
	for _, change := range planResp.Changes {
		for _, tc := range change.TableChanges {
			planned = append(planned, tc.TableName+" "+tc.ChangeType.String())
		}
	}
	require.Equal(t, []string{
		"replay_a_users CHANGE_TYPE_DROP",
		"replay_b_sessions CHANGE_TYPE_DROP",
		"replay_c_tokens CHANGE_TYPE_DROP",
	}, planned, "the plan drops the three tables in the order the scenario relies on")

	// A READ lock held on another connection stands in for the long
	// transaction on the second table: its RENAME waits for the exclusive
	// metadata lock until the lock is released.
	locker, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { spiritutils.CloseAndLog(locker) })
	_, err = locker.ExecContext(ctx, "LOCK TABLES `replay_b_sessions` READ")
	require.NoError(t, err)

	applyResp, err := first.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      planResp.PlanId,
		Environment: localClientTestEnvironment,
		Options:     map[string]string{"allow_unsafe": "true"},
	})
	require.NoError(t, err)
	require.True(t, applyResp.Accepted, "apply rejected: %s", applyResp.ErrorMessage)
	apply := resolveDispatchedApply(t, stor, applyResp.ApplyId)

	claimed, err := stor.Applies().ClaimApplyByID(ctx, apply.ID, "test-operator-first-"+t.Name())
	require.NoError(t, err)
	require.NotNil(t, claimed, "the queued apply must be claimable")
	firstDrive := make(chan error, 1)
	go func() {
		firstDrive <- first.ResumeApply(storage.WithApplyLease(ctx, claimed.Lease()), claimed)
	}()

	waitutil.Poll(t, pendingDropReplayDeadline, 20*time.Millisecond,
		func() bool { return renameBlockedOnLock(t, db, "replay_b_sessions") },
		func() string { return "the second table's RENAME never started waiting on its lock" },
	)
	requireQuarantinedOnce(t, dsn, "replay_a_users", rows)

	stopResp, err := first.Stop(ctx, &ternv1.StopRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.True(t, stopResp.Accepted, "stop rejected: %s", stopResp.ErrorMessage)
	select {
	case err := <-firstDrive:
		require.NoError(t, err, "the first drive must stand down cleanly on the stop")
	case <-time.After(pendingDropReplayDeadline):
		require.FailNow(t, "the first drive did not stand down after the stop")
	}
	requireApplyState(t, stor, apply.ID, state.Apply.Stopped)
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationStop, storage.ControlRequestCompleted)

	// The transaction ends. The RENAME the stop abandoned is still queued on
	// the server and lands now.
	_, err = locker.ExecContext(ctx, "UNLOCK TABLES")
	require.NoError(t, err)
	waitutil.Poll(t, pendingDropReplayDeadline, 20*time.Millisecond,
		func() bool { return !targetTableExists(t, dsn, "replay_b_sessions") },
		func() string { return "the abandoned RENAME of the second table never completed on the server" },
	)
	requireQuarantinedOnce(t, dsn, "replay_b_sessions", rows)
	assert.True(t, targetTableExists(t, dsn, "replay_c_tokens"), "the stop must leave the third table in place")

	// A driver on another server resumes the apply. Its engine never saw the
	// first attempt.
	second := newSpiritControlClient(t, dsn, stor)
	second.taskPollIntervalOverride = 50 * time.Millisecond
	startResp, err := second.Start(ctx, &ternv1.StartRequest{
		ApplyId:     apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.True(t, startResp.Accepted, "start rejected: %s", startResp.ErrorMessage)
	driveQueuedApply(t, stor, second, apply.ApplyIdentifier)
	requireApplyState(t, stor, apply.ID, state.Apply.Completed)
	requireControlRequestStatus(t, stor, apply.ID, storage.ControlOperationStart, storage.ControlRequestCompleted)

	for _, table := range pendingDropReplayTables {
		requireQuarantinedOnce(t, dsn, table, rows)
	}

	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, len(pendingDropReplayTables))
	taskByTable := make(map[string]*storage.Task, len(tasks))
	for _, task := range tasks {
		assert.Equal(t, state.Task.Completed, task.State, "task for %s", task.TableName)
		assert.Equal(t, 100, task.ProgressPercent, "task for %s", task.TableName)
		assert.NotNil(t, task.CompletedAt, "task for %s", task.TableName)
		assert.Empty(t, task.ErrorMessage, "task for %s", task.TableName)
		taskByTable[task.TableName] = task
	}

	logs, err := stor.ApplyLogs().GetByApply(ctx, apply.ID)
	require.NoError(t, err)
	firstTask, abandonedTask, thirdTask := taskByTable["replay_a_users"], taskByTable["replay_b_sessions"], taskByTable["replay_c_tokens"]
	require.NotNil(t, firstTask, "a task must exist for replay_a_users")
	require.NotNil(t, abandonedTask, "a task must exist for replay_b_sessions")
	require.NotNil(t, thirdTask, "a task must exist for replay_c_tokens")

	assert.Equal(t, 1, countLogMessagesContaining(logs,
		fmt.Sprintf("Task %s finished: engine_state=completed", firstTask.TaskIdentifier)),
		"the first table's task completes in the first drive, before the stop")
	assert.Equal(t, 1, countLogMessagesContaining(logs,
		fmt.Sprintf("Task %s already completed (live schema matches the reviewed target)", abandonedTask.TaskIdentifier)),
		"the resume must settle the abandoned RENAME's task from the live schema")
	assert.Equal(t, 1, countLogMessagesContaining(logs,
		fmt.Sprintf("Task %s finished: engine_state=completed", thirdTask.TaskIdentifier)),
		"the third table's DROP must run on resume")
	for _, task := range []*storage.Task{firstTask, abandonedTask} {
		assert.Zero(t, countLogMessagesContaining(logs, fmt.Sprintf("Resuming task %s ", task.TaskIdentifier)),
			"the resume must not hand %s's DROP to the engine again", task.TableName)
	}
	assert.Equal(t, 1, countLogMessagesContaining(logs, fmt.Sprintf("Resuming task %s ", thirdTask.TaskIdentifier)),
		"the resume hands only the third table's DROP to the engine")
	assert.Zero(t, countLogMessagesContaining(logs, "was not quarantined by this attempt"),
		"the resume must never hand the engine a DROP for a table already in pending drops")

	replanResp, err := second.Plan(ctx, &ternv1.PlanRequest{
		Type:        storage.DatabaseTypeMySQL,
		Database:    "testdb",
		SchemaFiles: map[string]*ternv1.SchemaFiles{"testdb": {Files: schemaFiles}},
	})
	require.NoError(t, err)
	for _, change := range replanResp.Changes {
		for _, tc := range change.TableChanges {
			assert.NotContains(t, pendingDropReplayTables, tc.TableName,
				"planning again must find nothing left to do for %s, got %s", tc.TableName, tc.ChangeType)
		}
	}
}
