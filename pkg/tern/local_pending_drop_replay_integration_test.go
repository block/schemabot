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
// scenario: a drive reaching the blocked RENAME, a stop settling, a resume
// drive returning, and a server-side RENAME landing once its lock is released.
const pendingDropReplayDeadline = 30 * time.Second

// pendingDropReplayTables are the tables the scenario's plan drops, named so
// the plan orders their DROP statements the way the scenario needs: the first
// is quarantined before the stop, the second is the one whose RENAME the stop
// abandons, and the third is never reached before the stop.
var pendingDropReplayTables = []string{"replay_a_users", "replay_b_sessions", "replay_c_tokens"}

// pendingDropReplayRows is how many rows each table carries, so a quarantined
// copy can be checked for keeping all of them.
const pendingDropReplayRows = 3

// seedPendingDropReplayTables creates the scenario's tables with rows on the
// target and removes them, and any copy of them in the pending drops
// quarantine, when the test ends.
func seedPendingDropReplayTables(t *testing.T, db *sql.DB, rows int) {
	t.Helper()
	// The quarantine is shared with everything else running against this
	// target, so only this scenario's own copies are removed from it. The
	// lookup matches copies the same way quarantinedCopies does, by the
	// table's own name as a suffix; the two must find the same tables, or the
	// cleanup leaves copies behind for the next run's requireQuarantinedOnce
	// to trip over.
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

// renamesWaitingOnLock counts the RENAMEs of table into pending drops that are
// blocked on a metadata lock on the server. A RENAME that is executing rather
// than waiting is not counted.
func renamesWaitingOnLock(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var waiting int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.processlist WHERE info LIKE ? AND state LIKE 'Waiting for%'",
		"RENAME TABLE%"+table+"%").Scan(&waiting))
	return waiting
}

// quarantineStatementsWaitingOnLock counts the statements that move a table
// into pending drops, the RENAME itself or the creation of the quarantine
// schema ahead of it, that are blocked on a metadata lock on the server.
func quarantineStatementsWaitingOnLock(t *testing.T, db *sql.DB) int {
	t.Helper()
	var waiting int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.processlist WHERE info LIKE ? AND state LIKE 'Waiting for%metadata lock'",
		"%`"+pendingdrops.Database+"`%").Scan(&waiting))
	return waiting
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

// pendingDropReplay is the apply the scenario's shared opening leaves stopped
// mid-quarantine, together with the handles the rest of a scenario needs: the
// connection holding the second table's lock, the storage both drivers share,
// and the schema files that plan the three DROPs.
type pendingDropReplay struct {
	dsn         string
	db          *sql.DB
	stor        storage.Storage
	apply       *storage.Apply
	locker      *sql.Conn
	schemaFiles map[string]string
}

// stopPendingDropReplayMidQuarantine runs the opening every pending drops
// replay scenario shares. An apply's plan drops three tables with the pending
// drops quarantine enabled. The first table is renamed into pending drops and
// its task completes. A lock held on another connection stands in for a long
// transaction on the second table, so its RENAME waits on a metadata lock, and
// the operator stops the apply while it waits: the drive abandons the RENAME
// and stands down, but the statement stays queued on the server until the lock
// is released. The third table is never reached.
func stopPendingDropReplayMidQuarantine(t *testing.T) *pendingDropReplay {
	t.Helper()
	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	ctx := t.Context()
	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { spiritutils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(ctx))

	seedPendingDropReplayTables(t, db, pendingDropReplayRows)
	schemaFiles := buildSchemaWithAllTables(t, dsn, nil)
	for _, table := range pendingDropReplayTables {
		delete(schemaFiles, table+".sql")
	}

	stor := createStorage(t, dsn)
	t.Cleanup(func() { spiritutils.CloseAndLog(stor) })
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
	// Closing the connection hands its session back to the pool with any
	// lock it still holds, where the cleanup's DROPs would then run, so the
	// lock is released before the session goes back.
	locker, err := db.Conn(ctx)
	require.NoError(t, err)
	lockerCleanupCtx := context.WithoutCancel(ctx)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(lockerCleanupCtx, pendingDropReplayDeadline)
		defer cancel()
		_, err := locker.ExecContext(ctx, "UNLOCK TABLES")
		assert.NoError(t, err, "release the lock on the second table")
		spiritutils.CloseAndLog(locker)
	})
	_, err = locker.ExecContext(ctx, "LOCK TABLES `replay_b_sessions` READ")
	require.NoError(t, err)

	applyResp, err := first.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      planResp.PlanId,
		Environment: localClientTestEnvironment,
		Options:     map[string]string{"allow_unsafe": "true"},
	})
	require.NoError(t, err)
	require.True(t, applyResp.Accepted, "apply rejected: %s", applyResp.ErrorMessage)
	s := &pendingDropReplay{
		dsn:         dsn,
		db:          db,
		stor:        stor,
		apply:       resolveDispatchedApply(t, stor, applyResp.ApplyId),
		locker:      locker,
		schemaFiles: schemaFiles,
	}

	firstDrive := s.claimAndDrive(t, first, "first")
	waitutil.Poll(t, pendingDropReplayDeadline, 20*time.Millisecond,
		func() bool { return renamesWaitingOnLock(t, db, "replay_b_sessions") > 0 },
		func() string { return "the second table's RENAME never started waiting on its lock" },
	)
	// The first table's task finishes before the second's RENAME is issued
	// only while tasks run one at a time, which nothing here configures, so
	// wait for its quarantine rather than expect it at this instant.
	waitutil.Poll(t, pendingDropReplayDeadline, 20*time.Millisecond,
		func() bool { return !targetTableExists(t, dsn, "replay_a_users") },
		func() string { return "the first table never left the target before the stop" },
	)
	requireQuarantinedOnce(t, dsn, "replay_a_users", pendingDropReplayRows)

	stopResp, err := first.Stop(ctx, &ternv1.StopRequest{
		ApplyId:     s.apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.True(t, stopResp.Accepted, "stop rejected: %s", stopResp.ErrorMessage)
	require.NoError(t, awaitPendingDropReplayDrive(t, firstDrive, "the first drive did not stand down after the stop"),
		"the first drive must stand down cleanly on the stop")
	requireApplyState(t, stor, s.apply.ID, state.Apply.Stopped)
	requireControlRequestStatus(t, stor, s.apply.ID, storage.ControlOperationStop, storage.ControlRequestCompleted)
	return s
}

// claimAndDrive claims the apply for the client the way an operator driver
// does and drives it in the background, the way the test operator does, so
// the test can act on the target while the drive runs. The returned channel
// carries the drive's result once it returns.
func (s *pendingDropReplay) claimAndDrive(t *testing.T, client *LocalClient, driver string) <-chan error {
	t.Helper()
	ctx := t.Context()
	owner := "test-operator-" + driver + "-" + t.Name()
	var claimed *storage.Apply
	require.Eventually(t, func() bool {
		var err error
		claimed, err = s.stor.Applies().ClaimApplyByID(ctx, s.apply.ID, owner)
		return err == nil && claimed != nil
	}, pendingDropReplayDeadline, 50*time.Millisecond, "the apply never became claimable for the %s driver", driver)
	done := make(chan error, 1)
	go func() {
		done <- client.ResumeApply(storage.WithApplyLease(ctx, claimed.Lease()), claimed)
	}()
	return done
}

// awaitPendingDropReplayDrive returns the drive's result, failing the test
// if the drive has not returned within the scenario's deadline.
func awaitPendingDropReplayDrive(t *testing.T, drive <-chan error, hung string) error {
	t.Helper()
	select {
	case err := <-drive:
		return err
	case <-time.After(pendingDropReplayDeadline):
		require.FailNow(t, hung)
		return nil
	}
}

// startPendingDropReplayResume takes the operator's start on a driver whose
// engine has no memory of the first attempt and drives the apply in the
// background.
func (s *pendingDropReplay) startPendingDropReplayResume(t *testing.T) (*LocalClient, <-chan error) {
	t.Helper()
	second := newSpiritControlClient(t, s.dsn, s.stor)
	second.taskPollIntervalOverride = 50 * time.Millisecond
	startResp, err := second.Start(t.Context(), &ternv1.StartRequest{
		ApplyId:     s.apply.ApplyIdentifier,
		Environment: localClientTestEnvironment,
	})
	require.NoError(t, err)
	require.True(t, startResp.Accepted, "start rejected: %s", startResp.ErrorMessage)
	return second, s.claimAndDrive(t, second, "second")
}

// requirePendingDropReplayConverged asserts the apply completed with every
// table quarantined exactly once with all of its rows, every task completed,
// the start request settled, and nothing left for a fresh plan to drop. It
// returns the tasks by table so a scenario can go on to check how each was
// reached.
func (s *pendingDropReplay) requirePendingDropReplayConverged(t *testing.T, client *LocalClient) map[string]*storage.Task {
	t.Helper()
	ctx := t.Context()
	requireApplyState(t, s.stor, s.apply.ID, state.Apply.Completed)
	completed, err := s.stor.Applies().Get(ctx, s.apply.ID)
	require.NoError(t, err)
	assert.Empty(t, completed.ErrorMessage, "a completed apply carries no error")
	requireControlRequestStatus(t, s.stor, s.apply.ID, storage.ControlOperationStart, storage.ControlRequestCompleted)

	for _, table := range pendingDropReplayTables {
		requireQuarantinedOnce(t, s.dsn, table, pendingDropReplayRows)
	}

	tasks, err := s.stor.Tasks().GetByApplyID(ctx, s.apply.ID)
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
	for _, table := range pendingDropReplayTables {
		require.NotNil(t, taskByTable[table], "a task must exist for %s", table)
	}

	replanResp, err := client.Plan(ctx, &ternv1.PlanRequest{
		Type:        storage.DatabaseTypeMySQL,
		Database:    "testdb",
		SchemaFiles: map[string]*ternv1.SchemaFiles{"testdb": {Files: s.schemaFiles}},
	})
	require.NoError(t, err)
	for _, change := range replanResp.Changes {
		for _, tc := range change.TableChanges {
			assert.NotContains(t, pendingDropReplayTables, tc.TableName,
				"planning again must find nothing left to do for %s, got %s", tc.TableName, tc.ChangeType)
		}
	}
	return taskByTable
}

// The operator stops an apply while the second of its three DROPs waits on a
// metadata lock, the long transaction ends after the stop so the abandoned
// RENAME lands on the server, and only then does a driver on another server,
// whose engine has no memory of the first attempt, take the operator's start
// and resume the apply.
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
	s := stopPendingDropReplayMidQuarantine(t)
	ctx := t.Context()

	// The transaction ends. The RENAME the stop abandoned is still queued on
	// the server and lands now.
	_, err := s.locker.ExecContext(ctx, "UNLOCK TABLES")
	require.NoError(t, err)
	waitutil.Poll(t, pendingDropReplayDeadline, 20*time.Millisecond,
		func() bool { return !targetTableExists(t, s.dsn, "replay_b_sessions") },
		func() string { return "the abandoned RENAME of the second table never completed on the server" },
	)
	requireQuarantinedOnce(t, s.dsn, "replay_b_sessions", pendingDropReplayRows)
	assert.True(t, targetTableExists(t, s.dsn, "replay_c_tokens"), "the stop must leave the third table in place")

	second, resume := s.startPendingDropReplayResume(t)
	require.NoError(t, awaitPendingDropReplayDrive(t, resume, "the resume did not return"),
		"the resume must complete the apply")
	taskByTable := s.requirePendingDropReplayConverged(t, second)
	firstTask, abandonedTask, thirdTask := taskByTable["replay_a_users"], taskByTable["replay_b_sessions"], taskByTable["replay_c_tokens"]

	logs, err := s.stor.ApplyLogs().GetByApply(ctx, s.apply.ID)
	require.NoError(t, err)
	assert.Equal(t, 1, countLogMessagesContaining(logs,
		fmt.Sprintf("Task %s finished: engine_state=completed", firstTask.TaskIdentifier)),
		"the first table's task completes in the first drive, before the stop")
	assert.Zero(t, countLogMessagesContaining(logs,
		fmt.Sprintf("Task %s already completed", firstTask.TaskIdentifier)),
		"the resume must leave the first table's completed task alone rather than settle it again")
	assert.Equal(t, 1, countLogMessagesContaining(logs,
		fmt.Sprintf("Task %s already completed (live schema matches the reviewed target)", abandonedTask.TaskIdentifier)),
		"the resume must settle the abandoned RENAME's task from the live schema")
	assert.Equal(t, 1, countLogMessagesContaining(logs,
		fmt.Sprintf("Task %s finished: engine_state=completed", thirdTask.TaskIdentifier)),
		"the third table's DROP must run on resume")
	// The first drive starts its tasks fresh and never logs a resume, so one
	// resumed task across the whole apply means the resume handed the engine
	// exactly one DROP, and the task count below says which.
	assert.Equal(t, 1, countLogMessagesContaining(logs, "Resuming task "),
		"the resume hands the engine exactly one DROP")
	assert.Equal(t, 1, countLogMessagesContaining(logs, fmt.Sprintf("Resuming task %s ", thirdTask.TaskIdentifier)),
		"the one DROP the resume hands the engine is the third table's")
}

// The same opening, but the operator resumes while the long transaction still
// holds the second table, so the RENAME the stop abandoned is still queued on
// the server when the resume plans again. The second table is still on the
// target, so the resume hands its DROP to the engine, whose quarantine of it
// queues on the server behind the abandoned RENAME. When the transaction ends
// the abandoned RENAME lands first and the resume's attempt fails on a table
// that is already in pending drops: the apply pauses as retryable without
// touching the quarantined copy, and the next drive settles the task from the
// live schema and finishes the apply. Every table ends up quarantined exactly
// once and none is dropped outright.
func TestLocalClient_ResumeWhileAbandonedRenameQueuedQuarantinesEachTableOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	s := stopPendingDropReplayMidQuarantine(t)
	ctx := t.Context()

	second, resume := s.startPendingDropReplayResume(t)
	waitutil.Poll(t, pendingDropReplayDeadline, 20*time.Millisecond,
		func() bool { return quarantineStatementsWaitingOnLock(t, s.db) >= 2 },
		func() string {
			return "the resume's quarantine of the second table never queued behind the abandoned RENAME"
		},
	)

	// The transaction ends while both the abandoned RENAME and the resume's
	// quarantine wait on it.
	_, err := s.locker.ExecContext(ctx, "UNLOCK TABLES")
	require.NoError(t, err)
	if err := awaitPendingDropReplayDrive(t, resume, "the resume did not return once the lock was released"); err != nil {
		t.Logf("resume drive returned: %v", err)
	}

	resumed, err := s.stor.Applies().Get(ctx, s.apply.ID)
	require.NoError(t, err)
	require.NotNil(t, resumed)
	require.True(t, state.IsState(resumed.State, state.Apply.FailedRetryable),
		"the resume that lost the lock to the abandoned RENAME must pause as retryable, got %s (error: %s)",
		resumed.State, resumed.ErrorMessage)
	assert.Contains(t, resumed.ErrorMessage, "replay_b_sessions", "the pause must name the table whose quarantine was overtaken")
	requireQuarantinedOnce(t, s.dsn, "replay_b_sessions", pendingDropReplayRows)
	assert.True(t, targetTableExists(t, s.dsn, "replay_c_tokens"), "the pause must leave the third table in place")

	retry := s.claimAndDrive(t, second, "retry")
	require.NoError(t, awaitPendingDropReplayDrive(t, retry, "the retry did not return"),
		"the retry must complete the apply")
	s.requirePendingDropReplayConverged(t, second)
}
