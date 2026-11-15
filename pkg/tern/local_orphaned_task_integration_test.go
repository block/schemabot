//go:build integration

package tern

import (
	"database/sql"
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

// A pending task whose apply already reached a terminal state is orphaned: the
// apply will never be claimed again, so the task can never start. Without
// cleanup it blocks every later apply targeting its database as phantom active
// work. A new dispatch must cancel the orphan (durably, with an apply-log
// entry) and proceed, instead of being refused forever.
func TestLocalClient_DispatchCancelsOrphanedPendingTaskAndProceeds(t *testing.T) {
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

	now := time.Now()
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-orphan-%d", now.UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      now,
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {
				Tables: []storage.TableChange{
					{Namespace: "testdb", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", Operation: "alter"},
				},
			},
		},
	}
	planID, err := stor.Plans().Create(ctx, plan)
	require.NoError(t, err)

	terminalApply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-orphan-owner-%d", now.UnixNano()),
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "testdb",
		Environment:     localClientTestEnvironment,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Pending,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	orphan := &storage.Task{
		TaskIdentifier: fmt.Sprintf("task-orphan-%d", now.UnixNano()),
		PlanID:         planID,
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Engine:         storage.EngineSpirit,
		Environment:    localClientTestEnvironment,
		State:          state.Task.Pending,
		Namespace:      "testdb",
		TableName:      "users",
		Shard:          "-80",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		DDLAction:      "alter",
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	applyID, err := stor.Applies().CreateWithTasksAndOperations(ctx, terminalApply, []*storage.Task{orphan}, []*storage.ApplyOperation{{
		Deployment: terminalApply.Deployment,
		State:      state.ApplyOperation.Pending,
		CreatedAt:  now,
		UpdatedAt:  now,
	}})
	require.NoError(t, err)
	terminalApply.ID = applyID

	// The owning apply settles without its task ever starting, leaving the
	// pending task orphaned as the only non-terminal work on the database.
	completedAt := time.Now()
	terminalApply.State = state.Apply.Completed
	terminalApply.CompletedAt = &completedAt
	terminalApply.UpdatedAt = completedAt
	require.NoError(t, stor.Applies().Update(ctx, terminalApply))

	// A new dispatch on the same database must cancel the orphan and queue its
	// apply instead of being refused.
	newApply := dispatchQueuedApply(t, stor, client, []storage.TableChange{
		{Namespace: "testdb", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `phone` varchar(32)", Operation: "alter"},
	})
	require.NotNil(t, newApply)
	assert.NotEqual(t, terminalApply.ApplyIdentifier, newApply.ApplyIdentifier)

	persisted, err := stor.Tasks().Get(ctx, orphan.TaskIdentifier)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Task.Cancelled, persisted.State, "the orphaned pending task must be durably cancelled")
	assert.Contains(t, persisted.ErrorMessage, "orphaned")
	assert.NotNil(t, persisted.CompletedAt)

	logs, err := stor.ApplyLogs().GetByApply(ctx, terminalApply.ID)
	require.NoError(t, err)
	var found bool
	for _, entry := range logs {
		if entry.EventType == storage.LogEventStateTransition && entry.Message == orphanedTaskSettlementLogMessage {
			found = true
			break
		}
	}
	assert.True(t, found, "the cancellation must be recorded in the owning apply's durable logs")

	assert.Empty(t, eng.recorded(), "cancelling an orphan must not touch the engine")
}

// A stop settles the apply row under the apply lease and its task rows under
// the operation lease. When driver churn moves the operation lease mid-stop the
// task writes are refused while the apply write lands, leaving a task waiting
// for a driver under an apply that will never be claimed again. Nothing can act
// on that task: start resumes only stopped work, and the takeover path releases
// only stopped work, so the change holds its whole database. A new dispatch
// must settle it into the state the stop meant to write — durably, with an
// apply-log entry — take over its copy, and proceed.
func TestLocalClient_DispatchSettlesStrandedTaskOfStoppedApplyAndProceeds(t *testing.T) {
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

	stoppedApply, stranded := seedStrandedStoppedApply(t, stor)

	// A new dispatch on the same database must settle the stranded task and
	// queue its apply instead of being refused.
	newApply := dispatchQueuedApply(t, stor, client, []storage.TableChange{
		{Namespace: "testdb", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", Operation: "alter"},
	})
	require.NotNil(t, newApply)
	assert.NotEqual(t, stoppedApply.ApplyIdentifier, newApply.ApplyIdentifier)

	persisted, err := stor.Tasks().Get(ctx, stranded.TaskIdentifier)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Task.Stopped, persisted.State,
		"the stranded task must be durably settled into the stop it missed, so its copy stays resumable")
	assert.Equal(t, "error writing checkpoint: too many connections", persisted.ErrorMessage,
		"the engine failure that paused the copy is preserved")
	assert.Nil(t, persisted.CompletedAt)

	logs, err := stor.ApplyLogs().GetByApply(ctx, stoppedApply.ID)
	require.NoError(t, err)
	var found bool
	for _, entry := range logs {
		if entry.EventType == storage.LogEventStateTransition && entry.Message == orphanedTaskSettlementLogMessage {
			found = true
			break
		}
	}
	assert.True(t, found, "the settlement must be recorded in the holding apply's durable logs")

	assert.Empty(t, eng.recorded(), "settling a stranded task must not touch the engine")
}

// seedStrandedStoppedApply writes the exact divergence a stop leaves behind
// when it holds the apply lease but a peer has taken the operation lease: the
// apply row settles to stopped while its task write is refused, so the task
// keeps the engine failure that paused it. Returns the stopped apply and its
// stranded task.
func seedStrandedStoppedApply(t *testing.T, stor storage.Storage) (*storage.Apply, *storage.Task) {
	t.Helper()
	ctx := t.Context()
	now := time.Now()

	const ddl = "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
	planID, err := stor.Plans().Create(ctx, &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-stranded-%d", now.UnixNano()),
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "testdb",
		Environment:    localClientTestEnvironment,
		CreatedAt:      now,
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {Tables: []storage.TableChange{{Namespace: "testdb", Table: "users", DDL: ddl, Operation: "alter"}}},
		},
	})
	require.NoError(t, err)

	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-stranded-owner-%d", now.UnixNano()),
		PlanID:          planID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "testdb",
		Environment:     localClientTestEnvironment,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Running,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	stranded := &storage.Task{
		TaskIdentifier: fmt.Sprintf("task-stranded-%d", now.UnixNano()),
		PlanID:         planID,
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Engine:         storage.EngineSpirit,
		Environment:    localClientTestEnvironment,
		State:          state.Task.FailedRetryable,
		ErrorMessage:   "error writing checkpoint: too many connections",
		Namespace:      "testdb",
		TableName:      "users",
		DDL:            ddl,
		DDLAction:      "alter",
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	applyID, err := stor.Applies().CreateWithTasksAndOperations(ctx, apply, []*storage.Task{stranded}, []*storage.ApplyOperation{{
		Deployment: apply.Deployment,
		State:      state.ApplyOperation.Pending,
		CreatedAt:  now,
		UpdatedAt:  now,
	}})
	require.NoError(t, err)
	apply.ID = applyID

	// Only the apply-level write lands: the apply says stopped, its task does not.
	stoppedAt := time.Now()
	apply.State = state.Apply.Stopped
	apply.UpdatedAt = stoppedAt
	require.NoError(t, stor.Applies().Update(ctx, apply))

	return apply, stranded
}

// A stranded task closes both exits an operator has, and the refusal message
// names them both: the change "holds the database until it is started or
// cancelled", yet start resumes only stopped work and a takeover releases only
// stopped work. This is the whole wedge in one test — the stranded state
// refuses start, and settling the task into the stop it missed reopens it,
// resuming the existing copy rather than starting the table over.
func TestLocalClient_StartRecoversAfterAStrandedTaskIsSettled(t *testing.T) {
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

	stoppedApply, stranded := seedStrandedStoppedApply(t, stor)
	startReq := &ternv1.StartRequest{ApplyId: stoppedApply.ApplyIdentifier}

	// The wedge: the apply is stopped, but no task is, so start finds nothing to
	// resume. The refusal names the state it found rather than reporting the
	// change as absent, which is what sent an operator looking in the wrong place.
	_, _, _, err := client.resolveStartRequest(ctx, startReq)
	require.Error(t, err, "a stranded task leaves start with no stopped work to resume")
	assert.Contains(t, err.Error(), "state start cannot act on")
	assert.Contains(t, err.Error(), state.Task.FailedRetryable)
	assert.Contains(t, err.Error(), stoppedApply.ApplyIdentifier)

	// A conflict check on the same database settles the stranded task.
	plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}
	_, _, err = client.checkActiveTaskConflict(ctx, plan, localClientTestEnvironment, "", 0)
	require.NoError(t, err, "a settled task no longer holds the database")

	settled, err := stor.Tasks().Get(ctx, stranded.TaskIdentifier)
	require.NoError(t, err)
	require.Equal(t, state.Task.Stopped, settled.State)

	// Both exits are open again: start now resolves the apply and resumes the
	// one table, which is what makes the copy on the target recoverable.
	resumed, startedCount, skippedCount, err := client.resolveStartRequest(ctx, startReq)
	require.NoError(t, err, "start must resume the change once its task is settled")
	require.NotNil(t, resumed)
	assert.Equal(t, stoppedApply.ApplyIdentifier, resumed.ApplyIdentifier)
	assert.Equal(t, int64(1), startedCount, "the settled table is resumed, not skipped")
	assert.Equal(t, int64(0), skippedCount)
}

// staleRetryMember is one operation of an apply seeded by
// seedStaleRetryableApply, with the task it owns.
type staleRetryMember struct {
	deployment string
	table      string
	opState    string
	taskState  string
	// taskAge backdates the task's creation, so the conflict check, which reads
	// a database's newest task first, meets the members' tasks in a known order.
	taskAge time.Duration
}

// seedStaleRetryableApply stores a running apply with one operation and one
// task per member, and returns the apply and its operations in member order.
// apply_operations rows other tests left behind are cleared first, so an
// operation claim in the test can only return this apply's rows.
func seedStaleRetryableApply(t *testing.T, stor storage.Storage, dsn string, members []staleRetryMember) (*storage.Apply, []*storage.ApplyOperation) {
	t.Helper()
	ctx := t.Context()
	execSQL(t, dsn, "DELETE FROM apply_operations")
	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-stale-retry-%d", now.UnixNano()),
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      members[0].deployment,
		Environment:     localClientTestEnvironment,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Running,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	groups := make([]*storage.ApplyOperationWithTasks, 0, len(members))
	for _, m := range members {
		ddl := fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `email` varchar(255)", m.table)
		groups = append(groups, &storage.ApplyOperationWithTasks{
			Operation: &storage.ApplyOperation{
				Deployment:   m.deployment,
				OperationKey: m.deployment,
				Target:       m.deployment,
				State:        m.opState,
				OnFailure:    storage.OnFailureHalt,
				CreatedAt:    now,
				UpdatedAt:    now,
			},
			Tasks: []*storage.Task{{
				TaskIdentifier: fmt.Sprintf("%s-%s", apply.ApplyIdentifier, m.table),
				Database:       "testdb",
				DatabaseType:   storage.DatabaseTypeMySQL,
				Engine:         storage.EngineSpirit,
				Environment:    localClientTestEnvironment,
				State:          m.taskState,
				Namespace:      "testdb",
				TableName:      m.table,
				DDL:            ddl,
				DDLAction:      "alter",
				CreatedAt:      now.Add(-m.taskAge),
				UpdatedAt:      now,
			}},
		})
	}
	applyID, err := stor.Applies().CreateWithGroupedOperations(ctx, apply, groups)
	require.NoError(t, err)
	apply.ID = applyID

	ops := make([]*storage.ApplyOperation, 0, len(groups))
	for _, group := range groups {
		op, err := stor.ApplyOperations().Get(ctx, group.Operation.ID)
		require.NoError(t, err)
		require.NotNil(t, op)
		ops = append(ops, op)
	}
	return apply, ops
}

// execSQL runs one statement against the storage database.
func execSQL(t *testing.T, dsn, query string, args ...any) {
	t.Helper()
	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	_, err = db.ExecContext(t.Context(), query, args...)
	require.NoError(t, err, "exec %s", query)
}

// leaveLeaseStale records a lease on the row as a driver that has since died
// would leave it: an owner and a token, with a heartbeat older than the
// staleness window. table is applies or apply_operations.
func leaveLeaseStale(t *testing.T, dsn, table string, id int64, owner, token string) {
	t.Helper()
	execSQL(t, dsn, fmt.Sprintf(
		"UPDATE %s SET lease_owner = ?, lease_token = ?, lease_acquired_at = NOW(), updated_at = NOW() - INTERVAL ? MICROSECOND WHERE id = ?", table),
		owner, token, 2*storage.ApplyLeaseStaleAfter.Microseconds(), id)
}

// newRetryableFailureClient returns a client whose engine remembers the last
// copy it ran failing with an error it marks retryable.
func newRetryableFailureClient(t *testing.T, dsn string, stor storage.Storage) *LocalClient {
	t.Helper()
	client, _ := newTasklessControlClient(t, dsn, stor)
	client.spiritEngine = &fakeControlEngine{progressResult: retryableCopyFailure}
	return client
}

// checkConflictOnTestdb runs the dispatch conflict check for a new apply on
// testdb and returns its error.
func checkConflictOnTestdb(t *testing.T, client *LocalClient) error {
	t.Helper()
	_, _, err := client.checkActiveTaskConflict(t.Context(), &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}, "", "", 0)
	return err
}

// requireStoredStates asserts the stored states of an apply and its operations.
func requireStoredStates(t *testing.T, stor storage.Storage, applyID int64, applyState string, ops map[int64]string) {
	t.Helper()
	stored, err := stor.Applies().Get(t.Context(), applyID)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, applyState, stored.State, "apply state")
	for id, opState := range ops {
		op, err := stor.ApplyOperations().Get(t.Context(), id)
		require.NoError(t, err)
		require.NotNil(t, op)
		assert.Equal(t, opState, op.State, "state of operation %d", id)
	}
}

// A drive in this process was copying `users` and died, leaving its apply and
// operation running under stale leases. A new dispatch's conflict check asks
// the engine, which remembers the copy failing with an error it marks
// retryable. The task rests failed_retryable and keeps the database held, and
// the apply and its operation are paused for retry with it, so the recovery
// that follows goes through the retryable claim and is charged against the
// retry budget instead of re-leasing the apply for free.
func TestLocalClient_ConflictCheckPausesAStaleApplyForRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	client := newRetryableFailureClient(t, dsn, stor)

	apply, ops := seedStaleRetryableApply(t, stor, dsn, []staleRetryMember{
		{deployment: "testdb", table: "users", opState: state.ApplyOperation.Running, taskState: state.Task.Running},
	})
	leaveLeaseStale(t, dsn, "applies", apply.ID, storage.LeaseOwnerProcess()+"/driver-0", "apply-token-dead")
	leaveLeaseStale(t, dsn, "apply_operations", ops[0].ID, storage.LeaseOwnerProcess()+"/driver-0", "op-token-dead")

	err := checkConflictOnTestdb(t, client)
	require.Error(t, err, "a task paused for retry keeps holding the database")
	assert.Contains(t, err.Error(), "schema change already in progress")

	task, err := stor.Tasks().Get(ctx, apply.ApplyIdentifier+"-users")
	require.NoError(t, err)
	require.NotNil(t, task)
	assert.Equal(t, state.Task.FailedRetryable, task.State)
	paused, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, paused)
	assert.Equal(t, state.Apply.FailedRetryable, paused.State)
	assert.Equal(t, "lost connection to the target during the copy", paused.ErrorMessage)
	assert.Empty(t, paused.LeaseOwner)
	assert.Equal(t, 0, paused.Attempt, "the pause leaves the attempt for the recovery claim to charge")
	requireStoredStates(t, stor, apply.ID, state.Apply.FailedRetryable, map[int64]string{ops[0].ID: state.ApplyOperation.FailedRetryable})

	claimed, err := stor.Applies().ClaimApplyByID(ctx, apply.ID, "recovery-driver")
	require.NoError(t, err)
	require.NotNil(t, claimed, "a freshly paused apply is within its retry budget")
	assert.Equal(t, 1, claimed.Attempt, "the recovery is charged against the retry budget")
}

// A multi-operation apply: the drive copying `users` died, its operation and
// the parent's leases are stale, and a sibling operation for `orders` has not
// started. The engine reports the `users` copy failing retryably. The parent and
// the failed operation are paused for retry together — the parent state the
// rollout projection derives from a failed_retryable operation — and the
// pending sibling is left as it was. Redispatching the failed operation charges
// the parent's retry budget.
func TestLocalClient_ConflictCheckPausesAStaleMultiOperationApplyForRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	client := newRetryableFailureClient(t, dsn, stor)

	apply, ops := seedStaleRetryableApply(t, stor, dsn, []staleRetryMember{
		{deployment: "testdb", table: "users", opState: state.ApplyOperation.Running, taskState: state.Task.Running},
		{deployment: "testdb-2", table: "orders", opState: state.ApplyOperation.Pending, taskState: state.Task.Pending, taskAge: time.Minute},
	})
	leaveLeaseStale(t, dsn, "applies", apply.ID, storage.LeaseOwnerProcess()+"/driver-0", "apply-token-dead")
	leaveLeaseStale(t, dsn, "apply_operations", ops[0].ID, storage.LeaseOwnerProcess()+"/driver-0", "op-token-dead")

	err := checkConflictOnTestdb(t, client)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema change already in progress")

	requireStoredStates(t, stor, apply.ID, state.Apply.FailedRetryable, map[int64]string{
		ops[0].ID: state.ApplyOperation.FailedRetryable,
		ops[1].ID: state.ApplyOperation.Pending,
	})

	redispatched, err := stor.ApplyOperations().FindNextApplyOperation(ctx, "recovery-driver")
	require.NoError(t, err)
	require.NotNil(t, redispatched)
	assert.Equal(t, ops[0].ID, redispatched.ID, "the paused operation is redispatched")
	charged, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, charged)
	assert.Equal(t, 1, charged.Attempt, "the redispatch is charged against the parent's retry budget")
}

// The same stale `users` drive, but a sibling operation's drive for `orders`
// is live and heartbeating its own lease. Pausing the parent would put a live
// rollout under a retry it did not ask for, so the store refuses the move: the
// task still rests failed_retryable and holds the database, and the apply and
// both operations are left as they were.
func TestLocalClient_ConflictCheckLeavesTheApplyActiveUnderALiveSiblingDrive(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)

	ctx := t.Context()
	stor := createStorage(t, dsn)
	defer utils.CloseAndLog(stor)
	client := newRetryableFailureClient(t, dsn, stor)

	apply, ops := seedStaleRetryableApply(t, stor, dsn, []staleRetryMember{
		{deployment: "testdb", table: "users", opState: state.ApplyOperation.Running, taskState: state.Task.Running},
		{deployment: "testdb-2", table: "orders", opState: state.ApplyOperation.Running, taskState: state.Task.Running, taskAge: time.Minute},
	})
	leaveLeaseStale(t, dsn, "applies", apply.ID, storage.LeaseOwnerProcess()+"/driver-0", "apply-token-dead")
	leaveLeaseStale(t, dsn, "apply_operations", ops[0].ID, storage.LeaseOwnerProcess()+"/driver-0", "op-token-dead")
	execSQL(t, dsn, "UPDATE apply_operations SET lease_owner = ?, lease_token = ?, lease_acquired_at = NOW(), updated_at = NOW() WHERE id = ?",
		"live-host/77/driver-3", "op-token-live", ops[1].ID)

	err := checkConflictOnTestdb(t, client)
	require.Error(t, err, "the task keeps holding the database")
	assert.Contains(t, err.Error(), "schema change already in progress")

	task, err := stor.Tasks().Get(ctx, apply.ApplyIdentifier+"-users")
	require.NoError(t, err)
	require.NotNil(t, task)
	assert.Equal(t, state.Task.FailedRetryable, task.State)
	requireStoredStates(t, stor, apply.ID, state.Apply.Running, map[int64]string{
		ops[0].ID: state.ApplyOperation.Running,
		ops[1].ID: state.ApplyOperation.Running,
	})
	stored, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, "apply-token-dead", stored.LeaseToken, "the refused move writes nothing")
}
