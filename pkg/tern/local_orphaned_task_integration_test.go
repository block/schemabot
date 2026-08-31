//go:build integration

package tern

import (
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

	// The wedge: the apply is stopped, but no task is, so start finds nothing
	// to resume and the operator is told there is no stopped schema change.
	_, _, _, err := client.resolveStartRequest(ctx, startReq)
	require.Error(t, err, "a stranded task leaves start with no stopped work to resume")
	assert.Contains(t, err.Error(), "no stopped schema change to resume")

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
