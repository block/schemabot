//go:build integration

package tern

import (
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

	now := time.Now()
	plan := &storage.Plan{
		PlanIdentifier: fmt.Sprintf("plan-stranded-%d", now.UnixNano()),
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

	stoppedApply := &storage.Apply{
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
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		DDLAction:      "alter",
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	applyID, err := stor.Applies().CreateWithTasksAndOperations(ctx, stoppedApply, []*storage.Task{stranded}, []*storage.ApplyOperation{{
		Deployment: stoppedApply.Deployment,
		State:      state.ApplyOperation.Pending,
		CreatedAt:  now,
		UpdatedAt:  now,
	}})
	require.NoError(t, err)
	stoppedApply.ID = applyID

	// The stop settles the apply while its task write is refused: the apply row
	// says stopped, the task row still carries the failure that paused it.
	stoppedAt := time.Now()
	stoppedApply.State = state.Apply.Stopped
	stoppedApply.UpdatedAt = stoppedAt
	require.NoError(t, stor.Applies().Update(ctx, stoppedApply))

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
