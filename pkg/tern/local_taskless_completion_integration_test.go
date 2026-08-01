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

// An apply whose task rows exist but were not returned by the whole-apply
// loader (e.g. reflected per-shard progress rows with no loadable drive task)
// is undriveable, not done. The drive must refuse to complete it as a
// task-less no-op: completing would report success on the PR for schema
// changes that never ran, while the unloaded rows kept blocking the database
// as active work. The refused apply stays claimable and non-terminal so the
// stuck work remains visible.
func TestLocalClient_ResumeRefusesTasklessCompletionWhenApplyOwnsTaskRows(t *testing.T) {
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
		PlanIdentifier: fmt.Sprintf("plan-hidden-tasks-%d", now.UnixNano()),
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

	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-hidden-tasks-%d", now.UnixNano()),
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
	// The task row is shard-tagged under an operation whose key does not match
	// it, so no drive loader returns it — the reflected per-shard progress
	// shape, owned by the apply but invisible to the whole-apply drive.
	hiddenTask := &storage.Task{
		TaskIdentifier: fmt.Sprintf("task-hidden-%d", now.UnixNano()),
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
	applyID, err := stor.Applies().CreateWithTasksAndOperations(ctx, apply, []*storage.Task{hiddenTask}, []*storage.ApplyOperation{{
		Deployment: apply.Deployment,
		State:      state.ApplyOperation.Pending,
		CreatedAt:  now,
		UpdatedAt:  now,
	}})
	require.NoError(t, err)
	apply.ID = applyID

	loaded, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Empty(t, loaded, "the hidden task row must not load through the whole-apply loader for this scenario to hold")

	claimed, err := stor.Applies().FindNextApply(ctx, "test-operator-"+t.Name())
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)

	driveCtx := storage.WithApplyLease(ctx, claimed.Lease())
	err = client.ResumeApply(driveCtx, claimed)
	require.ErrorIs(t, err, ErrApplyTasksNotLoaded)

	assert.Empty(t, eng.recorded(), "a refused task-less completion must never reach the engine")

	persisted, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.False(t, state.IsTerminalApplyState(persisted.State),
		"an apply owning unloaded task rows must not be settled (state %q)", persisted.State)
	assert.Nil(t, persisted.CompletedAt, "an apply owning unloaded task rows must not be stamped completed")

	tasks, err := stor.Tasks().CountByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(1), tasks, "the refused drive must not delete or settle the apply's task rows")
}

// A genuinely task-less apply — it owns no task rows at all — completes as a
// no-op, and the completion is recorded in the apply's durable logs so remote
// log readers and operators can see why the apply finished without any
// engine work.
func TestLocalClient_TasklessApplyCompletesWithDurableLog(t *testing.T) {
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

	apply := dispatchQueuedApply(t, stor, client, nil)

	claimed, err := stor.Applies().FindNextApply(ctx, "test-operator-"+t.Name())
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)

	driveCtx := storage.WithApplyLease(ctx, claimed.Lease())
	require.NoError(t, client.ResumeApply(driveCtx, claimed))

	persisted, err := stor.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.True(t, state.IsState(persisted.State, state.Apply.Completed),
		"a genuinely task-less apply completes as a no-op (state %q)", persisted.State)
	assert.NotNil(t, persisted.CompletedAt)

	logs, err := stor.ApplyLogs().GetByApply(ctx, apply.ID)
	require.NoError(t, err)
	var found bool
	for _, entry := range logs {
		if entry.EventType == storage.LogEventStateTransition && entry.Message == "Apply owns no task work; completed without engine work" {
			found = true
			break
		}
	}
	assert.True(t, found, "the no-op completion must be recorded in the apply's durable logs")

	assert.Empty(t, eng.recorded(), "a task-less no-op completion must not touch the engine")
}
