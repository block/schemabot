//go:build integration

package sqlstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// createAttachFixtureApply creates an active apply with one shard-scoped
// operation and its task, mirroring the shape a deployment's first keyed
// dispatch persists, so attach tests exercise the sibling-dispatch case.
func createAttachFixtureApply(t *testing.T, store *Storage) *storage.Apply {
	t.Helper()
	lock := createTestLock(t, store, "payments", storage.DatabaseTypeMySQL, "production")
	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: "apply_attach_fixture",
		LockID:          lock.ID,
		PlanID:          1,
		Database:        "payments",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "production",
		Deployment:      "payments-a",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Pending,
		Options:         []byte("{}"),
		IdempotencyKey:  "schemabot:v1:attach-fixture",
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	tasks := []*storage.Task{{
		TaskIdentifier: "task_attach_fixture_first",
		PlanID:         1,
		Database:       "payments",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Engine:         storage.EngineSpirit,
		Environment:    "production",
		State:          state.Task.Pending,
		TableName:      "users",
		Namespace:      "payments",
		Shard:          "-80",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		DDLAction:      "alter",
		Options:        []byte("{}"),
		CreatedAt:      now,
		UpdatedAt:      now,
	}}
	operations := []*storage.ApplyOperation{{
		Deployment:   "payments-a",
		OperationKey: "payments/-80/users",
		Target:       "payments",
		State:        state.ApplyOperation.Pending,
		CreatedAt:    now,
		UpdatedAt:    now,
	}}
	applyID, err := store.Applies().CreateWithTasksAndOperations(t.Context(), apply, tasks, operations)
	require.NoError(t, err)
	apply.ID = applyID
	return apply
}

// attachSiblingOperation returns the operation/tasks pair a sibling
// shard-scoped dispatch of the same keyed generation would attach.
func attachSiblingOperation(operationKey, shard string) (*storage.ApplyOperation, []*storage.Task) {
	now := time.Now()
	operation := &storage.ApplyOperation{
		Deployment:   "payments-a",
		OperationKey: operationKey,
		Target:       "payments",
		State:        state.ApplyOperation.Pending,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	tasks := []*storage.Task{{
		TaskIdentifier: "task_attach_" + shard,
		PlanID:         1,
		Database:       "payments",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Engine:         storage.EngineSpirit,
		Environment:    "production",
		State:          state.Task.Pending,
		TableName:      "users",
		Namespace:      "payments",
		Shard:          shard,
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		DDLAction:      "alter",
		Options:        []byte("{}"),
		CreatedAt:      now,
		UpdatedAt:      now,
	}}
	return operation, tasks
}

// A sibling dispatch of the same keyed generation attaches its operation and
// tasks to the deployment's existing apply: the apply gains a second
// claimable operation, and the new tasks link to it the same way create-time
// tasks link to theirs.
func TestApplyStore_AttachOperationWithTasksAppendsSibling(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)
	apply := createAttachFixtureApply(t, store)

	operation, tasks := attachSiblingOperation("payments/80-/users", "80-")
	require.NoError(t, store.Applies().AttachOperationWithTasks(ctx, apply, operation, tasks))

	// The operation and task structs are back-filled with their persisted IDs
	// and links (same contract as CreateWithTasksAndOperations).
	assert.Equal(t, apply.ID, operation.ApplyID)
	require.NotZero(t, operation.ID)
	require.NotNil(t, tasks[0].ApplyOperationID)
	assert.Equal(t, operation.ID, *tasks[0].ApplyOperationID)

	ops, err := store.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, ops, 2, "the apply must carry both the create-time and the attached operation")
	keys := []string{ops[0].OperationKey, ops[1].OperationKey}
	assert.ElementsMatch(t, []string{"payments/-80/users", "payments/80-/users"}, keys)

	storedTasks, err := store.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, storedTasks, 2)
	for _, task := range storedTasks {
		require.NotNil(t, task.ApplyOperationID, "task %s must link to an operation", task.TaskIdentifier)
		if task.Shard == "80-" {
			assert.Equal(t, operation.ID, *task.ApplyOperationID, "the attached task must link to the attached operation")
		}
	}
}

// The unique (apply_id, deployment, operation_key) index is the idempotency
// guard for attaches: re-attaching an operation key the apply already carries
// returns ErrApplyOperationExists so the caller replays the winner's row
// instead of double-inserting the operation.
func TestApplyStore_AttachOperationWithTasksRejectsDuplicateKey(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)
	apply := createAttachFixtureApply(t, store)

	operation, tasks := attachSiblingOperation("payments/-80/users", "-80")
	err := store.Applies().AttachOperationWithTasks(ctx, apply, operation, tasks)
	require.ErrorIs(t, err, storage.ErrApplyOperationExists)

	// The failed attach left no partial rows behind.
	ops, listErr := store.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, listErr)
	assert.Len(t, ops, 1)
	storedTasks, tasksErr := store.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, tasksErr)
	assert.Len(t, storedTasks, 1, "the duplicate attach must not insert its tasks")
}

// Attaching to a terminal apply fails closed with ErrApplyNotActive: the
// apply's target reservation is released and no drive will claim new work
// under it, so accepting the operation would strand it as permanently pending.
func TestApplyStore_AttachOperationWithTasksRefusesTerminalApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)
	apply := createAttachFixtureApply(t, store)
	apply.State = state.Apply.Completed
	require.NoError(t, store.Applies().Update(ctx, apply))

	operation, tasks := attachSiblingOperation("payments/80-/users", "80-")
	err := store.Applies().AttachOperationWithTasks(ctx, apply, operation, tasks)
	require.ErrorIs(t, err, storage.ErrApplyNotActive)

	ops, listErr := store.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, listErr)
	assert.Len(t, ops, 1, "the refused attach must not insert its operation")
}

// Attaching to an apply row that does not exist reports ErrApplyNotFound so
// the caller can distinguish a vanished apply from a terminal one.
func TestApplyStore_AttachOperationWithTasksMissingApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	missing := &storage.Apply{ID: 999999, ApplyIdentifier: "apply_attach_missing"}
	operation, tasks := attachSiblingOperation("payments/80-/users", "80-")
	err := store.Applies().AttachOperationWithTasks(ctx, missing, operation, tasks)
	require.ErrorIs(t, err, storage.ErrApplyNotFound)
}
