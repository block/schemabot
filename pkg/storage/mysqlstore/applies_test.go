//go:build integration

package mysqlstore

import (
	"context"
	"database/sql"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func TestApplyStore_Create(t *testing.T) {
	clearTables(t)
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	created := createTestApply(t, store, lock, "apply_create_test", 1)

	require.NotZero(t, created.ID)
}

func TestApplyStore_CreateDuplicate(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")

	apply := &storage.Apply{
		ApplyIdentifier: "apply_dup_test",
		LockID:          lock.ID,
		PlanID:          1,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Pending,
	}
	_, err := store.Applies().Create(ctx, apply)
	require.NoError(t, err)

	// Duplicate apply_identifier should fail
	apply2 := &storage.Apply{
		ApplyIdentifier: "apply_dup_test",
		LockID:          lock.ID,
		PlanID:          2,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Completed,
	}
	_, err = store.Applies().Create(ctx, apply2)
	require.ErrorIs(t, err, storage.ErrApplyIDExists)
}

func TestApplyStore_CreateWithTasksCommitsQueueAtomically(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: "apply_create_with_tasks",
		LockID:          lock.ID,
		PlanID:          1,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Pending,
		Options:         []byte("{}"),
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	tasks := []*storage.Task{
		{
			TaskIdentifier: "task_create_with_tasks_users",
			PlanID:         1,
			Database:       "testdb",
			DatabaseType:   storage.DatabaseTypeMySQL,
			Engine:         storage.EngineSpirit,
			Environment:    "staging",
			State:          state.Task.Pending,
			TableName:      "users",
			DDL:            "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
			DDLAction:      "alter",
			Options:        []byte("{}"),
			CreatedAt:      now,
			UpdatedAt:      now,
		},
		{
			TaskIdentifier: "task_create_with_tasks_orders",
			PlanID:         1,
			Database:       "testdb",
			DatabaseType:   storage.DatabaseTypeMySQL,
			Engine:         storage.EngineSpirit,
			Environment:    "staging",
			State:          state.Task.Pending,
			TableName:      "orders",
			DDL:            "ALTER TABLE orders ADD COLUMN status VARCHAR(255)",
			DDLAction:      "alter",
			Options:        []byte("{}"),
			CreatedAt:      now,
			UpdatedAt:      now,
		},
	}

	applyID, err := store.Applies().CreateWithTasks(ctx, apply, tasks)
	require.NoError(t, err)
	require.NotZero(t, applyID)

	storedTasks, err := store.Tasks().GetByApplyID(ctx, applyID)
	require.NoError(t, err)
	require.Len(t, storedTasks, 2)
	assert.Equal(t, applyID, tasks[0].ApplyID)
	assert.Equal(t, applyID, tasks[1].ApplyID)

	// A pending apply created with its full task set is immediately ready for
	// operator dispatch; workers never see a partially populated task list.
	claimed, err := store.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)
}

func TestApplyStore_CreateWithTasksAndOperationsCommitsAtomically(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "payments", storage.DatabaseTypeMySQL, "production")
	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: "apply_create_with_ops",
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
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	tasks := []*storage.Task{
		{
			TaskIdentifier: "task_create_with_ops_users",
			PlanID:         1,
			Database:       "payments",
			DatabaseType:   storage.DatabaseTypeMySQL,
			Engine:         storage.EngineSpirit,
			Environment:    "production",
			State:          state.Task.Pending,
			TableName:      "users",
			DDL:            "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
			DDLAction:      "alter",
			Options:        []byte("{}"),
			CreatedAt:      now,
			UpdatedAt:      now,
		},
	}
	operations := []*storage.ApplyOperation{
		{
			Deployment: "payments-a",
			Target:     "payments",
			State:      state.ApplyOperation.Pending,
			CreatedAt:  now,
			UpdatedAt:  now,
		},
	}

	applyID, err := store.Applies().CreateWithTasksAndOperations(ctx, apply, tasks, operations)
	require.NoError(t, err)
	require.NotZero(t, applyID)

	storedTasks, err := store.Tasks().GetByApplyID(ctx, applyID)
	require.NoError(t, err)
	require.Len(t, storedTasks, 1)
	assert.Equal(t, applyID, tasks[0].ApplyID)

	storedOps, err := store.ApplyOperations().ListByApply(ctx, applyID)
	require.NoError(t, err)
	require.Len(t, storedOps, 1)
	assert.Equal(t, applyID, storedOps[0].ApplyID)
	assert.Equal(t, "payments-a", storedOps[0].Deployment)
	assert.Equal(t, "payments", storedOps[0].Target)
	assert.Equal(t, state.ApplyOperation.Pending, storedOps[0].State)
	// CreateWithTasksAndOperations back-fills the operation's ApplyID
	// onto the caller-supplied struct (same contract as CreateWithTasks).
	assert.Equal(t, applyID, operations[0].ApplyID)
	// It also back-fills the operation's ID onto every task so the row in
	// MySQL carries the apply_operation_id link the operator claim loop
	// will consume. The link must be present both in-memory (on the
	// caller-supplied struct) and on the persisted row.
	require.NotNil(t, tasks[0].ApplyOperationID, "task.ApplyOperationID must be back-filled in-memory")
	assert.Equal(t, operations[0].ID, *tasks[0].ApplyOperationID)
	require.NotNil(t, storedTasks[0].ApplyOperationID, "task.apply_operation_id must be persisted")
	assert.Equal(t, operations[0].ID, *storedTasks[0].ApplyOperationID)
}

// TestApplyStore_CreateWithTasksAndOperationsRollsBackOnTaskFailure pins the
// post-reorder invariant: operations are inserted before tasks, so a task
// insert failure must roll back the already-inserted apply_operations rows
// (and the apply row) — no orphan operations left behind.
func TestApplyStore_CreateWithTasksAndOperationsRollsBackOnTaskFailure(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "payments", storage.DatabaseTypeMySQL, "production")
	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: "apply_rollback_on_task_failure",
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
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	// Two tasks sharing the same task_identifier violates the UNIQUE KEY
	// idx_task_identifier on the second insert.
	tasks := []*storage.Task{
		{TaskIdentifier: "task_dup", PlanID: 1, Database: "payments", DatabaseType: storage.DatabaseTypeMySQL, Engine: storage.EngineSpirit, Environment: "production", State: state.Task.Pending, TableName: "users", DDL: "ALTER TABLE users ADD COLUMN a INT", DDLAction: "alter", Options: []byte("{}"), CreatedAt: now, UpdatedAt: now},
		{TaskIdentifier: "task_dup", PlanID: 1, Database: "payments", DatabaseType: storage.DatabaseTypeMySQL, Engine: storage.EngineSpirit, Environment: "production", State: state.Task.Pending, TableName: "orders", DDL: "ALTER TABLE orders ADD COLUMN b INT", DDLAction: "alter", Options: []byte("{}"), CreatedAt: now, UpdatedAt: now},
	}
	operations := []*storage.ApplyOperation{
		{Deployment: "payments-a", Target: "payments", State: state.ApplyOperation.Pending, CreatedAt: now, UpdatedAt: now},
	}

	_, err := store.Applies().CreateWithTasksAndOperations(ctx, apply, tasks, operations)
	require.Error(t, err)

	gotApply, err := store.Applies().GetByApplyIdentifier(ctx, apply.ApplyIdentifier)
	require.NoError(t, err)
	assert.Nil(t, gotApply, "apply row must not exist after rollback")

	// The op insert succeeded before the task insert failed; the rollback
	// must drop it too — no orphan apply_operations rows for this deployment.
	var opCount int
	require.NoError(t, testDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM apply_operations WHERE deployment = ?`, "payments-a").Scan(&opCount))
	assert.Zero(t, opCount, "apply_operations row must be rolled back along with tasks and apply")
}

// TestApplyStore_CreateWithTasksAndOperationsRejectsMultiOpWithoutTaskMapping
// pins the multi-op guard: when an apply has >1 operations, the caller MUST
// pre-populate task.ApplyOperationID; the store will not silently link every
// task to operations[0]. This guard prevents a wrong default from getting
// locked in once the config-layer multi-entry-deployments block is lifted.
func TestApplyStore_CreateWithTasksAndOperationsRejectsMultiOpWithoutTaskMapping(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "payments", storage.DatabaseTypeMySQL, "production")
	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: "apply_multi_op_no_mapping",
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
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	tasks := []*storage.Task{
		{TaskIdentifier: "task_unmapped", PlanID: 1, Database: "payments", DatabaseType: storage.DatabaseTypeMySQL, Engine: storage.EngineSpirit, Environment: "production", State: state.Task.Pending, TableName: "users", DDL: "ALTER TABLE users ADD COLUMN a INT", DDLAction: "alter", Options: []byte("{}"), CreatedAt: now, UpdatedAt: now},
	}
	operations := []*storage.ApplyOperation{
		{Deployment: "payments-a", Target: "payments", State: state.ApplyOperation.Pending, CreatedAt: now, UpdatedAt: now},
		{Deployment: "payments-b", Target: "payments", State: state.ApplyOperation.Pending, CreatedAt: now, UpdatedAt: now},
	}

	_, err := store.Applies().CreateWithTasksAndOperations(ctx, apply, tasks, operations)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing apply_operation_id")

	gotApply, err := store.Applies().GetByApplyIdentifier(ctx, apply.ApplyIdentifier)
	require.NoError(t, err)
	assert.Nil(t, gotApply, "apply row must not exist after rejected multi-op create")
}

// TestApplyStore_CreateWithTasksAndOperationsRejectsTaskApplyOperationIDMismatch
// pins the ID-membership check: every non-nil task.ApplyOperationID must
// point at one of the apply_operations rows just inserted for this apply.
// tasks.apply_operation_id is not a foreign key (only an index), so without
// this check a caller could persist an arbitrary or zero id and silently
// break per-operation task scoping once the operator claim loop comes
// online.
func TestApplyStore_CreateWithTasksAndOperationsRejectsTaskApplyOperationIDMismatch(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "payments", storage.DatabaseTypeMySQL, "production")
	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: "apply_op_id_mismatch",
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
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	// Caller supplies an arbitrary id (9_999_999) that cannot possibly
	// match an apply_operations row created in the same transaction.
	bogusOpID := int64(9_999_999)
	tasks := []*storage.Task{
		{TaskIdentifier: "task_bogus_op_id", ApplyOperationID: &bogusOpID, PlanID: 1, Database: "payments", DatabaseType: storage.DatabaseTypeMySQL, Engine: storage.EngineSpirit, Environment: "production", State: state.Task.Pending, TableName: "users", DDL: "ALTER TABLE users ADD COLUMN a INT", DDLAction: "alter", Options: []byte("{}"), CreatedAt: now, UpdatedAt: now},
	}
	operations := []*storage.ApplyOperation{
		{Deployment: "payments-a", Target: "payments", State: state.ApplyOperation.Pending, CreatedAt: now, UpdatedAt: now},
	}

	_, err := store.Applies().CreateWithTasksAndOperations(ctx, apply, tasks, operations)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match any inserted operation")

	gotApply, err := store.Applies().GetByApplyIdentifier(ctx, apply.ApplyIdentifier)
	require.NoError(t, err)
	assert.Nil(t, gotApply, "apply row must not exist after rejected mismatched-op-id create")

	var opCount int
	require.NoError(t, testDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM apply_operations WHERE deployment = ?`, "payments-a").Scan(&opCount))
	assert.Zero(t, opCount, "apply_operations row must be rolled back along with the rejected apply")
}

// TestApplyStore_CreateWithTasksAndOperationsRejectsTasksWithApplyOperationIDWhenNoOperations
// pins the no-operations case explicitly: when an apply is created with
// tasks but no apply_operations, every task.ApplyOperationID must be nil.
// A non-nil value here cannot reference any row this apply owns.
func TestApplyStore_CreateWithTasksAndOperationsRejectsTasksWithApplyOperationIDWhenNoOperations(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "payments", storage.DatabaseTypeMySQL, "production")
	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: "apply_tasks_no_ops",
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
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	bogusOpID := int64(42)
	tasks := []*storage.Task{
		{TaskIdentifier: "task_no_ops", ApplyOperationID: &bogusOpID, PlanID: 1, Database: "payments", DatabaseType: storage.DatabaseTypeMySQL, Engine: storage.EngineSpirit, Environment: "production", State: state.Task.Pending, TableName: "users", DDL: "ALTER TABLE users ADD COLUMN a INT", DDLAction: "alter", Options: []byte("{}"), CreatedAt: now, UpdatedAt: now},
	}

	_, err := store.Applies().CreateWithTasksAndOperations(ctx, apply, tasks, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "apply has no operations")

	gotApply, err := store.Applies().GetByApplyIdentifier(ctx, apply.ApplyIdentifier)
	require.NoError(t, err)
	assert.Nil(t, gotApply, "apply row must not exist after rejected no-ops create")
}

func TestApplyStore_CreateWithTasksAndOperationsRollsBackOnOperationFailure(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "payments", storage.DatabaseTypeMySQL, "production")
	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: "apply_rollback_on_op_failure",
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
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	// Two operations sharing the same deployment violates the
	// UNIQUE KEY (apply_id, deployment) constraint on the second insert and
	// must roll back the whole transaction — no orphan apply or tasks rows.
	operations := []*storage.ApplyOperation{
		{Deployment: "payments-a", Target: "payments", State: state.ApplyOperation.Pending, CreatedAt: now, UpdatedAt: now},
		{Deployment: "payments-a", Target: "payments", State: state.ApplyOperation.Pending, CreatedAt: now, UpdatedAt: now},
	}

	_, err := store.Applies().CreateWithTasksAndOperations(ctx, apply, nil, operations)
	require.Error(t, err)

	got, err := store.Applies().GetByApplyIdentifier(ctx, apply.ApplyIdentifier)
	require.NoError(t, err)
	assert.Nil(t, got, "apply row must not exist after rollback")
}

func TestApplyStore_CreateBlocksActiveApplyForSameTarget(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	active := createTestApply(t, store, lock, "apply_active", 1)

	_, err := store.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply_same_target",
		LockID:          lock.ID,
		PlanID:          2,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Pending,
	})
	require.ErrorIs(t, err, storage.ErrActiveApplyExists)

	_, err = store.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply_terminal_same_target",
		LockID:          lock.ID,
		PlanID:          3,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Completed,
	})
	require.NoError(t, err)

	_, err = store.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply_other_env",
		LockID:          lock.ID,
		PlanID:          4,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "production",
		Engine:          "spirit",
		State:           state.Apply.Pending,
	})
	require.NoError(t, err)

	active.State = state.Apply.Completed
	require.NoError(t, store.Applies().Update(ctx, active))

	_, err = store.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply_same_target_after_terminal",
		LockID:          lock.ID,
		PlanID:          5,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Pending,
	})
	require.NoError(t, err)
}

// TestApplyStore_CreateScopesActiveApplyByDeployment verifies the active-apply
// invariant is keyed on the full (database, type, environment, deployment)
// target. Two deployments under the same environment are distinct physical
// targets, so both may be active at once; a second active apply for the same
// deployment is still rejected.
func TestApplyStore_CreateScopesActiveApplyByDeployment(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")

	newApply := func(identifier, deployment string, planID int64) *storage.Apply {
		return &storage.Apply{
			ApplyIdentifier: identifier,
			LockID:          lock.ID,
			PlanID:          planID,
			Database:        "testdb",
			DatabaseType:    "mysql",
			Repository:      "org/repo",
			PullRequest:     123,
			Environment:     "staging",
			Deployment:      deployment,
			Engine:          "spirit",
			State:           state.Apply.Running,
		}
	}

	// First deployment becomes active.
	_, err := store.Applies().Create(ctx, newApply("apply_region_a", "region-a", 1))
	require.NoError(t, err)

	// A different deployment under the same environment is allowed concurrently.
	_, err = store.Applies().Create(ctx, newApply("apply_region_b", "region-b", 2))
	require.NoError(t, err, "different deployments are distinct targets and may be active together")

	// A second active apply for an already-active deployment is rejected.
	_, err = store.Applies().Create(ctx, newApply("apply_region_a_again", "region-a", 3))
	require.ErrorIs(t, err, storage.ErrActiveApplyExists, "same 4-tuple target must still be exclusive")
}

func TestApplyStore_CreateWaitsForApplyTargetLock(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")

	// Hold the same target lock that the create path must acquire. The creates
	// below use the public store API; a result before release means active
	// apply writes are not serialized by the per-target lock.
	guardConn, guardLockName, err := acquireApplyTargetLockConn(ctx, testDB, "testdb", "mysql", "staging")
	require.NoError(t, err)
	releaseGuard := func() {
		if guardConn == nil {
			return
		}
		releaseApplyTargetLockConn(ctx, guardConn, guardLockName, "test active apply guard")
		guardConn = nil
	}
	t.Cleanup(releaseGuard)

	createCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	_, err = store.Applies().Create(createCtx, &storage.Apply{
		ApplyIdentifier: "apply_waiting_same_target",
		LockID:          lock.ID,
		PlanID:          6,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Pending,
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)

	applies, err := store.Applies().GetByDatabase(ctx, "testdb", "mysql", "staging")
	require.NoError(t, err)
	assert.Empty(t, applies)

	releaseGuard()

	id, err := store.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply_after_target_lock_release",
		LockID:          lock.ID,
		PlanID:          7,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Pending,
	})
	require.NoError(t, err)
	assert.NotZero(t, id)

	applies, err = store.Applies().GetByDatabase(ctx, "testdb", "mysql", "staging")
	require.NoError(t, err)
	assert.Len(t, applies, 1)
}

func TestApplyStore_CreateAllowsConcurrentActiveAppliesForDifferentTargets(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	type applyTarget struct {
		database    string
		dbType      string
		environment string
		engine      string
	}
	targets := make([]applyTarget, 0, 16)
	for i := range 8 {
		targets = append(targets, applyTarget{
			database:    "testapp",
			dbType:      "mysql",
			environment: "env-" + strconv.Itoa(i),
			engine:      "spirit",
		})
		targets = append(targets, applyTarget{
			database:    "testapp-vitess",
			dbType:      "vitess",
			environment: "env-" + strconv.Itoa(i),
			engine:      "planetscale",
		})
	}

	locks := make(map[string]*storage.Lock)
	for _, target := range targets {
		key := target.database + "/" + target.dbType
		if _, ok := locks[key]; !ok {
			locks[key] = createTestLock(t, store, target.database, target.dbType, target.environment)
		}
	}

	start := make(chan struct{})
	errs := make(chan error, len(targets))
	var wg sync.WaitGroup
	for i, target := range targets {
		lock := locks[target.database+"/"+target.dbType]
		wg.Go(func() {
			<-start
			// These creates all start at the same time, but every apply targets
			// a different database/type/environment. Storage should serialize
			// only same-target active applies, so every independent target can
			// create its first active apply successfully.
			_, err := store.Applies().Create(ctx, &storage.Apply{
				ApplyIdentifier: "apply_concurrent_target_" + strconv.Itoa(i),
				LockID:          lock.ID,
				PlanID:          int64(20 + i),
				Database:        target.database,
				DatabaseType:    target.dbType,
				Repository:      "org/repo",
				PullRequest:     123,
				Environment:     target.environment,
				Engine:          target.engine,
				State:           state.Apply.Pending,
			})
			errs <- err
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	close(start)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		require.Fail(t, "concurrent active apply creates for different targets blocked")
	}
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	for _, target := range targets {
		applies, err := store.Applies().GetByDatabase(ctx, target.database, target.dbType, target.environment)
		require.NoError(t, err)
		assert.Len(t, applies, 1)
	}
}

func TestApplyStore_Get(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")

	// Get non-existent should return nil
	apply, err := store.Applies().Get(ctx, 99999)
	require.NoError(t, err)
	require.Nil(t, apply)

	// Create apply
	created := createTestApply(t, store, lock, "apply_get_test", 123)

	// Get should return the apply
	apply, err = store.Applies().Get(ctx, created.ID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	require.Equal(t, "apply_get_test", apply.ApplyIdentifier)
	require.Equal(t, "testdb", apply.Database)
}

func TestApplyStore_GetByApplyIdentifier(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")

	// Get non-existent should return nil
	apply, err := store.Applies().GetByApplyIdentifier(ctx, "nonexistent")
	require.NoError(t, err)
	require.Nil(t, apply)

	// Create apply
	createTestApply(t, store, lock, "apply_byid_test", 42)

	// GetByApplyIdentifier should return the apply
	apply, err = store.Applies().GetByApplyIdentifier(ctx, "apply_byid_test")
	require.NoError(t, err)
	require.NotNil(t, apply)
	require.Equal(t, int64(42), apply.PlanID)
}

func TestApplyStore_GetByPlan(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")

	// Get non-existent should return nil
	apply, err := store.Applies().GetByPlan(ctx, 99999)
	require.NoError(t, err)
	require.Nil(t, apply)

	// Create apply with a specific plan_id
	created := createTestApply(t, store, lock, "apply_byplan", 12345)

	// GetByPlan should return the apply
	apply, err = store.Applies().GetByPlan(ctx, 12345)
	require.NoError(t, err)
	require.NotNil(t, apply)
	require.Equal(t, created.ApplyIdentifier, apply.ApplyIdentifier)
}

func TestApplyStore_GetByLock(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")

	// GetByLock with no applies should return empty slice
	applies, err := store.Applies().GetByLock(ctx, lock.ID)
	require.NoError(t, err)
	require.Empty(t, applies)

	// Create two applies for the same lock.
	first := createTestApply(t, store, lock, "apply_first", 100)
	first.State = state.Apply.Completed
	require.NoError(t, store.Applies().Update(ctx, first))
	createTestApply(t, store, lock, "apply_second", 101)

	// GetByLock should return both applies
	applies, err = store.Applies().GetByLock(ctx, lock.ID)
	require.NoError(t, err)
	require.Len(t, applies, 2)
}

func TestApplyStore_GetByDatabase(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Create locks for different databases
	lock1 := createTestLock(t, store, "db1", "mysql", "staging")
	lock2 := createTestLock(t, store, "db2", "mysql", "staging")

	// Create applies
	createTestApply(t, store, lock1, "apply_db1", 200)
	createTestApply(t, store, lock2, "apply_db2", 201)

	// GetByDatabase should only return applies for db1
	applies, err := store.Applies().GetByDatabase(ctx, "db1", "mysql", "staging")
	require.NoError(t, err)
	require.Len(t, applies, 1)
	require.Equal(t, "apply_db1", applies[0].ApplyIdentifier)
}

func TestApplyStore_GetRecentLimitAndEnvironment(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "recentdb", "mysql", "staging")
	createTestApplyWithStateAndEnv(t, store, lock, "apply_recent_staging_old", 210, state.Apply.Completed, "staging")
	createTestApplyWithStateAndEnv(t, store, lock, "apply_recent_production", 211, state.Apply.Completed, "production")
	createTestApplyWithStateAndEnv(t, store, lock, "apply_recent_staging_new", 212, state.Apply.Completed, "staging")
	createTestApplyWithStateAndEnv(t, store, lock, "apply_recent_staging_failed", 213, state.Apply.Failed, "staging")

	applies, err := store.Applies().GetRecent(ctx, storage.RecentAppliesFilter{
		Limit:       1,
		Environment: "staging",
	})
	require.NoError(t, err)
	require.Len(t, applies, 1)
	assert.Equal(t, "apply_recent_staging_failed", applies[0].ApplyIdentifier)

	applies, err = store.Applies().GetRecent(ctx, storage.RecentAppliesFilter{Limit: 2})
	require.NoError(t, err)
	require.Len(t, applies, 2)
	assert.Equal(t, "apply_recent_staging_failed", applies[0].ApplyIdentifier)
	assert.Equal(t, "apply_recent_staging_new", applies[1].ApplyIdentifier)

	applies, err = store.Applies().GetRecent(ctx, storage.RecentAppliesFilter{
		Limit:       10,
		Environment: "staging",
		States:      []string{state.Apply.Failed, state.Apply.FailedRetryable},
	})
	require.NoError(t, err)
	require.Len(t, applies, 1)
	assert.Equal(t, "apply_recent_staging_failed", applies[0].ApplyIdentifier)
}

func TestApplyStore_Update(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_update", 300)

	// Update state
	apply.State = state.Apply.Running
	apply.ErrorMessage = ""
	now := time.Now()
	apply.StartedAt = &now

	require.NoError(t, store.Applies().Update(ctx, apply))

	// Verify update
	updated, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.Equal(t, state.Apply.Running, updated.State)
	require.NotNil(t, updated.StartedAt)
}

func TestApplyStore_UpdateBlocksActiveApplyForSameTarget(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	active := createTestApply(t, store, lock, "apply_update_active", 301)
	completed := createTestApplyWithStateAndEnv(t, store, lock, "apply_update_completed", 302, state.Apply.Completed, "staging")

	completed.State = state.Apply.Running
	require.ErrorIs(t, store.Applies().Update(ctx, completed), storage.ErrActiveApplyExists)

	active.State = state.Apply.Completed
	require.NoError(t, store.Applies().Update(ctx, active))
	require.NoError(t, store.Applies().Update(ctx, completed))
}

func TestApplyStore_UpdateNonExistent(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	apply := &storage.Apply{
		ID:    99999,
		State: state.Apply.Running,
	}

	// Update on a non-existent row is a no-op (0 rows affected), not an error.
	// MySQL UPDATE with WHERE id=? succeeds even when no row matches.
	require.NoError(t, store.Applies().Update(ctx, apply))
}

func TestApplyStore_GetInProgress(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")

	pending := createTestApply(t, store, lock, "apply_pending", 400)
	running := createTestApplyWithStateAndEnv(t, store, lock, "apply_running", 401, state.Apply.Running, "production")
	completed := createTestApplyWithStateAndEnv(t, store, lock, "apply_completed", 402, state.Apply.Completed, "staging")
	failed := createTestApplyWithStateAndEnv(t, store, lock, "apply_failed", 403, state.Apply.Failed, "staging")

	require.NotZero(t, completed.ID)
	require.NotZero(t, failed.ID)

	// GetInProgress should return only pending and running
	applies, err := store.Applies().GetInProgress(ctx)
	require.NoError(t, err)
	require.Len(t, applies, 2)

	// Verify we got the right ones
	applyIDs := make(map[string]bool)
	for _, a := range applies {
		applyIDs[a.ApplyIdentifier] = true
	}
	assert.True(t, applyIDs[pending.ApplyIdentifier], "expected pending apply")
	assert.True(t, applyIDs[running.ApplyIdentifier], "expected running apply")
}

// TestApplyStore_FindNextApplyClaimsRetryable verifies the storage-level retry
// claim behavior. The caller sees the retryable state that was claimed, while
// the stored row is leased as running with an incremented apply attempt.
func TestApplyStore_FindNextApplyClaimsRetryable(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_retryable_claim", 500, state.Apply.FailedRetryable, "staging")
	apply.ErrorMessage = "transient engine failure"
	require.NoError(t, store.Applies().Update(ctx, apply))

	claimed, err := store.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)
	assert.Equal(t, state.Apply.FailedRetryable, claimed.State)
	assert.Equal(t, 1, claimed.Attempt)
	assert.Empty(t, claimed.ErrorMessage)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.Running, persisted.State)
	assert.Equal(t, 1, persisted.Attempt)
	assert.Empty(t, persisted.ErrorMessage)

	claimedAgain, err := store.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	assert.Nil(t, claimedAgain)
}

// ClaimApplyByID claims one specific apply by ID with the same claimability
// rules as FindNextApply. The operation-level claim loop uses it to acquire the
// parent apply lease after claiming an apply_operations row, so a pending apply
// with tasks must be claimable, the claim must rotate a fresh lease, and a
// repeat claim must be rejected while the lease is fresh.
func TestApplyStore_ClaimApplyByIDClaimsPendingWithTasks(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_claim_by_id_pending", 600, state.Apply.Pending, "staging")
	addClaimByIDTask(t, store, apply, "task_claim_by_id")

	claimed, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-a")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)
	assert.Equal(t, state.Apply.Pending, claimed.State, "caller sees the pre-claim state")
	assert.Equal(t, "operator-a", claimed.LeaseOwner)
	assert.NotEmpty(t, claimed.LeaseToken)
	require.NotNil(t, claimed.LeaseAcquiredAt)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.Running, persisted.State)
	assert.Equal(t, "operator-a", persisted.LeaseOwner)

	again, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-b")
	require.NoError(t, err)
	assert.Nil(t, again, "a freshly claimed apply is owned by its current worker")
}

// ClaimApplyByID must not steal a fresh lease from a healthy apply-level worker,
// so a running apply with a fresh heartbeat is not claimable; it only becomes
// claimable once its heartbeat goes stale, matching FindNextApply recovery.
func TestApplyStore_ClaimApplyByIDSkipsFreshRunningUntilStale(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_claim_by_id_running", 601, state.Apply.Running, "staging")

	fresh, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-a")
	require.NoError(t, err)
	assert.Nil(t, fresh, "a fresh running apply is owned by its active worker")

	_, err = testDB.ExecContext(ctx, `
		UPDATE applies
		SET updated_at = NOW() - INTERVAL 2 MINUTE
		WHERE id = ?
	`, apply.ID)
	require.NoError(t, err)

	claimed, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-a")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, state.Apply.Running, claimed.State)
	assert.Equal(t, "operator-a", claimed.LeaseOwner)
	assert.NotEmpty(t, claimed.LeaseToken)
}

// ClaimApplyByID returns nil for applies that are not claimable (terminal) or
// do not exist, so the operation loop fails closed instead of driving work.
func TestApplyStore_ClaimApplyByIDReturnsNilForTerminalAndMissing(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	completed := createTestApplyWithStateAndEnv(t, store, lock, "apply_claim_by_id_done", 602, state.Apply.Completed, "staging")

	claimed, err := store.Applies().ClaimApplyByID(ctx, completed.ID, "operator-a")
	require.NoError(t, err)
	assert.Nil(t, claimed, "terminal applies are never claimable")

	missing, err := store.Applies().ClaimApplyByID(ctx, 9_999_999, "operator-a")
	require.NoError(t, err)
	assert.Nil(t, missing, "a non-existent apply id yields no claim")
}

// ClaimApplyByID requires an owner so a lease can never be acquired without an
// identity that lease-guarded writes can fail closed against.
func TestApplyStore_ClaimApplyByIDRequiresOwner(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	_, err := store.Applies().ClaimApplyByID(ctx, 1, "")
	require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
}

func addClaimByIDTask(t *testing.T, store *Storage, apply *storage.Apply, taskID string) {
	t.Helper()
	task := &storage.Task{
		TaskIdentifier: taskID,
		ApplyID:        apply.ID,
		PlanID:         apply.PlanID,
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Engine:         storage.EngineSpirit,
		Environment:    apply.Environment,
		State:          state.Task.Pending,
		TableName:      "users",
		DDL:            "ALTER TABLE `users` ADD COLUMN `note` varchar(255)",
		DDLAction:      "alter",
		Options:        []byte("{}"),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	id, err := store.Tasks().Create(t.Context(), task)
	require.NoError(t, err)
	task.ID = id
}

func TestApplyStore_LeaseGuardsOwnedWrites(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_lease", 507, state.Apply.Pending, "staging")
	task := &storage.Task{
		TaskIdentifier: "task_lease_users",
		ApplyID:        apply.ID,
		PlanID:         apply.PlanID,
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Engine:         storage.EngineSpirit,
		Environment:    apply.Environment,
		State:          state.Task.Pending,
		TableName:      "users",
		DDL:            "ALTER TABLE `users` ADD COLUMN `lease_note` varchar(255)",
		DDLAction:      "alter",
		Options:        []byte("{}"),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	taskID, err := store.Tasks().Create(ctx, task)
	require.NoError(t, err)
	task.ID = taskID

	claimed, err := store.Applies().FindNextApply(ctx, "worker-a")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, "worker-a", claimed.LeaseOwner)
	require.NotEmpty(t, claimed.LeaseToken)
	require.NotNil(t, claimed.LeaseAcquiredAt)

	staleCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: apply.ID, Owner: "worker-old", Token: "stale-token"})
	claimed.State = state.Apply.Failed
	claimed.ErrorMessage = "stale worker failure"
	require.ErrorIs(t, store.Applies().Update(staleCtx, claimed), storage.ErrApplyLeaseLost)
	require.ErrorIs(t, store.Applies().Heartbeat(staleCtx, apply.ID), storage.ErrApplyLeaseLost)

	task.State = state.Task.Completed
	require.ErrorIs(t, store.Tasks().Update(staleCtx, task), storage.ErrApplyLeaseLost)
	require.ErrorIs(t, store.ApplyLogs().Append(staleCtx, &storage.ApplyLog{
		ApplyID:   apply.ID,
		Level:     storage.LogLevelInfo,
		EventType: storage.LogEventStateTransition,
		Source:    storage.LogSourceSchemaBot,
		Message:   "stale worker log",
	}), storage.ErrApplyLeaseLost)

	persistedApply, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persistedApply)
	assert.Equal(t, state.Apply.Running, persistedApply.State)
	assert.Empty(t, persistedApply.ErrorMessage)

	persistedTask, err := store.Tasks().Get(ctx, task.TaskIdentifier)
	require.NoError(t, err)
	require.NotNil(t, persistedTask)
	assert.Equal(t, state.Task.Pending, persistedTask.State)

	logs, err := store.ApplyLogs().GetByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Empty(t, logs)

	ownedCtx := storage.WithApplyLease(ctx, claimed.Lease())
	claimed.State = state.Apply.Completed
	claimed.ErrorMessage = ""
	require.NoError(t, store.Applies().Update(ownedCtx, claimed))
	task.State = state.Task.Completed
	require.NoError(t, store.Tasks().Update(ownedCtx, task))
	require.NoError(t, store.ApplyLogs().Append(ownedCtx, &storage.ApplyLog{
		ApplyID:   apply.ID,
		Level:     storage.LogLevelInfo,
		EventType: storage.LogEventStateTransition,
		Source:    storage.LogSourceSchemaBot,
		Message:   "owned worker log",
	}))
}

// TestApplyStore_FindNextApplySkipsOldRetryable verifies that automatic
// operator recovery only redispatches recently updated retryable failures.
// Old failures require deliberate operator action instead of being picked up
// later by a policy or retry-budget change.
func TestApplyStore_FindNextApplySkipsOldRetryable(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_old_retryable_claim", 501, state.Apply.FailedRetryable, "staging")
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies
		SET updated_at = NOW() - INTERVAL 2 DAY
		WHERE id = ?
	`, apply.ID)
	require.NoError(t, err)

	claimed, err := store.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	assert.Nil(t, claimed)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.FailedRetryable, persisted.State)
	assert.Equal(t, 0, persisted.Attempt)
}

func TestApplyStore_FindNextApplyRequiresTasksForPendingApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_pending_claim", 502, state.Apply.Pending, "staging")

	// A pending apply record can be visible before its task rows are written.
	// The operator must wait for the task list so dispatch has concrete table
	// work to run.
	claimedBeforeTasks, err := store.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	assert.Nil(t, claimedBeforeTasks, "pending applies are not ready for operator dispatch until their tasks are persisted")

	now := time.Now()
	_, err = store.Tasks().Create(ctx, &storage.Task{
		TaskIdentifier: "task_pending_claim",
		ApplyID:        apply.ID,
		PlanID:         apply.PlanID,
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Engine:         storage.EngineSpirit,
		Environment:    apply.Environment,
		State:          state.Task.Pending,
		TableName:      "users",
		DDL:            "CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY)",
		DDLAction:      "CREATE",
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	require.NoError(t, err)

	// Once at least one task exists, the pending apply is ready to claim. The
	// caller sees the state it claimed, and the stored row is leased as running.
	claimed, err := store.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)
	assert.Equal(t, state.Apply.Pending, claimed.State)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.Running, persisted.State)
}

func TestApplyStore_FindNextApplyClaimsPendingControlRequestWithoutTasks(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_pending_start_request", 503, state.Apply.Pending, "staging")
	_, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:   apply.ID,
		Operation: storage.ControlOperationStart,
		Status:    storage.ControlRequestPending,
		Metadata:  []byte(`{}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	claimed, err := store.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)
	assert.Equal(t, state.Apply.Pending, claimed.State)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.Running, persisted.State)

	claimedAgain, err := store.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	assert.Nil(t, claimedAgain, "claim heartbeat should prevent another worker from immediately taking the same start request")
}

func TestApplyStore_FindNextApplyClaimsStoppedStartControlRequest(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_stopped_start_request", 504, state.Apply.Stopped, "staging")
	_, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:   apply.ID,
		Operation: storage.ControlOperationStart,
		Status:    storage.ControlRequestPending,
		Metadata:  []byte(`{}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	claimed, err := store.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)
	assert.Equal(t, state.Apply.Stopped, claimed.State)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.Running, persisted.State)

	claimedAgain, err := store.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	assert.Nil(t, claimedAgain, "claim transition should prevent another worker from taking the same stopped start request")
}

func TestApplyStore_FindNextApplyClaimsStaleWaitingForCutoverControlRequest(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_cutover_request", 506, state.Apply.WaitingForCutover, "staging")
	_, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:   apply.ID,
		Operation: storage.ControlOperationCutover,
		Status:    storage.ControlRequestPending,
		Metadata:  []byte(`{}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	freshClaim, err := store.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	assert.Nil(t, freshClaim, "fresh waiting-for-cutover applies are owned by their active worker")

	_, err = testDB.ExecContext(ctx, `
		UPDATE applies
		SET updated_at = NOW() - INTERVAL 2 MINUTE
		WHERE id = ?
	`, apply.ID)
	require.NoError(t, err)

	claimed, err := store.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)
	assert.Equal(t, state.Apply.WaitingForCutover, claimed.State)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.WaitingForCutover, persisted.State)

	claimedAgain, err := store.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	assert.Nil(t, claimedAgain, "claim heartbeat should prevent another worker from immediately taking the same stale cutover request")
}

func TestApplyStore_FindNextApplySkipsFailedStoppedStartControlRequest(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_failed_start_request", 505, state.Apply.Stopped, "staging")
	_, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:   apply.ID,
		Operation: storage.ControlOperationStart,
		Status:    storage.ControlRequestPending,
		Metadata:  []byte(`{}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)
	require.NoError(t, store.ControlRequests().FailPending(ctx, apply.ID, storage.ControlOperationStart, "remote start failed"))

	claimed, err := store.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	assert.Nil(t, claimed, "failed start requests should not be retried automatically by operator claims")

	reset, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator-retry",
		Metadata:    []byte(`{"started_count":1}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)
	assert.Equal(t, storage.ControlRequestPending, reset.Status)
	assert.Equal(t, "operator-retry", reset.RequestedBy)

	claimedAfterRetry, err := store.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimedAfterRetry)
	assert.Equal(t, apply.ApplyIdentifier, claimedAfterRetry.ApplyIdentifier)
}

func TestApplyStore_FindNextApplyDoesNotClaimFreshRunningStopControlRequest(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_running_stop_request", 505, state.Apply.Running, "staging")
	_, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:   apply.ID,
		Operation: storage.ControlOperationStop,
		Status:    storage.ControlRequestPending,
		Metadata:  []byte(`{}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	claimed, err := store.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	assert.Nil(t, claimed, "fresh running applies are owned by their active worker; pending stop must not create a second owner")
}

func TestApplyStore_FindNextApplyConcurrentPendingClaims(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: "apply_concurrent_pending_claim",
		LockID:          lock.ID,
		PlanID:          503,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Pending,
		Options:         []byte("{}"),
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	tasks := []*storage.Task{
		{
			TaskIdentifier: "task_concurrent_pending_claim",
			PlanID:         503,
			Database:       "testdb",
			DatabaseType:   storage.DatabaseTypeMySQL,
			Engine:         storage.EngineSpirit,
			Environment:    "staging",
			State:          state.Task.Pending,
			TableName:      "users",
			DDL:            "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
			DDLAction:      "alter",
			Options:        []byte("{}"),
			CreatedAt:      now,
			UpdatedAt:      now,
		},
	}
	applyID, err := store.Applies().CreateWithTasks(ctx, apply, tasks)
	require.NoError(t, err)
	apply.ID = applyID

	const workers = 16
	stores := make([]*Storage, workers)
	for i := range workers {
		db, openErr := sql.Open("mysql", testDSNChangedRows)
		require.NoError(t, openErr)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		t.Cleanup(func() {
			require.NoError(t, db.Close())
		})
		stores[i] = New(db)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var claimed []*storage.Apply
	var claimErrors []error

	for i := range workers {
		workerStore := stores[i]
		wg.Go(func() {
			<-start
			got, claimErr := workerStore.Applies().FindNextApply(ctx, "test-owner")

			mu.Lock()
			defer mu.Unlock()
			if claimErr != nil {
				claimErrors = append(claimErrors, claimErr)
				return
			}
			if got != nil {
				claimed = append(claimed, got)
			}
		})
	}

	close(start)
	wg.Wait()

	require.Empty(t, claimErrors)
	require.Len(t, claimed, 1, "only one operator worker should claim a pending apply")
	assert.Equal(t, apply.ApplyIdentifier, claimed[0].ApplyIdentifier)
	assert.Equal(t, state.Apply.Pending, claimed[0].State)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.Running, persisted.State)
}

// TestApplyStore_ExpireRetryable verifies retryable expiry at the storage
// layer. A retryable apply that has used all attempts becomes failed, and
// unfinished tasks are finalized as failed with completion timestamps.
func TestApplyStore_ExpireRetryable(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_retryable_expired", 501, state.Apply.FailedRetryable, "staging")
	apply.Attempt = maxRecoveryAttempts
	require.NoError(t, store.Applies().Update(ctx, apply))

	now := time.Now()
	_, err := store.Tasks().Create(ctx, &storage.Task{
		TaskIdentifier: "task_retryable_expired",
		ApplyID:        apply.ID,
		PlanID:         apply.PlanID,
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Engine:         storage.EngineSpirit,
		Environment:    apply.Environment,
		State:          state.Task.FailedRetryable,
		Options:        []byte("{}"),
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	require.NoError(t, err)
	_, err = store.Tasks().Create(ctx, &storage.Task{
		TaskIdentifier: "task_retryable_pending",
		ApplyID:        apply.ID,
		PlanID:         apply.PlanID,
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Engine:         storage.EngineSpirit,
		Environment:    apply.Environment,
		State:          state.Task.Pending,
		TableName:      "posts",
		Options:        []byte("{}"),
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	require.NoError(t, err)

	expired, err := store.Applies().ExpireRetryable(ctx)
	require.NoError(t, err)
	require.Len(t, expired, 1)
	assert.Equal(t, storage.RetryableExpirationAttemptBudget, expired[0].Reason)
	assert.Equal(t, apply.ApplyIdentifier, expired[0].Apply.ApplyIdentifier)
	assert.Equal(t, state.Apply.Failed, expired[0].Apply.State)
	assert.NotNil(t, expired[0].Apply.CompletedAt)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.Failed, persisted.State)
	assert.NotNil(t, persisted.CompletedAt)

	task, err := store.Tasks().Get(ctx, "task_retryable_expired")
	require.NoError(t, err)
	require.NotNil(t, task)
	assert.Equal(t, state.Task.Failed, task.State)
	assert.NotNil(t, task.CompletedAt)

	pendingTask, err := store.Tasks().Get(ctx, "task_retryable_pending")
	require.NoError(t, err)
	require.NotNil(t, pendingTask)
	assert.Equal(t, state.Task.Failed, pendingTask.State)
	assert.NotNil(t, pendingTask.CompletedAt)
}

// TestApplyStore_ExpireRetryableExpiresOldFailures verifies that retryable
// failures are not kept non-terminal forever after their recovery window passes.
func TestApplyStore_ExpireRetryableExpiresOldFailures(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_retryable_old_expired", 502, state.Apply.FailedRetryable, "staging")
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies
		SET updated_at = NOW() - INTERVAL 2 DAY
		WHERE id = ?
	`, apply.ID)
	require.NoError(t, err)

	expired, err := store.Applies().ExpireRetryable(ctx)
	require.NoError(t, err)
	require.Len(t, expired, 1)
	assert.Equal(t, storage.RetryableExpirationRecoveryWindow, expired[0].Reason)
	assert.Equal(t, apply.ApplyIdentifier, expired[0].Apply.ApplyIdentifier)
	assert.Equal(t, state.Apply.Failed, expired[0].Apply.State)
	assert.Equal(t, 0, expired[0].Apply.Attempt)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.Failed, persisted.State)
	assert.NotNil(t, persisted.CompletedAt)
}

func TestApplyStore_FindMissingSummaryComment_ExcludesAppliesWithoutGitHubDestination(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	now := time.Now()
	startedAt := now.Add(-time.Minute)

	githubLock := createTestLockWithPR(t, store, "github_db", storage.DatabaseTypeMySQL, "staging", "org/repo", 123)
	githubApply := &storage.Apply{
		ApplyIdentifier: "apply_missing_summary_github",
		LockID:          githubLock.ID,
		PlanID:          600,
		Database:        githubLock.DatabaseName,
		DatabaseType:    githubLock.DatabaseType,
		Repository:      githubLock.Repository,
		PullRequest:     githubLock.PullRequest,
		Environment:     "staging",
		Caller:          "org/repo#123",
		InstallationID:  12345,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Completed,
	}
	githubApplyID, err := store.Applies().Create(ctx, githubApply)
	require.NoError(t, err)
	githubApply.ID = githubApplyID
	githubApply.StartedAt = &startedAt
	githubApply.CompletedAt = &now
	require.NoError(t, store.Applies().Update(ctx, githubApply))
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         githubApply.ID,
		CommentState:    state.Comment.Progress,
		GitHubCommentID: 1001,
	}))

	cliLock := createTestLockWithPR(t, store, "cli_db", storage.DatabaseTypeMySQL, "staging", "", 0)
	cliApply := &storage.Apply{
		ApplyIdentifier: "apply_missing_summary_cli",
		LockID:          cliLock.ID,
		PlanID:          601,
		Database:        cliLock.DatabaseName,
		DatabaseType:    cliLock.DatabaseType,
		Repository:      cliLock.Repository,
		PullRequest:     cliLock.PullRequest,
		Environment:     "staging",
		Caller:          "cli:user@host",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Completed,
	}
	cliApplyID, err := store.Applies().Create(ctx, cliApply)
	require.NoError(t, err)
	cliApply.ID = cliApplyID
	cliApply.StartedAt = &startedAt
	cliApply.CompletedAt = &now
	require.NoError(t, store.Applies().Update(ctx, cliApply))

	// Even if a CLI-style apply somehow has a progress marker, it cannot be
	// reconciled into a GitHub summary without repository, PR, and installation ID.
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         cliApply.ID,
		CommentState:    state.Comment.Progress,
		GitHubCommentID: 1002,
	}))

	applies, err := store.Applies().FindMissingSummaryComment(ctx)
	require.NoError(t, err)
	require.Len(t, applies, 1)
	assert.Equal(t, githubApply.ApplyIdentifier, applies[0].ApplyIdentifier)
}

func TestApplyStore_GetByPR(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Create locks for different PRs
	lock1 := createTestLockWithPR(t, store, "db1", "mysql", "staging", "org/repo", 100)
	lock2 := createTestLockWithPR(t, store, "db2", "mysql", "staging", "org/repo", 200)

	// Create applies
	createTestApply(t, store, lock1, "apply_pr100", 500)
	createTestApply(t, store, lock2, "apply_pr200", 501)

	// GetByPR should only return applies for PR 100
	applies, err := store.Applies().GetByPR(ctx, "org/repo", 100)
	require.NoError(t, err)
	require.Len(t, applies, 1)
	require.Equal(t, 100, applies[0].PullRequest)
}

func TestApplyStore_Delete(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_delete", 600)

	// Delete should succeed
	require.NoError(t, store.Applies().Delete(ctx, apply.ID))

	// Verify deleted
	deleted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.Nil(t, deleted)

	// Delete non-existent should fail
	require.ErrorIs(t, store.Applies().Delete(ctx, apply.ID), storage.ErrApplyNotFound)
}

func TestApplyStore_DeleteByPR(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	// Create locks for different PRs
	lock1 := createTestLockWithPR(t, store, "db1", "mysql", "staging", "org/repo", 100)
	lock2 := createTestLockWithPR(t, store, "db2", "mysql", "staging", "org/repo", 100)
	lock3 := createTestLockWithPR(t, store, "db3", "mysql", "staging", "org/repo", 200)

	// Create applies
	createTestApply(t, store, lock1, "apply_pr100_1", 701)
	createTestApply(t, store, lock2, "apply_pr100_2", 702)
	createTestApply(t, store, lock3, "apply_pr200", 703)

	// DeleteByPR should only delete applies for PR 100
	require.NoError(t, store.Applies().DeleteByPR(ctx, "org/repo", 100))

	// Verify PR 100 applies deleted
	applies, err := store.Applies().GetByPR(ctx, "org/repo", 100)
	require.NoError(t, err)
	require.Empty(t, applies)

	// Verify PR 200 apply still exists
	applies, err = store.Applies().GetByPR(ctx, "org/repo", 200)
	require.NoError(t, err)
	require.Len(t, applies, 1)
}

// TestApplyStore_Delete_RemovesApplyOperations verifies that deleting an apply
// also removes its per-deployment apply_operations rows in the same transaction.
// Orphan child rows would otherwise be re-claimed forever by the operator claim
// loop, since their parent lookup returns nil.
func TestApplyStore_Delete_RemovesApplyOperations(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_delete_ops", 610)
	opID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a",
	})
	require.NoError(t, err)

	require.NoError(t, store.Applies().Delete(ctx, apply.ID))

	deleted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.Nil(t, deleted)

	op, err := store.ApplyOperations().Get(ctx, opID)
	require.NoError(t, err)
	require.Nil(t, op, "apply_operations row must be deleted with its parent apply")
}

// TestApplyStore_DeleteByPR_RemovesApplyOperations verifies that DeleteByPR
// removes the apply_operations rows of the deleted applies while leaving other
// PRs' operations intact.
func TestApplyStore_DeleteByPR_RemovesApplyOperations(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock1 := createTestLockWithPR(t, store, "db1", "mysql", "staging", "org/repo", 110)
	lock2 := createTestLockWithPR(t, store, "db2", "mysql", "staging", "org/repo", 210)
	apply1 := createTestApply(t, store, lock1, "apply_pr110", 711)
	apply2 := createTestApply(t, store, lock2, "apply_pr210", 712)

	op1, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply1.ID, Deployment: "region-a",
	})
	require.NoError(t, err)
	op2, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply2.ID, Deployment: "region-a",
	})
	require.NoError(t, err)

	require.NoError(t, store.Applies().DeleteByPR(ctx, "org/repo", 110))

	got1, err := store.ApplyOperations().Get(ctx, op1)
	require.NoError(t, err)
	require.Nil(t, got1, "deleted PR's apply_operations row must be removed")

	got2, err := store.ApplyOperations().Get(ctx, op2)
	require.NoError(t, err)
	require.NotNil(t, got2, "other PR's apply_operations row must be preserved")
}

func TestApplyStore_Options(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")

	// Create apply with options
	apply := &storage.Apply{
		ApplyIdentifier: "apply_options_test",
		LockID:          lock.ID,
		PlanID:          800,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{
		AllowUnsafe:  true,
		DeferCutover: true,
		SkipRevert:   false,
		Volume:       5,
	})

	id, err := store.Applies().Create(ctx, apply)
	require.NoError(t, err)

	// Retrieve and verify options
	retrieved, err := store.Applies().Get(ctx, id)
	require.NoError(t, err)

	opts := retrieved.GetOptions()
	assert.True(t, opts.AllowUnsafe)
	assert.True(t, opts.DeferCutover)
	assert.False(t, opts.SkipRevert)
	assert.Equal(t, 5, opts.Volume)
}

func TestApplyStore_UpdateOptions(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := &storage.Apply{
		ApplyIdentifier: "apply_update_options_test",
		LockID:          lock.ID,
		PlanID:          801,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Engine:          "spirit",
		State:           state.Apply.Stopped,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "testdb"})

	id, err := store.Applies().Create(ctx, apply)
	require.NoError(t, err)

	retrieved, err := store.Applies().Get(ctx, id)
	require.NoError(t, err)
	retrieved.State = state.Apply.Pending

	require.NoError(t, store.Applies().Update(ctx, retrieved))

	updated, err := store.Applies().Get(ctx, id)
	require.NoError(t, err)
	updatedOpts := updated.GetOptions()
	assert.Equal(t, "testdb", updatedOpts.Target)

	partial := *updated
	partial.Options = nil
	partial.State = state.Apply.Running
	require.NoError(t, store.Applies().Update(ctx, &partial))

	preserved, err := store.Applies().Get(ctx, id)
	require.NoError(t, err)
	preservedOpts := preserved.GetOptions()
	assert.Equal(t, "testdb", preservedOpts.Target)
}

func TestApplyStore_AllFields(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")

	now := time.Now().Truncate(time.Second)
	apply := &storage.Apply{
		ApplyIdentifier: "apply_allfields",
		LockID:          lock.ID,
		PlanID:          900,
		Database:        "testdb",
		DatabaseType:    "mysql",
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "staging",
		Caller:          "cli:user@host",
		ExternalID:      "ext_remote_abc123",
		Engine:          "spirit",
		State:           state.Apply.WaitingForCutover,
		ErrorMessage:    "test error",
		Attempt:         3,
	}
	apply.SetOptions(storage.ApplyOptions{
		AllowUnsafe:  true,
		DeferCutover: true,
		SkipRevert:   true,
	})

	id, err := store.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = id

	// Update with timestamps
	apply.StartedAt = &now
	completedTime := now.Add(time.Hour)
	apply.CompletedAt = &completedTime
	apply.State = state.Apply.Completed

	require.NoError(t, store.Applies().Update(ctx, apply))

	// Retrieve and verify all fields
	retrieved, err := store.Applies().Get(ctx, id)
	require.NoError(t, err)

	assert.Equal(t, "apply_allfields", retrieved.ApplyIdentifier)
	assert.Equal(t, lock.ID, retrieved.LockID)
	assert.Equal(t, int64(900), retrieved.PlanID)
	assert.Equal(t, "testdb", retrieved.Database)
	assert.Equal(t, storage.DatabaseTypeMySQL, retrieved.DatabaseType)
	assert.Equal(t, "org/repo", retrieved.Repository)
	assert.Equal(t, 123, retrieved.PullRequest)
	assert.Equal(t, "staging", retrieved.Environment)
	assert.Equal(t, "cli:user@host", retrieved.Caller)
	assert.Equal(t, "ext_remote_abc123", retrieved.ExternalID)
	assert.Equal(t, "spirit", retrieved.Engine)
	assert.Equal(t, state.Apply.Completed, retrieved.State)
	assert.Equal(t, "test error", retrieved.ErrorMessage)
	assert.Equal(t, 3, retrieved.Attempt)
	assert.NotNil(t, retrieved.StartedAt)
	assert.NotNil(t, retrieved.CompletedAt)

	// Verify options
	opts := retrieved.GetOptions()
	assert.True(t, opts.AllowUnsafe)
	assert.True(t, opts.DeferCutover)
	assert.True(t, opts.SkipRevert)
}

// Helper functions

func createTestLock(t *testing.T, store *Storage, dbName, dbType, env string) *storage.Lock {
	t.Helper()
	return createTestLockWithPR(t, store, dbName, dbType, env, "org/repo", 123)
}

func createTestLockWithPR(t *testing.T, store *Storage, dbName, dbType, env, repo string, pr int) *storage.Lock {
	t.Helper()
	ctx := t.Context()

	_ = env // unused, but kept for API compatibility with tests

	lock := &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: dbType,
		Repository:   repo,
		PullRequest:  pr,
		Owner:        "testuser",
	}

	require.NoError(t, store.Locks().Acquire(ctx, lock))

	lock, err := store.Locks().Get(ctx, dbName, dbType)
	require.NoError(t, err)
	return lock
}

func createTestApply(t *testing.T, store *Storage, lock *storage.Lock, applyID string, planID int64) *storage.Apply {
	t.Helper()
	return createTestApplyWithEnv(t, store, lock, applyID, planID, "staging")
}

func createTestApplyWithEnv(t *testing.T, store *Storage, lock *storage.Lock, applyID string, planID int64, env string) *storage.Apply {
	t.Helper()
	return createTestApplyWithStateAndEnv(t, store, lock, applyID, planID, state.Apply.Pending, env)
}

func createTestApplyWithStateAndEnv(t *testing.T, store *Storage, lock *storage.Lock, applyID string, planID int64, applyState, env string) *storage.Apply {
	t.Helper()
	ctx := t.Context()

	apply := &storage.Apply{
		ApplyIdentifier: applyID,
		LockID:          lock.ID,
		PlanID:          planID,
		Database:        lock.DatabaseName,
		DatabaseType:    lock.DatabaseType,
		Repository:      lock.Repository,
		PullRequest:     lock.PullRequest,
		Environment:     env,
		Engine:          "spirit",
		State:           applyState,
	}

	id, err := store.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = id
	return apply
}

// DB error tests

func TestApplyStore_Create_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Applies().Create(t.Context(), &storage.Apply{
		ApplyIdentifier: "test",
		State:           state.Apply.Pending,
	})
	require.Error(t, err)
}

func TestApplyStore_Get_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Applies().Get(t.Context(), 1)
	require.Error(t, err)
}

func TestApplyStore_GetByApplyIdentifier_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Applies().GetByApplyIdentifier(t.Context(), "test")
	require.Error(t, err)
}

func TestApplyStore_GetByLock_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Applies().GetByLock(t.Context(), 1)
	require.Error(t, err)
}

func TestApplyStore_GetInProgress_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Applies().GetInProgress(t.Context())
	require.Error(t, err)
}

func TestApplyStore_Update_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.Applies().Update(t.Context(), &storage.Apply{ID: 1, State: "running"})
	require.Error(t, err)
}

func TestApplyStore_Delete_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.Applies().Delete(t.Context(), 1)
	require.Error(t, err)
}

func TestApplyStore_DeleteByPR_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	err = store.Applies().DeleteByPR(t.Context(), "org/repo", 123)
	require.Error(t, err)
}

func TestApplyStore_GetByDatabase_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Applies().GetByDatabase(t.Context(), "db", "mysql", "staging")
	require.Error(t, err)
}

func TestApplyStore_GetByPR_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Applies().GetByPR(t.Context(), "org/repo", 123)
	require.Error(t, err)
}

func TestApplyStore_GetByPlan_DBError(t *testing.T) {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := New(db)
	_, err = store.Applies().GetByPlan(t.Context(), 123)
	require.Error(t, err)
}
