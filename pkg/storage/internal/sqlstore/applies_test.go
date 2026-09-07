//go:build integration

package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/storagetest"
)

// The behavioral suite for ApplyStore lives in pkg/storage/storagetest and
// runs against every dialect via parity_test.go. The tests here cover only
// scenarios that require MySQL-specific SQL, connection semantics, or fixtures
// that the storage interface cannot express.

func TestApplyStore_Create(t *testing.T) {
	clearTables(t)
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
	created := createTestApply(t, store, lock, "apply_create_test", 1)

	require.NotZero(t, created.ID)
}

func TestApplyStore_CreateRejectsChangedLockIntent(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	require.NoError(t, store.Locks().Acquire(ctx, &storage.Lock{
		DatabaseName:  "testdb",
		DatabaseType:  storage.DatabaseTypeMySQL,
		Repository:    "org/repo",
		PullRequest:   123,
		Owner:         "org/repo#123",
		PendingPlanID: "rollback:replacement",
	}))
	lock, err := store.Locks().Get(ctx, "testdb", storage.DatabaseTypeMySQL)
	require.NoError(t, err)
	require.NotNil(t, lock)

	_, err = store.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier:       "apply_changed_lock_intent",
		LockID:                lock.ID,
		ExpectedLockOwner:     "org/repo#123",
		ExpectedPendingPlanID: "apply-original",
		PlanID:                1,
		Database:              "testdb",
		DatabaseType:          storage.DatabaseTypeMySQL,
		Repository:            "org/repo",
		PullRequest:           123,
		Environment:           "staging",
		Deployment:            "default",
		Engine:                storage.EngineSpirit,
		State:                 state.Apply.Pending,
	})
	require.ErrorIs(t, err, storage.ErrLockIntentChanged)

	applies, err := store.Applies().GetByPR(ctx, "org/repo", 123)
	require.NoError(t, err)
	assert.Empty(t, applies)
}

func TestApplyStore_CreateRejectsPendingPlanIntentWithoutOwner(t *testing.T) {
	clearTables(t)
	store := NewMySQL(testDB)

	_, err := store.Applies().Create(t.Context(), &storage.Apply{
		ApplyIdentifier:       "apply_plan_intent_without_owner",
		ExpectedPendingPlanID: "plan-123",
		Database:              "testdb",
		DatabaseType:          storage.DatabaseTypeMySQL,
		Environment:           "staging",
		State:                 state.Apply.Completed,
	})
	require.ErrorContains(t, err, "expected pending plan ID set without an expected lock owner")
}

func TestApplyStore_CreateWithMatchingLockIntent(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	tests := []struct {
		name          string
		pendingPlanID string
	}{
		{name: "pinned lock", pendingPlanID: "plan-abc"},
		{name: "unpinned lock", pendingPlanID: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearTables(t)
			require.NoError(t, store.Locks().Acquire(ctx, &storage.Lock{
				DatabaseName:  "testdb",
				DatabaseType:  storage.DatabaseTypeMySQL,
				Repository:    "org/repo",
				PullRequest:   123,
				Owner:         "org/repo#123",
				PendingPlanID: tt.pendingPlanID,
			}))
			lock, err := store.Locks().Get(ctx, "testdb", storage.DatabaseTypeMySQL)
			require.NoError(t, err)
			require.NotNil(t, lock)

			id, err := store.Applies().Create(ctx, &storage.Apply{
				ApplyIdentifier:       "apply_matching_intent_" + strings.ReplaceAll(tt.name, " ", "_"),
				ExpectedLockOwner:     "org/repo#123",
				ExpectedPendingPlanID: tt.pendingPlanID,
				PlanID:                1,
				Database:              "testdb",
				DatabaseType:          storage.DatabaseTypeMySQL,
				Repository:            "org/repo",
				PullRequest:           123,
				Environment:           "staging",
				Deployment:            "default",
				Engine:                storage.EngineSpirit,
				State:                 state.Apply.Pending,
			})
			require.NoError(t, err)
			require.NotZero(t, id)

			created, err := store.Applies().Get(ctx, id)
			require.NoError(t, err)
			require.NotNil(t, created)
			assert.Equal(t, lock.ID, created.LockID)
		})
	}
}

func TestApplyStore_CreateRejectsIntentAgainstUnpinnedLock(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	require.NoError(t, store.Locks().Acquire(ctx, &storage.Lock{
		DatabaseName: "testdb",
		DatabaseType: storage.DatabaseTypeMySQL,
		Repository:   "org/repo",
		PullRequest:  123,
		Owner:        "org/repo#123",
	}))

	_, err := store.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier:       "apply_pinned_intent_unpinned_lock",
		ExpectedLockOwner:     "org/repo#123",
		ExpectedPendingPlanID: "plan-abc",
		PlanID:                1,
		Database:              "testdb",
		DatabaseType:          storage.DatabaseTypeMySQL,
		Repository:            "org/repo",
		PullRequest:           123,
		Environment:           "staging",
		Deployment:            "default",
		Engine:                storage.EngineSpirit,
		State:                 state.Apply.Pending,
	})
	require.ErrorIs(t, err, storage.ErrLockIntentChanged)
}

func TestApplyStore_CreateDuplicate(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")

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
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
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
	// operator dispatch; drivers never see a partially populated task list.
	claimed, err := store.Applies().ClaimApplyByID(ctx, applyID, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)
}

func TestApplyStore_CreateWithTasksAndOperationsCommitsAtomically(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "payments", storage.DatabaseTypeMySQL)
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

// Grouped operation creation links each deployment's independent task copies to that deployment's operation.
func TestApplyStore_CreateWithGroupedOperationsLinksTasksPerOperation(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)
	now := time.Now()
	apply := newGroupedCreateApply(now, "apply_grouped_multi")
	groups := []*storage.ApplyOperationWithTasks{
		newGroupedCreateGroup(now, "payments-a", "payments-a-target", "users", "orders"),
		newGroupedCreateGroup(now, "payments-b", "payments-b-target", "users", "orders"),
		newGroupedCreateGroup(now, "payments-c", "payments-c-target", "users", "orders"),
	}

	applyID, err := store.Applies().CreateWithGroupedOperations(ctx, apply, groups)
	require.NoError(t, err)
	require.NotZero(t, applyID)

	storedOps, err := store.ApplyOperations().ListByApply(ctx, applyID)
	require.NoError(t, err)
	require.Len(t, storedOps, 3)
	for i, op := range storedOps {
		assert.Equal(t, applyID, op.ApplyID)
		assert.Equal(t, groups[i].Operation.Deployment, op.Deployment)
		assert.Equal(t, groups[i].Operation.Target, op.Target)
	}

	storedTasks, err := store.Tasks().GetByApplyID(ctx, applyID)
	require.NoError(t, err)
	require.Len(t, storedTasks, 6)
	storedTaskCountsByOp := map[int64]int{}
	for _, task := range storedTasks {
		require.NotNil(t, task.ApplyOperationID)
		assert.NotZero(t, *task.ApplyOperationID)
		storedTaskCountsByOp[*task.ApplyOperationID]++
	}
	for _, group := range groups {
		require.NotZero(t, group.Operation.ID)
		assert.Equal(t, applyID, group.Operation.ApplyID)
		assert.Equal(t, 2, storedTaskCountsByOp[group.Operation.ID])
		for _, task := range group.Tasks {
			require.NotNil(t, task.ApplyOperationID)
			assert.Equal(t, group.Operation.ID, *task.ApplyOperationID)
			assert.Equal(t, applyID, task.ApplyID)
		}
	}
}

// Single-group creation produces the same operation/task ownership shape as the single-operation create path.
func TestApplyStore_CreateWithGroupedOperationsSingleGroupMatchesSingleOperationCreate(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)
	now := time.Now()

	// The two applies share the same operation (deployment, target) shape but
	// sit in different environments so the per-environment active-apply target
	// lock does not reject the second create — both are non-terminal.
	singleApply := newGroupedCreateApply(now, "apply_grouped_single_flat")
	singleApply.Environment = "staging"
	singleTask := newGroupedCreateTask(now, "payments-a", "users")
	singleOperation := &storage.ApplyOperation{Deployment: "payments-a", Target: "payments-target", State: state.ApplyOperation.Pending, CreatedAt: now, UpdatedAt: now}
	singleApplyID, err := store.Applies().CreateWithTasksAndOperations(ctx, singleApply, []*storage.Task{singleTask}, []*storage.ApplyOperation{singleOperation})
	require.NoError(t, err)

	groupedApply := newGroupedCreateApply(now, "apply_grouped_single_group")
	groupedTask := newGroupedCreateTask(now, "payments-a", "users")
	// Distinct task identifier (the column is globally unique) while keeping the
	// table/DDL identical so the shape assertions below still hold.
	groupedTask.TaskIdentifier = "task_grouped_payments-a_users"
	groupedOperation := &storage.ApplyOperation{Deployment: "payments-a", Target: "payments-target", State: state.ApplyOperation.Pending, CreatedAt: now, UpdatedAt: now}
	groupedApplyID, err := store.Applies().CreateWithGroupedOperations(ctx, groupedApply, []*storage.ApplyOperationWithTasks{{Operation: groupedOperation, Tasks: []*storage.Task{groupedTask}}})
	require.NoError(t, err)

	singleOps, err := store.ApplyOperations().ListByApply(ctx, singleApplyID)
	require.NoError(t, err)
	groupedOps, err := store.ApplyOperations().ListByApply(ctx, groupedApplyID)
	require.NoError(t, err)
	require.Len(t, singleOps, 1)
	require.Len(t, groupedOps, 1)
	assert.Equal(t, singleOps[0].Deployment, groupedOps[0].Deployment)
	assert.Equal(t, singleOps[0].Target, groupedOps[0].Target)
	assert.Equal(t, singleOps[0].State, groupedOps[0].State)

	singleTasks, err := store.Tasks().GetByApplyID(ctx, singleApplyID)
	require.NoError(t, err)
	groupedTasks, err := store.Tasks().GetByApplyID(ctx, groupedApplyID)
	require.NoError(t, err)
	require.Len(t, singleTasks, 1)
	require.Len(t, groupedTasks, 1)
	assert.Equal(t, singleTasks[0].TableName, groupedTasks[0].TableName)
	assert.Equal(t, singleTasks[0].DDL, groupedTasks[0].DDL)
	require.NotNil(t, singleTasks[0].ApplyOperationID)
	require.NotNil(t, groupedTasks[0].ApplyOperationID)
	assert.Equal(t, singleOperation.ID, *singleTasks[0].ApplyOperationID)
	assert.Equal(t, groupedOperation.ID, *groupedTasks[0].ApplyOperationID)
}

// Grouped operation creation rejects incomplete group definitions before any rows are committed.
func TestApplyStore_CreateWithGroupedOperationsRejectsInvalidGroups(t *testing.T) {
	tests := []struct {
		name      string
		groups    []*storage.ApplyOperationWithTasks
		wantError string
	}{
		{name: "empty groups", groups: nil, wantError: "grouped operations are empty"},
		{name: "nil group", groups: []*storage.ApplyOperationWithTasks{nil}, wantError: "grouped operation is nil"},
		{name: "nil operation", groups: []*storage.ApplyOperationWithTasks{{Tasks: []*storage.Task{newGroupedCreateTask(time.Now(), "payments-a", "users")}}}, wantError: "grouped operation is missing its operation row"},
		{name: "work op no tasks", groups: []*storage.ApplyOperationWithTasks{{Operation: &storage.ApplyOperation{Deployment: "payments-a", Target: "payments-target", OperationKind: storage.ApplyOperationKindWork}}}, wantError: "grouped work operation has no tasks"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearTables(t)
			ctx := t.Context()
			store := NewMySQL(testDB)
			apply := newGroupedCreateApply(time.Now(), "apply_grouped_invalid_"+strings.ReplaceAll(tt.name, " ", "_"))

			_, err := store.Applies().CreateWithGroupedOperations(ctx, apply, tt.groups)
			require.Error(t, err)
			assert.Contains(t, err.Error(), apply.ApplyIdentifier)
			assert.Contains(t, err.Error(), tt.wantError)

			gotApply, getErr := store.Applies().GetByApplyIdentifier(ctx, apply.ApplyIdentifier)
			require.NoError(t, getErr)
			assert.Nil(t, gotApply)
		})
	}
}

// A group_finalizer operation carries no tasks: it applies namespace-level work
// (VSchema) reconstructed from the plan at drive time. CreateWithGroupedOperations
// must accept a task-less finalizer alongside its work siblings rather than
// rejecting it as an empty group.
func TestApplyStore_CreateWithGroupedOperationsAllowsTaskLessFinalizer(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)
	now := time.Now()

	apply := newGroupedCreateApply(now, "apply_grouped_taskless_finalizer")
	groups := []*storage.ApplyOperationWithTasks{
		newGroupedCreateGroup(now, "payments-a", "payments-a-target", "users"),
		{Operation: &storage.ApplyOperation{
			Deployment:    "payments-a",
			OperationKey:  "commerce/group_finalizer",
			OperationKind: storage.ApplyOperationKindGroupFinalizer,
			Target:        "payments-a-target",
			State:         state.ApplyOperation.Pending,
			CutoverPolicy: storage.CutoverPolicyRolling,
			OnFailure:     storage.OnFailureHalt,
			CreatedAt:     now,
			UpdatedAt:     now,
		}},
	}

	applyID, err := store.Applies().CreateWithGroupedOperations(ctx, apply, groups)
	require.NoError(t, err)

	ops, err := store.ApplyOperations().ListByApply(ctx, applyID)
	require.NoError(t, err)
	require.Len(t, ops, 2)
	var finalizer *storage.ApplyOperation
	for _, op := range ops {
		if op.OperationKind == storage.ApplyOperationKindGroupFinalizer {
			finalizer = op
		}
	}
	require.NotNil(t, finalizer, "task-less group_finalizer operation should be persisted")
	assert.Equal(t, "commerce/group_finalizer", finalizer.OperationKey)
}

// TestApplyStore_CreateWithGroupedOperationsBlocksOverlapOnSecondaryDeployment
// proves the active-apply invariant covers every deployment a fan-out apply
// owns, not just the parent's primary deployment. A non-terminal apply spanning
// deployments [payments-a, payments-b] must block a new apply whose secondary
// deployment is payments-b, while still allowing a new apply whose deployments
// are disjoint from the active apply.
func TestApplyStore_CreateWithGroupedOperationsBlocksOverlapOnSecondaryDeployment(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)
	now := time.Now()

	groupedApply := func(identifier, primary string) *storage.Apply {
		a := newGroupedCreateApply(now, identifier)
		a.Deployment = primary
		return a
	}

	first := groupedApply("apply_fanout_first", "payments-a")
	_, err := store.Applies().CreateWithGroupedOperations(ctx, first, []*storage.ApplyOperationWithTasks{
		newGroupedCreateGroup(now, "payments-a", "payments-a-target", "users"),
		newGroupedCreateGroup(now, "payments-b", "payments-b-target", "users"),
	})
	require.NoError(t, err)

	// A new apply whose secondary deployment overlaps payments-b is rejected,
	// even though its primary (payments-x) is otherwise free. Distinct table
	// names keep the globally-unique task identifiers from colliding.
	overlapping := groupedApply("apply_fanout_overlap", "payments-x")
	_, err = store.Applies().CreateWithGroupedOperations(ctx, overlapping, []*storage.ApplyOperationWithTasks{
		newGroupedCreateGroup(now, "payments-x", "payments-x-target", "accounts"),
		newGroupedCreateGroup(now, "payments-b", "payments-b-target", "accounts"),
	})
	require.ErrorIs(t, err, storage.ErrActiveApplyExists)

	// A new apply whose deployments are disjoint from the active apply is allowed.
	disjoint := groupedApply("apply_fanout_disjoint", "payments-y")
	_, err = store.Applies().CreateWithGroupedOperations(ctx, disjoint, []*storage.ApplyOperationWithTasks{
		newGroupedCreateGroup(now, "payments-y", "payments-y-target", "ledger"),
		newGroupedCreateGroup(now, "payments-z", "payments-z-target", "ledger"),
	})
	require.NoError(t, err)
}

// TestApplyStore_ClaimStoppedFanOutRefusedWhenSecondaryDeploymentActive proves
// the stopped-claim re-check covers every deployment a fan-out apply
// owns. A stopped apply spanning [region-a, region-b] must not restart while a
// different active apply already owns region-b, even though the stopped apply's
// primary deployment (region-a) is free.
func TestApplyStore_ClaimStoppedFanOutRefusedWhenSecondaryDeploymentActive(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)
	now := time.Now()

	stopped := newGroupedCreateApply(now, "apply_fanout_stopped")
	stopped.Deployment = "region-a"
	stopped.State = state.Apply.Stopped
	stoppedID, err := store.Applies().CreateWithGroupedOperations(ctx, stopped, []*storage.ApplyOperationWithTasks{
		newGroupedCreateGroup(now, "region-a", "region-a-target", "users"),
		newGroupedCreateGroup(now, "region-b", "region-b-target", "users"),
	})
	require.NoError(t, err)

	// A different active apply owns region-b, one of the stopped apply's
	// secondary deployments.
	_, err = store.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply_region_b_active",
		PlanID:          2,
		Database:        "payments",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Repository:      "org/repo",
		PullRequest:     123,
		Environment:     "production",
		Deployment:      "region-b",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Running,
	})
	require.NoError(t, err)

	_, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:   stoppedID,
		Operation: storage.ControlOperationStart,
		Status:    storage.ControlRequestPending,
		Metadata:  []byte(`{}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	claimed, err := store.Applies().ClaimApplyByID(ctx, stoppedID, "test-owner")
	require.NoError(t, err)
	assert.Nil(t, claimed, "claim must be refused while another active apply owns a secondary deployment")

	persisted, err := store.Applies().Get(ctx, stoppedID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.Stopped, persisted.State, "refused claim must leave the apply stopped")
}

func newGroupedCreateApply(now time.Time, identifier string) *storage.Apply {
	return &storage.Apply{
		ApplyIdentifier: identifier,
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
}

func newGroupedCreateGroup(now time.Time, deployment, target string, tables ...string) *storage.ApplyOperationWithTasks {
	tasks := make([]*storage.Task, 0, len(tables))
	for _, table := range tables {
		tasks = append(tasks, newGroupedCreateTask(now, deployment, table))
	}
	return &storage.ApplyOperationWithTasks{
		Operation: &storage.ApplyOperation{Deployment: deployment, Target: target, State: state.ApplyOperation.Pending, CreatedAt: now, UpdatedAt: now},
		Tasks:     tasks,
	}
}

func newGroupedCreateTask(now time.Time, deployment, table string) *storage.Task {
	return &storage.Task{
		TaskIdentifier: "task_" + deployment + "_" + table,
		PlanID:         1,
		Database:       "payments",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Engine:         storage.EngineSpirit,
		Repository:     "org/repo",
		PullRequest:    123,
		Environment:    "production",
		State:          state.Task.Pending,
		TableName:      table,
		DDL:            "ALTER TABLE " + table + " ADD COLUMN email VARCHAR(255)",
		DDLAction:      "alter",
		Options:        []byte("{}"),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

// TestApplyStore_CreateWithTasksAndOperationsRollsBackOnTaskFailure pins the
// post-reorder invariant: operations are inserted before tasks, so a task
// insert failure must roll back the already-inserted apply_operations rows
// (and the apply row) — no orphan operations left behind.
func TestApplyStore_CreateWithTasksAndOperationsRollsBackOnTaskFailure(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "payments", storage.DatabaseTypeMySQL)
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
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "payments", storage.DatabaseTypeMySQL)
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
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "payments", storage.DatabaseTypeMySQL)
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
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "payments", storage.DatabaseTypeMySQL)
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
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "payments", storage.DatabaseTypeMySQL)
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
	// Two operations sharing the same storage identity violate the UNIQUE KEY
	// (apply_id, deployment, operation_key) constraint on the second insert and
	// must roll back the whole transaction — no orphan apply or tasks rows.
	operations := []*storage.ApplyOperation{
		{Deployment: "payments-a", OperationKey: "schema", Target: "payments", State: state.ApplyOperation.Pending, CreatedAt: now, UpdatedAt: now},
		{Deployment: "payments-a", OperationKey: "schema", Target: "payments", State: state.ApplyOperation.Pending, CreatedAt: now, UpdatedAt: now},
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
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
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
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")

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
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")

	// Hold the same target lock that the create path must acquire. The creates
	// below use the public store API; a result before release means active
	// apply writes are not serialized by the per-target lock.
	guardConn, guardLockName, err := acquireApplyTargetLockConn(ctx, newRebindDB(testDB, MySQLDialect{}), namedlock.MySQL{}, "testdb", "mysql", "staging")
	require.NoError(t, err)
	releaseGuard := func() {
		if guardConn == nil {
			return
		}
		releaseApplyTargetLockConn(ctx, namedlock.MySQL{}, guardConn, guardLockName, "test active apply guard")
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
	store := NewMySQL(testDB)

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
			locks[key] = createTestLock(t, store, target.database, target.dbType)
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
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")

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
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")

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

// TestApplyStore_IdempotencyKey verifies the dedupe anchor for remote apply
// dispatch: applies with no key store NULL and never collide, a non-empty key is
// unique, and GetByIdempotencyKey resolves a stamped apply while treating the
// empty key as no lookup.
func TestApplyStore_IdempotencyKey(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")

	// Empty key is never deduplicated: many applies store NULL and coexist.
	// Terminal state keeps the active-apply target guard from rejecting the
	// repeated inserts so the test isolates the NULL-key uniqueness behavior.
	for i := range 3 {
		_, err := store.Applies().Create(ctx, &storage.Apply{
			ApplyIdentifier: "apply_nokey_" + strconv.Itoa(i),
			LockID:          lock.ID,
			PlanID:          int64(i + 1),
			Database:        lock.DatabaseName,
			DatabaseType:    lock.DatabaseType,
			Repository:      lock.Repository,
			PullRequest:     lock.PullRequest,
			Environment:     "staging",
			Engine:          storage.EngineForType(lock.DatabaseType),
			State:           state.Apply.Completed,
		})
		require.NoError(t, err, "applies without an idempotency key must not collide on NULL")
	}

	// Empty key lookup short-circuits to nil without matching the NULL rows.
	got, err := store.Applies().GetByIdempotencyKey(ctx, "")
	require.NoError(t, err)
	assert.Nil(t, got, "empty key must not resolve any NULL-keyed apply")

	// A stamped key is resolvable.
	keyed := &storage.Apply{
		ApplyIdentifier: "apply_keyed",
		LockID:          lock.ID,
		PlanID:          100,
		Database:        lock.DatabaseName,
		DatabaseType:    lock.DatabaseType,
		Repository:      lock.Repository,
		PullRequest:     lock.PullRequest,
		Environment:     "staging",
		Engine:          storage.EngineForType(lock.DatabaseType),
		State:           state.Apply.Completed,
		IdempotencyKey:  "schemabot:v1:abc",
	}
	keyedID, err := store.Applies().Create(ctx, keyed)
	require.NoError(t, err)

	got, err = store.Applies().GetByIdempotencyKey(ctx, "schemabot:v1:abc")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, keyedID, got.ID)
	assert.Equal(t, "apply_keyed", got.ApplyIdentifier)
	assert.Equal(t, "schemabot:v1:abc", got.IdempotencyKey)

	// An unseen key resolves to nil.
	got, err = store.Applies().GetByIdempotencyKey(ctx, "schemabot:v1:unseen")
	require.NoError(t, err)
	assert.Nil(t, got)

	// The same non-empty key cannot be inserted twice.
	_, err = store.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply_keyed_dup",
		LockID:          lock.ID,
		PlanID:          101,
		Database:        lock.DatabaseName,
		DatabaseType:    lock.DatabaseType,
		Repository:      lock.Repository,
		PullRequest:     lock.PullRequest,
		Environment:     "staging",
		Engine:          storage.EngineForType(lock.DatabaseType),
		State:           state.Apply.Completed,
		IdempotencyKey:  "schemabot:v1:abc",
	})
	require.Error(t, err, "a duplicate non-empty idempotency key must be rejected")
}

func TestApplyStore_GetByPlan(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")

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
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")

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
	store := NewMySQL(testDB)

	// Create locks for different databases
	lock1 := createTestLock(t, store, "db1", "mysql")
	lock2 := createTestLock(t, store, "db2", "mysql")

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
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "recentdb", "mysql")
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

// The status list puts in-flight work first: a copy that has been running for
// days stays on the first page while applies created after it — and finished
// since — sort below it.
func TestApplyStore_GetRecentPutsInFlightWorkFirst(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)
	now := time.Now()

	lock := createTestLock(t, store, "activitydb", "mysql")
	longRunning := createTestApplyWithStateAndEnv(t, store, lock, "apply_long_running", 230, state.Apply.Running, "staging")
	finishedRecently := createTestApplyWithStateAndEnv(t, store, lock, "apply_finished_recently", 231, state.Apply.Completed, "staging")
	finishedEarlier := createTestApplyWithStateAndEnv(t, store, lock, "apply_finished_earlier", 232, state.Apply.Completed, "staging")

	// The long-running apply was requested first and is still heartbeating; the
	// two that follow were created after it and have since gone quiet.
	setApplyStartedAt(t, longRunning.ID, now.Add(-48*time.Hour))
	setApplyUpdatedAt(t, longRunning.ID, now)
	setApplyUpdatedAt(t, finishedRecently.ID, now.Add(-time.Hour))
	setApplyUpdatedAt(t, finishedEarlier.ID, now.Add(-2*time.Hour))

	applies, err := store.Applies().GetRecent(ctx, storage.RecentAppliesFilter{
		Limit:       3,
		Environment: "staging",
	})
	require.NoError(t, err)
	require.Len(t, applies, 3)
	assert.Equal(t, "apply_long_running", applies[0].ApplyIdentifier)
	assert.Equal(t, "apply_finished_recently", applies[1].ApplyIdentifier)
	assert.Equal(t, "apply_finished_earlier", applies[2].ApplyIdentifier)
}

// Every driver heartbeats on the same interval, so concurrent applies all carry
// a timestamp from within one interval. Ordering them by that timestamp would
// reshuffle the top of the list between calls, so in-flight applies are ordered
// by when they started, oldest first — the longest-held table reads first and a
// newly started apply joins the bottom of the group.
func TestApplyStore_GetRecentOrdersInFlightByStart(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)
	now := time.Now()

	// One database admits one in-flight apply, so concurrent rollouts are always
	// against different databases.
	oldest := createTestApplyWithStateAndEnv(t, store, createTestLock(t, store, "concurrent_a", "mysql"),
		"apply_running_oldest", 250, state.Apply.Running, "staging")
	middle := createTestApplyWithStateAndEnv(t, store, createTestLock(t, store, "concurrent_b", "mysql"),
		"apply_running_middle", 251, state.Apply.Running, "staging")
	newest := createTestApplyWithStateAndEnv(t, store, createTestLock(t, store, "concurrent_c", "mysql"),
		"apply_running_newest", 252, state.Apply.Running, "staging")

	setApplyStartedAt(t, oldest.ID, now.Add(-48*time.Hour))
	setApplyStartedAt(t, middle.ID, now.Add(-2*time.Hour))
	setApplyStartedAt(t, newest.ID, now.Add(-5*time.Minute))

	// Heartbeats land in a different order than the applies started, spread
	// across one heartbeat interval the way concurrent drivers produce.
	setApplyUpdatedAt(t, middle.ID, now)
	setApplyUpdatedAt(t, newest.ID, now.Add(-3*time.Second))
	setApplyUpdatedAt(t, oldest.ID, now.Add(-6*time.Second))

	applies, err := store.Applies().GetRecent(ctx, storage.RecentAppliesFilter{
		Limit:       3,
		Environment: "staging",
	})
	require.NoError(t, err)
	require.Len(t, applies, 3)
	assert.Equal(t, "apply_running_oldest", applies[0].ApplyIdentifier)
	assert.Equal(t, "apply_running_middle", applies[1].ApplyIdentifier)
	assert.Equal(t, "apply_running_newest", applies[2].ApplyIdentifier)
}

// An apply left in a non-terminal state with nothing driving it stops
// heartbeating, so it is not in flight and does not hold the top of the list
// ahead of work that is actually moving.
func TestApplyStore_GetRecentSinksAbandonedApplies(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)
	now := time.Now()

	abandonedLock := createTestLock(t, store, "abandoned_a", "mysql")
	liveLock := createTestLock(t, store, "abandoned_b", "mysql")
	abandoned := createTestApplyWithStateAndEnv(t, store, abandonedLock, "apply_abandoned", 260, state.Apply.FailedRetryable, "staging")
	running := createTestApplyWithStateAndEnv(t, store, liveLock, "apply_still_running", 261, state.Apply.Running, "staging")
	finished := createTestApplyWithStateAndEnv(t, store, abandonedLock, "apply_finished", 262, state.Apply.Completed, "staging")

	setApplyStartedAt(t, abandoned.ID, now.Add(-72*time.Hour))
	setApplyStartedAt(t, running.ID, now.Add(-time.Hour))
	setApplyUpdatedAt(t, abandoned.ID, now.Add(-72*time.Hour))
	setApplyUpdatedAt(t, running.ID, now)
	setApplyUpdatedAt(t, finished.ID, now.Add(-time.Minute*30))

	applies, err := store.Applies().GetRecent(ctx, storage.RecentAppliesFilter{
		Limit:       3,
		Environment: "staging",
	})
	require.NoError(t, err)
	require.Len(t, applies, 3)
	assert.Equal(t, "apply_still_running", applies[0].ApplyIdentifier)
	assert.Equal(t, "apply_finished", applies[1].ApplyIdentifier, "the abandoned apply does not outrank work that settled since")
	assert.Equal(t, "apply_abandoned", applies[2].ApplyIdentifier)
}

// A fanned-out apply is driven per operation, and an operation-scoped drive is
// refused a parent heartbeat, so the parent row stays frozen while the rollout
// advances. Its operations' heartbeats keep it in flight.
func TestApplyStore_GetRecentTreatsOperationActivityAsInFlight(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)
	now := time.Now()

	lock := createTestLock(t, store, "opactivitydb", "mysql")
	fannedOut := createTestApplyWithStateAndEnv(t, store, lock, "apply_fanned_out", 240, state.Apply.Running, "staging")
	finished := createTestApplyWithStateAndEnv(t, store, lock, "apply_finished", 241, state.Apply.Completed, "staging")

	// Only the operation is heartbeating: the parent apply row last changed
	// before the finished apply did.
	setApplyStartedAt(t, fannedOut.ID, now.Add(-24*time.Hour))
	setApplyUpdatedAt(t, fannedOut.ID, now.Add(-3*time.Hour))
	setApplyUpdatedAt(t, finished.ID, now.Add(-time.Hour))
	insertOperationWithUpdatedAt(t, fannedOut.ID, "deploy-a", state.ApplyOperation.Running, now)

	applies, err := store.Applies().GetRecent(ctx, storage.RecentAppliesFilter{
		Limit:       2,
		Environment: "staging",
	})
	require.NoError(t, err)
	require.Len(t, applies, 2)
	assert.Equal(t, "apply_fanned_out", applies[0].ApplyIdentifier)
	assert.Equal(t, "apply_finished", applies[1].ApplyIdentifier)

	// An operation quieter than its parent must not drag the apply down: the
	// parent's own state change is the later activity.
	setApplyUpdatedAt(t, finished.ID, now.Add(-4*time.Hour))
	insertOperationWithUpdatedAt(t, finished.ID, "deploy-b", state.ApplyOperation.Completed, now.Add(-5*time.Hour))

	applies, err = store.Applies().GetRecent(ctx, storage.RecentAppliesFilter{
		Limit:       2,
		Environment: "staging",
	})
	require.NoError(t, err)
	require.Len(t, applies, 2)
	assert.Equal(t, "apply_fanned_out", applies[0].ApplyIdentifier)
	assert.Equal(t, "apply_finished", applies[1].ApplyIdentifier)
}

func setApplyStartedAt(t *testing.T, applyID int64, at time.Time) {
	t.Helper()
	_, err := testDB.ExecContext(t.Context(),
		"UPDATE `applies` SET started_at = ?, updated_at = updated_at WHERE id = ?", at, applyID)
	require.NoError(t, err)
}

func setApplyUpdatedAt(t *testing.T, applyID int64, at time.Time) {
	t.Helper()
	_, err := testDB.ExecContext(t.Context(),
		"UPDATE `applies` SET updated_at = ? WHERE id = ?", at, applyID)
	require.NoError(t, err)
}

func insertOperationWithUpdatedAt(t *testing.T, applyID int64, deployment, opState string, at time.Time) {
	t.Helper()
	_, err := testDB.ExecContext(t.Context(), `
		INSERT INTO apply_operations (apply_id, deployment, target, state, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		applyID, deployment, deployment+"-target", opState, at, at)
	require.NoError(t, err)
}

// TestApplyStore_GetRecentUpdatedSince verifies the status window bound: only
// applies whose last activity falls at or after the bound are returned, so an
// apply that went quiet before the window drops out while fresh activity stays
// visible.
func TestApplyStore_GetRecentUpdatedSince(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "windowdb", "mysql")
	createTestApplyWithStateAndEnv(t, store, lock, "apply_window_stale", 220, state.Apply.Completed, "staging")
	createTestApplyWithStateAndEnv(t, store, lock, "apply_window_fresh", 221, state.Apply.Completed, "staging")

	_, err := testDB.ExecContext(ctx,
		"UPDATE `applies` SET updated_at = ? WHERE apply_identifier = ?",
		time.Now().Add(-2*time.Hour), "apply_window_stale")
	require.NoError(t, err)

	applies, err := store.Applies().GetRecent(ctx, storage.RecentAppliesFilter{
		Limit:        10,
		Environment:  "staging",
		UpdatedSince: time.Now().Add(-time.Hour),
	})
	require.NoError(t, err)
	require.Len(t, applies, 1)
	assert.Equal(t, "apply_window_fresh", applies[0].ApplyIdentifier)

	applies, err = store.Applies().GetRecent(ctx, storage.RecentAppliesFilter{
		Limit:        10,
		Environment:  "staging",
		UpdatedSince: time.Now().Add(-3 * time.Hour),
	})
	require.NoError(t, err)
	require.Len(t, applies, 2)
}

// Work that just finished reads newest-first, not as if it were still running.
// A terminal apply's last heartbeat stays fresh for as long as the staleness
// window, so recency of activity alone would group it with the live work and
// sort it by start time — showing an operator the oldest of the applies that
// just settled, under a heading that says in flight.
func TestApplyStore_GetRecentOrdersSettledWorkByFinish(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)
	now := time.Now()

	lock := createTestLock(t, store, "settled", "mysql")
	earlier := createTestApplyWithStateAndEnv(t, store, lock, "apply_settled_earlier", 280, state.Apply.Completed, "staging")
	later := createTestApplyWithStateAndEnv(t, store, lock, "apply_settled_later", 281, state.Apply.Completed, "staging")

	// Both finished within the staleness window, the longer-running one first.
	setApplyStartedAt(t, earlier.ID, now.Add(-3*time.Hour))
	setApplyStartedAt(t, later.ID, now.Add(-2*time.Hour))
	setApplyUpdatedAt(t, earlier.ID, now.Add(-30*time.Second))
	setApplyUpdatedAt(t, later.ID, now.Add(-10*time.Second))

	applies, err := store.Applies().GetRecent(ctx, storage.RecentAppliesFilter{
		Limit:       10,
		Environment: "staging",
	})
	require.NoError(t, err)
	require.Len(t, applies, 2)
	assert.Equal(t, "apply_settled_later", applies[0].ApplyIdentifier)
	assert.Equal(t, "apply_settled_earlier", applies[1].ApplyIdentifier)
}

// A time window keeps a fanned-out apply visible while it is still working. An
// operation-scoped drive is refused a parent heartbeat by design, so the parent
// row can be hours stale while the operations tick every few seconds; windowing
// on the parent row alone would hide a running rollout from both the list and
// the summary counts that an operator reads together.
func TestApplyStore_GetRecentWindowKeepsOperationActivity(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)
	now := time.Now()

	lock := createTestLock(t, store, "window_fanout", "mysql")
	fannedOut := createTestApplyWithStateAndEnv(t, store, lock, "apply_window_fanout", 270, state.Apply.Running, "staging")
	insertOperationWithUpdatedAt(t, fannedOut.ID, "deploy-a", state.ApplyOperation.Running, now)
	setApplyUpdatedAt(t, fannedOut.ID, now.Add(-2*time.Hour))

	filter := storage.RecentAppliesFilter{
		Limit:        10,
		Environment:  "staging",
		UpdatedSince: now.Add(-time.Hour),
	}

	applies, err := store.Applies().GetRecent(ctx, filter)
	require.NoError(t, err)
	require.Len(t, applies, 1)
	assert.Equal(t, "apply_window_fanout", applies[0].ApplyIdentifier)

	counts, err := store.Applies().CountRecentByState(ctx, filter)
	require.NoError(t, err)
	assert.Equal(t, 1, counts[state.Apply.Running])
}

// A deployment-scoped view answers for the deployment the operator asked about.
// A rollout still heartbeating in some other deployment reads as quiet here,
// matching the per-deployment state rendered on the same row.
func TestApplyStore_GetRecentScopesActivityToDeployment(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)
	now := time.Now()

	// Two fanned-out applies. In one deployment the first is still working and
	// the second went quiet hours ago; in the other it is the other way round.
	first := createTestApplyWithStateAndEnv(t, store, createTestLock(t, store, "scoped_a", "mysql"),
		"apply_scoped_first", 290, state.Apply.Running, "staging")
	second := createTestApplyWithStateAndEnv(t, store, createTestLock(t, store, "scoped_b", "mysql"),
		"apply_scoped_second", 291, state.Apply.Running, "staging")

	insertOperationWithUpdatedAt(t, first.ID, "deploy-east", state.ApplyOperation.Running, now)
	insertOperationWithUpdatedAt(t, first.ID, "deploy-west", state.ApplyOperation.Completed, now.Add(-3*time.Hour))
	insertOperationWithUpdatedAt(t, second.ID, "deploy-east", state.ApplyOperation.Completed, now.Add(-3*time.Hour))
	insertOperationWithUpdatedAt(t, second.ID, "deploy-west", state.ApplyOperation.Running, now)
	setApplyUpdatedAt(t, first.ID, now.Add(-3*time.Hour))
	setApplyUpdatedAt(t, second.ID, now.Add(-3*time.Hour))

	east, err := store.Applies().GetRecent(ctx, storage.RecentAppliesFilter{
		Limit: 10, Environment: "staging", Deployment: "deploy-east",
	})
	require.NoError(t, err)
	require.Len(t, east, 2)
	assert.Equal(t, "apply_scoped_first", east[0].ApplyIdentifier)

	west, err := store.Applies().GetRecent(ctx, storage.RecentAppliesFilter{
		Limit: 10, Environment: "staging", Deployment: "deploy-west",
	})
	require.NoError(t, err)
	require.Len(t, west, 2)
	assert.Equal(t, "apply_scoped_second", west[0].ApplyIdentifier)
}

// A deployment-scoped time window is scoped the same way as the ordering, so an
// apply quiet in the requested deployment falls outside the window even while
// another deployment keeps heartbeating.
func TestApplyStore_GetRecentDeploymentWindowScopesActivity(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)
	now := time.Now()

	lock := createTestLock(t, store, "scoped_window", "mysql")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_scoped_window", 292, state.Apply.Running, "staging")
	insertOperationWithUpdatedAt(t, apply.ID, "deploy-east", state.ApplyOperation.Completed, now.Add(-3*time.Hour))
	insertOperationWithUpdatedAt(t, apply.ID, "deploy-west", state.ApplyOperation.Running, now)
	setApplyUpdatedAt(t, apply.ID, now.Add(-3*time.Hour))

	quiet, err := store.Applies().GetRecent(ctx, storage.RecentAppliesFilter{
		Limit: 10, Environment: "staging", Deployment: "deploy-east",
		UpdatedSince: now.Add(-time.Hour),
	})
	require.NoError(t, err)
	assert.Empty(t, quiet)

	working, err := store.Applies().GetRecent(ctx, storage.RecentAppliesFilter{
		Limit: 10, Environment: "staging", Deployment: "deploy-west",
		UpdatedSince: now.Add(-time.Hour),
	})
	require.NoError(t, err)
	require.Len(t, working, 1)
	assert.Equal(t, "apply_scoped_window", working[0].ApplyIdentifier)
}

// An active-only query answers "is this target busy" without reading settled
// history. It is expressed as "not terminal", so an apply in a state the registry
// does not know still counts as active — a caller must never be told a target is
// free because a state was missed.
func TestApplyStore_GetRecentActiveOnly(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	createTestApplyWithStateAndEnv(t, store, createTestLock(t, store, "active_a", "mysql"),
		"apply_active_running", 300, state.Apply.Running, "staging")
	createTestApplyWithStateAndEnv(t, store, createTestLock(t, store, "active_b", "mysql"),
		"apply_active_retryable", 301, state.Apply.FailedRetryable, "staging")
	unknownState := createTestApplyWithStateAndEnv(t, store, createTestLock(t, store, "active_c", "mysql"),
		"apply_active_unknown", 302, state.Apply.Completed, "staging")
	_, err := testDB.ExecContext(ctx,
		"UPDATE `applies` SET state = ?, updated_at = updated_at WHERE id = ?", "some_future_state", unknownState.ID)
	require.NoError(t, err)

	// Settled work that must not appear.
	createTestApplyWithStateAndEnv(t, store, createTestLock(t, store, "active_d", "mysql"),
		"apply_active_completed", 303, state.Apply.Completed, "staging")
	createTestApplyWithStateAndEnv(t, store, createTestLock(t, store, "active_e", "mysql"),
		"apply_active_stopped", 304, state.Apply.Stopped, "staging")

	applies, err := store.Applies().GetRecent(ctx, storage.RecentAppliesFilter{
		Limit: 10, Environment: "staging", ActiveOnly: true,
	})
	require.NoError(t, err)

	got := make([]string, 0, len(applies))
	for _, a := range applies {
		got = append(got, a.ApplyIdentifier)
	}
	assert.ElementsMatch(t, []string{"apply_active_running", "apply_active_retryable", "apply_active_unknown"}, got)
}

// A status summary must tally every apply matching the filters — unbounded by
// the list limit, scoped by environment, and windowed by UpdatedSince — so an
// operator reading a truncated status page still sees truthful totals.
func TestApplyStore_CountRecentByState(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "countdb", "mysql")
	createTestApplyWithStateAndEnv(t, store, lock, "apply_count_completed_one", 230, state.Apply.Completed, "staging")
	createTestApplyWithStateAndEnv(t, store, lock, "apply_count_completed_two", 231, state.Apply.Completed, "staging")
	createTestApplyWithStateAndEnv(t, store, lock, "apply_count_failed", 232, state.Apply.Failed, "staging")
	createTestApplyWithStateAndEnv(t, store, lock, "apply_count_other_env", 233, state.Apply.Completed, "production")

	counts, err := store.Applies().CountRecentByState(ctx, storage.RecentAppliesFilter{
		Limit:       1,
		Environment: "staging",
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]int{
		state.Apply.Completed: 2,
		state.Apply.Failed:    1,
	}, counts, "counts cover every matching row even when the list limit is 1")

	_, err = testDB.ExecContext(ctx,
		"UPDATE `applies` SET updated_at = ? WHERE apply_identifier = ?",
		time.Now().Add(-2*time.Hour), "apply_count_completed_one")
	require.NoError(t, err)

	counts, err = store.Applies().CountRecentByState(ctx, storage.RecentAppliesFilter{
		Limit:        10,
		Environment:  "staging",
		UpdatedSince: time.Now().Add(-time.Hour),
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]int{
		state.Apply.Completed: 1,
		state.Apply.Failed:    1,
	}, counts)
}

func TestApplyStore_GetRecentDeploymentFilterMatchesParentAndOperation(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "recentdeploydb", "mysql")
	createTestApplyWithStateEnvDeployment(t, store, lock, "apply_recent_parent_deployment", 214, state.Apply.Completed, "staging", "deploy-a")

	operationMatch := createTestApplyWithStateEnvDeployment(t, store, lock, "apply_recent_operation_deployment", 215, state.Apply.Completed, "staging", "deploy-parent")
	_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:             operationMatch.ID,
		Deployment:          "deploy-a",
		OperationKey:        "",
		OperationKind:       storage.ApplyOperationKindWork,
		Target:              "target-a",
		State:               state.Apply.Completed,
		CutoverPolicy:       storage.CutoverPolicyRolling,
		OnFailure:           storage.OnFailureHalt,
		EngineResumeContext: "remote-operation-1",
	})
	require.NoError(t, err)

	createTestApplyWithStateEnvDeployment(t, store, lock, "apply_recent_other_deployment", 216, state.Apply.Completed, "staging", "deploy-b")

	applies, err := store.Applies().GetRecent(ctx, storage.RecentAppliesFilter{
		Limit:      10,
		Deployment: "deploy-a",
	})
	require.NoError(t, err)
	require.Len(t, applies, 2)
	assert.Equal(t, "apply_recent_operation_deployment", applies[0].ApplyIdentifier)
	assert.Equal(t, "apply_recent_parent_deployment", applies[1].ApplyIdentifier)
}

func TestApplyStore_Update(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
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

// The recovery budget (attempt) is owned by the insert at create time and the
// claim transition's atomic increment. A driver's in-memory apply goes stale
// the moment the stored budget is adjusted out-of-band mid-drive — an operator
// spending or granting attempts — so the driver persisting its mutable state
// must leave the stored budget in place rather than resetting it to the value
// it loaded at claim time.
func TestApplyStore_UpdatePreservesStoredRecoveryBudget(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_update_keeps_budget", 303, state.Apply.Running, "staging")

	// Spend the stored budget underneath the drive, after its in-memory copy
	// was loaded.
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies SET attempt = ? WHERE id = ?
	`, maxRecoveryAttempts, apply.ID)
	require.NoError(t, err)

	apply.State = state.Apply.FailedRetryable
	apply.ErrorMessage = "transient engine failure"
	require.NoError(t, store.Applies().Update(ctx, apply))

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.FailedRetryable, persisted.State, "the drive's own state write lands")
	assert.Equal(t, "transient engine failure", persisted.ErrorMessage)
	assert.Equal(t, maxRecoveryAttempts, persisted.Attempt,
		"the stored recovery budget survives a drive persisting a stale in-memory copy")
}

func TestApplyStore_UpdateBlocksActiveApplyForSameTarget(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
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
	store := NewMySQL(testDB)

	apply := &storage.Apply{
		ID:    99999,
		State: state.Apply.Running,
	}

	// Update on a non-existent row is a no-op (0 rows affected), not an error.
	// MySQL UPDATE with WHERE id=? succeeds even when no row matches.
	require.NoError(t, store.Applies().Update(ctx, apply))
}

// TestApplyStore_UpdateDerivedState verifies the rollout-projection compare-and-
// swap: the write lands only when the row still holds the expected state, so a
// stale projection cannot clobber a newer state another drive already wrote.
func TestApplyStore_UpdateDerivedState(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_derived_cas", 800, state.Apply.Running, "staging")

	// Matching expected state swaps and writes the projected fields.
	completedAt := time.Now()
	swapped, err := store.Applies().UpdateDerivedState(ctx, apply.ID, state.Apply.Running, state.Apply.Failed, "deployment failed", nil, &completedAt)
	require.NoError(t, err)
	require.True(t, swapped, "expected state matched, so the swap must land")

	updated, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Failed, updated.State)
	assert.Equal(t, "deployment failed", updated.ErrorMessage)
	require.NotNil(t, updated.CompletedAt)

	// A stale expected state misses: the row already moved on, so the write is
	// skipped and the row is left untouched.
	swapped, err = store.Applies().UpdateDerivedState(ctx, apply.ID, state.Apply.Running, state.Apply.Completed, "", nil, nil)
	require.NoError(t, err)
	assert.False(t, swapped, "expected state no longer matches, so the swap must miss")

	unchanged, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Failed, unchanged.State, "a CAS miss must not overwrite the newer state")
	assert.Equal(t, "deployment failed", unchanged.ErrorMessage)
}

// TestApplyStore_UpdateDerivedStateNoOpUnderChangedRows verifies that under
// production changed-rows semantics a no-op projection write (the steady-state
// case where the derived state equals the current state) reports a successful
// swap rather than a false CAS miss, so progress side-effects still fire.
func TestApplyStore_UpdateDerivedStateNoOpUnderChangedRows(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := newChangedRowsStore(t)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_derived_cas_noop", 802, state.Apply.Running, "staging")

	// Re-deriving the same running state with no other field change is a no-op
	// that affects zero rows under changed-rows semantics, but the row still
	// holds the expected state, so the swap is reported as successful.
	swapped, err := store.Applies().UpdateDerivedState(ctx, apply.ID, state.Apply.Running, state.Apply.Running, "", nil, nil)
	require.NoError(t, err)
	assert.True(t, swapped, "a no-op write to a row already in the expected state is an idempotent swap, not a miss")

	// A stale expected state still misses even under changed-rows semantics.
	swapped, err = store.Applies().UpdateDerivedState(ctx, apply.ID, state.Apply.Pending, state.Apply.Pending, "", nil, nil)
	require.NoError(t, err)
	assert.False(t, swapped, "the row is not in the expected state, so the swap must miss")
}

// TestApplyStore_UpdateDerivedStateLeaseGuard verifies that a leased projection
// write fails closed on a lost lease (an ownership change the caller must
// surface) while the current lease holder's matching swap succeeds.
func TestApplyStore_UpdateDerivedStateLeaseGuard(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_derived_cas_lease", 801, state.Apply.Pending, "staging")

	task := &storage.Task{
		TaskIdentifier: "task_derived_cas_lease",
		ApplyID:        apply.ID,
		PlanID:         apply.PlanID,
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Engine:         storage.EngineSpirit,
		Environment:    apply.Environment,
		State:          state.Task.Pending,
		TableName:      "users",
		DDL:            "ALTER TABLE `users` ADD COLUMN `cas_note` varchar(255)",
		DDLAction:      "alter",
		Options:        []byte("{}"),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	taskID, err := store.Tasks().Create(ctx, task)
	require.NoError(t, err)
	task.ID = taskID

	claimed, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "driver-a")
	require.NoError(t, err)
	require.NotNil(t, claimed)

	// The claim may rotate the persisted state, so read the row to learn the
	// state the compare-and-swap must expect.
	leased, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	expectedState := leased.State

	// A stale lease cannot write, even when the expected state matches.
	staleCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: apply.ID, Owner: "driver-old", Token: "stale-token"})
	_, err = store.Applies().UpdateDerivedState(staleCtx, apply.ID, expectedState, state.Apply.Failed, "stale", nil, nil)
	require.ErrorIs(t, err, storage.ErrApplyLeaseLost)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	assert.Equal(t, expectedState, persisted.State, "a lost lease must not write the projection")

	// The current lease holder's matching swap lands.
	ownedCtx := storage.WithApplyLease(ctx, claimed.Lease())
	swapped, err := store.Applies().UpdateDerivedState(ownedCtx, apply.ID, expectedState, state.Apply.Failed, "owned failure", nil, nil)
	require.NoError(t, err)
	assert.True(t, swapped)

	updated, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Failed, updated.State)
}

// TestApplyStore_UpdateDerivedStateOperationLeaseGuard verifies that the
// projection can be authorized by an operation lease (so a multi-operation drive
// can advance the parent only through the aggregate CAS): the swap lands under a
// current operation token, fails closed on a stale token or a token bound to a
// different apply, and the operation lease takes precedence over a current apply
// lease also on the context.
func TestApplyStore_UpdateDerivedStateOperationLeaseGuard(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	opLeaseCtx := func(applyID, opID int64, token string) context.Context {
		return storage.WithOperationLease(ctx, storage.OperationLease{
			ApplyID: applyID, OperationID: opID, Owner: "driver", Token: token,
		})
	}

	// Each running apply needs its own target so the active-apply uniqueness
	// check does not reject the second one.
	runningApply := func(identifier, env string, planID int64) *storage.Apply {
		lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
		return createTestApplyWithStateAndEnv(t, store, lock, identifier, planID, state.Apply.Running, env)
	}

	// A current operation lease authorizes the projection swap.
	apply := runningApply("apply_op_cas_ok", "staging-ok", 900)
	opID := createApplyOperationForLeaseTest(t, store, apply.ID, "primary")
	stampOperationLease(t, opID, "driver", "op-token")
	swapped, err := store.Applies().UpdateDerivedState(opLeaseCtx(apply.ID, opID, "op-token"), apply.ID, state.Apply.Running, state.Apply.Failed, "op failure", nil, nil)
	require.NoError(t, err)
	require.True(t, swapped, "a current operation lease must authorize the projection swap")
	updated, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Failed, updated.State)

	// A stale operation token fails closed even when the expected state matches.
	staleApply := runningApply("apply_op_cas_stale", "staging-stale", 901)
	staleOpID := createApplyOperationForLeaseTest(t, store, staleApply.ID, "primary")
	stampOperationLease(t, staleOpID, "driver", "op-token")
	_, err = store.Applies().UpdateDerivedState(opLeaseCtx(staleApply.ID, staleOpID, "stale-op-token"), staleApply.ID, state.Apply.Running, state.Apply.Failed, "stale", nil, nil)
	require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
	persisted, err := store.Applies().Get(ctx, staleApply.ID)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Running, persisted.State, "a lost operation lease must not write the projection")

	// An operation lease bound to a different apply cannot write the target.
	_, err = store.Applies().UpdateDerivedState(opLeaseCtx(apply.ID, opID, "op-token"), staleApply.ID, state.Apply.Running, state.Apply.Failed, "cross", nil, nil)
	require.ErrorIs(t, err, storage.ErrApplyLeaseLost)

	// Operation lease takes precedence: a stale operation token fails closed even
	// with a current apply lease also on the context.
	precApply := runningApply("apply_op_cas_prec", "staging-prec", 903)
	precOpID := createApplyOperationForLeaseTest(t, store, precApply.ID, "primary")
	stampOperationLease(t, precOpID, "driver", "op-token")
	_, err = testDB.ExecContext(ctx, `UPDATE applies SET lease_owner=?, lease_token=?, lease_acquired_at=NOW() WHERE id=?`, "current-driver", "apply-token", precApply.ID)
	require.NoError(t, err)
	bothCtx := storage.WithApplyLease(opLeaseCtx(precApply.ID, precOpID, "stale-op-token"), storage.ApplyLease{
		ApplyID: precApply.ID, Owner: "current-driver", Token: "apply-token",
	})
	_, err = store.Applies().UpdateDerivedState(bothCtx, precApply.ID, state.Apply.Running, state.Apply.Failed, "prec", nil, nil)
	require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
	precPersisted, err := store.Applies().Get(ctx, precApply.ID)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Running, precPersisted.State)

	// A current operation lease whose expected state no longer matches the row is
	// a benign CAS miss: swapped=false with no error, so a stale projection is
	// reconciled on the next poll rather than mistaken for a lost lease.
	missApply := runningApply("apply_op_cas_miss", "staging-miss", 904)
	missOpID := createApplyOperationForLeaseTest(t, store, missApply.ID, "primary")
	stampOperationLease(t, missOpID, "driver", "op-token")
	swapped, err = store.Applies().UpdateDerivedState(opLeaseCtx(missApply.ID, missOpID, "op-token"), missApply.ID, state.Apply.Pending, state.Apply.Failed, "stale projection", nil, nil)
	require.NoError(t, err, "a state mismatch under a current operation lease must not be reported as a lost lease")
	assert.False(t, swapped, "the expected state no longer matches, so the swap must miss")
	missPersisted, err := store.Applies().Get(ctx, missApply.ID)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Running, missPersisted.State, "a CAS miss must not write the projection")
}

// TestApplyStore_UpdateDerivedStateStampsStartedAt verifies that the projection
// stamps started_at when it is still NULL (so it can move the parent into an
// active state) but never rewinds a start time a drive already recorded.
func TestApplyStore_UpdateDerivedStateStampsStartedAt(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_started_stamp", 905, state.Apply.Running, "staging")

	initial, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.Nil(t, initial.StartedAt, "a freshly created apply has no recorded start time")

	// A same-state projection stamps started_at while it is still NULL.
	startedAt := time.Now().Truncate(time.Second)
	swapped, err := store.Applies().UpdateDerivedState(ctx, apply.ID, state.Apply.Running, state.Apply.Running, "", &startedAt, nil)
	require.NoError(t, err)
	require.True(t, swapped)
	stamped, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, stamped.StartedAt)
	assert.WithinDuration(t, startedAt, *stamped.StartedAt, time.Second)

	// A later projection must not rewind the recorded start time.
	later := startedAt.Add(time.Hour)
	swapped, err = store.Applies().UpdateDerivedState(ctx, apply.ID, state.Apply.Running, state.Apply.Running, "", &later, nil)
	require.NoError(t, err)
	require.True(t, swapped)
	preserved, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, preserved.StartedAt)
	assert.WithinDuration(t, startedAt, *preserved.StartedAt, time.Second, "started_at must be preserved, not rewound")
}

// TestApplyStore_UpdateRejectsOperationLeaseOnlyContext verifies that a drive
// holding only an operation lease cannot write the parent applies row directly:
// the parent is owned by the projection. A single-operation drive carries the
// parent apply lease alongside the operation lease, so its direct write still
// lands.
func TestApplyStore_UpdateRejectsOperationLeaseOnlyContext(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_update_oplease", 906, state.Apply.Running, "staging")
	opID := createApplyOperationForLeaseTest(t, store, apply.ID, "primary")
	stampOperationLease(t, opID, "driver", "op-token")

	opOnlyCtx := storage.WithOperationLease(ctx, storage.OperationLease{
		ApplyID: apply.ID, OperationID: opID, Owner: "driver", Token: "op-token",
	})

	apply.State = state.Apply.Failed
	apply.ErrorMessage = "direct write attempt"
	err := store.Applies().Update(opOnlyCtx, apply)
	require.ErrorIs(t, err, storage.ErrApplyLeaseLost, "an operation-lease-only context must not directly write the parent apply")
	unchanged, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Running, unchanged.State, "the parent row must be untouched")

	// A single-operation drive carries both leases, so the direct write lands on
	// the parent-lease path.
	_, err = testDB.ExecContext(ctx, `UPDATE applies SET lease_owner=?, lease_token=?, lease_acquired_at=NOW() WHERE id=?`, "current-driver", "apply-token", apply.ID)
	require.NoError(t, err)
	dualCtx := storage.WithApplyLease(opOnlyCtx, storage.ApplyLease{ApplyID: apply.ID, Owner: "current-driver", Token: "apply-token"})
	require.NoError(t, store.Applies().Update(dualCtx, apply))
	written, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Failed, written.State)
}

// TestApplyStore_HeartbeatRejectsOperationLeaseOnlyContext verifies that a drive
// holding only an operation lease cannot heartbeat the parent applies row: the
// parent's liveness is owned by the parent lease and the rollout projection. A
// single-operation drive carries the parent apply lease alongside the operation
// lease, so its heartbeat still lands.
func TestApplyStore_HeartbeatRejectsOperationLeaseOnlyContext(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_heartbeat_oplease", 907, state.Apply.Running, "staging")
	opID := createApplyOperationForLeaseTest(t, store, apply.ID, "primary")
	stampOperationLease(t, opID, "driver", "op-token")

	_, err := testDB.ExecContext(ctx, `UPDATE applies SET updated_at = NOW() - INTERVAL 5 MINUTE WHERE id = ?`, apply.ID)
	require.NoError(t, err)
	before, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)

	opOnlyCtx := storage.WithOperationLease(ctx, storage.OperationLease{
		ApplyID: apply.ID, OperationID: opID, Owner: "driver", Token: "op-token",
	})
	require.ErrorIs(t, store.Applies().Heartbeat(opOnlyCtx, apply.ID), storage.ErrApplyLeaseLost,
		"an operation-lease-only context must not heartbeat the parent apply")
	unchanged, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	assert.Equal(t, before.UpdatedAt, unchanged.UpdatedAt, "the parent row must be untouched")

	// A single-operation drive carries both leases, so the heartbeat lands.
	_, err = testDB.ExecContext(ctx, `UPDATE applies SET lease_owner=?, lease_token=?, lease_acquired_at=NOW() WHERE id=?`, "current-driver", "apply-token", apply.ID)
	require.NoError(t, err)
	dualCtx := storage.WithApplyLease(opOnlyCtx, storage.ApplyLease{ApplyID: apply.ID, Owner: "current-driver", Token: "apply-token"})
	require.NoError(t, store.Applies().Heartbeat(dualCtx, apply.ID))
	after, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	assert.True(t, after.UpdatedAt.After(before.UpdatedAt), "the heartbeat must move updated_at forward")
}

func TestApplyStore_GetInProgress(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")

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

// ClaimApplyByID claims one specific apply by ID under the operator
// claimability rules. The operation-level claim loop uses it to acquire the
// parent apply lease after claiming an apply_operations row, so a pending apply
// with tasks must be claimable, the claim must rotate a fresh lease, and a
// repeat claim must be rejected while the lease is fresh.
func TestApplyStore_ClaimApplyByIDClaimsPendingWithTasks(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
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
	assert.Nil(t, again, "a freshly claimed apply is owned by its current driver")
}

// ClaimApplyByID must not steal a fresh lease from a healthy apply-level driver,
// so a running apply with a fresh heartbeat is not claimable; it only becomes
// claimable once its heartbeat goes stale.
func TestApplyStore_ClaimApplyByIDSkipsFreshRunningUntilStale(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_claim_by_id_running", 601, state.Apply.Running, "staging")

	fresh, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-a")
	require.NoError(t, err)
	assert.Nil(t, fresh, "a fresh running apply is owned by its active driver")

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

// ClaimApplyByID must refuse a failed_retryable apply whose recovery budget is
// exhausted or whose failure is outside the redispatch freshness window. The
// stale-active operation claim arm can hand such a parent to the operation
// loop; the refused parent claim is what routes it to unclaimable-parent
// reconciliation for settling instead of another drive.
func TestApplyStore_ClaimApplyByIDRefusesUnrecoverableRetryable(t *testing.T) {
	t.Run("failure outside the freshness window", func(t *testing.T) {
		clearTables(t)
		ctx := t.Context()
		store := NewMySQL(testDB)

		lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
		apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_claim_by_id_old_retryable", 604, state.Apply.FailedRetryable, "staging")
		_, err := testDB.ExecContext(ctx, `
			UPDATE applies
			SET updated_at = NOW() - INTERVAL ? DAY
			WHERE id = ?
		`, retryableRecoveryFreshnessDays+1, apply.ID)
		require.NoError(t, err)

		claimed, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-a")
		require.NoError(t, err)
		assert.Nil(t, claimed, "stale retryable failures are never auto-redispatched")

		persisted, err := store.Applies().Get(ctx, apply.ID)
		require.NoError(t, err)
		require.NotNil(t, persisted)
		assert.Equal(t, state.Apply.FailedRetryable, persisted.State)
		assert.Equal(t, 0, persisted.Attempt)
	})

	t.Run("recovery budget exhausted", func(t *testing.T) {
		clearTables(t)
		ctx := t.Context()
		store := NewMySQL(testDB)

		lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
		apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_claim_by_id_spent_retryable", 605, state.Apply.FailedRetryable, "staging")
		_, err := testDB.ExecContext(ctx, `
			UPDATE applies SET attempt = ? WHERE id = ?
		`, maxRecoveryAttempts, apply.ID)
		require.NoError(t, err)

		claimed, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-a")
		require.NoError(t, err)
		assert.Nil(t, claimed, "a retryable apply with no recovery budget left must not be redispatched")

		persisted, err := store.Applies().Get(ctx, apply.ID)
		require.NoError(t, err)
		require.NotNil(t, persisted)
		assert.Equal(t, state.Apply.FailedRetryable, persisted.State)
		assert.Equal(t, maxRecoveryAttempts, persisted.Attempt)
	})
}

// ClaimApplyByID is how the operation-level claim loop acquires the parent apply
// lease after leasing a stale operation row. When a PlanetScale driver crashes
// mid-setup the operation row stays running (stale) while the parent apply sits
// in one of the setup-phase states, so ClaimApplyByID must reclaim a stale
// parent in every one of them — a state missing from the claim list would
// strand a crash in that exact phase — while still refusing one whose
// heartbeat is fresh.
func TestApplyStore_ClaimApplyByIDClaimsStaleSetupPhase(t *testing.T) {
	for i, setupState := range []string{
		state.Apply.PreparingBranch,
		state.Apply.ApplyingBranchChanges,
		state.Apply.ValidatingBranch,
		state.Apply.CreatingDeployRequest,
		state.Apply.ValidatingDeployRequest,
	} {
		t.Run(setupState, func(t *testing.T) {
			clearTables(t)
			ctx := t.Context()
			store := NewMySQL(testDB)

			lock := createTestLock(t, store, "testdb", storage.DatabaseTypeVitess)
			apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_claim_setup_"+setupState, int64(701+i), setupState, "staging")
			require.Equal(t, storage.EnginePlanetScale, apply.Engine, "setup-phase states only occur for the PlanetScale engine")

			fresh, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-a")
			require.NoError(t, err)
			assert.Nil(t, fresh, "a fresh setup-phase apply is owned by its active driver")

			_, err = testDB.ExecContext(ctx, `
				UPDATE applies
				SET updated_at = NOW() - INTERVAL 2 MINUTE
				WHERE id = ?
			`, apply.ID)
			require.NoError(t, err)

			claimed, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-a")
			require.NoError(t, err)
			require.NotNil(t, claimed, "a stale setup-phase parent apply must be reclaimable")
			assert.Equal(t, setupState, claimed.State)
			assert.Equal(t, storage.EnginePlanetScale, claimed.Engine, "the reclaimed parent apply keeps its PlanetScale engine")
			assert.Equal(t, "operator-a", claimed.LeaseOwner)
			assert.NotEmpty(t, claimed.LeaseToken)
		})
	}
}

// An apply dwells in a revert-phase state (reverting, skipping_revert) while
// the engine reverts or finalizes the deploy request. If the driver dies there
// — routine pod churn is enough — a fresh driver must reclaim the apply once
// the heartbeat goes stale and resume polling the engine to its terminal
// state; otherwise the apply is stranded non-terminal forever with no driver.
// The operation-level claim loop leases the orphaned apply_operations row
// first and then needs the parent apply lease via ClaimApplyByID, so a stale
// revert-phase parent must be reclaimable through it as well. The claim only
// rotates the lease: the persisted state must stay the revert-phase state so
// the resumed drive re-attaches to the in-flight engine revert instead of
// restarting the apply.
func TestApplyStore_ClaimApplyByIDClaimsStaleRevertPhase(t *testing.T) {
	for _, revertState := range []string{
		state.Apply.Reverting,
		state.Apply.SkippingRevert,
	} {
		t.Run(revertState, func(t *testing.T) {
			clearTables(t)
			ctx := t.Context()
			store := NewMySQL(testDB)

			lock := createTestLock(t, store, "testdb", storage.DatabaseTypeVitess)
			apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_claim_revert_"+revertState, 703, revertState, "staging")
			require.Equal(t, storage.EnginePlanetScale, apply.Engine, "revert-phase states only occur for the PlanetScale engine")

			fresh, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-a")
			require.NoError(t, err)
			assert.Nil(t, fresh, "a fresh revert-phase apply is owned by its active driver")

			_, err = testDB.ExecContext(ctx, `
				UPDATE applies
				SET updated_at = NOW() - INTERVAL 2 MINUTE
				WHERE id = ?
			`, apply.ID)
			require.NoError(t, err)

			claimed, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-a")
			require.NoError(t, err)
			require.NotNil(t, claimed, "a stale revert-phase parent apply must be reclaimable")
			assert.Equal(t, revertState, claimed.State)
			assert.Equal(t, "operator-a", claimed.LeaseOwner)
			assert.NotEmpty(t, claimed.LeaseToken)

			persisted, err := store.Applies().Get(ctx, apply.ID)
			require.NoError(t, err)
			require.NotNil(t, persisted)
			assert.Equal(t, revertState, persisted.State, "the claim rotates the lease without transitioning the revert-phase state")
			assert.Equal(t, "operator-a", persisted.LeaseOwner)

			again, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-b")
			require.NoError(t, err)
			assert.Nil(t, again, "the reclaimed apply's fresh lease is not stolen by a peer")
		})
	}
}

// TestApplyStore_ClaimApplyByIDClaimsStaleWaitingForCutover verifies driver
// recovery at the cutover gate: waiting_for_cutover is a claimable dwell
// state, so an apply parked there belongs to its live driver while the
// heartbeat is fresh and becomes reclaimable through the stale-active arm once
// the heartbeat goes stale — a driver crash at the highest-risk dwell state
// never strands the operator's cutover.
func TestApplyStore_ClaimApplyByIDClaimsStaleWaitingForCutover(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_waiting_for_cutover", 1, state.Apply.WaitingForCutover, "staging")

	freshClaim, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-a")
	require.NoError(t, err)
	assert.Nil(t, freshClaim, "a fresh waiting-for-cutover apply is owned by its active driver")

	_, err = testDB.ExecContext(ctx, `
		UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?
	`, apply.ID)
	require.NoError(t, err)

	claimed, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-a")
	require.NoError(t, err)
	require.NotNil(t, claimed, "a stale waiting-for-cutover apply must be reclaimable so the pending cutover is never stranded")
	assert.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)
	assert.Equal(t, state.Apply.WaitingForCutover, claimed.State)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.WaitingForCutover, persisted.State, "the claim rotates the lease without leaving the cutover gate")
	assert.Equal(t, "operator-a", persisted.LeaseOwner)

	again, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-b")
	require.NoError(t, err)
	assert.Nil(t, again, "the reclaimed apply's fresh lease is not stolen by a peer")
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

// FindStuckPendingApplies is the operator's stuck-pending observability probe.
// It surfaces pending applies a driver should already have claimed — pending
// with child rows, the same claimability predicate ClaimApplyByID uses — that
// have aged past the threshold, so an operator can tell a wedged or saturated
// driver pool from a healthy one. It must never surface a young pending apply
// (still within normal claim latency), a pending apply with no child rows (not
// yet claimable), or a terminal apply.
func TestApplyStore_FindStuckPendingApplies(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	// Each pending apply needs a distinct target: apply creation rejects a
	// second active apply for the same database/type/environment, and pending is
	// an active state.
	seedPending := func(t *testing.T, applyID string, planID int64, withTask bool) *storage.Apply {
		t.Helper()
		lock := createTestLock(t, store, "db_"+applyID, storage.DatabaseTypeMySQL)
		apply := createTestApplyWithStateAndEnv(t, store, lock, applyID, planID, state.Apply.Pending, "staging")
		if withTask {
			now := time.Now()
			_, err := store.Tasks().Create(ctx, &storage.Task{
				TaskIdentifier: "task_" + applyID,
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
		}
		return apply
	}
	// A VSchema-only apply carries an apply_operations row but no tasks; that
	// operation row alone makes it claimable, so the stuck scan must surface it.
	seedPendingVSchemaOnly := func(t *testing.T, applyID string, planID int64) *storage.Apply {
		t.Helper()
		lock := createTestLock(t, store, "db_"+applyID, storage.DatabaseTypeVitess)
		apply := createTestApplyWithStateAndEnv(t, store, lock, applyID, planID, state.Apply.Pending, "staging")
		now := time.Now()
		_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID:    apply.ID,
			Deployment: apply.Database,
			State:      state.ApplyOperation.Pending,
			CreatedAt:  now,
			UpdatedAt:  now,
		})
		require.NoError(t, err)
		return apply
	}
	backdate := func(t *testing.T, id int64, age time.Duration) {
		t.Helper()
		_, err := testDB.ExecContext(ctx,
			fmt.Sprintf(`UPDATE applies SET created_at = NOW() - INTERVAL %d SECOND WHERE id = ?`, int(age.Seconds())),
			id)
		require.NoError(t, err)
	}

	// Old claimable pending applies → stuck, oldest first. The tasks arm and the
	// apply_operations arm are both claimable, so both must be surfaced.
	oldest := seedPending(t, "apply_oldest_claimable", 9001, true)
	backdate(t, oldest.ID, 40*time.Minute)
	vschemaOnly := seedPendingVSchemaOnly(t, "apply_vschema_only", 9006)
	backdate(t, vschemaOnly.ID, 30*time.Minute)
	older := seedPending(t, "apply_old_claimable", 9002, true)
	backdate(t, older.ID, 20*time.Minute)

	// Young claimable pending apply (created now) → below threshold, not stuck.
	seedPending(t, "apply_young_claimable", 9003, true)

	// Old pending apply with no child rows → not yet claimable, excluded.
	noChildren := seedPending(t, "apply_old_no_children", 9004, false)
	backdate(t, noChildren.ID, 40*time.Minute)

	// Old terminal apply → not pending, excluded.
	completedLock := createTestLock(t, store, "db_completed", storage.DatabaseTypeMySQL)
	completed := createTestApplyWithStateAndEnv(t, store, completedLock, "apply_old_completed", 9005, state.Apply.Completed, "staging")
	backdate(t, completed.ID, 40*time.Minute)

	threshold := 10 * time.Minute

	t.Run("returns aged claimable pending applies oldest first", func(t *testing.T) {
		stuck, err := store.Applies().FindStuckPendingApplies(ctx, threshold, 0)
		require.NoError(t, err)
		require.Len(t, stuck, 3)
		assert.Equal(t, oldest.ApplyIdentifier, stuck[0].ApplyIdentifier)
		assert.Equal(t, vschemaOnly.ApplyIdentifier, stuck[1].ApplyIdentifier,
			"a task-less pending apply with an apply_operations row must be surfaced")
		assert.Equal(t, older.ApplyIdentifier, stuck[2].ApplyIdentifier)
		assert.Equal(t, state.Apply.Pending, stuck[0].State)
	})

	t.Run("limit caps the result to the oldest rows", func(t *testing.T) {
		stuck, err := store.Applies().FindStuckPendingApplies(ctx, threshold, 1)
		require.NoError(t, err)
		require.Len(t, stuck, 1)
		assert.Equal(t, oldest.ApplyIdentifier, stuck[0].ApplyIdentifier)
	})

	t.Run("a threshold above every age surfaces nothing", func(t *testing.T) {
		stuck, err := store.Applies().FindStuckPendingApplies(ctx, time.Hour, 0)
		require.NoError(t, err)
		assert.Empty(t, stuck)
	})
}

// A VSchema-only apply carries an apply_operations row but no tasks — the
// VSchema application is the whole apply. Its dual-written operation row proves
// the create committed fully, so the pending claim must accept it; gating the
// claim on tasks alone would strand every queued VSchema-only apply pending
// forever.
func TestApplyStore_ClaimApplyByIDClaimsTasklessPendingApplyWithOperation(t *testing.T) {
	clearTables(t)
	store := NewMySQL(testDB)
	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeVitess)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_vschema_only_byid", 503, state.Apply.Pending, "staging")
	now := time.Now()
	_, err := store.ApplyOperations().Insert(t.Context(), &storage.ApplyOperation{
		ApplyID:    apply.ID,
		Deployment: apply.Database,
		State:      state.ApplyOperation.Pending,
		CreatedAt:  now,
		UpdatedAt:  now,
	})
	require.NoError(t, err)

	claimed, err := store.Applies().ClaimApplyByID(t.Context(), apply.ID, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimed, "a task-less pending apply with an operation row must be claimable")
	assert.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)
}

func TestApplyStore_ClaimApplyByIDClaimsPendingControlRequestWithoutTasks(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_pending_start_request", 503, state.Apply.Pending, "staging")
	_, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:   apply.ID,
		Operation: storage.ControlOperationStart,
		Status:    storage.ControlRequestPending,
		Metadata:  []byte(`{}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	claimed, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)
	assert.Equal(t, state.Apply.Pending, claimed.State)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.Running, persisted.State)

	claimedAgain, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "test-owner")
	require.NoError(t, err)
	assert.Nil(t, claimedAgain, "claim heartbeat should prevent another driver from immediately taking the same start request")
}

func TestApplyStore_ClaimApplyByIDClaimsWaitingForDeployStartControlRequest(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeVitess)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_waiting_deploy_start_request", 505, state.Apply.WaitingForDeploy, "staging")
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies
		SET lease_owner = 'waiting-owner', lease_token = 'waiting-token', lease_acquired_at = NOW() - INTERVAL 2 MINUTE, updated_at = NOW()
		WHERE id = ?
	`, apply.ID)
	require.NoError(t, err)
	_, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
		Metadata:    []byte(`{}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	claimed, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)
	assert.Equal(t, state.Apply.WaitingForDeploy, claimed.State)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.WaitingForDeploy, persisted.State)
	assert.Equal(t, "test-owner", persisted.LeaseOwner)
	assert.Equal(t, claimed.LeaseToken, persisted.LeaseToken)

	claimedAgain, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "test-owner")
	require.NoError(t, err)
	assert.Nil(t, claimedAgain, "fresh processing lease should prevent another driver from taking the same deferred deploy start request")

	_, err = testDB.ExecContext(ctx, `
		UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?
	`, apply.ID)
	require.NoError(t, err)

	reclaimed, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "recovery-owner")
	require.NoError(t, err)
	require.NotNil(t, reclaimed, "stale deferred deploy start processing owner should be reclaimable")
	assert.Equal(t, apply.ApplyIdentifier, reclaimed.ApplyIdentifier)
}

// TestApplyStore_ClaimStoppedStartRefusedWhenTargetActive verifies the
// one-active-apply-per-target invariant survives a stopped-apply claim. A
// stopped apply is not "active", so a newer apply can become active for the same
// target while it sits stopped. Claiming the stopped apply must re-check the
// target under the apply-target lock: when another active apply owns the target
// the claim is refused, the stopped apply stays stopped, and the pending start
// control request is failed with an operator-visible reason — so the target
// never ends up with two running applies.
func TestApplyStore_ClaimStoppedStartRefusedWhenTargetActive(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
	active := createTestApplyWithStateAndEnv(t, store, lock, "apply_active_running", 1, state.Apply.Running, "staging")
	stopped := createTestApplyWithStateAndEnv(t, store, lock, "apply_stopped_blocked", 2, state.Apply.Stopped, "staging")

	_, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:   stopped.ID,
		Operation: storage.ControlOperationStart,
		Status:    storage.ControlRequestPending,
		Metadata:  []byte(`{}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	claimed, err := store.Applies().ClaimApplyByID(ctx, stopped.ID, "test-owner")
	require.NoError(t, err)
	assert.Nil(t, claimed, "claim must be refused while another active apply owns the target")

	persistedStopped, err := store.Applies().Get(ctx, stopped.ID)
	require.NoError(t, err)
	require.NotNil(t, persistedStopped)
	assert.Equal(t, state.Apply.Stopped, persistedStopped.State, "refused claim must leave the apply stopped")

	stillPending, err := store.ControlRequests().GetPending(ctx, stopped.ID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.Nil(t, stillPending, "the start request must no longer be pending after refusal")

	failed := getStartControlRequest(t, stopped.ID)
	assert.Equal(t, storage.ControlRequestFailed, failed.Status)
	assert.Contains(t, failed.ErrorMessage, "another active apply exists for testdb/mysql/staging")

	assertExactlyOneRunningApply(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")

	persistedActive, err := store.Applies().Get(ctx, active.ID)
	require.NoError(t, err)
	require.NotNil(t, persistedActive)
	assert.Equal(t, state.Apply.Running, persistedActive.State)
}

// TestApplyStore_ClaimStoppedStartSucceedsWhenTargetClear verifies the happy
// path of the stopped→resuming claim re-check: with no other active apply on the
// target, claiming a stopped apply that carries a pending start control request
// transitions it to resuming and leaves exactly one active apply for the target.
func TestApplyStore_ClaimStoppedStartSucceedsWhenTargetClear(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
	stopped := createTestApplyWithStateAndEnv(t, store, lock, "apply_stopped_clear", 1, state.Apply.Stopped, "staging")

	_, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:   stopped.ID,
		Operation: storage.ControlOperationStart,
		Status:    storage.ControlRequestPending,
		Metadata:  []byte(`{}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	claimed, err := store.Applies().ClaimApplyByID(ctx, stopped.ID, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimed, "clear target must allow the stopped apply to be claimed")
	assert.Equal(t, stopped.ApplyIdentifier, claimed.ApplyIdentifier)
	assert.Equal(t, state.Apply.Stopped, claimed.State, "caller sees the pre-claim state")

	persisted, err := store.Applies().Get(ctx, stopped.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.Resuming, persisted.State, "claim must transition stopped → resuming")

	// The resume path (operator) completes the start request after a successful
	// claim; the storage claim's job is only to safely move the apply into the
	// transient resuming state.
	stillPending, err := store.ControlRequests().GetPending(ctx, stopped.ID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.NotNil(t, stillPending, "a successful claim leaves the start request pending for the resume path to complete")

	assertExactlyOneActiveApply(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
}

// TestApplyStore_ClaimApplyByIDSkipsFailedStoppedStartUntilRerequested verifies
// that a failed start request never restarts a stopped apply on its own: the
// stopped arm of the claim predicate requires a pending start control request,
// so after a start attempt fails the apply stays stopped until an operator
// deliberately re-requests the start, at which point the claim succeeds again.
func TestApplyStore_ClaimApplyByIDSkipsFailedStoppedStartUntilRerequested(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_stopped_failed_start", 1, state.Apply.Stopped, "staging")

	_, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:   apply.ID,
		Operation: storage.ControlOperationStart,
		Status:    storage.ControlRequestPending,
		Metadata:  []byte(`{}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)
	require.NoError(t, store.ControlRequests().FailPending(ctx, apply.ID, storage.ControlOperationStart, "remote start failed"))

	claimed, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "test-owner")
	require.NoError(t, err)
	assert.Nil(t, claimed, "a failed start request must not be retried automatically by operator claims")

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.Stopped, persisted.State, "the apply stays stopped until an operator re-requests the start")

	rerequested, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator-retry",
		Metadata:    []byte(`{}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)
	require.Equal(t, storage.ControlRequestPending, rerequested.Status)

	claimedAfterRetry, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimedAfterRetry, "a re-requested start makes the stopped apply claimable again")
	assert.Equal(t, apply.ApplyIdentifier, claimedAfterRetry.ApplyIdentifier)

	persistedAfterRetry, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persistedAfterRetry)
	assert.Equal(t, state.Apply.Resuming, persistedAfterRetry.State, "the successful claim transitions stopped → resuming")

	again, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-b")
	require.NoError(t, err)
	assert.Nil(t, again, "the just-resumed apply belongs to its claimer; a repeat claim must not steal the fresh lease")
}

// TestApplyStore_ClaimApplyByIDClaimsStoppedWithPendingCancel verifies that a
// stopped apply carrying a pending cancel control request is claimable so a
// drive can deliver the cancel. The claim rotates only the lease: the apply
// keeps its stopped state (the drive, not the claim, terminalizes it) and the
// cancel request stays pending for the drive to consume. A lease acquired after
// the request blocks rematching, so a cancel the drive could not finish is
// retried on lease staleness, not on every poll — but a request whose stored
// timestamp equals the lease's (these columns are second-precision on MySQL)
// still claims, so an operator who stops then immediately cancels does not wait
// out the staleness window.
func TestApplyStore_ClaimApplyByIDClaimsStoppedWithPendingCancel(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_stopped_cancel", 1, state.Apply.Stopped, "staging")

	requestPendingControlOperation(t, store, apply.ID, storage.ControlOperationCancel)

	claimed, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-a")
	require.NoError(t, err)
	require.NotNil(t, claimed, "a stopped apply with a pending cancel must be claimable")
	assert.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)
	assert.Equal(t, "operator-a", claimed.LeaseOwner)
	assert.NotEmpty(t, claimed.LeaseToken)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.Stopped, persisted.State, "a cancel claim rotates only the lease; the drive terminalizes the state")

	stillPending, err := store.ControlRequests().GetPending(ctx, apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.NotNil(t, stillPending, "the cancel request stays pending for the drive to consume")

	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_control_requests
		SET updated_at = NOW() - INTERVAL 2 SECOND
		WHERE apply_id = ?
	`, apply.ID)
	require.NoError(t, err)

	again, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-b")
	require.NoError(t, err)
	assert.Nil(t, again, "a lease acquired after the request must block rematching until the request is re-issued or the lease goes stale")

	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_control_requests
		SET updated_at = (SELECT lease_acquired_at FROM applies WHERE id = ?)
		WHERE apply_id = ?
	`, apply.ID, apply.ID)
	require.NoError(t, err)

	sameSecond, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-c")
	require.NoError(t, err)
	require.NotNil(t, sameSecond, "a request issued in the same second as the lease must claim without waiting out the staleness window")
	assert.Equal(t, "operator-c", sameSecond.LeaseOwner)
}

// TestApplyStore_ClaimApplyByIDStoppedCancelReclaimableWhenLeaseStale verifies
// the retry pacing of the stopped-cancel claim: a claim whose drive could not
// finish the cancel leaves the apply stopped with the request pending, and once
// the lease heartbeat goes stale the apply is claimable again so a peer can
// retry delivery.
func TestApplyStore_ClaimApplyByIDStoppedCancelReclaimableWhenLeaseStale(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_stopped_cancel_stale", 1, state.Apply.Stopped, "staging")

	requestPendingControlOperation(t, store, apply.ID, storage.ControlOperationCancel)

	claimed, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-a")
	require.NoError(t, err)
	require.NotNil(t, claimed)

	_, err = testDB.ExecContext(ctx, `
		UPDATE applies
		SET updated_at = NOW() - INTERVAL 2 MINUTE
		WHERE id = ?
	`, apply.ID)
	require.NoError(t, err)

	reclaimed, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-b")
	require.NoError(t, err)
	require.NotNil(t, reclaimed, "a stale lease with the cancel still pending must make the apply claimable again")
	assert.Equal(t, "operator-b", reclaimed.LeaseOwner)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.Stopped, persisted.State)
}

// TestApplyStore_ClaimStoppedCancelWinsOverPendingStart verifies precedence
// when a stopped apply carries both a pending start and a pending cancel: the
// claim delivers the cancel (the apply stays stopped, never resuming) and fails
// the start request in the same transaction — resuming a copy the operator
// asked to discard is never the right answer, and once the apply terminalizes
// nothing would consume the start.
func TestApplyStore_ClaimStoppedCancelWinsOverPendingStart(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_stopped_cancel_vs_start", 1, state.Apply.Stopped, "staging")

	requestPendingControlOperation(t, store, apply.ID, storage.ControlOperationStart)
	requestPendingControlOperation(t, store, apply.ID, storage.ControlOperationCancel)

	claimed, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "operator-a")
	require.NoError(t, err)
	require.NotNil(t, claimed)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.Stopped, persisted.State, "cancel wins: the claim must not transition the apply toward resume")

	cancelPending, err := store.ControlRequests().GetPending(ctx, apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.NotNil(t, cancelPending, "the cancel request stays pending for the drive to consume")

	startPending, err := store.ControlRequests().GetPending(ctx, apply.ID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.Nil(t, startPending, "the superseded start request must no longer be pending")

	failedStart := getStartControlRequest(t, apply.ID)
	assert.Equal(t, storage.ControlRequestFailed, failedStart.Status)
	assert.Contains(t, failedStart.ErrorMessage, "cancel request is pending")
}

// TestApplyStore_ClaimStoppedCancelAllowedWhileTargetActive verifies that a
// newer active apply on the same target does not block cancelling an older
// stopped apply: the cancel claim performs no active-target re-check because
// delivering a cancel cannot add active work to the target at the apply level —
// the apply stays stopped, so the one-active-apply-per-target invariant the
// re-check protects cannot be violated. (The operation row does move to
// resuming so the drive can terminalize it; that is not an apply the
// active-target check counts.) This is the shape
// an operator hits when a stopped apply must be discarded so its retry — often
// already running on the same database — can proceed cleanly.
func TestApplyStore_ClaimStoppedCancelAllowedWhileTargetActive(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
	active := createTestApplyWithStateAndEnv(t, store, lock, "apply_active_retry", 1, state.Apply.Running, "staging")
	stopped := createTestApplyWithStateAndEnv(t, store, lock, "apply_stopped_discard", 2, state.Apply.Stopped, "staging")

	requestPendingControlOperation(t, store, stopped.ID, storage.ControlOperationCancel)

	claimed, err := store.Applies().ClaimApplyByID(ctx, stopped.ID, "operator-a")
	require.NoError(t, err)
	require.NotNil(t, claimed, "an active apply on the target must not block a cancel claim of the stopped apply")

	persisted, err := store.Applies().Get(ctx, stopped.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.Stopped, persisted.State)

	persistedActive, err := store.Applies().Get(ctx, active.ID)
	require.NoError(t, err)
	require.NotNil(t, persistedActive)
	assert.Equal(t, state.Apply.Running, persistedActive.State)
}

func TestApplyStore_ClaimApplyByIDDoesNotClaimFreshRunningStopControlRequest(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_running_stop_request", 505, state.Apply.Running, "staging")
	_, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:   apply.ID,
		Operation: storage.ControlOperationStop,
		Status:    storage.ControlRequestPending,
		Metadata:  []byte(`{}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)

	claimed, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "test-owner")
	require.NoError(t, err)
	assert.Nil(t, claimed, "fresh running applies are owned by their active driver; pending stop must not create a second owner")
}

func TestApplyStore_ClaimApplyByIDConcurrentPendingClaims(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
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

	const drivers = 16
	stores := make([]*Storage, drivers)
	for i := range drivers {
		db, openErr := sql.Open("block-mysql", testDSNChangedRows)
		require.NoError(t, openErr)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		t.Cleanup(func() {
			require.NoError(t, db.Close())
		})
		stores[i] = NewMySQL(db)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var claimed []*storage.Apply
	var claimErrors []error

	for i := range drivers {
		driverStore := stores[i]
		wg.Go(func() {
			<-start
			got, claimErr := driverStore.Applies().ClaimApplyByID(ctx, apply.ID, "test-owner")

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
	require.Len(t, claimed, 1, "only one operator driver should claim a pending apply")
	assert.Equal(t, apply.ApplyIdentifier, claimed[0].ApplyIdentifier)
	assert.Equal(t, state.Apply.Pending, claimed[0].State)

	persisted, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	assert.Equal(t, state.Apply.Running, persisted.State)
}

// TestApplyStore_ExpireRetryable verifies retryable expiry at the storage
// layer. A retryable apply that has used all attempts becomes failed, and
// unfinished tasks are finalized with completion timestamps: the task that
// had started work is failed, while a pending task that never started is
// cancelled — it was blocked behind the failure, and marking it failed would
// misattribute the failure to a table that did no work.
func TestApplyStore_ExpireRetryable(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_retryable_expired", 501, state.Apply.FailedRetryable, "staging")
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies SET attempt = ? WHERE id = ?
	`, maxRecoveryAttempts, apply.ID)
	require.NoError(t, err)

	now := time.Now()
	_, err = store.Tasks().Create(ctx, &storage.Task{
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
	assert.Equal(t, state.Task.Cancelled, pendingTask.State, "a task that never started is cancelled, not failed")
	assert.NotNil(t, pendingTask.CompletedAt)
}

// TestApplyStore_ExpireRetryableExpiresOldFailures verifies that retryable
// failures are not kept non-terminal forever after their recovery window passes.
func TestApplyStore_ExpireRetryableExpiresOldFailures(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
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

// TestApplyStore_ExpireRetryableTerminalizesRetryableOperations verifies OC-5
// Part A: when a multi-deployment apply's retry budget is spent, ExpireRetryable
// terminalizes the apply's failed_retryable operation rows to failed alongside
// the apply, but leaves a healthy successor parked at waiting_for_cutover
// untouched. The deployment-order claim gates read earlier.state from
// apply_operations, so a row left failed_retryable would keep blocking the
// healthy successor from cutting over under on_failure "continue" even though
// the rollout has already failed.
func TestApplyStore_ExpireRetryableTerminalizesRetryableOperations(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_retryable_ops_expired", 510, state.Apply.FailedRetryable, "staging")
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies SET attempt = ? WHERE id = ?
	`, maxRecoveryAttempts, apply.ID)
	require.NoError(t, err)

	failedOpID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.FailedRetryable,
		OnFailure: storage.OnFailureContinue,
	})
	require.NoError(t, err)
	parkedOpID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b", State: state.ApplyOperation.WaitingForCutover,
		OnFailure: storage.OnFailureContinue,
	})
	require.NoError(t, err)

	expired, err := store.Applies().ExpireRetryable(ctx)
	require.NoError(t, err)
	require.Len(t, expired, 1)
	assert.Equal(t, state.Apply.Failed, expired[0].Apply.State)

	persistedApply, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, persistedApply)
	assert.Equal(t, state.Apply.Failed, persistedApply.State)

	failedOp, err := store.ApplyOperations().Get(ctx, failedOpID)
	require.NoError(t, err)
	require.NotNil(t, failedOp)
	assert.Equal(t, state.ApplyOperation.Failed, failedOp.State, "failed_retryable operation must be terminalized to failed once the parent budget is spent")
	assert.NotNil(t, failedOp.CompletedAt, "terminalized operation must stamp completed_at")

	parkedOp, err := store.ApplyOperations().Get(ctx, parkedOpID)
	require.NoError(t, err)
	require.NotNil(t, parkedOp)
	assert.Equal(t, state.ApplyOperation.WaitingForCutover, parkedOp.State, "a healthy successor parked at the cutover barrier must be left untouched")
}

func TestApplyStore_FindMissingSummaryComment_ExcludesAppliesWithoutGitHubDestination(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	now := time.Now()
	startedAt := now.Add(-time.Minute)

	githubLock := createTestLockWithPR(t, store, "github_db", storage.DatabaseTypeMySQL, "org/repo", 123)
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

	cliLock := createTestLockWithPR(t, store, "cli_db", storage.DatabaseTypeMySQL, "", 0)
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

// seedApplyMissingSummary creates a GitHub-backed apply in the given state with
// a tracked progress comment and no summary marker — the shape
// FindMissingSummaryComment exists to repair. Terminal states other than
// stopped stamp completed_at; a stopped apply keeps it NULL (resumable) and
// must qualify by recency of updated_at instead.
func seedApplyMissingSummary(t *testing.T, store *Storage, name, applyState string, planID int64) *storage.Apply {
	t.Helper()
	ctx := t.Context()
	lock := createTestLockWithPR(t, store, name+"_db", storage.DatabaseTypeMySQL, "org/repo", 123)
	apply := &storage.Apply{
		ApplyIdentifier: name,
		LockID:          lock.ID,
		PlanID:          planID,
		Database:        lock.DatabaseName,
		DatabaseType:    lock.DatabaseType,
		Repository:      lock.Repository,
		PullRequest:     lock.PullRequest,
		Environment:     "staging",
		Caller:          "org/repo#123",
		InstallationID:  12345,
		Engine:          storage.EngineSpirit,
		State:           applyState,
	}
	applyID, err := store.Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID
	if applyState != state.Apply.Stopped {
		now := time.Now()
		apply.CompletedAt = &now
		require.NoError(t, store.Applies().Update(ctx, apply))
	}
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         apply.ID,
		CommentState:    state.Comment.Progress,
		GitHubCommentID: 1001,
	}))
	return apply
}

// TestApplyStore_FindMissingSummaryComment_IncludesRecentlyStoppedApplies
// verifies stopped applies participate in summary reconciliation: a stop is
// terminal for summary purposes even though it is resumable and never stamps
// completed_at, so recency is judged by updated_at. A stopped apply whose last
// update is older than the reconciliation lookback is left alone.
func TestApplyStore_FindMissingSummaryComment_IncludesRecentlyStoppedApplies(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	recent := seedApplyMissingSummary(t, store, "apply_stopped_recent", state.Apply.Stopped, 700)
	stale := seedApplyMissingSummary(t, store, "apply_stopped_stale", state.Apply.Stopped, 701)
	_, err := testDB.ExecContext(ctx, `UPDATE applies SET updated_at = NOW() - INTERVAL 2 HOUR WHERE id = ?`, stale.ID)
	require.NoError(t, err)

	applies, err := store.Applies().FindMissingSummaryComment(ctx)
	require.NoError(t, err)
	require.Len(t, applies, 1)
	assert.Equal(t, recent.ApplyIdentifier, applies[0].ApplyIdentifier)
	assert.Equal(t, state.Apply.Stopped, applies[0].State)
}

// TestApplyStore_FindMissingSummaryComment_SummaryClaimSentinels verifies the
// claim-aware missing test: a fresh claim sentinel means a publisher is posting
// right now, so the apply is not reported; a sentinel stale past
// storage.SummaryClaimStaleAfter means that publisher crashed before posting,
// so the apply is reported for repair; and a marker recording a real posted
// comment never reports.
func TestApplyStore_FindMissingSummaryComment_SummaryClaimSentinels(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	freshClaim := seedApplyMissingSummary(t, store, "apply_claim_fresh", state.Apply.Completed, 710)
	won, err := store.ApplyComments().ClaimSummaryComment(ctx, freshClaim.ID)
	require.NoError(t, err)
	require.True(t, won)

	staleClaim := seedApplyMissingSummary(t, store, "apply_claim_stale", state.Apply.Completed, 711)
	won, err = store.ApplyComments().ClaimSummaryComment(ctx, staleClaim.ID)
	require.NoError(t, err)
	require.True(t, won)
	backdateSummaryClaim(t, staleClaim.ID)

	posted := seedApplyMissingSummary(t, store, "apply_summary_posted", state.Apply.Completed, 712)
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         posted.ID,
		CommentState:    state.Comment.Summary,
		GitHubCommentID: 2002,
	}))

	applies, err := store.Applies().FindMissingSummaryComment(ctx)
	require.NoError(t, err)
	require.Len(t, applies, 1)
	assert.Equal(t, staleClaim.ApplyIdentifier, applies[0].ApplyIdentifier)
}

// TestApplyStore_FindMissingSummaryComment_SupersededSummaryMarker verifies a
// superseded summary marker counts as missing: a stop's summary consumed by a
// resume rotation belongs to an earlier terminal state, so once the apply
// reaches its next terminal state, reconciliation must repair the summary
// rather than treating the stale marker as posted.
func TestApplyStore_FindMissingSummaryComment_SupersededSummaryMarker(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	superseded := seedApplyMissingSummary(t, store, "apply_summary_superseded", state.Apply.Completed, 720)
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         superseded.ID,
		CommentState:    state.Comment.Summary,
		GitHubCommentID: 2003,
	}))
	require.NoError(t, store.ApplyComments().Supersede(ctx, superseded.ID, state.Comment.Summary))

	live := seedApplyMissingSummary(t, store, "apply_summary_live", state.Apply.Completed, 721)
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID:         live.ID,
		CommentState:    state.Comment.Summary,
		GitHubCommentID: 2004,
	}))

	applies, err := store.Applies().FindMissingSummaryComment(ctx)
	require.NoError(t, err)
	require.Len(t, applies, 1)
	assert.Equal(t, superseded.ApplyIdentifier, applies[0].ApplyIdentifier)
}

func TestApplyStore_GetByPR(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	// Create locks for different PRs
	lock1 := createTestLockWithPR(t, store, "db1", "mysql", "org/repo", 100)
	lock2 := createTestLockWithPR(t, store, "db2", "mysql", "org/repo", 200)

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
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
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
	store := NewMySQL(testDB)

	// Create locks for different PRs
	lock1 := createTestLockWithPR(t, store, "db1", "mysql", "org/repo", 100)
	lock2 := createTestLockWithPR(t, store, "db2", "mysql", "org/repo", 100)
	lock3 := createTestLockWithPR(t, store, "db3", "mysql", "org/repo", 200)

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
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
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
	store := NewMySQL(testDB)

	lock1 := createTestLockWithPR(t, store, "db1", "mysql", "org/repo", 110)
	lock2 := createTestLockWithPR(t, store, "db2", "mysql", "org/repo", 210)
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
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")

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
}

func TestApplyStore_UpdateOptions(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
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
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")

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

// The fixture helpers delegate to storagetest so this package's tests and the
// cross-dialect parity suite always build identical Lock/Apply row shapes.

func createTestLock(t *testing.T, store *Storage, dbName, dbType string) *storage.Lock {
	t.Helper()
	return storagetest.CreateLock(t, store, dbName, dbType)
}

func createTestLockWithPR(t *testing.T, store *Storage, dbName, dbType, repo string, pr int) *storage.Lock {
	t.Helper()
	return storagetest.CreateLockWithPR(t, store, dbName, dbType, repo, pr)
}

// requestPendingControlOperation records a pending control request of the
// given operation for the apply, failing the test if one is already pending.
func requestPendingControlOperation(t *testing.T, store *Storage, applyID int64, op storage.ControlOperation) {
	t.Helper()
	_, alreadyPending, err := store.ControlRequests().RequestPending(t.Context(), &storage.ApplyControlRequest{
		ApplyID:   applyID,
		Operation: op,
		Status:    storage.ControlRequestPending,
		Metadata:  []byte(`{}`),
	})
	require.NoError(t, err)
	require.False(t, alreadyPending)
}

// getStartControlRequest reads the 'start' control request for an apply
// regardless of status, so tests can assert on a failed request that the
// status-scoped GetPending accessor cannot return.
func getStartControlRequest(t *testing.T, applyID int64) *storage.ApplyControlRequest {
	t.Helper()
	row := testDB.QueryRowContext(t.Context(), `
		SELECT `+controlRequestColumns+`
		FROM apply_control_requests
		WHERE apply_id = ? AND operation = ?
	`, applyID, storage.ControlOperationStart)
	req, err := scanControlRequest(row)
	require.NoError(t, err)
	require.NotNil(t, req, "expected a start control request for apply %d", applyID)
	return req
}

// assertExactlyOneRunningApply fails unless exactly one running apply exists for
// the target, guarding the one-active-apply-per-target invariant.
func assertExactlyOneRunningApply(t *testing.T, store *Storage, database, dbType, environment string) {
	t.Helper()
	applies, err := store.Applies().GetByDatabase(t.Context(), database, dbType, environment)
	require.NoError(t, err)
	running := 0
	for _, a := range applies {
		if state.IsState(a.State, state.Apply.Running) {
			running++
		}
	}
	assert.Equal(t, 1, running, "exactly one running apply must exist for %s/%s/%s", database, dbType, environment)
}

// assertExactlyOneActiveApply fails unless exactly one non-terminal apply exists
// for the target, guarding the one-active-apply-per-target invariant for
// transient active states such as resuming that are not running-family.
func assertExactlyOneActiveApply(t *testing.T, store *Storage, database, dbType, environment string) {
	t.Helper()
	applies, err := store.Applies().GetByDatabase(t.Context(), database, dbType, environment)
	require.NoError(t, err)
	active := 0
	for _, a := range applies {
		if !state.IsTerminalApplyState(a.State) {
			active++
		}
	}
	assert.Equal(t, 1, active, "exactly one active apply must exist for %s/%s/%s", database, dbType, environment)
}

func createTestApply(t *testing.T, store *Storage, lock *storage.Lock, applyID string, planID int64) *storage.Apply {
	t.Helper()
	return storagetest.CreateApply(t, store, lock, applyID, planID)
}

func createTestApplyWithStateAndEnv(t *testing.T, store *Storage, lock *storage.Lock, applyID string, planID int64, applyState, env string) *storage.Apply {
	t.Helper()
	return storagetest.CreateApplyWithStateAndEnv(t, store, lock, applyID, planID, applyState, env)
}

func createTestApplyWithStateEnvDeployment(t *testing.T, store *Storage, lock *storage.Lock, applyID string, planID int64, applyState, env, deployment string) *storage.Apply {
	t.Helper()
	return storagetest.CreateApplyWithStateEnvDeployment(t, store, lock, applyID, planID, applyState, env, deployment)
}

// DB error tests

func TestApplyStore_Create_DBError(t *testing.T) {
	db, err := sql.Open("block-mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := NewMySQL(db)
	_, err = store.Applies().Create(t.Context(), &storage.Apply{
		ApplyIdentifier: "test",
		State:           state.Apply.Pending,
	})
	require.Error(t, err)
}

func TestApplyStore_Get_DBError(t *testing.T) {
	db, err := sql.Open("block-mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := NewMySQL(db)
	_, err = store.Applies().Get(t.Context(), 1)
	require.Error(t, err)
}

func TestApplyStore_GetByApplyIdentifier_DBError(t *testing.T) {
	db, err := sql.Open("block-mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := NewMySQL(db)
	_, err = store.Applies().GetByApplyIdentifier(t.Context(), "test")
	require.Error(t, err)
}

func TestApplyStore_GetByLock_DBError(t *testing.T) {
	db, err := sql.Open("block-mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := NewMySQL(db)
	_, err = store.Applies().GetByLock(t.Context(), 1)
	require.Error(t, err)
}

func TestApplyStore_GetInProgress_DBError(t *testing.T) {
	db, err := sql.Open("block-mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := NewMySQL(db)
	_, err = store.Applies().GetInProgress(t.Context())
	require.Error(t, err)
}

func TestApplyStore_Update_DBError(t *testing.T) {
	db, err := sql.Open("block-mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := NewMySQL(db)
	err = store.Applies().Update(t.Context(), &storage.Apply{ID: 1, State: "running"})
	require.Error(t, err)
}

func TestApplyStore_Delete_DBError(t *testing.T) {
	db, err := sql.Open("block-mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := NewMySQL(db)
	err = store.Applies().Delete(t.Context(), 1)
	require.Error(t, err)
}

func TestApplyStore_DeleteByPR_DBError(t *testing.T) {
	db, err := sql.Open("block-mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := NewMySQL(db)
	err = store.Applies().DeleteByPR(t.Context(), "org/repo", 123)
	require.Error(t, err)
}

func TestApplyStore_GetByDatabase_DBError(t *testing.T) {
	db, err := sql.Open("block-mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := NewMySQL(db)
	_, err = store.Applies().GetByDatabase(t.Context(), "db", "mysql", "staging")
	require.Error(t, err)
}

func TestApplyStore_GetByPR_DBError(t *testing.T) {
	db, err := sql.Open("block-mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := NewMySQL(db)
	_, err = store.Applies().GetByPR(t.Context(), "org/repo", 123)
	require.Error(t, err)
}

func TestApplyStore_GetByPlan_DBError(t *testing.T) {
	db, err := sql.Open("block-mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	store := NewMySQL(db)
	_, err = store.Applies().GetByPlan(t.Context(), 123)
	require.Error(t, err)
}

// TestApplyStore_FindNextApplyForStopReconciliation_ClaimsStrandedContinueApply
// verifies the stop-reconciliation trigger: an apply held running under
// on_failure "continue" (a failed earlier sibling) with a pending stop and a
// pending sibling that the claim gate keeps from starting is claimed here, so
// the operator can stop the pending siblings and let the apply settle. Without
// this path no operation is claimable and the stop would strand forever.
func TestApplyStore_FindNextApplyForStopReconciliation_ClaimsStrandedContinueApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_stop_recon", 1, state.Apply.Running, "staging")

	failedID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.Failed, OnFailure: storage.OnFailureContinue,
	})
	require.NoError(t, err)
	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b", OnFailure: storage.OnFailureContinue,
	})
	require.NoError(t, err)
	// Backdate the failed row so it can't be mistaken for a fresh active op.
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 1 HOUR WHERE id = ?
	`, failedID)
	require.NoError(t, err)

	_, _, err = store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
	})
	require.NoError(t, err)

	claimed, err := store.Applies().FindNextApplyForStopReconciliation(ctx, "test-operator")
	require.NoError(t, err)
	require.NotNil(t, claimed, "an apply with a pending stop and pending siblings must be claimable for reconciliation")
	assert.Equal(t, apply.ID, claimed.ID)
	assert.Equal(t, "test-operator", claimed.LeaseOwner, "claim rotates the lease owner")
	assert.Equal(t, state.Apply.Running, claimed.State, "reconciliation claim refreshes the lease without changing apply state")
}

// TestApplyStore_FindNextApplyForStopReconciliation_ClaimsStrandedPausedApply
// verifies the trigger also covers on_failure "pause": a rollout held paused by
// a failed earlier sibling (no release) with a pending stop and a pending
// sibling is claimed here so the operator can stop the held siblings and settle
// the apply failed. paused is deliberately absent from claimableApplyStates, so
// without paused in the reconciliation parent eligibility a stopped paused apply
// would strand its pending siblings forever.
func TestApplyStore_FindNextApplyForStopReconciliation_ClaimsStrandedPausedApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_stop_recon_paused", 1, state.Apply.Paused, "staging")

	failedID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.Failed, OnFailure: storage.OnFailurePause,
	})
	require.NoError(t, err)
	_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b", OnFailure: storage.OnFailurePause,
	})
	require.NoError(t, err)
	// Backdate the failed row so it can't be mistaken for a fresh active op.
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_operations SET updated_at = NOW() - INTERVAL 1 HOUR WHERE id = ?
	`, failedID)
	require.NoError(t, err)

	_, _, err = store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
	})
	require.NoError(t, err)

	claimed, err := store.Applies().FindNextApplyForStopReconciliation(ctx, "test-operator")
	require.NoError(t, err)
	require.NotNil(t, claimed, "a paused apply with a pending stop and pending siblings must be claimable for reconciliation")
	assert.Equal(t, apply.ID, claimed.ID)
	assert.Equal(t, "test-operator", claimed.LeaseOwner, "claim rotates the lease owner")
	assert.Equal(t, state.Apply.Paused, claimed.State, "reconciliation claim refreshes the lease without changing apply state")
}

// TestApplyStore_FindNextApplyForStopReconciliation_SkipsApplyWithActiveOp
// verifies the trigger defers to the operation-claim path whenever any operation
// is active — whether freshly heartbeating or stale-and-crashed. That path drives
// the operation through the engine (which observes the stop), so reconciliation
// must not settle the apply terminally out from under it. Only once nothing is
// active does this path own the remaining pending siblings.
func TestApplyStore_FindNextApplyForStopReconciliation_SkipsApplyWithActiveOp(t *testing.T) {
	cases := []struct {
		name      string
		freshness string
	}{
		{name: "fresh heartbeat", freshness: "NOW()"},
		{name: "stale heartbeat", freshness: "NOW() - INTERVAL 1 HOUR"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearTables(t)
			ctx := t.Context()
			store := NewMySQL(testDB)

			lock := createTestLock(t, store, "testdb", "mysql")
			apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_stop_recon_active", 1, state.Apply.Running, "staging")

			runningID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
				ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.Running, OnFailure: storage.OnFailureContinue,
			})
			require.NoError(t, err)
			_, err = store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
				ApplyID: apply.ID, Deployment: "region-b", OnFailure: storage.OnFailureContinue,
			})
			require.NoError(t, err)
			_, err = testDB.ExecContext(ctx, `
				UPDATE apply_operations SET updated_at = `+tc.freshness+` WHERE id = ?
			`, runningID)
			require.NoError(t, err)

			_, _, err = store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
				ApplyID:     apply.ID,
				Operation:   storage.ControlOperationStop,
				Status:      storage.ControlRequestPending,
				RequestedBy: "operator",
			})
			require.NoError(t, err)

			claimed, err := store.Applies().FindNextApplyForStopReconciliation(ctx, "test-operator")
			require.NoError(t, err)
			assert.Nil(t, claimed, "an apply with any active operation is left to the operation-claim path")
		})
	}
}

// TestApplyStore_FindNextApplyForStopReconciliation_ClaimsPendingApplyWithStop
// verifies a stop requested before the first operation is ever claimed is not
// stranded: with the claim gate refusing the pending ops, reconciliation claims
// the still-pending apply (persistApplyClaim transitions it to running) so the
// operator can stop the pending operations and settle it.
func TestApplyStore_FindNextApplyForStopReconciliation_ClaimsPendingApplyWithStop(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_stop_recon_pending", 1, state.Apply.Pending, "staging")

	_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", OnFailure: storage.OnFailureContinue,
	})
	require.NoError(t, err)

	_, _, err = store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
	})
	require.NoError(t, err)

	claimed, err := store.Applies().FindNextApplyForStopReconciliation(ctx, "test-operator")
	require.NoError(t, err)
	require.NotNil(t, claimed, "a pending apply with a pending stop must be claimable so the stop is not stranded")
	assert.Equal(t, apply.ID, claimed.ID)
	assert.Equal(t, "test-operator", claimed.LeaseOwner)
}

// TestApplyStore_FindNextApplyForStopReconciliation_SkipsApplyWithoutPendingStop
// verifies the trigger is scoped to applies that actually have a pending stop:
// pending siblings alone (no stop) are normal rollout work, not reconciliation
// candidates.
func TestApplyStore_FindNextApplyForStopReconciliation_SkipsApplyWithoutPendingStop(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_stop_recon_nostop", 1, state.Apply.Running, "staging")

	_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", OnFailure: storage.OnFailureContinue,
	})
	require.NoError(t, err)

	claimed, err := store.Applies().FindNextApplyForStopReconciliation(ctx, "test-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "an apply without a pending stop is not a reconciliation candidate")
}

// stageOperationProjectionOrphan builds an apply whose operations are all in the
// given states while the apply itself stays in parentState, then ages the apply's
// heartbeat by staleBy so the projection claim's staleness gate can be exercised
// in either direction.
func stageOperationProjectionOrphan(t *testing.T, store *Storage, identifier, parentState string, staleBy time.Duration, opStates ...string) *storage.Apply {
	t.Helper()
	ctx := t.Context()

	lock := createTestLock(t, store, "testdb", "mysql")
	apply := createTestApplyWithStateAndEnv(t, store, lock, identifier, 1, parentState, "staging")
	for i, opState := range opStates {
		_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID:    apply.ID,
			Deployment: fmt.Sprintf("region-%d", i),
			State:      opState,
		})
		require.NoError(t, err)
	}
	_, err := testDB.ExecContext(ctx,
		`UPDATE applies SET updated_at = NOW() - INTERVAL ? SECOND WHERE id = ?`,
		int64(staleBy.Seconds()), apply.ID)
	require.NoError(t, err)
	return apply
}

// createStrandedStopApply seeds the minimal stop-reconciliation shape: a
// running apply with one pending operation and a pending stop control request,
// so FindNextApplyForStopReconciliation considers it a candidate.
func createStrandedStopApply(t *testing.T, store *Storage, lock *storage.Lock, applyID string, planID int64) *storage.Apply {
	t.Helper()
	ctx := t.Context()
	apply := createTestApplyWithStateAndEnv(t, store, lock, applyID, planID, state.Apply.Running, "staging")

	_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", OnFailure: storage.OnFailureContinue,
	})
	require.NoError(t, err)

	_, _, err = store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "operator",
	})
	require.NoError(t, err)
	return apply
}

// TestApplyStore_FindNextApplyForOperationProjection_ClaimsSettledOperationOrphan
// verifies the repair trigger for a parent left behind its own children: every
// operation has reached a terminal state, the apply itself is still running, and
// its heartbeat has gone stale, so no drive is coming back to project the
// outcome. The apply is claimable here so the operator can derive its state from
// the operations and release the target.
func TestApplyStore_FindNextApplyForOperationProjection_ClaimsSettledOperationOrphan(t *testing.T) {
	clearTables(t)
	store := NewMySQL(testDB)

	apply := stageOperationProjectionOrphan(t, store, "apply_op_projection", state.Apply.Running,
		2*storage.ApplyLeaseStaleAfter, state.ApplyOperation.Completed, state.ApplyOperation.Completed)

	claimed, err := store.Applies().FindNextApplyForOperationProjection(t.Context(), "test-operator")
	require.NoError(t, err)
	require.NotNil(t, claimed, "an apply whose operations have all settled must be claimable for projection")
	assert.Equal(t, apply.ID, claimed.ID)
	assert.Equal(t, "test-operator", claimed.LeaseOwner, "the claim rotates the lease owner")
	assert.Equal(t, state.Apply.Running, claimed.State, "the claim refreshes the lease without changing apply state")
}

// TestApplyStore_FindNextApplyForOperationProjection_SkipsLiveAndUnsettledApplies
// verifies the two gates that keep this repair off applies that are not
// stranded. A fresh heartbeat means a driver is still on the apply and may be
// mid-projection. Any non-terminal operation — pending queued work included —
// means an operation claim arm can still move the parent on its own.
func TestApplyStore_FindNextApplyForOperationProjection_SkipsLiveAndUnsettledApplies(t *testing.T) {
	cases := []struct {
		name     string
		staleBy  time.Duration
		opStates []string
		reason   string
	}{
		{
			name:     "fresh heartbeat",
			staleBy:  0,
			opStates: []string{state.ApplyOperation.Completed, state.ApplyOperation.Completed},
			reason:   "a driver still heartbeating the apply may be mid-projection",
		},
		{
			name:     "pending operation left",
			staleBy:  2 * storage.ApplyLeaseStaleAfter,
			opStates: []string{state.ApplyOperation.Completed, state.ApplyOperation.Pending},
			reason:   "a pending operation is queued work for the operation claim, not residue",
		},
		{
			name:     "running operation left",
			staleBy:  2 * storage.ApplyLeaseStaleAfter,
			opStates: []string{state.ApplyOperation.Completed, state.ApplyOperation.Running},
			reason:   "a running operation is recovered by the operation claim, which projects the parent itself",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearTables(t)
			store := NewMySQL(testDB)

			stageOperationProjectionOrphan(t, store, "apply_op_projection_skip", state.Apply.Running, tc.staleBy, tc.opStates...)

			claimed, err := store.Applies().FindNextApplyForOperationProjection(t.Context(), "test-operator")
			require.NoError(t, err)
			assert.Nil(t, claimed, tc.reason)
		})
	}
}

// TestApplyStore_FindNextApplyForOperationProjection_SkipsTerminalAndChildlessApplies
// verifies the repair is scoped to parents that are genuinely behind their
// children. A terminal apply has already recorded its verdict, and an apply with
// no operation rows has nothing to derive a state from.
func TestApplyStore_FindNextApplyForOperationProjection_SkipsTerminalAndChildlessApplies(t *testing.T) {
	t.Run("terminal apply", func(t *testing.T) {
		clearTables(t)
		store := NewMySQL(testDB)

		stageOperationProjectionOrphan(t, store, "apply_op_projection_terminal", state.Apply.Completed,
			2*storage.ApplyLeaseStaleAfter, state.ApplyOperation.Completed)

		claimed, err := store.Applies().FindNextApplyForOperationProjection(t.Context(), "test-operator")
		require.NoError(t, err)
		assert.Nil(t, claimed, "a terminal apply has already recorded its verdict")
	})

	t.Run("apply with no operations", func(t *testing.T) {
		clearTables(t)
		store := NewMySQL(testDB)

		stageOperationProjectionOrphan(t, store, "apply_op_projection_childless", state.Apply.Running,
			2*storage.ApplyLeaseStaleAfter)

		claimed, err := store.Applies().FindNextApplyForOperationProjection(t.Context(), "test-operator")
		require.NoError(t, err)
		assert.Nil(t, claimed, "an apply with no operation rows has nothing to project from")
	})
}

// TestApplyStore_FindNextApplyForOperationProjection_RequiresOwner verifies the
// claim fails closed without an owner: an unowned claim would rotate a lease
// nobody holds, leaving the apply writable by any driver.
func TestApplyStore_FindNextApplyForOperationProjection_RequiresOwner(t *testing.T) {
	clearTables(t)
	store := NewMySQL(testDB)

	_, err := store.Applies().FindNextApplyForOperationProjection(t.Context(), "")
	require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
}

// TestApplyStore_FindNextApplyForStopReconciliation_ReclaimGatedOnLeaseStaleness
// verifies the claim is once-per-request, not once-per-poll: the first claim
// (never claimed, no lease) succeeds and rotates the lease, and the same stop
// request does not re-match while that lease is fresh — the driver that claimed
// it owns the reconciliation, and re-claiming every poll would consume the tick
// that the rest of the claim ladder runs on. Once the claim's heartbeat goes
// stale (a crashed or wedged driver), the apply is claimable again so the stop
// is never stranded.
func TestApplyStore_FindNextApplyForStopReconciliation_ReclaimGatedOnLeaseStaleness(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
	apply := createStrandedStopApply(t, store, lock, "apply_stop_recon_lease", 1)

	claimed, err := store.Applies().FindNextApplyForStopReconciliation(ctx, "operator-a")
	require.NoError(t, err)
	require.NotNil(t, claimed, "a never-claimed apply with a pending stop must be claimable")
	assert.Equal(t, apply.ID, claimed.ID)
	assert.Equal(t, "operator-a", claimed.LeaseOwner)

	reclaimed, err := store.Applies().FindNextApplyForStopReconciliation(ctx, "operator-b")
	require.NoError(t, err)
	assert.Nil(t, reclaimed, "a freshly claimed stop request must not re-match while the lease heartbeat is fresh")

	// A crashed driver stops heartbeating; once the claim goes stale the apply
	// must be claimable again so the stop is not stranded.
	_, err = testDB.ExecContext(ctx, `
		UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?
	`, apply.ID)
	require.NoError(t, err)

	recovered, err := store.Applies().FindNextApplyForStopReconciliation(ctx, "operator-b")
	require.NoError(t, err)
	require.NotNil(t, recovered, "a stale claim must be recoverable by another driver")
	assert.Equal(t, apply.ID, recovered.ID)
	assert.Equal(t, "operator-b", recovered.LeaseOwner, "recovery rotates the lease to the new owner")
}

// TestApplyStore_FindNextApplyForStopReconciliation_ReclaimableWhenStopReissued
// verifies a fresh lease yields to a newer stop request: re-opening the stop
// control request stamps cr.updated_at past the lease (RequestPending's re-open
// path), which re-admits the apply immediately — the operator asked again, so
// reconciliation must not wait out the previous claim's staleness window.
func TestApplyStore_FindNextApplyForStopReconciliation_ReclaimableWhenStopReissued(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
	apply := createStrandedStopApply(t, store, lock, "apply_stop_recon_reissue", 1)

	claimed, err := store.Applies().FindNextApplyForStopReconciliation(ctx, "operator-a")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, apply.ID, claimed.ID)

	// Separate the request and lease timestamps so the ordering under test is
	// unambiguous at column precision: the stop request predates the lease, and
	// the lease's heartbeat stays fresh.
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_control_requests SET updated_at = NOW() - INTERVAL 10 SECOND WHERE apply_id = ?
	`, apply.ID)
	require.NoError(t, err)
	_, err = testDB.ExecContext(ctx, `
		UPDATE applies SET lease_acquired_at = NOW() - INTERVAL 5 SECOND, updated_at = updated_at WHERE id = ?
	`, apply.ID)
	require.NoError(t, err)

	reclaimed, err := store.Applies().FindNextApplyForStopReconciliation(ctx, "operator-b")
	require.NoError(t, err)
	assert.Nil(t, reclaimed, "a lease newer than the stop request must hold the claim")

	// Re-opening the stop request moves cr.updated_at past the lease.
	_, err = testDB.ExecContext(ctx, `
		UPDATE apply_control_requests SET updated_at = NOW() WHERE apply_id = ?
	`, apply.ID)
	require.NoError(t, err)

	reissued, err := store.Applies().FindNextApplyForStopReconciliation(ctx, "operator-b")
	require.NoError(t, err)
	require.NotNil(t, reissued, "a stop request newer than the lease must be claimable immediately")
	assert.Equal(t, apply.ID, reissued.ID)
	assert.Equal(t, "operator-b", reissued.LeaseOwner)
}

// TestApplyStore_FindNextApplyForStopReconciliation_FreshLeaseDoesNotBlockOtherApplies
// verifies claim ordering across applies: an already-claimed stop at the head
// of the queue (oldest created_at, fresh lease) does not shadow another apply's
// pending stop — the claim skips it and reaches the never-claimed apply behind
// it.
func TestApplyStore_FindNextApplyForStopReconciliation_FreshLeaseDoesNotBlockOtherApplies(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	olderLock := createTestLock(t, store, "testdb", "mysql")
	older := createStrandedStopApply(t, store, olderLock, "apply_stop_recon_older", 1)
	newerLock := createTestLock(t, store, "testdb2", "mysql")
	newer := createStrandedStopApply(t, store, newerLock, "apply_stop_recon_newer", 2)

	// Make the queue order deterministic at column precision.
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies SET created_at = NOW() - INTERVAL 1 MINUTE, updated_at = updated_at WHERE id = ?
	`, older.ID)
	require.NoError(t, err)

	first, err := store.Applies().FindNextApplyForStopReconciliation(ctx, "operator-a")
	require.NoError(t, err)
	require.NotNil(t, first)
	assert.Equal(t, older.ID, first.ID, "the oldest candidate is claimed first")

	second, err := store.Applies().FindNextApplyForStopReconciliation(ctx, "operator-a")
	require.NoError(t, err)
	require.NotNil(t, second, "a fresh lease at the head of the queue must not starve other applies' stops")
	assert.Equal(t, newer.ID, second.ID)
}

// SetRevertSkipped records skip-revert on the apply and the timestamp round-trips
// through Get, so progress can show that revert was skipped without an
// engine-specific side table. It is a targeted write that leaves other fields
// (here, state) untouched.
func TestApplyStore_SetRevertSkipped(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "vitess")
	apply := createTestApply(t, store, lock, "apply_revert_skipped", 1)

	got, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Nil(t, got.RevertSkippedAt, "revert_skipped_at starts unset")

	// Age updated_at so a heartbeat bump would be observable: updated_at is the
	// apply's lease heartbeat (the staleness gate in the claim predicate), and
	// SetRevertSkipped must not renew it from a non-lease caller.
	_, err = testDB.ExecContext(ctx, `UPDATE applies SET updated_at = NOW() - INTERVAL 5 MINUTE WHERE id = ?`, apply.ID)
	require.NoError(t, err)
	before, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)

	require.NoError(t, store.Applies().SetRevertSkipped(ctx, apply.ID, time.Now()))

	got, err = store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, got.RevertSkippedAt, "revert_skipped_at round-trips through Get")
	assert.Equal(t, apply.State, got.State, "SetRevertSkipped must not change other apply fields")
	assert.Equal(t, before.UpdatedAt, got.UpdatedAt, "SetRevertSkipped preserves the lease heartbeat (updated_at)")
}

// MarkSuperseded records the handoff without renewing the apply's lease
// heartbeat: the caller holds no lease on the apply it is marking, and bumping
// updated_at would delay another driver's recovery claim. It is a targeted
// write that leaves other fields (here, state) untouched.
func TestApplyStore_MarkSuperseded_PreservesHeartbeat(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
	apply := createTestApply(t, store, lock, "apply_superseded_heartbeat", 1)

	// Age updated_at so a heartbeat bump would be observable: updated_at is the
	// apply's lease heartbeat (the staleness gate in the claim predicate), and
	// MarkSuperseded must not renew it from a non-lease caller.
	_, err := testDB.ExecContext(ctx, `UPDATE applies SET updated_at = NOW() - INTERVAL 5 MINUTE WHERE id = ?`, apply.ID)
	require.NoError(t, err)
	before, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, before)

	require.NoError(t, store.Applies().MarkSuperseded(ctx, apply.ID, "apply_superseded_heartbeat_successor"))

	got, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "apply_superseded_heartbeat_successor", got.SupersededBy)
	assert.Equal(t, apply.State, got.State, "MarkSuperseded must not change other apply fields")
	assert.Equal(t, before.UpdatedAt, got.UpdatedAt, "MarkSuperseded preserves the lease heartbeat (updated_at)")
}

// A lease with no token authorizes no write, so a release attempted with one is
// refused rather than silently clearing whatever lease the row currently holds.
func TestApplyStore_ReleaseClaim_InvalidLeaseIsRefused(t *testing.T) {
	clearTables(t)
	store := NewMySQL(testDB)

	released, err := store.Applies().ReleaseClaim(t.Context(), storage.ApplyLease{ApplyID: 1})

	require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
	assert.False(t, released)
}
