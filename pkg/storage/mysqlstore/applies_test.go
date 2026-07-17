//go:build integration

package mysqlstore

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
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

func TestApplyStore_CreateRejectsChangedLockIntent(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

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
	store := New(testDB)

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
	store := New(testDB)

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
	store := New(testDB)

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
	// operator dispatch; drivers never see a partially populated task list.
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

// Grouped operation creation links each deployment's independent task copies to that deployment's operation.
func TestApplyStore_CreateWithGroupedOperationsLinksTasksPerOperation(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)
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
	store := New(testDB)
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
			store := New(testDB)
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
	store := New(testDB)
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
	store := New(testDB)
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
	store := New(testDB)
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

// TestApplyStore_IdempotencyKey verifies the dedupe anchor for remote apply
// dispatch: applies with no key store NULL and never collide, a non-empty key is
// unique, and GetByIdempotencyKey resolves a stamped apply while treating the
// empty key as no lookup.
func TestApplyStore_IdempotencyKey(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")

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

func TestApplyStore_GetRecentDeploymentFilterMatchesParentAndOperation(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "recentdeploydb", "mysql", "staging")
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

// TestApplyStore_UpdateDerivedState verifies the rollout-projection compare-and-
// swap: the write lands only when the row still holds the expected state, so a
// stale projection cannot clobber a newer state another drive already wrote.
func TestApplyStore_UpdateDerivedState(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
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

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
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
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
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

	claimed, err := store.Applies().FindNextApply(ctx, "driver-a")
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
	store := New(testDB)

	opLeaseCtx := func(applyID, opID int64, token string) context.Context {
		return storage.WithOperationLease(ctx, storage.OperationLease{
			ApplyID: applyID, OperationID: opID, Owner: "driver", Token: token,
		})
	}

	// Each running apply needs its own target so the active-apply uniqueness
	// check does not reject the second one.
	runningApply := func(identifier, env string, planID int64) *storage.Apply {
		lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, env)
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
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
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
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
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
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
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
	assert.Nil(t, again, "a freshly claimed apply is owned by its current driver")
}

// ClaimApplyByID must not steal a fresh lease from a healthy apply-level driver,
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

// A PlanetScale apply transitions through engine setup-phase states
// (preparing_branch through validating_deploy_request) before per-table work
// begins. If the driver driving setup crashes, the apply is left in one of
// those states; a fresh driver must reclaim it once the heartbeat goes stale so
// setup resumes from persisted branch/deploy metadata. While the original
// driver is still alive its heartbeat stays fresh, so the apply is not claimed
// out from under it.
func TestApplyStore_FindNextApplyClaimsStaleSetupPhase(t *testing.T) {
	for _, setupState := range []string{
		state.Apply.PreparingBranch,
		state.Apply.ApplyingBranchChanges,
		state.Apply.ValidatingBranch,
		state.Apply.CreatingDeployRequest,
		state.Apply.ValidatingDeployRequest,
	} {
		t.Run(setupState, func(t *testing.T) {
			clearTables(t)
			ctx := t.Context()
			store := New(testDB)

			lock := createTestLock(t, store, "testdb", storage.DatabaseTypeVitess, "staging")
			apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_setup_"+setupState, 700, setupState, "staging")
			require.Equal(t, storage.EnginePlanetScale, apply.Engine, "setup-phase states only occur for the PlanetScale engine")

			fresh, err := store.Applies().FindNextApply(ctx, "operator-a")
			require.NoError(t, err)
			assert.Nil(t, fresh, "a setup-phase apply with a fresh heartbeat is owned by its active driver")

			_, err = testDB.ExecContext(ctx, `
				UPDATE applies
				SET updated_at = NOW() - INTERVAL 2 MINUTE
				WHERE id = ?
			`, apply.ID)
			require.NoError(t, err)

			claimed, err := store.Applies().FindNextApply(ctx, "operator-a")
			require.NoError(t, err)
			require.NotNil(t, claimed, "a stale setup-phase apply must be reclaimable for recovery")
			assert.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)
			assert.Equal(t, setupState, claimed.State, "the caller sees the setup-phase state to resume from")
			assert.Equal(t, storage.EnginePlanetScale, claimed.Engine, "the reclaimed apply keeps its PlanetScale engine")
			assert.Equal(t, "operator-a", claimed.LeaseOwner)
			assert.NotEmpty(t, claimed.LeaseToken)
		})
	}
}

// ClaimApplyByID is how the operation-level claim loop acquires the parent apply
// lease after leasing a stale operation row. When a PlanetScale driver crashes
// mid-setup the operation row stays running (stale) while the parent apply sits
// in a setup-phase state, so ClaimApplyByID must reclaim that stale parent for
// recovery while still refusing one whose heartbeat is fresh.
func TestApplyStore_ClaimApplyByIDClaimsStaleSetupPhase(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeVitess, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_claim_setup", 701, state.Apply.ApplyingBranchChanges, "staging")
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
	assert.Equal(t, state.Apply.ApplyingBranchChanges, claimed.State)
	assert.Equal(t, storage.EnginePlanetScale, claimed.Engine, "the reclaimed parent apply keeps its PlanetScale engine")
	assert.Equal(t, "operator-a", claimed.LeaseOwner)
	assert.NotEmpty(t, claimed.LeaseToken)
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

	claimed, err := store.Applies().FindNextApply(ctx, "driver-a")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, "driver-a", claimed.LeaseOwner)
	require.NotEmpty(t, claimed.LeaseToken)
	require.NotNil(t, claimed.LeaseAcquiredAt)

	staleCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: apply.ID, Owner: "driver-old", Token: "stale-token"})
	claimed.State = state.Apply.Failed
	claimed.ErrorMessage = "stale driver failure"
	require.ErrorIs(t, store.Applies().Update(staleCtx, claimed), storage.ErrApplyLeaseLost)
	require.ErrorIs(t, store.Applies().Heartbeat(staleCtx, apply.ID), storage.ErrApplyLeaseLost)

	task.State = state.Task.Completed
	require.ErrorIs(t, store.Tasks().Update(staleCtx, task), storage.ErrApplyLeaseLost)
	require.ErrorIs(t, store.ApplyLogs().Append(staleCtx, &storage.ApplyLog{
		ApplyID:   apply.ID,
		Level:     storage.LogLevelInfo,
		EventType: storage.LogEventStateTransition,
		Source:    storage.LogSourceSchemaBot,
		Message:   "stale driver log",
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
		Message:   "owned driver log",
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

// A VSchema-only apply carries an apply_operations row but no tasks — the
// VSchema application is the whole apply. Its dual-written operation row proves
// the create committed fully, so the pending claim must accept it; gating the
// claim on tasks alone would strand every queued VSchema-only apply pending
// forever.
func TestApplyStore_FindNextApplyClaimsTasklessPendingApplyWithOperation(t *testing.T) {
	newTasklessApply := func(t *testing.T, store *Storage, database, applyID string) *storage.Apply {
		t.Helper()
		lock := createTestLock(t, store, database, storage.DatabaseTypeVitess, "staging")
		apply := createTestApplyWithStateAndEnv(t, store, lock, applyID, 503, state.Apply.Pending, "staging")
		now := time.Now()
		_, err := store.ApplyOperations().Insert(t.Context(), &storage.ApplyOperation{
			ApplyID:    apply.ID,
			Deployment: apply.Database,
			State:      state.ApplyOperation.Pending,
			CreatedAt:  now,
			UpdatedAt:  now,
		})
		require.NoError(t, err)
		return apply
	}

	t.Run("FindNextApply claims it", func(t *testing.T) {
		clearTables(t)
		store := New(testDB)
		apply := newTasklessApply(t, store, "testdb", "apply_vschema_only")

		claimed, err := store.Applies().FindNextApply(t.Context(), "test-owner")
		require.NoError(t, err)
		require.NotNil(t, claimed, "a task-less pending apply with an operation row must be claimable")
		assert.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)
	})

	t.Run("ClaimApplyByID acquires the parent lease", func(t *testing.T) {
		clearTables(t)
		store := New(testDB)
		apply := newTasklessApply(t, store, "testdb", "apply_vschema_only_byid")

		claimed, err := store.Applies().ClaimApplyByID(t.Context(), apply.ID, "test-owner")
		require.NoError(t, err)
		require.NotNil(t, claimed, "the operation-claim path must acquire the parent lease for a task-less pending apply")
		assert.Equal(t, apply.ApplyIdentifier, claimed.ApplyIdentifier)
	})
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
	assert.Nil(t, claimedAgain, "claim heartbeat should prevent another driver from immediately taking the same start request")
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
	assert.Equal(t, state.Apply.Resuming, persisted.State, "claiming a stopped apply with a pending start moves it into resuming until the data plane leaves stopped")

	claimedAgain, err := store.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	assert.Nil(t, claimedAgain, "claim transition should prevent another driver from taking the same stopped start request")
}

func TestApplyStore_FindNextApplyClaimsWaitingForDeployStartControlRequest(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeVitess, "staging")
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

	claimed, err := store.Applies().FindNextApply(ctx, "test-owner")
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

	claimedAgain, err := store.Applies().FindNextApply(ctx, "test-owner")
	require.NoError(t, err)
	assert.Nil(t, claimedAgain, "fresh processing lease should prevent another driver from taking the same deferred deploy start request")

	_, err = testDB.ExecContext(ctx, `
		UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?
	`, apply.ID)
	require.NoError(t, err)

	reclaimed, err := store.Applies().FindNextApply(ctx, "recovery-owner")
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
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
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
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
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
	assert.Nil(t, freshClaim, "fresh waiting-for-cutover applies are owned by their active driver")

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
	assert.Nil(t, claimedAgain, "claim heartbeat should prevent another driver from immediately taking the same stale cutover request")
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
	assert.Nil(t, claimed, "fresh running applies are owned by their active driver; pending stop must not create a second owner")
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

	const drivers = 16
	stores := make([]*Storage, drivers)
	for i := range drivers {
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

	for i := range drivers {
		driverStore := stores[i]
		wg.Go(func() {
			<-start
			got, claimErr := driverStore.Applies().FindNextApply(ctx, "test-owner")

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
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_retryable_ops_expired", 510, state.Apply.FailedRetryable, "staging")
	apply.Attempt = maxRecoveryAttempts
	require.NoError(t, store.Applies().Update(ctx, apply))

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

// TestApplyStore_ReapplyFailed verifies that deliberate reapply reopens a
// recent failed apply while preserving completed work as already done.
func TestApplyStore_ReapplyFailed(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_reapply_failed", 520, state.Apply.Failed, "staging")
	now := time.Now()
	apply.Attempt = maxRecoveryAttempts
	apply.Deployment = "region-a"
	apply.ErrorMessage = "retry budget exhausted"
	apply.CompletedAt = &now
	require.NoError(t, store.Applies().Update(ctx, apply))
	_, err := testDB.ExecContext(ctx, `UPDATE applies SET deployment = ? WHERE id = ?`, apply.Deployment, apply.ID)
	require.NoError(t, err)

	_, err = store.Tasks().Create(ctx, &storage.Task{
		TaskIdentifier: "task_reapply_failed",
		ApplyID:        apply.ID,
		PlanID:         apply.PlanID,
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Engine:         storage.EngineSpirit,
		Environment:    apply.Environment,
		State:          state.Task.Failed,
		ErrorMessage:   "copy failed",
		TableName:      "orders",
		Options:        []byte("{}"),
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	require.NoError(t, err)
	_, err = store.Tasks().Create(ctx, &storage.Task{
		TaskIdentifier: "task_reapply_completed",
		ApplyID:        apply.ID,
		PlanID:         apply.PlanID,
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Engine:         storage.EngineSpirit,
		Environment:    apply.Environment,
		State:          state.Task.Completed,
		TableName:      "customers",
		Options:        []byte("{}"),
		CreatedAt:      now,
		UpdatedAt:      now,
		CompletedAt:    &now,
	})
	require.NoError(t, err)

	failedOpID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", State: state.ApplyOperation.Failed, ErrorMessage: "copy failed",
	})
	require.NoError(t, err)
	completedOpID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b", State: state.ApplyOperation.Completed, CompletedAt: &now,
	})
	require.NoError(t, err)

	reapplied, err := store.Applies().ReapplyFailed(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, reapplied)
	assert.Equal(t, state.Apply.FailedRetryable, reapplied.State)
	assert.Zero(t, reapplied.Attempt)
	assert.Empty(t, reapplied.ErrorMessage)
	assert.Nil(t, reapplied.CompletedAt)

	failedTask, err := store.Tasks().Get(ctx, "task_reapply_failed")
	require.NoError(t, err)
	require.NotNil(t, failedTask)
	assert.Equal(t, state.Task.FailedRetryable, failedTask.State)
	assert.Empty(t, failedTask.ErrorMessage)
	assert.Nil(t, failedTask.CompletedAt)

	completedTask, err := store.Tasks().Get(ctx, "task_reapply_completed")
	require.NoError(t, err)
	require.NotNil(t, completedTask)
	assert.Equal(t, state.Task.Completed, completedTask.State)
	assert.NotNil(t, completedTask.CompletedAt)

	failedOp, err := store.ApplyOperations().Get(ctx, failedOpID)
	require.NoError(t, err)
	require.NotNil(t, failedOp)
	assert.Equal(t, state.ApplyOperation.FailedRetryable, failedOp.State)
	assert.Empty(t, failedOp.ErrorMessage)
	assert.Nil(t, failedOp.CompletedAt)

	completedOp, err := store.ApplyOperations().Get(ctx, completedOpID)
	require.NoError(t, err)
	require.NotNil(t, completedOp)
	assert.Equal(t, state.ApplyOperation.Completed, completedOp.State)
	assert.NotNil(t, completedOp.CompletedAt)
}

func TestApplyStore_ReapplyFailedRejectsOldFailure(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_reapply_old_failed", 521, state.Apply.Failed, "staging")
	completedAt := time.Now().AddDate(0, 0, -storage.ReapplyFailureFreshnessDays-1)
	apply.Deployment = "region-a"
	apply.CompletedAt = &completedAt
	require.NoError(t, store.Applies().Update(ctx, apply))
	_, err := testDB.ExecContext(ctx, `UPDATE applies SET deployment = ?, completed_at = ? WHERE id = ?`, apply.Deployment, completedAt, apply.ID)
	require.NoError(t, err)

	reapplied, err := store.Applies().ReapplyFailed(ctx, apply.ID)
	require.ErrorIs(t, err, storage.ErrApplyNotReappliable)
	assert.Nil(t, reapplied)
}

func TestApplyStore_ReapplyFailedRejectsNonReappliableOperation(t *testing.T) {
	for _, tc := range []struct {
		name           string
		operationState string
		taskState      string
	}{
		{name: "stopped", operationState: state.ApplyOperation.Stopped, taskState: state.Task.Stopped},
		{name: "cancelled", operationState: state.ApplyOperation.Cancelled, taskState: state.Task.Cancelled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearTables(t)
			ctx := t.Context()
			store := New(testDB)

			lock := createTestLock(t, store, "testdb", storage.DatabaseTypeMySQL, "staging")
			apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_reapply_"+tc.name+"_operation", 522, state.Apply.Failed, "staging")
			now := time.Now()
			apply.Deployment = "region-a"
			apply.CompletedAt = &now
			require.NoError(t, store.Applies().Update(ctx, apply))
			_, err := testDB.ExecContext(ctx, `UPDATE applies SET deployment = ? WHERE id = ?`, apply.Deployment, apply.ID)
			require.NoError(t, err)

			operationID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
				ApplyID: apply.ID, Deployment: "region-a", State: tc.operationState, CompletedAt: &now,
			})
			require.NoError(t, err)
			_, err = store.Tasks().Create(ctx, &storage.Task{
				TaskIdentifier:   "task_reapply_" + tc.name + "_operation",
				ApplyID:          apply.ID,
				ApplyOperationID: &operationID,
				PlanID:           apply.PlanID,
				Database:         apply.Database,
				DatabaseType:     apply.DatabaseType,
				Engine:           storage.EngineSpirit,
				Environment:      apply.Environment,
				State:            tc.taskState,
				TableName:        "orders",
				Options:          []byte("{}"),
				CreatedAt:        now,
				UpdatedAt:        now,
				CompletedAt:      &now,
			})
			require.NoError(t, err)

			reapplied, err := store.Applies().ReapplyFailed(ctx, apply.ID)
			require.ErrorIs(t, err, storage.ErrApplyNotReappliable)
			assert.Nil(t, reapplied)

			task, err := store.Tasks().Get(ctx, "task_reapply_"+tc.name+"_operation")
			require.NoError(t, err)
			require.NotNil(t, task)
			assert.Equal(t, tc.taskState, task.State)
		})
	}
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
	return createTestApplyWithEnv(t, store, lock, applyID, planID, "staging")
}

func createTestApplyWithEnv(t *testing.T, store *Storage, lock *storage.Lock, applyID string, planID int64, env string) *storage.Apply {
	t.Helper()
	return createTestApplyWithStateAndEnv(t, store, lock, applyID, planID, state.Apply.Pending, env)
}

func createTestApplyWithStateAndEnv(t *testing.T, store *Storage, lock *storage.Lock, applyID string, planID int64, applyState, env string) *storage.Apply {
	t.Helper()
	return createTestApplyWithStateEnvDeployment(t, store, lock, applyID, planID, applyState, env, "")
}

func createTestApplyWithStateEnvDeployment(t *testing.T, store *Storage, lock *storage.Lock, applyID string, planID int64, applyState, env, deployment string) *storage.Apply {
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
		Deployment:      deployment,
		Engine:          storage.EngineForType(lock.DatabaseType),
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

// TestApplyStore_FindNextApplyForStopReconciliation_ClaimsStrandedContinueApply
// verifies the stop-reconciliation trigger: an apply held running under
// on_failure "continue" (a failed earlier sibling) with a pending stop and a
// pending sibling that the claim gate keeps from starting is claimed here, so
// the operator can stop the pending siblings and let the apply settle. Without
// this path no operation is claimable and the stop would strand forever.
func TestApplyStore_FindNextApplyForStopReconciliation_ClaimsStrandedContinueApply(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
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
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
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
			store := New(testDB)

			lock := createTestLock(t, store, "testdb", "mysql", "staging")
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
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
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
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_stop_recon_nostop", 1, state.Apply.Running, "staging")

	_, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", OnFailure: storage.OnFailureContinue,
	})
	require.NoError(t, err)

	claimed, err := store.Applies().FindNextApplyForStopReconciliation(ctx, "test-operator")
	require.NoError(t, err)
	assert.Nil(t, claimed, "an apply without a pending stop is not a reconciliation candidate")
}

// SetRevertSkipped records skip-revert on the apply and the timestamp round-trips
// through Get, so progress can show that revert was skipped without an
// engine-specific side table. It is a targeted write that leaves other fields
// (here, state) untouched.
func TestApplyStore_SetRevertSkipped(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "vitess", "staging")
	apply := createTestApply(t, store, lock, "apply_revert_skipped", 1)

	got, err := store.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Nil(t, got.RevertSkippedAt, "revert_skipped_at starts unset")

	// Age updated_at so a heartbeat bump would be observable: updated_at is the
	// apply's lease heartbeat (the staleness gate in FindNextApply), and
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
