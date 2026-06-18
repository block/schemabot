//go:build integration

package mysqlstore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// An operation lease scopes a task update to the task's own operation and is
// enforced on the operation's token, taking precedence over a current parent
// apply lease. A stale operation token must fail closed and leave the task row
// untouched.
func TestTaskStore_OperationLeaseGuardsUpdate(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_task_oplease", 1)

	// Parent apply holds a current lease throughout; a successful update proves
	// the operation token is enforced, not the apply token.
	_, err := testDB.ExecContext(ctx, `
		UPDATE applies
		SET lease_owner = ?, lease_token = ?, lease_acquired_at = NOW()
		WHERE id = ?
	`, "current-driver", "apply-token", apply.ID)
	require.NoError(t, err)

	opID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", Target: "payments",
	})
	require.NoError(t, err)
	stampOperationLease(t, opID, "driver", "op-token")

	now := time.Now()
	taskID, err := store.Tasks().Create(ctx, &storage.Task{
		TaskIdentifier:   "task_oplease_users",
		ApplyID:          apply.ID,
		ApplyOperationID: &opID,
		PlanID:           apply.PlanID,
		Database:         apply.Database,
		DatabaseType:     apply.DatabaseType,
		Engine:           storage.EngineSpirit,
		Environment:      apply.Environment,
		State:            state.Task.Pending,
		TableName:        "users",
		DDL:              "ALTER TABLE `users` ADD COLUMN email VARCHAR(255)",
		DDLAction:        "ALTER",
		Options:          []byte("{}"),
		CreatedAt:        now,
		UpdatedAt:        now,
	})
	require.NoError(t, err)

	task, err := store.Tasks().Get(ctx, "task_oplease_users")
	require.NoError(t, err)
	require.NotNil(t, task)
	task.ID = taskID

	opCtx := func(token string) context.Context {
		return storage.WithOperationLease(ctx, storage.OperationLease{
			ApplyID: apply.ID, OperationID: opID, Owner: "driver", Token: token,
		})
	}

	task.State = state.Task.Completed
	require.ErrorIs(t, store.Tasks().Update(opCtx("stale-op-token"), task), storage.ErrApplyLeaseLost)
	reloaded, err := store.Tasks().Get(ctx, "task_oplease_users")
	require.NoError(t, err)
	assert.Equal(t, state.Task.Pending, reloaded.State)

	require.NoError(t, store.Tasks().Update(opCtx("op-token"), task))
	reloaded, err = store.Tasks().Get(ctx, "task_oplease_users")
	require.NoError(t, err)
	assert.Equal(t, state.Task.Completed, reloaded.State)

	// Operation lease takes precedence: a stale operation token fails closed even
	// when a current apply lease is also on the context.
	task.State = state.Task.Failed
	bothCtx := storage.WithApplyLease(opCtx("stale-op-token"), storage.ApplyLease{
		ApplyID: apply.ID, Owner: "current-driver", Token: "apply-token",
	})
	require.ErrorIs(t, store.Tasks().Update(bothCtx, task), storage.ErrApplyLeaseLost)
	reloaded, err = store.Tasks().Get(ctx, "task_oplease_users")
	require.NoError(t, err)
	assert.Equal(t, state.Task.Completed, reloaded.State)
}

// TestTaskStore_GetByApplyOperationID verifies that tasks can be loaded for a
// single apply_operation (one deployment) independently of its sibling
// deployments under the same apply. This is the read primitive an operator
// driver uses to drive only the deployment it has claimed.
func TestTaskStore_GetByApplyOperationID(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
	apply := createTestApply(t, store, lock, "apply_tasks_by_op", 1)

	opA, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", Target: "payments",
	})
	require.NoError(t, err)
	opB, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-b", Target: "payments",
	})
	require.NoError(t, err)
	opEmpty, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-c", Target: "payments",
	})
	require.NoError(t, err)

	createTask := func(identifier, table string, operationID int64) {
		now := time.Now()
		_, err := store.Tasks().Create(ctx, &storage.Task{
			TaskIdentifier:   identifier,
			ApplyID:          apply.ID,
			ApplyOperationID: &operationID,
			PlanID:           apply.PlanID,
			Database:         apply.Database,
			DatabaseType:     apply.DatabaseType,
			Engine:           storage.EngineSpirit,
			Environment:      apply.Environment,
			State:            state.Task.Pending,
			TableName:        table,
			DDL:              "ALTER TABLE " + table + " ADD COLUMN email VARCHAR(255)",
			DDLAction:        "ALTER",
			CreatedAt:        now,
			UpdatedAt:        now,
		})
		require.NoError(t, err)
	}

	createTask("task_a_users", "users", opA)
	createTask("task_a_orders", "orders", opA)
	createTask("task_b_users", "users", opB)

	// region-a's operation returns only its two tasks, never region-b's.
	// created_at is second-precision, so both tasks usually share a timestamp;
	// the id DESC tiebreaker makes the order deterministic (newest id first).
	tasksA, err := store.Tasks().GetByApplyOperationID(ctx, opA)
	require.NoError(t, err)
	require.Len(t, tasksA, 2)
	assert.Equal(t, "task_a_orders", tasksA[0].TaskIdentifier)
	assert.Equal(t, "task_a_users", tasksA[1].TaskIdentifier)
	for _, task := range tasksA {
		require.NotNil(t, task.ApplyOperationID)
		assert.Equal(t, opA, *task.ApplyOperationID)
	}

	// region-b's operation returns only its single task.
	tasksB, err := store.Tasks().GetByApplyOperationID(ctx, opB)
	require.NoError(t, err)
	require.Len(t, tasksB, 1)
	assert.Equal(t, "task_b_users", tasksB[0].TaskIdentifier)
	require.NotNil(t, tasksB[0].ApplyOperationID)
	assert.Equal(t, opB, *tasksB[0].ApplyOperationID)

	// A real operation row that owns zero tasks returns a non-nil empty slice —
	// never nil and never a fallback to the parent apply's tasks.
	tasksEmpty, err := store.Tasks().GetByApplyOperationID(ctx, opEmpty)
	require.NoError(t, err)
	require.NotNil(t, tasksEmpty)
	assert.Empty(t, tasksEmpty)
}
