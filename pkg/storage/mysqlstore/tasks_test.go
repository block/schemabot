//go:build integration

package mysqlstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestTaskStore_GetByApplyOperationID verifies that tasks can be loaded for a
// single apply_operation (one deployment) independently of its sibling
// deployments under the same apply. This is the read primitive an operator
// worker uses to drive only the deployment it has claimed.
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
