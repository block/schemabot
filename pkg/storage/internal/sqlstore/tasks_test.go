//go:build integration

package sqlstore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/namedlock"
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
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
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

// CountByApplyID reports every task row an apply owns — unsharded drive rows
// and shard-tagged rows alike, with no operation-key filtering — and never
// another apply's rows. It is the predicate a drive uses to distinguish a
// genuinely task-less apply (safe to complete as a no-op) from one whose rows
// a filtered loader did not return (must fail closed).
func TestTaskStore_CountByApplyID(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
	applyWithTasks := createTestApply(t, store, lock, "apply_count_tasks", 1)
	tasklessLock := createTestLock(t, store, "otherdb", "mysql")
	applyTaskless := createTestApply(t, store, tasklessLock, "apply_count_taskless", 2)

	opID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: applyWithTasks.ID, Deployment: "region-a", Target: "payments",
	})
	require.NoError(t, err)

	createTask := func(identifier, table, shard string) {
		now := time.Now()
		_, err := store.Tasks().Create(ctx, &storage.Task{
			TaskIdentifier:   identifier,
			ApplyID:          applyWithTasks.ID,
			ApplyOperationID: &opID,
			PlanID:           applyWithTasks.PlanID,
			Database:         applyWithTasks.Database,
			DatabaseType:     applyWithTasks.DatabaseType,
			Engine:           storage.EngineSpirit,
			Environment:      applyWithTasks.Environment,
			State:            state.Task.Pending,
			TableName:        table,
			Shard:            shard,
			DDL:              "ALTER TABLE `" + table + "` ADD COLUMN `email` varchar(255)",
			DDLAction:        "ALTER",
			CreatedAt:        now,
			UpdatedAt:        now,
		})
		require.NoError(t, err)
	}

	createTask("task_count_users", "users", "")
	createTask("task_count_users_-80", "users", "-80")
	createTask("task_count_users_80-", "users", "80-")

	count, err := store.Tasks().CountByApplyID(ctx, applyWithTasks.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(3), count, "every row counts, shard-tagged or not")

	count, err = store.Tasks().CountByApplyID(ctx, applyTaskless.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(0), count, "an apply with no rows counts zero, never a sibling apply's rows")
}

// TestTaskStore_GetByApplyOperationID verifies that tasks can be loaded for a
// single apply_operation (one deployment) independently of its sibling
// deployments under the same apply. This is the read primitive an operator
// driver uses to drive only the deployment it has claimed.
func TestTaskStore_GetByApplyOperationID(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
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

	// region-a's operation returns only its two tasks, never region-b's, in
	// creation order — the plan's statement order, which the sequential drive
	// executes as-is. created_at is second-precision, so both tasks usually
	// share a timestamp; the id tiebreaker keeps the order deterministic.
	tasksA, err := store.Tasks().GetByApplyOperationID(ctx, opA)
	require.NoError(t, err)
	require.Len(t, tasksA, 2)
	assert.Equal(t, "task_a_users", tasksA[0].TaskIdentifier)
	assert.Equal(t, "task_a_orders", tasksA[1].TaskIdentifier)
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

// A task is the per-(table, shard) execution record for a sharded engine: the
// shard and its cutover attempts round-trip through create/get, two shards of
// one table coexist under a single operation, cutover_attempts is updatable, and
// the shard is fixed at creation (Update never rewrites it).
func TestTaskStore_PerShardTaskRoundTrip(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "resolute", "vitess")
	apply := createTestApply(t, store, lock, "apply_shard_tasks", 1)
	opID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", Target: "resolute",
	})
	require.NoError(t, err)

	now := time.Now()
	createShardTask := func(identifier, shard string, cutoverAttempts int) {
		_, err := store.Tasks().Create(ctx, &storage.Task{
			TaskIdentifier:   identifier,
			ApplyID:          apply.ID,
			ApplyOperationID: &opID,
			PlanID:           apply.PlanID,
			Database:         apply.Database,
			DatabaseType:     apply.DatabaseType,
			Engine:           storage.EngineStrata,
			Environment:      apply.Environment,
			State:            state.Task.Running,
			Namespace:        "resolute",
			TableName:        "users",
			Shard:            shard,
			CutoverAttempts:  cutoverAttempts,
			DDL:              "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			DDLAction:        "ALTER",
			CreatedAt:        now,
			UpdatedAt:        now,
		})
		require.NoError(t, err)
	}

	createShardTask("task_users_-80", "-80", 1)
	createShardTask("task_users_80-", "80-", 0)

	// Round-trip: shard and cutover_attempts persist on the per-shard row.
	got, err := store.Tasks().Get(ctx, "task_users_-80")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "resolute", got.Namespace)
	assert.Equal(t, "users", got.TableName)
	assert.Equal(t, "-80", got.Shard)
	assert.Equal(t, 1, got.CutoverAttempts)

	// Both shards of the same table coexist under one operation. With no sharded
	// operation key, GetByApplyOperationID treats them as reflected progress rows
	// and keeps them out of the drive pipeline on reload.
	tasks, err := store.Tasks().GetShardProgressByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	perTable, err := store.Tasks().GetByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	assert.Empty(t, perTable, "per-shard rows must not leak into the per-table loader")
	shardAttempts := map[string]int{}
	for _, task := range tasks {
		assert.Equal(t, "users", task.TableName)
		shardAttempts[task.Shard] = task.CutoverAttempts
	}
	assert.Equal(t, map[string]int{"-80": 1, "80-": 0}, shardAttempts)

	// cutover_attempts is updatable (incremented across cutover retries); the
	// shard is set at creation and Update must not rewrite it.
	got.CutoverAttempts = 2
	got.Shard = "ignored-on-update"
	require.NoError(t, store.Tasks().Update(ctx, got))
	reloaded, err := store.Tasks().Get(ctx, "task_users_-80")
	require.NoError(t, err)
	assert.Equal(t, 2, reloaded.CutoverAttempts, "cutover_attempts is updatable")
	assert.Equal(t, "-80", reloaded.Shard, "shard is fixed at creation, not changed by Update")
}

// A table paused by the engine's throttler stores the pause flag and its
// display reason on the task row, so readers (PR comment, CLI) render the
// pause from storage without polling the engine. Both fields are written by
// the drive's progress sync every tick: they set while the pause is active and
// clear on the first unpaced tick.
func TestTaskStore_ThrottleRoundTrip(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testapp", "mysql")
	apply := createTestApply(t, store, lock, "apply_throttle", 1)

	now := time.Now()
	_, err := store.Tasks().Create(ctx, &storage.Task{
		TaskIdentifier: "task_throttle",
		ApplyID:        apply.ID,
		PlanID:         apply.PlanID,
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Engine:         storage.EngineSpirit,
		Environment:    apply.Environment,
		State:          state.Task.Running,
		Namespace:      "testapp",
		TableName:      "users",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		DDLAction:      "ALTER",
		Throttled:      true,
		ThrottleReason: "replica-lag 12s > 10s",
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	require.NoError(t, err)

	got, err := store.Tasks().Get(ctx, "task_throttle")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.True(t, got.Throttled)
	assert.Equal(t, "replica-lag 12s > 10s", got.ThrottleReason)

	got.Throttled = false
	got.ThrottleReason = ""
	require.NoError(t, store.Tasks().Update(ctx, got))
	cleared, err := store.Tasks().Get(ctx, "task_throttle")
	require.NoError(t, err)
	assert.False(t, cleared.Throttled, "the pause clears when the throttler releases")
	assert.Empty(t, cleared.ThrottleReason)
}

// A sharded work operation's operation key identifies which shard task is real
// drive input. Other shard rows remain progress detail and must not be replayed
// as extra table changes if the operation is resumed.
func TestTaskStore_GetByApplyOperationIDIncludesMatchingShardedWorkTask(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "resolute", storage.DatabaseTypeStrata)
	apply := createTestApply(t, store, lock, "apply_sharded_work_tasks", 1)
	opID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:       apply.ID,
		Deployment:    "region-a",
		OperationKey:  "commerce/-80/users",
		OperationKind: storage.ApplyOperationKindWork,
		Target:        "resolute",
	})
	require.NoError(t, err)

	now := time.Now()
	createShardTask := func(identifier, shard string) {
		_, err := store.Tasks().Create(ctx, &storage.Task{
			TaskIdentifier:   identifier,
			ApplyID:          apply.ID,
			ApplyOperationID: &opID,
			PlanID:           apply.PlanID,
			Database:         apply.Database,
			DatabaseType:     apply.DatabaseType,
			Engine:           storage.EngineStrata,
			Environment:      apply.Environment,
			State:            state.Task.Pending,
			Namespace:        "commerce",
			TableName:        "users",
			Shard:            shard,
			DDL:              "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			DDLAction:        "ALTER",
			CreatedAt:        now,
			UpdatedAt:        now,
		})
		require.NoError(t, err)
	}
	createShardTask("task_users_-80", "-80")
	createShardTask("task_users_80-", "80-")

	tasks, err := store.Tasks().GetByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, "task_users_-80", tasks[0].TaskIdentifier)
	assert.Equal(t, "-80", tasks[0].Shard)
}

// GetByApplyID is the whole-apply drive loader — an operator claiming a queued
// apply loads every task it must drive through it. A shard-tagged row loads
// only when it is the drive task of a sharded work operation (the operation's
// shard key names the row's namespace/shard/table); unsharded per-table rows
// always load; reflected per-shard progress rows — shard rows whose operation's
// key does not match — stay out of the drive pipeline.
// GetByApplyID returns tasks in creation order — the plan's statement order,
// which the apply-level sequential drive executes as-is. All rows of one plan
// are stamped with a shared timestamp and created_at has second precision, so
// every row ties on the sort key; only the id tiebreaker keeps the order
// deterministic. The row count is large enough that a tie-broken filesort
// would otherwise return rows in arbitrary order.
func TestTaskStore_GetByApplyIDReturnsTasksInCreationOrder(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
	apply := createTestApply(t, store, lock, "apply_tasks_creation_order", 1)

	op, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", Target: "payments",
	})
	require.NoError(t, err)

	const taskCount = 20
	now := time.Now()
	for i := range taskCount {
		table := fmt.Sprintf("table_%02d", i)
		_, err := store.Tasks().Create(ctx, &storage.Task{
			TaskIdentifier:   fmt.Sprintf("task_order_%02d", i),
			ApplyID:          apply.ID,
			ApplyOperationID: &op,
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

	tasks, err := store.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, taskCount)
	for i, task := range tasks {
		assert.Equalf(t, fmt.Sprintf("task_order_%02d", i), task.TaskIdentifier,
			"task at position %d is out of creation order", i)
	}
}

func TestTaskStore_GetByApplyIDIncludesShardScopedDriveTasks(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "resolute", storage.DatabaseTypeStrata)
	apply := createTestApply(t, store, lock, "apply_shard_drive_tasks", 1)

	// A sharded work operation: its shard-tagged row for the keyed shard is the
	// drive task; a sibling shard's row under the same operation is not.
	shardedOpID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:       apply.ID,
		Deployment:    "region-a",
		OperationKey:  "commerce/-80/users",
		OperationKind: storage.ApplyOperationKindWork,
		Target:        "resolute",
	})
	require.NoError(t, err)

	// An unsharded operation: its per-table row drives; a shard-tagged row under
	// it is reflected per-shard progress, not drive work.
	unshardedOpID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:    apply.ID,
		Deployment: "region-b",
		Target:     "resolute",
	})
	require.NoError(t, err)

	// A work operation of a different apply with the same key. tasks has no
	// foreign-key constraint on apply_operation_id, so a row mis-associated
	// with another apply's operation must still be excluded from drive work.
	otherLock := createTestLock(t, store, "resolute_other", storage.DatabaseTypeStrata)
	otherApply := createTestApply(t, store, otherLock, "apply_other_shard_drive", 1)
	foreignOpID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:       otherApply.ID,
		Deployment:    "region-a",
		OperationKey:  "commerce/-80/users",
		OperationKind: storage.ApplyOperationKindWork,
		Target:        "resolute_other",
	})
	require.NoError(t, err)

	now := time.Now()
	createTask := func(identifier string, opID int64, table, shard string) {
		_, err := store.Tasks().Create(ctx, &storage.Task{
			TaskIdentifier:   identifier,
			ApplyID:          apply.ID,
			ApplyOperationID: &opID,
			PlanID:           apply.PlanID,
			Database:         apply.Database,
			DatabaseType:     apply.DatabaseType,
			Engine:           storage.EngineStrata,
			Environment:      apply.Environment,
			State:            state.Task.Pending,
			Namespace:        "commerce",
			TableName:        table,
			Shard:            shard,
			DDL:              "ALTER TABLE `" + table + "` ADD COLUMN `email` varchar(255)",
			DDLAction:        "ALTER",
			CreatedAt:        now,
			UpdatedAt:        now,
		})
		require.NoError(t, err)
	}
	createTask("task_users_-80_drive", shardedOpID, "users", "-80")
	createTask("task_users_80-_reflected", shardedOpID, "users", "80-")
	createTask("task_orders_drive", unshardedOpID, "orders", "")
	createTask("task_orders_-80_reflected", unshardedOpID, "orders", "-80")
	createTask("task_users_-80_foreign_op", foreignOpID, "users", "-80")

	tasks, err := store.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	identifiers := make([]string, 0, len(tasks))
	for _, task := range tasks {
		identifiers = append(identifiers, task.TaskIdentifier)
	}
	assert.ElementsMatch(t, []string{"task_users_-80_drive", "task_orders_drive"}, identifiers)
}

// An unsharded engine (MySQL/Spirit) uses the empty-string shard sentinel, which
// preserves today's one-task-per-table behavior.
func TestTaskStore_UnshardedTaskHasEmptyShard(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
	apply := createTestApply(t, store, lock, "apply_unsharded", 1)

	now := time.Now()
	_, err := store.Tasks().Create(ctx, &storage.Task{
		TaskIdentifier: "task_unsharded_users",
		ApplyID:        apply.ID,
		PlanID:         apply.PlanID,
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Engine:         storage.EngineSpirit,
		Environment:    apply.Environment,
		State:          state.Task.Pending,
		TableName:      "users",
		DDL:            "ALTER TABLE `users` ADD COLUMN email VARCHAR(255)",
		DDLAction:      "ALTER",
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	require.NoError(t, err)

	got, err := store.Tasks().Get(ctx, "task_unsharded_users")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "", got.Shard, "unsharded tasks default to the empty-string shard")
	assert.Equal(t, 0, got.CutoverAttempts)
}

// UpsertShardProgress is the operator's lease-held write-through for reflected
// per-shard progress (e.g. PlanetScale shards from SHOW VITESS_MIGRATIONS). The
// single lease-holding operator inserts a new per-shard task and updates it in
// place on later passes (no duplicate row); a caller without the operation lease
// is refused, and a displaced operator that lost the lease fails closed without
// writing — on both the insert and update paths.
func TestTaskStore_UpsertShardProgress(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "resolute", "vitess")
	apply := createTestApply(t, store, lock, "apply_upsert_shard", 1)
	opID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", Target: "resolute",
	})
	require.NoError(t, err)
	stampOperationLease(t, opID, "driver", "op-token")

	opCtx := func(token string) context.Context {
		return storage.WithOperationLease(ctx, storage.OperationLease{
			ApplyID: apply.ID, OperationID: opID, Owner: "driver", Token: token,
		})
	}

	now := time.Now()
	shardTask := func(shard string) *storage.Task {
		return &storage.Task{
			TaskIdentifier:   "task-" + shard,
			ApplyID:          apply.ID,
			ApplyOperationID: &opID,
			PlanID:           apply.PlanID,
			Database:         apply.Database,
			DatabaseType:     apply.DatabaseType,
			Engine:           storage.EnginePlanetScale,
			Environment:      apply.Environment,
			State:            state.Task.Running,
			Namespace:        "resolute",
			TableName:        "users",
			Shard:            shard,
			DDL:              "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			DDLAction:        "ALTER",
			RowsCopied:       100000,
			RowsTotal:        500000,
			ProgressPercent:  20,
			CreatedAt:        now,
			UpdatedAt:        now,
		}
	}

	// Without any drive lease on the context the write is refused outright.
	require.ErrorContains(t, store.Tasks().UpsertShardProgress(ctx, shardTask("-80")),
		"requires an operation or apply lease")

	// A displaced operator (stale token) fails closed on the insert path: nothing written.
	require.ErrorIs(t, store.Tasks().UpsertShardProgress(opCtx("stale"), shardTask("-80")), storage.ErrApplyLeaseLost)
	got, err := store.Tasks().GetShardProgressByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	assert.Empty(t, got, "a lost lease must not insert a shard task")

	// The lease holder inserts the shard row.
	require.NoError(t, store.Tasks().UpsertShardProgress(opCtx("op-token"), shardTask("-80")))
	got, err = store.Tasks().GetShardProgressByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "-80", got[0].Shard)
	assert.Equal(t, 20, got[0].ProgressPercent)

	// Re-upserting the same shard with advanced progress updates in place — no duplicate row.
	advanced := shardTask("-80")
	advanced.State = state.Task.Completed
	advanced.ProgressPercent = 100
	advanced.RowsCopied = 500000
	require.NoError(t, store.Tasks().UpsertShardProgress(opCtx("op-token"), advanced))
	got, err = store.Tasks().GetShardProgressByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	require.Len(t, got, 1, "re-upserting the same shard must update in place, not insert a duplicate")
	assert.Equal(t, state.Task.Completed, got[0].State)
	assert.Equal(t, 100, got[0].ProgressPercent)

	// A stale operator must not overwrite an existing shard row (update path fails closed).
	stale := shardTask("-80")
	stale.State = state.Task.Failed
	stale.ProgressPercent = 5
	require.ErrorIs(t, store.Tasks().UpsertShardProgress(opCtx("stale"), stale), storage.ErrApplyLeaseLost)
	got, err = store.Tasks().GetShardProgressByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, state.Task.Completed, got[0].State, "a lost lease must not overwrite the shard row")
	assert.Equal(t, 100, got[0].ProgressPercent)

	// A different shard under the same operation is a separate row.
	require.NoError(t, store.Tasks().UpsertShardProgress(opCtx("op-token"), shardTask("80-")))
	got, err = store.Tasks().GetShardProgressByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	assert.Len(t, got, 2, "a different shard is its own per-shard task row")

	// A row targeting a different operation than the held lease is refused, so
	// the lease cannot gate a write that points at another operation.
	mismatched := shardTask("c0-")
	otherOp := opID + 1000
	mismatched.ApplyOperationID = &otherOp
	require.Error(t, store.Tasks().UpsertShardProgress(opCtx("op-token"), mismatched))

	// A per-shard row must identify its table and shard.
	noTable := shardTask("e0-")
	noTable.TableName = ""
	require.ErrorContains(t, store.Tasks().UpsertShardProgress(opCtx("op-token"), noTable), "requires a table name")
	noShard := shardTask("")
	require.ErrorContains(t, store.Tasks().UpsertShardProgress(opCtx("op-token"), noShard), "requires a non-empty shard")

	// None of the refused writes created rows.
	got, err = store.Tasks().GetShardProgressByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	assert.Len(t, got, 2, "refused writes must not create rows")
}

// A single-operation (whole-apply) drive holds the apply lease rather than an
// operation lease. UpsertShardProgress accepts the apply lease as the
// single-writer guarantee, so per-shard rows are persisted for PlanetScale
// applies that never claim an operation; a displaced operator (stale apply
// token) still fails closed on both the insert and update paths.
func TestTaskStore_UpsertShardProgressUnderApplyLease(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "resolute", "vitess")
	apply := createTestApply(t, store, lock, "apply_upsert_shard_applylease", 1)
	opID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: apply.ID, Deployment: "region-a", Target: "resolute",
	})
	require.NoError(t, err)

	// The whole-apply drive holds the apply lease; no operation lease is claimed.
	_, err = testDB.ExecContext(ctx, `
		UPDATE applies SET lease_owner = ?, lease_token = ?, lease_acquired_at = NOW() WHERE id = ?
	`, "driver", "apply-token", apply.ID)
	require.NoError(t, err)

	applyCtx := func(token string) context.Context {
		return storage.WithApplyLease(ctx, storage.ApplyLease{
			ApplyID: apply.ID, Owner: "driver", Token: token,
		})
	}

	now := time.Now()
	shardTask := func(shard string) *storage.Task {
		return &storage.Task{
			TaskIdentifier:   "task-applylease-" + shard,
			ApplyID:          apply.ID,
			ApplyOperationID: &opID,
			PlanID:           apply.PlanID,
			Database:         apply.Database,
			DatabaseType:     apply.DatabaseType,
			Engine:           storage.EnginePlanetScale,
			Environment:      apply.Environment,
			State:            state.Task.Running,
			Namespace:        "resolute",
			TableName:        "users",
			Shard:            shard,
			DDL:              "ALTER TABLE `users` ADD INDEX `idx_email` (`email`)",
			DDLAction:        "ALTER",
			RowsCopied:       100000,
			RowsTotal:        500000,
			ProgressPercent:  20,
			CreatedAt:        now,
			UpdatedAt:        now,
		}
	}

	// A displaced operator (stale apply token) fails closed on the insert path.
	require.ErrorIs(t, store.Tasks().UpsertShardProgress(applyCtx("stale"), shardTask("-80")), storage.ErrApplyLeaseLost)
	got, err := store.Tasks().GetShardProgressByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	assert.Empty(t, got, "a lost apply lease must not insert a shard task")

	// The apply-lease holder inserts the shard row.
	require.NoError(t, store.Tasks().UpsertShardProgress(applyCtx("apply-token"), shardTask("-80")))
	got, err = store.Tasks().GetShardProgressByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "-80", got[0].Shard)
	assert.Equal(t, 20, got[0].ProgressPercent)

	// Re-upserting advances the same row in place under the apply lease.
	advanced := shardTask("-80")
	advanced.State = state.Task.Completed
	advanced.ProgressPercent = 100
	advanced.RowsCopied = 500000
	require.NoError(t, store.Tasks().UpsertShardProgress(applyCtx("apply-token"), advanced))
	got, err = store.Tasks().GetShardProgressByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	require.Len(t, got, 1, "re-upserting the same shard must update in place")
	assert.Equal(t, state.Task.Completed, got[0].State)
	assert.Equal(t, 100, got[0].ProgressPercent)

	// A stale apply token must not overwrite the existing shard row (update path fails closed).
	stale := shardTask("-80")
	stale.State = state.Task.Failed
	stale.ProgressPercent = 5
	require.ErrorIs(t, store.Tasks().UpsertShardProgress(applyCtx("stale"), stale), storage.ErrApplyLeaseLost)
	got, err = store.Tasks().GetShardProgressByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, state.Task.Completed, got[0].State, "a lost apply lease must not overwrite the shard row")

	// A shard task whose operation belongs to a different apply is rejected:
	// tasks has no FK constraints, so the apply-lease guard alone would not catch
	// an inconsistent (apply_id, apply_operation_id) pair.
	otherLock := createTestLock(t, store, "other_resolute", "vitess")
	otherApply := createTestApply(t, store, otherLock, "apply_other_applylease", apply.PlanID)
	otherOpID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: otherApply.ID, Deployment: "region-a", Target: "resolute",
	})
	require.NoError(t, err)
	crossApply := shardTask("a0-")
	crossApply.ApplyOperationID = &otherOpID // belongs to otherApply, not the leased apply
	require.ErrorContains(t, store.Tasks().UpsertShardProgress(applyCtx("apply-token"), crossApply), "belongs to apply")
}

// The object-ownership lookup answers "which pull requests have changed this
// object in this deployment target?" — the question the plan path asks before
// rendering a drop. It is scoped to one database, database type, and
// environment; it collapses a pull request's many tasks into one row carrying
// its most recent activity; and it excludes CLI-originated tasks, which carry
// no pull request to attribute anything to.
func TestTaskStore_FindTableOwners(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
	apply := createTestApply(t, store, lock, "apply_object_owners", 1)

	createTask := func(identifier, table, environment, repo string, pr int, createdAt time.Time) {
		_, err := store.Tasks().Create(ctx, &storage.Task{
			TaskIdentifier: identifier,
			ApplyID:        apply.ID,
			PlanID:         apply.PlanID,
			Database:       apply.Database,
			DatabaseType:   apply.DatabaseType,
			Engine:         storage.EngineSpirit,
			Repository:     repo,
			PullRequest:    pr,
			Environment:    environment,
			State:          state.Task.Completed,
			TableName:      table,
			DDL:            "CREATE TABLE `" + table + "` (`id` bigint unsigned NOT NULL)",
			DDLAction:      "CREATE",
			CreatedAt:      createdAt,
			UpdatedAt:      createdAt,
		})
		require.NoError(t, err)
	}

	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	createTask("task_owner_first", "reconcile_state", "staging", "octocat/hello-world", 7, base)
	createTask("task_owner_second", "reconcile_state", "staging", "octocat/hello-world", 7, base.Add(30*time.Minute))
	createTask("task_owner_other_pr", "reconcile_state", "staging", "octocat/hello-world", 9, base.Add(time.Minute))
	createTask("task_owner_other_env", "reconcile_state", "production", "octocat/hello-world", 11, base)
	createTask("task_owner_other_table", "users", "staging", "octocat/hello-world", 13, base)
	createTask("task_owner_cli", "reconcile_state", "staging", "", 0, base)

	ref := storage.TableRef{
		Database:     apply.Database,
		DatabaseType: apply.DatabaseType,
		Environment:  "staging",
		TableName:    "reconcile_state",
	}
	owners, err := store.Tasks().FindTableOwners(ctx, ref)
	require.NoError(t, err)
	require.Len(t, owners, 2, "one row per pull request, and never the CLI-originated task")
	assert.Equal(t, "octocat/hello-world", owners[0].Repository)
	assert.Equal(t, 7, owners[0].PullRequest, "the pull request seen most recently comes first")
	assert.Equal(t, base.Add(30*time.Minute).UTC(), owners[0].LastSeen.UTC())
	assert.Equal(t, 9, owners[1].PullRequest)

	ref.TableName = "audit_log"
	owners, err = store.Tasks().FindTableOwners(ctx, ref)
	require.NoError(t, err)
	assert.Empty(t, owners, "an object no task names has no owner")
}

// A long-lived table accumulates an owner for every pull request that ever
// changed it, and the plan path resolves each one's state against GitHub in
// turn. The lookup returns a recency window rather than the whole history, so
// that walk stays bounded no matter how much history a table carries.
func TestTaskStore_FindTableOwnersReturnsARecencyWindow(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
	apply := createTestApply(t, store, lock, "apply_object_owner_window", 1)

	base := time.Now().Add(-24 * time.Hour).Truncate(time.Second)
	total := tableOwnerLookupLimit + 5
	for i := range total {
		_, err := store.Tasks().Create(ctx, &storage.Task{
			TaskIdentifier: fmt.Sprintf("task_owner_window_%d", i),
			ApplyID:        apply.ID,
			PlanID:         apply.PlanID,
			Database:       apply.Database,
			DatabaseType:   apply.DatabaseType,
			Engine:         storage.EngineSpirit,
			Repository:     "octocat/hello-world",
			PullRequest:    100 + i,
			Environment:    "staging",
			State:          state.Task.Completed,
			TableName:      "reconcile_state",
			DDL:            "ALTER TABLE `reconcile_state` ADD COLUMN `c` int",
			DDLAction:      "ALTER",
			CreatedAt:      base.Add(time.Duration(i) * time.Minute),
			UpdatedAt:      base.Add(time.Duration(i) * time.Minute),
		})
		require.NoError(t, err)
	}

	owners, err := store.Tasks().FindTableOwners(ctx, storage.TableRef{
		Database:     apply.Database,
		DatabaseType: apply.DatabaseType,
		Environment:  "staging",
		TableName:    "reconcile_state",
	})
	require.NoError(t, err)
	require.Len(t, owners, tableOwnerLookupLimit)
	assert.Equal(t, 100+total-1, owners[0].PullRequest, "the window holds the most recent owners")
	assert.Equal(t, 100+total-tableOwnerLookupLimit, owners[len(owners)-1].PullRequest)
}

// createRetryableReapTask stores one task row under the apply in the given
// state, carrying its own error message, and returns the stored row.
func createRetryableReapTask(t *testing.T, store *Storage, apply *storage.Apply, identifier, table, taskState, errorMessage string) *storage.Task {
	t.Helper()
	now := time.Now()
	_, err := store.Tasks().Create(t.Context(), &storage.Task{
		TaskIdentifier: identifier,
		ApplyID:        apply.ID,
		PlanID:         apply.PlanID,
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Engine:         storage.EngineSpirit,
		Environment:    apply.Environment,
		State:          taskState,
		ErrorMessage:   errorMessage,
		TableName:      table,
		DDL:            "ALTER TABLE `" + table + "` ADD COLUMN `email` varchar(255)",
		DDLAction:      "ALTER",
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	require.NoError(t, err)
	task, err := store.Tasks().Get(t.Context(), identifier)
	require.NoError(t, err)
	require.NotNil(t, task)
	return task
}

func assertTaskState(t *testing.T, store *Storage, identifier, want string) {
	t.Helper()
	task, err := store.Tasks().Get(t.Context(), identifier)
	require.NoError(t, err)
	require.NotNil(t, task)
	assert.Equal(t, want, task.State)
}

// A failed_retryable task promises a retry that only its parent apply's
// recovery path can dispatch. Once the parent has settled, nothing will ever
// dispatch it, and the row poisons readers that treat failed_retryable as "a
// retry is coming" — most critically the control plane's remote-progress
// snapshot, which copies the row verbatim and reads it as a permanent retryable
// pause. The reaper hardens such rows to failed, keeping the task's own error
// message: its failure is real, only the dead retry promise is retired. Parents
// that are merely paused — stopped or failed_retryable — are resumable, so
// their rows stay with the retry path, as do the rows of a live parent.
func TestTaskStore_ReapStrandedRetryable_HardensTasksUnderSettledParents(t *testing.T) {
	const taskError = "connection reset during copy"
	const parentError = "target rejected the schema change"

	tests := []struct {
		name        string
		parentState string
		settles     bool
	}{
		{"completed parent hardens its dead retryable task", state.Apply.Completed, true},
		{"failed parent hardens its dead retryable task", state.Apply.Failed, true},
		{"cancelled parent hardens its dead retryable task", state.Apply.Cancelled, true},
		{"reverted parent hardens its dead retryable task", state.Apply.Reverted, true},
		{"stopped parent keeps its task for the resume path", state.Apply.Stopped, false},
		{"failed_retryable parent keeps its task for the retry path", state.Apply.FailedRetryable, false},
		{"running parent keeps its task for its driver", state.Apply.Running, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearTables(t)
			ctx := t.Context()
			store := NewMySQL(testDB)

			lock := createTestLock(t, store, "reap_fr_db", "mysql")
			parent := createTestApplyWithStateAndEnv(t, store, lock, "apply_reap_fr_"+tt.parentState, 1, tt.parentState, "staging")
			// The parent carries its own explanation so the assertions below prove
			// the task keeps its own error message rather than inheriting the
			// parent's.
			setApplyErrorMessage(t, parent.ID, parentError)
			backdateApplyUpdatedAt(t, parent.ID, strandedRetryableQuiescence+time.Minute)

			retryable := createRetryableReapTask(t, store, parent, "task_reap_fr_users", "users", state.Task.FailedRetryable, taskError)
			// A sibling in another state is not the reaper's to touch, whatever
			// the parent's verdict: only the dead retry promise is retired.
			completedSibling := createRetryableReapTask(t, store, parent, "task_reap_fr_orders", "orders", state.Task.Completed, "")

			settled, err := store.tasks.reapStrandedRetryable(ctx, 10)
			require.NoError(t, err)

			reloaded, err := store.Tasks().Get(ctx, retryable.TaskIdentifier)
			require.NoError(t, err)
			require.NotNil(t, reloaded)

			if !tt.settles {
				assert.Empty(t, settled, "a %s parent can still dispatch the retry, so its task stays failed_retryable", tt.parentState)
				assert.Equal(t, state.Task.FailedRetryable, reloaded.State)
				assert.Nil(t, reloaded.CompletedAt, "an untouched retryable task has no completion time")
				return
			}

			require.Len(t, settled, 1, "the dead retryable task under a %s parent settles", tt.parentState)
			assert.Equal(t, retryable.TaskIdentifier, settled[0].Task.TaskIdentifier)
			assert.Equal(t, state.Task.Failed, settled[0].Task.State, "the returned row carries the state it was written to")
			assert.Equal(t, parent.ApplyIdentifier, settled[0].Parent.ApplyIdentifier, "the parent travels with the settlement for triage logging")

			assert.Equal(t, state.Task.Failed, reloaded.State, "the task hardens to failed, never to the parent's state")
			assert.Equal(t, taskError, reloaded.ErrorMessage, "the task keeps its own failure explanation")
			assert.NotNil(t, reloaded.CompletedAt, "failed is terminal, so the row is stamped complete")
			assertTaskState(t, store, completedSibling.TaskIdentifier, state.Task.Completed)

			again, err := store.tasks.reapStrandedRetryable(ctx, 10)
			require.NoError(t, err)
			assert.Empty(t, again, "a hardened row is no longer failed_retryable, so a later sweep finds nothing to do")
		})
	}
}

// The reaper's quiescence window is sized past the retryable-recovery freshness
// window the claim paths key on the same parent heartbeat, so a retry admitted
// at the very edge of that window can never race the reap. A parent quiet for
// less than the reaper's window — even one long settled by the operation
// reaper's standard — is left alone until the retry window has definitively
// lapsed.
func TestTaskStore_ReapStrandedRetryable_WaitsOutTheRetryFreshnessWindow(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "reap_fr_fresh_db", "mysql")
	parent := createTestApplyWithStateAndEnv(t, store, lock, "apply_reap_fr_fresh", 1, state.Apply.Failed, "staging")
	task := createRetryableReapTask(t, store, parent, "task_reap_fr_fresh", "users", state.Task.FailedRetryable, "copy failed")

	// Quiescent by the operation reaper's standard, but still inside the
	// retryable-recovery freshness window.
	backdateApplyUpdatedAt(t, parent.ID, strandedParentQuiescence+time.Minute)
	settled, err := store.tasks.reapStrandedRetryable(ctx, 10)
	require.NoError(t, err)
	assert.Empty(t, settled, "a parent inside the retry freshness window is not yet quiescent for the task reaper")
	assertTaskState(t, store, task.TaskIdentifier, state.Task.FailedRetryable)

	backdateApplyUpdatedAt(t, parent.ID, strandedRetryableQuiescence+time.Minute)
	settled, err = store.tasks.reapStrandedRetryable(ctx, 10)
	require.NoError(t, err)
	require.Len(t, settled, 1, "once the retry window has lapsed the row settles")
	assertTaskState(t, store, task.TaskIdentifier, state.Task.Failed)
}

// The sweep is a bounded maintenance pass: it settles at most the requested
// number of rows per call, oldest first, so a large backlog drains across ticks
// instead of in one long transaction.
func TestTaskStore_ReapStrandedRetryable_RespectsLimit(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "reap_fr_limit_db", "mysql")
	parent := createTestApplyWithStateAndEnv(t, store, lock, "apply_reap_fr_limit", 1, state.Apply.Failed, "staging")
	backdateApplyUpdatedAt(t, parent.ID, strandedRetryableQuiescence+time.Minute)

	first := createRetryableReapTask(t, store, parent, "task_reap_fr_1", "users", state.Task.FailedRetryable, "copy failed")
	second := createRetryableReapTask(t, store, parent, "task_reap_fr_2", "orders", state.Task.FailedRetryable, "copy failed")
	third := createRetryableReapTask(t, store, parent, "task_reap_fr_3", "items", state.Task.FailedRetryable, "copy failed")
	backdateTaskCreatedAt(t, first.TaskIdentifier, 3)
	backdateTaskCreatedAt(t, second.TaskIdentifier, 2)
	backdateTaskCreatedAt(t, third.TaskIdentifier, 1)

	settled, err := store.tasks.reapStrandedRetryable(ctx, 2)
	require.NoError(t, err)
	require.Len(t, settled, 2)
	assert.Equal(t, first.TaskIdentifier, settled[0].Task.TaskIdentifier, "the oldest stranded row settles first")
	assert.Equal(t, second.TaskIdentifier, settled[1].Task.TaskIdentifier)
	assertTaskState(t, store, third.TaskIdentifier, state.Task.FailedRetryable)

	settled, err = store.tasks.reapStrandedRetryable(ctx, 2)
	require.NoError(t, err)
	require.Len(t, settled, 1, "the next sweep drains the remainder")
	assert.Equal(t, third.TaskIdentifier, settled[0].Task.TaskIdentifier)
}

// backdateTaskCreatedAt ages a task's created_at so ordering assertions do not
// depend on sub-second insert timing (created_at has second precision).
func backdateTaskCreatedAt(t *testing.T, identifier string, ageSeconds int64) {
	t.Helper()
	_, err := testDB.ExecContext(t.Context(),
		"UPDATE tasks SET created_at = NOW() - INTERVAL ? SECOND WHERE task_identifier = ?",
		ageSeconds, identifier)
	require.NoError(t, err)
}

// Selection and the write are separate statements, so a concurrent writer can
// advance the row in between. The write is guarded on the row still being
// failed_retryable, so the sweep skips it rather than overwriting a newer state.
func TestTaskStore_ReapStrandedRetryable_SkipsRowThatLeftFailedRetryable(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "reap_fr_guard_db", "mysql")
	parent := createTestApplyWithStateAndEnv(t, store, lock, "apply_reap_fr_guard", 1, state.Apply.Failed, "staging")
	backdateApplyUpdatedAt(t, parent.ID, strandedRetryableQuiescence+time.Minute)

	task := createRetryableReapTask(t, store, parent, "task_reap_fr_guard", "users", state.Task.FailedRetryable, "copy failed")
	selected, err := store.Tasks().Get(ctx, task.TaskIdentifier)
	require.NoError(t, err)

	// The row advances after selection: another writer moved it to running.
	_, err = testDB.ExecContext(ctx, "UPDATE tasks SET state = ? WHERE id = ?", state.Task.Running, task.ID)
	require.NoError(t, err)

	settledRow, err := store.tasks.reapStrandedRetryableTask(ctx, selected)
	require.NoError(t, err, "losing the race is a skip, not an error")
	assert.False(t, settledRow, "the guarded write reports that it did not land")
	assertTaskState(t, store, task.TaskIdentifier, state.Task.Running)
}

// The parent's state is re-asserted by the write, not just by the sweep that
// chose the row: hardening a task under a parent that has left the settled set
// would retire a retry promise the revived rollout can still dispatch.
func TestTaskStore_ReapStrandedRetryable_SkipsParentThatLeftTheSettledSet(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "reap_fr_revived_db", "mysql")
	parent := createTestApplyWithStateAndEnv(t, store, lock, "apply_reap_fr_revived", 1, state.Apply.Failed, "staging")
	backdateApplyUpdatedAt(t, parent.ID, strandedRetryableQuiescence+time.Minute)

	task := createRetryableReapTask(t, store, parent, "task_reap_fr_revived", "users", state.Task.FailedRetryable, "copy failed")
	selected, err := store.Tasks().Get(ctx, task.TaskIdentifier)
	require.NoError(t, err)

	// The parent leaves the settled set after selection. Its quiescence is
	// restored so the state it moved to is the only thing the guarded write can
	// object to.
	_, err = testDB.ExecContext(ctx, `UPDATE applies SET state = ? WHERE id = ?`, state.Apply.FailedRetryable, parent.ID)
	require.NoError(t, err)
	backdateApplyUpdatedAt(t, parent.ID, strandedRetryableQuiescence+time.Minute)

	settledRow, err := store.tasks.reapStrandedRetryableTask(ctx, selected)
	require.NoError(t, err, "losing the race is a skip, not an error")
	assert.False(t, settledRow, "the guarded write reports that it did not land")
	assertTaskState(t, store, task.TaskIdentifier, state.Task.FailedRetryable)
}

// Only one instance reaps tasks per pass, and its election is independent of
// the stranded-operation sweep's: the two sweeps hold different locks, so one
// instance settling operations never parks another instance's task reap.
func TestTaskStore_ReapStrandedRetryable_ElectsOneReaperPerPass(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "reap_fr_elect_db", "mysql")
	parent := createTestApplyWithStateAndEnv(t, store, lock, "apply_reap_fr_elect", 1, state.Apply.Failed, "staging")
	backdateApplyUpdatedAt(t, parent.ID, strandedRetryableQuiescence+time.Minute)
	task := createRetryableReapTask(t, store, parent, "task_reap_fr_elect", "users", state.Task.FailedRetryable, "copy failed")

	// A second store stands in for another instance: it holds the task-reaper
	// lock on its own connection while the first store tries to reap.
	other := NewMySQL(testDB)
	held, err := other.tasks.db.Conn(ctx)
	require.NoError(t, err)
	acquired, err := namedlock.MySQL{}.Acquire(ctx, held.lockerConn(), strandedTaskReaperLockName, 0)
	require.NoError(t, err)
	require.True(t, acquired, "the stand-in instance should take the task reaper lock")

	_, err = store.Tasks().ReapStrandedRetryable(ctx, 10)
	require.ErrorIs(t, err, storage.ErrStrandedTaskReaperBusy, "a second reaper steps aside rather than re-scanning")
	assertTaskState(t, store, task.TaskIdentifier, state.Task.FailedRetryable)

	// The operation reaper's lock is a different election: holding the task
	// lock does not park it.
	settledOps, err := store.ApplyOperations().ReapStranded(ctx, 10)
	require.NoError(t, err, "the operation reaper elects independently of the task reaper")
	assert.Empty(t, settledOps)

	released, err := namedlock.MySQL{}.Release(ctx, held.lockerConn(), strandedTaskReaperLockName)
	require.NoError(t, err)
	require.True(t, released)
	require.NoError(t, held.Close())

	// With the lock free, the reaper settles the row and releases the lock again,
	// so the pass after it is not locked out by its own predecessor.
	settled, err := store.Tasks().ReapStrandedRetryable(ctx, 10)
	require.NoError(t, err)
	require.Len(t, settled, 1)
	assertTaskState(t, store, task.TaskIdentifier, state.Task.Failed)

	next := createRetryableReapTask(t, store, parent, "task_reap_fr_elect_2", "orders", state.Task.FailedRetryable, "copy failed")
	settled, err = store.Tasks().ReapStrandedRetryable(ctx, 10)
	require.NoError(t, err)
	require.Len(t, settled, 1, "the task reaper lock is released after each pass")
	assert.Equal(t, next.TaskIdentifier, settled[0].Task.TaskIdentifier)
}
