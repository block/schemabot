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

// A task is the per-(table, shard) execution record for a sharded engine: the
// shard and its cutover attempts round-trip through create/get, two shards of
// one table coexist under a single operation, cutover_attempts is updatable, and
// the shard is fixed at creation (Update never rewrites it).
func TestTaskStore_PerShardTaskRoundTrip(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "resolute", "vitess", "staging")
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

// A sharded work operation's operation key identifies which shard task is real
// drive input. Other shard rows remain progress detail and must not be replayed
// as extra table changes if the operation is resumed.
func TestTaskStore_GetByApplyOperationIDIncludesMatchingShardedWorkTask(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "resolute", storage.DatabaseTypeStrata, "staging")
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

// An unsharded engine (MySQL/Spirit) uses the empty-string shard sentinel, which
// preserves today's one-task-per-table behavior.
func TestTaskStore_UnshardedTaskHasEmptyShard(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := New(testDB)

	lock := createTestLock(t, store, "testdb", "mysql", "staging")
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
	store := New(testDB)

	lock := createTestLock(t, store, "resolute", "vitess", "staging")
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
	advanced.ReadyToComplete = true
	require.NoError(t, store.Tasks().UpsertShardProgress(opCtx("op-token"), advanced))
	got, err = store.Tasks().GetShardProgressByApplyOperationID(ctx, opID)
	require.NoError(t, err)
	require.Len(t, got, 1, "re-upserting the same shard must update in place, not insert a duplicate")
	assert.Equal(t, state.Task.Completed, got[0].State)
	assert.Equal(t, 100, got[0].ProgressPercent)
	assert.True(t, got[0].ReadyToComplete)

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
	store := New(testDB)

	lock := createTestLock(t, store, "resolute", "vitess", "staging")
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
	otherLock := createTestLock(t, store, "other_resolute", "vitess", "staging")
	otherApply := createTestApply(t, store, otherLock, "apply_other_applylease", apply.PlanID)
	otherOpID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID: otherApply.ID, Deployment: "region-a", Target: "resolute",
	})
	require.NoError(t, err)
	crossApply := shardTask("a0-")
	crossApply.ApplyOperationID = &otherOpID // belongs to otherApply, not the leased apply
	require.ErrorContains(t, store.Tasks().UpsertShardProgress(applyCtx("apply-token"), crossApply), "belongs to apply")
}
