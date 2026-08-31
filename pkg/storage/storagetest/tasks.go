package storagetest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestTasks runs the behavioral parity suite for storage.TaskStore.
func TestTasks(t *testing.T, h Harness) {
	t.Run("IdentityKeys_AreCaseInsensitive", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "task_mixed_case_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_task_mixed_case", 810)
		task := newTask(apply, "task_mixed_case", "CaseSensitiveTable", time.Now().UTC().Truncate(time.Second))
		task.Database = "MixedCase_DB"
		task.DatabaseType = "MySQL"
		task.Repository = "MixedCase/Sample-Repo"
		task.PullRequest = 813
		task.Environment = "StAgInG"
		id, err := store.Tasks().Create(ctx, task)
		require.NoError(t, err)
		require.Equal(t, "mixedcase_db", task.Database)
		require.Equal(t, "mysql", task.DatabaseType)
		require.Equal(t, "mixedcase/sample-repo", task.Repository)
		require.Equal(t, "staging", task.Environment)

		byDatabase, err := store.Tasks().GetByDatabase(ctx, "MIXEDCASE_DB")
		require.NoError(t, err)
		require.Len(t, byDatabase, 1)
		assert.Equal(t, id, byDatabase[0].ID)
		assert.Equal(t, "mixedcase_db", byDatabase[0].Database)
		assert.Equal(t, "mysql", byDatabase[0].DatabaseType)
		assert.Equal(t, "mixedcase/sample-repo", byDatabase[0].Repository)
		assert.Equal(t, "staging", byDatabase[0].Environment)
		assert.Equal(t, "CaseSensitiveTable", byDatabase[0].TableName)

		byPR, err := store.Tasks().GetByPR(ctx, "MIXEDCASE/SAMPLE-REPO", 813)
		require.NoError(t, err)
		require.Len(t, byPR, 1)
		assert.Equal(t, id, byPR[0].ID)

		listed, err := store.Tasks().List(ctx, storage.TaskFilter{
			Repository: "mixedCASE/sample-REPO", PullRequest: 813, IncludeCompleted: true,
		})
		require.NoError(t, err)
		require.Len(t, listed, 1)
		assert.Equal(t, id, listed[0].ID)

		owners, err := store.Tasks().FindTableOwners(ctx, storage.TableRef{
			Database: "MIXEDCASE_DB", DatabaseType: "MYSQL", Environment: "STAGING", TableName: "CaseSensitiveTable",
		})
		require.NoError(t, err)
		require.Len(t, owners, 1)
		assert.Equal(t, "mixedcase/sample-repo", owners[0].Repository)
		assert.Equal(t, 813, owners[0].PullRequest)
	})

	t.Run("Create_Get_Update", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "task_crud_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_task_crud", 901)

		missing, err := store.Tasks().Get(ctx, "task_missing")
		require.NoError(t, err)
		assert.Nil(t, missing)

		created := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
		task := newTask(apply, "task_crud", "users", created)
		task.Options = []byte(`{"lock_wait_timeout":10}`)
		id, err := store.Tasks().Create(ctx, task)
		require.NoError(t, err)
		require.Positive(t, id)

		got, err := store.Tasks().Get(ctx, task.TaskIdentifier)
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, id, got.ID)
		assert.Equal(t, apply.ID, got.ApplyID)
		assert.Equal(t, apply.PlanID, got.PlanID)
		assert.Equal(t, "task_crud_db", got.Database)
		assert.Equal(t, storage.DatabaseTypeMySQL, got.DatabaseType)
		assert.Equal(t, storage.EngineSpirit, got.Engine)
		assert.Equal(t, "org/repo", got.Repository)
		assert.Equal(t, 123, got.PullRequest)
		assert.Equal(t, "staging", got.Environment)
		assert.Equal(t, "task_crud_db", got.Namespace)
		assert.Equal(t, "users", got.TableName)
		assert.Empty(t, got.Shard)
		assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", got.DDL)
		assert.Equal(t, "alter", got.DDLAction)
		assert.JSONEq(t, `{"lock_wait_timeout":10}`, string(got.Options))
		assert.WithinDuration(t, created, got.CreatedAt, time.Second)

		started := created.Add(time.Minute)
		completed := started.Add(2 * time.Minute)
		got.State = state.Task.Completed
		got.Attempt = 2
		got.RowsCopied = 950
		got.RowsTotal = 1000
		got.ProgressPercent = 95
		got.ETASeconds = 12
		got.ChecksumRowsChecked = 800
		got.ChecksumRowsTotal = 1000
		got.Throttled = true
		got.ThrottleReason = "replica lag"
		got.CutoverAttempts = 3
		got.IsInstant = true
		got.EngineMigrationID = "engine-task-1"
		got.StartedAt = &started
		got.CompletedAt = &completed
		require.NoError(t, store.Tasks().Update(ctx, got))

		updated, err := store.Tasks().Get(ctx, task.TaskIdentifier)
		require.NoError(t, err)
		require.NotNil(t, updated)
		assert.Equal(t, state.Task.Completed, updated.State)
		assert.Equal(t, 2, updated.Attempt)
		assert.Equal(t, int64(950), updated.RowsCopied)
		assert.Equal(t, int64(1000), updated.RowsTotal)
		assert.Equal(t, 95, updated.ProgressPercent)
		assert.Equal(t, 12, updated.ETASeconds)
		assert.Equal(t, int64(800), updated.ChecksumRowsChecked)
		assert.Equal(t, int64(1000), updated.ChecksumRowsTotal)
		assert.True(t, updated.Throttled)
		assert.Equal(t, "replica lag", updated.ThrottleReason)
		assert.Equal(t, 3, updated.CutoverAttempts)
		assert.True(t, updated.IsInstant)
		assert.Equal(t, "engine-task-1", updated.EngineMigrationID)
		require.NotNil(t, updated.StartedAt)
		assert.WithinDuration(t, started, *updated.StartedAt, time.Second)
		require.NotNil(t, updated.CompletedAt)
		assert.WithinDuration(t, completed, *updated.CompletedAt, time.Second)
	})

	t.Run("GetByApplyID_CreationOrderAndIsolation", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "task_order_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_task_order", 902)
		other := CreateApplyWithStateAndEnv(t, store, lock, "apply_task_order_other", 903, state.Apply.Completed, "production")
		base := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)

		for _, task := range []*storage.Task{
			newTask(apply, "task_order_products", "products", base.Add(2*time.Second)),
			newTask(apply, "task_order_users", "users", base),
			newTask(apply, "task_order_orders", "orders", base.Add(time.Second)),
			newTask(apply, "task_order_accounts", "accounts", base.Add(time.Second)),
			newTask(other, "task_order_other", "other", base.Add(2*time.Second)),
		} {
			_, err := store.Tasks().Create(ctx, task)
			require.NoError(t, err)
		}

		tasks, err := store.Tasks().GetByApplyID(ctx, apply.ID)
		require.NoError(t, err)
		require.Len(t, tasks, 4)
		assert.Equal(t, []string{"task_order_users", "task_order_orders", "task_order_accounts", "task_order_products"}, taskIdentifiers(tasks),
			"same-second tasks must order by id ascending")

		count, err := store.Tasks().CountByApplyID(ctx, apply.ID)
		require.NoError(t, err)
		assert.Equal(t, int64(4), count)
	})

	t.Run("GetByApplyID_ShardScopedDriveTasksAndUnfilteredCount", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "task_shard_db", storage.DatabaseTypeStrata)
		apply := CreateApply(t, store, lock, "apply_task_shard", 905)
		created := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)

		namespace, shard, table := "commerce", "-80", "users"
		opID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID:       apply.ID,
			Deployment:    "region-a",
			OperationKey:  storage.ShardOperationKey(namespace, shard, table),
			OperationKind: storage.ApplyOperationKindWork,
			Target:        apply.Database,
		})
		require.NoError(t, err)

		matched := newTask(apply, "task_shard_matched", table, created)
		matched.ApplyOperationID = &opID
		matched.Namespace = namespace
		matched.Shard = shard
		_, err = store.Tasks().Create(ctx, matched)
		require.NoError(t, err)

		unmatched := newTask(apply, "task_shard_unmatched", "orders", created.Add(time.Second))
		unmatched.ApplyOperationID = &opID
		unmatched.Namespace = namespace
		unmatched.Shard = shard
		_, err = store.Tasks().Create(ctx, unmatched)
		require.NoError(t, err)

		tasks, err := store.Tasks().GetByApplyID(ctx, apply.ID)
		require.NoError(t, err)
		assert.Equal(t, []string{"task_shard_matched"}, taskIdentifiers(tasks),
			"only the shard row matching its work operation is drive work")

		count, err := store.Tasks().CountByApplyID(ctx, apply.ID)
		require.NoError(t, err)
		assert.Equal(t, int64(2), count, "the unfiltered count includes reflected shard rows")
	})

	// A shard row with no table name stores table_name as NULL, and
	// storage.ShardOperationKey builds its key with an empty table segment
	// ("ns/shard/"). Both shard-scoped loaders must apply the same key
	// semantics: the row matches a work operation keyed with the empty
	// table segment, on every dialect.
	t.Run("ShardScopedLoaders_EmptyTableNameKey", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "task_shard_notable_db", storage.DatabaseTypeStrata)
		apply := CreateApply(t, store, lock, "apply_task_shard_notable", 906)
		created := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)

		namespace, shard := "commerce", "80-"
		opID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID:       apply.ID,
			Deployment:    "region-a",
			OperationKey:  storage.ShardOperationKey(namespace, shard, ""),
			OperationKind: storage.ApplyOperationKindWork,
			Target:        apply.Database,
		})
		require.NoError(t, err)

		task := newTask(apply, "task_shard_notable", "", created)
		task.ApplyOperationID = &opID
		task.Namespace = namespace
		task.Shard = shard
		task.DDL = "CREATE TABLE `users` (`id` bigint unsigned NOT NULL)"
		task.DDLAction = "create"
		_, err = store.Tasks().Create(ctx, task)
		require.NoError(t, err)

		bystander := newTask(apply, "task_shard_notable_bystander", "orders", created.Add(time.Second))
		bystander.ApplyOperationID = &opID
		bystander.Namespace = namespace
		bystander.Shard = shard
		_, err = store.Tasks().Create(ctx, bystander)
		require.NoError(t, err)

		byApply, err := store.Tasks().GetByApplyID(ctx, apply.ID)
		require.NoError(t, err)
		assert.Equal(t, []string{"task_shard_notable"}, taskIdentifiers(byApply),
			"an empty-table shard row must match its work operation's empty-table key")

		byOp, err := store.Tasks().GetByApplyOperationID(ctx, opID)
		require.NoError(t, err)
		assert.Equal(t, []string{"task_shard_notable"}, taskIdentifiers(byOp),
			"the operation-scoped loader must apply the same empty-table key semantics")
	})

	t.Run("Update_LeaseGuardsWrites", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "task_lease_db", storage.DatabaseTypeMySQL)
		apply := CreateClaimedApply(t, store, lock, "apply_task_lease", 906, "current-driver")

		ownedCtx := storage.WithApplyLease(ctx, storage.ApplyLease{
			ApplyID: apply.ID, Owner: apply.LeaseOwner, Token: apply.LeaseToken,
		})
		staleCtx := storage.WithApplyLease(ctx, storage.ApplyLease{
			ApplyID: apply.ID, Owner: "old-driver", Token: "stale-token",
		})
		task, err := store.Tasks().Get(ctx, "task_"+apply.ApplyIdentifier)
		require.NoError(t, err)
		require.NotNil(t, task)

		task.State = state.Task.Failed
		require.ErrorIs(t, store.Tasks().Update(staleCtx, task), storage.ErrApplyLeaseLost)
		reloaded, err := store.Tasks().Get(ctx, task.TaskIdentifier)
		require.NoError(t, err)
		require.NotNil(t, reloaded)
		assert.Equal(t, state.Task.Pending, reloaded.State, "a stale lease must not update the task")

		task.State = state.Task.Completed
		require.NoError(t, store.Tasks().Update(ownedCtx, task))
		reloaded, err = store.Tasks().Get(ctx, task.TaskIdentifier)
		require.NoError(t, err)
		require.NotNil(t, reloaded)
		assert.Equal(t, state.Task.Completed, reloaded.State)
	})

	t.Run("ExpireRetryable_TerminalizesUnfinishedTasks", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "task_expiry_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "apply_task_expiry", 904, state.Apply.FailedRetryable, "staging")
		created := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)

		retryable := newTask(apply, "task_expiry_retryable", "users", created)
		retryable.State = state.Task.FailedRetryable
		pending := newTask(apply, "task_expiry_pending", "orders", created)
		completed := newTask(apply, "task_expiry_completed", "products", created)
		completed.State = state.Task.Completed
		completedAt := created.Add(time.Minute)
		completed.CompletedAt = &completedAt
		for _, task := range []*storage.Task{retryable, pending, completed} {
			_, err := store.Tasks().Create(ctx, task)
			require.NoError(t, err)
		}

		// Exhaust the recovery budget through the storage interface because the
		// parity suite cannot backdate the attempt counter with dialect-specific SQL.
		for range storage.MaxRecoveryAttempts {
			claimed, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "test-driver")
			require.NoError(t, err)
			require.NotNil(t, claimed)
			lease := storage.ApplyLease{
				ApplyID: claimed.ID, Owner: claimed.LeaseOwner, Token: claimed.LeaseToken,
			}
			claimed.State = state.Apply.FailedRetryable
			require.NoError(t, store.Applies().Update(storage.WithApplyLease(ctx, lease), claimed))
			released, err := store.Applies().ReleaseClaim(ctx, storage.ApplyLease{
				ApplyID: lease.ApplyID, Owner: lease.Owner, Token: lease.Token,
			})
			require.NoError(t, err)
			require.True(t, released)
		}

		expired, err := store.Applies().ExpireRetryable(ctx)
		require.NoError(t, err)
		require.Len(t, expired, 1)
		assert.Equal(t, storage.RetryableExpirationAttemptBudget, expired[0].Reason)
		reloadedApply, err := store.Applies().Get(ctx, apply.ID)
		require.NoError(t, err)
		require.NotNil(t, reloadedApply)
		assert.Equal(t, state.Apply.Failed, reloadedApply.State)
		assert.NotNil(t, reloadedApply.CompletedAt)

		assertTask := func(identifier, wantState string, wantCompletedAt time.Time) {
			t.Helper()
			task, err := store.Tasks().Get(ctx, identifier)
			require.NoError(t, err)
			require.NotNil(t, task)
			assert.Equal(t, wantState, task.State)
			require.NotNil(t, task.CompletedAt)
			if !wantCompletedAt.IsZero() {
				assert.WithinDuration(t, wantCompletedAt, *task.CompletedAt, time.Second)
			}
		}
		assertTask("task_expiry_retryable", state.Task.Failed, time.Time{})
		assertTask("task_expiry_pending", state.Task.Cancelled, time.Time{})
		assertTask("task_expiry_completed", state.Task.Completed, completedAt)
	})

	t.Run("Create_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Tasks().Create(t.Context(), &storage.Task{TaskIdentifier: "task_err"})
		require.Error(t, err)
	})

	t.Run("Get_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Tasks().Get(t.Context(), "task_err")
		require.Error(t, err)
	})

	t.Run("Update_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		require.Error(t, store.Tasks().Update(t.Context(), &storage.Task{ID: 1}))
	})

	t.Run("UpsertShardProgress_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		ctx := storage.WithApplyLease(t.Context(), storage.ApplyLease{ApplyID: 1, Owner: "driver", Token: "token"})
		operationID := int64(1)
		require.Error(t, store.Tasks().UpsertShardProgress(ctx, &storage.Task{ApplyID: 1, ApplyOperationID: &operationID}))
	})

	t.Run("GetByApplyID_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Tasks().GetByApplyID(t.Context(), 1)
		require.Error(t, err)
	})

	t.Run("CountByApplyID_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Tasks().CountByApplyID(t.Context(), 1)
		require.Error(t, err)
	})

	t.Run("GetByApplyOperationID_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Tasks().GetByApplyOperationID(t.Context(), 1)
		require.Error(t, err)
	})

	t.Run("GetShardProgressByApplyOperationID_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Tasks().GetShardProgressByApplyOperationID(t.Context(), 1)
		require.Error(t, err)
	})

	t.Run("GetByDatabase_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Tasks().GetByDatabase(t.Context(), "database")
		require.Error(t, err)
	})

	t.Run("GetActive_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Tasks().GetActive(t.Context())
		require.Error(t, err)
	})

	t.Run("GetByPR_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Tasks().GetByPR(t.Context(), "org/repo", 1)
		require.Error(t, err)
	})

	t.Run("FindTableOwners_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Tasks().FindTableOwners(t.Context(), storage.TableRef{Database: "database", TableName: "users"})
		require.Error(t, err)
	})

	t.Run("List_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Tasks().List(t.Context(), storage.TaskFilter{})
		require.Error(t, err)
	})

	t.Run("ReapStrandedRetryable_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Tasks().ReapStrandedRetryable(t.Context(), 1)
		require.Error(t, err)
	})
}

func newTask(apply *storage.Apply, identifier, table string, createdAt time.Time) *storage.Task {
	return &storage.Task{
		TaskIdentifier: identifier,
		ApplyID:        apply.ID,
		PlanID:         apply.PlanID,
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Engine:         apply.Engine,
		Repository:     apply.Repository,
		PullRequest:    apply.PullRequest,
		Environment:    apply.Environment,
		State:          state.Task.Pending,
		Namespace:      apply.Database,
		TableName:      table,
		DDL:            "ALTER TABLE `" + table + "` ADD COLUMN `email` varchar(255)",
		DDLAction:      "alter",
		CreatedAt:      createdAt,
		UpdatedAt:      createdAt,
	}
}

func taskIdentifiers(tasks []*storage.Task) []string {
	identifiers := make([]string, len(tasks))
	for i, task := range tasks {
		identifiers[i] = task.TaskIdentifier
	}
	return identifiers
}
