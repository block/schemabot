package storagetest

import (
	"strings"
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

	// A task's planned table size and row count are written once when the
	// task is created and read back with it. Progress updates rewrite the
	// task's live figures but never the plan's estimates, so a drive that does
	// not carry them cannot clear them.
	t.Run("PlanEstimates_WrittenAtCreate_SurvivesUpdate", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "task_bytes_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_task_bytes", 902)
		now := time.Now().UTC().Truncate(time.Second)

		sized := newTask(apply, "task_bytes_sized", "orders", now)
		bytes := int64(23_400_000_000)
		sized.EstimatedBytes = &bytes
		rows := int64(48_200_000)
		sized.EstimatedRows = &rows
		_, err := store.Tasks().Create(ctx, sized)
		require.NoError(t, err)
		unsized := newTask(apply, "task_bytes_unsized", "users", now)
		_, err = store.Tasks().Create(ctx, unsized)
		require.NoError(t, err)

		got, err := store.Tasks().Get(ctx, sized.TaskIdentifier)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.NotNil(t, got.EstimatedBytes)
		assert.Equal(t, bytes, *got.EstimatedBytes)
		require.NotNil(t, got.EstimatedRows)
		assert.Equal(t, rows, *got.EstimatedRows)

		got.EstimatedBytes = nil
		got.EstimatedRows = nil
		got.State = state.Task.Running
		got.RowsCopied = 500
		got.RowsTotal = 1000
		require.NoError(t, store.Tasks().Update(ctx, got))
		updated, err := store.Tasks().Get(ctx, sized.TaskIdentifier)
		require.NoError(t, err)
		require.NotNil(t, updated)
		assert.Equal(t, int64(500), updated.RowsCopied)
		require.NotNil(t, updated.EstimatedBytes, "a progress update never clears the planned size")
		assert.Equal(t, bytes, *updated.EstimatedBytes)
		require.NotNil(t, updated.EstimatedRows, "a progress update never clears the planned row count")
		assert.Equal(t, rows, *updated.EstimatedRows)

		gotUnsized, err := store.Tasks().Get(ctx, unsized.TaskIdentifier)
		require.NoError(t, err)
		require.NotNil(t, gotUnsized)
		assert.Nil(t, gotUnsized.EstimatedBytes, "a task created without an estimate reads back without one")
		assert.Nil(t, gotUnsized.EstimatedRows, "a task created without a row estimate reads back without one")
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
		got.DDL = "ALTER TABLE `app_b`.`users` ADD COLUMN `email` varchar(255)"
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
		assert.Equal(t, "ALTER TABLE `app_b`.`users` ADD COLUMN `email` varchar(255)", updated.DDL,
			"a task's statement follows Update so an adopted deployment rendering survives the next read")
		assert.Equal(t, "alter", updated.DDLAction)
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

	// A task row carries no terminal-state guard: the lease fence alone decides
	// who may write it. A displaced driver's late write over a terminal task is
	// refused and the row stays terminal, while the driver that now holds the
	// lease is the one whose write lands.
	t.Run("Update_DisplacedLeaseCannotWriteOverTerminalTask", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "task_displaced_db", storage.DatabaseTypeMySQL)
		first := CreateClaimedApply(t, store, lock, "apply_task_displaced", 907, "driver-a")
		firstCtx := storage.WithApplyLease(ctx, first.Lease())

		task, err := store.Tasks().Get(ctx, "task_"+first.ApplyIdentifier)
		require.NoError(t, err)
		require.NotNil(t, task)
		task.State = state.Task.Completed
		require.NoError(t, store.Tasks().Update(firstCtx, task))

		released, err := store.Applies().ReleaseClaim(ctx, first.Lease())
		require.NoError(t, err)
		require.True(t, released)
		second, err := store.Applies().ClaimApplyByID(ctx, first.ID, "driver-b")
		require.NoError(t, err)
		require.NotNil(t, second)

		task.State = state.Task.Running
		require.ErrorIs(t, store.Tasks().Update(firstCtx, task), storage.ErrApplyLeaseLost)
		reloaded, err := store.Tasks().Get(ctx, task.TaskIdentifier)
		require.NoError(t, err)
		require.NotNil(t, reloaded)
		assert.Equal(t, state.Task.Completed, reloaded.State, "a displaced driver must not reopen a terminal task")

		task.State = state.Task.Failed
		require.NoError(t, store.Tasks().Update(storage.WithApplyLease(ctx, second.Lease()), task))
		reloaded, err = store.Tasks().Get(ctx, task.TaskIdentifier)
		require.NoError(t, err)
		require.NotNil(t, reloaded)
		assert.Equal(t, state.Task.Failed, reloaded.State, "the current lease holder's write lands: the fence, not the row state, decides")
	})

	// A task an operation drive writes is fenced by the operation's lease. Once
	// that drive has ended and handed the operation's lease back, a late write
	// under the old lease is refused and the terminal task stays as it was.
	t.Run("Update_EndedOperationLeaseCannotWriteOverTerminalTask", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "task_ended_operation_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_task_ended_operation", 909)
		opID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "region-a", OperationKey: "schema",
		})
		require.NoError(t, err)
		task := newTask(apply, "task_ended_operation_users", "users", time.Now().UTC().Truncate(time.Second))
		task.ApplyOperationID = &opID
		_, err = store.Tasks().Create(ctx, task)
		require.NoError(t, err)
		task, err = store.Tasks().Get(ctx, "task_ended_operation_users")
		require.NoError(t, err)
		require.NotNil(t, task)

		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "driver-a")
		require.NoError(t, err)
		require.NotNil(t, claimed)
		require.Equal(t, opID, claimed.ID)
		opCtx := storage.WithOperationLease(ctx, claimed.Lease())

		task.State = state.Task.Completed
		require.NoError(t, store.Tasks().Update(opCtx, task))
		require.NoError(t, store.ApplyOperations().MarkCompleted(opCtx, opID))
		released, err := store.ApplyOperations().ReleaseFinishedClaim(ctx, claimed.Lease())
		require.NoError(t, err)
		require.True(t, released)

		task.State = state.Task.Running
		require.ErrorIs(t, store.Tasks().Update(opCtx, task), storage.ErrApplyLeaseLost)
		reloaded, err := store.Tasks().Get(ctx, task.TaskIdentifier)
		require.NoError(t, err)
		require.NotNil(t, reloaded)
		assert.Equal(t, state.Task.Completed, reloaded.State, "a drive that has ended must not reopen a terminal task")
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

		expired, err := store.Applies().ExpireRetryable(ctx, endedOperationStopTypes, 10)
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

	t.Run("ReapStrandedActive_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.Tasks().ReapStrandedActive(t.Context(), 1)
		require.Error(t, err)
	})

	// A failure message carries engine and database error text with no bound
	// of its own. A write that records one longer than the error_message
	// column holds lands with the message cut to the column on a character
	// boundary, on the task, its apply and its operation alike, instead of
	// being refused on every attempt and leaving the failure unrecorded.
	t.Run("FailureMessage_PastColumnWidth_IsCutToFit", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "task_long_failure_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_task_long_failure", 905)
		long, want := overLongFailureMessage, overLongFailureMessageStored

		task := newTask(apply, "task_long_failure", "orders", time.Now().UTC().Truncate(time.Second))
		task.ErrorMessage = long
		_, err := store.Tasks().Create(ctx, task)
		require.NoError(t, err, "a task created with a long failure message is stored")
		created, err := store.Tasks().Get(ctx, "task_long_failure")
		require.NoError(t, err)
		require.NotNil(t, created)
		assertStoredMessage(t, want, created.ErrorMessage, "the insert cuts the message on a character boundary")

		created.State = state.Task.Failed
		created.ErrorMessage = long
		require.NoError(t, store.Tasks().Update(ctx, created), "a task's failed verdict with a long message is stored")
		failedTask, err := store.Tasks().Get(ctx, "task_long_failure")
		require.NoError(t, err)
		assert.Equal(t, state.Task.Failed, failedTask.State)
		assertStoredMessage(t, want, failedTask.ErrorMessage, "the task update cuts the message")

		apply.State = state.Apply.Failed
		apply.ErrorMessage = long
		require.NoError(t, store.Applies().Update(ctx, apply), "an apply's failure with a long message is stored")
		failedApply, err := store.Applies().Get(ctx, apply.ID)
		require.NoError(t, err)
		assert.Equal(t, state.Apply.Failed, failedApply.State)
		assertStoredMessage(t, want, failedApply.ErrorMessage, "the apply update cuts the message")

		opID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "primary", OperationKey: "orders",
			OperationKind: storage.ApplyOperationKindWork, ErrorMessage: long,
		})
		require.NoError(t, err, "an operation created with a long failure message is stored")
		createdOp, err := store.ApplyOperations().Get(ctx, opID)
		require.NoError(t, err)
		assertStoredMessage(t, want, createdOp.ErrorMessage, "the operation insert cuts the message")
		require.NoError(t, store.ApplyOperations().MarkFailed(ctx, opID, long), "an operation's failure with a long message is stored")
		failedOp, err := store.ApplyOperations().Get(ctx, opID)
		require.NoError(t, err)
		assert.Equal(t, state.ApplyOperation.Failed, failedOp.State)
		assertStoredMessage(t, want, failedOp.ErrorMessage, "marking the operation failed cuts the message")
	})

	// The cut is to the column's width exactly: a message that fills the
	// column is stored whole, and one a byte longer loses only what the
	// marker needs.
	t.Run("FailureMessage_AtTheColumnWidth_IsStoredWhole", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "task_failure_width_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_task_failure_width", 906)
		cases := []struct {
			identifier, msg, want string
		}{
			{"task_failure_at_width", strings.Repeat("a", 65_535), strings.Repeat("a", 65_535)},
			{"task_failure_past_width", strings.Repeat("a", 65_536), strings.Repeat("a", 65_532) + "…"},
		}
		for _, tc := range cases {
			task := newTask(apply, tc.identifier, "orders", time.Now().UTC().Truncate(time.Second))
			task.ErrorMessage = tc.msg
			_, err := store.Tasks().Create(ctx, task)
			require.NoError(t, err, tc.identifier)
			got, err := store.Tasks().Get(ctx, tc.identifier)
			require.NoError(t, err)
			require.NotNil(t, got)
			assertStoredMessage(t, tc.want, got.ErrorMessage, tc.identifier)
		}
	})

	// An engine can quote a binary key value in its error, so a failure
	// message need not be valid UTF-8. It is stored with the invalid bytes
	// replaced rather than refused, on the apply and its task alike.
	t.Run("FailureMessage_NotValidUTF8_IsStoredWithTheBytesReplaced", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "task_invalid_utf8_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_task_invalid_utf8", 907)
		const msg = "duplicate key \xff\xfe in row"
		const want = "duplicate key \uFFFD in row"

		apply.State = state.Apply.Failed
		apply.ErrorMessage = msg
		require.NoError(t, store.Applies().Update(ctx, apply), "an apply failure that is not valid UTF-8 is stored")
		failedApply, err := store.Applies().Get(ctx, apply.ID)
		require.NoError(t, err)
		require.NotNil(t, failedApply)
		assert.Equal(t, state.Apply.Failed, failedApply.State)
		assert.Equal(t, want, failedApply.ErrorMessage)

		task := newTask(apply, "task_invalid_utf8", "orders", time.Now().UTC().Truncate(time.Second))
		task.State = state.Task.Failed
		task.ErrorMessage = msg
		_, err = store.Tasks().Create(ctx, task)
		require.NoError(t, err, "a task failure that is not valid UTF-8 is stored")
		failedTask, err := store.Tasks().Get(ctx, "task_invalid_utf8")
		require.NoError(t, err)
		require.NotNil(t, failedTask)
		assert.Equal(t, want, failedTask.ErrorMessage)
	})

	// A per-shard task row inserted through the shard progress write-through
	// stores an over-long failure cut to fit, like any other task row.
	t.Run("FailureMessage_PastColumnWidth_OnANewShardRow_IsCutToFit", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		lock := CreateLock(t, store, "task_shard_long_failure_db", storage.DatabaseTypeVitess)
		apply := CreateApply(t, store, lock, "apply_task_shard_long_failure", 908)
		opID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
			ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce",
		})
		require.NoError(t, err)
		claimed, err := store.ApplyOperations().FindNextApplyOperation(ctx, "current-driver")
		require.NoError(t, err)
		require.NotNil(t, claimed)
		require.Equal(t, opID, claimed.ID)
		ownedCtx := storage.WithOperationLease(ctx, storage.OperationLease{
			ApplyID: apply.ID, OperationID: opID, Owner: claimed.LeaseOwner, Token: claimed.LeaseToken,
		})

		shard := newTask(apply, "task_shard_long_failure", "users", time.Now().UTC().Truncate(time.Second))
		shard.ApplyOperationID = &opID
		shard.Engine = storage.EnginePlanetScale
		shard.Namespace = "commerce"
		shard.Shard = "-80"
		shard.State = state.Task.Failed
		shard.ErrorMessage = overLongFailureMessage
		require.NoError(t, store.Tasks().UpsertShardProgress(ownedCtx, shard), "a new shard row with a long failure message is stored")

		rows, err := store.Tasks().GetShardProgressByApplyOperationID(ctx, opID)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, state.Task.Failed, rows[0].State)
		assertStoredMessage(t, overLongFailureMessageStored, rows[0].ErrorMessage, "the shard insert cuts the message")
	})

	t.Run("ReapStrandedActive_RejectsNonPositiveLimit", func(t *testing.T) {
		store := h.NewStorage(t)
		_, err := store.Tasks().ReapStrandedActive(t.Context(), 0)
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

var (
	// overLongFailureMessage is a failure message past the column's width. The
	// leading byte puts every two-byte character at an odd offset, so the
	// column's byte width falls inside a character.
	overLongFailureMessage = "x" + strings.Repeat("é", 40_000)
	// overLongFailureMessageStored is overLongFailureMessage as storage
	// records it: cut at the start of the character the width falls inside,
	// with an ellipsis marking the cut.
	overLongFailureMessageStored = "x" + strings.Repeat("é", 32_765) + "…"
)

// assertStoredMessage compares a long stored message by its length and its
// end, where a cut shows, before comparing it whole, so a mismatch reports
// something readable instead of two messages too long to print.
func assertStoredMessage(t *testing.T, want, got, msg string) {
	t.Helper()
	if assert.Equal(t, len(want), len(got), "%s: stored length in bytes", msg) &&
		assert.Equal(t, want[len(want)-16:], got[len(got)-16:], "%s: end of the stored message", msg) {
		assert.True(t, want == got, "%s: stored message", msg)
	}
}

func taskIdentifiers(tasks []*storage.Task) []string {
	identifiers := make([]string, len(tasks))
	for i, task := range tasks {
		identifiers[i] = task.TaskIdentifier
	}
	return identifiers
}
