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
		created := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)

		for _, task := range []*storage.Task{
			newTask(apply, "task_order_users", "users", created),
			newTask(apply, "task_order_orders", "orders", created),
			newTask(apply, "task_order_products", "products", created),
			newTask(other, "task_order_other", "other", created),
		} {
			_, err := store.Tasks().Create(ctx, task)
			require.NoError(t, err)
		}

		tasks, err := store.Tasks().GetByApplyID(ctx, apply.ID)
		require.NoError(t, err)
		require.Len(t, tasks, 3)
		assert.Equal(t, []string{"task_order_users", "task_order_orders", "task_order_products"}, taskIdentifiers(tasks))

		count, err := store.Tasks().CountByApplyID(ctx, apply.ID)
		require.NoError(t, err)
		assert.Equal(t, int64(3), count)
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
