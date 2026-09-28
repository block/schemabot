//go:build integration

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/storagetest"
)

// leaseStealRaceDeadline bounds each wait in a lease-steal race: the displaced
// write reaching the lease row's lock, and the write returning once the steal
// commits.
const (
	leaseStealRaceDeadline  = 30 * time.Second
	leaseFenceProbeDeadline = 2 * time.Second
)

// lockWaiter reports whether a session is blocked on a row lock while running
// a statement whose text contains fragment.
type lockWaiter func(t *testing.T, fragment string) bool

// mysqlLockWaiter reads InnoDB's pending lock requests for one made by a
// thread running the statement. It reads performance_schema rather than
// information_schema.innodb_trx because a statement blocked while the
// optimizer reads a constant row does not always report LOCK WAIT there.
func mysqlLockWaiter(db *sql.DB) lockWaiter {
	return func(t *testing.T, fragment string) bool {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), leaseFenceProbeDeadline)
		defer cancel()
		var waiting int
		require.NoError(t, db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM performance_schema.data_lock_waits w
			JOIN performance_schema.threads th ON th.THREAD_ID = w.REQUESTING_THREAD_ID
			WHERE th.PROCESSLIST_INFO LIKE ?
		`, "%"+fragment+"%").Scan(&waiting))
		return waiting > 0
	}
}

// postgresLockWaiter reads pg_stat_activity for a backend waiting on a lock.
func postgresLockWaiter(db *sql.DB) lockWaiter {
	return func(t *testing.T, fragment string) bool {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), leaseFenceProbeDeadline)
		defer cancel()
		var waiting int
		require.NoError(t, db.QueryRowContext(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND query LIKE $1
		`, "%"+fragment+"%").Scan(&waiting))
		return waiting > 0
	}
}

// TestLeaseFencedWritesFailClosedAgainstConcurrentSteal runs the lease-steal
// race scenarios against MySQL; TestPostgresStorageParity runs the same
// scenarios against PostgreSQL.
func TestLeaseFencedWritesFailClosedAgainstConcurrentSteal(t *testing.T) {
	testLeaseFencedWritesFailClosedAgainstConcurrentSteal(t, func(t *testing.T) *Storage {
		t.Helper()
		clearTables(t)
		return NewMySQL(testDB)
	}, mysqlLockWaiter(testDB))
}

// testLeaseFencedWritesFailClosedAgainstConcurrentSteal pins that a displaced
// driver's task and comment writes never land once another driver has taken
// the lease. The steal is applied but left uncommitted while the displaced
// driver writes, so the write's statement snapshot still holds the token the
// steal is replacing. The write must wait on the lease row, fail closed with
// ErrApplyLeaseLost once the steal commits, and leave the row as it was; the
// driver that took the lease then writes normally.
func testLeaseFencedWritesFailClosedAgainstConcurrentSteal(t *testing.T, newStore func(t *testing.T) *Storage, waiting lockWaiter) {
	t.Run("task update under an apply lease", func(t *testing.T) {
		ctx := t.Context()
		store := newStore(t)
		lock := storagetest.CreateLock(t, store, "fence_task_apply_db", storage.DatabaseTypeMySQL)
		apply := storagetest.CreateClaimedApply(t, store, lock, "apply_fence_task_apply", 931, "driver-a")
		task := loadTask(t, store, "task_apply_fence_task_apply")

		displacedCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: apply.ID, Owner: apply.LeaseOwner, Token: apply.LeaseToken})
		task.State = state.Task.Failed
		err := writeDuringUncommittedSteal(t, store, waiting, "UPDATE tasks",
			func() error { return store.Tasks().Update(displacedCtx, task) },
			`UPDATE applies SET lease_owner = ?, lease_token = ? WHERE id = ?`, "driver-b", "tok-b", apply.ID)
		require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
		assert.Equal(t, state.Task.Pending, loadTask(t, store, task.TaskIdentifier).State, "a displaced driver must not write the task")

		ownerCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: apply.ID, Owner: "driver-b", Token: "tok-b"})
		task.State = state.Task.Running
		require.NoError(t, store.Tasks().Update(ownerCtx, task))
		assert.Equal(t, state.Task.Running, loadTask(t, store, task.TaskIdentifier).State)
	})

	t.Run("task update under an operation lease", func(t *testing.T) {
		ctx := t.Context()
		store := newStore(t)
		lock := storagetest.CreateLock(t, store, "fence_task_op_db", storage.DatabaseTypeMySQL)
		apply := storagetest.CreateApplyWithTask(t, store, lock, "apply_fence_task_op", 932)
		opID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{ApplyID: apply.ID, Deployment: "region-a", Target: "payments"})
		require.NoError(t, err)
		_, err = store.db.ExecContext(ctx, `UPDATE apply_operations SET lease_owner = ?, lease_token = ? WHERE id = ?`, "driver-a", "tok-a", opID)
		require.NoError(t, err)
		_, err = store.db.ExecContext(ctx, `UPDATE tasks SET apply_operation_id = ? WHERE task_identifier = ?`, opID, "task_apply_fence_task_op")
		require.NoError(t, err)
		task := loadTask(t, store, "task_apply_fence_task_op")

		displacedCtx := storage.WithOperationLease(ctx, storage.OperationLease{ApplyID: apply.ID, OperationID: opID, Owner: "driver-a", Token: "tok-a"})
		task.State = state.Task.Failed
		err = writeDuringUncommittedSteal(t, store, waiting, "UPDATE tasks",
			func() error { return store.Tasks().Update(displacedCtx, task) },
			`UPDATE apply_operations SET lease_owner = ?, lease_token = ? WHERE id = ?`, "driver-b", "tok-b", opID)
		require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
		assert.Equal(t, state.Task.Pending, loadTask(t, store, task.TaskIdentifier).State, "a displaced driver must not write the task")

		ownerCtx := storage.WithOperationLease(ctx, storage.OperationLease{ApplyID: apply.ID, OperationID: opID, Owner: "driver-b", Token: "tok-b"})
		task.State = state.Task.Running
		require.NoError(t, store.Tasks().Update(ownerCtx, task))
		assert.Equal(t, state.Task.Running, loadTask(t, store, task.TaskIdentifier).State)
	})

	// The upsert's lease row is its only source row, so the fence gates both
	// arms: the insert of a comment the displaced driver never recorded, and
	// the conflict update of one it had.
	for _, tc := range []struct {
		name     string
		existing bool
	}{
		{name: "comment upsert insert under an apply lease"},
		{name: "comment upsert conflict update under an apply lease", existing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			store := newStore(t)
			lock := storagetest.CreateLock(t, store, "fence_comment_db", storage.DatabaseTypeMySQL)
			apply := storagetest.CreateClaimedApply(t, store, lock, "apply_fence_comment", 933, "driver-a")

			displacedCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: apply.ID, Owner: apply.LeaseOwner, Token: apply.LeaseToken})
			if tc.existing {
				require.NoError(t, store.ApplyComments().Upsert(displacedCtx, &storage.ApplyComment{
					ApplyID: apply.ID, CommentState: state.Comment.Progress, GitHubCommentID: 100,
				}))
			}

			err := writeDuringUncommittedSteal(t, store, waiting, "INSERT INTO apply_comments",
				func() error {
					return store.ApplyComments().Upsert(displacedCtx, &storage.ApplyComment{
						ApplyID: apply.ID, CommentState: state.Comment.Progress, GitHubCommentID: 200,
					})
				},
				`UPDATE applies SET lease_owner = ?, lease_token = ? WHERE id = ?`, "driver-b", "tok-b", apply.ID)
			require.ErrorIs(t, err, storage.ErrApplyLeaseLost)

			persisted, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
			require.NoError(t, err)
			if tc.existing {
				require.NotNil(t, persisted)
				assert.Equal(t, int64(100), persisted.GitHubCommentID, "a displaced driver must not repoint the tracked comment")
			} else {
				assert.Nil(t, persisted, "a displaced driver must not create the comment record")
			}

			ownerCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: apply.ID, Owner: "driver-b", Token: "tok-b"})
			require.NoError(t, store.ApplyComments().Upsert(ownerCtx, &storage.ApplyComment{
				ApplyID: apply.ID, CommentState: state.Comment.Progress, GitHubCommentID: 300,
			}))
			persisted, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
			require.NoError(t, err)
			require.NotNil(t, persisted)
			assert.Equal(t, int64(300), persisted.GitHubCommentID)
		})
	}
}

// writeDuringUncommittedSteal applies steal in its own transaction, runs write
// while that transaction is still open, commits the steal once write is
// blocked on the lease row's lock, and returns what write returned. A write
// that returns before the steal commits never waited on the lease row, so its
// token check was decided against its snapshot rather than the steal, and the
// test fails outright.
func writeDuringUncommittedSteal(t *testing.T, store *Storage, waiting lockWaiter, fragment string, write func() error, steal string, stealArgs ...any) error {
	t.Helper()
	stealTx, err := store.db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		// A committed steal reports ErrTxDone; one left open when the test
		// failed early was already rolled back when the test's context ended.
		err := stealTx.Rollback()
		if errors.Is(err, sql.ErrTxDone) || errors.Is(err, context.Canceled) {
			return
		}
		assert.NoError(t, err, "roll back the steal transaction")
	})
	_, err = stealTx.ExecContext(t.Context(), steal, stealArgs...)
	require.NoError(t, err)

	result := make(chan error, 1)
	go func() { result <- write() }()

	deadline := time.Now().Add(leaseStealRaceDeadline)
	for !waiting(t, fragment) {
		select {
		case err := <-result:
			require.FailNow(t, "the displaced write returned before the steal committed instead of waiting on the lease row", "write returned: %v", err)
		default:
		}
		require.True(t, time.Now().Before(deadline), "the displaced write never waited on the lease row")
		time.Sleep(10 * time.Millisecond)
	}
	require.NoError(t, stealTx.Commit())

	select {
	case err := <-result:
		return err
	case <-time.After(leaseStealRaceDeadline):
		require.FailNow(t, "the displaced write did not return after the steal committed")
		return nil
	}
}

// loadTask reads a task by identifier, failing the test when it is missing.
func loadTask(t *testing.T, store *Storage, identifier string) *storage.Task {
	t.Helper()
	task, err := store.Tasks().Get(t.Context(), identifier)
	require.NoError(t, err)
	require.NotNil(t, task)
	return task
}
