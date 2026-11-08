//go:build integration

package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/storagetest"
)

// applyRowLockReader lists the locks any open transaction holds on applies
// rows in the store's database, read from a session of its own.
type applyRowLockReader func(ctx context.Context) ([]string, error)

// mysqlApplyRowLocks reads InnoDB's lock table. A transaction that has locked
// or written an applies row holds at least the table's intention lock until
// it ends, so an empty result means no open transaction holds an apply row.
func mysqlApplyRowLocks(db *sql.DB) applyRowLockReader {
	return func(ctx context.Context) ([]string, error) {
		return queryLockDescriptions(ctx, db, `
			SELECT CONCAT(LOCK_TYPE, ' ', LOCK_MODE, ' held by thread ', THREAD_ID)
			FROM performance_schema.data_locks
			WHERE OBJECT_SCHEMA = DATABASE() AND OBJECT_NAME = 'applies'
		`)
	}
}

// postgresApplyRowLocks reads pg_locks for the table-level lock that every
// row lock and row write on applies takes alongside it and holds until the
// transaction ends: ROW SHARE for SELECT ... FOR UPDATE / FOR SHARE, and ROW
// EXCLUSIVE for UPDATE, INSERT, and DELETE.
func postgresApplyRowLocks(db *sql.DB) applyRowLockReader {
	return func(ctx context.Context) ([]string, error) {
		return queryLockDescriptions(ctx, db, `
			SELECT mode || ' held by backend ' || pid
			FROM pg_locks
			WHERE locktype = 'relation'
				AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
				AND relation = 'applies'::regclass
				AND mode IN ('RowShareLock', 'RowExclusiveLock')
				AND granted
		`)
	}
}

func queryLockDescriptions(ctx context.Context, db *sql.DB, query string) ([]string, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("read apply row locks: %w", err)
	}
	defer utils.CloseAndLog(rows)
	var held []string
	for rows.Next() {
		var description string
		if err := rows.Scan(&description); err != nil {
			return nil, fmt.Errorf("scan apply row lock: %w", err)
		}
		held = append(held, description)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read apply row locks: %w", err)
	}
	return held, nil
}

// lockOrderCheckingLocker wraps the store's advisory locker. Before each apply
// target lock is taken it reads whether any transaction holds an apply row,
// and refuses the lock if one does, so a path that locks an apply row before
// the target lock fails with an error instead of passing or deadlocking. The
// read sees every session, so it is only meaningful while one caller at a
// time uses the store, which is how the lock-order test drives it.
type lockOrderCheckingLocker struct {
	namedlock.Locker
	heldApplyRowLocks applyRowLockReader

	mu       sync.Mutex
	acquired []string
}

func (l *lockOrderCheckingLocker) Acquire(ctx context.Context, conn *sql.Conn, name string, wait time.Duration) (bool, error) {
	if strings.HasPrefix(name, "schemabot_apply_") {
		held, err := l.heldApplyRowLocks(ctx)
		if err != nil {
			return false, fmt.Errorf("check apply row locks before taking %s: %w", name, err)
		}
		if len(held) > 0 {
			return false, fmt.Errorf("apply target lock %s requested while apply row locks are held (%s): OW-5 requires the target lock first", name, strings.Join(held, "; "))
		}
		l.mu.Lock()
		l.acquired = append(l.acquired, name)
		l.mu.Unlock()
	}
	return l.Locker.Acquire(ctx, conn, name, wait)
}

// takeAcquired returns the apply target locks taken since the last call.
func (l *lockOrderCheckingLocker) takeAcquired() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	acquired := l.acquired
	l.acquired = nil
	return acquired
}

// TestApplyTargetLockPrecedesApplyRowLocks runs the lock-order check against
// MySQL; TestPostgresStorageParity runs the same check against PostgreSQL.
func TestApplyTargetLockPrecedesApplyRowLocks(t *testing.T) {
	testApplyTargetLockPrecedesApplyRowLocks(t, func(t *testing.T, locker namedlock.Locker) *Storage {
		t.Helper()
		clearTables(t)
		dialect := MySQLDialect{}
		return NewWithDependencies(Dependencies{
			DB: testDB, Binder: dialect, Dialect: dialect, Identity: dialect,
			Locker: locker, Classifier: NewMySQLErrorClassifier(),
		})
	}, namedlock.MySQL{}, mysqlApplyRowLocks(testDB))
}

// testApplyTargetLockPrecedesApplyRowLocks drives every entry point that takes
// the apply target lock (applyTargetLockAcquirers) on its locking path, one at
// a time, and fails if any of them requests the target lock while a
// transaction holds an apply row (OW-5). Holding a row while waiting for the
// lock can deadlock against a lock holder that needs the same row, which the
// start/takeover race test shows for one interleaving; this check fails
// deterministically for any path, without needing the race to happen.
func testApplyTargetLockPrecedesApplyRowLocks(t *testing.T, newStore func(t *testing.T, locker namedlock.Locker) *Storage, baseLocker namedlock.Locker, heldApplyRowLocks applyRowLockReader) {
	locker := &lockOrderCheckingLocker{Locker: baseLocker, heldApplyRowLocks: heldApplyRowLocks}
	store := newStore(t, locker)
	ctx := t.Context()

	t.Run("the check sees a held apply row", func(t *testing.T) {
		lock := storagetest.CreateLock(t, store, "lock_order_probe_db", storage.DatabaseTypeMySQL)
		apply := storagetest.CreateApplyWithStateEnvDeployment(t, store, lock, "apply_lock_order_probe", 9700, state.Apply.Failed, "staging", "default")
		tx, err := store.db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer rollbackTx(ctx, tx, "lock order probe")
		var id int64
		require.NoError(t, tx.QueryRowContext(ctx, `SELECT id FROM applies WHERE id = ? FOR UPDATE`, apply.ID).Scan(&id))

		held, err := heldApplyRowLocks(ctx)
		require.NoError(t, err)
		assert.NotEmpty(t, held, "a transaction holding an apply row must be visible to the lock-order check")
		err = store.Applies().WithTargetHeld(ctx, lock.DatabaseName, lock.DatabaseType, "staging", func(context.Context) error { return nil })
		require.ErrorContains(t, err, "requested while apply row locks are held", "the check refuses the target lock while an apply row is held")
	})

	cases := map[string]func(t *testing.T, lock *storage.Lock){
		"applyStore.Create": func(t *testing.T, lock *storage.Lock) {
			_, err := store.Applies().Create(ctx, lockOrderApply(lock, "apply_lock_order_create"))
			require.NoError(t, err)
		},
		"applyStore.CreateWithTasks": func(t *testing.T, lock *storage.Lock) {
			apply := lockOrderApply(lock, "apply_lock_order_create_tasks")
			_, err := store.Applies().CreateWithTasks(ctx, apply, []*storage.Task{lockOrderTask(apply, "task_lock_order_create_tasks", "")})
			require.NoError(t, err)
		},
		"applyStore.CreateWithTasksAndOperations": func(t *testing.T, lock *storage.Lock) {
			apply := lockOrderApply(lock, "apply_lock_order_create_ops")
			_, err := store.Applies().CreateWithTasksAndOperations(ctx, apply,
				[]*storage.Task{lockOrderTask(apply, "task_lock_order_create_ops", "")},
				[]*storage.ApplyOperation{{Deployment: "default", OperationKey: "default", State: state.ApplyOperation.Pending}})
			require.NoError(t, err)
		},
		"applyStore.CreateWithGroupedOperations": func(t *testing.T, lock *storage.Lock) {
			apply := lockOrderApply(lock, "apply_lock_order_create_grouped")
			_, err := store.Applies().CreateWithGroupedOperations(ctx, apply, []*storage.ApplyOperationWithTasks{{
				Operation: &storage.ApplyOperation{Deployment: "default", Target: lock.DatabaseName, State: state.ApplyOperation.Pending},
				Tasks:     []*storage.Task{lockOrderTask(apply, "task_lock_order_create_grouped", "")},
			}})
			require.NoError(t, err)
		},
		"applyStore.AttachOperationWithTasks": func(t *testing.T, lock *storage.Lock) {
			apply := lockOrderApply(lock, "apply_lock_order_attach")
			id, err := store.Applies().CreateWithTasksAndOperations(ctx, apply,
				[]*storage.Task{lockOrderTask(apply, "task_lock_order_attach_first", "-80")},
				[]*storage.ApplyOperation{{Deployment: "default", OperationKey: lock.DatabaseName + "/-80/users", Target: lock.DatabaseName, State: state.ApplyOperation.Pending}})
			require.NoError(t, err)
			apply.ID = id
			locker.takeAcquired()

			sibling := &storage.ApplyOperation{Deployment: "default", OperationKey: lock.DatabaseName + "/80-/users", Target: lock.DatabaseName, State: state.ApplyOperation.Pending}
			require.NoError(t, store.Applies().AttachOperationWithTasks(ctx, apply, sibling, []*storage.Task{lockOrderTask(apply, "task_lock_order_attach_sibling", "80-")}))
		},
		"applyStore.Update": func(t *testing.T, lock *storage.Lock) {
			apply := storagetest.CreateApplyWithStateEnvDeployment(t, store, lock, "apply_lock_order_update", 9701, state.Apply.Pending, "staging", "default")
			locker.takeAcquired()

			apply.State = state.Apply.Running
			require.NoError(t, store.Applies().Update(ctx, apply))
		},
		"applyStore.ClaimApplyByID": func(t *testing.T, lock *storage.Lock) {
			apply := storagetest.CreateApplyWithStateEnvDeployment(t, store, lock, "apply_lock_order_claim", 9702, state.Apply.Stopped, "staging", "default")
			_, alreadyPending, err := store.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
				ApplyID:   apply.ID,
				Operation: storage.ControlOperationStart,
				Status:    storage.ControlRequestPending,
				Metadata:  []byte(`{}`),
			})
			require.NoError(t, err)
			require.False(t, alreadyPending)
			locker.takeAcquired()

			claimed, err := store.Applies().ClaimApplyByID(ctx, apply.ID, "driver-lock-order")
			require.NoError(t, err)
			require.NotNil(t, claimed, "the stopped apply with a pending start is claimed under the target lock")
		},
		"applyStore.WithExclusiveTarget": func(t *testing.T, lock *storage.Lock) {
			apply := storagetest.CreateApplyWithStateEnvDeployment(t, store, lock, "apply_lock_order_exclusive", 9703, state.Apply.Pending, "staging", "default")
			locker.takeAcquired()

			require.NoError(t, store.Applies().WithExclusiveTarget(ctx, apply, func(context.Context) error { return nil }))
		},
		"applyStore.WithTargetHeld": func(t *testing.T, lock *storage.Lock) {
			require.NoError(t, store.Applies().WithTargetHeld(ctx, lock.DatabaseName, lock.DatabaseType, "staging", func(context.Context) error { return nil }))
		},
	}

	names := make([]string, 0, len(cases))
	for name := range cases {
		names = append(names, name)
	}
	slices.Sort(names)
	listed := slices.Sorted(slices.Values(applyTargetLockAcquirers))
	require.Equal(t, listed, names, "every entry in applyTargetLockAcquirers must be driven here")

	for i, name := range names {
		t.Run(name, func(t *testing.T) {
			lock := storagetest.CreateLock(t, store, fmt.Sprintf("lock_order_db_%d", i), storage.DatabaseTypeMySQL)
			locker.takeAcquired()

			cases[name](t, lock)

			assert.Contains(t, locker.takeAcquired(), applyTargetLockName(lock.DatabaseName, lock.DatabaseType, "staging"),
				"%s must take the apply target lock on the path this case drives", name)
		})
	}
}

// lockOrderApply returns an active apply on the lock's staging target.
func lockOrderApply(lock *storage.Lock, identifier string) *storage.Apply {
	return &storage.Apply{
		ApplyIdentifier: identifier,
		LockID:          lock.ID,
		PlanID:          9710,
		Database:        lock.DatabaseName,
		DatabaseType:    lock.DatabaseType,
		Repository:      lock.Repository,
		PullRequest:     lock.PullRequest,
		Environment:     "staging",
		Deployment:      "default",
		Engine:          storage.EngineForType(lock.DatabaseType),
		State:           state.Apply.Pending,
	}
}

// lockOrderTask returns a pending task for the apply on the given shard.
func lockOrderTask(apply *storage.Apply, identifier, shard string) *storage.Task {
	task := attachRaceTask(apply, identifier)
	task.Shard = shard
	return task
}
