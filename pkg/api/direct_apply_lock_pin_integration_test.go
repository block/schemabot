//go:build integration

package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// directApplyLockPinDeadline bounds each wait in the direct apply lock pin
// tests: a statement reaching its row lock, and a request finishing once the
// row it waits on is free.
const directApplyLockPinDeadline = 15 * time.Second

const directApplyLockOwner = "cli:alice@laptop"

// directApplyLockPinFixture is a CLI lock on testapp, a stored CLI plan for
// testapp's one production target, and the service that applies it.
type directApplyLockPinFixture struct {
	db   *sql.DB
	st   storage.Storage
	lock *storage.Lock
}

func newDirectApplyLockPinFixture(t *testing.T) directApplyLockPinFixture {
	t.Helper()
	ctx := t.Context()
	db, err := sql.Open("block-mysql", newStorageDatabaseWithSchema(t).DSN)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { _ = db.Close() })
	st := mysqlstore.New(db)

	_, err = st.Plans().Create(ctx, &storage.Plan{
		PlanIdentifier: "plan-direct-apply",
		Database:       "testapp",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "eu",
		Target:         "testapp-001",
		Environment:    "production",
		Namespaces: map[string]*storage.NamespacePlanData{"testapp": {Tables: []storage.TableChange{
			{Namespace: "testapp", Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}}},
		CreatedAt: time.Now(),
	})
	require.NoError(t, err)

	require.NoError(t, st.Locks().Acquire(ctx, &storage.Lock{
		DatabaseName: "testapp",
		DatabaseType: storage.DatabaseTypeMySQL,
		Owner:        directApplyLockOwner,
	}))
	lock, err := st.Locks().Get(ctx, "testapp", storage.DatabaseTypeMySQL)
	require.NoError(t, err)
	require.NotNil(t, lock)
	return directApplyLockPinFixture{db: db, st: st, lock: lock}
}

// newService builds a service over store, which may wrap the fixture's.
func (f directApplyLockPinFixture) newService(t *testing.T, store storage.Storage) *Service {
	t.Helper()
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"testapp": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"production": {Deployment: "eu", Targets: targetNames("testapp-001")},
				},
			},
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(store, cfg, map[string]tern.Client{"eu/production": &mockTernClient{isRemote: true}}, logger)
}

// postDirectApply bounds the request by directApplyLockPinDeadline, so a
// handler stalled in a database call fails the test instead of waiting for the
// package timeout.
func postDirectApply(ctx context.Context, svc *Service) *httptest.ResponseRecorder {
	ctx, cancel := context.WithTimeout(ctx, directApplyLockPinDeadline)
	defer cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/apply",
		strings.NewReader(`{"plan_id":"plan-direct-apply","environment":"production","caller":"cli:alice@laptop"}`))
	rec := httptest.NewRecorder()
	svc.handleApply(rec, req)
	return rec
}

// statementWaitsOnRowLock reports whether a statement whose text contains
// fragment is waiting for an InnoDB row lock.
func statementWaitsOnRowLock(t *testing.T, db *sql.DB, fragment string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), directApplyLockPinDeadline)
	defer cancel()
	var waiting int
	require.NoError(t, db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM performance_schema.data_lock_waits w
		JOIN performance_schema.threads th ON th.THREAD_ID = w.REQUESTING_THREAD_ID
		WHERE th.PROCESSLIST_INFO LIKE ?
	`, "%"+fragment+"%").Scan(&waiting))
	return waiting > 0
}

// An operator runs `schemabot apply` under the lock they took, and while the
// server is creating the apply someone runs a release that checks for applies
// under the lock. The apply's creation holds the lock row, so the release
// waits for it, then sees the new apply and keeps the lock instead of deleting
// it from under the apply.
func TestHandleApply_DirectApplyHoldsItsLockRowAgainstRelease(t *testing.T) {
	ctx := t.Context()
	f := newDirectApplyLockPinFixture(t)
	svc := f.newService(t, f.st)
	t.Cleanup(func() { utils.CloseAndLog(svc) })

	// Park the apply's creation inside its transaction: it share-locks the
	// environment's freeze epoch after locking the lock row it records.
	_, err := f.st.Freezes().Epoch(ctx, "production")
	require.NoError(t, err)
	parked, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := parked.Rollback()
		if errors.Is(err, sql.ErrTxDone) || errors.Is(err, context.Canceled) {
			return
		}
		assert.NoError(t, err, "roll back the freeze epoch holder")
	})
	var epoch int64
	require.NoError(t, parked.QueryRowContext(ctx, `SELECT epoch FROM freeze_epochs WHERE environment = ? FOR UPDATE`, "production").Scan(&epoch))

	applied := make(chan *httptest.ResponseRecorder, 1)
	go func() { applied <- postDirectApply(ctx, svc) }()
	require.Eventually(t, func() bool { return statementWaitsOnRowLock(t, f.db, "freeze_epochs") },
		directApplyLockPinDeadline, 20*time.Millisecond, "the apply's creation reaches the freeze epoch")

	released := make(chan error, 1)
	go func() {
		released <- f.st.Locks().ReleaseByIDIfUnused(ctx, f.lock.ID, "testapp", storage.DatabaseTypeMySQL, directApplyLockOwner, "")
	}()
	require.Eventually(t, func() bool { return statementWaitsOnRowLock(t, f.db, "FROM locks") },
		directApplyLockPinDeadline, 20*time.Millisecond, "the release waits on the lock row the apply's creation holds")
	select {
	case err := <-released:
		require.FailNow(t, "the release decided while the apply's creation held the lock row", "err = %v", err)
	default:
	}

	require.NoError(t, parked.Rollback())
	var rec *httptest.ResponseRecorder
	select {
	case rec = <-applied:
	case <-time.After(directApplyLockPinDeadline):
		require.FailNow(t, "the apply did not finish after the freeze epoch was free")
	}
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var resp apitypes.ApplyResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.True(t, resp.Accepted, resp.ErrorMessage)
	apply, err := f.st.Applies().GetByApplyIdentifier(ctx, resp.ApplyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	assert.Equal(t, f.lock.ID, apply.LockID)

	select {
	case err := <-released:
		var inUse *storage.LockInUseError
		require.ErrorAs(t, err, &inUse, "the release sees the apply committed while it waited")
		assert.Equal(t, resp.ApplyID, inUse.Apply.ApplyIdentifier)
		assert.Equal(t, f.lock.ID, inUse.LockID)
	case <-time.After(directApplyLockPinDeadline):
		require.FailNow(t, "the release did not finish after the apply was created")
	}
	current, err := f.st.Locks().Get(ctx, "testapp", storage.DatabaseTypeMySQL)
	require.NoError(t, err)
	require.NotNil(t, current, "the lock the new apply records stays held")
	assert.Equal(t, f.lock.ID, current.ID)
}

// lockLostAfterReadStorage releases the lock right after apply creation reads
// it, and acquires it again under a new row when reacquire is set.
type lockLostAfterReadStorage struct {
	storage.Storage
	reacquire bool
}

func (s *lockLostAfterReadStorage) Locks() storage.LockStore {
	return lockLostAfterReadLocks{LockStore: s.Storage.Locks(), reacquire: s.reacquire}
}

type lockLostAfterReadLocks struct {
	storage.LockStore
	reacquire bool
}

func (l lockLostAfterReadLocks) Get(ctx context.Context, database, dbType string) (*storage.Lock, error) {
	lock, err := l.LockStore.Get(ctx, database, dbType)
	if err != nil || lock == nil {
		return lock, err
	}
	if err := l.ForceRelease(ctx, database, dbType); err != nil {
		return nil, err
	}
	if l.reacquire {
		if err := l.Acquire(ctx, &storage.Lock{DatabaseName: database, DatabaseType: dbType, Owner: lock.Owner}); err != nil {
			return nil, err
		}
	}
	return lock, nil
}

// An operator runs `schemabot apply` under the lock they took, and the lock is
// released, or released and taken again, after the server read it and before
// it created the apply. The apply is refused and nothing is stored, so no
// apply runs under a lock row that no longer exists.
func TestHandleApply_DirectApplyRefusedWhenLockReleasedBeforeCreate(t *testing.T) {
	for _, tc := range []struct {
		name      string
		reacquire bool
		wantError string
	}{
		{
			name:      "released",
			reacquire: false,
			wantError: `apply rejected: the lock on database "testapp" was released while this apply was being queued, so nothing was queued; take the lock again, then apply again`,
		},
		{
			name:      "released and acquired again",
			reacquire: true,
			wantError: "apply rejected: the lock on database \"testapp\" was released and taken again while this apply was being queued, so nothing was queued; check who holds it with `schemabot locks`, then apply again",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			f := newDirectApplyLockPinFixture(t)
			svc := f.newService(t, &lockLostAfterReadStorage{Storage: f.st, reacquire: tc.reacquire})
			t.Cleanup(func() { utils.CloseAndLog(svc) })

			rec := postDirectApply(ctx, svc)
			require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
			var resp apitypes.ErrorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tc.wantError, resp.Error)
			assert.Equal(t, apitypes.ErrCodeLockChanged, resp.ErrorCode)

			var applies int
			require.NoError(t, f.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM applies`).Scan(&applies))
			assert.Zero(t, applies, "a refused apply stores no row")

			current, err := f.st.Locks().Get(ctx, "testapp", storage.DatabaseTypeMySQL)
			require.NoError(t, err)
			if !tc.reacquire {
				assert.Nil(t, current)
				return
			}
			require.NotNil(t, current, "the lock acquired again stays held")
			assert.NotEqual(t, f.lock.ID, current.ID)
		})
	}
}
