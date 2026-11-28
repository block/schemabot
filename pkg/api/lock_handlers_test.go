package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/storage"
)

type capturingLockStore struct {
	storage.LockStore
	acquired        *storage.Lock
	releaseDatabase string
	releaseType     string
	releaseOwner    string
	getDatabase     string
	getType         string
}

func (s *capturingLockStore) Acquire(_ context.Context, lock *storage.Lock) error {
	s.acquired = lock
	return nil
}

func (s *capturingLockStore) Release(_ context.Context, database, dbType, owner string) error {
	s.releaseDatabase = database
	s.releaseType = dbType
	s.releaseOwner = owner
	return nil
}

func (s *capturingLockStore) Get(_ context.Context, database, dbType string) (*storage.Lock, error) {
	s.getDatabase = database
	s.getType = dbType
	if s.acquired != nil {
		stored := *s.acquired
		return &stored, nil
	}
	return &storage.Lock{DatabaseName: database, DatabaseType: dbType, Owner: "Org/Repo#42"}, nil
}

func newLockTestServer(locks storage.LockStore) *http.ServeMux {
	svc := New(&mockStorageWithApplyStores{locks: locks}, testServerConfig(), nil, slog.New(slog.DiscardHandler))
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	return mux
}

func TestLockHandlersCanonicalizeIdentityKeys(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		target     string
		body       string
		assertCall func(*testing.T, *capturingLockStore)
	}{
		{
			name:   "acquire body",
			method: http.MethodPost,
			target: "/api/locks/acquire",
			body:   `{"database":"TeStDb","database_type":"MySQL","owner":"Org/Repo#42","repository":"Org/Repo"}`,
			assertCall: func(t *testing.T, store *capturingLockStore) {
				require.NotNil(t, store.acquired)
				assert.Equal(t, "testdb", store.acquired.DatabaseName)
				assert.Equal(t, "mysql", store.acquired.DatabaseType)
				assert.Equal(t, "org/repo", store.acquired.Repository)
				assert.Equal(t, "org/repo#42", store.acquired.Owner,
					"the owner is a byte-exact match token at release; it must fold at acquire so a differently-cased release still matches")
			},
		},
		{
			name:   "release body",
			method: http.MethodDelete,
			target: "/api/locks",
			body:   `{"database":"TeStDb","database_type":"MySQL","owner":"Org/Repo#42"}`,
			assertCall: func(t *testing.T, store *capturingLockStore) {
				assert.Equal(t, "testdb", store.releaseDatabase)
				assert.Equal(t, "mysql", store.releaseType)
				assert.Equal(t, "org/repo#42", store.releaseOwner,
					"the owner must fold at release to match the folded spelling stored at acquire")
			},
		},
		{
			name:   "get path",
			method: http.MethodGet,
			target: "/api/locks/TeStDb/PostgreSQL",
			assertCall: func(t *testing.T, store *capturingLockStore) {
				assert.Equal(t, "testdb", store.getDatabase)
				assert.Equal(t, "postgresql", store.getType)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &capturingLockStore{}
			req := httptest.NewRequestWithContext(t.Context(), tc.method, tc.target, strings.NewReader(tc.body))
			w := httptest.NewRecorder()
			newLockTestServer(store).ServeHTTP(w, req)

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			tc.assertCall(t, store)
		})
	}
}

// A lock acquired through the API records the verified caller behind it and
// every operator group of the locked database that caller belongs to, so a
// later release can tell whose grant the lock falls under. The owner string
// the caller sends plays no part in it.
func TestLockAcquireRecordsAcquirer(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	body := `{"database":"payments","database_type":"mysql","owner":"cli:someone-else@laptop"}`

	acquire := func(t *testing.T, cfg *ServerConfig, user *auth.User, verified bool) *storage.Lock {
		t.Helper()
		store := &capturingLockStore{}
		svc := New(&mockStorageWithApplyStores{locks: store}, cfg, nil, logger)
		ctx := auth.WithUser(t.Context(), user)
		if verified {
			ctx = auth.WithVerifiedUser(t.Context(), user)
		}
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/locks/acquire", strings.NewReader(body))
		rec := httptest.NewRecorder()
		svc.handleLockAcquire(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.NotNil(t, store.acquired)
		assert.Equal(t, "cli:someone-else@laptop", store.acquired.Owner)
		assert.NotContains(t, rec.Body.String(), user.Subject,
			"the acquirer is recorded, not returned on the lock read surface")
		return store.acquired
	}

	t.Run("scoped operator records every operator group they hold on the database", func(t *testing.T) {
		cfg := scopedWriteConfig()
		payments := cfg.Databases["payments"]
		payments.OperatorGroups = []string{"payments-team", "payments-oncall"}
		cfg.Databases["payments"] = payments

		lock := acquire(t, cfg, &auth.User{Subject: "bob", Groups: []string{"payments-team", "payments-oncall", "unrelated"}}, true)
		require.NotNil(t, lock.Acquirer)
		assert.Equal(t, "bob", lock.Acquirer.Subject)
		assert.Equal(t, []string{"payments-oncall", "payments-team"}, lock.Acquirer.OperatorGroups)
	})

	t.Run("admin outside every operator group records no groups", func(t *testing.T) {
		lock := acquire(t, scopedWriteConfig(), &auth.User{Subject: "alice", Groups: []string{"schema-admins"}}, true)
		require.NotNil(t, lock.Acquirer)
		assert.Equal(t, "alice", lock.Acquirer.Subject)
		assert.NotNil(t, lock.Acquirer.OperatorGroups, "a recorded acquirer with no groups is an empty list, not an unrecorded one")
		assert.Empty(t, lock.Acquirer.OperatorGroups)
	})

	t.Run("admin who is also an operator records the operator group", func(t *testing.T) {
		lock := acquire(t, scopedWriteConfig(), &auth.User{Subject: "carol", Groups: []string{"schema-admins", "payments-team"}}, true)
		require.NotNil(t, lock.Acquirer)
		assert.Equal(t, "carol", lock.Acquirer.Subject)
		assert.Equal(t, []string{"payments-team"}, lock.Acquirer.OperatorGroups)
	})

	t.Run("deployment with no scoped operator grants records no acquirer", func(t *testing.T) {
		lock := acquire(t, testServerConfig(), &auth.User{Subject: "dave", Groups: []string{"payments-team"}}, true)
		assert.Nil(t, lock.Acquirer)
	})

	t.Run("unverified identity records no acquirer", func(t *testing.T) {
		lock := acquire(t, scopedWriteConfig(), &auth.User{Subject: "claimed", Groups: []string{"schema-admins", "payments-team"}}, false)
		assert.Nil(t, lock.Acquirer)
	})
}

// memoryLockStore holds a single lock row with the storage layer's release
// semantics, so a handler test can observe whether a release deleted it.
// beforeReleaseByID runs between the handler's read of the lock and its pinned
// delete, to stage a lock replaced in between.
type memoryLockStore struct {
	storage.LockStore
	lock              *storage.Lock
	beforeReleaseByID func(*memoryLockStore)
	// beforeAcquire runs at the start of an acquire, to stage a lock taken
	// under the same owner before this acquire's insert.
	beforeAcquire func(*memoryLockStore)
	// afterAcquire runs once an acquire has succeeded, to stage a release, a
	// replacement, or a failing read between the acquire and the handler's
	// read of the lock.
	afterAcquire func(*memoryLockStore)
	// getError, when set, fails every read of the lock.
	getError error
	lastID   int64
	// applyUsingLock, when set, is an apply that can still change and records
	// its LockID as its lock, so ReleaseByIDIfUnused keeps that row.
	applyUsingLock *storage.Apply
}

func (s *memoryLockStore) holds(database, dbType string) bool {
	return s.lock != nil && s.lock.DatabaseName == database && s.lock.DatabaseType == dbType
}

// Acquire has the storage layer's acquire semantics for a request carrying no
// pending plan: it creates the row when none is held, reporting the new row's
// ID on lock, succeeds without writing anything when the same owner already
// holds it, and refuses any other owner.
func (s *memoryLockStore) Acquire(_ context.Context, lock *storage.Lock) error {
	if s.beforeAcquire != nil {
		s.beforeAcquire(s)
	}
	if s.holds(lock.DatabaseName, lock.DatabaseType) {
		if s.lock.Owner != lock.Owner {
			return storage.ErrLockHeld
		}
	} else {
		s.lastID++
		lock.ID = s.lastID
		created := *lock
		s.lock = &created
	}
	if s.afterAcquire != nil {
		s.afterAcquire(s)
	}
	return nil
}

func (s *memoryLockStore) Get(_ context.Context, database, dbType string) (*storage.Lock, error) {
	if s.getError != nil {
		return nil, s.getError
	}
	if !s.holds(database, dbType) {
		return nil, nil
	}
	stored := *s.lock
	return &stored, nil
}

func (s *memoryLockStore) Release(_ context.Context, database, dbType, owner string) error {
	if !s.holds(database, dbType) {
		return storage.ErrLockNotFound
	}
	if s.lock.Owner != owner {
		return storage.ErrLockNotOwned
	}
	s.lock = nil
	return nil
}

func (s *memoryLockStore) ReleaseByID(_ context.Context, id int64, database, dbType, owner, pendingPlanID string) error {
	if s.beforeReleaseByID != nil {
		s.beforeReleaseByID(s)
	}
	if !s.holds(database, dbType) {
		return storage.ErrLockNotFound
	}
	if s.lock.ID != id {
		return storage.ErrLockReplaced
	}
	if s.lock.Owner != owner {
		return storage.ErrLockNotOwned
	}
	if s.lock.PendingPlanID != pendingPlanID {
		return storage.ErrLockIntentChanged
	}
	s.lock = nil
	return nil
}

// ForceRelease releases whichever lock is held, whoever owns it.
func (s *memoryLockStore) ForceRelease(_ context.Context, database, dbType string) error {
	if !s.holds(database, dbType) {
		return storage.ErrLockNotFound
	}
	s.lock = nil
	return nil
}

// ForceReleaseByID releases the row id names whoever owns it, and reports the
// row held now when a different one holds the lock.
func (s *memoryLockStore) ForceReleaseByID(_ context.Context, id int64, database, dbType string) error {
	if !s.holds(database, dbType) {
		return storage.ErrLockNotFound
	}
	if s.lock.ID != id {
		return &storage.LockReplacedError{LockID: id, HeldLockID: s.lock.ID, HeldOwner: s.lock.Owner}
	}
	s.lock = nil
	return nil
}

// ReleaseByIDIfUnused releases like ReleaseByID, and keeps the lock while
// applyUsingLock, an apply that can still change, records the row as its lock.
func (s *memoryLockStore) ReleaseByIDIfUnused(_ context.Context, id int64, database, dbType, owner, pendingPlanID string) error {
	if !s.holds(database, dbType) {
		return storage.ErrLockNotFound
	}
	if s.lock.ID != id {
		return storage.ErrLockReplaced
	}
	if s.lock.Owner != owner {
		return storage.ErrLockNotOwned
	}
	if s.lock.PendingPlanID != pendingPlanID {
		return storage.ErrLockIntentChanged
	}
	if s.applyUsingLock != nil && s.applyUsingLock.LockID == id {
		return &storage.LockInUseError{LockID: id, Apply: s.applyUsingLock}
	}
	s.lock = nil
	return nil
}

// paymentsLock is a lock on the payments database under the given owner,
// recorded as acquired by acquirer.
func paymentsLock(id int64, owner string, acquirer *storage.LockAcquirer) *storage.Lock {
	return &storage.Lock{
		ID:           id,
		DatabaseName: "payments",
		DatabaseType: "mysql",
		Repository:   "org/payments-service",
		Owner:        owner,
		Acquirer:     acquirer,
	}
}

// A scoped operator may release a lock only when its recorded acquirer shared
// one of the database's operator groups with them. The owner string is
// readable by anyone who can list locks, so sending it proves nothing: a lock
// taken by another group, by a caller in no operator group, or with no
// recorded acquirer at all stays held and the caller gets a 403 naming who may
// release it. Deployment write-group members release by owner as before.
func TestScopedLockReleaseIsPerOperatorGroup(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	cfg := scopedWriteConfig()
	payments := cfg.Databases["payments"]
	payments.OperatorGroups = []string{"payments-team", "payments-oncall"}
	cfg.Databases["payments"] = payments

	const owner = "cli:bob@laptop"
	body := `{"database":"payments","database_type":"mysql","owner":"` + owner + `"}`
	teamAcquirer := &storage.LockAcquirer{Subject: "bob", OperatorGroups: []string{"payments-team"}}

	release := func(t *testing.T, locks *memoryLockStore, user *auth.User) *httptest.ResponseRecorder {
		t.Helper()
		svc := New(&mockStorageWithApplyStores{locks: locks}, cfg, nil, logger)
		return scopedDenialRequest(t, svc.handleLockRelease, user, http.MethodDelete, "/api/locks", body)
	}

	t.Run("a teammate in the acquirer's operator group releases it", func(t *testing.T) {
		locks := &memoryLockStore{lock: paymentsLock(7, owner, teamAcquirer)}
		rec := release(t, locks, &auth.User{Subject: "carol", Groups: []string{"payments-team"}})

		assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Nil(t, locks.lock, "the lock is released")
	})

	t.Run("groups compare by configured name, so an org-qualified caller group matches", func(t *testing.T) {
		locks := &memoryLockStore{lock: paymentsLock(7, owner, teamAcquirer)}
		rec := release(t, locks, &auth.User{Subject: "carol", Groups: []string{"example-org/payments-team"}})

		assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Nil(t, locks.lock, "the lock is released")
	})

	t.Run("an operator in another of the database's groups is refused", func(t *testing.T) {
		locks := &memoryLockStore{lock: paymentsLock(7, owner, teamAcquirer)}
		rec := release(t, locks, &auth.User{Subject: "mallory", Groups: []string{"payments-oncall"}})

		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Contains(t, rec.Body.String(), "acquired under operator groups (payments-team)")
		assert.Contains(t, rec.Body.String(), "schema-admins", "the denial names the write groups that may release it")
		assert.NotContains(t, rec.Body.String(), apitypes.ErrCodeLockNotOwned,
			"the CLI must show the grant denial, not report the lock as held under another owner")
		require.NotNil(t, locks.lock, "a refused release leaves the lock held")
		assert.Equal(t, int64(7), locks.lock.ID)
	})

	t.Run("a lock with no recorded acquirer is refused", func(t *testing.T) {
		locks := &memoryLockStore{lock: paymentsLock(7, owner, nil)}
		rec := release(t, locks, &auth.User{Subject: "bob", Groups: []string{"payments-team"}})

		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Contains(t, rec.Body.String(), `the lock is held under owner \"`+owner+`\", the owner you sent, but records no verified acquirer`)
		assert.Contains(t, rec.Body.String(), "schema-admins")
		assert.NotNil(t, locks.lock, "a refused release leaves the lock held")
	})

	t.Run("a lock acquired by a caller in no operator group is refused", func(t *testing.T) {
		locks := &memoryLockStore{lock: paymentsLock(7, owner, &storage.LockAcquirer{Subject: "alice", OperatorGroups: []string{}})}
		rec := release(t, locks, &auth.User{Subject: "bob", Groups: []string{"payments-team"}})

		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Contains(t, rec.Body.String(), "none of the database's operator groups")
		assert.NotNil(t, locks.lock, "a refused release leaves the lock held")
	})

	t.Run("a wrong owner is refused as not owned before the grant is consulted", func(t *testing.T) {
		locks := &memoryLockStore{lock: paymentsLock(7, "cli:someone-else@desktop", teamAcquirer)}
		rec := release(t, locks, &auth.User{Subject: "mallory", Groups: []string{"payments-oncall"}})

		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Contains(t, rec.Body.String(), apitypes.ErrCodeLockNotOwned,
			"a mistyped owner reads as not owned whatever the caller's group, so the CLI maps it to ErrLockNotOwned")
		assert.NotContains(t, rec.Body.String(), "acquired under operator groups",
			"the grant is not consulted for a lock the caller did not name")
		assert.NotNil(t, locks.lock, "a refused release leaves the lock held")
	})

	t.Run("a lock replaced after the check is left held", func(t *testing.T) {
		otherTeam := paymentsLock(8, owner, &storage.LockAcquirer{Subject: "erin", OperatorGroups: []string{"payments-oncall"}})
		locks := &memoryLockStore{
			lock:              paymentsLock(7, owner, teamAcquirer),
			beforeReleaseByID: func(s *memoryLockStore) { s.lock = otherTeam },
		}
		rec := release(t, locks, &auth.User{Subject: "carol", Groups: []string{"payments-team"}})

		assert.Equal(t, http.StatusConflict, rec.Code)
		assert.Contains(t, rec.Body.String(), "released and acquired again")
		require.NotNil(t, locks.lock, "the new lock must survive a release decided against the one it replaced")
		assert.Equal(t, int64(8), locks.lock.ID)
	})

	t.Run("a lock pinned to a pending plan is released", func(t *testing.T) {
		held := paymentsLock(7, owner, teamAcquirer)
		held.PendingPlanID = "plan-1"
		locks := &memoryLockStore{lock: held}
		rec := release(t, locks, &auth.User{Subject: "carol", Groups: []string{"payments-team"}})

		assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Nil(t, locks.lock, "the checked row and its pending plan are released")
	})

	t.Run("a lock re-pinned to a new plan after the check is left held", func(t *testing.T) {
		held := paymentsLock(7, owner, teamAcquirer)
		held.PendingPlanID = "plan-1"
		locks := &memoryLockStore{
			lock: held,
			beforeReleaseByID: func(s *memoryLockStore) {
				refreshed := *s.lock
				refreshed.PendingPlanID = "plan-2"
				s.lock = &refreshed
			},
		}
		rec := release(t, locks, &auth.User{Subject: "carol", Groups: []string{"payments-team"}})

		assert.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), "released and acquired again")
		require.NotNil(t, locks.lock, "the lock re-pinned to a new plan stays held")
		assert.Equal(t, int64(7), locks.lock.ID)
		assert.Equal(t, "plan-2", locks.lock.PendingPlanID)
	})

	t.Run("a lock released after the check reports it missing", func(t *testing.T) {
		locks := &memoryLockStore{
			lock:              paymentsLock(7, owner, teamAcquirer),
			beforeReleaseByID: func(s *memoryLockStore) { s.lock = nil },
		}
		rec := release(t, locks, &auth.User{Subject: "carol", Groups: []string{"payments-team"}})

		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("a deployment write-group member releases any group's lock by owner", func(t *testing.T) {
		locks := &memoryLockStore{lock: paymentsLock(7, owner, nil)}
		rec := release(t, locks, &auth.User{Subject: "alice", Groups: []string{"schema-admins"}})

		assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Nil(t, locks.lock, "the lock is released")
	})

	t.Run("a deployment write-group member still needs the owner", func(t *testing.T) {
		locks := &memoryLockStore{lock: paymentsLock(7, owner, nil)}
		svc := New(&mockStorageWithApplyStores{locks: locks}, cfg, nil, logger)
		rec := scopedDenialRequest(t, svc.handleLockRelease, &auth.User{Subject: "alice", Groups: []string{"schema-admins"}},
			http.MethodDelete, "/api/locks", `{"database":"payments","database_type":"mysql","owner":"cli:alice@laptop"}`)

		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Contains(t, rec.Body.String(), apitypes.ErrCodeLockNotOwned)
		assert.NotNil(t, locks.lock)
	})
}

// A scoped operator who sends the owner string of a lock that is already held
// is told they hold it only when the lock's recorded acquirer shared one of the
// database's operator groups with them, the same rule that decides who may
// release it. The owner string is readable by anyone who can list locks, so a
// caller from another group, or any scoped caller facing a lock with no
// recorded acquirer, gets a 403 naming who holds it, and the lock is left
// exactly as it was. Re-acquiring your own group's lock stays idempotent, and
// deployment write-group members re-acquire by owner as before.
func TestScopedLockReacquireIsPerOperatorGroup(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	cfg := scopedWriteConfig()
	payments := cfg.Databases["payments"]
	payments.OperatorGroups = []string{"payments-team", "payments-oncall"}
	cfg.Databases["payments"] = payments

	const owner = "cli:bob@laptop"
	body := `{"database":"payments","database_type":"mysql","owner":"` + owner + `","repository":"org/payments-service"}`
	teamAcquirer := &storage.LockAcquirer{Subject: "bob", OperatorGroups: []string{"payments-team"}}
	bob := &auth.User{Subject: "bob", Groups: []string{"payments-team"}}

	acquire := func(t *testing.T, locks *memoryLockStore, user *auth.User) *httptest.ResponseRecorder {
		t.Helper()
		svc := New(&mockStorageWithApplyStores{locks: locks}, cfg, nil, logger)
		return scopedDenialRequest(t, svc.handleLockAcquire, user, http.MethodPost, "/api/locks/acquire", body)
	}
	// acquireUnverified sends the acquire as user without a verified identity,
	// so the request records no acquirer.
	acquireUnverified := func(t *testing.T, locks *memoryLockStore, user *auth.User) *httptest.ResponseRecorder {
		t.Helper()
		svc := New(&mockStorageWithApplyStores{locks: locks}, cfg, nil, logger)
		ctx := auth.WithUser(t.Context(), user)
		req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/locks/acquire", strings.NewReader(body))
		rec := httptest.NewRecorder()
		svc.handleLockAcquire(rec, req)
		return rec
	}
	assertUnchanged := func(t *testing.T, locks *memoryLockStore, want *storage.Lock) {
		t.Helper()
		require.NotNil(t, locks.lock, "a refused re-acquire leaves the lock held")
		assert.Equal(t, want, locks.lock, "a refused re-acquire changes nothing on the lock")
	}

	t.Run("an operator in another of the database's groups is refused", func(t *testing.T) {
		held := paymentsLock(7, owner, teamAcquirer)
		locks := &memoryLockStore{lock: paymentsLock(7, owner, teamAcquirer)}
		rec := acquire(t, locks, &auth.User{Subject: "mallory", Groups: []string{"payments-oncall"}})

		assert.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), "lock_acquire on database")
		assert.Contains(t, rec.Body.String(), "acquired under operator groups (payments-team)")
		assert.Contains(t, rec.Body.String(), "schema-admins", "the denial names the write groups that may release it")
		assertUnchanged(t, locks, held)
	})

	t.Run("a lock with no recorded acquirer is refused and named as held under the caller's own owner", func(t *testing.T) {
		held := paymentsLock(7, owner, nil)
		locks := &memoryLockStore{lock: paymentsLock(7, owner, nil)}
		rec := acquire(t, locks, bob)

		assert.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), `the lock is held under owner \"`+owner+`\", the owner you sent, but records no verified acquirer`)
		assert.Contains(t, rec.Body.String(), "schema-admins", "the denial names the write groups that may release it")
		assertUnchanged(t, locks, held)
	})

	t.Run("a lock acquired by a caller in no operator group is refused", func(t *testing.T) {
		admin := &storage.LockAcquirer{Subject: "alice", OperatorGroups: []string{}}
		held := paymentsLock(7, owner, admin)
		locks := &memoryLockStore{lock: paymentsLock(7, owner, admin)}
		rec := acquire(t, locks, bob)

		assert.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), "none of the database's operator groups")
		assertUnchanged(t, locks, held)
	})

	t.Run("the acquirer re-acquiring their own lock succeeds without changing it", func(t *testing.T) {
		held := paymentsLock(7, owner, teamAcquirer)
		locks := &memoryLockStore{lock: paymentsLock(7, owner, teamAcquirer)}
		rec := acquire(t, locks, bob)

		assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), owner)
		assert.Equal(t, held, locks.lock)
	})

	t.Run("a teammate in the acquirer's operator group re-acquires it", func(t *testing.T) {
		locks := &memoryLockStore{lock: paymentsLock(7, owner, teamAcquirer)}
		rec := acquire(t, locks, &auth.User{Subject: "carol", Groups: []string{"payments-team"}})

		assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Equal(t, int64(7), locks.lock.ID)
	})

	t.Run("a lock taken by another group while this acquire ran is refused", func(t *testing.T) {
		otherTeam := paymentsLock(8, owner, &storage.LockAcquirer{Subject: "erin", OperatorGroups: []string{"payments-oncall"}})
		locks := &memoryLockStore{beforeAcquire: func(s *memoryLockStore) {
			taken := *otherTeam
			s.lock = &taken
		}}
		rec := acquire(t, locks, bob)

		assert.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), "acquired under operator groups (payments-oncall)")
		assertUnchanged(t, locks, otherTeam)
	})

	t.Run("an unverified operator is refused before taking any lock", func(t *testing.T) {
		// Without a verified identity the lock could record no acquirer, so
		// the operator taking it could neither release it nor take it again.
		locks := &memoryLockStore{}
		rec := acquireUnverified(t, locks, bob)

		assert.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), `lock_acquire on database \"payments\": your identity is not verified`)
		assert.Contains(t, rec.Body.String(), "this request took no lock")
		assert.Contains(t, rec.Body.String(), "schema-admins", "the refusal names the write groups that can take the lock")
		assert.Contains(t, rec.Body.String(), "ask a member of a deployment write group to run the command for you")
		assert.Nil(t, locks.lock, "a refused acquire takes no lock")
	})

	t.Run("an unverified operator re-sending their own group's lock owner is refused with the lock unchanged", func(t *testing.T) {
		// The groups of an unverified caller are only claimed, so they do not
		// decide whether the caller holds the lock; the refusal says only
		// that this request took none, which stays true while the lock is
		// held.
		held := paymentsLock(7, owner, teamAcquirer)
		locks := &memoryLockStore{lock: paymentsLock(7, owner, teamAcquirer)}
		rec := acquireUnverified(t, locks, bob)

		assert.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), `lock_acquire on database \"payments\": your identity is not verified`)
		assert.Contains(t, rec.Body.String(), "this request took no lock")
		assertUnchanged(t, locks, held)
	})

	t.Run("a lock released before the acquire could be read back is not reported held", func(t *testing.T) {
		locks := &memoryLockStore{afterAcquire: func(s *memoryLockStore) { s.lock = nil }}
		rec := acquire(t, locks, bob)

		assert.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), "released before this acquire could be confirmed")
		assert.Nil(t, locks.lock)
	})

	t.Run("a lock this acquire created and then lost to another group before it was read back is refused", func(t *testing.T) {
		otherTeam := paymentsLock(99, owner, &storage.LockAcquirer{Subject: "erin", OperatorGroups: []string{"payments-oncall"}})
		locks := &memoryLockStore{afterAcquire: func(s *memoryLockStore) {
			taken := *otherTeam
			s.lock = &taken
		}}
		rec := acquire(t, locks, bob)

		assert.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), "acquired under operator groups (payments-oncall)")
		assertUnchanged(t, locks, otherTeam)
	})

	t.Run("a lock this acquire created that cannot be read back is not reported held and is named to the caller", func(t *testing.T) {
		locks := &memoryLockStore{afterAcquire: func(s *memoryLockStore) { s.getError = errors.New("storage unavailable") }}
		rec := acquire(t, locks, bob)

		assert.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), "was acquired but could not be read back, so it is not reported held")
		assert.Contains(t, rec.Body.String(), `the lock may be held under owner \"`+owner+`\"`)
		assert.Contains(t, rec.Body.String(), "release it under that owner")
		assert.NotContains(t, rec.Body.String(), `"lock":`, "a lock that could not be read back is not reported held")
		require.NotNil(t, locks.lock, "the created lock is left held")
		assert.Equal(t, owner, locks.lock.Owner)
	})

	t.Run("a lock already held under this owner that cannot be read back is not reported held", func(t *testing.T) {
		held := paymentsLock(7, owner, teamAcquirer)
		locks := &memoryLockStore{
			lock:         paymentsLock(7, owner, teamAcquirer),
			afterAcquire: func(s *memoryLockStore) { s.getError = errors.New("storage unavailable") },
		}
		rec := acquire(t, locks, bob)

		assert.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), "internal error")
		assert.NotContains(t, rec.Body.String(), owner, "a lock that could not be read back is not described to the caller")
		assertUnchanged(t, locks, held)
	})

	t.Run("a deployment write-group member re-acquires any group's lock by owner", func(t *testing.T) {
		locks := &memoryLockStore{lock: paymentsLock(7, owner, nil)}
		rec := acquire(t, locks, &auth.User{Subject: "alice", Groups: []string{"schema-admins"}})

		assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Equal(t, int64(7), locks.lock.ID)
	})
}

// Holding a lock by owner string alone, to re-acquire or release it, is the
// exception, not the default: only the deployment write groups and a
// deployment with no scoped grants get it. An allow reason the lock paths have
// not been taught about is held to the recorded acquirer, so a new grant cannot
// hold a lock by owner until someone decides it should.
func TestHoldsLockByOwnerAloneIsAnAllowlist(t *testing.T) {
	assert.True(t, holdsLockByOwnerAlone(DirectWriteReasonAdminAllow))
	assert.True(t, holdsLockByOwnerAlone(DirectWriteReasonScopedLaneDisabled))
	assert.False(t, holdsLockByOwnerAlone(DirectWriteReasonScopedAllow))
	assert.False(t, holdsLockByOwnerAlone("service_allow"), "an unknown allow reason is held to the recorded acquirer")
	assert.False(t, holdsLockByOwnerAlone(""))
}

// A scoped operator's acquire refused because no verified acquirer can be
// recorded is counted under its own status on the lock operation counter, and
// logged with the subject the caller claimed and why no acquirer could be
// recorded, so a break-glass refusal can be found and attributed from
// telemetry alone.
func TestUnverifiedScopedLockAcquireRefusalIsCountedAndLogged(t *testing.T) {
	reader := installManualMetricReader(t)
	var logs bytes.Buffer
	svc := New(&mockStorageWithApplyStores{locks: &memoryLockStore{}}, scopedWriteConfig(), nil,
		slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx := auth.WithUser(t.Context(), &auth.User{Subject: "bob", Groups: []string{"payments-team"}})
	body := `{"database":"payments","database_type":"mysql","owner":"cli:bob@laptop","repository":"org/payments-service"}`
	rec := httptest.NewRecorder()
	svc.handleLockAcquire(rec, httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/locks/acquire", strings.NewReader(body)))
	require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())

	points := collectCounterPoints(t, reader, "schemabot.lock_operations_total")
	require.Len(t, points, 1)
	assert.Equal(t, int64(1), points[0].Value)
	assert.Equal(t, "acquire", attributeValue(t, points[0], "operation"))
	assert.Equal(t, "payments", attributeValue(t, points[0], "database"))
	assert.Equal(t, "no_acquirer", attributeValue(t, points[0], "status"))

	var entry map[string]any
	require.NoError(t, json.Unmarshal(logs.Bytes(), &entry), logs.String())
	assert.Equal(t, "WARN", entry["level"])
	assert.Equal(t, "scoped lock acquire refused: there is no verified acquirer to record, so the caller could neither release nor re-acquire the lock; this request took no lock", entry["msg"])
	assert.Equal(t, "bob", entry["claimed_subject"])
	assert.Equal(t, DirectWriteReasonUnverifiedIdentity, entry["reason"])
	assert.Equal(t, "payments", entry["database"])
	assert.Equal(t, "mysql", entry["database_type"])
	assert.Equal(t, "cli:bob@laptop", entry["owner"])
	assert.Equal(t, "org/payments-service", entry["repository"])
}

// rowIDLockStore acquires the way storage does: insertedID is the ID the
// acquire assigns to a row it inserts, zero when the owner already held the
// lock, and the row read back afterwards carries storedID.
type rowIDLockStore struct {
	storage.LockStore
	insertedID int64
	storedID   int64
}

func (s *rowIDLockStore) Acquire(_ context.Context, lock *storage.Lock) error {
	lock.ID = s.insertedID
	return nil
}

func (s *rowIDLockStore) Get(_ context.Context, database, dbType string) (*storage.Lock, error) {
	return &storage.Lock{ID: s.storedID, DatabaseName: database, DatabaseType: dbType, Owner: "cli:bob@laptop"}, nil
}

// An acquire says whether it took the lock or found it already held under the
// same owner, so the CLI releases only a lock it took when its schema change
// is never submitted. A row another request inserted under the same owner
// while this acquire ran is one this acquire found.
func TestLockAcquireReportsWhetherItCreatedTheLock(t *testing.T) {
	tests := []struct {
		name       string
		insertedID int64
		storedID   int64
		created    bool
	}{
		{name: "the acquire inserted the row read back", insertedID: 7, storedID: 7, created: true},
		{name: "the owner already held the lock", insertedID: 0, storedID: 7, created: false},
		{name: "another request's row replaced the one this acquire inserted", insertedID: 7, storedID: 9, created: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &rowIDLockStore{insertedID: tc.insertedID, storedID: tc.storedID}
			body := `{"database":"payments","database_type":"mysql","owner":"cli:bob@laptop"}`
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/locks/acquire", strings.NewReader(body))
			rec := httptest.NewRecorder()
			newLockTestServer(store).ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			var resp LockResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.NotNil(t, resp.Lock)
			assert.Equal(t, "payments", resp.Lock.Database)
			assert.Equal(t, tc.storedID, resp.Lock.ID, "the answer names the lock row the caller holds, so a release can name it back")
			assert.Equal(t, tc.created, resp.Created)
		})
	}
}

// A lock request body holds one JSON value. Anything after it but white space
// is refused before anything changes, so a second object carrying "force", or
// a value followed by stray text, is never acted on as if only its first part
// had been sent.
func TestLockRequestsRejectDataAfterTheJSONValue(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	admin := &auth.User{Subject: "alice", Groups: []string{"schema-admins"}}
	const ownerRelease = `{"database":"payments","database_type":"mysql","owner":"cli:bob@laptop"}`

	release := func(t *testing.T, body string) (*httptest.ResponseRecorder, *memoryLockStore) {
		t.Helper()
		locks := &memoryLockStore{lock: paymentsLock(7, "cli:bob@laptop", nil)}
		svc := New(&mockStorageWithApplyStores{locks: locks}, scopedWriteConfig(), nil, logger)
		return scopedDenialRequest(t, svc.handleLockRelease, admin, http.MethodDelete, "/api/locks", body), locks
	}

	refused := []struct {
		name string
		body string
	}{
		{name: "a second object", body: ownerRelease + `{"force":true}`},
		{name: "a second object on its own line", body: ownerRelease + "\n" + `{"force":true}`},
		{name: "stray text", body: `{"database":"payments","database_type":"mysql","force":true} xx`},
		{name: "a value that is cut short", body: ownerRelease + `{"force":`},
	}
	for _, tt := range refused {
		t.Run("release followed by "+tt.name+" is refused", func(t *testing.T) {
			rec, locks := release(t, tt.body)

			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), "invalid request body: the body must hold one JSON value, but more data follows it")
			assert.Equal(t, paymentsLock(7, "cli:bob@laptop", nil), locks.lock, "the lock stays held")
		})
	}

	t.Run("release followed by white space is carried out", func(t *testing.T) {
		rec, locks := release(t, ownerRelease+" \r\n\t")

		assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Nil(t, locks.lock, "the lock is released")
	})

	t.Run("acquire followed by a second object is refused", func(t *testing.T) {
		locks := &memoryLockStore{}
		svc := New(&mockStorageWithApplyStores{locks: locks}, scopedWriteConfig(), nil, logger)
		rec := scopedDenialRequest(t, svc.handleLockAcquire, admin, http.MethodPost, "/api/locks/acquire",
			`{"database":"payments","database_type":"mysql","owner":"cli:alice@laptop"}{"repository":"org/payments-service"}`)

		assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), "more data follows it")
		assert.Nil(t, locks.lock, "no lock is taken")
	})
}

// A body that runs past the request body limit after its JSON value is
// answered as too large, with the limit, the same as one that runs past it
// inside the value.
func TestLockReleaseBodyOverTheLimitAfterItsJSONValueIsTooLarge(t *testing.T) {
	svc := newTestService()
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	body := `{"database":"payments","database_type":"mysql","owner":"cli:bob@laptop"}` + strings.Repeat(" ", maxAPIRequestBodyBytes)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodDelete, "/api/locks", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusRequestEntityTooLarge, w.Code, w.Body.String())
	assert.Contains(t, w.Body.String(), "request body exceeds the 32 MiB limit")
}

// A misspelt field in a lock request is refused before anything changes. A
// tolerant decode would drop "forcce" and fall through to an owner release
// the caller did not ask for, or take a lock without the field they meant.
func TestLockRequestsRejectUnknownFields(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	admin := &auth.User{Subject: "alice", Groups: []string{"schema-admins"}}

	t.Run("release", func(t *testing.T) {
		held := paymentsLock(7, "cli:bob@laptop", nil)
		locks := &memoryLockStore{lock: paymentsLock(7, "cli:bob@laptop", nil)}
		svc := New(&mockStorageWithApplyStores{locks: locks}, scopedWriteConfig(), nil, logger)
		rec := scopedDenialRequest(t, svc.handleLockRelease, admin, http.MethodDelete, "/api/locks",
			`{"database":"payments","database_type":"mysql","forcce":true}`)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), `unknown field \"forcce\"`)
		assert.Equal(t, held, locks.lock, "the lock stays held")
	})

	t.Run("acquire", func(t *testing.T) {
		locks := &memoryLockStore{}
		svc := New(&mockStorageWithApplyStores{locks: locks}, scopedWriteConfig(), nil, logger)
		rec := scopedDenialRequest(t, svc.handleLockAcquire, admin, http.MethodPost, "/api/locks/acquire",
			`{"database":"payments","database_type":"mysql","owner":"cli:alice@laptop","repo":"org/payments-service"}`)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), `unknown field \"repo\"`)
		assert.Nil(t, locks.lock, "no lock is taken")
	})
}
