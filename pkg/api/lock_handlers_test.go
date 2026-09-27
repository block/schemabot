package api

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

	acquire := func(t *testing.T, cfg *ServerConfig, user *auth.User) *storage.Lock {
		t.Helper()
		store := &capturingLockStore{}
		svc := New(&mockStorageWithApplyStores{locks: store}, cfg, nil, logger)
		rec := scopedDenialRequest(t, svc.handleLockAcquire, user, http.MethodPost, "/api/locks/acquire", body)
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

		lock := acquire(t, cfg, &auth.User{Subject: "bob", Groups: []string{"payments-team", "payments-oncall", "unrelated"}})
		require.NotNil(t, lock.Acquirer)
		assert.Equal(t, "bob", lock.Acquirer.Subject)
		assert.Equal(t, []string{"payments-oncall", "payments-team"}, lock.Acquirer.OperatorGroups)
	})

	t.Run("admin outside every operator group records no groups", func(t *testing.T) {
		lock := acquire(t, scopedWriteConfig(), &auth.User{Subject: "alice", Groups: []string{"schema-admins"}})
		require.NotNil(t, lock.Acquirer)
		assert.Equal(t, "alice", lock.Acquirer.Subject)
		assert.NotNil(t, lock.Acquirer.OperatorGroups, "a recorded acquirer with no groups is an empty list, not an unrecorded one")
		assert.Empty(t, lock.Acquirer.OperatorGroups)
	})

	t.Run("admin who is also an operator records the operator group", func(t *testing.T) {
		lock := acquire(t, scopedWriteConfig(), &auth.User{Subject: "carol", Groups: []string{"schema-admins", "payments-team"}})
		require.NotNil(t, lock.Acquirer)
		assert.Equal(t, "carol", lock.Acquirer.Subject)
		assert.Equal(t, []string{"payments-team"}, lock.Acquirer.OperatorGroups)
	})

	t.Run("deployment with no scoped operator grants records no acquirer", func(t *testing.T) {
		lock := acquire(t, testServerConfig(), &auth.User{Subject: "dave", Groups: []string{"payments-team"}})
		assert.Nil(t, lock.Acquirer)
	})
}
