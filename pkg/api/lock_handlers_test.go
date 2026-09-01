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

	"github.com/block/schemabot/pkg/storage"
)

type capturingLockStore struct {
	storage.LockStore
	acquired        *storage.Lock
	releaseDatabase string
	releaseType     string
	getDatabase     string
	getType         string
}

func (s *capturingLockStore) Acquire(_ context.Context, lock *storage.Lock) error {
	s.acquired = lock
	return nil
}

func (s *capturingLockStore) Release(_ context.Context, database, dbType, _ string) error {
	s.releaseDatabase = database
	s.releaseType = dbType
	return nil
}

func (s *capturingLockStore) Get(_ context.Context, database, dbType string) (*storage.Lock, error) {
	s.getDatabase = database
	s.getType = dbType
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
				assert.Equal(t, "Org/Repo#42", store.acquired.Owner)
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
