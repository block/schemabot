package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// databaseApplyStore answers GetRecent by database and environment, the way
// storage does, so a request for one database never sees another's applies.
type databaseApplyStore struct {
	storage.ApplyStore
	applies []*storage.Apply
	// lookupErr fails the database-wide existence lookup (no environment,
	// a limit of one) while page reads still succeed.
	lookupErr error
}

func (s *databaseApplyStore) GetRecent(_ context.Context, filter storage.RecentAppliesFilter) ([]*storage.Apply, error) {
	if s.lookupErr != nil && filter.Environment == "" && filter.Limit == 1 {
		return nil, s.lookupErr
	}
	var out []*storage.Apply
	for _, apply := range s.applies {
		if filter.Database != "" && apply.Database != filter.Database {
			continue
		}
		if filter.Environment != "" && apply.Environment != filter.Environment {
			continue
		}
		out = append(out, apply)
	}
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

func (s *databaseApplyStore) GetByApplyIdentifier(_ context.Context, applyIdentifier string) (*storage.Apply, error) {
	for _, apply := range s.applies {
		if apply.ApplyIdentifier == applyIdentifier {
			return apply, nil
		}
	}
	return nil, nil
}

// unknownScopeApplies records one completed apply for "orders" in staging.
// The test server configures only "testdb", so "orders" stands for a database
// this server once ran and no longer configures.
func unknownScopeApplies() []*storage.Apply {
	now := time.Now().UTC()
	return []*storage.Apply{{
		ID:              1,
		ApplyIdentifier: "apply-orders",
		Database:        "orders",
		Environment:     "staging",
		Deployment:      "primary",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Completed,
		CreatedAt:       now,
		UpdatedAt:       now,
	}}
}

func newUnknownScopeServer(t *testing.T, applies *databaseApplyStore) *http.ServeMux {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{applies: applies}, testServerConfig(), nil, logger)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	return mux
}

// A history request for a database or environment that does not exist on this
// server answers 404 and names what to check, instead of an empty page that
// reads as "no schema changes yet". A database the server no longer
// configures keeps its recorded history, and a configured environment with no
// applies is still an empty page.
func TestHandleDatabaseHistoryTellsAnUnknownScopeFromAnEmptyOne(t *testing.T) {
	cases := []struct {
		name     string
		database string
		query    url.Values
		wantCode int
		wantErr  string
	}{
		{
			name:     "a database neither configured nor recorded",
			database: "ordres",
			query:    url.Values{"environment": {"staging"}},
			wantCode: http.StatusNotFound,
			wantErr:  `database "ordres" is not configured on this server and has no recorded applies. Check the name against the databases list`,
		},
		{
			name:     "a database neither configured nor recorded, any environment",
			database: "ordres",
			query:    url.Values{},
			wantCode: http.StatusNotFound,
			wantErr:  `database "ordres" is not configured on this server and has no recorded applies. Check the name against the databases list`,
		},
		{
			name:     "an environment the configured database does not have",
			database: "testdb",
			query:    url.Values{"environment": {"prodution"}},
			wantCode: http.StatusNotFound,
			wantErr:  `database "testdb" has no environment "prodution". Its environments are staging`,
		},
		{
			name:     "a configured environment with no applies",
			database: "testdb",
			query:    url.Values{"environment": {"staging"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "an unconfigured database with recorded applies in another environment",
			database: "orders",
			query:    url.Values{"environment": {"production"}},
			wantCode: http.StatusOK,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mux := newUnknownScopeServer(t, &databaseApplyStore{applies: unknownScopeApplies()})
			code, resp, errResp := getHistoryPage(t, mux, tc.database, tc.query)
			require.Equal(t, tc.wantCode, code, errResp.Error)
			if tc.wantCode == http.StatusOK {
				assert.Empty(t, resp.Applies)
				assert.False(t, resp.HasMore)
				return
			}
			assert.Equal(t, tc.wantErr, errResp.Error)
		})
	}
}

// When the lookup that tells an unknown database from an empty history fails,
// the request still answers with the empty page, which is true: no applies are
// recorded for it.
func TestHandleDatabaseHistoryAnswersTheEmptyPageWhenTheLookupFails(t *testing.T) {
	applies := &databaseApplyStore{applies: unknownScopeApplies(), lookupErr: errors.New("connection reset")}
	mux := newUnknownScopeServer(t, applies)

	code, resp, errResp := getHistoryPage(t, mux, "ordres", url.Values{"environment": {"staging"}})
	require.Equal(t, http.StatusOK, code, errResp.Error)
	assert.Empty(t, resp.Applies)
}

// A logs request that names a database in the path and an apply of another
// database is refused, so the caller never reads one database's logs under
// another's name.
func TestHandleLogsRefusesAnApplyOfAnotherDatabase(t *testing.T) {
	mux := newUnknownScopeServer(t, &databaseApplyStore{applies: unknownScopeApplies()})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/logs/billing?apply_id=apply-orders", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	var errResp apitypes.ErrorResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errResp))
	assert.Equal(t, "apply apply-orders is for database orders, not billing; drop the database from the path or use that apply's database", errResp.Error)
}
