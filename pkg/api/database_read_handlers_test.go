package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// The database, deployment, and environment values a caller types into a read
// endpoint's path or query are identity strings, and the handlers fold them
// before any lookup. The configured-database map and the tern client registry
// are keyed by the canonical spelling, so an unfolded lookup misses a database
// the caller can plainly see in the server config; storage folds its own
// arguments, so the handler fold there is the ingress guard that also keeps
// the echoed database name canonical. Names that are not configured or have
// no applies keep their not-found and empty results.
func TestDatabaseReadHandlersCanonicalizePathAndQuery(t *testing.T) {
	applies := []*storage.Apply{
		{
			ID:              7,
			ApplyIdentifier: "apply-history",
			Database:        "testdb",
			DatabaseType:    storage.DatabaseTypeMySQL,
			Environment:     "staging",
		},
		{
			ID:              8,
			ApplyIdentifier: "apply-orders",
			Database:        "orders",
			DatabaseType:    storage.DatabaseTypeMySQL,
			Environment:     "staging",
		},
	}
	newServer := func() *http.ServeMux {
		svc := New(&mockStorageWithApplyStores{
			applies:   &staticApplyStore{applies: applies},
			applyLogs: &windowedApplyLogStore{available: 1},
		}, testServerConfig(), map[string]tern.Client{"default/staging": &mockTernClient{}}, slog.New(slog.DiscardHandler))
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)
		return mux
	}

	tests := []struct {
		name       string
		target     string
		wantCode   int
		assertBody func(*testing.T, []byte)
	}{
		{
			name:     "history path and environment",
			target:   "/api/history/TeStDb?environment=StAgInG",
			wantCode: http.StatusOK,
			assertBody: func(t *testing.T, body []byte) {
				var resp apitypes.DatabaseHistoryResponse
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "testdb", resp.Database)
				require.Len(t, resp.Applies, 1)
				assert.Equal(t, "apply-history", resp.Applies[0].ApplyID)
				assert.Equal(t, "staging", resp.Applies[0].Environment)
			},
		},
		{
			name:     "history for a database without applies",
			target:   "/api/history/UnKnOwN?environment=StAgInG",
			wantCode: http.StatusOK,
			assertBody: func(t *testing.T, body []byte) {
				var resp apitypes.DatabaseHistoryResponse
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "unknown", resp.Database)
				assert.Empty(t, resp.Applies)
			},
		},
		{
			name:     "environments path",
			target:   "/api/databases/TeStDb/environments",
			wantCode: http.StatusOK,
			assertBody: func(t *testing.T, body []byte) {
				var resp struct {
					Database     string   `json:"database"`
					Environments []string `json:"environments"`
				}
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "testdb", resp.Database)
				assert.Equal(t, []string{"staging"}, resp.Environments)
			},
		},
		{
			name:     "environments for an unconfigured database",
			target:   "/api/databases/OrDeRs/environments",
			wantCode: http.StatusNotFound,
			assertBody: func(t *testing.T, body []byte) {
				var resp apitypes.ErrorResponse
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp.Error, `no environments found for database "orders"`)
			},
		},
		{
			name:     "logs path and environment",
			target:   "/api/logs/TeStDb?environment=StAgInG",
			wantCode: http.StatusOK,
			assertBody: func(t *testing.T, body []byte) {
				var resp apitypes.LogsResponse
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "apply-history", resp.ApplyID)
				require.Len(t, resp.Logs, 1)
				assert.Equal(t, "entry 1", resp.Logs[0].Message)
			},
		},
		{
			name:     "logs for a database without applies",
			target:   "/api/logs/UnKnOwN?environment=StAgInG",
			wantCode: http.StatusOK,
			assertBody: func(t *testing.T, body []byte) {
				assert.JSONEq(t, `{"logs":[]}`, string(body))
			},
		},
		{
			name:     "tern health path segments",
			target:   "/tern-health/DeFaUlT/StAgInG",
			wantCode: http.StatusOK,
			assertBody: func(t *testing.T, body []byte) {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "ok", resp["status"])
				assert.Equal(t, "default", resp["deployment"])
				assert.Equal(t, "staging", resp["environment"])
			},
		},
		{
			name:     "tern health for an unconfigured deployment",
			target:   "/tern-health/UnKnOwN/StAgInG",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, tc.target, nil)
			w := httptest.NewRecorder()
			newServer().ServeHTTP(w, req)

			require.Equal(t, tc.wantCode, w.Code, w.Body.String())
			if tc.assertBody != nil {
				tc.assertBody(t, w.Body.Bytes())
			}
		})
	}
}

// A deployment typed on the status query reaches storage folded, so the
// filter matches the canonical deployment name recorded on the apply rows.
func TestHandleStatusCanonicalizesDeploymentAndEnvironment(t *testing.T) {
	applies := &recentApplyStore{}
	svc := New(&mockStorageWithApplyStores{applies: applies}, testServerConfig(), nil, slog.New(slog.DiscardHandler))
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status?environment=StAgInG&deployment=DePlOy-A", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, applies.filters, 1)
	assert.Equal(t, "staging", applies.filters[0].Environment)
	assert.Equal(t, "deploy-a", applies.filters[0].Deployment)
}

// A deployment typed on the logs query is matched against the canonical
// deployment name recorded on the apply's operations and used to resolve the
// tern client, so a differently-cased spelling reaches the same data plane.
func TestHandleLogsCanonicalizesDeployment(t *testing.T) {
	apply := &storage.Apply{ID: 13, ApplyIdentifier: "apply-remote", Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging"}
	operations := []*storage.ApplyOperation{
		{ApplyID: apply.ID, Deployment: "region-a", OperationKind: storage.ApplyOperationKindWork, Target: "cluster-a", ExternalID: "remote-a"},
	}
	client := &mockTernClient{isRemote: true}
	svc := New(&mockStorageWithApplyStores{
		applies:    &staticApplyStore{applies: []*storage.Apply{apply}},
		operations: &staticApplyOperationStore{operations: operations},
	}, testServerConfig(), map[string]tern.Client{"region-a/staging": client}, slog.New(slog.DiscardHandler))
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/logs?apply_id=apply-remote&deployment=ReGiOn-A", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, client.logsReqs, 1)
	assert.Equal(t, "remote-a", client.logsReqs[0].ApplyId)
}
