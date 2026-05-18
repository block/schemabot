package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
)

func TestListMysqlDatabases(t *testing.T) {
	svc := newMysqlInventoryTestService(&ServerConfig{
		Databases: map[string]DatabaseConfig{
			"app_vitess": {
				Type: storage.DatabaseTypeVitess,
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "vitess-dsn"},
				},
			},
			"orders": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"production": {DSN: "prod-dsn"},
					"staging":    {DSN: "staging-dsn"},
				},
			},
			"payments": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "payments-dsn"},
				},
			},
		},
	})
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/api/mysql/databases", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.NotContains(t, w.Body.String(), "prod-dsn")
	assert.NotContains(t, w.Body.String(), "staging-dsn")

	var resp apitypes.ListMysqlDatabasesResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err, "failed to decode response")

	require.Len(t, resp.Databases, 2)
	assert.Equal(t, 2, resp.Count)
	assert.Equal(t, "orders", resp.Databases[0].Database)
	assert.Equal(t, storage.DatabaseTypeMySQL, resp.Databases[0].DatabaseType)
	assert.Equal(t, "orders", resp.Databases[0].Deployment)
	assert.Equal(t, []string{"production", "staging"}, resp.Databases[0].Environments)
	assert.Equal(t, "payments", resp.Databases[1].Database)
	assert.Equal(t, []string{"staging"}, resp.Databases[1].Environments)
}

func TestListMysqlDatabasesEnvironmentFilter(t *testing.T) {
	svc := newMysqlInventoryTestService(&ServerConfig{
		Databases: map[string]DatabaseConfig{
			"orders": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"production": {DSN: "prod-dsn"},
					"staging":    {DSN: "staging-dsn"},
				},
			},
			"payments": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "payments-dsn"},
				},
			},
		},
	})
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/api/mysql/databases?environment=production", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp apitypes.ListMysqlDatabasesResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err, "failed to decode response")

	require.Len(t, resp.Databases, 1)
	assert.Equal(t, 1, resp.Count)
	assert.Equal(t, "orders", resp.Databases[0].Database)
	assert.Equal(t, []string{"production"}, resp.Databases[0].Environments)
}

func TestListMysqlDatabasesWithoutConfiguredDatabases(t *testing.T) {
	svc := newMysqlInventoryTestService(&ServerConfig{
		TernDeployments: TernConfig{
			"default": TernEndpoints{
				"staging": "localhost:9090",
			},
		},
	})
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), "GET", "/api/mysql/databases", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp apitypes.ListMysqlDatabasesResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err, "failed to decode response")

	assert.Empty(t, resp.Databases)
	assert.Zero(t, resp.Count)
}

func newMysqlInventoryTestService(config *ServerConfig) *Service {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorage{}, config, nil, logger)
}
