//go:build integration

package serve

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// A server configured with storage.dialect: postgres boots its storage stack
// end to end against a fresh PostgreSQL database: the dialect routes schema
// bootstrapping, the connection pool, and the storage implementation
// together, so a settings roundtrip works through the assembled store. The
// MySQL lane (the unset default) is exercised by the e2e suite, which boots
// the full server through this same path.
func TestConnectStoragePostgresBootsEndToEnd(t *testing.T) {
	dsn, assertDB := testutil.StartPostgres(t, "schemabot")
	logger := slog.New(slog.DiscardHandler)

	cfg := &api.ServerConfig{
		Storage: api.StorageConfig{DSN: dsn, Dialect: "postgres"},
		Databases: map[string]api.DatabaseConfig{
			"appdb": {
				Type: "mysql",
				Environments: map[string]api.EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/appdb"},
				},
			},
		},
	}
	require.NoError(t, cfg.Validate())

	dialect, err := cfg.Storage.ResolveDialect()
	require.NoError(t, err)
	require.Equal(t, schema.DialectPostgres, dialect)

	db, err := connectStorage(t.Context(), cfg, dialect, logger)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })

	require.True(t, testutil.PostgresTableExists(t, assertDB, "public", "settings"),
		"storage schema must be bootstrapped before the store is built")

	store, err := newStore(dialect, db)
	require.NoError(t, err)

	require.NoError(t, store.Settings().Set(t.Context(), "boot-check", "ok"))
	setting, err := store.Settings().Get(t.Context(), "boot-check")
	require.NoError(t, err)
	require.NotNil(t, setting)
	assert.Equal(t, "ok", setting.Value)
}

// A dialect without a registered store or connector fails closed instead of
// falling back to the MySQL implementation.
func TestStorageDialectDispatchFailsClosed(t *testing.T) {
	_, err := newStore(schema.Dialect("oracle"), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `no storage implementation for storage dialect "oracle"`)

	cfg := &api.ServerConfig{Storage: api.StorageConfig{DSN: "unused"}}
	_, err = openStoragePool(schema.Dialect("oracle"), "unused", cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `no storage connector for storage dialect "oracle"`)
}
