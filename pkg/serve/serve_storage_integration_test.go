//go:build integration

package serve

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/postgresconn"
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

// The long-lived storage pool carries SchemaBot's own statement budget rather
// than the platform's. This is the connection the budget rides for the whole
// process lifetime, and losing it is silent: the pool would quietly revert to
// whatever the hosted provider tuned for API queries, with nothing failing
// until a storage query is cancelled in production. So the assertion is made
// against the live session, not against the config that fed it.
func TestConnectStoragePostgresPoolCarriesStatementBudget(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)

	for _, tc := range []struct {
		name       string
		configured string
		want       string
	}{
		{name: "default applies when unset", want: "30s"},
		{name: "configured value reaches the pool", configured: "17s", want: "17s"},
		{name: "explicit zero disables the budget", configured: "0", want: "0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dsn, adminDB := testutil.StartPostgres(t, "schemabot")

			// Every case runs against a hostile database default, so what the
			// pool reports can only have come from SchemaBot writing it. On a
			// fresh container the server reports 0 with no budget set at all,
			// which is indistinguishable from an explicit disable — the zero
			// case would assert nothing.
			_, err := adminDB.ExecContext(t.Context(),
				`ALTER DATABASE schemabot SET statement_timeout = '50ms'`)
			require.NoError(t, err)

			baseline, err := postgresconn.Open(dsn)
			require.NoError(t, err)
			t.Cleanup(func() { utils.CloseAndLog(baseline) })
			var inherited string
			require.NoError(t, baseline.QueryRowContext(t.Context(), "SHOW statement_timeout").Scan(&inherited))
			require.Equal(t, "50ms", inherited, "the hostile default must reach a fresh session")

			cfg := &api.ServerConfig{
				Storage:  api.StorageConfig{DSN: dsn, Dialect: "postgres"},
				Postgres: api.PostgresConfig{StatementTimeout: tc.configured},
			}
			db, err := connectStorage(t.Context(), cfg, schema.DialectPostgres, logger)
			require.NoError(t, err)
			t.Cleanup(func() { utils.CloseAndLog(db) })

			var inForce string
			require.NoError(t, db.QueryRowContext(t.Context(), "SHOW statement_timeout").Scan(&inForce))
			assert.Equal(t, tc.want, inForce)
		})
	}
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
