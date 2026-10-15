//go:build integration

package serve

import (
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/storage"
	"github.com/testcontainers/testcontainers-go"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/tern"
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

	db, _, err := connectStorage(t.Context(), cfg, dialect, logger)
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
			db, _, err := connectStorage(t.Context(), cfg, schema.DialectPostgres, logger)
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
	_, err = openStoragePool(schema.Dialect("oracle"), "unused", cfg, slog.New(slog.DiscardHandler))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `no storage connector for storage dialect "oracle"`)
}

// Build carries local hosting from the option onto the server, so the adapter
// the server registers refuses a destructive convergence when the local
// runtime hosts it (AZ-6).
//
// The wiring under test only exists on a server Build returned, and Build
// bootstraps storage before it assembles one, so this needs a real database.
// Both servers are built against the same one: the option is then the only
// difference between a request that is refused and the same request honored.
func TestBuildCarriesLocalHostingToTheStorageSchemaAdapter(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "schemabot")
	logger := slog.New(slog.DiscardHandler)
	newConfig := func() *api.ServerConfig {
		return &api.ServerConfig{
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
	}

	local, err := Build(t.Context(), newConfig(), WithLogger(logger), withLocalHosting())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, local.Close()) })
	localAdapter, ok := local.storageSchema.(*storageSchemaAdapter)
	require.True(t, ok, "the server registers its own adapter")
	require.ErrorIs(t, localAdapter.checkDestructiveOptIn(true), tern.ErrInvalidStorageSchemaRequest,
		"a locally hosted server has no route to a destructive storage bootstrap")

	deployed, err := Build(t.Context(), newConfig(), WithLogger(logger))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, deployed.Close()) })
	deployedAdapter, ok := deployed.storageSchema.(*storageSchemaAdapter)
	require.True(t, ok, "the server registers its own adapter")
	assert.NoError(t, deployedAdapter.checkDestructiveOptIn(true),
		"the same request is honored on a normally hosted server")
}

// A MySQL storage server whose system time zone is west of UTC still agrees
// with SchemaBot on what time it is: a lock stamped by the database reads back
// as the current UTC instant, a deferred webhook delivery whose not-before
// time has passed is claimable, and one still in the future is not. Storage
// writes UTC timestamps from Go and compares them against the session's NOW(),
// so the storage pool must run its sessions in UTC.
func TestConnectStorageMySQLSessionsRunInUTC(t *testing.T) {
	ctx := t.Context()
	req := testutil.MySQLContainerRequest("mysql:8.0", "schemabot")
	req.Cmd = []string{"--default-time-zone=-07:00"}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req, Started: true,
	})
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, container)
	dsn, err := testutil.MySQLDSN(ctx, container, "schemabot")
	require.NoError(t, err)

	cfg := &api.ServerConfig{Storage: api.StorageConfig{DSN: dsn}}
	db, _, err := connectStorage(ctx, cfg, schema.DialectMySQL, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	store, err := newStore(schema.DialectMySQL, db)
	require.NoError(t, err)

	var globalZone, sessionZone string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT @@GLOBAL.time_zone, @@SESSION.time_zone").Scan(&globalZone, &sessionZone))
	require.Equal(t, "-07:00", globalZone, "the server must default to a non-UTC zone for this test to mean anything")
	assert.Equal(t, "+00:00", sessionZone)

	require.NoError(t, store.Locks().Acquire(ctx, &storage.Lock{
		DatabaseName: "appdb",
		DatabaseType: storage.DatabaseTypeMySQL,
		Repository:   "org/repo",
		PullRequest:  1,
		Owner:        "org/repo#1",
	}))
	lock, err := store.Locks().Get(ctx, "appdb", storage.DatabaseTypeMySQL)
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.WithinDuration(t, time.Now(), lock.CreatedAt, time.Minute)

	future := time.Now().Add(10 * time.Minute)
	_, err = store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
		DeliveryID: "deferred-future", Event: "pull_request", Action: "synchronize",
		Repository: "org/repo", PullRequest: 1, HeadSHA: "future-head", Payload: []byte(`{}`),
		RetryAfter: &future,
	})
	require.NoError(t, err)
	claimed, err := store.WebhookEvents().FindNext(ctx, "driver-1", time.Minute)
	require.NoError(t, err)
	assert.Nil(t, claimed, "a delivery deferred into the future is not claimable yet")

	past := time.Now().Add(-time.Minute)
	_, err = store.WebhookEvents().Create(ctx, &storage.WebhookEvent{
		DeliveryID: "deferred-past", Event: "pull_request", Action: "synchronize",
		Repository: "org/repo", PullRequest: 2, HeadSHA: "past-head", Payload: []byte(`{}`),
		RetryAfter: &past,
	})
	require.NoError(t, err)
	claimed, err = store.WebhookEvents().FindNext(ctx, "driver-1", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed, "a delivery whose not-before time has passed is claimable")
	assert.Equal(t, "deferred-past", claimed.DeliveryID)
}

func TestConnectStorageMySQLReadsPlanTimestamps(t *testing.T) {
	ctx := t.Context()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testutil.MySQLContainerRequest("mysql:8.0", "schemabot"), Started: true,
	})
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, container)
	dsn, err := testutil.MySQLDSN(ctx, container, "schemabot")
	require.NoError(t, err)
	for i, option := range []string{"", "?parseTime=false", "?parseTime=true"} {
		t.Run(fmt.Sprintf("option_%d", i), func(t *testing.T) {
			cfg := &api.ServerConfig{Storage: api.StorageConfig{DSN: dsn + option}}
			db, _, err := connectStorage(t.Context(), cfg, schema.DialectMySQL, slog.New(slog.DiscardHandler))
			require.NoError(t, err)
			t.Cleanup(func() { utils.CloseAndLog(db) })
			store, err := newStore(schema.DialectMySQL, db)
			require.NoError(t, err)
			plan := &storage.Plan{PlanIdentifier: fmt.Sprintf("timestamp-plan-%d", i), Database: "app", DatabaseType: "mysql", Environment: "development", CreatedAt: time.Now().UTC().Truncate(time.Second)}
			_, err = store.Plans().Create(t.Context(), plan)
			require.NoError(t, err)
			loaded, err := store.Plans().Get(t.Context(), plan.PlanIdentifier)
			require.NoError(t, err)
			require.NotNil(t, loaded)
			require.True(t, plan.CreatedAt.Equal(loaded.CreatedAt))
			var now time.Time
			require.NoError(t, db.QueryRowContext(t.Context(), "SELECT NOW(6)").Scan(&now))
			require.False(t, now.IsZero())
		})
	}
}
