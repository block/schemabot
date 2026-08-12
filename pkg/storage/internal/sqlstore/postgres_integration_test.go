//go:build integration

package sqlstore

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/storagetest"
	"github.com/block/schemabot/pkg/testutil"
)

type postgresHarness struct {
	db  *sql.DB
	dsn string
}

func (h postgresHarness) NewStorage(t *testing.T) storage.Storage {
	t.Helper()
	clearPostgresTables(t, h.db)
	dialect := PostgresDialect{}
	return NewWithDependencies(Dependencies{
		DB:         h.db,
		Binder:     dialect,
		Dialect:    dialect,
		Identity:   dialect,
		Locker:     namedlock.Postgres{},
		Classifier: NewPostgresErrorClassifier(),
	})
}

func (h postgresHarness) NewUnreachableStorage(t *testing.T) storage.Storage {
	t.Helper()
	db, err := sql.Open("pgx", h.dsn)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	dialect := PostgresDialect{}
	return NewWithDependencies(Dependencies{
		DB:         db,
		Binder:     dialect,
		Dialect:    dialect,
		Identity:   dialect,
		Locker:     namedlock.Postgres{},
		Classifier: NewPostgresErrorClassifier(),
	})
}

func TestPostgresStorageParity(t *testing.T) {
	dsn, fixtureDB := testutil.StartPostgres(t, "sqlstore_parity")
	db, err := postgresconn.Open(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.PingContext(t.Context()))
	applyPostgresTestSchema(t, fixtureDB)

	h := postgresHarness{db: db, dsn: dsn}
	t.Run("Settings", func(t *testing.T) { storagetest.TestSettings(t, h) })
	t.Run("SettingsUpdatedAtAdvances", func(t *testing.T) { testPostgresSettingsUpdatedAtAdvances(t, h) })
	t.Run("ApplyLogs", func(t *testing.T) { storagetest.TestApplyLogs(t, h) })
	t.Run("LeaseGuardedApplyLogAppend", func(t *testing.T) { testPostgresLeaseGuardedApplyLogAppend(t, h) })
}

// testPostgresSettingsUpdatedAtAdvances proves that a second Set renews
// updated_at through the upsert's explicit stamp. The row is backdated between
// writes so the assertion cannot pass on write-clock proximity alone; the
// PostgreSQL schema has no automatic renewal, so a dropped stamp would leave
// the backdated value in place.
func testPostgresSettingsUpdatedAtAdvances(t *testing.T, h postgresHarness) {
	store := h.NewStorage(t)
	ctx := t.Context()

	require.NoError(t, store.Settings().Set(ctx, "stamp_key", "v1"))
	_, err := h.db.ExecContext(ctx,
		`UPDATE settings SET updated_at = now() - interval '1 hour' WHERE setting_key = $1`, "stamp_key")
	require.NoError(t, err)
	backdated, err := store.Settings().Get(ctx, "stamp_key")
	require.NoError(t, err)
	require.NotNil(t, backdated)

	require.NoError(t, store.Settings().Set(ctx, "stamp_key", "v2"))
	updated, err := store.Settings().Get(ctx, "stamp_key")
	require.NoError(t, err)
	require.NotNil(t, updated)
	assert.Equal(t, "v2", updated.Value)
	assert.True(t, updated.UpdatedAt.After(backdated.UpdatedAt),
		"second Set must advance updated_at (got %s, was %s)", updated.UpdatedAt, backdated.UpdatedAt)
	assert.True(t, updated.CreatedAt.Equal(backdated.CreatedAt), "Set must not rewrite created_at")
}

// testPostgresLeaseGuardedApplyLogAppend pins the guarded-identity insert
// contract against a real PostgreSQL server: a guarded INSERT ... SELECT whose
// lease predicate matches must insert and return the generated id, while a
// stale lease must surface as the lease-lost error rather than a hard failure —
// the RETURNING row's absence is the only signal distinguishing the two.
func testPostgresLeaseGuardedApplyLogAppend(t *testing.T, h postgresHarness) {
	store := h.NewStorage(t)
	ctx := t.Context()

	lock := storagetest.CreateLock(t, store, "guarded_logs_db", storage.DatabaseTypeMySQL)
	apply := storagetest.CreateApplyWithStateAndEnv(t, store, lock, "apply_guarded_logs", 700, state.Apply.Running, "staging")

	_, err := h.db.ExecContext(ctx,
		`UPDATE applies SET lease_owner = $1, lease_token = $2, lease_acquired_at = now() WHERE id = $3`,
		"driver-a", "owned-token", apply.ID)
	require.NoError(t, err)

	ownedCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: apply.ID, Owner: "driver-a", Token: "owned-token"})
	owned := &storage.ApplyLog{
		ApplyID:   apply.ID,
		Level:     storage.LogLevelInfo,
		EventType: storage.LogEventInfo,
		Source:    storage.LogSourceSchemaBot,
		Message:   "owned driver log",
	}
	require.NoError(t, store.ApplyLogs().Append(ownedCtx, owned))
	assert.Positive(t, owned.ID, "guarded insert must return the generated id")

	staleCtx := storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: apply.ID, Owner: "driver-old", Token: "stale-token"})
	require.ErrorIs(t, store.ApplyLogs().Append(staleCtx, &storage.ApplyLog{
		ApplyID:   apply.ID,
		Level:     storage.LogLevelInfo,
		EventType: storage.LogEventInfo,
		Source:    storage.LogSourceSchemaBot,
		Message:   "stale driver log",
	}), storage.ErrApplyLeaseLost)

	logs, err := store.ApplyLogs().GetByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	assert.Equal(t, "owned driver log", logs[0].Message)
}

func TestPGXStdlibValueContracts(t *testing.T) {
	_, db := testutil.StartPostgres(t, "sqlstore_values")
	_, err := db.ExecContext(t.Context(), `CREATE TABLE value_contracts (
		id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		flag boolean NOT NULL,
		payload jsonb NOT NULL,
		observed_at timestamptz NOT NULL,
		recorded_at timestamp NOT NULL,
		label text NOT NULL
	)`)
	require.NoError(t, err)

	wantTime := time.Date(2026, time.August, 11, 12, 34, 56, 123456000, time.FixedZone("test", 2*60*60))
	_, err = db.ExecContext(t.Context(),
		`INSERT INTO value_contracts (flag, payload, observed_at, recorded_at, label) VALUES ($1, $2, $3, $4, $5)`,
		true, []byte(`{"enabled":true}`), wantTime, wantTime.UTC(), "same")
	require.NoError(t, err)

	var gotBool bool
	var gotJSON []byte
	var gotTime time.Time
	var gotPlain time.Time
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT flag, payload, observed_at, recorded_at FROM value_contracts WHERE id = 1`,
	).Scan(&gotBool, &gotJSON, &gotTime, &gotPlain))
	assert.True(t, gotBool)
	assert.JSONEq(t, `{"enabled":true}`, string(gotJSON))
	assert.Equal(t, wantTime.UTC(), gotTime.UTC())
	assert.Equal(t, wantTime.Nanosecond(), gotTime.Nanosecond(), "timestamptz retains microsecond precision")
	// Every datetime column in the PostgreSQL schema is a plain timestamp
	// (without time zone), which stores a wall-clock reading with no instant
	// semantics. The stores' portability contract is therefore UTC-in/UTC-out:
	// a UTC value must round-trip byte-exact, so predicates comparing stored
	// values against server-side now() (lease expiry, retry windows) hold as
	// long as writers hand the driver UTC times.
	assert.Equal(t, wantTime.UTC(), gotPlain.UTC(), "plain timestamp round-trips a UTC write unchanged")
	assert.Equal(t, wantTime.Nanosecond(), gotPlain.Nanosecond(), "plain timestamp retains microsecond precision")

	result, err := db.ExecContext(t.Context(), `UPDATE value_contracts SET label = $1 WHERE id = $2`, "same", 1)
	require.NoError(t, err)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected, "PostgreSQL reports matched rows even when values are unchanged")
}

func applyPostgresTestSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	entries, err := schema.PostgresFS.ReadDir("postgres")
	require.NoError(t, err)
	for _, entry := range entries {
		content, readErr := schema.PostgresFS.ReadFile("postgres/" + entry.Name())
		require.NoError(t, readErr)
		_, execErr := db.ExecContext(t.Context(), string(content))
		require.NoError(t, execErr, "execute %s", entry.Name())
	}
}

func clearPostgresTables(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT tablename FROM pg_tables WHERE schemaname = 'public'`)
	require.NoError(t, err)
	var tables []string
	for rows.Next() {
		var table string
		require.NoError(t, rows.Scan(&table))
		tables = append(tables, table)
	}
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())
	for _, table := range tables {
		_, err := db.ExecContext(t.Context(), fmt.Sprintf(`TRUNCATE TABLE %q RESTART IDENTITY CASCADE`, table))
		require.NoError(t, err)
	}
}
