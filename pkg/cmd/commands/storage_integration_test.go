//go:build integration

package commands

import (
	"database/sql"
	"errors"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// startResyncStorage boots a PostgreSQL storage database with the full
// storage schema and seeds the settings table with explicit-id rows, the way
// an id-preserving bulk load writes them. The seeded rows leave the identity
// sequence behind the stored maximum, so a default insert collides until the
// resync runs.
func startResyncStorage(t *testing.T) (string, *sql.DB) {
	t.Helper()
	dsn, db := testutil.StartPostgres(t, "schemabot")
	require.NoError(t, api.EnsureSchema(dsn, slog.New(slog.DiscardHandler), api.WithDialect(schema.DialectPostgres)))
	for id, key := range map[int64]string{1: "loaded-1", 2: "loaded-2", 3: "loaded-3"} {
		_, err := db.ExecContext(t.Context(),
			`INSERT INTO settings (id, setting_key, setting_value) VALUES ($1, $2, '')`, id, key)
		require.NoError(t, err)
	}
	requireHostileStatementTimeout(t, db)
	return dsn, db
}

// requireHostileStatementTimeout parks a statement budget on the database that
// no real query can finish inside, the way a hosted provider tunes one for API
// traffic. These subcommands are operator-supervised one-shots that must run to
// completion under ctx alone, so each opens its pool with the budget disabled
// outright; without that, the catalog reads over every storage table are
// cancelled part-way and the command fails. The caller's existing handle keeps
// its own session, since a database default reaches new sessions only.
func requireHostileStatementTimeout(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(t.Context(),
		`ALTER DATABASE schemabot SET statement_timeout = '1ms'`)
	require.NoError(t, err)
}

// requireDefaultInsertResumes asserts that a default insert into settings
// draws the id above the loaded maximum, proving the resync advanced the
// sequence past the explicit-id rows.
func requireDefaultInsertResumes(t *testing.T, db *sql.DB) {
	t.Helper()
	var id int64
	err := db.QueryRowContext(t.Context(),
		`INSERT INTO settings (setting_key, setting_value) VALUES ('after-resync', '') RETURNING id`).Scan(&id)
	require.NoError(t, err, "default insert must succeed after the resync")
	require.Equal(t, int64(4), id, "the first default insert after the resync draws max+1")
}

func requireDefaultInsertCollides(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(t.Context(),
		`INSERT INTO settings (setting_key, setting_value) VALUES ('before-resync', '')`)
	require.Error(t, err)
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "default insert must return a PostgreSQL error")
	require.Equal(t, "23505", pgErr.Code, "default insert must collide before the resync")
}

// After an explicit-id bulk load, the operator runs the resync subcommand
// with the storage DSN passed directly; afterwards default inserts resume
// above the loaded ids instead of colliding with them.
func TestResyncIdentitySequencesCmd_DSNFlag(t *testing.T) {
	dsn, db := startResyncStorage(t)
	requireDefaultInsertCollides(t, db)

	cmd := &ResyncIdentitySequencesCmd{DSN: dsn}
	require.NoError(t, cmd.Run(t.Context(), &Globals{Version: "test"}))

	requireDefaultInsertResumes(t, db)
}

// In a deployed pod the operator points the resync subcommand at the server
// config instead of hand-building a DSN; the command resolves the storage
// DSN from the config's storage section and resyncs the same way.
func TestResyncIdentitySequencesCmd_ConfigFile(t *testing.T) {
	dsn, db := startResyncStorage(t)
	requireDefaultInsertCollides(t, db)

	path := writeStorageTestConfig(t, `
storage:
  dialect: postgres
  dsn: `+dsn+`
`)
	cmd := &ResyncIdentitySequencesCmd{Config: path}
	require.NoError(t, cmd.Run(t.Context(), &Globals{Version: "test"}))

	requireDefaultInsertResumes(t, db)
}

// During the upgrade to a release that folds identity strings at the write
// boundaries, the operator runs the canonicalization subcommand with the
// storage DSN passed directly; afterwards rows written by earlier releases
// carry the canonical lowercase spelling the folded lookups expect.
func TestCanonicalizeIdentityKeysCmd_DSNFlag(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "schemabot")
	require.NoError(t, api.EnsureSchema(dsn, slog.New(slog.DiscardHandler), api.WithDialect(schema.DialectPostgres)))
	_, err := db.ExecContext(t.Context(),
		`INSERT INTO locks (database_name, database_type, repository, pull_request, owner)
		 VALUES ('MyDB', 'MySQL', 'Org/Repo', 42, 'Org/Repo#42')`)
	require.NoError(t, err)

	// Enough rows that the fold's own UPDATE cannot finish inside the hostile
	// budget below. The distinct numeric suffix survives the fold, so no two
	// rows collide on the unique (database_name, database_type) index.
	_, err = db.ExecContext(t.Context(),
		`INSERT INTO locks (database_name, database_type, repository, pull_request, owner)
		 SELECT 'MyDB' || i, 'MySQL', 'Org/Repo', i, 'Org/Repo#' || i
		 FROM generate_series(1000, 21000) AS i`)
	require.NoError(t, err)
	requireHostileStatementTimeout(t, db)

	cmd := &CanonicalizeIdentityKeysCmd{DSN: dsn, AutoApprove: true}
	require.NoError(t, cmd.Run(t.Context(), &Globals{Version: "test"}))

	var databaseName, databaseType, repository, owner string
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT database_name, database_type, repository, owner FROM locks WHERE pull_request = 42`).
		Scan(&databaseName, &databaseType, &repository, &owner))
	require.Equal(t, "mydb", databaseName)
	require.Equal(t, "mysql", databaseType)
	require.Equal(t, "org/repo", repository)
	require.Equal(t, "org/repo#42", owner)
}
