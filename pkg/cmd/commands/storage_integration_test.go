//go:build integration

package commands

import (
	"database/sql"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
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
	return dsn, db
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
	assert.Equal(t, int64(4), id, "the first default insert after the resync draws max+1")
}

// After an explicit-id bulk load, the operator runs the resync subcommand
// with the storage DSN passed directly; afterwards default inserts resume
// above the loaded ids instead of colliding with them.
func TestResyncIdentitySequencesCmd_DSNFlag(t *testing.T) {
	dsn, db := startResyncStorage(t)

	cmd := &ResyncIdentitySequencesCmd{DSN: dsn}
	require.NoError(t, cmd.Run(t.Context()))

	requireDefaultInsertResumes(t, db)
}

// In a deployed pod the operator points the resync subcommand at the server
// config instead of hand-building a DSN; the command resolves the storage
// DSN from the config's storage section and resyncs the same way.
func TestResyncIdentitySequencesCmd_ConfigFile(t *testing.T) {
	dsn, db := startResyncStorage(t)

	path := writeStorageTestConfig(t, `
storage:
  dialect: postgres
  dsn: `+dsn+`
`)
	cmd := &ResyncIdentitySequencesCmd{Config: path}
	require.NoError(t, cmd.Run(t.Context()))

	requireDefaultInsertResumes(t, db)
}
