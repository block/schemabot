//go:build integration

package api

import (
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// startCanonicalizeStorage boots a PostgreSQL storage database with the full
// storage schema, ready for rows to be seeded the way earlier releases wrote
// them — without folding identity strings.
func startCanonicalizeStorage(t *testing.T) *sql.DB {
	t.Helper()
	dsn, db := testutil.StartPostgres(t, "schemabot")
	require.NoError(t, EnsureSchema(dsn, slog.New(slog.DiscardHandler), WithDialect(schema.DialectPostgres)))
	return db
}

// insertLockRow seeds a locks row directly, bypassing the store's write
// boundary the way a pre-fold release did.
func insertLockRow(t *testing.T, db *sql.DB, databaseName, databaseType, repository, owner string) {
	t.Helper()
	_, err := db.ExecContext(t.Context(),
		`INSERT INTO locks (database_name, database_type, repository, pull_request, owner)
		 VALUES ($1, $2, $3, 42, $4)`, databaseName, databaseType, repository, owner)
	require.NoError(t, err)
}

// A storage database holding rows written before identity strings were
// folded at the write boundaries: the canonicalization folds every identity
// column to lowercase across the storage tables, leaves already-canonical
// rows untouched, and a rerun is a no-op.
func TestCanonicalizePostgresIdentityKeys_FoldsMixedCaseRows(t *testing.T) {
	db := startCanonicalizeStorage(t)
	logger := slog.New(slog.DiscardHandler)

	insertLockRow(t, db, "MyDB", "MySQL", "Org/Repo", "Org/Repo#42")
	insertLockRow(t, db, "otherdb", "mysql", "org/other", "cli:user@host")

	var canonicalUpdatedAt time.Time
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT updated_at FROM locks WHERE database_name = 'otherdb'`).Scan(&canonicalUpdatedAt))

	_, err := db.ExecContext(t.Context(),
		`INSERT INTO apply_target_locks (database_name, database_type, environment, deployment)
		 VALUES ('MyDB', 'MySQL', 'Production', 'Default')`)
	require.NoError(t, err)

	_, err = db.ExecContext(t.Context(),
		`INSERT INTO checks (repository, pull_request, head_sha, environment, database_type, database_name, status)
		 VALUES ('Org/Repo', 7, 'AbCd1234', 'Staging', 'MySQL', 'MyDB', 'queued')`)
	require.NoError(t, err)

	_, err = db.ExecContext(t.Context(),
		`INSERT INTO plans (plan_identifier, database_name, database_type, deployment, repository, pull_request, environment, schema_files, plan_data)
		 VALUES ('plan-AbC123', 'MyDB', 'MySQL', 'Default', 'Org/Repo', 7, 'Staging', '{}', '{}')`)
	require.NoError(t, err)

	_, err = db.ExecContext(t.Context(),
		`INSERT INTO webhook_events (delivery_id, event, repository, payload, state, received_at)
		 VALUES ('delivery-1', 'pull_request', 'Org/Repo', '{}', 'completed', CURRENT_TIMESTAMP)`)
	require.NoError(t, err)

	require.NoError(t, CanonicalizePostgresIdentityKeys(t.Context(), db, logger))

	var databaseName, databaseType, repository, owner string
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT database_name, database_type, repository, owner FROM locks WHERE pull_request = 42 AND database_name = 'mydb'`).
		Scan(&databaseName, &databaseType, &repository, &owner))
	assert.Equal(t, "mydb", databaseName)
	assert.Equal(t, "mysql", databaseType)
	assert.Equal(t, "org/repo", repository)
	assert.Equal(t, "org/repo#42", owner)

	var untouchedUpdatedAt time.Time
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT owner, updated_at FROM locks WHERE database_name = 'otherdb'`).Scan(&owner, &untouchedUpdatedAt))
	assert.Equal(t, "cli:user@host", owner, "an already-canonical row is left untouched")
	assert.True(t, untouchedUpdatedAt.Equal(canonicalUpdatedAt),
		"an already-canonical row must not be rewritten: updated_at moved from %s to %s", canonicalUpdatedAt, untouchedUpdatedAt)

	var environment, deployment string
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT database_name, database_type, environment, deployment FROM apply_target_locks`).
		Scan(&databaseName, &databaseType, &environment, &deployment))
	assert.Equal(t, "mydb", databaseName)
	assert.Equal(t, "mysql", databaseType)
	assert.Equal(t, "production", environment)
	assert.Equal(t, "default", deployment)

	var headSHA string
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT repository, environment, database_type, database_name, head_sha FROM checks WHERE pull_request = 7`).
		Scan(&repository, &environment, &databaseType, &databaseName, &headSHA))
	assert.Equal(t, "org/repo", repository)
	assert.Equal(t, "staging", environment)
	assert.Equal(t, "mysql", databaseType)
	assert.Equal(t, "mydb", databaseName)
	assert.Equal(t, "AbCd1234", headSHA, "SHAs are opaque values, not identity keys, and must not fold")

	var planIdentifier string
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT plan_identifier, database_name, database_type, deployment, repository, environment FROM plans`).
		Scan(&planIdentifier, &databaseName, &databaseType, &deployment, &repository, &environment))
	assert.Equal(t, "plan-AbC123", planIdentifier, "plan identifiers are opaque values and must not fold")
	assert.Equal(t, "mydb", databaseName)
	assert.Equal(t, "mysql", databaseType)
	assert.Equal(t, "default", deployment)
	assert.Equal(t, "org/repo", repository)
	assert.Equal(t, "staging", environment)

	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT repository FROM webhook_events WHERE delivery_id = 'delivery-1'`).Scan(&repository))
	assert.Equal(t, "org/repo", repository)

	require.NoError(t, CanonicalizePostgresIdentityKeys(t.Context(), db, logger),
		"a rerun over already-canonical rows must succeed as a no-op")
}

// Two lock rows whose database spelling differs only by case would collapse
// into one unique key when folded: the canonicalization fails naming the
// violated constraint so the operator resolves the duplicate rows by hand,
// instead of silently losing one of them.
func TestCanonicalizePostgresIdentityKeys_CollisionNamesConstraint(t *testing.T) {
	db := startCanonicalizeStorage(t)

	insertLockRow(t, db, "Shared", "mysql", "org/repo", "org/repo#1")
	insertLockRow(t, db, "shared", "mysql", "org/repo", "org/repo#2")

	err := CanonicalizePostgresIdentityKeys(t.Context(), db, slog.New(slog.DiscardHandler))
	require.Error(t, err)
	assert.ErrorContains(t, err, `storage table "locks"`)
	assert.ErrorContains(t, err, `unique index "idx_locks_database"`,
		"the violated index must be named in the fold's own phrasing, not the raw driver error")
	assert.ErrorContains(t, err, "differ only by case")
}

// The same collision refusal on a table whose unique keys span several
// folded columns: two apply-target locks whose database spelling differs
// only by case must fail the fold naming a violated unique index rather
// than collapse into one target.
func TestCanonicalizePostgresIdentityKeys_ApplyTargetLockCollision(t *testing.T) {
	db := startCanonicalizeStorage(t)

	_, err := db.ExecContext(t.Context(),
		`INSERT INTO apply_target_locks (database_name, database_type, environment, deployment)
		 VALUES ('Shared', 'mysql', 'production', ''), ('shared', 'mysql', 'production', '')`)
	require.NoError(t, err)

	err = CanonicalizePostgresIdentityKeys(t.Context(), db, slog.New(slog.DiscardHandler))
	require.Error(t, err)
	assert.ErrorContains(t, err, `storage table "apply_target_locks"`)
	// Both idx_apply_target_locks_target and its _v2 variant are violated;
	// PostgreSQL names whichever it checks first, so pin the shared prefix.
	assert.ErrorContains(t, err, `unique index "idx_apply_target_locks_target`)
	assert.ErrorContains(t, err, "differ only by case")
}

// Pointing the canonicalization at a database with none of the storage
// tables fails up front: folding arbitrary tables' columns on a mistyped DSN
// must be impossible.
func TestCanonicalizePostgresIdentityKeys_RefusesNonStorageDatabase(t *testing.T) {
	_, db := testutil.StartPostgres(t, "notschemabot")

	err := CanonicalizePostgresIdentityKeys(t.Context(), db, slog.New(slog.DiscardHandler))
	require.Error(t, err)
	assert.ErrorContains(t, err, "does not look like SchemaBot's storage database")
}
