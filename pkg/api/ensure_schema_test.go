package api

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
)

// closedPortDSN points at a closed loopback port: any connection attempt fails
// instantly with a refused connection rather than hanging, so tests using it
// never depend on a real database and stay fast.
const closedPortDSN = "user:pass@tcp(127.0.0.1:1)/schemabot"

// EnsureSchema routes to the schema bootstrapper for the storage database's
// dialect and fails closed for a dialect that has none: it must return an
// error naming the dialect before touching the database, never fall back to
// running another family's flow against the storage database. The DSN
// points at a closed loopback port so any accidental connection attempt fails
// the test rather than hanging.
func TestEnsureSchemaFailsClosedForUnsupportedDialect(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)

	unsupported := schema.Dialect("sqlite")
	err := EnsureSchema(closedPortDSN, logger, WithDialect(unsupported))

	require.Error(t, err)
	require.ErrorContains(t, err, "no schema bootstrapper")
	require.ErrorContains(t, err, string(unsupported))
}

// EnsureSchema with the postgres dialect routes into the PostgreSQL
// bootstrapper. The closed-port DSN makes its connection attempt fail
// instantly, and the resulting error proves the dialect routed into the
// PostgreSQL flow rather than the fail-closed branch.
func TestEnsureSchemaRoutesPostgresDialect(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)

	err := EnsureSchema("postgres://user:pass@127.0.0.1:1/schemabot", logger, WithDialect(schema.DialectPostgres))

	require.Error(t, err)
	require.NotContains(t, err.Error(), "no schema bootstrapper")
	require.ErrorContains(t, err, "ping storage database")
}

// EnsureSchema without a dialect option defaults to the MySQL bootstrapper —
// the behavior every existing call site relies on. The closed-port DSN makes
// the MySQL flow's connection attempt fail instantly, and the resulting error
// proves the default routed into the MySQL flow rather than the fail-closed
// branch: a zero-value dialect would return "no schema bootstrapper" and
// crash-loop every pod at startup.
func TestEnsureSchemaDefaultsToMySQLDialect(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)

	err := EnsureSchema(closedPortDSN, logger)

	require.Error(t, err)
	require.NotContains(t, err.Error(), "no schema bootstrapper")
	require.ErrorContains(t, err, "plan schema")
}

// partitionDestructiveChanges delegates its refusal vocabulary to Spirit's
// UnsafeLinter, so these cases pin the accept/refuse boundary the
// storage-schema bootstrap relies on: statements that destroy data are
// refused whole (never rewritten), statements that lose nothing execute, and
// a statement Spirit cannot classify fails the bootstrap rather than
// executing unclassified.
func TestPartitionDestructiveChangesPinsUnsafeVocabulary(t *testing.T) {
	t.Parallel()

	single := func(ddl string) []engine.SchemaChange {
		return []engine.SchemaChange{{TableChanges: []engine.TableChange{{Table: "applies", DDL: ddl}}}}
	}

	refusedCases := []struct {
		name string
		ddl  string
	}{
		{name: "DROP TABLE", ddl: "DROP TABLE `applies`"},
		{name: "DROP COLUMN", ddl: "ALTER TABLE `applies` DROP COLUMN `caller`"},
		{name: "DROP COLUMN mixed with additive clauses is refused whole", ddl: "ALTER TABLE `applies` ADD COLUMN `caller` VARCHAR(64), DROP COLUMN `lease_owner`"},
		{name: "DROP PRIMARY KEY", ddl: "ALTER TABLE `applies` DROP PRIMARY KEY"},
		{name: "DROP PARTITION", ddl: "ALTER TABLE `applies` DROP PARTITION p2020"},
		{name: "TRUNCATE PARTITION", ddl: "ALTER TABLE `applies` TRUNCATE PARTITION p2020"},
	}
	for _, tt := range refusedCases {
		t.Run("refuses "+tt.name, func(t *testing.T) {
			t.Parallel()
			allowed, refused, err := partitionDestructiveChanges(single(tt.ddl))
			require.NoError(t, err)
			assert.Empty(t, allowed)
			require.Len(t, refused, 1)
			assert.Equal(t, tt.ddl, refused[0].change.DDL)
			assert.NotEmpty(t, refused[0].reason)
		})
	}

	allowedCases := []struct {
		name string
		ddl  string
	}{
		{name: "ADD COLUMN", ddl: "ALTER TABLE `applies` ADD COLUMN `caller` VARCHAR(64)"},
		{name: "DROP INDEX loses no data", ddl: "ALTER TABLE `applies` DROP INDEX `idx_state`"},
		{name: "CREATE TABLE", ddl: "CREATE TABLE `audit` (`id` BIGINT UNSIGNED AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"},
	}
	for _, tt := range allowedCases {
		t.Run("allows "+tt.name, func(t *testing.T) {
			t.Parallel()
			allowed, refused, err := partitionDestructiveChanges(single(tt.ddl))
			require.NoError(t, err)
			assert.Empty(t, refused)
			require.Len(t, allowed, 1)
			require.Len(t, allowed[0].TableChanges, 1)
			assert.Equal(t, tt.ddl, allowed[0].TableChanges[0].DDL)
		})
	}

	t.Run("a statement Spirit cannot classify fails the bootstrap", func(t *testing.T) {
		t.Parallel()
		_, _, err := partitionDestructiveChanges(single("TRUNCATE TABLE `applies`"))
		require.Error(t, err)
		require.ErrorContains(t, err, "classify storage schema change")
	})
}
