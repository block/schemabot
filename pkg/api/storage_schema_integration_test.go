//go:build integration

package api

import (
	"database/sql"
	"log/slog"
	"os"
	"strings"
	"testing"

	_ "github.com/block/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

func storageSchemaTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// statementFor returns the statement a report holds for one table, so an
// assertion names the table it is about instead of indexing into a slice whose
// order would change with the schema.
func statementFor(t *testing.T, statements []StorageSchemaStatement, table string) StorageSchemaStatement {
	t.Helper()
	for _, statement := range statements {
		if statement.Table == table {
			return statement
		}
	}
	require.Failf(t, "no statement for table", "table %q not among %d statements: %v", table, len(statements), statementTables(statements))
	return StorageSchemaStatement{}
}

func statementTables(statements []StorageSchemaStatement) []string {
	tables := make([]string, 0, len(statements))
	for _, statement := range statements {
		tables = append(tables, statement.Table)
	}
	return tables
}

// An empty MySQL storage database needs its whole schema, and the diff says so
// without touching it: every embedded table is reported as a CREATE TABLE, and
// the database is still empty afterwards. This is the read an operator runs
// first during a deploy that did not converge, so it has to be safe to run
// against a database in any state.
func TestDiffStorageSchemaMySQL_EmptyDatabaseNeedsEveryTable(t *testing.T) {
	sdb, db := openEnsureSchemaDatabase(t)

	report, err := DiffStorageSchema(t.Context(), sdb.DSN, storageSchemaTestLogger())
	require.NoError(t, err)

	assert.Equal(t, schema.DialectMySQL, report.Dialect)
	assert.Equal(t, sdb.Name, report.Database)
	assert.False(t, report.Converged(), "an empty storage database cannot be converged")
	assert.Empty(t, report.Destructive)
	assert.Empty(t, report.Manual)

	files, err := readEmbeddedSchemaFiles()
	require.NoError(t, err)
	require.Contains(t, files, storageSchemaNamespace)
	assert.Len(t, report.Outstanding, len(files[storageSchemaNamespace].Files),
		"every embedded schema file's table should be outstanding on an empty database")

	applies := statementFor(t, report.Outstanding, "applies")
	assert.Equal(t, "create_table", applies.Operation)
	assert.Contains(t, applies.DDL, "CREATE TABLE")
	assert.Contains(t, applies.DDL, "`applies`")
	assert.Empty(t, applies.Reason, "a CREATE TABLE is neither destructive nor manual")

	// The diff executed nothing: the tables it reported are still absent.
	assert.False(t, testutil.TableExists(t, db, sdb.Name, "applies"),
		"a diff must not create the tables it reports")
}

// Converging a storage database with the storage apply command is the startup
// bootstrap: afterwards the database matches the embedded schema, the report
// names what ran, and a second diff finds nothing. This is the pre-deploy
// convergence step, so it has to leave the database in exactly the state a
// boot would.
func TestApplyStorageSchemaMySQL_ConvergesEmptyDatabase(t *testing.T) {
	sdb, db := openEnsureSchemaDatabase(t)

	planned, remaining, err := ApplyStorageSchema(t.Context(), sdb.DSN, storageSchemaTestLogger())
	require.NoError(t, err)

	assert.NotEmpty(t, planned.Outstanding, "an empty database has statements to run")
	assert.True(t, remaining.Converged(), "nothing should be outstanding after a convergence: %v", statementTables(remaining.Outstanding))
	assert.Equal(t, sdb.Name, remaining.Database)

	for _, table := range []string{"applies", "tasks", "plans", "locks", "checks", "settings", "apply_operations"} {
		assert.True(t, testutil.TableExists(t, db, sdb.Name, table), "table %s missing after convergence", table)
	}

	// The confirming read is the same read an operator would run next.
	after, err := DiffStorageSchema(t.Context(), sdb.DSN, storageSchemaTestLogger())
	require.NoError(t, err)
	assert.True(t, after.Converged())
	assert.Empty(t, after.Outstanding)
}

// A storage database that a deploy left one column short reports exactly that
// one statement, naming the column. This is the incident the command exists
// for: the answer has to come from the live catalog, because a release tag
// would say the column is present.
func TestDiffStorageSchemaMySQL_ReportsMissingColumn(t *testing.T) {
	sdb, db := openEnsureSchemaDatabase(t)
	require.NoError(t, EnsureSchema(sdb.DSN, storageSchemaTestLogger()))

	// Put the database back into the state a partly converged deploy leaves:
	// the table is there, one column the embedded schema declares is not.
	_, err := db.ExecContext(t.Context(), "ALTER TABLE `applies` DROP COLUMN `deployment`")
	require.NoError(t, err, "drop a column the embedded schema declares")
	require.False(t, testutil.ColumnExists(t, db, sdb.Name, "applies", "deployment"))

	report, err := DiffStorageSchema(t.Context(), sdb.DSN, storageSchemaTestLogger())
	require.NoError(t, err)

	assert.False(t, report.Converged())
	assert.Empty(t, report.Destructive, "restoring a missing column destroys nothing")
	assert.Empty(t, report.Manual)
	require.Len(t, report.Outstanding, 1, "only the one table diverges: %v", statementTables(report.Outstanding))

	statement := report.Outstanding[0]
	assert.Equal(t, "applies", statement.Table)
	assert.Equal(t, "alter_table", statement.Operation)
	assert.Contains(t, statement.DDL, "ADD COLUMN")
	assert.Contains(t, statement.DDL, "`deployment`")

	// Applying the reported statement is what converges it, and nothing else
	// is left behind.
	_, remaining, err := ApplyStorageSchema(t.Context(), sdb.DSN, storageSchemaTestLogger())
	require.NoError(t, err)
	assert.True(t, remaining.Converged(), "the reported statement should be the whole difference")
	assert.True(t, testutil.ColumnExists(t, db, sdb.Name, "applies", "deployment"))
}

// A storage database holding a table the embedded schema does not declare —
// what an older binary sees after a rollback — reports the DROP TABLE as
// refused, not as outstanding, and leaves the table in place. Converging under
// that refusal is a success: the surplus state is deliberate, and destroying it
// would take newer schema state away from the release that is about to be
// rolled forward again (AV-9).
func TestDiffStorageSchemaMySQL_RefusesSurplusTable(t *testing.T) {
	sdb, db := openEnsureSchemaDatabase(t)
	require.NoError(t, EnsureSchema(sdb.DSN, storageSchemaTestLogger()))

	_, err := db.ExecContext(t.Context(),
		"CREATE TABLE `newer_release_state` (`id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err, "create a table a newer release would own")

	report, err := DiffStorageSchema(t.Context(), sdb.DSN, storageSchemaTestLogger())
	require.NoError(t, err)

	assert.False(t, report.Converged(), "a refused statement is not convergence")
	assert.False(t, report.DestructiveAllowed)
	assert.Empty(t, report.Outstanding)
	require.Len(t, report.Destructive, 1)
	statement := report.Destructive[0]
	assert.Equal(t, "newer_release_state", statement.Table)
	assert.Equal(t, "drop_table", statement.Operation)
	assert.Contains(t, statement.DDL, "DROP TABLE")
	assert.NotEmpty(t, statement.Reason, "a refusal has to say why it was refused")

	planned, remaining, err := ApplyStorageSchema(t.Context(), sdb.DSN, storageSchemaTestLogger())
	require.NoError(t, err, "a convergence that refuses destructive statements still succeeds")
	assert.Len(t, planned.Destructive, 1)
	assert.Len(t, remaining.Destructive, 1, "the refused statement is still outstanding afterwards")
	assert.True(t, testutil.TableExists(t, db, sdb.Name, "newer_release_state"),
		"a refused DROP TABLE must leave the table in place")

	// The same report with destructive changes allowed says the statement would
	// run, which is what the operator opting in is asking to be told.
	allowed, err := DiffStorageSchema(t.Context(), sdb.DSN, storageSchemaTestLogger(), WithAllowDestructiveSchemaChanges(true))
	require.NoError(t, err)
	assert.True(t, allowed.DestructiveAllowed)
	require.Len(t, allowed.Destructive, 1)
	assert.Equal(t, "newer_release_state", allowed.Destructive[0].Table)
}

// A diff is safe to run against a storage database another process is
// converging: it takes no advisory lock, so it answers while the bootstrap
// holds one rather than blocking behind it. During an incident that is the
// difference between reading the state and waiting on it.
func TestDiffStorageSchemaMySQL_ReadsWhileBootstrapHoldsLock(t *testing.T) {
	sdb, db := openEnsureSchemaDatabase(t)
	require.NoError(t, EnsureSchema(sdb.DSN, storageSchemaTestLogger()))

	// Hold the bootstrap's advisory lock on a dedicated session, the way a
	// converging pod does.
	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	var acquired int
	require.NoError(t, conn.QueryRowContext(t.Context(), "SELECT GET_LOCK(?, 5)", ensureSchemaLockName).Scan(&acquired))
	require.Equal(t, 1, acquired, "hold the bootstrap lock")
	defer func() {
		var released sql.NullInt64
		_ = conn.QueryRowContext(t.Context(), "SELECT RELEASE_LOCK(?)", ensureSchemaLockName).Scan(&released)
	}()

	report, err := DiffStorageSchema(t.Context(), sdb.DSN, storageSchemaTestLogger())
	require.NoError(t, err, "a diff must not wait on the bootstrap lock")
	assert.True(t, report.Converged())
}

// An empty PostgreSQL storage database reports every embedded table as a
// CREATE TABLE, and converging it leaves nothing outstanding. The additive
// convergence never drops anything, so the report has no destructive set at
// all on this dialect.
func TestDiffStorageSchemaPostgres_ConvergesEmptyDatabase(t *testing.T) {
	dsn, db := startPostgresStorage(t)
	logger := storageSchemaTestLogger()
	postgres := WithDialect(schema.DialectPostgres)

	report, err := DiffStorageSchema(t.Context(), dsn, logger, postgres)
	require.NoError(t, err)
	assert.Equal(t, schema.DialectPostgres, report.Dialect)
	assert.Equal(t, "schemabot", report.Database)
	assert.False(t, report.Converged())
	assert.Empty(t, report.Destructive, "the PostgreSQL convergence is additive-only")
	assert.Empty(t, report.Manual)

	tables, _, err := readEmbeddedPostgresSchemaFiles()
	require.NoError(t, err)
	assert.Len(t, report.Outstanding, len(tables))
	applies := statementFor(t, report.Outstanding, "applies")
	assert.Equal(t, postgresOpCreateTable, applies.Operation)
	assert.Contains(t, strings.ToUpper(applies.DDL), "CREATE TABLE")

	planned, remaining, err := ApplyStorageSchema(t.Context(), dsn, logger, postgres)
	require.NoError(t, err)
	assert.Len(t, planned.Outstanding, len(tables))
	assert.True(t, remaining.Converged(), "outstanding after convergence: %v", statementTables(remaining.Outstanding))
	requireStorageTables(t, db)
}

// A PostgreSQL storage database missing one column reports everything that
// restores it — the ADD COLUMN, and the index that went with the column — each
// naming what it acts on, and the convergence runs exactly those.
func TestDiffStorageSchemaPostgres_ReportsMissingColumn(t *testing.T) {
	dsn, db := startPostgresStorage(t)
	logger := storageSchemaTestLogger()
	postgres := WithDialect(schema.DialectPostgres)
	require.NoError(t, EnsureSchema(dsn, logger, postgres))

	// Dropping the column takes the index that covers it with it, which is the
	// state a partly converged deploy leaves: both are outstanding, and the
	// report has to name both or the convergence looks incomplete afterwards.
	_, err := db.ExecContext(t.Context(), `ALTER TABLE "applies" DROP COLUMN "deployment"`)
	require.NoError(t, err, "drop a column the embedded schema declares")

	report, err := DiffStorageSchema(t.Context(), dsn, logger, postgres)
	require.NoError(t, err)
	assert.False(t, report.Converged())
	assert.Empty(t, report.Manual)
	require.Len(t, report.Outstanding, 2, "the column and its index diverge: %v", report.Outstanding)

	column := report.Outstanding[0]
	assert.Equal(t, "applies", column.Table)
	assert.Equal(t, postgresOpAddColumn, column.Operation)
	assert.Contains(t, strings.ToUpper(column.DDL), "ADD COLUMN")
	assert.Contains(t, column.DDL, "deployment")
	assert.Empty(t, column.Reason, "a metadata-only column converges automatically")

	index := report.Outstanding[1]
	assert.Equal(t, "applies", index.Table)
	assert.Equal(t, postgresOpCreateIndex, index.Operation)
	assert.Contains(t, strings.ToUpper(index.DDL), "CREATE INDEX")
	assert.Contains(t, index.DDL, "deployment")

	_, remaining, err := ApplyStorageSchema(t.Context(), dsn, logger, postgres)
	require.NoError(t, err)
	assert.True(t, remaining.Converged(), "outstanding after convergence: %v", remaining.Outstanding)
	assert.True(t, testutil.PostgresColumnExists(t, db, "public", "applies", "deployment"))
}

// A storage dialect with no differ fails closed rather than running another
// family's catalog queries against it, and the refusal names both the dialect
// asked for and the ones that exist.
func TestDiffStorageSchema_UnsupportedDialectFailsClosed(t *testing.T) {
	_, err := DiffStorageSchema(t.Context(), "unused", storageSchemaTestLogger(), WithDialect(schema.Dialect("sqlite")))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sqlite")
	assert.Contains(t, err.Error(), string(schema.DialectMySQL))
	assert.Contains(t, err.Error(), string(schema.DialectPostgres))
}
