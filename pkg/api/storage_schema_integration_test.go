//go:build integration

package api

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	seedutil "github.com/block/schemabot/e2e/testutil"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/postgresconn"
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

	report, err := PlanStorageSchema(t.Context(), sdb.DSN, nil, storageSchemaTestLogger())
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

	planned, remaining, err := ApplyStorageSchema(t.Context(), sdb.DSN, nil, storageSchemaTestLogger())
	require.NoError(t, err)

	assert.NotEmpty(t, planned.Outstanding, "an empty database has statements to run")
	assert.True(t, remaining.Converged(), "nothing should be outstanding after a convergence: %v", statementTables(remaining.Outstanding))
	assert.Equal(t, sdb.Name, remaining.Database)

	for _, table := range []string{"applies", "tasks", "plans", "locks", "checks", "settings", "apply_operations"} {
		assert.True(t, testutil.TableExists(t, db, sdb.Name, table), "table %s missing after convergence", table)
	}

	// The confirming read is the same read an operator would run next.
	after, err := PlanStorageSchema(t.Context(), sdb.DSN, nil, storageSchemaTestLogger())
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
	// the table is there, one column the embedded schema declares is not. The
	// column is one no index covers, so the diff is the missing column and
	// nothing else: dropping an indexed column narrows the index with it, and
	// the diff of a narrowed index is a removal the convergence refuses.
	_, err := db.ExecContext(t.Context(), "ALTER TABLE `applies` DROP COLUMN `caller`")
	require.NoError(t, err, "drop a column the embedded schema declares")
	require.False(t, testutil.ColumnExists(t, db, sdb.Name, "applies", "caller"))

	report, err := PlanStorageSchema(t.Context(), sdb.DSN, nil, storageSchemaTestLogger())
	require.NoError(t, err)

	assert.False(t, report.Converged())
	assert.Empty(t, report.Destructive, "restoring a missing column destroys nothing")
	assert.Empty(t, report.Manual)
	require.Len(t, report.Outstanding, 1, "only the one table diverges: %v", statementTables(report.Outstanding))

	statement := report.Outstanding[0]
	assert.Equal(t, "applies", statement.Table)
	assert.Equal(t, "alter_table", statement.Operation)
	assert.Contains(t, statement.DDL, "ADD COLUMN")
	assert.Contains(t, statement.DDL, "`caller`")

	// Applying the reported statement is what converges it, and nothing else
	// is left behind.
	_, remaining, err := ApplyStorageSchema(t.Context(), sdb.DSN, nil, storageSchemaTestLogger())
	require.NoError(t, err)
	assert.True(t, remaining.Converged(), "the reported statement should be the whole difference")
	assert.True(t, testutil.ColumnExists(t, db, sdb.Name, "applies", "caller"))
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

	report, err := PlanStorageSchema(t.Context(), sdb.DSN, nil, storageSchemaTestLogger())
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

	planned, remaining, err := ApplyStorageSchema(t.Context(), sdb.DSN, nil, storageSchemaTestLogger())
	require.NoError(t, err, "a convergence that refuses destructive statements still succeeds")
	assert.Len(t, planned.Destructive, 1)
	assert.Len(t, remaining.Destructive, 1, "the refused statement is still outstanding afterwards")
	assert.True(t, testutil.TableExists(t, db, sdb.Name, "newer_release_state"),
		"a refused DROP TABLE must leave the table in place")

	// The same report with destructive changes allowed says the statement would
	// run, which is what the operator opting in is asking to be told.
	allowed, err := PlanStorageSchema(t.Context(), sdb.DSN, nil, storageSchemaTestLogger(), WithDestructiveSchemaChangePolicy(DestructivePolicyPermits, false))
	require.NoError(t, err)
	assert.True(t, allowed.DestructiveAllowed)
	require.Len(t, allowed.Destructive, 1)
	assert.Equal(t, "newer_release_state", allowed.Destructive[0].Table)
}

// A surplus index is reported as refused, the same as a surplus table or
// column. Dropping one destroys no rows and still takes an index the rest of
// the fleet plans its queries around away from them, so it is inside the set
// that protects newer storage state from an older binary (AV-9).
//
// That is what an operator pre-creating an index ahead of a release relies on:
// an instance of the earlier release booting against that index leaves it in
// place, and a diff run with the earlier release's binary says so, naming the
// statement it will not run.
func TestDiffStorageSchemaMySQL_RefusesSurplusIndex(t *testing.T) {
	sdb, db := openEnsureSchemaDatabase(t)
	require.NoError(t, EnsureSchema(sdb.DSN, storageSchemaTestLogger()))

	// An index a later release declares, created ahead of that release the way
	// an operator pre-creates one to keep the startup budget clear.
	_, err := db.ExecContext(t.Context(), "CREATE INDEX `idx_applies_caller` ON `applies` (`caller`)")
	require.NoError(t, err, "pre-create an index the embedded schema does not declare")
	require.Equal(t, []string{"caller"}, testutil.IndexColumns(t, db, sdb.Name, "applies", "idx_applies_caller"))

	report, err := PlanStorageSchema(t.Context(), sdb.DSN, nil, storageSchemaTestLogger())
	require.NoError(t, err)

	assert.False(t, report.Converged(), "a refused statement is not convergence")
	assert.False(t, report.DestructiveAllowed)
	assert.Empty(t, report.Outstanding, "the drop is the only difference, and it is refused")
	assert.Empty(t, report.Manual)
	require.Len(t, report.Destructive, 1, "only the one table diverges: %v", statementTables(report.Destructive))
	statement := report.Destructive[0]
	assert.Equal(t, "applies", statement.Table)
	assert.Equal(t, "alter_table", statement.Operation)
	assert.Contains(t, statement.DDL, "idx_applies_caller")
	assert.NotEmpty(t, statement.Reason, "a refusal has to say why it was refused")

	// And a convergence leaves it intact, which is what a boot of this binary
	// does to an index a later release owns.
	_, remaining, err := ApplyStorageSchema(t.Context(), sdb.DSN, nil, storageSchemaTestLogger())
	require.NoError(t, err, "a convergence that refuses destructive statements still succeeds")
	assert.Len(t, remaining.Destructive, 1, "the refused statement is still outstanding afterwards")
	assert.Equal(t, []string{"caller"}, testutil.IndexColumns(t, db, sdb.Name, "applies", "idx_applies_caller"),
		"a surplus index must survive intact, like a surplus table or column")

	// An operator who removed the index from the embedded schema on purpose
	// opts in, and the report then says the statement would run.
	allowed, err := PlanStorageSchema(t.Context(), sdb.DSN, nil, storageSchemaTestLogger(), WithDestructiveSchemaChangePolicy(DestructivePolicyPermits, false))
	require.NoError(t, err)
	assert.True(t, allowed.DestructiveAllowed)
	require.Len(t, allowed.Destructive, 1)
	assert.Equal(t, "applies", allowed.Destructive[0].Table)
}

// A report separates what this call may run from what the next pod to boot
// will run, because only one of them is moved by the caller asking.
//
// An operator converging a later release's schema ahead of the deploy is
// asking whether the state they apply survives until the deploy, and the
// answer belongs to the boots in between. Those read the deployment's config
// and have never heard of this request, so a per-request opt-in has to leave
// the boot's answer exactly where it was. Reported as one field, an operator
// passing --allow-unsafe would be told their own flag had changed what the
// fleet does.
func TestDiffStorageSchemaMySQL_BootPolicyIsNotMovedByTheRequestOptIn(t *testing.T) {
	sdb, db := openEnsureSchemaDatabase(t)
	require.NoError(t, EnsureSchema(sdb.DSN, storageSchemaTestLogger()))

	_, err := db.ExecContext(t.Context(), "CREATE INDEX `idx_applies_caller` ON `applies` (`caller`)")
	require.NoError(t, err, "pre-create an index the embedded schema does not declare")

	// A deployment that configured nothing, on the run the destructive gate's
	// own rerun hint asks for.
	optedIn, err := PlanStorageSchema(t.Context(), sdb.DSN, nil, storageSchemaTestLogger(),
		WithDestructiveSchemaChangePolicy(DestructivePolicyForbids, true))
	require.NoError(t, err)
	require.Len(t, optedIn.Destructive, 1, "the surplus index is the drift under test")
	assert.True(t, optedIn.DestructiveAllowed, "this convergence may drop it")
	assert.Equal(t, apitypes.BootRemovalPreserves, optedIn.BootRemovalPolicy,
		"and every boot of the deployed release still refuses to, which is what keeps pre-applied state alive")

	// The same drift on a deployment that has permitted destructive storage
	// changes: here the boots really do drop it, and only the config says so.
	configured, err := PlanStorageSchema(t.Context(), sdb.DSN, nil, storageSchemaTestLogger(),
		WithDestructiveSchemaChangePolicy(DestructivePolicyPermits, false))
	require.NoError(t, err)
	assert.True(t, configured.DestructiveAllowed)
	assert.Equal(t, apitypes.BootRemovalRemoves, configured.BootRemovalPolicy)

	// And a deployment that permitted nothing, where neither this run nor a
	// boot removes anything.
	neither, err := PlanStorageSchema(t.Context(), sdb.DSN, nil, storageSchemaTestLogger(),
		WithDestructiveSchemaChangePolicy(DestructivePolicyForbids, false))
	require.NoError(t, err)
	assert.False(t, neither.DestructiveAllowed)
	assert.Equal(t, apitypes.BootRemovalPreserves, neither.BootRemovalPolicy)

	// A database addressed directly, with no deployment config to read. The
	// run itself is still refused, but the boot's answer is nobody's to give:
	// reported as preservation, an operator pre-applying against a fleet that
	// drops surplus state would be told it survives.
	unread, err := PlanStorageSchema(t.Context(), sdb.DSN, nil, storageSchemaTestLogger())
	require.NoError(t, err)
	assert.False(t, unread.DestructiveAllowed, "an unread policy grants this run nothing")
	assert.Equal(t, apitypes.BootRemovalUnknown, unread.BootRemovalPolicy)
}

// One table can drift in both directions at once: it misses a column the
// running binary declares and holds an index a later release owns. The differ
// emits that as a single ALTER, and the report has to say what the boot will
// actually do with it, which is run the addition and withhold the drop. An
// operator reading only "destructive" here would expect the column to stay
// missing and go looking for a convergence that never comes.
func TestDiffStorageSchemaMySQL_ReportsBothHalvesOfAMixedStatement(t *testing.T) {
	sdb, db := openEnsureSchemaDatabase(t)
	require.NoError(t, EnsureSchema(sdb.DSN, storageSchemaTestLogger()))

	const missingColumn = "throttle_reason"
	_, err := db.ExecContext(t.Context(), "CREATE INDEX `idx_tasks_later_release` ON `tasks` (`environment`)")
	require.NoError(t, err, "pre-create an index the embedded schema does not declare")
	_, err = db.ExecContext(t.Context(), fmt.Sprintf("ALTER TABLE `tasks` DROP COLUMN `%s`", missingColumn))
	require.NoError(t, err, "drop a column the embedded schema does declare")

	report, err := PlanStorageSchema(t.Context(), sdb.DSN, nil, storageSchemaTestLogger())
	require.NoError(t, err)
	assert.False(t, report.Converged())

	require.Len(t, report.Outstanding, 1, "the addition is reported as a statement that will run: %v", statementTables(report.Outstanding))
	assert.Equal(t, "tasks", report.Outstanding[0].Table)
	assert.Contains(t, report.Outstanding[0].DDL, missingColumn)
	assert.NotContains(t, report.Outstanding[0].DDL, "DROP",
		"the half that will run carries no removal")

	require.Len(t, report.Destructive, 1, "the removal is reported as a statement that will not run")
	assert.Equal(t, "tasks", report.Destructive[0].Table)
	assert.Contains(t, report.Destructive[0].DDL, "idx_tasks_later_release")
	assert.NotContains(t, report.Destructive[0].DDL, missingColumn,
		"the half that will not run carries no addition")
	assert.NotEmpty(t, report.Destructive[0].Reason)

	// The convergence does what the report said: the column lands, the index
	// survives, and the refusal is still outstanding afterwards.
	_, remaining, err := ApplyStorageSchema(t.Context(), sdb.DSN, nil, storageSchemaTestLogger())
	require.NoError(t, err)
	assert.True(t, testutil.ColumnExists(t, db, sdb.Name, "tasks", missingColumn),
		"the addition the report listed as outstanding must have run")
	assert.Equal(t, []string{"environment"}, testutil.IndexColumns(t, db, sdb.Name, "tasks", "idx_tasks_later_release"),
		"the index a later release owns must survive")
	assert.Empty(t, remaining.Outstanding, "nothing additive is left")
	assert.Len(t, remaining.Destructive, 1, "the withheld clause is still waiting for an operator")
}

// A diff is safe to run against a storage database another process is
// converging: it takes no advisory lock, so it answers while the bootstrap
// holds one rather than blocking behind it. During an incident that is the
// difference between reading the state and waiting on it.
func TestDiffStorageSchemaMySQL_ReadsWhileBootstrapHoldsLock(t *testing.T) {
	sdb := newStorageDatabase(t)
	require.NoError(t, EnsureSchema(sdb.DSN, storageSchemaTestLogger()))

	holdMySQLBootstrapLock(t, sdb.DSN)

	report, err := PlanStorageSchema(t.Context(), sdb.DSN, nil, storageSchemaTestLogger())
	require.NoError(t, err, "a diff must not wait on the bootstrap lock")
	assert.True(t, report.Converged())
}

// holdMySQLBootstrapLock takes the storage bootstrap's advisory lock on a
// dedicated session, the way a converging pod holds it, and releases it when
// the test ends.
//
// The release is the session ending, not a RELEASE_LOCK: the lock lives on a
// session, so releasing it by name needs a query, and a query needs a context
// that is still live — t.Context() is already cancelled when cleanup runs.
// Handing the holder its own pool makes the session's end something cleanup
// can cause without a context, by returning the connection and closing the
// pool behind it.
//
// Releasing deterministically matters more than the single test suggests.
// MySQL scopes lock names to the server rather than to a database and these
// tests share a server, so a leaked lock would not stall this test's own
// database: it would stall every other test's convergence on that server.
func holdMySQLBootstrapLock(t *testing.T, dsn string) {
	t.Helper()
	db := openStorageDB(t, dsn)
	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	var acquired int
	require.NoError(t, conn.QueryRowContext(t.Context(), "SELECT GET_LOCK(?, 5)", ensureSchemaLockName).Scan(&acquired))
	require.Equal(t, 1, acquired, "hold the bootstrap lock")
	// Registered after openStorageDB's own close, so it runs before it: the
	// connection goes back to the pool, then the pool closes and takes the
	// session with it.
	t.Cleanup(func() { utils.CloseAndLog(conn) })
}

// A convergence in progress is invisible in a diff — a statement it is working
// on stays out of the live catalog until it finishes with it, so a database
// being converged reports the same outstanding statements as one nobody has
// touched. The report says so separately, which is what tells an operator
// whose session dropped that their run is still going, and what keeps a second
// operator from starting an apply that would do nothing but wait out the first
// one's budget on a lock.
func TestDiffStorageSchemaMySQL_ReportsAConvergenceInFlight(t *testing.T) {
	sdb := newStorageDatabase(t)
	logger := storageSchemaTestLogger()
	require.NoError(t, EnsureSchema(sdb.DSN, logger))

	report, err := PlanStorageSchema(t.Context(), sdb.DSN, nil, logger)
	require.NoError(t, err)
	require.False(t, report.ConvergenceInFlight, "nothing is converging this database")

	holdMySQLBootstrapLock(t, sdb.DSN)

	report, err = PlanStorageSchema(t.Context(), sdb.DSN, nil, logger)
	require.NoError(t, err)
	assert.True(t, report.ConvergenceInFlight,
		"a held bootstrap lock is a convergence in flight")
	assert.True(t, report.Converged(),
		"the statement sets are unchanged by another instance holding the lock")
}

// The PostgreSQL twin: the same lock, read out of a different catalog, means
// the same thing. An operator reading two deployments' reports gets the same
// answer to the same question whichever family the storage runs on.
func TestDiffStorageSchemaPostgres_ReportsAConvergenceInFlight(t *testing.T) {
	dsn, _ := startPostgresStorage(t)
	logger := storageSchemaTestLogger()
	postgres := WithDialect(schema.DialectPostgres)
	require.NoError(t, EnsureSchema(dsn, logger, postgres))

	report, err := PlanStorageSchema(t.Context(), dsn, nil, logger, postgres)
	require.NoError(t, err)
	require.False(t, report.ConvergenceInFlight, "nothing is converging this database")

	holdPostgresBootstrapLock(t, dsn)

	report, err = PlanStorageSchema(t.Context(), dsn, nil, logger, postgres)
	require.NoError(t, err)
	assert.True(t, report.ConvergenceInFlight,
		"a held bootstrap lock is a convergence in flight")
	assert.True(t, report.Converged(),
		"the statement sets are unchanged by another instance holding the lock")
}

// An empty PostgreSQL storage database reports every embedded table as a
// CREATE TABLE, and converging it leaves nothing outstanding. The additive
// convergence never drops anything, so the report has no destructive set at
// all on this dialect.
func TestDiffStorageSchemaPostgres_ConvergesEmptyDatabase(t *testing.T) {
	dsn, db := startPostgresStorage(t)
	logger := storageSchemaTestLogger()
	postgres := WithDialect(schema.DialectPostgres)

	report, err := PlanStorageSchema(t.Context(), dsn, nil, logger, postgres)
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

	planned, remaining, err := ApplyStorageSchema(t.Context(), dsn, nil, logger, postgres)
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

	report, err := PlanStorageSchema(t.Context(), dsn, nil, logger, postgres)
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

	_, remaining, err := ApplyStorageSchema(t.Context(), dsn, nil, logger, postgres)
	require.NoError(t, err)
	assert.True(t, remaining.Converged(), "outstanding after convergence: %v", remaining.Outstanding)
	assert.True(t, testutil.PostgresColumnExists(t, db, "public", "applies", "deployment"))
}

// The question a deploy actually asks is whether the storage is ready for the
// release about to roll, not whether it matches the release that is running. A
// database converged against the running schema still reports the next
// release's column as outstanding when the diff is given that release's schema
// files, and the report attributes the answer to those files rather than to the
// binary that read them.
//
// The live side is unaffected by any of it: the column is reported because the
// catalog does not have it, which is why the answer stays correct on a database
// a failed deploy left half converged.
func TestDiffStorageSchemaMySQL_DiffsAgainstASuppliedSchema(t *testing.T) {
	sdb, db := openEnsureSchemaDatabase(t)
	require.NoError(t, EnsureSchema(sdb.DSN, storageSchemaTestLogger()))

	converged, err := PlanStorageSchema(t.Context(), sdb.DSN, EmbeddedStorageSchema("v1.2.3"), storageSchemaTestLogger())
	require.NoError(t, err)
	require.True(t, converged.Converged(), "outstanding against the running schema: %v", statementTables(converged.Outstanding))
	assert.Equal(t, "the schema embedded in v1.2.3", converged.SchemaSource)

	// The next release's schema: the running one, plus a column on one table.
	files, err := storageSchemaFilesForTest()
	require.NoError(t, err)
	files["applies.sql"] = strings.Replace(files["applies.sql"],
		"PRIMARY KEY (`id`)", "`release_note` varchar(255) NOT NULL DEFAULT '',\n  PRIMARY KEY (`id`)", 1)
	require.Contains(t, files["applies.sql"], "release_note", "the fixture must actually declare the new column")
	desired, err := StorageSchemaFromFiles("the schema files of release v1.4.0", files)
	require.NoError(t, err)

	report, err := PlanStorageSchema(t.Context(), sdb.DSN, desired, storageSchemaTestLogger())
	require.NoError(t, err)
	assert.False(t, report.Converged(), "the next release's column is not on this database yet")
	assert.Equal(t, "the schema files of release v1.4.0", report.SchemaSource)
	assert.Empty(t, report.Destructive)
	assert.Empty(t, report.Manual)
	require.Len(t, report.Outstanding, 1, "only the one table diverges: %v", statementTables(report.Outstanding))
	assert.Equal(t, "applies", report.Outstanding[0].Table)
	assert.Contains(t, report.Outstanding[0].DDL, "ADD COLUMN")
	assert.Contains(t, report.Outstanding[0].DDL, "`release_note`")

	// Attribution survives the stamp the responder applies: the answer came
	// from the supplied files, whoever read them.
	report.AttributeTo("v1.2.3")
	assert.Equal(t, "the schema files of release v1.4.0", report.SchemaSource)
	assert.Equal(t, "v1.2.3", report.Version)

	// The convergence runs the supplied schema, which is the whole point of the
	// command: the column the next release declares is on the database before
	// any pod of that release starts.
	planned, remaining, err := ApplyStorageSchema(t.Context(), sdb.DSN, desired, storageSchemaTestLogger())
	require.NoError(t, err)
	require.Len(t, planned.Outstanding, 1, "the supplied schema's one statement is what ran: %v", statementTables(planned.Outstanding))
	assert.True(t, remaining.Converged(), "outstanding against the supplied schema after converging it: %v", statementTables(remaining.Outstanding))
	assert.Equal(t, "the schema files of release v1.4.0", remaining.SchemaSource,
		"a convergence is attributed to the schema whose files it ran")
	assert.True(t, testutil.ColumnExists(t, db, sdb.Name, "applies", "release_note"),
		"the supplied schema's column has to be on the database, or the convergence ran the embedded files instead")

	// And it survives the running release. Against the schema this binary
	// carries the column is now surplus, which is a DROP COLUMN — refused
	// unless destroying storage state was permitted — so every pod of the
	// deployed release leaves it in place until that release rolls (AV-9).
	running, err := PlanStorageSchema(t.Context(), sdb.DSN, nil, storageSchemaTestLogger())
	require.NoError(t, err)
	assert.Empty(t, running.Outstanding, "nothing about the running schema runs automatically here: %v", statementTables(running.Outstanding))
	surplus := statementFor(t, running.Destructive, "applies")
	assert.Contains(t, surplus.DDL, "DROP COLUMN")
	assert.Contains(t, surplus.DDL, "`release_note`")

	require.NoError(t, EnsureSchema(sdb.DSN, storageSchemaTestLogger()), "a boot of the running release must not fail on surplus state")
	assert.True(t, testutil.ColumnExists(t, db, sdb.Name, "applies", "release_note"),
		"a pre-applied column survives a boot of the release that does not declare it")
}

// An operator converges a later release's schema ahead of its roll, and that
// schema declares an index the deployed release does not. This is the case the
// confirmation's notice makes a prediction about, so the prediction is checked
// here against what a boot of the deployed release actually does: it refuses
// the drop and leaves the index in place, exactly as it does for a table or a
// column, and it says so in the log every time a pod starts. An operator told
// the opposite would schedule the deploy window around an index that was never
// at risk, and one not told about the logging would read a fleet warning on
// every boot as an incident.
func TestApplyStorageSchemaMySQL_PreAppliedIndexSurvivesTheRunningRelease(t *testing.T) {
	const preAppliedIndex = "idx_release_caller_created"
	preAppliedColumns := []string{"caller", "created_at"}

	sdb, db := openEnsureSchemaDatabase(t)
	require.NoError(t, EnsureSchema(sdb.DSN, storageSchemaTestLogger()))

	// The next release's schema: the running one, plus an index on one table.
	files, err := storageSchemaFilesForTest()
	require.NoError(t, err)
	const lastKey = "KEY `idx_completed_at_state` (`completed_at`,`state`)"
	require.Contains(t, files["applies.sql"], lastKey, "the fixture this test appends to has moved")
	files["applies.sql"] = strings.Replace(files["applies.sql"], lastKey,
		lastKey+",\n  KEY `"+preAppliedIndex+"` (`caller`,`created_at`)", 1)
	desired, err := StorageSchemaFromFiles("the schema files of release v1.4.0", files)
	require.NoError(t, err)

	_, remaining, err := ApplyStorageSchema(t.Context(), sdb.DSN, desired, storageSchemaTestLogger())
	require.NoError(t, err)
	require.True(t, remaining.Converged(), "outstanding against the supplied schema after converging it: %v", statementTables(remaining.Outstanding))
	require.Equal(t, preAppliedColumns, testutil.IndexColumns(t, db, sdb.Name, "applies", preAppliedIndex),
		"the named schema's index has to be on the database, or the convergence ran the embedded files instead")

	// Against the running release the index is now surplus, and the drop that
	// would remove it is refused rather than outstanding.
	running, err := PlanStorageSchema(t.Context(), sdb.DSN, nil, storageSchemaTestLogger())
	require.NoError(t, err)
	assert.Empty(t, running.Outstanding, "nothing about the running schema runs automatically here: %v", statementTables(running.Outstanding))
	surplus := statementFor(t, running.Destructive, "applies")
	assert.Contains(t, surplus.DDL, "DROP INDEX")
	assert.Contains(t, surplus.DDL, preAppliedIndex)

	// So every pod of the deployed release leaves it in place, and logs the
	// refusal naming it, until the release that declares it rolls (AV-9).
	var logBuf syncBuffer
	bootLogger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	require.NoError(t, EnsureSchema(sdb.DSN, bootLogger), "a boot of the running release must not fail on a surplus index")
	assert.Equal(t, preAppliedColumns, testutil.IndexColumns(t, db, sdb.Name, "applies", preAppliedIndex),
		"a pre-applied index survives a boot of the release that does not declare it, like a pre-applied column")

	logs := logBuf.String()
	assert.Contains(t, logs, "refusing destructive storage-schema change")
	assert.Contains(t, logs, preAppliedIndex,
		"the refusal has to name the index, or an operator cannot tell which object the fleet is warning about")
}

// The PostgreSQL bootstrap converges the schema it is given too, which the
// MySQL test cannot show for it: the two dialects read their files and compute
// their drift in separate code, so a source that reached one and not the other
// would converge the running release's schema while reporting the named one.
//
// The supplied set is deliberately one table. An empty storage database
// converged against it has exactly that table afterwards, and would have every
// storage table if the embedded files had been used instead.
func TestApplyStorageSchemaPostgres_ConvergesASuppliedSchema(t *testing.T) {
	dsn, db := startPostgresStorage(t)
	logger := storageSchemaTestLogger()
	postgres := WithDialect(schema.DialectPostgres)

	desired, err := StorageSchemaFromFiles("the schema files of release v1.4.0", map[string]string{
		"locks.sql": `CREATE TABLE IF NOT EXISTS "locks" (
	"name" TEXT PRIMARY KEY,
	"owner" TEXT NOT NULL
)`,
	})
	require.NoError(t, err)

	planned, remaining, err := ApplyStorageSchema(t.Context(), dsn, desired, logger, postgres)
	require.NoError(t, err)
	require.Len(t, planned.Outstanding, 1, "the supplied schema declares one table: %v", statementTables(planned.Outstanding))
	assert.Equal(t, "locks", planned.Outstanding[0].Table)
	assert.True(t, remaining.Converged(), "outstanding after converging the supplied schema: %v", statementTables(remaining.Outstanding))
	assert.Equal(t, "the schema files of release v1.4.0", remaining.SchemaSource)

	assert.True(t, postgresTableExists(t, db, "locks"), "the supplied schema's table must be created")
	assert.False(t, postgresTableExists(t, db, "applies"),
		"a table only the embedded schema declares must not appear, or the convergence ran the embedded files")
}

// postgresTableExists reports whether one table is in the storage database's
// current schema.
func postgresTableExists(t *testing.T, db *sql.DB, table string) bool {
	t.Helper()
	var exists bool
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables
			WHERE table_schema = current_schema() AND table_name = $1)`, table).Scan(&exists))
	return exists
}

// storageSchemaFilesForTest is the embedded MySQL schema as a file-name map, the
// shape a caller-supplied schema takes.
func storageSchemaFilesForTest() (map[string]string, error) {
	schemaFiles, err := readEmbeddedSchemaFiles()
	if err != nil {
		return nil, err
	}
	namespace := schemaFiles[storageSchemaNamespace]
	if namespace == nil {
		return nil, fmt.Errorf("embedded schema has no %q namespace", storageSchemaNamespace)
	}
	files := make(map[string]string, len(namespace.Files))
	maps.Copy(files, namespace.Files)
	return files, nil
}

// A storage dialect with no differ fails closed rather than running another
// family's catalog queries against it, and the refusal names both the dialect
// asked for and the ones that exist.
func TestDiffStorageSchema_UnsupportedDialectFailsClosed(t *testing.T) {
	_, err := PlanStorageSchema(t.Context(), "unused", nil, storageSchemaTestLogger(), WithDialect(schema.Dialect("sqlite")))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sqlite")
	assert.Contains(t, err.Error(), string(schema.DialectMySQL))
	assert.Contains(t, err.Error(), string(schema.DialectPostgres))
}

// An operator watching a convergence can stop it, and the whole point is that
// it stops rather than running on out of sight. The case here is the one that
// makes an operator reach for Ctrl-C: a second convergence queued behind a
// first, waiting on the bootstrap lock for as long as the run ahead of it
// takes. Stopping returns promptly, says nothing was changed, and does not
// name a budget — none fired.
func TestApplyStorageSchemaMySQL_StopsWhenItsCallerStops(t *testing.T) {
	sdb, db := openEnsureSchemaDatabase(t)
	logger := storageSchemaTestLogger()
	require.NoError(t, EnsureSchema(sdb.DSN, logger))

	// Drift the schema, so the run reaches the lock rather than short-circuiting
	// on a converged catalog.
	_, err := db.ExecContext(t.Context(), "ALTER TABLE `applies` DROP COLUMN `deployment`")
	require.NoError(t, err)

	// A convergence already in progress: the lock is held by somebody else for
	// the whole of this test.
	holdMySQLBootstrapLock(t, sdb.DSN)

	ctx, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, _, err := ApplyStorageSchema(ctx, sdb.DSN, nil, logger)
		done <- err
	}()

	// Stop it once it is demonstrably on the lock rather than still diffing.
	// Waiting for the waiter itself, instead of for a duration chosen to be
	// longer than a diff, is what keeps the test from stopping a run that had
	// not got there yet on a slow machine — which would pass while proving
	// nothing.
	waitForEnsureSchemaLockWaiter(t, db, sdb.Name)
	stop()

	select {
	case err := <-done:
		require.Error(t, err, "a stopped convergence is not a successful one")
		assert.Contains(t, err.Error(), "this run changed nothing")
		assert.NotContains(t, err.Error(), "timed out",
			"an operator stopping a run must not be told a budget expired")
	case <-time.After(storageConvergenceStopDeadline):
		t.Fatal("a stopped convergence must return rather than wait out its budget")
	}

	// It stopped before taking the lock, so the drift it was going to fix is
	// still there and nothing half-ran.
	assert.False(t, testutil.ColumnExists(t, db, sdb.Name, "applies", "deployment"))
}

// A stop that arrives while the DDL is running stops the DDL, not only the
// operator's wait for it. This is the window a stop almost always lands in:
// the convergence polls the engine in-process, so the loop spends all of its
// time blocked between polls, and returning from there without cancelling the
// engine would release the bootstrap lock and leave a table copy running
// against the storage database under a convergence nobody is watching any
// more (AV-13).
func TestApplyStorageSchemaMySQL_StopsTheSchemaChangeItStarted(t *testing.T) {
	sdb, db := openEnsureSchemaDatabase(t)
	logger := storageSchemaTestLogger()
	require.NoError(t, EnsureSchema(sdb.DSN, logger))

	// Seed before the drift, so the rows are indexed on the way in rather than
	// by the statement under test.
	seedutil.SeedRows(t, sdb.DSN, "`applies`",
		"apply_identifier, lock_id, plan_id, database_name, database_type, repository, pull_request, environment, engine, state, options",
		"CONCAT('seed-', seq), 0, 0, 'seed', 'mysql', 'example/repo', 0, 'development', 'spirit', 'queued', '{}'",
		storageConvergenceCopyRows)

	// An index the embedded schema declares, dropped. Converging it back is an
	// ADD INDEX, which always copies the table rather than running instantly,
	// so the convergence has a copy in flight for the stop to land inside.
	_, err := db.ExecContext(t.Context(), "ALTER TABLE `applies` DROP INDEX `idx_completed_at_state`")
	require.NoError(t, err)

	ctx, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, _, err := ApplyStorageSchema(ctx, sdb.DSN, nil, logger)
		done <- err
	}()

	// The shadow table is the copy: Spirit creates it, alters it, and fills it,
	// so its appearance is the convergence being somewhere inside the DDL
	// rather than still diffing or waiting on the lock.
	require.Eventually(t, func() bool {
		return testutil.TableExists(t, db, sdb.Name, "_applies_new")
	}, storageConvergenceStopDeadline, 100*time.Millisecond,
		"expected the convergence to start copying the table")
	stop()

	select {
	case err := <-done:
		require.Error(t, err, "a stopped convergence is not a successful one")
		assert.Contains(t, err.Error(), "storage schema convergence stopped")
		assert.NotContains(t, err.Error(), "did not complete within",
			"an operator stopping a run must not be told a budget expired")
	case <-time.After(storageConvergenceStopDeadline):
		t.Fatal("a stopped convergence must return rather than run its copy to completion")
	}

	// The copy went with the stop. A shadow table still standing here is a
	// copy still running: the stop released the bootstrap lock, so nothing is
	// serializing it against the next pod to boot.
	assert.False(t, testutil.TableExists(t, db, sdb.Name, "_applies_new"),
		"stopping the convergence must cancel the schema change, not just stop waiting for it")

	// And it stopped before cutting over, so the index it was adding is still
	// missing — which is what the next plan will say.
	assert.False(t, testutil.IndexExists(t, db, sdb.Name, "applies", "idx_completed_at_state"))
}

// storageConvergenceCopyRows is how many rows the stop-mid-copy test puts in
// the table it has converged. It is sized so the copy is still running when
// the stop arrives: a table that copies faster than the test can cancel it
// would pass by finishing rather than by stopping.
const storageConvergenceCopyRows = 300_000

// The PostgreSQL twin. The lock is the same lock and the stop means the same
// thing, so an operator gets the same answer whichever family their storage
// runs on.
func TestApplyStorageSchemaPostgres_StopsWhenItsCallerStops(t *testing.T) {
	dsn, db := startPostgresStorage(t)
	logger := storageSchemaTestLogger()
	postgres := WithDialect(schema.DialectPostgres)
	require.NoError(t, EnsureSchema(dsn, logger, postgres))

	_, err := db.ExecContext(t.Context(), `ALTER TABLE applies DROP COLUMN caller`)
	require.NoError(t, err)

	holdPostgresBootstrapLock(t, dsn)

	ctx, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, _, err := ApplyStorageSchema(ctx, dsn, nil, logger, postgres)
		done <- err
	}()

	waitForPostgresBootstrapLockWaiter(t, db)
	stop()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "this run changed nothing")
		assert.NotContains(t, err.Error(), "timed out")
	case <-time.After(storageConvergenceStopDeadline):
		t.Fatal("a stopped convergence must return rather than wait out its budget")
	}

	columns, err := postgresTableColumns(t.Context(), db, "applies")
	require.NoError(t, err)
	assert.False(t, columns["caller"])
}

// storageConvergenceStopDeadline bounds how long a stopped convergence may
// take to return. It is generous against the stop itself and tiny against the
// budget the run would otherwise sit on the lock for, which is the difference
// these tests exist to prove.
const storageConvergenceStopDeadline = 20 * time.Second

// holdPostgresBootstrapLock takes the storage bootstrap's advisory lock on a
// dedicated session, the way a converging pod holds it, and releases it when
// the test ends.
//
// It holds the lock on its own pool for the reason its MySQL twin does: the
// lock lives on a session, closing a pooled connection only returns that
// session to the pool, and cleanup has no live context to release the lock by
// name on. Closing the pool behind the returned connection ends the session,
// which is what the release actually is.
func holdPostgresBootstrapLock(t *testing.T, dsn string) {
	t.Helper()
	db, err := postgresconn.Open(dsn)
	require.NoError(t, err, "open the lock holder's own pool")
	t.Cleanup(func() { utils.CloseAndLog(db) })

	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	acquired, err := namedlock.Postgres{}.Acquire(t.Context(), conn, ensureSchemaLockName, 5*time.Second)
	require.NoError(t, err)
	require.True(t, acquired, "hold the bootstrap lock")
	// Registered after the pool's close so it runs before it, returning the
	// session for that close to end.
	t.Cleanup(func() { utils.CloseAndLog(conn) })
}

// waitForPostgresBootstrapLockWaiter blocks until a session other than this
// one is waiting on an advisory lock in the database db is connected to. It is
// the PostgreSQL counterpart of waitForEnsureSchemaLockWaiter: a convergence
// queued behind the holder blocks in pg_advisory_lock, which the server
// reports as an ungranted advisory row.
//
// Advisory locks are per-database here, so the database predicate is the
// connection rather than a name to match — unlike MySQL, where the
// server-wide PROCESSLIST would show every other test's waiter too.
func waitForPostgresBootstrapLockWaiter(t *testing.T, db *sql.DB) {
	t.Helper()
	var count int
	require.Eventually(t, func() bool {
		err := db.QueryRowContext(t.Context(),
			`SELECT COUNT(*) FROM pg_locks
			  WHERE locktype = 'advisory'
			    AND NOT granted
			    AND pid <> pg_backend_pid()
			    AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`,
		).Scan(&count)
		require.NoError(t, err)
		return count > 0
	}, storageConvergenceStopDeadline, 100*time.Millisecond,
		"expected the convergence to be waiting on the bootstrap advisory lock, waiter count: %d", count)
}

// A convergence an operator is watching reports itself as it runs. Without it
// the command is silent between the plan and the answer, which on a storage
// database with any history is the whole of a long index build — and a blank
// terminal is what gets a run killed that was about to finish.
func TestApplyStorageSchemaMySQL_ReportsProgressToAWatchingCaller(t *testing.T) {
	sdb, _ := openEnsureSchemaDatabase(t)
	logger := storageSchemaTestLogger()

	var observed []StorageConvergenceProgress
	_, remaining, err := ApplyStorageSchema(t.Context(), sdb.DSN, nil, logger,
		WithConvergenceProgress(func(p StorageConvergenceProgress) { observed = append(observed, p) }))
	require.NoError(t, err)
	require.True(t, remaining.Converged())

	require.NotEmpty(t, observed, "a convergence that ran DDL must have said so")
	for _, p := range observed {
		assert.NotEmpty(t, p.State, "an observation with no state says nothing an operator can read")
		assert.Positive(t, p.DDLCount, "an observation names how much work the run is doing")
	}
	assert.Equal(t, string(engine.StateCompleted), observed[len(observed)-1].State,
		"the last thing an operator sees is the run finishing")
}

// The per-table lines are the ones an operator watches through a long copy,
// and the phase on them is the engine's own word passed through rather than a
// rendering of it — so an alert written against one keeps matching.
//
// It takes a real table copy to see any of this. A convergence over a fresh
// database only creates tables, and creates report no per-table progress at
// all, which leaves the whole per-table path unexercised by a run that looks
// like it covers it.
func TestApplyStorageSchemaMySQL_ReportsTheEnginesOwnPerTableVocabulary(t *testing.T) {
	sdb, db := openEnsureSchemaDatabase(t)
	logger := storageSchemaTestLogger()
	require.NoError(t, EnsureSchema(sdb.DSN, logger))

	// Seeded before the drift, so the rows are indexed on the way in rather
	// than by the statement under test.
	seedutil.SeedRows(t, sdb.DSN, "`applies`",
		"apply_identifier, lock_id, plan_id, database_name, database_type, repository, pull_request, environment, engine, state, options",
		"CONCAT('seed-', seq), 0, 0, 'seed', 'mysql', 'example/repo', 0, 'development', 'spirit', 'queued', '{}'",
		storageConvergenceCopyRows)

	// Converging a dropped index back is an ADD INDEX, which always copies the
	// table rather than running instantly, so there is a copy to report on.
	_, err := db.ExecContext(t.Context(), "ALTER TABLE `applies` DROP INDEX `idx_completed_at_state`")
	require.NoError(t, err)

	var observed []StorageConvergenceProgress
	_, remaining, err := ApplyStorageSchema(t.Context(), sdb.DSN, nil, logger,
		WithConvergenceProgress(func(p StorageConvergenceProgress) { observed = append(observed, p) }))
	require.NoError(t, err)
	require.True(t, remaining.Converged())

	states := map[string]bool{}
	for _, p := range observed {
		for _, tp := range p.Tables {
			if tp.Table == "applies" && tp.State != "" {
				states[tp.State] = true
			}
		}
	}
	require.NotEmpty(t, states, "a convergence that copied a table must report that table's phases")

	// Taken from the engine's own enum rather than listed here, so a state it
	// gains is in the set the moment it exists. A phase outside it is
	// SchemaBot inventing a word for something the engine already named,
	// which is how a documented status an operator matches on comes to be one
	// no line ever carries.
	enginePhases := map[string]bool{string(engine.StateCompleted): true}
	for s := status.Initial; s <= status.ErrCleanup; s++ {
		enginePhases[s.String()] = true
	}
	require.NotContains(t, enginePhases, "unknown",
		"the range must cover the enum; String() falling through would make this assertion vacuous")
	for phase := range states {
		assert.True(t, enginePhases[phase],
			"per-table phase %q is not one the engine names; phases are passed through rather than translated", phase)
	}
	assert.True(t, states["copyRows"],
		"a table copy of this size is observed mid-copy, under the engine's name for that phase")
}

// The PostgreSQL convergence has no engine polling behind it, so it reports
// what it does know: which table it is about to converge, and how much of the
// drift set is behind it. That is the answer an operator waiting on a long
// CREATE INDEX is after, and it is the honest one — each table runs in one
// transaction, so there is no partial state to report.
func TestApplyStorageSchemaPostgres_ReportsProgressToAWatchingCaller(t *testing.T) {
	dsn, _ := startPostgresStorage(t)
	logger := storageSchemaTestLogger()
	postgres := WithDialect(schema.DialectPostgres)

	var observed []StorageConvergenceProgress
	_, remaining, err := ApplyStorageSchema(t.Context(), dsn, nil, logger, postgres,
		WithConvergenceProgress(func(p StorageConvergenceProgress) { observed = append(observed, p) }))
	require.NoError(t, err)
	require.True(t, remaining.Converged())

	require.NotEmpty(t, observed)
	named := make(map[string]bool)
	for _, p := range observed {
		require.Len(t, p.Tables, 1, "a PostgreSQL observation is about the one table in flight")
		named[p.Tables[0].Table] = true
		assert.Contains(t, []string{postgresConvergenceRunning, postgresConvergenceComplete}, p.State)
	}
	tables, _, err := readEmbeddedPostgresSchemaFiles()
	require.NoError(t, err)
	for _, table := range tables {
		assert.True(t, named[table], "every table the convergence created should have been reported: %s", table)
	}
	assert.Equal(t, 100, observed[len(observed)-1].Percent,
		"the last observation of a clean convergence is the whole drift set behind it")

	// Every dialect ends a table with the same word, so a reader of these
	// observations can tell a finished subject from a running one without
	// knowing which dialect produced it. That is what decides whether there is
	// anything left to report about it, and a per-dialect spelling would make
	// it a lookup table instead of a check.
	last := observed[len(observed)-1]
	assert.True(t, engine.State(last.State).IsTerminal(),
		"a converged run ends in the shared terminal state, not a spelling of this dialect's own: %q", last.State)
	assert.True(t, engine.State(last.Tables[0].State).IsTerminal(),
		"and so does the table it ended on: %q", last.Tables[0].State)
}
