//go:build integration

package spirit

import (
	"database/sql"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/pendingdrops"
	"github.com/block/schemabot/pkg/schema"
)

// testDropTablesQuietPeriod is the quiet period the tests that plan a drop
// give the engine, short enough that the shared test server has been up for
// all of it.
const testDropTablesQuietPeriod = time.Second

// newEngineWithShortQuietPeriod returns an engine whose drop_tables quiet
// period is testDropTablesQuietPeriod, once the test server has been up for at
// least that long. A table the test never wrote to has no write time, so the
// server's uptime is what shows it has been quiet.
func newEngineWithShortQuietPeriod(t *testing.T, db *sql.DB) *Engine {
	t.Helper()

	deadline := time.Now().Add(copyProgressPollDeadline)
	for {
		var name, value string
		require.NoError(t, db.QueryRowContext(t.Context(), "SHOW GLOBAL STATUS LIKE 'Uptime'").Scan(&name, &value))
		seconds, err := strconv.ParseInt(value, 10, 64)
		require.NoError(t, err, "parse server uptime %q", value)
		if time.Duration(seconds)*time.Second >= testDropTablesQuietPeriod {
			break
		}
		require.True(t, time.Now().Before(deadline), "test server uptime did not reach %s", testDropTablesQuietPeriod)
		time.Sleep(100 * time.Millisecond)
	}

	eng := New(Config{})
	eng.dropTablesQuietPeriod = testDropTablesQuietPeriod
	return eng
}

// A leftover table from an online schema change starts with "_", and a
// retired table is named <name>_archive_YYYY, so the planner leaves both out
// of every plan. Naming them in drop_tables is the one way to drop them: each
// is planned as an unsafe DROP TABLE, leaves the naming convention's
// disclosure, and the apply moves it into pending drops, where it can be
// recovered. A leftover the config does not name stays disclosed and
// untouched.
func TestEngine_Plan_DropTablesDropsTablesTheNamingConventionsSkip(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	for _, stmt := range []string{
		`CREATE TABLE executions (id INT PRIMARY KEY)`,
		"CREATE TABLE `_jobs_old` (id INT PRIMARY KEY AUTO_INCREMENT)",
		"CREATE TABLE `_other_old` (id INT PRIMARY KEY)",
		"CREATE TABLE `executions_archive_2019` (id INT PRIMARY KEY)",
	} {
		_, err := db.ExecContext(t.Context(), stmt)
		require.NoError(t, err, stmt)
	}

	eng := newEngineWithShortQuietPeriod(t, db)
	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: testDatabase,
		SchemaFiles: testSchemaFiles(map[string]string{
			"executions.sql": `CREATE TABLE executions (id INT PRIMARY KEY)`,
		}),
		Credentials: &engine.Credentials{DSN: dsn},
		DropTables:  []string{"_jobs_old", "executions_archive_2019", "never_existed"},
	})
	require.NoError(t, err, "Plan()")
	assert.ElementsMatch(t, []string{"_jobs_old", "executions_archive_2019"}, droppedTables(result),
		"each listed table that exists is planned as DROP TABLE; an entry naming nothing plans nothing")
	for _, change := range result.FlatTableChanges() {
		assert.True(t, change.IsUnsafe, "dropping %s is an unsafe change that needs confirmation", change.Table)
	}
	assert.True(t, result.HasErrors(), "the unsafe drop stays a blocking lint result until confirmed")
	assert.Equal(t, []*engine.ExemptTables{
		{Namespace: testDatabase, Tables: []string{"_other_old"}, Reason: engine.ExemptReasonLeadingUnderscore},
	}, result.ExemptTables, "only the unlisted leftover is still disclosed as skipped")

	for _, change := range result.FlatTableChanges() {
		if change.Table != "_jobs_old" {
			continue
		}
		applyTableChange(t, eng, dsn, change.Table, change.DDL)
		waitForTerminalOutcome(t, eng, engine.StateCompleted)
	}
	assert.False(t, tableExists(t, db, "_jobs_old"), "the apply dropped the listed leftover")
	assert.True(t, tableExists(t, db, "_other_old"), "the unlisted leftover is untouched")

	quarantined := listQuarantinedTables(t, db)
	require.Len(t, quarantined, 1, "the dropped leftover is kept in pending drops, and nothing else is")
	assert.Contains(t, quarantined[0], "_jobs_old", "the quarantined table carries the dropped table's name")
	_, ok := pendingdrops.ParseTimestamp(quarantined[0])
	assert.True(t, ok, "quarantined table name %q must carry a parseable timestamp", quarantined[0])
}

// A plan records the definitions a rollback restores, and a drop_tables entry
// is not among them. Rolling back an apply that dropped a listed leftover
// restores the schema files the plan manages and leaves the leftover dropped,
// rather than recreating an empty copy that a resume of the rollback, planning
// without the list, would refuse as a declared table the planner never
// manages.
func TestEngine_Plan_DropTablesEntryIsNotRestoredByARollback(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)
	cleanupPendingDropsDB(t, db)

	for _, stmt := range []string{
		`CREATE TABLE executions (id INT PRIMARY KEY)`,
		"CREATE TABLE `_jobs_old` (id INT PRIMARY KEY)",
	} {
		_, err := db.ExecContext(t.Context(), stmt)
		require.NoError(t, err, stmt)
	}

	eng := newEngineWithShortQuietPeriod(t, db)
	forward, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: testDatabase,
		SchemaFiles: testSchemaFiles(map[string]string{
			"executions.sql": `CREATE TABLE executions (id INT PRIMARY KEY, status INT)`,
		}),
		Credentials: &engine.Credentials{DSN: dsn},
		DropTables:  []string{"_jobs_old"},
	})
	require.NoError(t, err, "Plan()")
	require.Contains(t, droppedTables(forward), "_jobs_old")
	require.Len(t, forward.Changes, 1, "both changes belong to the one namespace")
	originals := forward.Changes[0].OriginalFiles
	assert.Contains(t, originals, "executions.sql", "the managed table's definition is recorded for a rollback")
	assert.NotContains(t, originals, "_jobs_old.sql", "the dropped leftover's definition is not recorded")

	for _, change := range forward.FlatTableChanges() {
		applyTableChange(t, eng, dsn, change.Table, change.DDL)
		waitForTerminalOutcome(t, eng, engine.StateCompleted)
	}
	require.False(t, tableExists(t, db, "_jobs_old"), "the forward apply dropped the listed leftover")

	rollback, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database:    testDatabase,
		SchemaFiles: schema.SchemaFiles{testDatabase: &schema.Namespace{Files: originals}},
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "a rollback plans from the recorded definitions without the drop list")
	var reverted []string
	for _, change := range rollback.FlatTableChanges() {
		reverted = append(reverted, change.Table)
	}
	assert.Equal(t, []string{"executions"}, reverted, "the rollback reverts the managed table and does not recreate the dropped leftover")
}

// A name alone does not prove a table is abandoned. The shadow table of an
// online schema change another tool is running is written continuously, so a
// drop_tables entry written within the quiet period fails the plan instead of
// being proposed for DROP TABLE. The error says how long ago the table was
// written, and the target keeps the table.
func TestEngine_Plan_DropTablesRefusesARecentlyWrittenTable(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	for _, stmt := range []string{
		`CREATE TABLE executions (id INT PRIMARY KEY)`,
		"CREATE TABLE `_executions_new` (id INT PRIMARY KEY)",
		"INSERT INTO `_executions_new` (id) VALUES (1), (2)",
	} {
		_, err := db.ExecContext(t.Context(), stmt)
		require.NoError(t, err, stmt)
	}

	result, err := New(Config{}).Plan(t.Context(), &engine.PlanRequest{
		Database: testDatabase,
		SchemaFiles: testSchemaFiles(map[string]string{
			"executions.sql": `CREATE TABLE executions (id INT PRIMARY KEY)`,
		}),
		Credentials: &engine.Credentials{DSN: dsn},
		DropTables:  []string{"_executions_new"},
	})
	require.Error(t, err, "Plan() must refuse to drop a table that is still being written")
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "drop_tables entries were written in the last 1d")
	assert.Contains(t, err.Error(), `"_executions_new" (`)
	assert.Contains(t, err.Error(), "Plan again once they have gone 1d without a write")
	assert.True(t, tableExists(t, db, "_executions_new"))
}

// The server keeps a table's last write time in memory, so it has none for a
// table not written since it started. On a server that started inside the
// quiet period that leaves a write before the start unaccounted for, so the
// plan fails instead of proposing DROP TABLE for a table whose recent history
// is unknown. The error says when the plan can be tried again, and the target
// keeps the table.
func TestEngine_Plan_DropTablesRefusesATableWithNoWriteTimeOnARecentlyStartedServer(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	for _, stmt := range []string{
		`CREATE TABLE executions (id INT PRIMARY KEY)`,
		"CREATE TABLE `_jobs_old` (id INT PRIMARY KEY)",
	} {
		_, err := db.ExecContext(t.Context(), stmt)
		require.NoError(t, err, stmt)
	}

	result, err := New(Config{}).Plan(t.Context(), &engine.PlanRequest{
		Database: testDatabase,
		SchemaFiles: testSchemaFiles(map[string]string{
			"executions.sql": `CREATE TABLE executions (id INT PRIMARY KEY)`,
		}),
		Credentials: &engine.Credentials{DSN: dsn},
		DropTables:  []string{"_jobs_old"},
	})
	require.Error(t, err, "Plan() must refuse to drop a table whose recent writes the server cannot account for")
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), `SchemaBot cannot tell whether drop_tables entries "_jobs_old" were written in the last 1d: the server has no write time for them and started `)
	assert.Contains(t, err.Error(), "Plan again in ")
	assert.True(t, tableExists(t, db, "_jobs_old"))
}

// An entry a schema file declares, or that ignore_tables also withholds, asks
// for two contradictory things, so the plan fails naming the entry rather
// than guessing which the repository meant. The contradiction is reported
// ahead of anything the target says about when the table was written.
func TestEngine_Plan_DropTablesRefusesContradictoryConfig(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	for _, stmt := range []string{
		`CREATE TABLE executions (id INT PRIMARY KEY)`,
		`INSERT INTO executions (id) VALUES (1)`,
	} {
		_, err := db.ExecContext(t.Context(), stmt)
		require.NoError(t, err, stmt)
	}
	files := testSchemaFiles(map[string]string{
		"executions.sql": `CREATE TABLE Executions (id INT PRIMARY KEY)`,
	})

	_, err := New(Config{}).Plan(t.Context(), &engine.PlanRequest{
		Database:    testDatabase,
		SchemaFiles: files,
		Credentials: &engine.Credentials{DSN: dsn},
		DropTables:  []string{"executions"},
	})
	require.Error(t, err, "Plan() must refuse an entry a schema file declares")
	assert.Contains(t, err.Error(), `drop_tables entry "executions" (the file spells it "Executions") is also declared by a schema file`)

	_, err = New(Config{}).Plan(t.Context(), &engine.PlanRequest{
		Database:     testDatabase,
		SchemaFiles:  files,
		Credentials:  &engine.Credentials{DSN: dsn},
		DropTables:   []string{"_jobs_old"},
		IgnoreTables: []string{`/^_jobs_.*$/`},
	})
	require.Error(t, err, "Plan() must refuse an entry ignore_tables also withholds")
	assert.Contains(t, err.Error(), `drop_tables entry "_jobs_old" is also withheld by ignore_tables`)
}

// When two namespaces are diffed against one database, a table no schema file
// declares cannot be attributed to either of them. A drop_tables entry is such
// a table, so its drop is refused like any other the plan cannot place, and
// the target keeps the table.
func TestEngine_Plan_DropTablesRefusesADropNoNamespaceOwns(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	for _, stmt := range []string{
		`CREATE TABLE executions (id INT PRIMARY KEY)`,
		`CREATE TABLE invoices (id INT PRIMARY KEY)`,
		"CREATE TABLE `_jobs_old` (id INT PRIMARY KEY)",
	} {
		_, err := db.ExecContext(t.Context(), stmt)
		require.NoError(t, err, stmt)
	}

	result, err := newEngineWithShortQuietPeriod(t, db).Plan(t.Context(), &engine.PlanRequest{
		Database: testDatabase,
		SchemaFiles: schema.SchemaFiles{
			"app":     &schema.Namespace{Files: map[string]string{"executions.sql": `CREATE TABLE executions (id INT PRIMARY KEY)`}},
			"billing": &schema.Namespace{Files: map[string]string{"invoices.sql": `CREATE TABLE invoices (id INT PRIMARY KEY)`}},
		},
		Credentials: &engine.Credentials{DSN: dsn},
		DropTables:  []string{"_jobs_old"},
	})
	require.Error(t, err, "Plan() must refuse a drop it cannot attribute to a namespace")
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), `DROP TABLE "_jobs_old" cannot be placed: namespaces [app billing] all diff against database`)
	assert.True(t, tableExists(t, db, "_jobs_old"))
}
