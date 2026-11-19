//go:build integration

package spirit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// A leftover table from an online schema change starts with "_", and a
// retired table is named <name>_archive_YYYY, so the planner leaves both out
// of every plan. Naming them in drop_tables is the one way to drop them: each
// is planned as an unsafe DROP TABLE, leaves the naming convention's
// disclosure, and the apply removes it. A leftover the config does not name
// stays disclosed and untouched.
func TestEngine_Plan_DropTablesDropsTablesTheNamingConventionsSkip(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	for _, stmt := range []string{
		`CREATE TABLE executions (id INT PRIMARY KEY)`,
		"CREATE TABLE `_jobs_old` (id INT PRIMARY KEY AUTO_INCREMENT)",
		"CREATE TABLE `_other_old` (id INT PRIMARY KEY)",
		"CREATE TABLE `executions_archive_2019` (id INT PRIMARY KEY)",
	} {
		_, err := db.ExecContext(t.Context(), stmt)
		require.NoError(t, err, stmt)
	}

	eng := New(Config{})
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
}

// A name alone does not prove a table is abandoned. The shadow table of an
// online schema change another tool is running is written continuously, so a
// drop_tables entry written within the quiet period fails the plan instead of
// being proposed for DROP TABLE, and the target keeps the table.
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
	assert.Contains(t, err.Error(), `drop_tables entries "_executions_new" were written in the last 24h0m0s`)
	assert.True(t, tableExists(t, db, "_executions_new"))
}

// An entry a schema file declares, or that ignore_tables also withholds, asks
// for two contradictory things, so the plan fails naming the entry rather
// than guessing which the repository meant.
func TestEngine_Plan_DropTablesRefusesContradictoryConfig(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE executions (id INT PRIMARY KEY)`)
	require.NoError(t, err)
	files := testSchemaFiles(map[string]string{
		"executions.sql": `CREATE TABLE Executions (id INT PRIMARY KEY)`,
	})

	_, err = New(Config{}).Plan(t.Context(), &engine.PlanRequest{
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
