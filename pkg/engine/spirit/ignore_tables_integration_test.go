//go:build integration

package spirit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
)

const flywayTable = "flyway_schema_history"

// A live table no schema file declares is planned as DROP TABLE, which is an
// unsafe change and blocks the merge. This is the behavior ignore_tables
// exists to change, so it is pinned first: without the exclusion the drop is
// proposed, and the tests below show the same target planning cleanly once the
// table is named in the config.
func TestEngine_Plan_UndeclaredTableIsDroppedWithoutIgnoreTables(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE executions (id INT PRIMARY KEY, name VARCHAR(100))`)
	require.NoError(t, err, "create executions")
	_, err = db.ExecContext(t.Context(), "CREATE TABLE `"+flywayTable+"` (installed_rank INT PRIMARY KEY, version VARCHAR(50))")
	require.NoError(t, err, "create bookkeeping table")

	result, err := New(Config{}).Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"executions.sql": `CREATE TABLE executions (id INT PRIMARY KEY, name VARCHAR(100))`,
		}),
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "Plan()")
	require.NotNil(t, result)

	drops := droppedTables(result)
	assert.Equal(t, []string{flywayTable}, drops, "the undeclared table is proposed for DROP TABLE")
	assert.Empty(t, result.ExemptTables, "nothing was withheld, so nothing is disclosed")
}

// A live table named by ignore_tables is withheld from the planner, so it is
// neither dropped nor created, and the plan discloses what it withheld with a
// reason naming the config key the decision is recorded in.
func TestEngine_Plan_IgnoreTablesWithholdsUndeclaredTable(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE executions (id INT PRIMARY KEY, name VARCHAR(100))`)
	require.NoError(t, err, "create executions")
	_, err = db.ExecContext(t.Context(), "CREATE TABLE `"+flywayTable+"` (installed_rank INT PRIMARY KEY, version VARCHAR(50))")
	require.NoError(t, err, "create bookkeeping table")

	result, err := New(Config{}).Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"executions.sql": `CREATE TABLE executions (id INT PRIMARY KEY, name VARCHAR(100), note VARCHAR(50))`,
		}),
		Credentials:  &engine.Credentials{DSN: dsn},
		IgnoreTables: []string{flywayTable},
	})
	require.NoError(t, err, "Plan()")
	require.NotNil(t, result)

	assert.Empty(t, droppedTables(result), "the withheld table is not proposed for DROP TABLE: %v", result.FlatDDL())
	assert.False(t, result.NoChanges, "the declared table's own change is still planned")
	assertWithheld(t, result, "testdb", flywayTable)
}

// A clean plan is where the disclosure matters most: it is the only evidence a
// reviewer has that the withheld table was seen and deliberately skipped
// rather than missed.
func TestEngine_Plan_IgnoreTablesDisclosedOnNoChanges(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE executions (id INT PRIMARY KEY, name VARCHAR(100))`)
	require.NoError(t, err, "create executions")
	_, err = db.ExecContext(t.Context(), "CREATE TABLE `"+flywayTable+"` (installed_rank INT PRIMARY KEY, version VARCHAR(50))")
	require.NoError(t, err, "create bookkeeping table")

	result, err := New(Config{}).Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"executions.sql": `CREATE TABLE executions (id INT PRIMARY KEY, name VARCHAR(100))`,
		}),
		Credentials:  &engine.Credentials{DSN: dsn},
		IgnoreTables: []string{flywayTable},
	})
	require.NoError(t, err, "Plan()")
	require.NotNil(t, result)

	assert.True(t, result.NoChanges, "the withheld table leaves the plan clean: %v", result.FlatDDL())
	assertWithheld(t, result, "testdb", flywayTable)
}

// An entry that matches no live table withholds nothing, and the plan says so
// by disclosing nothing rather than implying an exclusion that is not
// happening. Withholding is exact and case-sensitive, so a misspelled entry
// leaves its table exactly as exposed to the drop as no entry at all — which
// is why the caller reports the unmatched entry.
func TestEngine_Plan_IgnoreTablesMatchingNothingDisclosesNothing(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE executions (id INT PRIMARY KEY, name VARCHAR(100))`)
	require.NoError(t, err, "create executions")
	_, err = db.ExecContext(t.Context(), "CREATE TABLE `"+flywayTable+"` (installed_rank INT PRIMARY KEY, version VARCHAR(50))")
	require.NoError(t, err, "create bookkeeping table")

	result, err := New(Config{}).Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"executions.sql": `CREATE TABLE executions (id INT PRIMARY KEY, name VARCHAR(100))`,
		}),
		Credentials:  &engine.Credentials{DSN: dsn},
		IgnoreTables: []string{"never_existed", "Flyway_Schema_History"},
	})
	require.NoError(t, err, "Plan()")
	require.NotNil(t, result)

	assert.Empty(t, result.ExemptTables, "matching is exact and case-sensitive, so neither entry withheld anything")
	assert.Equal(t, []string{flywayTable}, droppedTables(result),
		"the table the entry failed to name is still proposed for DROP TABLE")
}

// A table the config withholds that a schema file also declares is a
// contradiction: the whole database is diffed as one unit, so the withheld
// live table has no counterpart to match the declaration against and the diff
// would propose creating a table that already exists. The plan fails instead.
func TestEngine_Plan_IgnoreTablesRefusesDeclaredTable(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), "CREATE TABLE `"+flywayTable+"` (installed_rank INT PRIMARY KEY, version VARCHAR(50))")
	require.NoError(t, err, "create bookkeeping table")

	_, err = New(Config{}).Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"bookkeeping.sql": "CREATE TABLE `" + flywayTable + "` (installed_rank INT PRIMARY KEY, version VARCHAR(50))",
		}),
		Credentials:  &engine.Credentials{DSN: dsn},
		IgnoreTables: []string{flywayTable},
	})
	require.Error(t, err, "Plan() must refuse a table that is both withheld and declared")
	assert.Contains(t, err.Error(), flywayTable)
	assert.Contains(t, err.Error(), "ignore_tables")
	assert.Contains(t, err.Error(), "remove the ignore_tables entry or delete the declaring schema file")
}

// Whether a declaration and a differently-cased entry name one table is the
// target's answer, and the plan does not ask it: a database configured to fold
// identifiers stores both spellings as one table, so honoring the file there
// would propose creating a table that already exists. The contradiction is
// refused whatever the target's own case handling, because refusing a
// repository that meant two tables costs a plan and an error naming both
// spellings, while letting a real contradiction through costs an apply that
// fails part way.
func TestEngine_Plan_IgnoreTablesRefusesDeclaredTableWhateverTheCase(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), "CREATE TABLE `"+flywayTable+"` (installed_rank INT PRIMARY KEY, version VARCHAR(50))")
	require.NoError(t, err, "create bookkeeping table")

	_, err = New(Config{}).Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"bookkeeping.sql": "CREATE TABLE `Flyway_Schema_History` (installed_rank INT PRIMARY KEY, version VARCHAR(50))",
		}),
		Credentials:  &engine.Credentials{DSN: dsn},
		IgnoreTables: []string{flywayTable},
	})
	require.Error(t, err, "Plan() must refuse a declaration that differs from the entry only by case")
	assert.Contains(t, err.Error(), `"flyway_schema_history" (declared as "Flyway_Schema_History")`,
		"the error names both spellings so an operator can find the entry they wrote")
	assert.Contains(t, err.Error(), "remove the ignore_tables entry or delete the declaring schema file")
}

func droppedTables(result *engine.PlanResult) []string {
	var dropped []string
	for _, change := range result.FlatTableChanges() {
		if change.Operation == ddl.StatementDropTable {
			dropped = append(dropped, change.Table)
		}
	}
	return dropped
}

func assertWithheld(t *testing.T, result *engine.PlanResult, namespace string, tables ...string) {
	t.Helper()

	require.Len(t, result.ExemptTables, 1, "one disclosure group for the withheld tables")
	group := result.ExemptTables[0]
	require.NotNil(t, group)
	assert.Equal(t, namespace, group.Namespace)
	assert.Equal(t, tables, group.Tables)
	assert.Equal(t, engine.ExemptReasonIgnoreTables, group.Reason,
		"the disclosure names the config key a reviewer reads the decision in")
}
