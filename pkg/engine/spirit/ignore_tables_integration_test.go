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
	assert.Contains(t, err.Error(), "Remove the entry or the schema file")
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
	assert.Contains(t, err.Error(), `"flyway_schema_history" (the file spells it "Flyway_Schema_History")`,
		"the error names both spellings so an operator can find the entry they wrote")
	assert.Contains(t, err.Error(), "Remove the entry or the schema file")
}

// The planner excludes a table whose name follows the archive convention on
// its own, and says nothing about it. An entry naming such a table is matched
// against the target's catalog before that exclusion reaches it, so the plan
// reports it as withheld by the config — the same answer every other engine
// gives — rather than as an entry that matched no live table.
//
// A second archive table the config does not name shares the target, because
// the entry takes the archive exclusion off the loader for the whole read: the
// unnamed one comes back from the loader and must still be excluded by its
// name, undisclosed, exactly as it was before the entry existed.
func TestEngine_Plan_IgnoreTablesOutranksArchiveNaming(t *testing.T) {
	const (
		archiveTable        = "executions_archive_2024"
		unnamedArchiveTable = "audit_log_archive_2019"
	)

	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE executions (id INT PRIMARY KEY, name VARCHAR(100))`)
	require.NoError(t, err, "create executions")
	_, err = db.ExecContext(t.Context(), "CREATE TABLE `"+archiveTable+"` (id INT PRIMARY KEY)")
	require.NoError(t, err, "create archive table")
	_, err = db.ExecContext(t.Context(), "CREATE TABLE `"+unnamedArchiveTable+"` (id INT PRIMARY KEY)")
	require.NoError(t, err, "create the archive table the config does not name")

	files := testSchemaFiles(map[string]string{
		"executions.sql": `CREATE TABLE executions (id INT PRIMARY KEY, name VARCHAR(100))`,
	})

	// Without the entry both archive tables are excluded by name alone: not
	// dropped, and not disclosed.
	result, err := New(Config{}).Plan(t.Context(), &engine.PlanRequest{
		Database:    "testdb",
		SchemaFiles: files,
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err, "Plan()")
	assert.Empty(t, droppedTables(result), "the archive tables are not proposed for DROP TABLE")
	assert.Empty(t, result.ExemptTables, "the archive exclusion is the engine's own and is not disclosed here")

	result, err = New(Config{}).Plan(t.Context(), &engine.PlanRequest{
		Database:     "testdb",
		SchemaFiles:  files,
		Credentials:  &engine.Credentials{DSN: dsn},
		IgnoreTables: []string{archiveTable},
	})
	require.NoError(t, err, "Plan()")
	assert.Empty(t, droppedTables(result),
		"neither the withheld table nor the unnamed archive table is proposed for DROP TABLE")
	assertWithheld(t, result, "testdb", archiveTable)
}

// An application that creates one table per configured trigger leaves an
// unbounded family of undeclared tables on the target, and no list of names
// can cover the member created after the config was written. A pattern entry
// withholds the whole family, including a member created between two plans,
// and discloses each one it withheld; an undeclared table the pattern does not
// match in full is still proposed for DROP TABLE.
func TestEngine_Plan_IgnoreTablesPatternWithholdsRuntimeTableFamily(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	for _, stmt := range []string{
		`CREATE TABLE executions (id INT PRIMARY KEY, name VARCHAR(100))`,
		`CREATE TABLE relay_1_feed (id BIGINT PRIMARY KEY, payload JSON)`,
		`CREATE TABLE relay_2_feed (id BIGINT PRIMARY KEY, payload JSON)`,
		`CREATE TABLE relay_feed_settings (id INT PRIMARY KEY)`,
	} {
		_, err := db.ExecContext(t.Context(), stmt)
		require.NoError(t, err, stmt)
	}
	req := &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"executions.sql": `CREATE TABLE executions (id INT PRIMARY KEY, name VARCHAR(100))`,
		}),
		Credentials:  &engine.Credentials{DSN: dsn},
		IgnoreTables: []string{`/^relay_\d+_feed$/`},
	}

	result, err := New(Config{}).Plan(t.Context(), req)
	require.NoError(t, err, "Plan()")
	assert.Equal(t, []string{"relay_feed_settings"}, droppedTables(result),
		"only the undeclared table the pattern does not match is proposed for DROP TABLE")
	assertWithheld(t, result, "testdb", "relay_1_feed", "relay_2_feed")

	// The application creates the next member of the family at runtime. The
	// same config withholds it on the next plan without anyone editing it.
	_, err = db.ExecContext(t.Context(), `CREATE TABLE relay_30_feed (id BIGINT PRIMARY KEY, payload JSON)`)
	require.NoError(t, err)

	result, err = New(Config{}).Plan(t.Context(), req)
	require.NoError(t, err, "Plan()")
	assert.Equal(t, []string{"relay_feed_settings"}, droppedTables(result))
	assertWithheld(t, result, "testdb", "relay_1_feed", "relay_2_feed", "relay_30_feed")
}

// A pattern written for undeclared tables that also matches a table a schema
// file declares is the same contradiction as a plain entry naming it, refused
// ignoring case, and the target is left as it was.
func TestEngine_Plan_IgnoreTablesPatternRefusesDeclaredTable(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE relay_1_feed (id BIGINT PRIMARY KEY)`)
	require.NoError(t, err)

	_, err = New(Config{}).Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"relay_1_feed.sql": `CREATE TABLE Relay_1_Feed (id BIGINT PRIMARY KEY)`,
		}),
		Credentials:  &engine.Credentials{DSN: dsn},
		IgnoreTables: []string{`/^relay_\d+_feed$/`},
	})
	require.Error(t, err, "Plan() must refuse a pattern that matches a declared table")
	assert.Contains(t, err.Error(), `ignore_tables entry "/^relay_\d+_feed$/" matches "Relay_1_Feed"`)
	assert.Contains(t, err.Error(), "Narrow the pattern")

	var count int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'testdb' AND table_name = 'relay_1_feed'").Scan(&count))
	assert.Equal(t, 1, count, "a refused plan leaves the target as it found it")
}

// A pattern that does not compile fails the plan with an error naming the
// entry. Skipping it would withhold nothing, and the plan would propose
// dropping the tables it was written to protect.
func TestEngine_Plan_IgnoreTablesInvalidPatternFailsThePlan(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	_, err := db.ExecContext(t.Context(), `CREATE TABLE relay_1_feed (id BIGINT PRIMARY KEY)`)
	require.NoError(t, err)

	result, err := New(Config{}).Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"executions.sql": `CREATE TABLE executions (id INT PRIMARY KEY)`,
		}),
		Credentials:  &engine.Credentials{DSN: dsn},
		IgnoreTables: []string{`/^relay_(\d+_feed$/`},
	})
	require.Error(t, err)
	assert.Nil(t, result, "no plan is produced to review")
	assert.Contains(t, err.Error(), `ignore_tables entry "/^relay_(\d+_feed$/" is not a valid regular expression`)
}

// The archive naming exclusion runs inside the loader unless the config could
// withhold an archive table, and a pattern could, so a pattern takes the
// exclusion off the loader. An archive table the pattern matches is then
// disclosed as the config's exclusion, and one it does not match is still
// excluded by its name alone, undisclosed and not dropped.
func TestEngine_Plan_IgnoreTablesPatternOutranksArchiveNaming(t *testing.T) {
	dsn, db := setupTestMySQL(t)
	cleanupTables(t, db)

	for _, stmt := range []string{
		`CREATE TABLE executions (id INT PRIMARY KEY)`,
		"CREATE TABLE `executions_archive_2024` (id INT PRIMARY KEY)",
		"CREATE TABLE `audit_log_archive_2019` (id INT PRIMARY KEY)",
	} {
		_, err := db.ExecContext(t.Context(), stmt)
		require.NoError(t, err, stmt)
	}

	result, err := New(Config{}).Plan(t.Context(), &engine.PlanRequest{
		Database: "testdb",
		SchemaFiles: testSchemaFiles(map[string]string{
			"executions.sql": `CREATE TABLE executions (id INT PRIMARY KEY)`,
		}),
		Credentials:  &engine.Credentials{DSN: dsn},
		IgnoreTables: []string{`/^executions_archive_\d{4}$/`},
	})
	require.NoError(t, err, "Plan()")
	assert.Empty(t, droppedTables(result), "neither archive table is proposed for DROP TABLE")
	assertWithheld(t, result, "testdb", "executions_archive_2024")
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
