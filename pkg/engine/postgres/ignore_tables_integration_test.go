//go:build integration

package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// A live table named by ignore_tables is withheld from the planner, so
// nothing proposes dropping it and the plan discloses what it
// withheld with a reason naming the config key. An entry that matches nothing
// withholds nothing: the plan proceeds and the table it does not name is still
// reported as a blocked drop, which is what makes the disclosure worth reading.
func TestEnginePlanIgnoreTablesWithholdsUndeclaredTable(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "plan_ignore_tables_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY, email text NOT NULL);
		CREATE TABLE public.flyway_schema_history (installed_rank integer PRIMARY KEY, version text);
		CREATE TABLE public.legacy_users (id bigint PRIMARY KEY)`)
	require.NoError(t, err)

	result, err := New().Plan(t.Context(), &engine.PlanRequest{
		Database: "plan_ignore_tables_test",
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY, email text NOT NULL)",
			}},
		},
		Credentials:  &engine.Credentials{DSN: dsn},
		IgnoreTables: []string{"flyway_schema_history", "never_existed"},
	})
	require.NoError(t, err)

	require.Len(t, result.ExemptTables, 1)
	assert.Equal(t, "public", result.ExemptTables[0].Namespace)
	assert.Equal(t, []string{"flyway_schema_history"}, result.ExemptTables[0].Tables,
		"only the entry that matched a live table is disclosed")
	assert.Equal(t, engine.ExemptReasonIgnoreTables, result.ExemptTables[0].Reason)

	require.Len(t, result.Changes, 1)
	require.Len(t, result.Changes[0].TableChanges, 1,
		"the withheld table is gone from the verdict; the table the config does not name is not")
	assert.Equal(t, "legacy_users", result.Changes[0].TableChanges[0].Table)

	for _, table := range []string{"users", "flyway_schema_history", "legacy_users"} {
		assert.True(t, testutil.PostgresTableExists(t, db, "public", table), "planning must never touch the target")
	}
}

// An operator who wrote a table name into the config deserves to read their
// own reason back, so a table that both matches ignore_tables and the engine's
// archive naming convention is disclosed as the config's exclusion.
func TestEnginePlanIgnoreTablesOutranksArchiveNaming(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "plan_ignore_archive_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.audit_log_archive_2019 (id bigint PRIMARY KEY)`)
	require.NoError(t, err)

	result, err := New().Plan(t.Context(), &engine.PlanRequest{
		Database: "plan_ignore_archive_test",
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY)",
			}},
		},
		Credentials:  &engine.Credentials{DSN: dsn},
		IgnoreTables: []string{"audit_log_archive_2019"},
	})
	require.NoError(t, err)

	require.Len(t, result.ExemptTables, 1)
	assert.Equal(t, []string{"audit_log_archive_2019"}, result.ExemptTables[0].Tables)
	assert.Equal(t, engine.ExemptReasonIgnoreTables, result.ExemptTables[0].Reason)
	assert.True(t, result.NoChanges, "nothing is left for the plan to propose")
}

// A table the config withholds that a schema file also declares is a
// contradiction: honoring the file would manage a table the config says to
// leave alone, and withholding it would leave the declaring file unreconciled.
// The plan fails rather than choosing.
func TestEnginePlanIgnoreTablesRefusesDeclaredTable(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "plan_ignore_declared_test")
	_, err := db.ExecContext(t.Context(), `CREATE TABLE public.flyway_schema_history (installed_rank integer PRIMARY KEY)`)
	require.NoError(t, err)

	_, err = New().Plan(t.Context(), &engine.PlanRequest{
		Database: "plan_ignore_declared_test",
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"bookkeeping.sql": "CREATE TABLE flyway_schema_history (installed_rank integer PRIMARY KEY)",
			}},
		},
		Credentials:  &engine.Credentials{DSN: dsn},
		IgnoreTables: []string{"flyway_schema_history"},
	})
	require.Error(t, err, "Plan() must refuse a table that is both withheld and declared")
	assert.Contains(t, err.Error(), "flyway_schema_history")
	assert.Contains(t, err.Error(), `namespace "public"`)
	assert.Contains(t, err.Error(), "Remove the entry or the schema file")

	assert.True(t, testutil.PostgresTableExists(t, db, "public", "flyway_schema_history"),
		"a refused plan must never touch the target")
}
