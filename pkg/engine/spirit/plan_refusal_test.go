package spirit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
)

// A declared schema the MySQL engine will not plan as written is reported as a
// plan refusal, not as a failure of the engine, and keeps its message word for
// word: the message tells whoever edits the schema files or the config what to
// change. Each case is a refusal the engine reaches without reading the target.
func TestPlanRefusalsAreTyped(t *testing.T) {
	live := &liveSchema{underscore: []string{"_legacy"}}

	tests := []struct {
		name    string
		refuse  func() error
		wantErr string
	}{
		{
			name: "a declared table the config ignores",
			refuse: func() error {
				return refuseDeclaredTables("app", []string{"orders_seq"}, mustIgnoredTables(t, []string{"orders_seq"}), engine.DroppedTables{}, live)
			},
			wantErr: `ignore_tables entry "orders_seq" is also declared by a schema file in namespace "app". Remove the entry or the schema file`,
		},
		{
			name: "a declared table the config drops",
			refuse: func() error {
				drops, err := engine.NewDroppedTables([]string{"orders_old"})
				require.NoError(t, err)
				return refuseDeclaredTables("app", []string{"orders_old"}, engine.IgnoredTables{}, drops, live)
			},
			wantErr: `drop_tables entry "orders_old" is also declared by a schema file in namespace "app". Remove the entry or the schema file`,
		},
		{
			name: "a declared table the naming convention skips",
			refuse: func() error {
				return refuseDeclaredTables("app", []string{"_legacy"}, engine.IgnoredTables{}, engine.DroppedTables{}, live)
			},
			wantErr: `a schema file in namespace "app" declares table "_legacy", which the MySQL planner never manages because its name starts with "_". Rename the table, or remove its schema file`,
		},
		{
			name: "a dropped table that no namespace can own",
			refuse: func() error {
				sf := schema.SchemaFiles{
					"billing": {Files: map[string]string{"invoices.sql": ""}},
					"users":   {Files: map[string]string{"users.sql": ""}},
				}
				_, err := namespaceForTable("dropped_table", nil, sf, "shop")
				return err
			},
			wantErr: `DROP TABLE "dropped_table" cannot be placed: namespaces [billing users] all diff against database "shop". Keep this database's schema files in one namespace directory, or ask a SchemaBot operator to use a target with no database in its DSN`,
		},
		{
			name: "a dropped table in a request with no schema files",
			refuse: func() error {
				_, err := namespaceForTable("dropped_table", nil, schema.SchemaFiles{}, "shop")
				return err
			},
			wantErr: `DROP TABLE "dropped_table" cannot be placed: the request carries no schema files for database "shop". Restore this database's schema files, or remove the database from the SchemaBot config`,
		},
		{
			name: "a drop_tables entry written inside the quiet period",
			refuse: func() error {
				writes := []dropTableWrite{{table: "_jobs_old", known: true, age: time.Hour}}
				return refuseDropsNotQuiet(writes, 30*24*time.Hour, 24*time.Hour)
			},
			wantErr: `drop_tables entries were written in the last 1d, so something may still be using them, such as an online schema change another tool is running: "_jobs_old" (1h ago). Plan again once they have gone 1d without a write, or remove them from drop_tables`,
		},
		{
			name: "a drop_tables entry with no write time on a recently started server",
			refuse: func() error {
				writes := []dropTableWrite{{table: "_jobs_old"}}
				return refuseDropsNotQuiet(writes, 2*time.Hour, 24*time.Hour)
			},
			wantErr: `SchemaBot cannot tell whether drop_tables entries "_jobs_old" were written in the last 1d: the server has no write time for them and started 2h ago, so it has no record of a write before that. Plan again in 22h, or remove them from drop_tables`,
		},
		{
			name: "a drop_tables entry the config also ignores",
			refuse: func() error {
				// The contradiction is refused before the engine opens the
				// target, so the address below is never dialed.
				_, err := New(Config{}).Plan(t.Context(), &engine.PlanRequest{
					Database:     "shop",
					Credentials:  &engine.Credentials{DSN: "user:pass@tcp(127.0.0.1:1)/shop"},
					IgnoreTables: []string{"_jobs_old"},
					DropTables:   []string{"_jobs_old"},
				})
				return err
			},
			wantErr: `plan database shop: drop_tables entry "_jobs_old" is also withheld by ignore_tables. Remove it from one of the two lists`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.refuse()
			require.Error(t, err)
			assert.True(t, engine.IsPlanRefused(err), "the refusal must be typed so consumers do not treat it as an engine failure")
			assert.EqualError(t, err, tc.wantErr)
		})
	}
}

// A drop_tables entry that parses and does not contradict the schema files
// passes the declared-table refusals untouched.
func TestRefuseDeclaredTablesAcceptsUndeclaredEntries(t *testing.T) {
	drops, err := engine.NewDroppedTables([]string{"orders_old"})
	require.NoError(t, err)

	require.NoError(t, refuseDeclaredTables("app", []string{"orders"}, mustIgnoredTables(t, []string{"flyway_schema_history"}), drops, &liveSchema{}))
}

// A schema file the MySQL parser rejects is the file's to fix, so it is a
// plan refusal that names the file, as on every engine, and never an engine
// failure.
func TestParseDesiredTablesRefusesWhatTheFileGetsWrong(t *testing.T) {
	for name, tc := range map[string]struct {
		content string
		wantErr string
	}{
		"a syntax error":                         {"CREATE TABLE `orders` (`id` bigint NOT NULL PRIMARY KEY", "app/orders.sql"},
		"a statement that is not a CREATE TABLE": {"ALTER TABLE `orders` ADD COLUMN `note` varchar(255);", "parse desired schema in app/orders.sql"},
		"an index on a missing column":           {"CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`), KEY `k` (`missing`));", "SQL usage error in app/orders.sql"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseDesiredTables("app", "orders.sql", tc.content)
			require.Error(t, err)
			assert.True(t, engine.IsPlanRefused(err), "the refusal must be typed")
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
	tables, err := parseDesiredTables("app", "orders.sql", "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`));")
	require.NoError(t, err)
	require.Len(t, tables, 1)
	assert.Equal(t, "orders", tables[0].table)
}
