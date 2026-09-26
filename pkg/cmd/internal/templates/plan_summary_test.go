package templates

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/schema"
	webhooktemplates "github.com/block/schemabot/pkg/webhook/templates"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDDLSummaryParts_DeduplicatesTableChanges(t *testing.T) {
	changes := []DDLChange{
		{ChangeType: "ALTER", TableName: "users", DDL: "ALTER TABLE `users` DROP PRIMARY KEY, ADD PRIMARY KEY(`id`, `tenant_id`)"},
		{ChangeType: "ALTER", TableName: "orders", DDL: "ALTER TABLE `orders` ADD CONSTRAINT `fk_orders_user` FOREIGN KEY (`user_id`) REFERENCES `users`(`id`)"},
		{ChangeType: "ALTER", TableName: "orders", DDL: "ALTER TABLE `orders` ADD COLUMN `notes` text"},
	}
	require.Equal(t, []string{"2 tables to alter"}, ddlSummaryParts(changes))
}

// A change without a table name cannot be judged a duplicate of anything, so
// it is counted on its own rather than merged with every other nameless
// change. A named table alongside them still dedupes as usual.
func TestDDLSummaryParts_CountsNamelessChangesWithoutMerging(t *testing.T) {
	changes := []DDLChange{
		{ChangeType: "ALTER"},
		{ChangeType: "ALTER"},
		{ChangeType: "ALTER", TableName: "orders"},
		{ChangeType: "ALTER", TableName: "orders"},
	}
	require.Equal(t, []string{"3 tables to alter"}, ddlSummaryParts(changes))
}

func TestPlanSummarySurfacesShareCounts(t *testing.T) {
	cliChanges := []DDLChange{
		{ChangeType: "CHANGE_TYPE_CREATE", TableName: "users"},
		{ChangeType: "CHANGE_TYPE_ALTER", TableName: "users"},
		{ChangeType: "ALTER", TableName: "users"},
		{ChangeType: "ALTER", TableName: "orders"},
		{ChangeType: "CREATE_INDEX", TableName: "orders"},
	}
	commentData := webhooktemplates.PlanCommentData{
		DatabaseType: "mysql",
		IsMySQL:      true,
		Changes: []webhooktemplates.KeyspaceChangeData{{Keyspace: "app", Statements: []string{
			"CREATE TABLE users (id INT)",
			"ALTER TABLE users ADD COLUMN name TEXT",
			"ALTER TABLE users ADD COLUMN email TEXT",
			"ALTER TABLE orders ADD COLUMN state TEXT",
			"CREATE INDEX orders_state ON orders (state)",
		}}},
	}

	assert.Equal(t, []string{"1 table to create", "2 tables to alter", "1 index to create"}, ddlSummaryParts(cliChanges))
	assert.Equal(t, "1 create, 2 alters, 1 index create", webhooktemplates.SummarizeChanges(commentData))
}

// A CREATE INDEX on a table no other statement touches is one index to
// create on both surfaces, not a table to alter, and a PostgreSQL DROP INDEX
// is one index to drop on both even though only the CLI's change record
// carries the table the engine resolved for it.
func TestPlanSummarySurfacesShareIndexCounts(t *testing.T) {
	cliChanges := []DDLChange{
		{ChangeType: "CHANGE_TYPE_ALTER", TableName: "orders"},
		{ChangeType: "CHANGE_TYPE_CREATE_INDEX", TableName: "events"},
		{ChangeType: "CHANGE_TYPE_DROP_INDEX", TableName: "orders"},
	}
	commentData := webhooktemplates.PlanCommentData{
		DatabaseType: "postgres",
		Changes: []webhooktemplates.KeyspaceChangeData{{Keyspace: "app", Statements: []string{
			"ALTER TABLE orders ADD COLUMN state text",
			"CREATE INDEX CONCURRENTLY events_at_idx ON events (occurred_at)",
			"DROP INDEX CONCURRENTLY orders_legacy_idx",
		}}},
	}

	assert.Equal(t, []string{"1 table to alter", "1 index to create", "1 index to drop"}, ddlSummaryParts(cliChanges))
	assert.Equal(t, "1 alter, 1 index create, 1 index drop", webhooktemplates.SummarizeChanges(commentData))
}

// A CREATE INDEX planned as its own step on a table the same plan creates is
// one table to create and one index to create on both surfaces: the index is
// its own apply task, and the table is never also counted as a table to alter.
func TestPlanSummarySurfacesCountIndexOnNewTableAsIndexWork(t *testing.T) {
	cliChanges := []DDLChange{
		{ChangeType: "CHANGE_TYPE_CREATE", TableName: "widgets"},
		{ChangeType: "CHANGE_TYPE_CREATE_INDEX", TableName: "widgets"},
	}
	commentData := webhooktemplates.PlanCommentData{
		DatabaseType: "postgres",
		Changes: []webhooktemplates.KeyspaceChangeData{{Keyspace: "app", Statements: []string{
			"CREATE TABLE widgets (id bigint PRIMARY KEY, sku text)",
			"CREATE INDEX CONCURRENTLY widgets_sku_idx ON widgets (sku)",
		}}},
	}

	assert.Equal(t, []string{"1 table to create", "1 index to create"}, ddlSummaryParts(cliChanges))
	assert.Equal(t, "1 create, 1 index create", webhooktemplates.SummarizeChanges(commentData))
}

// The CLI plan summary counts every statement the plan will run, matching the
// PR comment: index work is named in its own clauses, statements outside every
// bucket are named in a mixed plan, and a plan made only of them reports its
// raw total, so a type-only plan never prints a blank summary.
func TestWritePlanSummary_CountsEveryStatement(t *testing.T) {
	tests := []struct {
		name    string
		changes []DDLChange
		want    string
	}{
		{
			name: "table changes only",
			changes: []DDLChange{
				{ChangeType: "create", TableName: "a"},
				{ChangeType: "alter", TableName: "b"},
				{ChangeType: "alter", TableName: "c"},
				{ChangeType: "drop", TableName: "d"},
			},
			want: "📋 Plan: 1 table to create, 2 tables to alter, 1 table to drop\n\n",
		},
		{
			name: "mixed table and other statements",
			changes: []DDLChange{
				{ChangeType: "alter", TableName: "orders"},
				{ChangeType: "create_index", TableName: "orders"},
				{ChangeType: "create_index", TableName: "events"},
				{ChangeType: "other", TableName: "order_state"},
			},
			want: "📋 Plan: 1 table to alter, 2 indexes to create, 1 other DDL statement\n\n",
		},
		{
			name: "index work only is named as index work",
			changes: []DDLChange{
				{ChangeType: "drop_index", TableName: "orders"},
			},
			want: "📋 Plan: 1 index to drop\n\n",
		},
		{
			name: "only unbucketed statements report the raw total",
			changes: []DDLChange{
				{ChangeType: "other", TableName: "order_state"},
			},
			want: "📋 Plan: 1 DDL statement\n\n",
		},
		{
			name: "no changes prints only the separator",
			want: "\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := captureStdout(t, func() { WritePlanSummary(tt.changes) })
			assert.Equal(t, tt.want, out)
		})
	}
}

// The vschema variant shares the DDL clauses with WritePlanSummary and appends
// the vschema count, so a per-shard index drop plus a vschema update names both.
func TestWritePlanSummaryWithVSchema_CountsEveryStatement(t *testing.T) {
	out := captureStdout(t, func() {
		WritePlanSummaryWithVSchema(
			[]DDLChange{{ChangeType: "drop_index", TableName: "orders"}},
			[]VSchemaChange{{Keyspace: "orders"}},
		)
	})
	assert.Equal(t, "📋 **Plan**: 1 index to drop, 1 VSchema change\n\n", out)
}

// A derived-only VSchema refresh is not counted as a VSchema change; a plan
// whose only work is the refresh still names it rather than printing nothing.
func TestWritePlanSummaryWithVSchema_DerivedOnly(t *testing.T) {
	out := captureStdout(t, func() {
		WritePlanSummaryWithVSchema(
			[]DDLChange{{ChangeType: "create", TableName: "orders"}},
			[]VSchemaChange{{Keyspace: "orders", DerivedOnly: true}},
		)
	})
	assert.Equal(t, "📋 **Plan**: 1 table to create\n\n", out)

	out = captureStdout(t, func() {
		WritePlanSummaryWithVSchema(nil, []VSchemaChange{{Keyspace: "orders", DerivedOnly: true}})
	})
	assert.Equal(t, "📋 **Plan**: VSchema refresh from table DDL\n\n", out)
}

// A derived-only keyspace prints its DDL and a note, not a "~ VSchema:" block.
func TestWriteNamespaceChanges_DerivedOnlyVSchemaPrintsNote(t *testing.T) {
	out := captureStdout(t, func() {
		WriteNamespaceChanges([]NamespaceChange{{
			Namespace:          "orders_001",
			Changes:            []DDLChange{{ChangeType: "create", TableName: "orders", DDL: "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"}},
			VSchemaChanged:     true,
			VSchemaDerivedOnly: true,
		}}, false, "orders", schema.DialectMySQL)
	})
	assert.NotContains(t, out, "~ VSchema:")
	assert.Contains(t, out, "No vschema.json changes. The apply refreshes this keyspace's VSchema entries from its table DDL.")
	assert.Less(t, strings.Index(out, "CREATE TABLE"), strings.Index(out, "No vschema.json changes"), "the note follows the DDL it is derived from")
}
