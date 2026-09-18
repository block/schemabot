package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The CLI plan summary counts every statement the plan will run, matching the
// PR comment: statements outside the create/alter/drop buckets are named in a
// mixed plan and a plan made only of them reports its raw total, so an
// index-only or type-only plan never prints a blank summary.
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
				{ChangeType: "other", TableName: "order_state"},
			},
			want: "📋 Plan: 1 table to alter, 2 other DDL statements\n\n",
		},
		{
			name: "only unbucketed statements report the raw total",
			changes: []DDLChange{
				{ChangeType: "create_index", TableName: "orders"},
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
// the vschema count, so a per-shard index plus a vschema update names both.
func TestWritePlanSummaryWithVSchema_CountsEveryStatement(t *testing.T) {
	out := captureStdout(t, func() {
		WritePlanSummaryWithVSchema(
			[]DDLChange{{ChangeType: "create_index", TableName: "orders"}},
			[]VSchemaChange{{Keyspace: "orders"}},
		)
	})
	assert.Equal(t, "📋 **Plan**: 1 DDL statement, 1 VSchema change\n\n", out)
}
