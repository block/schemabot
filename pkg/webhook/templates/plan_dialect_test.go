package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/schema"
)

// A PostgreSQL plan classifies and renders its DDL under the PostgreSQL
// grammar: the summary counts every table — including ones whose column types
// only exist in PostgreSQL, and once for the table two ALTERs target — and
// the DDL block shows the statements in their own dialect's canonical form
// rather than reformatted under the MySQL grammar's quoting rules. The multi-clause ALTER is parseable under both
// grammars, so its rendering pins the dialect routing: PostgreSQL's canonical
// form keeps bare identifiers where the MySQL formatter would backtick them.
func TestRenderPlanComment_PostgresDialect(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database:     "testapp",
		DatabaseType: "postgres",
		Environment:  "staging",
		Changes: []KeyspaceChangeData{{
			Keyspace: "testapp",
			Statements: []string{
				"CREATE TABLE sessions (id uuid PRIMARY KEY, payload jsonb, created_at timestamptz)",
				"ALTER TABLE users ADD COLUMN raw bytea",
				"ALTER TABLE users ADD COLUMN a integer, ADD COLUMN b text",
			},
		}},
	})

	assert.Contains(t, out, "📋 **Plan**: **1** table to create, **1** table to alter")
	assert.Contains(t, out, "uuid")
	assert.Contains(t, out, "jsonb")
	assert.Contains(t, out, "timestamptz")
	assert.Contains(t, out, "bytea")
	assert.Contains(t, out, "ALTER TABLE users\n    ADD COLUMN a int,\n    ADD COLUMN b text;")
	assert.NotContains(t, out, "`sessions`", "identifiers must not gain MySQL backtick quoting")
	assert.NotContains(t, out, "`users`", "identifiers must not gain MySQL backtick quoting")
}

func TestWritePlanDDLBlocks_PostgresCreateSet(t *testing.T) {
	var out strings.Builder
	writePlanDDLBlocks(&out, []string{
		"CREATE TABLE t (id bigint, v text); CREATE INDEX t_v_idx ON t (v)",
	}, schema.DialectPostgres, newDDLBlockBudget(1, commentBodyLimit))

	assert.Equal(t, "```sql\nCREATE TABLE t (\n    id bigint,\n    v text\n);\nCREATE INDEX t_v_idx ON t USING btree (v);\n```\n\n", out.String())
}

// Each statement in a plan is one unit of work the apply runs, so the plan
// renders each as its own SQL block, in plan order: a reviewer reads one
// table's change at a time instead of picking it out of a single run of
// every statement. A greenfield create set stays one block, since its
// indexes ship with the table, and a concurrent index build on an existing
// table is a block of its own.
func TestWritePlanDDLBlocks_OneBlockPerStatement(t *testing.T) {
	var out strings.Builder
	writePlanDDLBlocks(&out, []string{
		"ALTER TABLE settings ADD COLUMN prefix text",
		"ALTER TABLE ledger ALTER COLUMN amount TYPE numeric",
		"CREATE TABLE t (id bigint, v text); CREATE INDEX t_v_idx ON t (v)",
		"CREATE INDEX CONCURRENTLY events_at_idx ON events (at)",
	}, schema.DialectPostgres, newDDLBlockBudget(1, commentBodyLimit))

	assert.Equal(t, strings.Join([]string{
		"```sql\nALTER TABLE settings ADD COLUMN prefix text;\n```\n",
		"```sql\nALTER TABLE ledger ALTER COLUMN amount TYPE numeric;\n```\n",
		"```sql\nCREATE TABLE t (\n    id bigint,\n    v text\n);\nCREATE INDEX t_v_idx ON t USING btree (v);\n```\n",
		"```sql\nCREATE INDEX CONCURRENTLY events_at_idx ON events USING btree (at);\n```\n",
	}, "\n")+"\n", out.String())
}
