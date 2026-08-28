package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A PostgreSQL plan classifies and renders its DDL under the PostgreSQL
// grammar: the summary counts every statement — including ones whose column
// types only exist in PostgreSQL — and the DDL block shows the statements in
// their own dialect's canonical form rather than reformatted under the MySQL
// grammar's quoting rules.
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
			},
		}},
	})

	assert.Contains(t, out, "📋 **Plan**: **1** table to create, **1** table to alter")
	assert.Contains(t, out, "uuid")
	assert.Contains(t, out, "jsonb")
	assert.Contains(t, out, "timestamptz")
	assert.Contains(t, out, "bytea")
	assert.NotContains(t, out, "`sessions`", "identifiers must not gain MySQL backtick quoting")
	assert.NotContains(t, out, "`users`", "identifiers must not gain MySQL backtick quoting")
}
