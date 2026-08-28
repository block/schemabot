package templates

import (
	"testing"

	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
)

func TestDialectForEngine(t *testing.T) {
	tests := []struct {
		engine string
		want   schema.Dialect
	}{
		{"spirit", schema.DialectMySQL},
		{"planetscale", schema.DialectMySQL},
		{"strata", schema.DialectMySQL},
		{"postgres", schema.DialectPostgres},
		{"Postgres", schema.DialectPostgres},
		// The engine field on comment data also carries database-type and
		// display-cased values from older producers; all of them are
		// MySQL-family except postgres.
		{"mysql", schema.DialectMySQL},
		{"Vitess", schema.DialectMySQL},
		{"Spirit", schema.DialectMySQL},
		{"", schema.DialectMySQL},
		{"unknown", schema.DialectMySQL},
	}
	for _, tc := range tests {
		assert.Equal(t, tc.want, dialectForEngine(tc.engine), "engine %q", tc.engine)
	}
}

// A PostgreSQL apply's progress comment renders each table's DDL in the
// PostgreSQL grammar's canonical form — keywords uppercased by the Postgres
// deparser, identifiers left unquoted — rather than being reformatted (or
// left untouched as unparseable) under the MySQL grammar.
func TestRenderApplyStatusComment_PostgresDialect(t *testing.T) {
	out := RenderApplyStatusComment(ApplyStatusCommentData{
		ApplyID:     "apply-1",
		Database:    "testapp",
		Environment: "staging",
		State:       state.Apply.Running,
		Engine:      "postgres",
		Tables: []TableProgressData{{
			TableName: "users",
			Status:    state.Task.Running,
			DDL:       "alter table users add column payload jsonb",
		}},
	})

	assert.Contains(t, out, "ALTER TABLE users ADD COLUMN payload jsonb;")
	assert.NotContains(t, out, "ALTER TABLE `users`", "identifiers must not gain MySQL backtick quoting")
}

// A PostgreSQL apply's terminal summary comment renders each table's DDL in
// the PostgreSQL grammar's canonical form, matching the progress comment.
func TestRenderApplySummaryComment_PostgresDialect(t *testing.T) {
	out := RenderApplySummaryComment(ApplyStatusCommentData{
		ApplyID:     "apply-1",
		Database:    "testapp",
		Environment: "staging",
		State:       state.Apply.Completed,
		Engine:      "postgres",
		Tables: []TableProgressData{{
			TableName: "users",
			Status:    state.Task.Completed,
			DDL:       "alter table users add column payload jsonb",
		}},
	})

	assert.Contains(t, out, "ALTER TABLE users ADD COLUMN payload jsonb;")
	assert.NotContains(t, out, "ALTER TABLE `users`", "identifiers must not gain MySQL backtick quoting")
}
