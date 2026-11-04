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

// A stored generated column whose live expression differs from its schema
// file plans as one blocked SET EXPRESSION step instead of failing the plan,
// so the rest of the file stays reviewable: the unrelated column the same
// file adds is planned to run, and the column's index is left alone because
// the change is planned in place rather than as a drop and re-add. A blocked
// step blocks the whole plan's apply.
func TestEnginePlanGeneratedExpressionChange(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "plan_generated_expression_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.notes (
		    id bigint PRIMARY KEY,
		    kind int NOT NULL,
		    body text NOT NULL,
		    search tsvector GENERATED ALWAYS AS (CASE WHEN kind = 1 THEN to_tsvector('english', body) END) STORED
		);
		CREATE INDEX notes_search_idx ON public.notes USING gin (search)`)
	require.NoError(t, err)
	req := &engine.PlanRequest{
		Database: "plan_generated_expression_test",
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"notes.sql": `CREATE TABLE notes (
    id bigint PRIMARY KEY,
    kind int NOT NULL,
    body text NOT NULL,
    search tsvector GENERATED ALWAYS AS (CASE WHEN kind IN (1, 2) THEN to_tsvector('english', body) END) STORED,
    archived_at timestamptz
);
CREATE INDEX notes_search_idx ON notes USING gin (search);`,
			}},
		},
		Credentials: &engine.Credentials{DSN: dsn},
	}

	result, err := New().Plan(t.Context(), req)
	require.NoError(t, err)

	var changes []engine.TableChange
	for _, sc := range result.Changes {
		changes = append(changes, sc.TableChanges...)
	}
	require.Len(t, changes, 2, "only the expression change and the column add are planned; the index is untouched")

	byDDL := map[string]engine.TableChange{}
	for _, tc := range changes {
		byDDL[tc.DDL] = tc
	}
	addColumn, ok := byDDL["ALTER TABLE public.notes ADD COLUMN archived_at timestamp with time zone"]
	require.True(t, ok, "the unrelated column add is planned, got %v", byDDL)
	assert.NotEqual(t, engine.ExecutionModeBlocked, addColumn.ExecutionMode)

	setExpression, ok := byDDL["ALTER TABLE public.notes ALTER COLUMN search SET EXPRESSION AS (CASE WHEN kind = ANY(ARRAY[1, 2]) THEN to_tsvector('english'::regconfig, body) ELSE NULL::tsvector END)"]
	require.True(t, ok, "the expression change is planned in place, got %v", byDDL)
	assert.Equal(t, engine.ExecutionModeBlocked, setExpression.ExecutionMode)
	assert.Equal(t, `statement for table "notes" changes a stored generated column's expression, which rewrites every row under a lock that blocks reads and writes, so SchemaBot does not apply it; to keep the live expression, declare it in the schema file and re-plan; to change it, declare the live expression to apply the rest of the plan first, then run this statement (PostgreSQL 17 or later) outside SchemaBot through a separately reviewed process and put the new expression back in the file`, setExpression.ModeReason)
}
