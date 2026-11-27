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

// A schema file renames column note to memo on a table that holds rows,
// which plans as a drop of note and an add of memo, and drops the index on
// note the file no longer declares. The drop's unsafe reason
// tells the reviewer that note's values are deleted, that a revert does not
// bring them back, what goes with the column, and how to rename it without
// losing them.
func TestEnginePlanColumnRenameNamesTheDroppedColumn(t *testing.T) {
	const database = "plan_column_rename_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.notes (id bigint PRIMARY KEY, note text);
		CREATE INDEX notes_note_idx ON public.notes (note);
		INSERT INTO public.notes VALUES (1, 'first'), (2, 'second')`)
	require.NoError(t, err)
	req := &engine.PlanRequest{
		Database: database,
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"notes.sql": `CREATE TABLE notes (id bigint PRIMARY KEY, memo text);`,
			}},
		},
		Credentials: &engine.Credentials{DSN: dsn},
	}

	result, err := New().Plan(t.Context(), req)
	require.NoError(t, err)

	var unsafe []engine.TableChange
	for _, sc := range result.Changes {
		for _, tc := range sc.TableChanges {
			if tc.IsUnsafe {
				unsafe = append(unsafe, tc)
			}
		}
	}
	byDDL := map[string]string{}
	for _, tc := range unsafe {
		byDDL[tc.DDL] = tc.UnsafeReason
	}
	assert.Equal(t, map[string]string{
		"DROP INDEX CONCURRENTLY public.notes_note_idx": `statement drops index "notes_note_idx" from table "notes": queries that use it lose it, and putting it back means building it again`,
		"ALTER TABLE public.notes DROP note": `statement drops column "note" from table "notes": its values are deleted, and reverting the schema file adds the column back empty. ` +
			`It also drops index notes_note_idx. ` +
			`This plan also adds a column to the table, and SchemaBot does not rename columns. To rename one, add the new column, copy the values into it, and drop the old column in a later change`,
	}, byDDL, "the index the file no longer declares and the column are the plan's two drops")
}
