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
		"ALTER TABLE public.notes DROP note": `statement drops column "note" from table "notes": its values are deleted, and reverting the schema file does not bring them back. ` +
			`It also drops index notes_note_idx: CREATE INDEX notes_note_idx ON public.notes USING btree (note). ` +
			`This plan also adds a column to the table, and SchemaBot does not rename columns. To rename one, add the new column, copy the values into it, and drop the old column in a later change`,
	}, byDDL, "the index the file no longer declares and the column are the plan's two drops")
}

// A schema file widens the unique constraint orders_email_key and narrows the
// CHECK orders_qty_check under their names, and stops declaring a CHECK, a
// foreign key, and a unique index. The two changed constraints are replaced
// in place, so their reasons say the table keeps them throughout. Each of
// the three drops names the rule that stops being checked.
func TestEnginePlanDropReasonsNameWhatStopsBeingChecked(t *testing.T) {
	const database = "plan_drop_reasons_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.accounts (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (
			id bigint PRIMARY KEY, account_id bigint, email text, org text, qty int, code text,
			CONSTRAINT orders_email_key UNIQUE (email),
			CONSTRAINT orders_qty_check CHECK (qty > 0),
			CONSTRAINT orders_code_check CHECK (code <> ''),
			CONSTRAINT orders_account_fkey FOREIGN KEY (account_id) REFERENCES public.accounts (id));
		CREATE UNIQUE INDEX orders_org_uq ON public.orders (org);
		INSERT INTO public.accounts VALUES (1);
		INSERT INTO public.orders VALUES (1, 1, 'a@example.com', 'x', 2, 'c')`)
	require.NoError(t, err)
	req := &engine.PlanRequest{
		Database: database,
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"accounts.sql": `CREATE TABLE accounts (id bigint PRIMARY KEY);`,
				"orders.sql": `CREATE TABLE orders (
	id bigint PRIMARY KEY, account_id bigint, email text, org text, qty int, code text,
	CONSTRAINT orders_email_key UNIQUE (email, org),
	CONSTRAINT orders_qty_check CHECK (qty > 1));`,
			}},
		},
		Credentials: &engine.Credentials{DSN: dsn},
	}

	result, err := New().Plan(t.Context(), req)
	require.NoError(t, err)

	var reasons []string
	for _, sc := range result.Changes {
		for _, tc := range sc.TableChanges {
			if tc.IsUnsafe {
				reasons = append(reasons, tc.UnsafeReason)
			}
		}
	}
	assert.ElementsMatch(t, []string{
		`statement replaces constraint "orders_email_key" on table "orders" with the index built as "orders_email_key_new", dropping the old definition in the same step that attaches the new one: the table keeps the constraint throughout, and from then on its old definition no longer applies`,
		`statement replaces constraint "orders_qty_check" on table "orders", dropping the old definition in the same step that adds the new one: every write is checked against one of them, the next step checks existing rows against the new one, and from then on the old definition no longer applies`,
		`statement drops check constraint "orders_code_check" from table "orders": rows written after the drop are not checked against CHECK ((code <> ''::text))`,
		`statement drops foreign key "orders_account_fkey" from table "orders": rows written after the drop are not checked against FOREIGN KEY (account_id) REFERENCES accounts(id), so they can point at rows that do not exist`,
		`statement drops unique index "orders_org_uq" from table "orders": rows written after the drop can repeat the values it keeps unique, and building it again fails once any do`,
	}, reasons)
}
