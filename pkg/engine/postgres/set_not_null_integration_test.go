//go:build integration

package postgres

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/testutil"
)

// columnNotNull reports whether the column of public.<table> is NOT NULL.
func columnNotNull(t *testing.T, db *sql.DB, table, column string) bool {
	t.Helper()
	var notNull bool
	require.NoError(t, db.QueryRowContext(t.Context(), `
		SELECT a.attnotnull
		FROM pg_catalog.pg_attribute a
		WHERE a.attrelid = pg_catalog.to_regclass('public.' || pg_catalog.quote_ident($1))
		  AND a.attname = $2`, table, column).Scan(&notNull))
	return notNull
}

// constraintCount returns how many constraints of public.<table> are named name.
func constraintCount(t *testing.T, db *sql.DB, table, name string) int {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), `
		SELECT pg_catalog.count(*)
		FROM pg_catalog.pg_constraint c
		WHERE c.conrelid = pg_catalog.to_regclass('public.' || pg_catalog.quote_ident($1))
		  AND c.conname = $2`, table, name).Scan(&count))
	return count
}

// An owner makes two columns NOT NULL, one of them named with a keyword. Each
// runs as the online sequence: a helper CHECK added NOT VALID, its validation
// while writes keep flowing, SET NOT NULL, and the helper's drop. Nothing in
// the plan is blocked, the apply completes every step, both columns are NOT
// NULL with no helper left behind, and a fresh plan is empty.
func TestEngineApplySetNotNull(t *testing.T) {
	const database = "apply_set_not_null_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.accounts (id bigint PRIMARY KEY, display_name text, "order" integer);
		INSERT INTO public.accounts VALUES (1, 'Ada', 1), (2, 'Grace', 2)`)
	require.NoError(t, err)
	files := map[string]string{
		"accounts.sql": `CREATE TABLE accounts (id bigint PRIMARY KEY, display_name text NOT NULL, "order" integer NOT NULL)`,
	}
	eng := New()

	plan := planForeignKeyFiles(t, eng, database, dsn, files)
	assert.Equal(t, []string{
		`ALTER TABLE "public"."accounts" ADD CONSTRAINT "accounts_display_name_not_null_check" CHECK ("display_name" IS NOT NULL) NOT VALID`,
		`ALTER TABLE "public"."accounts" VALIDATE CONSTRAINT "accounts_display_name_not_null_check"`,
		`ALTER TABLE "public"."accounts" ALTER COLUMN "display_name" SET NOT NULL`,
		`ALTER TABLE "public"."accounts" DROP CONSTRAINT "accounts_display_name_not_null_check"`,
		`ALTER TABLE "public"."accounts" ADD CONSTRAINT "accounts_order_not_null_check" CHECK ("order" IS NOT NULL) NOT VALID`,
		`ALTER TABLE "public"."accounts" VALIDATE CONSTRAINT "accounts_order_not_null_check"`,
		`ALTER TABLE "public"."accounts" ALTER COLUMN "order" SET NOT NULL`,
		`ALTER TABLE "public"."accounts" DROP CONSTRAINT "accounts_order_not_null_check"`,
	}, executableDDL(t, plan, "accounts"), "four steps per column, none blocked")
	for _, sc := range plan.Changes {
		for _, tc := range sc.TableChanges {
			assert.False(t, tc.IsUnsafe, "%s drops only the plan's own helper", tc.DDL)
		}
	}

	outcomes := applyPlannedSteps(t, eng, dsn, plan)
	require.Len(t, outcomes, 8)
	for _, progress := range outcomes {
		assert.Equal(t, engine.StateCompleted, progress.State, progress.ErrorMessage)
	}

	assert.True(t, columnNotNull(t, db, "accounts", "display_name"))
	assert.True(t, columnNotNull(t, db, "accounts", "order"))
	assert.Zero(t, constraintCount(t, db, "accounts", "accounts_display_name_not_null_check"), "the helper is dropped")
	assert.Zero(t, constraintCount(t, db, "accounts", "accounts_order_not_null_check"), "the helper is dropped")

	replan := planForeignKeyFiles(t, eng, database, dsn, files)
	assert.True(t, replan.NoChanges, "the applied columns match the file, got %+v", replan.Changes)
}

// An owner makes a column NOT NULL while a row still holds a null. The apply
// adds the helper check, then fails at its validation, naming the helper and
// the fix, and the column stays nullable. Once the owner fixes the row, the
// retry runs the stored validation and the steps after it, which finishes the
// change and leaves no helper behind.
func TestEngineApplySetNotNullOverANullRow(t *testing.T) {
	db, dsn, eng, plan := failSetNotNullOverANullRow(t, "apply_set_not_null_null_row_test")
	steps := plan.Changes[0].TableChanges

	_, err := db.ExecContext(t.Context(), `UPDATE public.accounts SET display_name = 'Grace' WHERE id = 2`)
	require.NoError(t, err)
	for i, tc := range steps[1:] {
		progress := applyStep(t, eng, dsn, plan.Changes[0].Namespace, fmt.Sprintf("retry-%d", i), tc)
		assert.Equal(t, engine.StateCompleted, progress.State, progress.ErrorMessage)
	}
	assert.True(t, columnNotNull(t, db, "accounts", "display_name"), "once the row is fixed, the retry finishes the change")
	assert.Zero(t, constraintCount(t, db, "accounts", "accounts_display_name_not_null_check"), "the helper is dropped")
}

// When the retries run out before the owner fixes the row, the helper the
// failed validation left stays NOT VALID. A new plan would drop it and add it
// again under the same name, which the apply refuses, so the owner drops the
// helper outside SchemaBot first. The plan after that is the same four steps
// as the first one, and it finishes the change.
func TestEngineApplySetNotNullAfterTheHelperIsDropped(t *testing.T) {
	db, dsn, eng, plan := failSetNotNullOverANullRow(t, "apply_set_not_null_helper_dropped_test")

	_, err := db.ExecContext(t.Context(), `UPDATE public.accounts SET display_name = 'Grace' WHERE id = 2`)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `ALTER TABLE public.accounts DROP CONSTRAINT accounts_display_name_not_null_check`)
	require.NoError(t, err)
	replan := planForeignKeyFiles(t, eng, "apply_set_not_null_helper_dropped_test", dsn, setNotNullOverANullRowFiles)
	require.Len(t, replan.Changes, 1)
	assert.Equal(t, ddlOf(plan.Changes[0].TableChanges), ddlOf(replan.Changes[0].TableChanges), "the plan is the original four steps")
	for _, progress := range applyPlannedSteps(t, eng, dsn, replan) {
		assert.Equal(t, engine.StateCompleted, progress.State, progress.ErrorMessage)
	}
	assert.True(t, columnNotNull(t, db, "accounts", "display_name"))
	assert.Zero(t, constraintCount(t, db, "accounts", "accounts_display_name_not_null_check"), "the helper is dropped")
}

// setNotNullOverANullRowFiles makes accounts.display_name NOT NULL.
var setNotNullOverANullRowFiles = map[string]string{
	"accounts.sql": "CREATE TABLE accounts (id bigint PRIMARY KEY, display_name text NOT NULL)",
}

// failSetNotNullOverANullRow plans display_name NOT NULL over a table where
// one row holds a null, applies the plan, and checks it stops at the helper's
// validation with the broken-rows detail. It returns the plan it applied.
func failSetNotNullOverANullRow(t *testing.T, database string) (*sql.DB, string, *Engine, *engine.PlanResult) {
	t.Helper()
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.accounts (id bigint PRIMARY KEY, display_name text);
		INSERT INTO public.accounts VALUES (1, 'Ada'), (2, NULL)`)
	require.NoError(t, err)
	eng := New()

	plan := planForeignKeyFiles(t, eng, database, dsn, setNotNullOverANullRowFiles)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 4, "helper, validate, SET NOT NULL, drop helper")
	outcomes := applyPlannedSteps(t, eng, dsn, plan)
	require.Len(t, outcomes, 2, "the helper is added and its validation fails")
	assert.Equal(t, engine.StateCompleted, outcomes[0].State, outcomes[0].ErrorMessage)
	assert.Equal(t, engine.StateFailed, outcomes[1].State)
	assert.Equal(t, `existing rows in "accounts" break check constraint "accounts_display_name_not_null_check"; the constraint is in place and checks new writes; fix those rows, then retry`, outcomes[1].ErrorMessage)
	assert.True(t, outcomes[1].Retryable)
	assert.False(t, columnNotNull(t, db, "accounts", "display_name"), "the column stays nullable")
	return db, dsn, eng, plan
}

// ddlOf lists the DDL of each step, in order.
func ddlOf(changes []engine.TableChange) []string {
	ddl := make([]string, len(changes))
	for i, tc := range changes {
		ddl[i] = tc.DDL
	}
	return ddl
}

// applyStep applies one planned step under key and waits for it to finish.
func applyStep(t *testing.T, eng *Engine, dsn, namespace, key string, tc engine.TableChange) *engine.ProgressResult {
	t.Helper()
	accepted, err := eng.Apply(t.Context(), &engine.ApplyRequest{
		Database:    "app",
		Changes:     []engine.SchemaChange{{Namespace: namespace, TableChanges: []engine.TableChange{tc}}},
		Credentials: &engine.Credentials{DSN: dsn},
		ResumeState: &engine.ResumeState{MigrationContext: key},
	})
	require.NoError(t, err)
	require.True(t, accepted.Accepted, accepted.Message)
	return awaitPostgresProgressFor(t, eng, key)
}

// An owner makes a column NOT NULL, and the apply runs the first three steps.
// Before the last step drops the helper, another session replaces the helper
// with a different check under the same name. The drop step reads the helper
// again under its lock, finds it is not the helper the plan added, and drops
// nothing; the operator is told to re-plan.
func TestEngineApplySetNotNullRefusesToDropAReplacedHelper(t *testing.T) {
	const database = "apply_set_not_null_replaced_helper_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.accounts (id bigint PRIMARY KEY, display_name text);
		INSERT INTO public.accounts VALUES (1, 'Ada')`)
	require.NoError(t, err)
	eng := New()
	plan := planForeignKeyFiles(t, eng, database, dsn, map[string]string{
		"accounts.sql": "CREATE TABLE accounts (id bigint PRIMARY KEY, display_name text NOT NULL)",
	})
	require.Len(t, plan.Changes, 1)
	steps := plan.Changes[0].TableChanges
	require.Len(t, steps, 4)
	for i, tc := range steps[:3] {
		progress := applyStep(t, eng, dsn, plan.Changes[0].Namespace, fmt.Sprintf("replaced-helper-%d", i), tc)
		require.Equal(t, engine.StateCompleted, progress.State, progress.ErrorMessage)
	}

	_, err = db.ExecContext(t.Context(), `
		ALTER TABLE public.accounts DROP CONSTRAINT accounts_display_name_not_null_check;
		ALTER TABLE public.accounts ADD CONSTRAINT accounts_display_name_not_null_check CHECK (display_name <> '')`)
	require.NoError(t, err)

	progress := applyStep(t, eng, dsn, plan.Changes[0].Namespace, "replaced-helper-3", steps[3])
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.Equal(t, `the object the planned drop on "accounts" removes changed after planning; re-plan against the current schema`, progress.ErrorMessage)
	assert.Equal(t, 1, constraintCount(t, db, "accounts", "accounts_display_name_not_null_check"), "the replacement is not dropped")
}
