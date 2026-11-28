//go:build integration

package postgres

import (
	"context"
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
// the fix, and ends the apply; the column stays nullable. Once the owner
// fixes the row, applying the stored validation and the steps after it
// finishes the change and leaves no helper behind.
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

// A helper a failed validation left on the table stays NOT VALID when the
// drive did not add it, so does not remove it. The owner fixes the row and plans
// again. The plan recognizes the helper as the sequence's own, so it plans no
// drop of live structure and nothing in it is unsafe: it starts at the
// helper's validation, and it finishes the change.
func TestEngineApplySetNotNullResumesAtTheHelperARunLeft(t *testing.T) {
	db, dsn, eng, plan := failSetNotNullOverANullRow(t, "apply_set_not_null_helper_left_test")

	_, err := db.ExecContext(t.Context(), `UPDATE public.accounts SET display_name = 'Grace' WHERE id = 2`)
	require.NoError(t, err)
	replan := planForeignKeyFiles(t, eng, "apply_set_not_null_helper_left_test", dsn, setNotNullOverANullRowFiles)
	require.Len(t, replan.Changes, 1)
	assert.Equal(t, ddlOf(plan.Changes[0].TableChanges[1:]), ddlOf(replan.Changes[0].TableChanges),
		"the plan is the original sequence from the helper's validation on")
	for _, tc := range replan.Changes[0].TableChanges {
		assert.False(t, tc.IsUnsafe, "%s: the helper is the sequence's own, so no step is unsafe", tc.DDL)
	}
	for _, progress := range applyPlannedSteps(t, eng, dsn, replan) {
		assert.Equal(t, engine.StateCompleted, progress.State, progress.ErrorMessage)
	}
	assert.True(t, columnNotNull(t, db, "accounts", "display_name"))
	assert.Zero(t, constraintCount(t, db, "accounts", "accounts_display_name_not_null_check"), "the helper is dropped")
}

// An owner can instead drop the helper outside SchemaBot. The plan after that is the same four steps as the first one, and
// it finishes the change.
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

// A SET NOT NULL that stops between adding its helper and dropping it, by a
// cancel or by a failure that ends the apply, leaves the helper rejecting
// NULL writes. Either release drops a helper the apply added, so those writes
// land again, and leaves one the apply did not add, naming the statement that
// drops it.
func TestEngineReleaseDropsOnlyTheHelperTheApplyAdded(t *testing.T) {
	for _, tc := range []struct {
		name    string
		release func(*Engine, context.Context, *engine.ReleaseArtifactsRequest) (*engine.ReleaseArtifactsResult, error)
	}{
		{name: "cancelled", release: (*Engine).ReleaseCancelledArtifacts},
		{name: "failed", release: (*Engine).ReleaseFailedArtifacts},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const helper = "accounts_display_name_not_null_check"
			db, dsn, eng, _ := failSetNotNullOverANullRow(t, "release_helper_"+tc.name+"_test")
			req := &engine.ReleaseArtifactsRequest{Database: "public", Tables: []string{"accounts"}, Credentials: &engine.Credentials{DSN: dsn}}

			result, err := tc.release(eng, t.Context(), req)
			require.NoError(t, err)
			assert.Empty(t, result.RemovedConstraints, "the apply did not add the helper, so the release keeps it")
			require.Len(t, result.PreexistingConstraints, 1)
			assert.Equal(t, `ALTER TABLE "public"."accounts" DROP CONSTRAINT "`+helper+`"`, result.PreexistingConstraints[0].Removal)
			assert.Equal(t, 1, constraintCount(t, db, "accounts", helper))

			req.AddedConstraints = map[string][]string{"accounts": {helper}}
			result, err = tc.release(eng, t.Context(), req)
			require.NoError(t, err)
			require.Len(t, result.RemovedConstraints, 1)
			assert.Equal(t, helper, result.RemovedConstraints[0].Name)
			assert.Zero(t, constraintCount(t, db, "accounts", helper), "the release drops the helper the apply added")
			_, err = db.ExecContext(t.Context(), `INSERT INTO public.accounts VALUES (3, NULL)`)
			require.NoError(t, err, "a NULL write lands again, as it did before the apply")
		})
	}
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
	assert.Equal(t, `existing rows in "accounts" break check constraint "accounts_display_name_not_null_check"; fix those rows, then apply again`, outcomes[1].ErrorMessage)
	assert.False(t, outcomes[1].Retryable, "a retry cannot pass until someone fixes the row, so the apply ends at once")
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
