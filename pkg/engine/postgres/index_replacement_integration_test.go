//go:build integration

package postgres

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/testutil"
)

const (
	replaceEmailTable = `CREATE TABLE public.orders (id bigint PRIMARY KEY, email text, qty int NOT NULL, deleted_at timestamptz);
		CREATE UNIQUE INDEX orders_email_key ON public.orders (lower(email)) WHERE deleted_at IS NULL;
		INSERT INTO public.orders VALUES (1, 'a@example.com', 3, NULL), (2, 'b@example.com', 7, NULL)`
	replaceEmailFile = `CREATE TABLE orders (id bigint PRIMARY KEY, email text, qty int NOT NULL, deleted_at timestamptz);
		CREATE UNIQUE INDEX orders_email_key ON orders (lower(email)) WHERE deleted_at IS NULL AND qty > 0;`
	replacedEmailDef = "CREATE UNIQUE INDEX orders_email_key_new ON public.orders USING btree (lower(email)) WHERE ((deleted_at IS NULL) AND (qty > 0))"
)

func stepDDLs(plan *engine.PlanResult) []string {
	var ddls []string
	for _, change := range plan.Changes {
		for _, tc := range change.TableChanges {
			ddls = append(ddls, tc.DDL)
		}
	}
	return ddls
}

// An apply of a changed unique index stops after the build, and again after
// the drop. Each re-plan picks up where the apply stopped: after the build it
// keeps the valid orders_email_key_new and plans only the drop and the
// rename, and once the old index is gone it plans the rename alone. No step
// builds the index twice or drops anything twice, and the finished table
// matches the file.
func TestEngineResumesAChangedIndexReplacement(t *testing.T) {
	const database = "resume_replace_index_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), replaceEmailTable)
	require.NoError(t, err)
	files := map[string]string{"orders.sql": replaceEmailFile}
	eng := New()

	plan := planForeignKeyFiles(t, eng, database, dsn, files)
	require.Len(t, stepDDLs(plan), 3)
	build := applyStep(t, eng, dsn, "public", "resume-build", plan.Changes[0].TableChanges[0])
	require.Equal(t, engine.StateCompleted, build.State, build.ErrorMessage)
	assert.Equal(t, replacedEmailDef, indexDefinition(t, db, "orders_email_key_new"))

	afterBuild := planForeignKeyFiles(t, eng, database, dsn, files)
	assert.Equal(t, []string{
		"DROP INDEX CONCURRENTLY public.orders_email_key",
		"ALTER INDEX public.orders_email_key_new RENAME TO orders_email_key",
	}, stepDDLs(afterBuild), "the valid replacement is not built again")
	drop := applyStep(t, eng, dsn, "public", "resume-drop", afterBuild.Changes[0].TableChanges[0])
	require.Equal(t, engine.StateCompleted, drop.State, drop.ErrorMessage)
	assert.Empty(t, indexDefinition(t, db, "orders_email_key"))

	afterDrop := planForeignKeyFiles(t, eng, database, dsn, files)
	assert.Equal(t, []string{"ALTER INDEX public.orders_email_key_new RENAME TO orders_email_key"}, stepDDLs(afterDrop))
	rename := applyStep(t, eng, dsn, "public", "resume-rename", afterDrop.Changes[0].TableChanges[0])
	require.Equal(t, engine.StateCompleted, rename.State, rename.ErrorMessage)
	assert.Contains(t, indexDefinition(t, db, "orders_email_key"), "WHERE ((deleted_at IS NULL) AND (qty > 0))")

	assert.True(t, planForeignKeyFiles(t, eng, database, dsn, files).NoChanges)
}

// An apply of a changed unique index builds orders_email_key_new and stops.
// Before it resumes, the replacement is dropped, or rebuilt with another
// definition. The drop the plan reviewed then refuses: it checks the
// replacement right before dropping the old index, so the old index keeps
// enforcing its definition and the operator is told to plan again.
func TestEngineApplyKeepsTheOldIndexWhenItsReplacementChanged(t *testing.T) {
	for name, change := range map[string]string{
		"dropped":   `DROP INDEX public.orders_email_key_new`,
		"rebuilt":   `DROP INDEX public.orders_email_key_new; CREATE UNIQUE INDEX orders_email_key_new ON public.orders (email)`,
		"invalided": `UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'public.orders_email_key_new'::regclass`,
	} {
		t.Run(name, func(t *testing.T) {
			database := "replacement_changed_" + name
			dsn, db := testutil.StartPostgres(t, database)
			_, err := db.ExecContext(t.Context(), replaceEmailTable)
			require.NoError(t, err)
			eng := New()
			original := indexDefinition(t, db, "orders_email_key")

			plan := planForeignKeyFiles(t, eng, database, dsn, map[string]string{"orders.sql": replaceEmailFile})
			steps := plan.Changes[0].TableChanges
			require.Len(t, steps, 3)
			build := applyStep(t, eng, dsn, "public", "changed-build", steps[0])
			require.Equal(t, engine.StateCompleted, build.State, build.ErrorMessage)
			_, err = db.ExecContext(t.Context(), change)
			require.NoError(t, err)

			drop := applyStep(t, eng, dsn, "public", "changed-drop", steps[1])
			assert.Equal(t, engine.StateFailed, drop.State)
			assert.Contains(t, drop.ErrorMessage, `to replace the one it drops changed before the drop, so the old index was kept`)
			assert.Equal(t, original, indexDefinition(t, db, "orders_email_key"), "the old index is kept")
		})
	}
}

// constraintDefinition returns the definition of the named constraint on
// public.orders, or "" when there is none.
func constraintDefinition(t *testing.T, db *sql.DB, name string) string {
	t.Helper()
	var definition sql.NullString
	err := db.QueryRowContext(t.Context(), `SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c
		WHERE c.conrelid = 'public.orders'::regclass AND c.conname = $1`, name).Scan(&definition)
	if errors.Is(err, sql.ErrNoRows) {
		return ""
	}
	require.NoError(t, err)
	return definition.String
}

// An owner widens the orders table's unique code constraint to (code,
// region). The plan builds the new index concurrently as orders_code_key_new,
// then drops the old constraint and attaches the index as the new one in one
// statement, which needs --allow-unsafe. The apply checks the built index
// under the table's lock before attaching it: one rebuilt with other columns
// after the build is not attached, and the old constraint keeps enforcing.
func TestEngineApplyReplacesAChangedUniqueConstraint(t *testing.T) {
	const setup = `CREATE TABLE public.orders (id bigint PRIMARY KEY, code text NOT NULL, region text NOT NULL,
			CONSTRAINT orders_code_key UNIQUE (code));
		INSERT INTO public.orders VALUES (1, 'a', 'us'), (2, 'b', 'eu')`
	files := map[string]string{"orders.sql": `CREATE TABLE orders (id bigint PRIMARY KEY, code text NOT NULL, region text NOT NULL,
		CONSTRAINT orders_code_key UNIQUE (code, region));`}

	t.Run("the reviewed index becomes the constraint", func(t *testing.T) {
		const database = "replace_constraint_test"
		dsn, db := testutil.StartPostgres(t, database)
		_, err := db.ExecContext(t.Context(), setup)
		require.NoError(t, err)
		eng := New()

		plan := planForeignKeyFiles(t, eng, database, dsn, files)
		steps := plan.Changes[0].TableChanges
		require.Len(t, steps, 2)
		assert.Equal(t, "CREATE UNIQUE INDEX CONCURRENTLY orders_code_key_new ON public.orders USING btree (code, region)", steps[0].DDL)
		assert.Contains(t, steps[1].DDL, "DROP CONSTRAINT orders_code_key, ADD CONSTRAINT orders_code_key UNIQUE USING INDEX orders_code_key_new")
		assert.Empty(t, steps[1].ExecutionMode, steps[1].ModeReason)
		assert.True(t, steps[1].IsUnsafe)
		assert.Contains(t, steps[1].Metadata, reviewedAdoptionMetadataKey)

		for _, progress := range applyPlannedSteps(t, eng, dsn, plan) {
			assert.Equal(t, engine.StateCompleted, progress.State, progress.ErrorMessage)
		}
		assert.Equal(t, "UNIQUE (code, region)", constraintDefinition(t, db, "orders_code_key"))
		assert.True(t, planForeignKeyFiles(t, eng, database, dsn, files).NoChanges)
	})

	t.Run("an index rebuilt after the plan is not attached", func(t *testing.T) {
		const database = "replace_constraint_drift_test"
		dsn, db := testutil.StartPostgres(t, database)
		_, err := db.ExecContext(t.Context(), setup)
		require.NoError(t, err)
		eng := New()

		plan := planForeignKeyFiles(t, eng, database, dsn, files)
		steps := plan.Changes[0].TableChanges
		require.Len(t, steps, 2)
		build := applyStep(t, eng, dsn, "public", "constraint-build", steps[0])
		require.Equal(t, engine.StateCompleted, build.State, build.ErrorMessage)
		_, err = db.ExecContext(t.Context(), `DROP INDEX public.orders_code_key_new; CREATE UNIQUE INDEX orders_code_key_new ON public.orders (region, code)`)
		require.NoError(t, err)

		swap := applyStep(t, eng, dsn, "public", "constraint-swap", steps[1])
		assert.Equal(t, engine.StateFailed, swap.State)
		assert.NotEmpty(t, swap.ErrorMessage)
		assert.Equal(t, "UNIQUE (code)", constraintDefinition(t, db, "orders_code_key"), "the old constraint keeps enforcing")
		_, err = db.ExecContext(t.Context(), `INSERT INTO public.orders VALUES (3, 'a', 'eu')`)
		require.Error(t, err, "the old constraint still rejects a duplicate code")
	})
}

// An owner relaxes the orders table's quantity check from positive to
// non-negative. One statement drops the old check and adds the new one NOT
// VALID, so every write is checked by one rule or the other, and a second
// step validates it while writes continue. The finished table has the new
// rule, validated, and a fresh plan is empty.
func TestEngineApplyReplacesAChangedCheckConstraint(t *testing.T) {
	const database = "replace_check_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `CREATE TABLE public.orders (id bigint PRIMARY KEY, qty int NOT NULL,
			CONSTRAINT orders_qty_check CHECK (qty > 0));
		INSERT INTO public.orders VALUES (1, 3), (2, 7)`)
	require.NoError(t, err)
	files := map[string]string{"orders.sql": `CREATE TABLE orders (id bigint PRIMARY KEY, qty int NOT NULL,
		CONSTRAINT orders_qty_check CHECK (qty >= 0));`}
	eng := New()

	plan := planForeignKeyFiles(t, eng, database, dsn, files)
	steps := plan.Changes[0].TableChanges
	require.Len(t, steps, 2)
	assert.Contains(t, steps[0].DDL, "DROP CONSTRAINT orders_qty_check, ADD CONSTRAINT orders_qty_check CHECK")
	assert.Contains(t, steps[0].DDL, "NOT VALID")
	assert.True(t, steps[0].IsUnsafe)
	assert.Equal(t, "ALTER TABLE public.orders VALIDATE CONSTRAINT orders_qty_check", steps[1].DDL)
	for _, step := range steps {
		assert.Empty(t, step.ExecutionMode, step.ModeReason)
	}

	for _, progress := range applyPlannedSteps(t, eng, dsn, plan) {
		assert.Equal(t, engine.StateCompleted, progress.State, progress.ErrorMessage)
	}
	assert.Equal(t, "CHECK ((qty >= 0))", constraintDefinition(t, db, "orders_qty_check"))
	assert.True(t, planForeignKeyFiles(t, eng, database, dsn, files).NoChanges)
}
