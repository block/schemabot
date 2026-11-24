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

// planUndeclaredDrops plans the target against a schema that declares only
// users, so every other live table in public is planned as a drop.
func planUndeclaredDrops(t *testing.T, dsn string) *engine.PlanResult {
	t.Helper()
	result, err := New().Plan(t.Context(), &engine.PlanRequest{
		Database: "app",
		SchemaFiles: schema.SchemaFiles{"public": {Files: map[string]string{
			"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY)",
		}}},
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err)
	require.Len(t, result.Changes, 1)
	for _, change := range result.Changes[0].TableChanges {
		require.Empty(t, change.ExecutionMode, "%s: %s", change.Table, change.ModeReason)
	}
	return result
}

// An operator deletes the schema files of three tables, two of which a
// foreign key ties together, and applies the plan with the unsafe
// acknowledgment. The drive runs the drops in the plan's order, so the
// referencing table goes before the table it references, and every
// undeclared table is gone while the declared one stays.
func TestEngineApplyDropsUndeclaredTablesInForeignKeyOrder(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "table_drop_order")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.regions (id bigint PRIMARY KEY);
		CREATE TABLE public.warehouses (id bigint PRIMARY KEY, region_id bigint REFERENCES public.regions (id));
		CREATE TABLE public.categories (id bigint PRIMARY KEY, parent_id bigint REFERENCES public.categories (id));
		INSERT INTO public.regions VALUES (1);
		INSERT INTO public.warehouses VALUES (1, 1)`)
	require.NoError(t, err)

	result := planUndeclaredDrops(t, dsn)
	var order []string
	for _, change := range result.Changes[0].TableChanges {
		order = append(order, change.Table)
	}
	assert.Equal(t, []string{"categories", "warehouses", "regions"}, order)

	outcomes := applyPlannedSteps(t, New(), dsn, result)
	require.Len(t, outcomes, 3)
	for _, outcome := range outcomes {
		assert.Equal(t, engine.StateCompleted, outcome.State, outcome.ErrorMessage)
	}
	for _, table := range order {
		assert.False(t, testutil.PostgresTableExists(t, db, "public", table), "%s must be dropped", table)
	}
	assert.True(t, testutil.PostgresTableExists(t, db, "public", "users"))
}

// Between the review and the apply, someone adds a column to the table the
// plan drops. The table is no longer the one the operator accepted losing,
// so the apply refuses with a curated message, drops nothing, and asks for a
// re-plan.
func TestEngineApplyRefusesATableDropWhoseTableChangedAfterReview(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "table_drop_drift")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.legacy_users (id bigint PRIMARY KEY)`)
	require.NoError(t, err)

	result := planUndeclaredDrops(t, dsn)
	_, err = db.ExecContext(t.Context(), "ALTER TABLE public.legacy_users ADD COLUMN email text")
	require.NoError(t, err)

	outcomes := applyPlannedSteps(t, New(), dsn, result)
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateFailed, outcomes[0].State)
	assert.False(t, outcomes[0].Retryable, "a retry would meet the same changed table")
	assert.Contains(t, outcomes[0].ErrorMessage, `the object the planned drop on "legacy_users" removes changed after planning; re-plan against the current schema`)
	assert.True(t, testutil.PostgresTableExists(t, db, "public", "legacy_users"), "the changed table must be left in place")
}

// A view that reads the table makes the server refuse the drop, because
// SchemaBot never cascades a drop into objects the plan did not show. The
// apply fails, names the cause, and both the table and the view remain.
func TestEngineApplyRefusesATableDropAViewDependsOn(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "table_drop_view")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.legacy_users (id bigint PRIMARY KEY, email text);
		CREATE VIEW public.legacy_emails AS SELECT email FROM public.legacy_users`)
	require.NoError(t, err)

	outcomes := applyPlannedSteps(t, New(), dsn, planUndeclaredDrops(t, dsn))
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateFailed, outcomes[0].State)
	assert.Contains(t, outcomes[0].ErrorMessage, `a view or other object depends on table "legacy_users"`)
	assert.True(t, testutil.PostgresTableExists(t, db, "public", "legacy_users"))
}

// A table that is already gone when the apply reaches it has reached the
// state the plan asked for, as when a resumed apply meets a table its earlier
// attempt dropped. The apply completes, as the MySQL engine's does, and logs
// a warning rather than failing.
func TestEngineApplySkipsATableDropWhoseTableIsAlreadyGone(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "table_drop_absent")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.legacy_users (id bigint PRIMARY KEY)`)
	require.NoError(t, err)

	result := planUndeclaredDrops(t, dsn)
	_, err = db.ExecContext(t.Context(), "DROP TABLE public.legacy_users")
	require.NoError(t, err)

	outcomes := applyPlannedSteps(t, New(), dsn, result)
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateCompleted, outcomes[0].State, outcomes[0].ErrorMessage)
}
