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

// indexDefinition returns the definition of public.<index>, or "" when no
// such index exists.
func indexDefinition(t *testing.T, db *sql.DB, index string) string {
	t.Helper()
	var definition string
	err := db.QueryRowContext(t.Context(),
		`SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' AND indexname = $1`, index).Scan(&definition)
	if errors.Is(err, sql.ErrNoRows) {
		return ""
	}
	require.NoError(t, err)
	return definition
}

// An owner removes an index from the schema file of a table larger than the
// size limit. The plan shows one unsafe DROP INDEX CONCURRENTLY step that is
// not blocked on size, since the drop reads no rows. The apply drops the
// index without locking the table against reads or writes, and a fresh plan
// is empty.
func TestEngineApplyDropsAnIndexOnATableOverTheSizeLimit(t *testing.T) {
	const database = "apply_drop_index_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.orders (id bigint PRIMARY KEY, status text);
		CREATE INDEX orders_status_idx ON public.orders (status);
		INSERT INTO public.orders SELECT g, 'open' FROM generate_series(1, 1000) g`)
	require.NoError(t, err)
	files := map[string]string{"orders.sql": "CREATE TABLE orders (id bigint PRIMARY KEY, status text)"}
	// A one-byte limit puts the table over it, as a large table would be.
	eng := NewWithTableSizeLimit(1)

	plan := planForeignKeyFiles(t, eng, database, dsn, files)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 1)
	step := plan.Changes[0].TableChanges[0]
	assert.Equal(t, "DROP INDEX CONCURRENTLY public.orders_status_idx", step.DDL)
	assert.Empty(t, step.ExecutionMode, step.ModeReason)
	assert.True(t, step.IsUnsafe)

	for _, progress := range applyPlannedSteps(t, eng, dsn, plan) {
		assert.Equal(t, engine.StateCompleted, progress.State, progress.ErrorMessage)
	}
	assert.Empty(t, indexDefinition(t, db, "orders_status_idx"), "the index is dropped")

	replan := planForeignKeyFiles(t, eng, database, dsn, files)
	assert.True(t, replan.NoChanges, "the table matches the file, got %+v", replan.Changes)
}

// Between the plan and the apply, another session drops the reviewed index
// and creates a different one under its name. The apply checks the index
// against the plan's record right before the drop, finds a different
// definition, and drops nothing. The failure says the plan is stale, and the
// index the plan never reviewed is still there.
func TestEngineApplyRefusesToDropAReplacedIndex(t *testing.T) {
	const database = "apply_drop_replaced_index_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.orders (id bigint PRIMARY KEY, status text, placed_at timestamptz);
		CREATE INDEX orders_status_idx ON public.orders (status)`)
	require.NoError(t, err)
	files := map[string]string{"orders.sql": "CREATE TABLE orders (id bigint PRIMARY KEY, status text, placed_at timestamptz)"}
	eng := New()

	plan := planForeignKeyFiles(t, eng, database, dsn, files)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 1)
	assert.Equal(t, "DROP INDEX CONCURRENTLY public.orders_status_idx", plan.Changes[0].TableChanges[0].DDL)

	_, err = db.ExecContext(t.Context(), `
		DROP INDEX public.orders_status_idx;
		CREATE INDEX orders_status_idx ON public.orders (placed_at)`)
	require.NoError(t, err)
	replacement := indexDefinition(t, db, "orders_status_idx")

	outcomes := applyPlannedSteps(t, eng, dsn, plan)
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateFailed, outcomes[0].State)
	assert.Contains(t, outcomes[0].ErrorMessage, `the index or constraint the planned statement for "orders" names changed after planning`)
	assert.False(t, outcomes[0].Retryable, "only a new plan changes the outcome")
	assert.Equal(t, replacement, indexDefinition(t, db, "orders_status_idx"), "the index the plan never reviewed stays")
}

// An owner narrows a soft-delete table's unique email index to rows with a
// positive quantity. The plan builds the new definition as
// orders_email_key_new, drops the old index, and renames the new one to its
// name; only the drop needs --allow-unsafe, and its reason names the
// replacement. The apply leaves one valid index under the old name with the
// new definition, and a fresh plan is empty. The table is over the size
// limit, as a table with an index worth replacing usually is: the build is
// bounded by its duration envelope, and the drop and rename read no rows.
func TestEngineApplyReplacesAChangedUniqueIndex(t *testing.T) {
	const database = "apply_replace_index_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.orders (id bigint PRIMARY KEY, email text, qty int NOT NULL, deleted_at timestamptz);
		CREATE UNIQUE INDEX orders_email_key ON public.orders (lower(email)) WHERE deleted_at IS NULL;
		INSERT INTO public.orders VALUES (1, 'a@example.com', 3, NULL), (2, 'b@example.com', 7, NULL)`)
	require.NoError(t, err)
	files := map[string]string{"orders.sql": `CREATE TABLE orders (id bigint PRIMARY KEY, email text, qty int NOT NULL, deleted_at timestamptz);
		CREATE UNIQUE INDEX orders_email_key ON orders (lower(email)) WHERE deleted_at IS NULL AND qty > 0;`}
	// A one-byte limit puts the table over it, as a large table would be.
	eng := NewWithTableSizeLimit(1)

	plan := planForeignKeyFiles(t, eng, database, dsn, files)
	require.Len(t, plan.Changes, 1)
	steps := plan.Changes[0].TableChanges
	require.Len(t, steps, 3)
	assert.Equal(t, "CREATE UNIQUE INDEX CONCURRENTLY orders_email_key_new ON public.orders USING btree (lower(email)) WHERE deleted_at IS NULL AND qty > 0", steps[0].DDL)
	assert.Equal(t, "DROP INDEX CONCURRENTLY public.orders_email_key", steps[1].DDL)
	assert.Equal(t, "ALTER INDEX public.orders_email_key_new RENAME TO orders_email_key", steps[2].DDL)
	for _, step := range steps {
		assert.Empty(t, step.ExecutionMode, step.ModeReason)
	}
	assert.False(t, steps[0].IsUnsafe)
	assert.True(t, steps[1].IsUnsafe)
	assert.Contains(t, steps[1].UnsafeReason, `statement replaces index "orders_email_key" on table "orders"`)
	assert.False(t, steps[2].IsUnsafe)

	for _, progress := range applyPlannedSteps(t, eng, dsn, plan) {
		assert.Equal(t, engine.StateCompleted, progress.State, progress.ErrorMessage)
	}
	assert.Contains(t, indexDefinition(t, db, "orders_email_key"), "WHERE ((deleted_at IS NULL) AND (qty > 0))")
	assert.Empty(t, indexDefinition(t, db, "orders_email_key_new"), "the new index takes the old index's name")

	replan := planForeignKeyFiles(t, eng, database, dsn, files)
	assert.True(t, replan.NoChanges, "the table matches the file, got %+v", replan.Changes)
}

// An owner widens a soft-delete table's unique email index to every row, but
// a deleted row shares a live row's email. The new definition fails to build,
// and the apply stops there: the old index is never dropped, still enforces
// uniqueness on live rows, and the failure names the build it left behind.
func TestEngineApplyKeepsTheOldUniqueIndexWhenItsReplacementFails(t *testing.T) {
	const database = "apply_replace_index_fails_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.orders (id bigint PRIMARY KEY, email text, deleted_at timestamptz);
		CREATE UNIQUE INDEX orders_email_key ON public.orders (lower(email)) WHERE deleted_at IS NULL;
		INSERT INTO public.orders VALUES (1, 'a@example.com', NULL), (2, 'A@example.com', now())`)
	require.NoError(t, err)
	files := map[string]string{"orders.sql": `CREATE TABLE orders (id bigint PRIMARY KEY, email text, deleted_at timestamptz);
		CREATE UNIQUE INDEX orders_email_key ON orders (lower(email));`}
	eng := New()
	original := indexDefinition(t, db, "orders_email_key")

	plan := planForeignKeyFiles(t, eng, database, dsn, files)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 3)

	outcomes := applyPlannedSteps(t, eng, dsn, plan)
	require.NotEmpty(t, outcomes)
	assert.Equal(t, engine.StateFailed, outcomes[0].State)
	assert.Contains(t, outcomes[0].ErrorMessage, "orders_email_key_new")
	assert.Equal(t, original, indexDefinition(t, db, "orders_email_key"), "the old index is untouched")
	_, err = db.ExecContext(t.Context(), `INSERT INTO public.orders VALUES (3, 'A@EXAMPLE.COM', NULL)`)
	require.Error(t, err, "the old index still rejects a duplicate live email")
}
