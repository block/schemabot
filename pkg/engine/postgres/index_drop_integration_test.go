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
