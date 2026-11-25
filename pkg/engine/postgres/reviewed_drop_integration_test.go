//go:build integration

package postgres

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// planReviewedDrop plans the removal of users.legacy and returns the planned
// change, which carries the planner's record of the column it drops.
func planReviewedDrop(t *testing.T, dsn string) engine.TableChange {
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
	require.Len(t, result.Changes[0].TableChanges, 1)
	change := result.Changes[0].TableChanges[0]
	require.Equal(t, "ALTER TABLE public.users DROP legacy", change.DDL)
	require.NotEqual(t, engine.ExecutionModeBlocked, change.ExecutionMode, change.ModeReason)
	dropped, err := reviewedDropFromMetadata(change.Metadata)
	require.NoError(t, err)
	require.NotNil(t, dropped, "the plan must record the column it drops")
	assert.Equal(t, "legacy", dropped.Name)
	return change
}

func applyPlannedChange(t *testing.T, dsn string, change engine.TableChange) *engine.ProgressResult {
	t.Helper()
	eng := New()
	result, err := eng.Apply(t.Context(), plannedApplyRequest(dsn, change))
	require.NoError(t, err)
	require.True(t, result.Accepted)
	return awaitPostgresProgress(t, eng, change.Table)
}

func legacyColumnExists(t *testing.T, db *sql.DB) bool {
	t.Helper()
	var exists bool
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'legacy')`).Scan(&exists))
	return exists
}

// An operator reviews a plan that drops users.legacy and applies it while the
// column is still the one the plan read: the column is dropped and the apply
// completes.
func TestEngineApplyDropsTheReviewedColumn(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "reviewed_drop")
	_, err := db.ExecContext(t.Context(), "CREATE TABLE public.users (id bigint PRIMARY KEY, legacy text)")
	require.NoError(t, err)

	progress := applyPlannedChange(t, dsn, planReviewedDrop(t, dsn))
	assert.Equal(t, engine.StateCompleted, progress.State, progress.ErrorMessage)
	assert.False(t, legacyColumnExists(t, db))
}

// Between the review and the apply, someone drops users.legacy and adds a new
// column under the same name. The apply refuses with a curated message and
// drops nothing, because the column is no longer the one the operator
// reviewed.
func TestEngineApplyRefusesADropWhoseObjectChangedAfterReview(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "reviewed_drop_drift")
	_, err := db.ExecContext(t.Context(), "CREATE TABLE public.users (id bigint PRIMARY KEY, legacy text)")
	require.NoError(t, err)

	change := planReviewedDrop(t, dsn)
	_, err = db.ExecContext(t.Context(), "ALTER TABLE public.users DROP COLUMN legacy, ADD COLUMN legacy jsonb")
	require.NoError(t, err)

	progress := applyPlannedChange(t, dsn, change)
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.False(t, progress.Retryable, "a retry would meet the same changed column")
	assert.Contains(t, progress.ErrorMessage, `the object the planned drop on "users" removes changed after planning; re-plan against the current schema`)
	assert.True(t, legacyColumnExists(t, db), "the changed column must be left in place")
}

// A confirmation holds the re-plan to the record of the objects the reviewed
// plan acts on, so the record must name the object and nothing else. Planning
// the same drop twice against an unchanged column records the same object.
// After someone drops users.legacy and adds a column of the same name and type,
// the re-plan records a different object, so the confirmation sees the
// replacement even though the statement reads the same.
func TestPlanRecordsTheSameDropOnlyForTheSameObject(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "reviewed_drop_replan")
	_, err := db.ExecContext(t.Context(), "CREATE TABLE public.users (id bigint PRIMARY KEY, legacy text)")
	require.NoError(t, err)

	reviewed := planReviewedDrop(t, dsn)
	assert.Equal(t, reviewed.Metadata, planReviewedDrop(t, dsn).Metadata, "a re-plan of an unchanged column records the same object")

	_, err = db.ExecContext(t.Context(), "ALTER TABLE public.users DROP COLUMN legacy, ADD COLUMN legacy text")
	require.NoError(t, err)
	replanned := planReviewedDrop(t, dsn)
	assert.Equal(t, reviewed.DDL, replanned.DDL, "the statement reads the same")
	assert.NotEqual(t, reviewed.Metadata, replanned.Metadata, "a replaced column is a different object")
}

// An operator removes users.legacy from a table past the size limit.
// Dropping a column changes only the catalog, so the plan keeps the drop
// executable and the apply runs it through the reviewed-drop path despite
// the limit: the column is gone afterward.
func TestEngineApplyOversizedTableRunsReviewedColumnDrop(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "oversized_reviewed_drop_test")
	_, err := db.ExecContext(t.Context(), "CREATE TABLE public.users (id bigint PRIMARY KEY, legacy text)")
	require.NoError(t, err)

	eng := NewWithTableSizeLimit(1)
	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database: "oversized_reviewed_drop_test",
		SchemaFiles: schema.SchemaFiles{"public": {Files: map[string]string{
			"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY)",
		}}},
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err)
	require.Len(t, result.Changes, 1)
	require.Len(t, result.Changes[0].TableChanges, 1)
	change := result.Changes[0].TableChanges[0]
	require.Equal(t, "ALTER TABLE public.users DROP legacy", change.DDL)
	assert.Empty(t, change.ExecutionMode, "a column drop is catalog-only, so it stays executable above the limit")
	assert.Empty(t, change.ModeReason)

	accepted, err := eng.Apply(t.Context(), plannedApplyRequest(dsn, change))
	require.NoError(t, err)
	require.True(t, accepted.Accepted)
	progress := awaitPostgresProgress(t, eng, change.Table)
	assert.Equal(t, engine.StateCompleted, progress.State, progress.ErrorMessage)
	assert.False(t, legacyColumnExists(t, db))
}
