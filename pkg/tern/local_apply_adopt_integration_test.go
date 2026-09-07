//go:build integration

package tern

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// adoptTestFixture is a client with one table on the target and a stored plan
// that alters it, ready to be dispatched.
type adoptTestFixture struct {
	client *LocalClient
	stor   storage.Storage
	db     *sql.DB
	dsn    string
	planID string
}

// newAdoptTestFixture prepares a database with a single table and a plan that
// adds a column to it — the smallest change set two dispatches can agree or
// disagree on.
func newAdoptTestFixture(t *testing.T, desired map[string]string) *adoptTestFixture {
	t.Helper()

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTasks(t, dsn)
	cleanupTestTables(t, dsn)

	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(t.Context()))

	_, err = db.ExecContext(t.Context(), "CREATE TABLE users (id INT PRIMARY KEY)")
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	t.Cleanup(func() { utils.CloseAndLog(stor) })

	client, err := NewLocalClient(LocalConfig{
		Database:  "testdb",
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: dsn,
	}, stor, logger)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(client) })

	planResp, err := client.Plan(t.Context(), &ternv1.PlanRequest{
		Type:     storage.DatabaseTypeMySQL,
		Database: "testdb",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"testdb": {Files: buildSchemaWithAllTables(t, dsn, desired)},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, planResp.PlanId)

	return &adoptTestFixture{client: client, stor: stor, db: db, dsn: dsn, planID: planResp.PlanId}
}

// dispatch applies the fixture's plan under its own idempotency key, so each
// call is a distinct dispatch identity rather than an idempotent replay.
func (f *adoptTestFixture) dispatch(t *testing.T, key string) *ternv1.ApplyResponse {
	t.Helper()
	return f.dispatchPlan(t, f.planID, key)
}

// dispatchPlan applies a named plan, for tests that re-plan after the target
// changes rather than reusing the plan the fixture was built with.
func (f *adoptTestFixture) dispatchPlan(t *testing.T, planID, key string) *ternv1.ApplyResponse {
	t.Helper()
	resp, err := f.client.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:         planID,
		Environment:    localClientTestEnvironment,
		Database:       "testdb",
		Type:           storage.DatabaseTypeMySQL,
		IdempotencyKey: key,
	})
	require.NoError(t, err)
	return resp
}

// planFor stores a plan for the target as it stands now, so a test can re-plan
// against a target that changed since the fixture was built.
func (f *adoptTestFixture) planFor(t *testing.T, desired map[string]string) string {
	t.Helper()
	resp, err := f.client.Plan(t.Context(), &ternv1.PlanRequest{
		Type:     storage.DatabaseTypeMySQL,
		Database: "testdb",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"testdb": {Files: buildSchemaWithAllTables(t, f.dsn, desired)},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.PlanId)
	return resp.PlanId
}

// A row copy can outlive the apply identity that started it: the work keeps
// running on the target and keeps holding the database, but the dispatch that
// started it is gone. Re-applying the same schema change resolves into that
// live apply instead of being refused by it, so the operator rejoins the copy
// already in flight rather than being told their own work is in the way.
func TestApplyAdoptsTheLiveApplyRunningTheSameChangeSet(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newAdoptTestFixture(t, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})

	first := f.dispatch(t, "schemabot:v1:adopt-first")
	require.True(t, first.Accepted, "first dispatch must be accepted: %s", first.ErrorMessage)
	require.NotEmpty(t, first.ApplyId)

	// The first apply holds the database with live work. A second dispatch under
	// a new identity has no idempotency key to resolve, so without adoption it
	// would be refused as a conflict with itself.
	second := f.dispatch(t, "schemabot:v1:adopt-second")
	require.True(t, second.Accepted, "re-apply of the running change set must be adopted: %s", second.ErrorMessage)
	assert.Equal(t, first.ApplyId, second.ApplyId,
		"the dispatch must resolve into the live apply rather than starting a second one")
	assert.Equal(t, first.ApplyOperationId, second.ApplyOperationId,
		"the adopted dispatch must address the live apply's own operation")

	// Adoption resolves an operator to existing work; it never creates more of it.
	var applies, tasks int
	require.NoError(t, f.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM applies").Scan(&applies))
	require.NoError(t, f.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM tasks").Scan(&tasks))
	assert.Equal(t, 1, applies, "adoption must not create a second apply")
	assert.Equal(t, 1, tasks, "adoption must not duplicate the live apply's work")
}

// A batch can finish one table and then lose its tracker with the rest still
// running. Re-planning no longer emits the finished table — its change is
// already on the target — so the dispatch carries a strictly smaller change set
// than the live apply owns. It is still the same work: what the operator is
// asking for is exactly what is still in flight, so the dispatch resolves into
// the live apply rather than being refused on behalf of a table that is done.
func TestApplyAdoptsALiveApplyThatAlreadyFinishedOneTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	desired := map[string]string{
		"users":  "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
		"orders": "CREATE TABLE orders (id INT PRIMARY KEY, total INT)",
	}
	f := newAdoptTestFixture(t, desired)
	_, err := f.db.ExecContext(t.Context(), "CREATE TABLE orders (id INT PRIMARY KEY)")
	require.NoError(t, err)

	first := f.dispatchPlan(t, f.planFor(t, desired), "schemabot:v1:adopt-partial-first")
	require.True(t, first.Accepted, "first dispatch must be accepted: %s", first.ErrorMessage)

	apply, err := f.stor.Applies().GetByApplyIdentifier(t.Context(), first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	tasks, err := f.stor.Tasks().GetByApplyID(t.Context(), apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 2, "the live apply must own both tables' changes")

	// `users` finishes: its change lands on the target, and its task completes.
	// The other table's copy is still in flight and still holds the database.
	_, err = f.db.ExecContext(t.Context(), "ALTER TABLE users ADD COLUMN email VARCHAR(255)")
	require.NoError(t, err)
	res, err := f.db.ExecContext(t.Context(),
		"UPDATE tasks SET state = ? WHERE apply_id = ? AND table_name = ?",
		state.Task.Completed, apply.ID, "users")
	require.NoError(t, err)
	affected, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected, "exactly one table's task must be completed")

	// Re-planned against the target as it now stands, only `orders` still differs.
	second := f.dispatchPlan(t, f.planFor(t, desired), "schemabot:v1:adopt-partial-second")
	require.True(t, second.Accepted,
		"a dispatch for the work still in flight must be adopted: %s", second.ErrorMessage)
	assert.Equal(t, first.ApplyId, second.ApplyId,
		"the dispatch must resolve into the live apply rather than starting a second one")

	var applies, taskRows int
	require.NoError(t, f.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM applies").Scan(&applies))
	require.NoError(t, f.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM tasks").Scan(&taskRows))
	assert.Equal(t, 1, applies, "adoption must not create a second apply")
	assert.Equal(t, 2, taskRows, "adoption must not duplicate the live apply's work")
}

// Adoption is only ever a resolution to work the operator already asked for, so
// a dispatch carrying a different schema change than the live apply is running
// stays refused. Resolving it would report a change as under way that nothing
// is applying.
func TestApplyRefusesToAdoptADifferentChangeSet(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newAdoptTestFixture(t, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})

	first := f.dispatch(t, "schemabot:v1:adopt-diff-first")
	require.True(t, first.Accepted, "first dispatch must be accepted: %s", first.ErrorMessage)

	// A plan for a different column against the same table and database.
	other, err := f.client.Plan(t.Context(), &ternv1.PlanRequest{
		Type:     storage.DatabaseTypeMySQL,
		Database: "testdb",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"testdb": {Files: buildSchemaWithAllTables(t, f.dsn, map[string]string{
				"users": "CREATE TABLE users (id INT PRIMARY KEY, phone VARCHAR(32))",
			})},
		},
	})
	require.NoError(t, err)

	resp, err := f.client.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:         other.PlanId,
		Environment:    localClientTestEnvironment,
		Database:       "testdb",
		Type:           storage.DatabaseTypeMySQL,
		IdempotencyKey: "schemabot:v1:adopt-diff-second",
	})
	require.NoError(t, err)
	assert.False(t, resp.Accepted, "a different change set must not be adopted")
	assert.Contains(t, resp.ErrorMessage, "schema change already in progress",
		"the refusal must still name what holds the database")
	assert.Contains(t, resp.ErrorMessage, "users",
		"the refusal names the table whose work holds the database")

	// The refusal also travels as structured facts, so a control plane can tell
	// an operator why the database is busy without rendering engine prose. The
	// engine's own apply identifier is not among them: it resolves only here, so
	// an operator who took it to the control-plane CLI would be refused.
	require.NotNil(t, resp.Conflict, "a refused dispatch reports the conflict that refused it")
	assert.Equal(t, "users", resp.Conflict.Table)
	assert.NotEmpty(t, resp.Conflict.BlockingState, "the state holding the database decides what an operator does next")
	assert.NotContains(t, resp.ErrorMessage, first.ApplyId)
}

// A terminal apply is not running anything to rejoin, so a dispatch is never
// resolved into it, however its own work is recorded. Its stopped task rests
// with no driver coming for it, so it holds nothing either: the dispatch gets
// an apply of its own rather than joining a settled one or being refused by it.
// The new apply changes the same table, so it meets the copy the resting task
// left behind and is recorded as the apply that took that work over — a later
// start on the settled apply is refused rather than replaying it.
func TestApplyDoesNotAdoptATerminalApply(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newAdoptTestFixture(t, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})

	first := f.dispatch(t, "schemabot:v1:adopt-terminal-first")
	require.True(t, first.Accepted, "first dispatch must be accepted: %s", first.ErrorMessage)

	// Terminalize the apply while its work stays recorded as unfinished — the
	// split between the two planes that adoption must not paper over.
	apply, err := f.stor.Applies().GetByApplyIdentifier(t.Context(), first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	_, err = f.db.ExecContext(t.Context(), "UPDATE applies SET state = ? WHERE id = ?", state.Apply.Failed, apply.ID)
	require.NoError(t, err)
	_, err = f.db.ExecContext(t.Context(), "UPDATE tasks SET state = ? WHERE apply_id = ?", state.Task.Stopped, apply.ID)
	require.NoError(t, err)

	resp := f.dispatch(t, "schemabot:v1:adopt-terminal-second")
	require.True(t, resp.Accepted, "a resting task of a terminal apply holds nothing: %s", resp.ErrorMessage)
	assert.NotEqual(t, first.ApplyId, resp.ApplyId,
		"a terminal apply has no live work to adopt, so the dispatch gets its own apply")

	superseded, err := f.stor.Applies().GetByApplyIdentifier(t.Context(), first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, superseded)
	assert.Equal(t, resp.ApplyId, superseded.SupersededBy,
		"the dispatch that meets the resting copy is recorded as having taken its work over")
}

// An apply that is reverting is live, but it is running the change backwards.
// Resolving a forward dispatch into it would report the operator's schema
// change as under way while the system is actively removing it, so the refusal
// stands and names the work the operator has to deal with first.
func TestApplyRefusesToAdoptAnApplyThatIsUndoingItsChange(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newAdoptTestFixture(t, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})

	first := f.dispatch(t, "schemabot:v1:adopt-reverting-first")
	require.True(t, first.Accepted, "first dispatch must be accepted: %s", first.ErrorMessage)

	apply, err := f.stor.Applies().GetByApplyIdentifier(t.Context(), first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	_, err = f.db.ExecContext(t.Context(), "UPDATE applies SET state = ? WHERE id = ?", state.Apply.Reverting, apply.ID)
	require.NoError(t, err)

	resp := f.dispatch(t, "schemabot:v1:adopt-reverting-second")
	assert.False(t, resp.Accepted, "an apply undoing its change is not forward work a dispatch can rejoin")
	assert.Contains(t, resp.ErrorMessage, "schema change already in progress")
}

// Two targets can share one namespace and environment, and the operation is the
// only row in the comparison that records which one an apply runs against. A
// dispatch for a different target therefore stays refused even though its
// change set is identical — adopting it would report a schema change as under
// way on a target nothing is applying it to.
func TestApplyRefusesToAdoptAnOperationRunningAgainstADifferentTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newAdoptTestFixture(t, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})

	first := f.dispatch(t, "schemabot:v1:adopt-target-first")
	require.True(t, first.Accepted, "first dispatch must be accepted: %s", first.ErrorMessage)

	apply, err := f.stor.Applies().GetByApplyIdentifier(t.Context(), first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	res, err := f.db.ExecContext(t.Context(),
		"UPDATE apply_operations SET target = ? WHERE apply_id = ?", "another-target", apply.ID)
	require.NoError(t, err)
	affected, err := res.RowsAffected()
	require.NoError(t, err)
	require.Positive(t, affected, "the live apply must have an operation whose target can differ")

	resp := f.dispatch(t, "schemabot:v1:adopt-target-second")
	assert.False(t, resp.Accepted, "work running against another target is not the dispatch's own")
	assert.Contains(t, resp.ErrorMessage, "schema change already in progress")
}

// An operation that has finished is not running the dispatch's work even under
// a parent apply that is still live, so its id is never handed back: it would
// resolve the operator to a handle that never moves again.
func TestApplyRefusesToAdoptAnOperationThatHasAlreadyTerminalized(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newAdoptTestFixture(t, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})

	first := f.dispatch(t, "schemabot:v1:adopt-op-terminal-first")
	require.True(t, first.Accepted, "first dispatch must be accepted: %s", first.ErrorMessage)

	apply, err := f.stor.Applies().GetByApplyIdentifier(t.Context(), first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	_, err = f.db.ExecContext(t.Context(),
		"UPDATE apply_operations SET state = ? WHERE apply_id = ?", state.Apply.Completed, apply.ID)
	require.NoError(t, err)
	_, err = f.db.ExecContext(t.Context(),
		"UPDATE applies SET state = ? WHERE id = ?", state.Apply.Running, apply.ID)
	require.NoError(t, err)

	resp := f.dispatch(t, "schemabot:v1:adopt-op-terminal-second")
	assert.False(t, resp.Accepted, "a finished operation is not live work to resolve into")
	assert.Contains(t, resp.ErrorMessage, "schema change already in progress")
}

// Adoption writes nothing to the apply it resolves into, so its repeatability
// comes from the decision being a pure function of stored state rather than
// from a recorded idempotency key. A dispatcher that retries under a fresh key
// after losing its answer therefore lands on the same apply and the same
// operation instead of starting a second copy of work already running. The
// apply's own timeline records that it changed hands, so who is driving it and
// since when survives the retention of any server log.
func TestAdoptingTheSameLiveApplyIsRepeatableAndRecordedOnItsTimeline(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newAdoptTestFixture(t, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})

	first := f.dispatch(t, "schemabot:v1:adopt-repeat-first")
	require.True(t, first.Accepted, "first dispatch must be accepted: %s", first.ErrorMessage)

	for _, key := range []string{"schemabot:v1:adopt-repeat-retry-1", "schemabot:v1:adopt-repeat-retry-2"} {
		retry := f.dispatch(t, key)
		require.True(t, retry.Accepted, "every retry of the running change set must be adopted: %s", retry.ErrorMessage)
		assert.Equal(t, first.ApplyId, retry.ApplyId, "each retry must resolve into the same live apply")
		assert.Equal(t, first.ApplyOperationId, retry.ApplyOperationId, "each retry must address the same operation")
	}

	var applies, tasks int
	require.NoError(t, f.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM applies").Scan(&applies))
	require.NoError(t, f.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM tasks").Scan(&tasks))
	assert.Equal(t, 1, applies, "repeated adoption must not create a second apply")
	assert.Equal(t, 1, tasks, "repeated adoption must not duplicate the live apply's work")

	apply, err := f.stor.Applies().GetByApplyIdentifier(t.Context(), first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	logs, err := f.stor.ApplyLogs().GetByApply(t.Context(), apply.ID)
	require.NoError(t, err)
	var adoptions int
	for _, l := range logs {
		if l.EventType == "apply_adopted" {
			adoptions++
			assert.Contains(t, l.Message, "already running its change set",
				"the timeline entry must say what the dispatch resolved into")
		}
	}
	assert.Equal(t, 2, adoptions, "each adoption must leave its own entry on the apply's timeline")
}

// A batch at realistic size: ten tables having a column widened, one of which
// finished before the apply lost its tracker. The operator's re-plan carries
// nine changes against a live apply that owns ten, and the change is a column
// type modification rather than an addition — the two sides of the comparison
// render it from different sources, so the DDL each produces must still agree
// once canonicalized. A live task carrying stale error text from a failure it
// already recovered from must not perturb the match either.
func TestApplyAdoptsALiveMultiTableApplyThatFinishedOneTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newAdoptTestFixture(t, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})

	const tableCount = 10
	desired := make(map[string]string, tableCount)
	names := make([]string, 0, tableCount)
	for i := range tableCount {
		name := fmt.Sprintf("widgets_%02d", i+1)
		names = append(names, name)
		_, err := f.db.ExecContext(t.Context(),
			fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, owner_id BIGINT)", name))
		require.NoError(t, err)
		desired[name] = fmt.Sprintf(
			"CREATE TABLE %s (id INT PRIMARY KEY, owner_id BIGINT NOT NULL)", name)
	}

	first := f.dispatchPlan(t, f.planFor(t, desired), "schemabot:v1:adopt-batch-first")
	require.True(t, first.Accepted, "first dispatch must be accepted: %s", first.ErrorMessage)

	apply, err := f.stor.Applies().GetByApplyIdentifier(t.Context(), first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	tasks, err := f.stor.Tasks().GetByApplyID(t.Context(), apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, tableCount, "the live apply must own every table's change")

	// The first table finishes: its change lands on the target and its task
	// completes. The rest are still in flight and still hold the database.
	finished := names[0]
	_, err = f.db.ExecContext(t.Context(),
		fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN owner_id BIGINT NOT NULL", finished))
	require.NoError(t, err)
	res, err := f.db.ExecContext(t.Context(),
		"UPDATE tasks SET state = ? WHERE apply_id = ? AND table_name = ?",
		state.Task.Completed, apply.ID, finished)
	require.NoError(t, err)
	affected, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected, "exactly one table's task must be completed")

	// A copy that recovered from a transient failure keeps running while still
	// carrying the error text of the failure it survived.
	res, err = f.db.ExecContext(t.Context(),
		"UPDATE tasks SET state = ?, error_message = ? WHERE apply_id = ? AND table_name = ?",
		state.Task.Running, "schema change failed: failed to execute chunklet insert",
		apply.ID, names[1])
	require.NoError(t, err)
	affected, err = res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected, "one table's copy must be running with stale error text")

	// Re-planned against the target as it now stands, only the nine unfinished
	// tables still differ.
	secondPlan := f.planFor(t, desired)
	second := f.dispatchPlan(t, secondPlan, "schemabot:v1:adopt-batch-second")
	require.True(t, second.Accepted,
		"a dispatch for the work still in flight must be adopted: %s", second.ErrorMessage)
	assert.Equal(t, first.ApplyId, second.ApplyId,
		"the dispatch must resolve into the live apply rather than starting a second one")
	assert.Equal(t, first.ApplyOperationId, second.ApplyOperationId,
		"the adopted dispatch must address the live apply's own operation")

	var applies int
	require.NoError(t, f.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM applies").Scan(&applies))
	assert.Equal(t, 1, applies, "adoption must not create a second apply")

	tasksAfter, err := f.stor.Tasks().GetByApplyID(t.Context(), apply.ID)
	require.NoError(t, err)
	assert.Len(t, tasksAfter, tableCount, "adoption must not duplicate the live apply's work")
}

// Excluding a completed table from the live side rests on the target really
// having that change: the planner reads the target, so a table wrongly marked
// finished is still planned, lands on the dispatch side alone, and unbalances the
// comparison. A dispatch is then refused rather than resolved into an apply that
// is not running the operator's change — the boundary that makes the exclusion
// safe to make at all.
func TestApplyRefusesAdoptionWhenACompletedTaskChangeNeverLanded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	desired := map[string]string{
		"users":  "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
		"orders": "CREATE TABLE orders (id INT PRIMARY KEY, total INT)",
	}
	f := newAdoptTestFixture(t, desired)
	_, err := f.db.ExecContext(t.Context(), "CREATE TABLE orders (id INT PRIMARY KEY)")
	require.NoError(t, err)

	first := f.dispatchPlan(t, f.planFor(t, desired), "schemabot:v1:adopt-lying-first")
	require.True(t, first.Accepted, "first dispatch must be accepted: %s", first.ErrorMessage)

	apply, err := f.stor.Applies().GetByApplyIdentifier(t.Context(), first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)

	// `users` is marked finished, but its ALTER is deliberately never run, so the
	// target still lacks the column and the planner still asks for it.
	res, err := f.db.ExecContext(t.Context(),
		"UPDATE tasks SET state = ? WHERE apply_id = ? AND table_name = ?",
		state.Task.Completed, apply.ID, "users")
	require.NoError(t, err)
	affected, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected, "exactly one table's task must be marked completed")

	second := f.dispatchPlan(t, f.planFor(t, desired), "schemabot:v1:adopt-lying-second")
	assert.False(t, second.Accepted,
		"a dispatch carrying work the live apply is not running must not be adopted")

	var applies int
	require.NoError(t, f.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM applies").Scan(&applies))
	assert.Equal(t, 1, applies, "a refused dispatch must not create a second apply")
}
