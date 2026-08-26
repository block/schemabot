//go:build integration

package tern

import (
	"database/sql"
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

	db, err := sql.Open("mysql", dsn)
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
	resp, err := f.client.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:         f.planID,
		Environment:    localClientTestEnvironment,
		Database:       "testdb",
		Type:           storage.DatabaseTypeMySQL,
		IdempotencyKey: key,
	})
	require.NoError(t, err)
	return resp
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
	assert.Contains(t, resp.ErrorMessage, first.ApplyId,
		"the refusal must name the apply an operator acts on")
}

// A terminal apply is not running anything to rejoin, so a dispatch is never
// resolved into it even while its task still holds the database. The operator
// keeps the refusal, which names the apply and what clears it.
func TestApplyRefusesToAdoptATerminalApply(t *testing.T) {
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
	assert.False(t, resp.Accepted, "a terminal apply has no live work to adopt")
	assert.Contains(t, resp.ErrorMessage, "schema change already in progress")
}

// An apply that is reverting is live, but it is running the change backwards.
// Resolving a forward dispatch into it would report the operator's schema
// change as under way while the system is actively removing it, so the refusal
// stands and names the apply the operator has to deal with first.
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
