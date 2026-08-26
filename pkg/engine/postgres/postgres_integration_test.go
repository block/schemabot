//go:build integration

package postgres

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

const postgresApplyDeadline = 10 * time.Second

// TestEnginePlanCreateTable proves a CREATE TABLE derived from desired state
// is planned with a blocked verdict: the native-safe path cannot execute that
// shape, so the plan the operator reviews must say so instead of emitting an
// executable change that deterministically fails at apply.
func TestEnginePlanCreateTable(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "plan_test")
	req := &engine.PlanRequest{
		Database: "plan_test",
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY, email text NOT NULL)",
			}},
		},
		Credentials: &engine.Credentials{DSN: dsn},
	}

	result, err := New().Plan(t.Context(), req)
	require.NoError(t, err)
	assert.False(t, result.NoChanges)
	require.Len(t, result.Changes, 1)
	require.Len(t, result.Changes[0].TableChanges, 1)
	change := result.Changes[0].TableChanges[0]
	assert.Equal(t, "public", result.Changes[0].Namespace)
	assert.Equal(t, "users", change.Table)
	assert.Contains(t, change.DDL, "CREATE TABLE public.users")
	assert.Equal(t, engine.ExecutionModeBlocked, change.ExecutionMode)
	assert.Equal(t, `statement for table "users" is a shape SchemaBot's PostgreSQL support does not execute yet; rewriting the change cannot make it eligible`, change.ModeReason)
}

// TestEngineApplyNativeSafe proves a planned metadata-only ALTER runs through
// pg-sprite's preflight and bounded optimistic executor.
func TestEngineApplyNativeSafe(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "apply_test")
	_, err := db.ExecContext(t.Context(), "CREATE TABLE public.users (id bigint PRIMARY KEY)")
	require.NoError(t, err)

	eng := New()
	result, err := eng.Apply(t.Context(), applyRequest(dsn, "users", "ALTER TABLE public.users ADD COLUMN email text"))
	require.NoError(t, err)
	assert.True(t, result.Accepted)
	progress := awaitPostgresProgress(t, eng, "users")
	assert.Equal(t, engine.StateCompleted, progress.State)
	assert.Equal(t, 100, progress.Progress)
	assert.Equal(t, "completed", progress.Metadata["phase"])
	assert.Equal(t, "1", progress.Metadata["step"])

	var exists bool
	err = db.QueryRowContext(t.Context(), `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'email')`).Scan(&exists)
	require.NoError(t, err)
	assert.True(t, exists)
}

// TestEngineApplyPrivilegeRefusal proves a role that cannot alter the target
// is blocked before execution and receives exact provisioning SQL.
func TestEngineApplyPrivilegeRefusal(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "privilege_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE ROLE limited LOGIN PASSWORD 'limited';
		GRANT CONNECT ON DATABASE privilege_test TO limited;
		GRANT USAGE ON SCHEMA public TO limited`)
	require.NoError(t, err)

	limitedDSN, err := url.Parse(dsn)
	require.NoError(t, err)
	limitedDSN.User = url.UserPassword("limited", "limited")
	eng := New()
	_, err = eng.Apply(t.Context(), applyRequest(limitedDSN.String(), "users", "ALTER TABLE public.users ADD COLUMN email text"))
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng, "users")
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.Equal(t, "refused", progress.Metadata["phase"])
	assert.False(t, progress.Retryable, "a refusal is permanent; the drive must not offer a retry")
	assert.Contains(t, progress.ErrorMessage, "GRANT")
}

// TestEngineApplyTableNotFoundRefusal proves a change targeting a table that
// does not exist on the target is a permanent refusal — retrying cannot
// succeed until the schema change is re-planned — not a retryable failure.
func TestEngineApplyTableNotFoundRefusal(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "failure_test")
	eng := New()
	_, err := eng.Apply(t.Context(), applyRequest(dsn, "missing_users", "ALTER TABLE public.missing_users ADD COLUMN email text"))
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng, "missing_users")
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.Equal(t, "refused", progress.Metadata["phase"])
	assert.False(t, progress.Retryable, "a refusal is permanent; the drive must not offer a retry")
	assert.Contains(t, progress.ErrorMessage, "missing_users")
}

// TestEngineApplyTableSizeRefusal proves the configured native-safe ceiling
// reaches preflight and permanently refuses an ALTER when the table exceeds it.
func TestEngineApplyTableSizeRefusal(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "size_limit_test")
	_, err := db.ExecContext(t.Context(), "CREATE TABLE public.users (id bigint PRIMARY KEY)")
	require.NoError(t, err)

	eng := NewWithTableSizeLimit(1)
	_, err = eng.Apply(t.Context(), applyRequest(dsn, "users", "ALTER TABLE public.users ADD COLUMN email text"))
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng, "users")
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.Equal(t, "refused", progress.Metadata["phase"])
	assert.False(t, progress.Retryable, "a size refusal is permanent until the ceiling or target changes")
	assert.Contains(t, progress.ErrorMessage, "1-byte threshold")
}

// TestEngineApplyOperationalFailure proves an execution-path error — here an
// unreachable target — remains a failed operation rather than being
// misclassified as a safety refusal.
func TestEngineApplyOperationalFailure(t *testing.T) {
	eng := New()
	unreachableDSN := "postgres://postgres:postgres@127.0.0.1:1/failure_test?sslmode=disable"
	_, err := eng.Apply(t.Context(), applyRequest(unreachableDSN, "users", "ALTER TABLE public.users ADD COLUMN email text"))
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng, "users")
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.Equal(t, "failed", progress.Metadata["phase"])
	assert.True(t, progress.Retryable, "an operational failure must reach the retryable task state, not cancel the apply's remaining work")
}

// TestEngineSharedAcrossAppliesKeepsProgressPerApply proves the engine
// answers Progress for the apply the caller identifies. One engine lives for
// a target's lifetime, so after a second apply claims it, the first apply's
// identity must read the idle sentinel — never the other apply's state — and
// Drain must leave the engine idle for the next drive's clean poll.
func TestEngineSharedAcrossAppliesKeepsProgressPerApply(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "shared_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.t_a (id bigint PRIMARY KEY);
		CREATE TABLE public.t_b (id bigint PRIMARY KEY)`)
	require.NoError(t, err)

	eng := New()
	_, err = eng.Apply(t.Context(), applyRequest(dsn, "t_a", "ALTER TABLE public.t_a ADD COLUMN a text"))
	require.NoError(t, err)
	first := awaitPostgresProgress(t, eng, "t_a")
	assert.Equal(t, engine.StateCompleted, first.State)

	// Before the second apply exists, polling its identity must not surface
	// the first apply's terminal state.
	other, err := eng.Progress(t.Context(), progressRequestFor("t_b"))
	require.NoError(t, err)
	assert.Equal(t, engine.StatePending, other.State)
	assert.Equal(t, "No active schema change", other.Message)

	_, err = eng.Apply(t.Context(), applyRequest(dsn, "t_b", "ALTER TABLE public.t_b ADD COLUMN b text"))
	require.NoError(t, err)
	second := awaitPostgresProgress(t, eng, "t_b")
	assert.Equal(t, engine.StateCompleted, second.State)
	require.Len(t, second.Tables, 1)
	assert.Equal(t, "t_b", second.Tables[0].Table)

	// The superseded apply's identity now reads idle, and a drain leaves the
	// engine idle for every identity.
	stale, err := eng.Progress(t.Context(), progressRequestFor("t_a"))
	require.NoError(t, err)
	assert.Equal(t, engine.StatePending, stale.State)
	eng.Drain()
	drained, err := eng.Progress(t.Context(), progressRequestFor("t_b"))
	require.NoError(t, err)
	assert.Equal(t, engine.StatePending, drained.State)
	assert.Equal(t, "No active schema change", drained.Message)
}

// TestEngineApplyRefusesNonNativeShape proves a statement shape the
// native-safe path cannot execute is refused synchronously at acceptance,
// before any background work starts.
func TestEngineApplyRefusesNonNativeShape(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "shape_test")
	eng := New()
	_, err := eng.Apply(t.Context(), applyRequest(dsn, "users", "CREATE TABLE public.users (id bigint PRIMARY KEY)"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not execute this statement shape yet")
}

// applyRequest builds a single-statement apply request with the same identity
// shape the drive layer uses: the task identifier stamped into
// ResumeState.MigrationContext keys the engine's progress to this apply.
func applyRequest(dsn, table, statement string) *engine.ApplyRequest {
	return &engine.ApplyRequest{
		Database: "app",
		Changes: []engine.SchemaChange{{
			Namespace:    "public",
			TableChanges: []engine.TableChange{{Table: table, DDL: statement}},
		}},
		Credentials: &engine.Credentials{DSN: dsn},
		ResumeState: &engine.ResumeState{MigrationContext: applyTaskID(table)},
	}
}

func applyTaskID(table string) string {
	return "task-" + table
}

func progressRequestFor(table string) *engine.ProgressRequest {
	return &engine.ProgressRequest{ResumeState: &engine.ResumeState{MigrationContext: applyTaskID(table)}}
}

func awaitPostgresProgress(t *testing.T, eng *Engine, table string) *engine.ProgressResult {
	t.Helper()
	var result *engine.ProgressResult
	// assert then require so the failure message formats the last polled
	// progress instead of the pre-poll nil value.
	finished := assert.Eventually(t, func() bool {
		var err error
		result, err = eng.Progress(t.Context(), progressRequestFor(table))
		require.NoError(t, err)
		return result.State.IsTerminal()
	}, postgresApplyDeadline, 10*time.Millisecond)
	require.True(t, finished, "PostgreSQL apply did not finish; last progress: %+v", result)
	return result
}
