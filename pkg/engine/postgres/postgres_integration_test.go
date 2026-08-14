//go:build integration

package postgres

import (
	"fmt"
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

// TestEnginePlanCreateTable proves the adapter derives an executable CREATE
// TABLE plan from desired state when the live PostgreSQL table is absent.
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
	assert.Empty(t, change.ExecutionMode)
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
	progress := awaitPostgresProgress(t, eng)
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
	progress := awaitPostgresProgress(t, eng)
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.Equal(t, engine.ExecutionModeBlocked, progress.Metadata["execution_mode"])
	assert.Equal(t, "insufficient-privileges", progress.Metadata["refusal_reason"])
	assert.Contains(t, progress.ErrorMessage, "GRANT")
}

// TestEngineApplyOperationalFailure proves an execution-path error remains a
// failed operation rather than being misclassified as a safety refusal.
func TestEngineApplyOperationalFailure(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "failure_test")
	eng := New()
	_, err := eng.Apply(t.Context(), applyRequest(dsn, "missing_users", "ALTER TABLE public.missing_users ADD COLUMN email text"))
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng)
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.Equal(t, "failed", progress.Metadata["phase"])
	assert.Empty(t, progress.Metadata["execution_mode"])
}

func applyRequest(dsn, table, statement string) *engine.ApplyRequest {
	return &engine.ApplyRequest{
		Database: "app",
		Changes: []engine.SchemaChange{{
			Namespace:    "public",
			TableChanges: []engine.TableChange{{Table: table, DDL: statement}},
		}},
		Credentials: &engine.Credentials{DSN: dsn},
	}
}

func awaitPostgresProgress(t *testing.T, eng *Engine) *engine.ProgressResult {
	t.Helper()
	var result *engine.ProgressResult
	require.Eventually(t, func() bool {
		var err error
		result, err = eng.Progress(t.Context(), &engine.ProgressRequest{})
		require.NoError(t, err)
		return result.State.IsTerminal()
	}, postgresApplyDeadline, 10*time.Millisecond, fmt.Sprintf("PostgreSQL apply did not finish; last progress: %+v", result))
	return result
}
