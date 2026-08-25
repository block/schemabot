package postgres

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/preflight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// TestClassifyRefusal pins the boundary between permanent refusals (retrying
// cannot succeed until the plan, target, or provisioning changes) and
// operational failures (retry may succeed). Lock-budget exhaustion sits on
// the operational side: the statement is native-safe and only lost a bounded
// race with concurrent lock holders.
func TestClassifyRefusal(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantReason string
	}{
		{
			name:       "privilege error is a refusal with provisioning detail",
			err:        fmt.Errorf("check privileges: %w", &preflight.PrivilegeError{Grant: "GRANT ALTER ON users TO app"}),
			wantReason: "insufficient-privileges",
		},
		{
			name:       "statement budget exhaustion is a refusal",
			err:        fmt.Errorf("execute: %w", &executor.BudgetError{Cause: executor.CauseStatement, Budget: time.Second}),
			wantReason: "not-native-safe-budget-exceeded",
		},
		{
			name: "lock budget exhaustion is operational",
			err:  fmt.Errorf("execute: %w", &executor.BudgetError{Cause: executor.CauseLock, Budget: time.Second}),
		},
		{
			name:       "oversized table is a refusal",
			err:        fmt.Errorf("preflight: %w", &preflight.SizeError{TotalBytes: 2, LimitBytes: 1}),
			wantReason: "table-too-large",
		},
		{
			name:       "missing table is a refusal",
			err:        fmt.Errorf("preflight: %w", preflight.ErrTableNotFound),
			wantReason: "table-not-found",
		},
		{
			name:       "non-table relation is a refusal",
			err:        fmt.Errorf("preflight: %w", preflight.ErrNotTable),
			wantReason: "not-a-table",
		},
		{
			name: "untyped error is operational",
			err:  errors.New("dial tcp: connection refused"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := classifyRefusal(tt.err, "users")
			if tt.wantReason == "" {
				assert.Nil(t, r)
				return
			}
			require.NotNil(t, r)
			assert.Equal(t, tt.wantReason, r.reason)
			assert.NotEmpty(t, r.detail)
		})
	}
}

func TestConfiguredTableSizeLimitIsReportedInRefusal(t *testing.T) {
	const limit = int64(4 << 30)
	eng := NewWithTableSizeLimit(limit)
	assert.Equal(t, limit, eng.tableSizeLimit)

	r := classifyRefusal(&preflight.SizeError{TotalBytes: limit + 1, LimitBytes: eng.tableSizeLimit}, "users")
	require.NotNil(t, r)
	assert.Equal(t, "table-too-large", r.reason)
	assert.Contains(t, r.detail, "4294967296-byte threshold")
}

// TestProgressIsKeyedToTheRequestingApply proves the engine answers Progress
// for the apply the caller identifies, not for whichever apply wrote last:
// one engine is shared for a target's lifetime, so a mismatched identity must
// read the idle sentinel instead of another schema change's state.
func TestProgressIsKeyedToTheRequestingApply(t *testing.T) {
	eng := New()
	change := nativeApply{namespace: "public", table: "t_a", sql: "ALTER TABLE public.t_a ADD COLUMN a text"}
	eng.claimProgress("task-a", progressResult(engine.StateCompleted, "completed", time.Now(), change, ""))

	tracked, err := eng.Progress(t.Context(), &engine.ProgressRequest{
		ResumeState: &engine.ResumeState{MigrationContext: "task-a"},
	})
	require.NoError(t, err)
	assert.Equal(t, engine.StateCompleted, tracked.State)
	require.Len(t, tracked.Tables, 1)
	assert.Equal(t, "t_a", tracked.Tables[0].Table)

	other, err := eng.Progress(t.Context(), &engine.ProgressRequest{
		ResumeState: &engine.ResumeState{MigrationContext: "task-b"},
	})
	require.NoError(t, err)
	assert.Equal(t, engine.StatePending, other.State)
	assert.Equal(t, "No active schema change", other.Message)
	assert.False(t, other.State.IsTerminal(), "another apply's identity must never read a terminal state")

	anonymous, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
	require.NoError(t, err)
	assert.Equal(t, engine.StatePending, anonymous.State)
}

// TestStaleApplyCannotOverwriteTrackedProgress proves a background writer
// from a superseded apply cannot replace the tracked apply's state: once a
// newer apply claims the engine, the stale terminal write is discarded.
func TestStaleApplyCannotOverwriteTrackedProgress(t *testing.T) {
	eng := New()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	changeB := nativeApply{namespace: "public", table: "t_b", sql: "ALTER TABLE public.t_b ADD COLUMN b text"}
	eng.claimProgress("task-b", progressResult(engine.StateRunning, "preflight", time.Now(), changeB, ""))

	changeA := nativeApply{namespace: "public", table: "t_a", sql: "ALTER TABLE public.t_a ADD COLUMN a text"}
	eng.publishProgress("task-a", progressResult(engine.StateCompleted, "completed", time.Now(), changeA, ""), logger)

	tracked, err := eng.Progress(t.Context(), &engine.ProgressRequest{
		ResumeState: &engine.ResumeState{MigrationContext: "task-b"},
	})
	require.NoError(t, err)
	assert.Equal(t, engine.StateRunning, tracked.State)
	require.Len(t, tracked.Tables, 1)
	assert.Equal(t, "t_b", tracked.Tables[0].Table)
}

// TestDrainClearsTrackedSchemaChange proves a drain leaves the engine idle:
// resume paths drain precisely so the next poll reads the idle sentinel
// instead of the previous change's terminal snapshot.
func TestDrainClearsTrackedSchemaChange(t *testing.T) {
	eng := New()
	change := nativeApply{namespace: "public", table: "t_a", sql: "ALTER TABLE public.t_a ADD COLUMN a text"}
	eng.claimProgress("task-a", progressResult(engine.StateCompleted, "completed", time.Now(), change, ""))

	eng.Drain()

	progress, err := eng.Progress(t.Context(), &engine.ProgressRequest{
		ResumeState: &engine.ResumeState{MigrationContext: "task-a"},
	})
	require.NoError(t, err)
	assert.Equal(t, engine.StatePending, progress.State)
	assert.Equal(t, "No active schema change", progress.Message)
}

// TestValidateOptimisticApplyRefusesNonNativeShape proves acceptance-time
// validation refuses statement shapes the native-safe path cannot execute,
// without touching the target database.
func TestValidateOptimisticApplyRefusesNonNativeShape(t *testing.T) {
	req := &engine.ApplyRequest{
		Database: "app",
		Changes: []engine.SchemaChange{{
			Namespace: "public",
			TableChanges: []engine.TableChange{{
				Table: "users", DDL: "CREATE TABLE public.users (id bigint PRIMARY KEY)",
			}},
		}},
		Credentials: &engine.Credentials{DSN: "postgres://localhost/app"},
	}

	_, err := validateOptimisticApply(req)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not execute this statement shape yet")
}

// The apply pool inherits the CA bundle the acceptance path resolved; a
// bundle that disappears between acceptance and execution fails the pool
// build closed, before any statement is attempted.
func TestExecuteOptimisticRefusesUnreadableCABundle(t *testing.T) {
	conn := targetConn{
		dsn:        "postgres://schemabot:secret@localhost:5432/app?sslmode=verify-full",
		caCertPath: filepath.Join(t.TempDir(), "missing.pem"),
	}

	err := executeOptimistic(t.Context(), conn, nativeApply{namespace: "public", table: "widgets", sql: "CREATE TABLE widgets (id bigint PRIMARY KEY)"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "open pg-sprite apply pool")
	assert.Contains(t, err.Error(), "read CA bundle")
}
