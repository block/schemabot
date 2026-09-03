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
		name          string
		err           error
		wantReason    string
		wantDetail    []string
		wantNotDetail []string
	}{
		{
			name: "privilege error is a refusal with provisioning detail",
			err: fmt.Errorf("check privileges: %w", &preflight.PrivilegeError{
				Tier:  preflight.TierAlterInPlace,
				Check: "pg_has_role(limited, app_owner, 'USAGE')",
				Grant: `GRANT "app_owner" TO "limited" WITH INHERIT TRUE`,
				Hint:  "membership must be inheritable",
			}),
			wantReason: "insufficient-privileges",
			wantDetail: []string{
				"in-place ALTER TABLE",
				`table "users"`,
				`GRANT "app_owner" TO "limited" WITH INHERIT TRUE`,
				"pg_has_role(limited, app_owner, 'USAGE')",
				"membership must be inheritable",
			},
		},
		{
			name: "database-sourced identifiers are sanitized for Markdown",
			err: fmt.Errorf("check privileges: %w", &preflight.PrivilegeError{
				Tier:  preflight.TierAlterInPlace,
				Check: "pg_has_role(evil\nrole, app|owner, 'USAGE')",
				Grant: "GRANT \"app|owner\" TO \"evil\nrole\"",
				Hint:  "membership\tmust be inheritable",
			}),
			wantReason: "insufficient-privileges",
			wantDetail: []string{
				`GRANT "app/owner" TO "evil role"`,
				"pg_has_role(evil role, app/owner, 'USAGE')",
				"membership must be inheritable",
			},
		},
		{
			name: "create-tier privilege refusal names the schema, not the absent table",
			err: fmt.Errorf("check creation access: %w", &preflight.PrivilegeError{
				Tier:  preflight.TierCreateTable,
				Check: "has_schema_privilege(limited, 'public', 'CREATE')",
				Grant: `GRANT CREATE ON SCHEMA "public" TO "limited"`,
			}),
			wantReason: "insufficient-privileges",
			wantDetail: []string{
				`in the schema that would hold table "users"`,
				`GRANT CREATE ON SCHEMA "public" TO "limited"`,
			},
			wantNotDetail: []string{`on table "users"`},
		},
		{
			name:       "IF NOT EXISTS create is a refusal that names the clause",
			err:        fmt.Errorf("execute PostgreSQL CREATE TABLE %q: %w", "users", executor.ErrIfNotExistsUnsupported),
			wantReason: "unsupported-create-step",
			wantDetail: []string{"IF NOT EXISTS", "drop the clause"},
		},
		{
			name:       "create collision is a refusal whoever took the name first",
			err:        fmt.Errorf("execute: %w", executor.ErrCreateCollision),
			wantReason: "create-collision",
			wantDetail: []string{"already taken on the target", "re-plan"},
		},
		{
			name:       "invariant violation fails closed as a refusal",
			err:        fmt.Errorf("execute: %w", executor.ErrInvariantViolation),
			wantReason: "engine-invariant-violation",
			wantDetail: []string{"inspect the target and server logs"},
		},
		{
			name: "external cancellation is operational",
			err:  fmt.Errorf("execute: %w", executor.ErrCancelledExternally),
		},
		{
			name: "partitioned-parent admission refusal renders the typed sentence",
			err: fmt.Errorf("admit statement for partitioned PostgreSQL table %q: %w", "users",
				&preflight.UnsupportedPartitionedParentError{Cause: preflight.PartitionCauseConcurrentIndexBuild}),
			wantReason: "unsupported-partitioned-parent",
			wantDetail: []string{"cannot build parent-level indexes concurrently"},
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
			name: "invalid-index verdict is operational even when it wraps a statement-budget cause",
			err: fmt.Errorf("execute: %w", &executor.InvalidIndexError{
				Schema:  "public",
				Index:   "big_ref_idx",
				Build:   &executor.BudgetError{Cause: executor.CauseStatement, Budget: time.Second},
				Cleanup: executor.ErrBuildLeftInvalidIndex,
			}),
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
			for _, want := range tt.wantDetail {
				assert.Contains(t, r.detail, want)
			}
			for _, notWant := range tt.wantNotDetail {
				assert.NotContains(t, r.detail, notWant)
			}
		})
	}
}

// TestRefusalForOutcomeTotalOverExecutorCodes pins the classifier to
// pg-sprite's full outcome vocabulary: every code the executor can return
// maps to an explicit disposition — refusal or operational — so a code added
// upstream fails this test instead of silently draining into the generic
// retryable tail.
func TestRefusalForOutcomeTotalOverExecutorCodes(t *testing.T) {
	for _, code := range executor.Codes() {
		t.Run(string(code), func(t *testing.T) {
			_, known := refusalForOutcome(code, "users")
			assert.True(t, known, "outcome code %q has no explicit apply disposition", code)
		})
	}
}

// TestConcurrentIndexBudgetLeavesHeadroomUnderApplyCeiling pins the gap the
// two bounds depend on: the server-side index budget must expire with at
// least the named headroom to spare before the client-side ceiling cancels
// the session, so an exhausted build surfaces as the typed budget verdict —
// and its invalid-index catalog check still gets to run inside the ceiling —
// rather than as an ambiguous external cancellation. Retuning either bound
// without keeping the headroom fails here instead of in an apply.
func TestConcurrentIndexBudgetLeavesHeadroomUnderApplyCeiling(t *testing.T) {
	// A zero headroom would let the two bounds coincide and race, so the
	// strict ordering is pinned on its own as well as through the gap.
	require.Positive(t, concurrentIndexHeadroom)
	assert.Less(t, concurrentIndexBudget, optimisticApplyCeiling)
	assert.GreaterOrEqual(t, optimisticApplyCeiling-concurrentIndexBudget, concurrentIndexHeadroom)
}

// TestRetryPathFitsUnderApplyCeiling pins the other execution path against
// the same ceiling: every attempt the default retry policy allows, each at
// its full statement limit, plus the longest backoff between them, must
// finish before the ceiling cancels the session — otherwise a lock-contended
// native statement would surface as an external cancellation instead of the
// typed budget verdict. The policy comes from pg-sprite, so a dependency
// bump that widens it fails here instead of in an apply.
func TestRetryPathFitsUnderApplyCeiling(t *testing.T) {
	policy := executor.DefaultRetryPolicy()
	require.Positive(t, policy.MaxAttempts)
	attempts := time.Duration(policy.MaxAttempts)
	worstCase := attempts*optimisticStatementLimit + (attempts-1)*policy.MaxBackoff
	assert.Less(t, worstCase, optimisticApplyCeiling)
}

// TestInvalidIndexDetailMatchesVerdictOwnership pins the advice ladder to
// the verdict code: a drop is named only for the build's own proven
// leftover; a pre-existing entry gets an in-progress-build check; an
// unproven verdict gets catalog inspection because the index may be healthy.
// Every branch names the index and none renders the wrapped build or
// cleanup errors, which may carry raw server text.
func TestInvalidIndexDetailMatchesVerdictOwnership(t *testing.T) {
	rawServerText := errors.New("ERROR: deadline exceeded at host db-internal-1.example.com")
	tests := []struct {
		name          string
		err           *executor.InvalidIndexError
		wantDetail    []string
		wantNotDetail []string
	}{
		{
			name: "own leftover names the drop",
			err: &executor.InvalidIndexError{Schema: "public", Index: "big_ref_idx",
				Build: rawServerText, Cleanup: executor.ErrBuildLeftInvalidIndex},
			wantDetail:    []string{`"public"."big_ref_idx"`, "drop the invalid index", "retry"},
			wantNotDetail: []string{"db-internal-1"},
		},
		{
			name: "pre-existing entry gets an in-progress-build check, never a drop",
			err: &executor.InvalidIndexError{Schema: "public", Index: "big_ref_idx",
				Build: rawServerText, Cleanup: executor.ErrPreexistingInvalidIndex},
			wantDetail:    []string{`"public"."big_ref_idx"`, "another actor's build", "pg_stat_activity"},
			wantNotDetail: []string{"drop the invalid index", "db-internal-1"},
		},
		{
			name: "unproven verdict gets catalog inspection, never a drop",
			err: &executor.InvalidIndexError{Schema: "public", Index: "big_ref_idx",
				Build: rawServerText, Cleanup: rawServerText},
			wantDetail:    []string{`"public"."big_ref_idx"`, "pg_index.indisvalid"},
			wantNotDetail: []string{"drop the invalid index", "db-internal-1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			detail := invalidIndexDetail(tt.err)
			for _, want := range tt.wantDetail {
				assert.Contains(t, detail, want)
			}
			for _, notWant := range tt.wantNotDetail {
				assert.NotContains(t, detail, notWant)
			}
		})
	}
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
				Table: "users", DDL: "DROP TABLE public.users",
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

	err := executeOptimistic(t.Context(), conn, nativeApply{namespace: "public", table: "widgets", sql: "CREATE TABLE widgets (id bigint PRIMARY KEY)"}, DefaultNativeSafeTableSizeLimitBytes)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "open pg-sprite apply pool")
	assert.Contains(t, err.Error(), "read CA bundle")
}
