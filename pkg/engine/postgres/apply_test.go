package postgres

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
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
			wantDetail: []string{"(table, index, constraint, or sequence)", createCollisionRemedy},
		},
		{
			name: "create collision identifies the failed sequence step",
			err: fmt.Errorf("execute: %w", &executor.SequenceStepError{
				Step: 2, Total: 3, Err: executor.ErrCreateCollision,
			}),
			wantReason:    "create-collision",
			wantDetail:    []string{"step 2 of 3 failed", `CREATE TABLE for "users" committed`, createCollisionRemedy},
			wantNotDetail: []string{replanRemedy},
		},
		{
			name: "single-statement refusal omits sequence position",
			err: fmt.Errorf("execute: %w", &executor.SequenceStepError{
				Step: 1, Total: 1, Err: executor.ErrCreateCollision,
			}),
			wantReason:    "create-collision",
			wantDetail:    []string{"already occupied"},
			wantNotDetail: []string{"step 1 of 1"},
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
			if strings.Contains(r.detail, "CREATE TABLE") {
				assert.LessOrEqual(t, strings.Count(r.detail, "re-plan"), 1)
			}
		})
	}
}

func TestCreateCollisionRefusalEntryPathsMatch(t *testing.T) {
	preflightRefusal := classifyRefusal(fmt.Errorf("preflight: %w", preflight.ErrRelationExists), "users")
	executorRefusal := classifyRefusal(fmt.Errorf("execute: %w", executor.ErrCreateCollision), "users")

	require.NotNil(t, preflightRefusal)
	require.NotNil(t, executorRefusal)
	assert.Equal(t, "create-collision", preflightRefusal.reason)
	assert.Equal(t, preflightRefusal.detail, executorRefusal.detail)
	assert.Equal(t, `a name the create set for "users" needs is already occupied (table, index, constraint, or sequence); re-plan, and if it recurs drop or rename the occupant or give the constraint, index, or sequence another name`, preflightRefusal.detail)
}

// statusReasonColumnWidth is the narrowest operator surface a refusal detail
// is rendered on: the CLI status table clamps its failure-reason column and
// truncates from the tail, which is where the remedy sits. A create-collision
// detail for any realistic table name must fit so the remedy survives.
const statusReasonColumnWidth = 240

func TestCreateCollisionRefusalFitsStatusReasonColumn(t *testing.T) {
	table := strings.Repeat("a", 30)

	r := classifyRefusal(fmt.Errorf("preflight: %w", preflight.ErrRelationExists), table)

	require.NotNil(t, r)
	assert.LessOrEqual(t, len(r.detail), statusReasonColumnWidth, r.detail)
	assert.True(t, strings.HasSuffix(r.detail, createCollisionRemedy), r.detail)
}

func TestCreateCollisionRefusalAfterCommittedCreateStep(t *testing.T) {
	err := &executor.SequenceStepError{Step: 2, Total: 3, Err: executor.ErrCreateCollision}

	r := classifyRefusal(err, "users")

	require.NotNil(t, r)
	assert.Equal(t, `a name the create set for "users" needs is already occupied (table, index, constraint, or sequence); step 2 of 3 failed after the CREATE TABLE for "users" committed; re-plan, and if it recurs drop or rename the occupant or give the constraint, index, or sequence another name`, r.detail)
	assert.NotContains(t, r.detail, replanRemedy)
	assert.Equal(t, 1, strings.Count(r.detail, createCollisionRemedy))
}

// TestRefusalWithoutRemedyAtFirstStepGetsNoReplan pins the boundary of the
// re-plan fallback: a first-step refusal committed nothing, so the plan still
// matches the target and there is nothing to re-plan. The detail names the
// step and stops.
func TestRefusalWithoutRemedyAtFirstStepGetsNoReplan(t *testing.T) {
	err := fmt.Errorf("execute: %w", &executor.SequenceStepError{Step: 1, Total: 3,
		Err: &executor.BudgetError{Cause: executor.CauseStatement, Budget: time.Second}})

	r := classifyRefusal(err, "users")

	require.NotNil(t, r)
	assert.Equal(t, "not-native-safe-budget-exceeded", r.reason)
	assert.True(t, strings.HasSuffix(r.detail, "; step 1 of 3 failed"), r.detail)
	assert.NotContains(t, r.detail, "re-plan")
}

// TestTableNotFoundRefusalCarriesItsOwnReplan reads the missing-table refusal
// at the first step, where the fallback cannot contribute a re-plan, so the
// re-plan in the detail is provably the refusal's own remedy.
func TestTableNotFoundRefusalCarriesItsOwnReplan(t *testing.T) {
	err := fmt.Errorf("execute: %w", &executor.SequenceStepError{Step: 1, Total: 3, Err: preflight.ErrTableNotFound})

	r := classifyRefusal(err, "users")

	require.NotNil(t, r)
	assert.Equal(t, "table-not-found", r.reason)
	assert.Equal(t, `table "users" does not exist on the target; step 1 of 3 failed; `+replanRemedy, r.detail)
}

// TestRefusalAfterCommittedCreateStepKeepsOwnRemedy pins the shape of every
// refusal that fails past the first step of a create set: the cause, then
// the committed CREATE TABLE, then exactly one remedy — the refusal's own when
// it carries one, a re-plan only when it does not. An operator reading the
// last clause never sees two competing instructions.
func TestRefusalAfterCommittedCreateStepKeepsOwnRemedy(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantReason string
		wantRemedy string
	}{
		{
			name:       "invariant violation keeps its inspection remedy",
			err:        executor.ErrInvariantViolation,
			wantReason: "engine-invariant-violation",
			wantRemedy: "inspect the target and server logs before re-running",
		},
		{
			name:       "IF NOT EXISTS keeps its drop-the-clause remedy",
			err:        executor.ErrIfNotExistsUnsupported,
			wantReason: "unsupported-create-step",
			wantRemedy: "drop the clause and re-plan",
		},
		{
			name:       "duplicate create name keeps its fix-the-file remedy",
			err:        executor.ErrDuplicateCreateName,
			wantReason: "duplicate-create-name",
			wantRemedy: "fix the schema file and re-plan",
		},
		{
			name:       "missing schema keeps its create-the-schema remedy",
			err:        preflight.ErrSchemaNotFound,
			wantReason: "schema-not-found",
			wantRemedy: "create the schema first",
		},
		{
			name:       "missing table states its re-plan once",
			err:        preflight.ErrTableNotFound,
			wantReason: "table-not-found",
			wantRemedy: replanRemedy,
		},
		{
			name:       "privilege refusal keeps its provisioning remedy",
			err:        &preflight.PrivilegeError{Tier: preflight.TierCreateTable, Check: "has_schema_privilege(limited, 'public', 'CREATE')", Grant: `GRANT CREATE ON SCHEMA "public" TO "limited"`},
			wantReason: "insufficient-privileges",
			wantRemedy: `provision with: GRANT CREATE ON SCHEMA "public" TO "limited" (verified by: has_schema_privilege(limited, 'public', 'CREATE'))`,
		},
		{
			name:       "refusal without a remedy gets a re-plan",
			err:        &executor.BudgetError{Cause: executor.CauseStatement, Budget: time.Second},
			wantReason: "not-native-safe-budget-exceeded",
			wantRemedy: replanRemedy,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := fmt.Errorf("execute: %w", &executor.SequenceStepError{Step: 2, Total: 3, Err: tt.err})

			r := classifyRefusal(err, "users")

			require.NotNil(t, r)
			assert.Equal(t, tt.wantReason, r.reason)
			step := `; step 2 of 3 failed after the CREATE TABLE for "users" committed; `
			assert.True(t, strings.HasSuffix(r.detail, step+tt.wantRemedy), r.detail)
			assert.Equal(t, 1, strings.Count(r.detail, tt.wantRemedy), r.detail)
			assert.Equal(t, 1, strings.Count(r.detail, "; step 2 of 3 failed"), r.detail)
		})
	}
}

// TestRefusalAtFirstSequenceStepPlacesRemedyLast pins that a first-step
// failure of a create set names the step without claiming a committed CREATE
// TABLE, and still ends on the refusal's own remedy.
func TestRefusalAtFirstSequenceStepPlacesRemedyLast(t *testing.T) {
	err := fmt.Errorf("execute: %w", &executor.SequenceStepError{Step: 1, Total: 3, Err: executor.ErrIfNotExistsUnsupported})

	r := classifyRefusal(err, "users")

	require.NotNil(t, r)
	assert.Equal(t, `the planned statement for "users" carries IF NOT EXISTS, whose no-op outcome the native-safe path cannot prove; step 1 of 3 failed; drop the clause and re-plan`, r.detail)
	assert.NotContains(t, r.detail, "committed")
}

// TestCommittedCreatePrefixDetail pins the retry boundary for create sets: a
// first-step failure has changed nothing and may be retried, while any later
// failure leaves the CREATE TABLE committed and requires a fresh plan.
func TestCommittedCreatePrefixDetail(t *testing.T) {
	first := &executor.SequenceStepError{Step: 1, Total: 3, Err: errors.New("server failure")}
	detail, committed := committedCreatePrefixDetail(first, "users")
	assert.False(t, committed)
	assert.Empty(t, detail)

	later := &executor.SequenceStepError{Step: 2, Total: 3, Err: errors.New("server failure")}
	detail, committed = committedCreatePrefixDetail(later, "users")
	assert.True(t, committed)
	assert.Equal(t, `step 2 of 3 failed after the CREATE TABLE for "users" committed; re-plan against the current schema`, detail)
}

func TestClassifyApplyFailureCommittedCreatePrefix(t *testing.T) {
	err := &executor.SequenceStepError{Step: 2, Total: 3, Err: errors.New("server failure")}

	failure := classifyApplyFailure(err, "users")

	assert.False(t, failure.retryable)
	assert.True(t, failure.committedPrefix)
	assert.Contains(t, failure.detail, `CREATE TABLE for "users" committed`)
	assert.Contains(t, failure.detail, "re-plan against the current schema")
	assert.Equal(t, 1, strings.Count(failure.detail, "re-plan"))
}

func TestProgressResultReportsCreateSequenceLength(t *testing.T) {
	result := progressResult(engine.StateRunning, "running", time.Now(), nativeApply{
		namespace: "public",
		table:     "widgets",
		sql:       "CREATE TABLE public.widgets (id bigint PRIMARY KEY)",
		steps:     3,
	}, "")

	assert.Equal(t, "1", result.Metadata["step"])
	assert.Equal(t, "3", result.Metadata["steps_total"])
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

// TestConcurrentAppliesEachAnswerForTheirOwnWork proves accepting a second
// apply on the same target leaves the first one's progress intact. One engine
// serves a target for its whole lifetime, and a running apply's driver reading
// pending would take it as evidence its work was lost and settle the apply
// against the target schema while the statement is still executing.
func TestConcurrentAppliesEachAnswerForTheirOwnWork(t *testing.T) {
	eng := New()
	changeA := nativeApply{namespace: "public", table: "t_a", sql: "ALTER TABLE public.t_a ADD COLUMN a text"}
	changeB := nativeApply{namespace: "public", table: "t_b", sql: "ALTER TABLE public.t_b ADD COLUMN b text"}
	eng.claimProgress("task-a", progressResult(engine.StateRunning, "preflight", time.Now(), changeA, ""))
	eng.claimProgress("task-b", progressResult(engine.StateRunning, "preflight", time.Now(), changeB, ""))

	first, err := eng.Progress(t.Context(), &engine.ProgressRequest{
		ResumeState: &engine.ResumeState{MigrationContext: "task-a"},
	})
	require.NoError(t, err)
	assert.Equal(t, engine.StateRunning, first.State)
	require.Len(t, first.Tables, 1)
	assert.Equal(t, "t_a", first.Tables[0].Table)

	second, err := eng.Progress(t.Context(), &engine.ProgressRequest{
		ResumeState: &engine.ResumeState{MigrationContext: "task-b"},
	})
	require.NoError(t, err)
	assert.Equal(t, engine.StateRunning, second.State)
	require.Len(t, second.Tables, 1)
	assert.Equal(t, "t_b", second.Tables[0].Table)
}

// TestClaimProgressRetiresSettledApplies proves accepting an apply retires the
// entries that already reached a terminal state, so a long-lived engine does
// not accumulate one entry per apply it has ever served, while entries for
// applies that are still running survive untouched.
func TestClaimProgressRetiresSettledApplies(t *testing.T) {
	eng := New()
	settled := nativeApply{namespace: "public", table: "t_settled", sql: "ALTER TABLE public.t_settled ADD COLUMN a text"}
	running := nativeApply{namespace: "public", table: "t_running", sql: "ALTER TABLE public.t_running ADD COLUMN b text"}
	fresh := nativeApply{namespace: "public", table: "t_fresh", sql: "ALTER TABLE public.t_fresh ADD COLUMN c text"}
	eng.claimProgress("task-settled", progressResult(engine.StateCompleted, "completed", time.Now(), settled, ""))
	eng.claimProgress("task-running", progressResult(engine.StateRunning, "preflight", time.Now(), running, ""))

	eng.claimProgress("task-fresh", progressResult(engine.StateRunning, "preflight", time.Now(), fresh, ""))

	retired, err := eng.Progress(t.Context(), &engine.ProgressRequest{
		ResumeState: &engine.ResumeState{MigrationContext: "task-settled"},
	})
	require.NoError(t, err)
	assert.Equal(t, engine.StatePending, retired.State)
	assert.Equal(t, "No active schema change", retired.Message)

	survivor, err := eng.Progress(t.Context(), &engine.ProgressRequest{
		ResumeState: &engine.ResumeState{MigrationContext: "task-running"},
	})
	require.NoError(t, err)
	assert.Equal(t, engine.StateRunning, survivor.State)
	require.Len(t, survivor.Tables, 1)
	assert.Equal(t, "t_running", survivor.Tables[0].Table)
}

// TestUntrackedApplyProgressIsDiscarded proves a background writer whose apply
// the engine has stopped tracking cannot resurrect an entry or disturb the
// applies still being tracked. Drain is what stops tracking a running apply,
// and it means the drive that accepted the work has given it up.
func TestUntrackedApplyProgressIsDiscarded(t *testing.T) {
	eng := New()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	changeB := nativeApply{namespace: "public", table: "t_b", sql: "ALTER TABLE public.t_b ADD COLUMN b text"}
	eng.claimProgress("task-b", progressResult(engine.StateRunning, "preflight", time.Now(), changeB, ""))

	changeA := nativeApply{namespace: "public", table: "t_a", sql: "ALTER TABLE public.t_a ADD COLUMN a text"}
	eng.publishProgress("task-a", progressResult(engine.StateCompleted, "completed", time.Now(), changeA, ""), logger)

	discarded, err := eng.Progress(t.Context(), &engine.ProgressRequest{
		ResumeState: &engine.ResumeState{MigrationContext: "task-a"},
	})
	require.NoError(t, err)
	assert.Equal(t, engine.StatePending, discarded.State)
	assert.Equal(t, "No active schema change", discarded.Message)

	tracked, err := eng.Progress(t.Context(), &engine.ProgressRequest{
		ResumeState: &engine.ResumeState{MigrationContext: "task-b"},
	})
	require.NoError(t, err)
	assert.Equal(t, engine.StateRunning, tracked.State)
	require.Len(t, tracked.Tables, 1)
	assert.Equal(t, "t_b", tracked.Tables[0].Table)
}

// TestDrainStopsTrackingEverySchemaChange proves a drain leaves the engine
// idle for every apply it was serving: resume paths drain precisely so the next
// poll reads the idle sentinel instead of a previous change's snapshot.
func TestDrainStopsTrackingEverySchemaChange(t *testing.T) {
	eng := New()
	changeA := nativeApply{namespace: "public", table: "t_a", sql: "ALTER TABLE public.t_a ADD COLUMN a text"}
	changeB := nativeApply{namespace: "public", table: "t_b", sql: "ALTER TABLE public.t_b ADD COLUMN b text"}
	eng.claimProgress("task-a", progressResult(engine.StateCompleted, "completed", time.Now(), changeA, ""))
	eng.claimProgress("task-b", progressResult(engine.StateRunning, "preflight", time.Now(), changeB, ""))

	eng.Drain()

	for _, key := range []string{"task-a", "task-b"} {
		progress, err := eng.Progress(t.Context(), &engine.ProgressRequest{
			ResumeState: &engine.ResumeState{MigrationContext: key},
		})
		require.NoError(t, err)
		assert.Equal(t, engine.StatePending, progress.State, "apply %q must read the idle sentinel after a drain", key)
		assert.Equal(t, "No active schema change", progress.Message)
	}
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

func TestValidateOptimisticApplyAcceptsCreateSet(t *testing.T) {
	req := &engine.ApplyRequest{
		Database: "app",
		Changes: []engine.SchemaChange{{Namespace: "public", TableChanges: []engine.TableChange{{
			Table: "widgets",
			DDL:   "CREATE TABLE public.widgets (id bigint PRIMARY KEY, name text);\nCREATE UNIQUE INDEX widgets_name_key ON public.widgets (name)",
		}}}},
		Credentials: &engine.Credentials{DSN: "postgres://localhost/app"},
	}

	change, err := validateOptimisticApply(req)
	require.NoError(t, err)
	assert.Equal(t, req.Changes[0].TableChanges[0].DDL, change.sql)
}

func TestValidateOptimisticApplyRefusesMixedCreateScript(t *testing.T) {
	req := &engine.ApplyRequest{
		Database: "app",
		Changes: []engine.SchemaChange{{Namespace: "public", TableChanges: []engine.TableChange{{
			Table: "widgets",
			DDL:   "CREATE TABLE public.widgets (id bigint PRIMARY KEY);\nALTER TABLE public.widgets ADD COLUMN name text",
		}}}},
		Credentials: &engine.Credentials{DSN: "postgres://localhost/app"},
	}

	_, err := validateOptimisticApply(req)
	require.Error(t, err)
	assert.Equal(t, `apply PostgreSQL table "widgets": planned DDL is not one statement or a valid greenfield create set`, err.Error())
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
