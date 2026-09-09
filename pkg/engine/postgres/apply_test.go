package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/block/pg-sprite/pkg/dbconn"
	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/preflight"
	"github.com/block/pg-sprite/pkg/progress"
	"github.com/jackc/pgx/v5"
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
			wantDetail: []string{"(table, view, index, or sequence)", createCollisionRemedy},
		},
		{
			name: "create collision identifies the failed sequence step",
			err: fmt.Errorf("execute: %w", &executor.SequenceStepError{
				Step: 2, Total: 3, Err: executor.ErrCreateCollision,
			}),
			wantReason:    "create-collision",
			wantDetail:    []string{`for "users"`, "step 2 of 3 failed after the CREATE TABLE committed", createCollisionRemedy},
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
			name: "the caller's own cancellation is operational",
			err:  fmt.Errorf("execute: %w", executor.ErrCancelledByCaller),
		},
		{
			name: "invalid index on another table is a refusal that renders the typed advice",
			err: fmt.Errorf("execute: %w", &executor.InvalidIndexError{
				Schema:  "public",
				Index:   "users_ref_idx",
				Table:   "shipments",
				Cleanup: executor.ErrInvalidIndexOnOtherTable,
			}),
			wantReason:    "invalid-index-occupied",
			wantDetail:    []string{`"public"."users_ref_idx"`, `"shipments"`, "re-plan"},
			wantNotDetail: []string{"retry removes it"},
		},
		{
			name: "non-droppable invalid index is a refusal even when it wraps a statement-budget cause",
			err: fmt.Errorf("execute: %w", &executor.InvalidIndexError{
				Schema:  "public",
				Index:   "users_pkey",
				Table:   "users",
				Build:   &executor.BudgetError{Cause: executor.CauseStatement, Budget: time.Second},
				Cleanup: executor.ErrInvalidIndexNotDroppable,
			}),
			wantReason:    "invalid-index-occupied",
			wantDetail:    []string{`"public"."users_pkey"`, "constraint's index", "operator must resolve"},
			wantNotDetail: []string{"budget", "retry removes it"},
		},
		{
			name: "abandoned invalid index is operational",
			err: fmt.Errorf("execute: %w", &executor.InvalidIndexError{
				Schema: "public", Index: "users_ref_idx", Table: "users",
				Cleanup: executor.ErrAbandonedInvalidIndex,
			}),
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
			name: "concurrent build that ran past its bound and left nothing is a refusal naming the option",
			err: fmt.Errorf("build PostgreSQL index concurrently on table %q: %w", "users",
				nameConcurrentIndexBound(&executor.BudgetError{Cause: executor.CauseStatement, Budget: 36 * time.Hour}, 36*time.Hour)),
			wantReason:    "concurrent-index-bound-exceeded",
			wantDetail:    []string{"ran past postgres.concurrent_index_max_duration (36h0m0s)", "raise postgres.concurrent_index_max_duration and re-run"},
			wantNotDetail: []string{"statement budget", replanRemedy},
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
			name: "invalid-index verdict stays operational when the bound wrapper names it",
			err: fmt.Errorf("build PostgreSQL index concurrently on table %q: %w", "users",
				nameConcurrentIndexBound(&executor.InvalidIndexError{
					Schema:  "public",
					Index:   "big_ref_idx",
					Build:   &executor.BudgetError{Cause: executor.CauseStatement, Budget: 36 * time.Hour},
					Cleanup: executor.ErrBuildLeftInvalidIndex,
				}, 36*time.Hour)),
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
			name:       "pool too small for the build's sessions is a refusal that names the pool",
			err:        fmt.Errorf("admit concurrent build: %w", executor.ErrPoolTooSmall),
			wantReason: "pool-too-small",
			wantDetail: []string{"connection pool", `"users"`, "raise the pool size"},
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
	assert.Equal(t, `a name the create set for "users" needs is already occupied (table, view, index, or sequence); re-plan, and if it recurs drop or rename the occupant or give the constraint, index, or sequence another name`, preflightRefusal.detail)
}

// The CLI status listing is the narrowest operator surface a refusal detail
// is rendered on: it clamps the failure reason to statusReasonColumnWidth
// bytes and truncates from the tail, where the remedy sits. The full
// create-collision remedy cannot fit there for every shape — a legal table
// name alone runs to maxIdentifierLength bytes — so what the composition
// guarantees, and what these tests pin, is narrower: the remedy leads with
// the re-plan that resolves a lost race on its own, and that lead lands
// inside the clamp for a table name of any legal length, whether the
// collision was the single statement or a step past the committed CREATE
// TABLE. A realistic single-statement detail still fits whole.
const (
	statusReasonColumnWidth = 240
	statusReasonKeptWidth   = statusReasonColumnWidth - len("...")
	maxIdentifierLength     = 63
	createCollisionLead     = "; re-plan"
)

func TestCreateCollisionRefusalLeadSurvivesStatusReasonClamp(t *testing.T) {
	longest := strings.Repeat("a", maxIdentifierLength)
	tests := []struct {
		name string
		err  error
	}{
		{name: "single statement", err: fmt.Errorf("preflight: %w", preflight.ErrRelationExists)},
		{name: "step past the committed CREATE TABLE", err: fmt.Errorf("execute: %w",
			&executor.SequenceStepError{Step: 2, Total: 3, Err: executor.ErrCreateCollision})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := classifyRefusal(tt.err, longest)

			require.NotNil(t, r)
			lead := strings.Index(r.detail, createCollisionLead)
			require.GreaterOrEqual(t, lead, 0, r.detail)
			assert.LessOrEqual(t, lead+len(createCollisionLead), statusReasonKeptWidth, r.detail)
		})
	}
}

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
	assert.Equal(t, `a name the create set for "users" needs is already occupied (table, view, index, or sequence); step 2 of 3 failed after the CREATE TABLE committed; re-plan, and if it recurs drop or rename the occupant or give the constraint, index, or sequence another name`, r.detail)
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
		{
			name: "permanent invalid-index verdict keeps the typed advice's remedy",
			err: &executor.InvalidIndexError{Schema: "public", Index: "users_ref_idx", Table: "shipments",
				Cleanup: executor.ErrInvalidIndexOnOtherTable},
			wantReason: "invalid-index-occupied",
			wantRemedy: "this change cannot claim it — rename the index in the schema file and re-plan, or clear the entry through that table's own change",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := fmt.Errorf("execute: %w", &executor.SequenceStepError{Step: 2, Total: 3, Err: tt.err})

			r := classifyRefusal(err, "users")

			require.NotNil(t, r)
			assert.Equal(t, tt.wantReason, r.reason)
			step := `; step 2 of 3 failed after the CREATE TABLE committed; `
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

func newTestTracker(t *testing.T) *progress.Tracker {
	t.Helper()
	tracker, err := progress.NewTracker(progress.WallClock{})
	require.NoError(t, err)
	return tracker
}

// TestProgressReportsExecutorStepPosition proves a poll during execution
// reads the step the executor is running from the pg-sprite tracker — not
// the first-step position the engine recorded at accept — and carries that
// step's statement in the single-line, control-free form the progress
// metadata is contracted to hold.
func TestProgressReportsExecutorStepPosition(t *testing.T) {
	eng := New()
	change := nativeApply{namespace: "public", table: "widgets", sql: "CREATE TABLE public.widgets (id bigint PRIMARY KEY)", steps: 3}
	tracker := newTestTracker(t)
	eng.claimProgress("task-a", progressResult(engine.StateRunning, "preflight", time.Now(), change, ""), tracker, slog.Default())
	req := &engine.ProgressRequest{ResumeState: &engine.ResumeState{MigrationContext: "task-a"}}

	before, err := eng.Progress(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, "1", before.Metadata["step"], "before the executor reports, the record keeps the planned first step")
	assert.Equal(t, "3", before.Metadata["steps_total"])
	assert.NotContains(t, before.Metadata, "statement")

	tracker.Start(3, progress.OperationAdmitting)
	tracker.StartStep(2, progress.OperationBrief, "CREATE INDEX widgets_name_idx\n\tON public.widgets ((first_name || ' ' ||\x00 last_name))")

	during, err := eng.Progress(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, engine.StateRunning, during.State)
	assert.Equal(t, "2", during.Metadata["step"])
	assert.Equal(t, "3", during.Metadata["steps_total"])
	assert.Equal(t, "CREATE INDEX widgets_name_idx ON public.widgets ((first_name || ' ' || last_name))", during.Metadata["statement"],
		"the statement collapses whitespace and drops control runes but keeps its SQL operators intact")
	assert.Equal(t, change.sql, during.Tables[0].DDL, "the table's DDL stays the planned change, not the step in flight")
}

// TestSanitizeStatementText pins the statement metadata contract: one line,
// no control or format runes, bounded length, and otherwise the SQL exactly
// as the executor runs it — a reason may trade its pipes for slashes because
// it is prose, but a statement's pipes are operators.
func TestSanitizeStatementText(t *testing.T) {
	t.Run("keeps SQL operators and collapses layout", func(t *testing.T) {
		got := sanitizeStatementText("SELECT 1 | 2,\n\ta || b\u202e FROM t\r\n")
		assert.Equal(t, "SELECT 1 | 2, a || b FROM t", got)
	})
	t.Run("clamps on a rune boundary with an ellipsis", func(t *testing.T) {
		long := strings.Repeat("é", maxStatementMetadataLen+40)
		got := sanitizeStatementText(long)
		runes := []rune(got)
		assert.Len(t, runes, maxStatementMetadataLen)
		assert.Equal(t, '…', runes[len(runes)-1])
		assert.Equal(t, strings.Repeat("é", maxStatementMetadataLen-1), string(runes[:len(runes)-1]))
	})
	t.Run("leaves a statement at the limit untouched", func(t *testing.T) {
		exact := strings.Repeat("x", maxStatementMetadataLen)
		assert.Equal(t, exact, sanitizeStatementText(exact))
	})
}

// TestPublishProgressKeepsTerminalPositionAsPublished proves a terminal
// result answers with the position folded in at publish time: a later poll
// never re-reads the tracker for a finished apply.
func TestPublishProgressKeepsTerminalPositionAsPublished(t *testing.T) {
	eng := New()
	change := nativeApply{namespace: "public", table: "widgets", sql: "CREATE TABLE public.widgets (id bigint PRIMARY KEY)", steps: 2}
	tracker := newTestTracker(t)
	eng.claimProgress("task-a", progressResult(engine.StateRunning, "preflight", time.Now(), change, ""), tracker, slog.Default())
	tracker.Start(2, progress.OperationAdmitting)
	tracker.StartStep(2, progress.OperationBrief, "CREATE INDEX widgets_name_idx ON public.widgets (name)")
	tracker.Finish(errors.New("boom"))

	terminal := progressResult(engine.StateFailed, "failed", time.Now(), change, "detail")
	require.NoError(t, executorProgressMetadata(t.Context(), tracker, terminal.Metadata))
	eng.publishProgress("task-a", terminal, slog.Default())

	got, err := eng.Progress(t.Context(), &engine.ProgressRequest{ResumeState: &engine.ResumeState{MigrationContext: "task-a"}})
	require.NoError(t, err)
	assert.Equal(t, engine.StateFailed, got.State)
	assert.Equal(t, "2", got.Metadata["step"])
	assert.Equal(t, "2", got.Metadata["steps_total"])
	assert.Equal(t, "CREATE INDEX widgets_name_idx ON public.widgets (name)", got.Metadata["statement"])
}

// backgroundApplyDeadline bounds how long a unit test waits for the
// engine's background apply drive to publish, or for a blocked call to
// return. The drives under test run a scripted executor, so anything close
// to this is a hang, not a slow target.
const backgroundApplyDeadline = 10 * time.Second

// scriptedExecutor stands in for pg-sprite's executor behind the engine's
// execute seam. It hands the tracker the drive gave it to the test, then
// waits to be released so the test can poll the engine mid-execution, and
// returns whatever outcome the test scripted.
type scriptedExecutor struct {
	trackers    chan *progress.Tracker
	envelopes   chan applyEnvelope
	released    chan struct{}
	releaseOnce sync.Once
	run         func(tracker *progress.Tracker) error
}

// applyEnvelope is what the drive handed the executor: the change as the
// drive resolved it and the deadline of the context it runs under, read at
// observed so a test can measure the time the drive granted.
type applyEnvelope struct {
	change      nativeApply
	deadline    time.Time
	hasDeadline bool
	observed    time.Time
}

func newScriptedExecutor(run func(tracker *progress.Tracker) error) *scriptedExecutor {
	return &scriptedExecutor{
		trackers:  make(chan *progress.Tracker, 1),
		envelopes: make(chan applyEnvelope, 1),
		released:  make(chan struct{}),
		run:       run,
	}
}

func (s *scriptedExecutor) execute(ctx context.Context, _ targetConn, change nativeApply, _ int64, tracker *progress.Tracker, _ *slog.Logger) error {
	deadline, hasDeadline := ctx.Deadline()
	s.envelopes <- applyEnvelope{change: change, deadline: deadline, hasDeadline: hasDeadline, observed: time.Now()}
	s.trackers <- tracker
	select {
	case <-s.released:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.run(tracker)
}

// release lets the parked executor return. It is safe to call more than
// once, so a test can release mid-way and cleanup can release again for a
// test that failed before it got there — otherwise the drive would stay
// parked and Drain would wait on it for the apply ceiling.
func (s *scriptedExecutor) release() {
	s.releaseOnce.Do(func() { close(s.released) })
}

// tracker returns the tracker the drive handed the executor, failing the
// test if the drive never reached it.
func (s *scriptedExecutor) tracker(t *testing.T) *progress.Tracker {
	t.Helper()
	select {
	case tracker := <-s.trackers:
		return tracker
	case <-time.After(backgroundApplyDeadline):
		t.Fatal("the background drive never reached the executor")
		return nil
	}
}

// envelope returns what the drive handed the executor, failing the test if
// the drive never reached it.
func (s *scriptedExecutor) envelope(t *testing.T) applyEnvelope {
	t.Helper()
	select {
	case env := <-s.envelopes:
		require.True(t, env.hasDeadline, "the drive must bound the executor's context")
		return env
	case <-time.After(backgroundApplyDeadline):
		t.Fatal("the background drive never reached the executor")
		return applyEnvelope{}
	}
}

// applyAlterUsers accepts one native-safe ALTER on public.users through
// applyChange.
func applyAlterUsers(t *testing.T, eng *Engine, scripted *scriptedExecutor, key string, logger *slog.Logger) {
	t.Helper()
	applyChange(t, eng, scripted, key, "ALTER TABLE public.users ADD COLUMN email text", logger)
}

// applyChange wires scripted in as eng's executor and accepts one change on
// public.users through Apply under key, logging to logger, with credentials
// the executor never dials. When the test ends the executor is released and
// the engine drained, in that order, so a drive parked at the executor
// cannot outlive the test or hold Drain open.
func applyChange(t *testing.T, eng *Engine, scripted *scriptedExecutor, key, ddl string, logger *slog.Logger) {
	t.Helper()
	eng.execute = scripted.execute
	t.Cleanup(eng.Drain)
	t.Cleanup(scripted.release)
	accepted, err := eng.Apply(t.Context(), &engine.ApplyRequest{
		Database: "app",
		Changes: []engine.SchemaChange{{
			Namespace: "public",
			TableChanges: []engine.TableChange{{
				Table: "users", DDL: ddl,
			}},
		}},
		Credentials: &engine.Credentials{DSN: "postgres://schemabot:secret@db.invalid/app?sslmode=disable"},
		ResumeState: &engine.ResumeState{MigrationContext: key},
		Logger:      logger,
	})
	require.NoError(t, err)
	require.True(t, accepted.Accepted)
}

// pollProgress reads the engine's progress for key.
func pollProgress(t *testing.T, eng *Engine, key string) *engine.ProgressResult {
	t.Helper()
	got, err := eng.Progress(t.Context(), &engine.ProgressRequest{ResumeState: &engine.ResumeState{MigrationContext: key}})
	require.NoError(t, err)
	return got
}

// TestApplyRegistersExecutorTracker proves Apply hands the executor the very
// tracker it claimed for the progress record, so a poll during execution
// reads the step the executor is running, and the terminal result published
// when the executor returns carries the position the executor finished at.
// The poll-time tests above inject the tracker through claimProgress
// directly; this one pins the wiring they take for granted, end to end
// through the apply drive.
func TestApplyRegistersExecutorTracker(t *testing.T) {
	scripted := newScriptedExecutor(func(tracker *progress.Tracker) error {
		tracker.Finish(nil)
		return nil
	})
	eng := New()
	const key = "task-a"
	applyAlterUsers(t, eng, scripted, key, slog.New(slog.NewTextHandler(io.Discard, nil)))

	tracker := scripted.tracker(t)
	eng.mu.Lock()
	tracked := eng.progress[key]
	eng.mu.Unlock()
	require.NotNil(t, tracked, "Apply must claim a progress record under the task identity")
	require.Same(t, tracker, tracked.tracker, "the executor must feed the tracker the progress record reads")

	tracker.Start(2, progress.OperationAdmitting)
	tracker.StartStep(2, progress.OperationBrief, "ALTER TABLE public.users ADD COLUMN email text")
	during := pollProgress(t, eng, key)
	assert.Equal(t, engine.StateRunning, during.State)
	assert.Equal(t, "2", during.Metadata["step"], "a poll mid-execution reads the executor's live step")
	assert.Equal(t, "2", during.Metadata["steps_total"])

	scripted.release()
	require.Eventually(t, func() bool {
		return pollProgress(t, eng, key).State.IsTerminal()
	}, backgroundApplyDeadline, 10*time.Millisecond, "the drive must publish a terminal result once the executor returns")
	terminal := pollProgress(t, eng, key)
	assert.Equal(t, engine.StateCompleted, terminal.State)
	assert.Equal(t, "2", terminal.Metadata["step"], "the terminal result carries the position the executor finished at")
	assert.Equal(t, "2", terminal.Metadata["steps_total"])
	assert.Equal(t, "ALTER TABLE public.users ADD COLUMN email text", terminal.Metadata["statement"])
}

// TestTerminalPublishReportsAnUnfinishedExecutorBuild pins the guard on the
// terminal publish's premise: an executor that returns while its tracker
// still reports a live build has broken the contract the fold-in relies on,
// and the drive says so — the terminal result carries the tracker's
// last-known position and the failed read is logged under the apply's
// identifiers — instead of publishing silently as though the position were
// final.
func TestTerminalPublishReportsAnUnfinishedExecutorBuild(t *testing.T) {
	session := &indexProgressSession{err: errors.New("connection reset by peer")}
	scripted := newScriptedExecutor(func(tracker *progress.Tracker) error {
		tracker.Start(2, progress.OperationAdmitting)
		tracker.StartStep(2, progress.OperationConcurrentIndex, "CREATE INDEX CONCURRENTLY users_email_idx ON public.users (email)")
		tracker.SetConcurrentBuild(session, 42)
		return errors.New("executor returned mid-build")
	})
	eng := New()
	var logs bytes.Buffer
	const key = "task-a"
	applyAlterUsers(t, eng, scripted, key, slog.New(slog.NewTextHandler(&logs, nil)))
	scripted.tracker(t)
	scripted.release()

	require.Eventually(t, func() bool {
		return pollProgress(t, eng, key).State.IsTerminal()
	}, backgroundApplyDeadline, 10*time.Millisecond)
	terminal := pollProgress(t, eng, key)
	assert.Equal(t, engine.StateFailed, terminal.State)
	assert.Equal(t, "2", terminal.Metadata["step"], "the terminal result keeps the tracker's last-known position")
	assert.Equal(t, "CREATE INDEX CONCURRENTLY users_email_idx ON public.users (email)", terminal.Metadata["statement"])
	assert.True(t, session.queried, "the terminal fold-in reads the tracker the executor left active")
	assert.Contains(t, logs.String(), "terminal progress reports the last-known executor position")
	assert.Contains(t, logs.String(), "task_id=task-a")
	assert.Contains(t, logs.String(), "connection reset by peer")
}

// blockingProgressSession is a reserved build session whose progress read
// parks until the test releases it, so a test can hold a tracker read open
// and observe what else the engine lets through meanwhile.
type blockingProgressSession struct {
	started chan struct{}
	release chan struct{}
}

func (s *blockingProgressSession) QueryRow(ctx context.Context, _ string, _ ...any) pgx.Row {
	close(s.started)
	select {
	case <-s.release:
	case <-ctx.Done():
	}
	return failingRow{err: errors.New("progress read released")}
}

// TestProgressReadsTheTrackerOutsideTheEngineLock proves a poll's tracker
// read — a database round trip for an active concurrent index build — never
// holds the engine lock: while one poll is parked on the server, Apply's
// claim and the drive's terminal publish still go through. Otherwise a slow
// progress view would stall every apply the engine tracks.
func TestProgressReadsTheTrackerOutsideTheEngineLock(t *testing.T) {
	eng := New()
	change := nativeApply{namespace: "public", table: "widgets", sql: "CREATE TABLE public.widgets (id bigint PRIMARY KEY)", steps: 2}
	tracker := newTestTracker(t)
	tracker.Start(2, progress.OperationAdmitting)
	tracker.StartStep(2, progress.OperationConcurrentIndex, "CREATE INDEX CONCURRENTLY widgets_name_idx ON public.widgets (name)")
	session := &blockingProgressSession{started: make(chan struct{}), release: make(chan struct{})}
	tracker.SetConcurrentBuild(session, 42)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng.claimProgress("task-a", progressResult(engine.StateRunning, "preflight", time.Now(), change, ""), tracker, logger)

	polled := make(chan error, 1)
	go func() {
		_, err := eng.Progress(t.Context(), &engine.ProgressRequest{ResumeState: &engine.ResumeState{MigrationContext: "task-a"}})
		polled <- err
	}()
	select {
	case <-session.started:
	case <-time.After(backgroundApplyDeadline):
		t.Fatal("the poll never reached the tracker's progress read")
	}

	claimed := make(chan struct{})
	go func() {
		defer close(claimed)
		eng.claimProgress("task-b", progressResult(engine.StateRunning, "preflight", time.Now(), change, ""), newTestTracker(t), logger)
		eng.publishProgress("task-b", progressResult(engine.StateCompleted, "completed", time.Now(), change, ""), logger)
	}()
	select {
	case <-claimed:
	case <-time.After(backgroundApplyDeadline):
		t.Fatal("a claim and publish waited behind another apply's tracker read")
	}

	close(session.release)
	select {
	case err := <-polled:
		require.NoError(t, err, "a failed tracker read is logged, not returned to the poller")
	case <-time.After(backgroundApplyDeadline):
		t.Fatal("the parked poll did not return once its read was released")
	}
}

// TestProgressNeverReadsTheTrackerForATerminalApply proves a poll on a
// published terminal result answers from the record alone: even a tracker
// that still holds an active build session is not consulted, so a finished
// apply's poll never reaches the target.
func TestProgressNeverReadsTheTrackerForATerminalApply(t *testing.T) {
	eng := New()
	change := nativeApply{namespace: "public", table: "widgets", sql: "CREATE TABLE public.widgets (id bigint PRIMARY KEY)", steps: 2}
	tracker, session := activeBuildTracker(t, errors.New("progress view unavailable"))
	eng.claimProgress("task-a", progressResult(engine.StateRunning, "preflight", time.Now(), change, ""), tracker, slog.Default())
	terminal := progressResult(engine.StateFailed, "failed", time.Now(), change, "detail")
	terminal.Metadata["step"] = "2"
	eng.publishProgress("task-a", terminal, slog.Default())

	got, err := eng.Progress(t.Context(), &engine.ProgressRequest{ResumeState: &engine.ResumeState{MigrationContext: "task-a"}})

	require.NoError(t, err)
	assert.Equal(t, engine.StateFailed, got.State)
	assert.Equal(t, "2", got.Metadata["step"])
	assert.False(t, session.queried, "a terminal poll must not read the tracker")
}

// TestExecutorProgressReadOutlivesTheSessionStatementTimeout pins the order
// of the two deadlines on a tracker read: the apply pool leaves pg-sprite's
// default statement_timeout on every session, and the engine's read deadline
// must sit past it, so a slow progress query is cancelled by the server —
// which hands the session back intact — before the client gives up on the
// socket and closes the connection the build's failure verdict runs on.
func TestExecutorProgressReadOutlivesTheSessionStatementTimeout(t *testing.T) {
	cfg, err := spritePoolConfig("postgres://schemabot:secret@db.invalid/app", "")
	require.NoError(t, err)
	require.Zero(t, cfg.StatementTimeout, "the apply pool runs under pg-sprite's default statement_timeout")
	require.Positive(t, executorProgressReadHeadroom)
	assert.Greater(t, executorProgressReadTimeout, dbconn.DefaultStatementTimeout)
	assert.Equal(t, dbconn.DefaultStatementTimeout+executorProgressReadHeadroom, executorProgressReadTimeout)
}

// indexProgressSession stands in for the session the executor reserves for a
// concurrent index build, with a progress view whose read fails. It records
// the context the read arrived on so a test can check how the engine bounds
// it.
type indexProgressSession struct {
	err             error
	queried         bool
	cancelledAtRead error
	hadDeadline     bool
}

func (s *indexProgressSession) QueryRow(ctx context.Context, _ string, _ ...any) pgx.Row {
	s.queried = true
	s.cancelledAtRead = ctx.Err()
	_, s.hadDeadline = ctx.Deadline()
	return failingRow{err: s.err}
}

type failingRow struct{ err error }

func (r failingRow) Scan(...any) error { return r.err }

// activeBuildTracker returns a tracker mid-way through a concurrent index
// build whose server-side progress read fails with readErr.
func activeBuildTracker(t *testing.T, readErr error) (*progress.Tracker, *indexProgressSession) {
	t.Helper()
	tracker := newTestTracker(t)
	tracker.Start(2, progress.OperationAdmitting)
	tracker.StartStep(2, progress.OperationConcurrentIndex, "CREATE INDEX CONCURRENTLY widgets_name_idx ON public.widgets (name)")
	session := &indexProgressSession{err: readErr}
	tracker.SetConcurrentBuild(session, 42)
	return tracker, session
}

// TestExecutorProgressMetadataKeepsLastKnownPositionOnReadError proves the
// read's error contract: when the server's progress view cannot be queried,
// the metadata still receives the tracker's last-known step, total, and
// statement, and the wrapped error goes back to the caller to log.
func TestExecutorProgressMetadataKeepsLastKnownPositionOnReadError(t *testing.T) {
	readErr := errors.New("connection reset by peer")
	tracker, session := activeBuildTracker(t, readErr)
	metadata := map[string]string{"step": "1", "steps_total": "1"}

	err := executorProgressMetadata(t.Context(), tracker, metadata)

	require.Error(t, err)
	assert.ErrorIs(t, err, readErr)
	assert.Contains(t, err.Error(), "read pg-sprite executor progress")
	assert.True(t, session.queried)
	assert.Equal(t, "2", metadata["step"])
	assert.Equal(t, "2", metadata["steps_total"])
	assert.Equal(t, "CREATE INDEX CONCURRENTLY widgets_name_idx ON public.widgets (name)", metadata["statement"])
}

// TestExecutorProgressMetadataReadsOnItsOwnBoundedContext proves the
// progress-view read never inherits the caller's cancellation — a cancelled
// poller or drive context must not tear down the session the executor
// reserved for its failure verdict — while still carrying a deadline of the
// engine's own.
func TestExecutorProgressMetadataReadsOnItsOwnBoundedContext(t *testing.T) {
	tracker, session := activeBuildTracker(t, errors.New("progress view unavailable"))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := executorProgressMetadata(ctx, tracker, map[string]string{})

	require.Error(t, err)
	require.True(t, session.queried)
	assert.NoError(t, session.cancelledAtRead, "the read must not observe the caller's cancellation")
	assert.True(t, session.hadDeadline, "the read must carry the engine's own deadline")
}

// TestProgressAnswersLastKnownPositionWhenTrackerReadFails proves a poll
// whose progress-view read fails still answers: the running state and the
// tracker's last-known position come back with a nil error, and the failure
// is logged for triage instead of telling the driver its apply is
// unobservable.
func TestProgressAnswersLastKnownPositionWhenTrackerReadFails(t *testing.T) {
	eng := New()
	change := nativeApply{namespace: "public", table: "widgets", sql: "CREATE TABLE public.widgets (id bigint PRIMARY KEY)", steps: 2}
	tracker, _ := activeBuildTracker(t, errors.New("connection reset by peer"))
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	eng.claimProgress("task-a", progressResult(engine.StateRunning, "preflight", time.Now(), change, ""), tracker, logger)

	got, err := eng.Progress(t.Context(), &engine.ProgressRequest{ResumeState: &engine.ResumeState{MigrationContext: "task-a"}})

	require.NoError(t, err)
	assert.Equal(t, engine.StateRunning, got.State)
	assert.Equal(t, "2", got.Metadata["step"])
	assert.Equal(t, "2", got.Metadata["steps_total"])
	assert.Equal(t, "CREATE INDEX CONCURRENTLY widgets_name_idx ON public.widgets (name)", got.Metadata["statement"])
	assert.Contains(t, logs.String(), "reports the last-known executor position")
	assert.Contains(t, logs.String(), "task_id=task-a")
	assert.Contains(t, logs.String(), "connection reset by peer")
}

// TestRefusalForOutcomeTotalOverExecutorCodes pins the classifier to
// pg-sprite's full outcome vocabulary: every code the executor can return
// maps to an explicit disposition — refusal or operational — so a code added
// upstream fails this test instead of silently draining into the generic
// retryable tail. Every refusal also carries a cause: it is the clause the
// composed detail opens with, and a remedy alone would publish a detail that
// starts mid-sentence.
//
// The disposition is also pinned to the executor's own verdict: a code
// pg-sprite marks permanent refuses here, and a code it does not marks
// operational, unless the exception table names the code and states why
// SchemaBot's apply policy departs from the engine's floor. Departing in the
// other direction — retrying a code the engine calls permanent — is never
// sanctioned, because the drive would re-run a verdict that cannot change
// until its attempt ceiling ends it.
func TestRefusalForOutcomeTotalOverExecutorCodes(t *testing.T) {
	// refusedThoughNotPermanent lists the codes SchemaBot refuses although
	// pg-sprite leaves them retryable, with the reason for each. The
	// engine's Permanent doc names this direction of disagreement as an
	// adapter's own retry policy.
	refusedThoughNotPermanent := map[executor.Code]string{
		// The statement budget is sized as SchemaBot's native-safety lease:
		// a statement that needs longer is not native-safe under this
		// policy, so re-running it unchanged would only spend the lease
		// again.
		executor.CodeBudgetStatementExceeded: "the statement budget is the native-safety lease",
	}
	for _, code := range executor.Codes() {
		t.Run(string(code), func(t *testing.T) {
			r, known := refusalForOutcome(code, "users")
			require.True(t, known, "outcome code %q has no explicit apply disposition", code)
			if r != nil {
				assert.NotEmpty(t, r.reason, "refusal for %q has no reason", code)
				assert.NotEmpty(t, r.cause, "refusal for %q has no cause", code)
			}
			refused := r != nil
			if why, exempt := refusedThoughNotPermanent[code]; exempt {
				assert.False(t, code.Permanent(), "exception for %q is stale: the engine now marks it permanent", code)
				assert.True(t, refused, "code %q is listed as a policy refusal (%s) but is operational", code, why)
				return
			}
			assert.Equal(t, code.Permanent(), refused,
				"disposition for %q disagrees with the engine's permanence verdict", code)
		})
	}
}

// TestOrdinaryApplyRunsUnderTheFixedCeiling proves a native statement's
// drive hands the executor a context bounded by the fixed apply ceiling —
// not by the concurrent index envelope — with room for the statement budget
// under it.
func TestOrdinaryApplyRunsUnderTheFixedCeiling(t *testing.T) {
	scripted := newScriptedExecutor(func(*progress.Tracker) error { return nil })
	eng := NewWithOptions(0, 3*time.Hour)
	applyAlterUsers(t, eng, scripted, "task-a", slog.New(slog.DiscardHandler))

	env := scripted.envelope(t)
	granted := env.deadline.Sub(env.observed)
	assert.False(t, env.change.concurrentIndex)
	assert.Zero(t, env.change.concurrentIndexMaxDuration, "the build bound is stamped only on a concurrent index apply")
	assert.LessOrEqual(t, granted, optimisticApplyCeiling)
	assert.Greater(t, granted, optimisticStatementLimit, "the statement budget must fit under the ceiling")
}

// TestConcurrentIndexApplyCeilingLeavesSetupHeadroom proves the drive runs a
// concurrent index build under a ceiling that sits above the configured
// build bound — so the build's own deadline, not the ceiling, is what ends
// an over-long build — and stamps that bound on the change the executor
// receives. The granted time is measured from the executor's side: a ceiling
// equal to the bound would fail here, and so would one padded past the
// headroom the engine documents.
func TestConcurrentIndexApplyCeilingLeavesSetupHeadroom(t *testing.T) {
	const bound = 3 * time.Hour
	scripted := newScriptedExecutor(func(*progress.Tracker) error { return nil })
	eng := NewWithOptions(0, bound)
	applyChange(t, eng, scripted, "task-a",
		"CREATE INDEX CONCURRENTLY users_email_idx ON public.users (email)", slog.New(slog.DiscardHandler))

	env := scripted.envelope(t)
	granted := env.deadline.Sub(env.observed)
	assert.True(t, env.change.concurrentIndex)
	assert.Equal(t, bound, env.change.concurrentIndexMaxDuration)
	assert.Greater(t, granted, bound, "the ceiling must outlast the build bound it wraps")
	assert.LessOrEqual(t, granted, bound+concurrentIndexHeadroom)
}

// TestConcurrentIndexHeadroomCoversSessionSetup pins the headroom to what it
// exists to absorb: the session setup executeOptimistic runs before the
// build starts, with every step at the server-side limit the apply pool
// gives it — one dial at the connect timeout, then the privilege check, the
// table preflight and the partition facts lookup each at the pool's
// statement_timeout. Under that headroom the build gets the full configured
// bound even when setup is as slow as the server allows it to be.
//
// The privilege check counts as one read: its second query runs only when
// the first finds no target table, and that ends the apply as a refusal
// before any build a bound could be shortened for; the SET ROLE probe it
// also carries belongs to the copy-and-swap tier, which a concurrent index
// build never reaches.
func TestConcurrentIndexHeadroomCoversSessionSetup(t *testing.T) {
	setup := dbconn.DefaultConnectTimeout + // pool dial
		dbconn.DefaultStatementTimeout + // preflight.CheckPrivileges
		dbconn.DefaultStatementTimeout + // preflight.CheckTable
		dbconn.DefaultStatementTimeout // preflight.LookupTargetFacts
	assert.GreaterOrEqual(t, concurrentIndexHeadroom, setup)
}

// TestConcurrentIndexMaximumIsTheServerStatementTimeoutCeiling pins the
// largest accepted build bound to the largest statement_timeout PostgreSQL
// itself accepts, and the constructor's normalization around it: a bound
// above it is clamped, and a bound that is not positive adopts the default
// instead of a deadline that would end every build on arrival.
func TestConcurrentIndexMaximumIsTheServerStatementTimeoutCeiling(t *testing.T) {
	assert.Equal(t, time.Duration(math.MaxInt32)*time.Millisecond, MaxConcurrentIndexMaxDuration)
	assert.Less(t, MaxConcurrentIndexMaxDuration+concurrentIndexHeadroom, time.Duration(math.MaxInt64),
		"the ceiling built on the maximum must not overflow")

	assert.Equal(t, MaxConcurrentIndexMaxDuration, NewWithOptions(0, MaxConcurrentIndexMaxDuration+1).ConcurrentIndexMaxDuration())
	assert.Equal(t, DefaultConcurrentIndexMaxDuration, NewWithOptions(0, 0).ConcurrentIndexMaxDuration())
	assert.Equal(t, DefaultConcurrentIndexMaxDuration, NewWithOptions(0, -time.Second).ConcurrentIndexMaxDuration())
	assert.Equal(t, 36*time.Hour, NewWithOptions(0, 36*time.Hour).ConcurrentIndexMaxDuration())
}

// TestBuildIndexConcurrentlyRefusesAnUnsetBound proves a concurrent build
// never starts under a bound the drive did not stamp: the bound is the
// build's server-side statement timeout and the recovery's only deadline, so
// a missing one is refused before any session is acquired rather than handed
// to the executor as an unbounded budget or run to an instant cancellation.
// The drive always stamps a normalized bound, so this exercises the guard a
// directly constructed nativeApply meets, not a state the drive produces.
func TestBuildIndexConcurrentlyRefusesAnUnsetBound(t *testing.T) {
	change := nativeApply{namespace: "public", table: "users",
		sql: "CREATE INDEX CONCURRENTLY users_email_idx ON public.users (email)", concurrentIndex: true}

	err := buildIndexConcurrently(t.Context(), nil, change, newTestTracker(t), slog.New(slog.DiscardHandler))

	require.ErrorContains(t, err, `build PostgreSQL index concurrently on table "users": build bound is unset`)
}

// TestNameConcurrentIndexBoundWrapsOnlyTheBoundsOwnVerdict pins which build
// failures get the option named: the statement budget verdict is the bound's
// statement_timeout firing and is wrapped with the verdict still reachable,
// while a lock budget, a cancellation, and an invalid-index verdict that
// carries no build failure pass through untouched — none of them was the
// bound's doing.
func TestNameConcurrentIndexBoundWrapsOnlyTheBoundsOwnVerdict(t *testing.T) {
	bound := 36 * time.Hour
	statementBudget := &executor.BudgetError{Cause: executor.CauseStatement, Budget: bound}

	named := nameConcurrentIndexBound(fmt.Errorf("build: %w", statementBudget), bound)
	var boundErr *concurrentIndexBoundError
	require.ErrorAs(t, named, &boundErr)
	assert.Equal(t, "the concurrent index build ran past postgres.concurrent_index_max_duration (36h0m0s) and was cancelled", boundErr.Error())
	var budgetErr *executor.BudgetError
	require.ErrorAs(t, named, &budgetErr)
	assert.Same(t, statementBudget, budgetErr)

	for name, err := range map[string]error{
		"lock budget":  &executor.BudgetError{Cause: executor.CauseLock, Budget: time.Second},
		"cancellation": executor.ErrCancelledByCaller,
		"abandoned entry without a build failure": &executor.InvalidIndexError{Schema: "public", Index: "big_ref_idx", Table: "users",
			Cleanup: executor.ErrAbandonedInvalidIndex},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Same(t, err, nameConcurrentIndexBound(err, bound))
		})
	}
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
// the verdict code: a removal is promised only where the recovery proves the
// entry is a failed build's debris on the target table before it drops —
// this build's own leftover, an abandoned entry, or an entry whose builder
// the role cannot see, which the recovery settles under the table's lock —
// and there the retry performs it; a build in flight says wait and names the
// builder; an entry on another table, one the server will not drop
// concurrently, and an unproven verdict get investigation steps because the
// index may be healthy or intentional, and never mention a drop in any
// form. Every branch names the index and none renders the wrapped build or
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
			name: "own leftover says the retry removes and rebuilds it",
			err: &executor.InvalidIndexError{Schema: "public", Index: "big_ref_idx",
				Build: rawServerText, Cleanup: executor.ErrBuildLeftInvalidIndex},
			wantDetail:    []string{`"public"."big_ref_idx"`, "own invalid index", "retry removes it", "rebuilds the index"},
			wantNotDetail: []string{"db-internal-1", "concurrent_index_max_duration"},
		},
		{
			name: "own leftover the bound cancelled names the option to raise before the retry",
			err: &executor.InvalidIndexError{Schema: "public", Index: "big_ref_idx",
				Build: &executor.BudgetError{Cause: executor.CauseStatement, Budget: 36 * time.Hour}, Cleanup: executor.ErrBuildLeftInvalidIndex},
			wantDetail:    []string{`"public"."big_ref_idx"`, "own invalid index", "running past postgres.concurrent_index_max_duration (36h0m0s)", "retry removes it", "rebuilds the index under the same bound", "raise postgres.concurrent_index_max_duration first"},
			wantNotDetail: []string{"statement budget"},
		},
		{
			name: "own leftover from a lost lock budget does not blame the bound",
			err: &executor.InvalidIndexError{Schema: "public", Index: "big_ref_idx",
				Build: &executor.BudgetError{Cause: executor.CauseLock, Budget: time.Second}, Cleanup: executor.ErrBuildLeftInvalidIndex},
			wantDetail:    []string{`"public"."big_ref_idx"`, "own invalid index", "retry removes it"},
			wantNotDetail: []string{"concurrent_index_max_duration"},
		},
		{
			name: "abandoned entry says the retry removes and rebuilds it",
			err: &executor.InvalidIndexError{Schema: "public", Index: "big_ref_idx", Table: "orders",
				Cleanup: executor.ErrAbandonedInvalidIndex},
			wantDetail:    []string{`"public"."big_ref_idx"`, "abandoned", "no backend building it", "retry removes it", "rebuilds the index"},
			wantNotDetail: []string{"db-internal-1"},
		},
		{
			name: "build in flight says wait and names the builder, never a drop",
			err: &executor.InvalidIndexError{Schema: "public", Index: "big_ref_idx", Table: "orders",
				BuilderPID: 4242, Cleanup: executor.ErrInvalidIndexBuildInFlight},
			wantDetail:    []string{`"public"."big_ref_idx"`, "backend 4242", "still building it", "wait"},
			wantNotDetail: []string{"retry removes it", "drop", "db-internal-1"},
		},
		{
			name: "unobservable builder says the retry proves abandonment under the lock and rebuilds",
			err: &executor.InvalidIndexError{Schema: "public", Index: "big_ref_idx", Table: "orders",
				Cleanup: executor.ErrInvalidIndexBuilderUnobservable},
			wantDetail:    []string{`"public"."big_ref_idx"`, "cannot observe", "table's lock", "removes it", "rebuilds the index", "lock budget"},
			wantNotDetail: []string{"pg_read_all_stats", "drop", "db-internal-1"},
		},
		{
			name: "entry on another table names that table and a re-plan, never a drop",
			err: &executor.InvalidIndexError{Schema: "public", Index: "big_ref_idx", Table: "shipments",
				Cleanup: executor.ErrInvalidIndexOnOtherTable},
			wantDetail:    []string{`"public"."big_ref_idx"`, "different table", `"shipments"`, "re-plan"},
			wantNotDetail: []string{"retry removes it", "retry", "drop", "db-internal-1"},
		},
		{
			name: "entry on another table with no inspected table name still re-plans",
			err: &executor.InvalidIndexError{Schema: "public", Index: "big_ref_idx",
				Cleanup: executor.ErrInvalidIndexOnOtherTable},
			wantDetail:    []string{`"public"."big_ref_idx"`, "different table;", "re-plan"},
			wantNotDetail: []string{"retry removes it", "drop", `("")`},
		},
		{
			name: "non-droppable entry is left to an operator, never a drop",
			err: &executor.InvalidIndexError{Schema: "public", Index: "big_ref_idx", Table: "orders",
				Cleanup: executor.ErrInvalidIndexNotDroppable},
			wantDetail:    []string{`"public"."big_ref_idx"`, "constraint's index", "operator must resolve", "re-plan"},
			wantNotDetail: []string{"retry removes it", "retry", "drop", "db-internal-1"},
		},
		{
			name: "unproven verdict gets catalog inspection, never a drop",
			err: &executor.InvalidIndexError{Schema: "public", Index: "big_ref_idx",
				Build: rawServerText, Cleanup: rawServerText},
			wantDetail:    []string{`"public"."big_ref_idx"`, "pg_index.indisvalid"},
			wantNotDetail: []string{"retry removes it", "drop", "db-internal-1"},
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

// TestAbandonedBeforeBuildAdmitsOnlyProvenRecoveryVerdicts pins the gate on
// the one action in this engine that removes an index from a live table:
// pg-sprite's recovery runs only over a verdict the build returned before
// running — an entry proven abandoned on the target table, or one there
// whose builder this role cannot observe and the recovery settles under the
// table's lock. This drive's own leftover stays with the failure that
// produced it whether or not a build error is attached; a visibly in-flight
// build, another table's entry, an index the server will not drop
// concurrently, and an unproven verdict are never this actor's to clear, and
// a verdict wrapping a build failure means the build ran.
func TestAbandonedBeforeBuildAdmitsOnlyProvenRecoveryVerdicts(t *testing.T) {
	buildFailure := &executor.BudgetError{Cause: executor.CauseStatement, Budget: time.Second}
	tests := []struct {
		name string
		err  *executor.InvalidIndexError
		want bool
	}{
		{
			name: "abandoned entry on the target table is recovered",
			err:  &executor.InvalidIndexError{Schema: "public", Index: "ref_idx", Table: "orders", Cleanup: executor.ErrAbandonedInvalidIndex},
			want: true,
		},
		{
			name: "entry whose builder the role cannot observe is recovered under the lock",
			err:  &executor.InvalidIndexError{Schema: "public", Index: "ref_idx", Table: "orders", Cleanup: executor.ErrInvalidIndexBuilderUnobservable},
			want: true,
		},
		{
			name: "own leftover with its build failure stays with that failure",
			err:  &executor.InvalidIndexError{Schema: "public", Index: "ref_idx", Build: buildFailure, Cleanup: executor.ErrBuildLeftInvalidIndex},
			want: false,
		},
		{
			name: "own leftover from a build that reported success stays with that outcome",
			err:  &executor.InvalidIndexError{Schema: "public", Index: "ref_idx", Cleanup: executor.ErrBuildLeftInvalidIndex},
			want: false,
		},
		{
			name: "abandoned verdict wrapping a build failure means the build ran",
			err:  &executor.InvalidIndexError{Schema: "public", Index: "ref_idx", Table: "orders", Build: buildFailure, Cleanup: executor.ErrAbandonedInvalidIndex},
			want: false,
		},
		{
			name: "visibly in-flight build is waited out, never recovered",
			err:  &executor.InvalidIndexError{Schema: "public", Index: "ref_idx", Table: "orders", BuilderPID: 4242, Cleanup: executor.ErrInvalidIndexBuildInFlight},
			want: false,
		},
		{
			name: "entry on another table is never this change's to clear",
			err:  &executor.InvalidIndexError{Schema: "public", Index: "ref_idx", Table: "shipments", Cleanup: executor.ErrInvalidIndexOnOtherTable},
			want: false,
		},
		{
			name: "index the server will not drop concurrently is never recovered",
			err:  &executor.InvalidIndexError{Schema: "public", Index: "ref_idx", Table: "orders", Cleanup: executor.ErrInvalidIndexNotDroppable},
			want: false,
		},
		{
			name: "unproven verdict fails safe",
			err:  &executor.InvalidIndexError{Schema: "public", Index: "ref_idx", Cleanup: errors.New("catalog read failed")},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, abandonedBeforeBuild(tt.err))
		})
	}
}

// TestRecoveryFailureDetailNamesTheAbandonedIndex proves a recovery that
// fails for a reason pg-sprite types without an index in it — its proof lock
// lost to a build still holding the table, a pool with no room for its extra
// session — still publishes a detail naming the abandoned entry the retry
// acts on, in front of the failure's own classification and remedy; while a
// recovery that ends in a fresh invalid-index verdict is classified by that
// verdict alone, since it already names the index.
func TestRecoveryFailureDetailNamesTheAbandonedIndex(t *testing.T) {
	verdict := &executor.InvalidIndexError{Schema: "public", Index: "orders_ref_idx", Table: "orders",
		Cleanup: executor.ErrAbandonedInvalidIndex}
	wrap := func(recoveryErr error) error {
		return fmt.Errorf("rebuild PostgreSQL index concurrently on table %q: %w", "orders",
			&indexRecoveryError{verdict: verdict, err: recoveryErr})
	}

	t.Run("lost proof lock stays retryable and names the index before the lock remedy", func(t *testing.T) {
		err := wrap(&executor.BudgetError{Cause: executor.CauseLock, Budget: 5 * time.Second})
		require.Nil(t, classifyRefusal(err, "orders"), "a lock budget lost during recovery is operational")
		failure := classifyApplyFailure(err, "orders")
		assert.True(t, failure.retryable)
		assert.True(t, failure.lockBudget)
		detail := recoveryContextDetail(err, failure.detail)
		assert.True(t, strings.HasPrefix(detail, `recovering abandoned invalid index "public"."orders_ref_idx" failed: `), detail)
		assert.Contains(t, detail, "lock budget")
		assert.Contains(t, detail, "retry once lock contention subsides")
	})

	t.Run("pool too small stays a permanent refusal and names the index before the pool remedy", func(t *testing.T) {
		err := wrap(fmt.Errorf("admit recovery session: %w", executor.ErrPoolTooSmall))
		r := classifyRefusal(err, "orders")
		require.NotNil(t, r)
		assert.Equal(t, "pool-too-small", r.reason)
		detail := recoveryContextDetail(err, r.detail)
		assert.True(t, strings.HasPrefix(detail, `recovering abandoned invalid index "public"."orders_ref_idx" failed: `), detail)
		assert.Contains(t, detail, "raise the pool size")
	})

	t.Run("a fresh invalid-index verdict from the recovery is the outcome", func(t *testing.T) {
		unproven := &executor.InvalidIndexError{Schema: "public", Index: "orders_ref_idx",
			Cleanup: executor.ErrAbandonmentUnproven}
		err := wrap(unproven)
		var got *executor.InvalidIndexError
		require.ErrorAs(t, err, &got)
		assert.Same(t, unproven, got, "the recovery's own verdict must win over the pre-build one")
		assert.Equal(t, executor.CodeInvalidIndexUnproven, got.Code())
	})

	t.Run("a failure outside any recovery is left alone", func(t *testing.T) {
		detail := recoveryContextDetail(errors.New("connection reset"), "PostgreSQL schema change failed; see server logs")
		assert.Equal(t, "PostgreSQL schema change failed; see server logs", detail)
	})
}

// TestProgressIsKeyedToTheRequestingApply proves the engine answers Progress
// for the apply the caller identifies, not for whichever apply wrote last:
// one engine is shared for a target's lifetime, so a mismatched identity must
// read the idle sentinel instead of another schema change's state.
func TestProgressIsKeyedToTheRequestingApply(t *testing.T) {
	eng := New()
	change := nativeApply{namespace: "public", table: "t_a", sql: "ALTER TABLE public.t_a ADD COLUMN a text"}
	eng.claimProgress("task-a", progressResult(engine.StateCompleted, "completed", time.Now(), change, ""), newTestTracker(t), slog.Default())

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
	eng.claimProgress("task-a", progressResult(engine.StateRunning, "preflight", time.Now(), changeA, ""), newTestTracker(t), slog.Default())
	eng.claimProgress("task-b", progressResult(engine.StateRunning, "preflight", time.Now(), changeB, ""), newTestTracker(t), slog.Default())

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
	eng.claimProgress("task-settled", progressResult(engine.StateCompleted, "completed", time.Now(), settled, ""), newTestTracker(t), slog.Default())
	eng.claimProgress("task-running", progressResult(engine.StateRunning, "preflight", time.Now(), running, ""), newTestTracker(t), slog.Default())

	eng.claimProgress("task-fresh", progressResult(engine.StateRunning, "preflight", time.Now(), fresh, ""), newTestTracker(t), slog.Default())

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
	eng.claimProgress("task-b", progressResult(engine.StateRunning, "preflight", time.Now(), changeB, ""), newTestTracker(t), slog.Default())

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
	eng.claimProgress("task-a", progressResult(engine.StateCompleted, "completed", time.Now(), changeA, ""), newTestTracker(t), slog.Default())
	eng.claimProgress("task-b", progressResult(engine.StateRunning, "preflight", time.Now(), changeB, ""), newTestTracker(t), slog.Default())

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
	assert.False(t, change.concurrentIndex)
}

// TestValidateOptimisticApplyClassifiesConcurrentIndex pins both halves of
// the predicate that routes a statement to the concurrent-build executor:
// only a lone CREATE INDEX that names CONCURRENTLY qualifies. A plain CREATE
// INDEX is the same statement kind and must stay on the transactional path;
// TestValidateOptimisticApplyAcceptsCreateSet pins that a create set, which
// is never one statement, does not qualify either.
func TestValidateOptimisticApplyClassifiesConcurrentIndex(t *testing.T) {
	tests := []struct {
		name       string
		ddl        string
		concurrent bool
	}{
		{
			name:       "concurrent index",
			ddl:        "CREATE INDEX CONCURRENTLY widgets_name_idx ON public.widgets (name)",
			concurrent: true,
		},
		{
			name:       "plain index stays transactional",
			ddl:        "CREATE INDEX widgets_name_idx ON public.widgets (name)",
			concurrent: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &engine.ApplyRequest{
				Database: "app",
				Changes: []engine.SchemaChange{{Namespace: "public", TableChanges: []engine.TableChange{{
					Table: "widgets",
					DDL:   tc.ddl,
				}}}},
				Credentials: &engine.Credentials{DSN: "postgres://localhost/app"},
			}

			change, err := validateOptimisticApply(req)
			require.NoError(t, err)
			assert.Equal(t, tc.concurrent, change.concurrentIndex)
		})
	}
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

	err := executeOptimistic(t.Context(), conn, nativeApply{namespace: "public", table: "widgets", sql: "CREATE TABLE widgets (id bigint PRIMARY KEY)"}, DefaultNativeSafeTableSizeLimitBytes, newTestTracker(t), slog.Default())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "open pg-sprite apply pool")
	assert.Contains(t, err.Error(), "read CA bundle")
}
