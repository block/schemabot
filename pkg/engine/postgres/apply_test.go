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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
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
			name: "create name mismatch names the relations and leaves the table standing",
			err: fmt.Errorf("execute: %w", &executor.SequenceStepError{
				Step: 1, Total: 3, Err: &executor.CreateNameMismatchError{
					Schema: "public", Table: "users",
					Missing:   []string{"users_pkey", "users_id_seq"},
					Unclaimed: []string{"users_pkey1", "users_id_seq1"},
				},
			}),
			wantReason: "create-name-mismatch",
			wantDetail: []string{
				`the CREATE TABLE for "users" committed`,
				`"users_pkey", "users_id_seq"`,
				`owns "users_pkey1", "users_id_seq1" instead`,
				"step 1 of 3 failed",
				"free the first-choice name and rename the owned relation to it, or drop the table, then re-plan against the current schema",
			},
			wantNotDetail: []string{"after the CREATE TABLE committed"},
		},
		{
			name: "create name mismatch identifiers are sanitized for Markdown",
			err: &executor.CreateNameMismatchError{
				Schema: "public", Table: "users",
				Missing:   []string{"users|pkey"},
				Unclaimed: []string{"users\npkey1"},
			},
			wantReason:    "create-name-mismatch",
			wantDetail:    []string{`"users/pkey"`, `"users\npkey1"`},
			wantNotDetail: []string{"|", "\n"},
		},
		{
			name:       "bare create name mismatch code still refuses",
			err:        fmt.Errorf("execute: %w", executor.ErrCreateNameMismatch),
			wantReason: "create-name-mismatch",
			wantDetail: []string{`the CREATE TABLE for "users" committed`, "suffixed name", createNameMismatchRemedy},
		},
		{
			name: "unverified create names refuse because a retry collides with the committed table",
			err: fmt.Errorf("execute: %w", &executor.SequenceStepError{
				Step: 1, Total: 2, Err: fmt.Errorf("%w: %w", executor.ErrCreateNamesUnverified, context.Canceled),
			}),
			wantReason: "create-names-unverified",
			wantDetail: []string{
				`the CREATE TABLE for "users" committed`,
				"could not be read",
				"step 1 of 2 failed",
				"compare the table's constraint-index and sequence names against the schema file, then " + replanRemedy,
			},
			wantNotDetail: []string{context.Canceled.Error()},
		},
		{
			name: "unverified create names keep their verdict when the read-back found no table at the name",
			err: fmt.Errorf("execute: %w", &executor.SequenceStepError{
				Step: 1, Total: 2, Err: fmt.Errorf("%w: public.users: %w", executor.ErrCreateNamesUnverified,
					fmt.Errorf("%w: public.users", preflight.ErrTableNotFound)),
			}),
			wantReason:    "create-names-unverified",
			wantDetail:    []string{`the CREATE TABLE for "users" committed`, "could not be read"},
			wantNotDetail: []string{"does not exist on the target"},
		},
		{
			name: "unverified create names keep their verdict when the read-back found a non-table at the name",
			err: fmt.Errorf("execute: %w", &executor.SequenceStepError{
				Step: 1, Total: 2, Err: fmt.Errorf("%w: public.users: %w", executor.ErrCreateNamesUnverified,
					fmt.Errorf("%w: public.users has relkind %q", preflight.ErrNotTable, "v")),
			}),
			wantReason:    "create-names-unverified",
			wantDetail:    []string{`the CREATE TABLE for "users" committed`, "could not be read"},
			wantNotDetail: []string{"is not an ordinary or partitioned table"},
		},
		{
			name: "create name mismatch with nothing owned unclaimed omits the owned clause",
			err: &executor.CreateNameMismatchError{
				Schema: "public", Table: "users",
				Missing: []string{"users_pkey"},
			},
			wantReason:    "create-name-mismatch",
			wantDetail:    []string{`does not own "users_pkey" the schema file claims`, createNameMismatchRemedy},
			wantNotDetail: []string{"instead", "no name"},
		},
		{
			name: "create name mismatch enumerates the prefix of a wide table's names that fits and counts the rest",
			err: &executor.CreateNameMismatchError{
				Schema: "public", Table: "users",
				Missing:   []string{"users_a_key", "users_b_key", "users_c_key", "users_d_key", "users_e_key"},
				Unclaimed: []string{"users_a_key1", "users_b_key1", "users_c_key1", "users_d_key1", "users_e_key1"},
			},
			wantReason: "create-name-mismatch",
			wantDetail: []string{
				`does not own "users_a_key", and 4 more the schema file claims`,
				`owns "users_a_key1", and 4 more instead`,
			},
			wantNotDetail: []string{"users_b_key", "users_e_key"},
		},
		{
			name: "create name mismatch counts names too long to show",
			err: &executor.CreateNameMismatchError{
				Schema: "public", Table: "users",
				Missing:   []string{strings.Repeat("m", 63), strings.Repeat("n", 63)},
				Unclaimed: []string{strings.Repeat("o", 63)},
			},
			wantReason:    "create-name-mismatch",
			wantDetail:    []string{"does not own 2 names the schema file claims; it owns 1 name instead"},
			wantNotDetail: []string{`"mmm`, `"ooo`},
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
				nameConcurrentIndexBound(&executor.BudgetError{Cause: executor.CauseStatement, Budget: 36 * time.Hour}, 36*time.Hour, 36*time.Hour)),
			wantReason:    "concurrent-index-bound-exceeded",
			wantDetail:    []string{"ran past postgres.concurrent_index_max_duration (36h0m0s)", "raise postgres.concurrent_index_max_duration and re-run"},
			wantNotDetail: []string{"statement budget", replanRemedy},
		},
		{
			name: "bound ending the executor's catalog reads ahead of the build is the same refusal",
			err: fmt.Errorf("build PostgreSQL index concurrently on table %q: %w", "users",
				nameConcurrentIndexBound(fmt.Errorf("resolve target: %w", &pgconn.PgError{Code: sqlstateQueryCanceled}),
					36*time.Hour, 36*time.Hour+time.Second)),
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
				}, 36*time.Hour, 36*time.Hour)),
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

// The mismatch cause carries the identifiers the remedy acts on, and the
// enumeration yields room to the table name so that, like the collision
// detail, the remedy's lead lands inside the clamp for a table name of any
// legal length, however many names of whatever length the table claims and
// owns. A short table name with the shape a real table produces, a primary
// key and a serial column with their suffixed twins, still shows every name.
func TestCreateNameMismatchRefusalLeadSurvivesStatusReasonClamp(t *testing.T) {
	const lead = "; free the first-choice name"
	longest := strings.Repeat("a", maxIdentifierLength)
	widest := []string{longest, longest, longest, longest, longest}
	tests := []struct {
		name       string
		table      string
		mismatch   *executor.CreateNameMismatchError
		total      int
		wantDetail []string
	}{
		{
			name:  "realistic shape shows every name",
			table: "users",
			mismatch: &executor.CreateNameMismatchError{
				Missing: []string{"users_pkey", "users_id_seq"}, Unclaimed: []string{"users_pkey1", "users_id_seq1"},
			},
			total:      3,
			wantDetail: []string{`"users_pkey", "users_id_seq" the schema file claims; it owns "users_pkey1", "users_id_seq1" instead`},
		},
		{
			name:  "ordinary table name past the width a count bound protected",
			table: "payment_methods",
			mismatch: &executor.CreateNameMismatchError{
				Missing: []string{"payment_methods_pkey", "payment_methods_id_seq"}, Unclaimed: []string{"payment_methods_pkey1", "payment_methods_id_seq1"},
			},
			total:      3,
			wantDetail: []string{`"payment_methods_pkey", and 1 more the schema file claims`},
		},
		{
			name:       "maximal table name, maximal names, three-digit create set",
			table:      longest,
			mismatch:   &executor.CreateNameMismatchError{Missing: widest, Unclaimed: widest},
			total:      120,
			wantDetail: []string{"does not own 5 names the schema file claims; it owns 5 names instead"},
		},
		{
			name:     "maximal table name with nothing owned unclaimed",
			table:    longest,
			mismatch: &executor.CreateNameMismatchError{Missing: widest},
			total:    120,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mismatch.Schema, tt.mismatch.Table = "public", tt.table
			err := fmt.Errorf("execute: %w", &executor.SequenceStepError{Step: 1, Total: tt.total, Err: tt.mismatch})

			r := classifyRefusal(err, tt.table)

			require.NotNil(t, r)
			assert.Equal(t, "create-name-mismatch", r.reason)
			at := strings.Index(r.detail, lead)
			require.GreaterOrEqual(t, at, 0, r.detail)
			assert.LessOrEqual(t, at+len(lead), statusReasonKeptWidth, r.detail)
			for _, want := range tt.wantDetail {
				assert.Contains(t, r.detail, want)
			}
		})
	}
}

// quotedNames spends its budget on whole names: a name is shown only when it
// fits together with the separator before it and the count of what follows,
// so the rendering never exceeds the budget it was given, and a budget too
// small for even the first name still says how many names there are.
func TestQuotedNamesFitsBudget(t *testing.T) {
	names := []string{"users_pkey", "users_id_seq", "users_email_key"}
	tests := []struct {
		name   string
		names  []string
		budget int
		want   string
	}{
		{name: "everything fits", names: names, budget: 60, want: `"users_pkey", "users_id_seq", "users_email_key"`},
		{name: "the count of the rest is charged against the budget", names: names, budget: 39, want: `"users_pkey", and 2 more`},
		{name: "two names and the count fit exactly", names: names, budget: 40, want: `"users_pkey", "users_id_seq", and 1 more`},
		{name: "nothing fits", names: names, budget: 11, want: "3 names"},
		{name: "a single name that does not fit is counted in the singular", names: names[:1], budget: 0, want: "1 name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := quotedNames(tt.names, tt.budget)

			assert.Equal(t, tt.want, got)
			assert.LessOrEqual(t, len(got), max(tt.budget, len(countedNames(len(tt.names)))))
		})
	}
}

// The bare create-names-unverified code is never what production hands the
// outcome switch — the sentinel rides along whenever the code does, and
// refusalForCause decides on it first — so the arm that keeps the switch total
// is pinned to the same verdict the sentinel produces.
func TestRefusalForOutcomeBareUnverifiedCodeMatchesTheSentinelVerdict(t *testing.T) {
	fromCode, known := refusalForOutcome(executor.CodeCreateNamesUnverified, "users")
	fromSentinel := classifyRefusal(fmt.Errorf("execute: %w", executor.ErrCreateNamesUnverified), "users")

	require.True(t, known)
	require.NotNil(t, fromCode)
	require.NotNil(t, fromSentinel)
	assert.Equal(t, "create-names-unverified", fromCode.reason)
	assert.Equal(t, fromSentinel.reason, fromCode.reason)
	assert.Equal(t, fromSentinel.cause, fromCode.cause)
	assert.Equal(t, fromSentinel.remedy, fromCode.remedy)
	assert.Contains(t, fromCode.remedy, "compare the table's constraint-index and sequence names against the schema file")
}

// The unverified cause carries only the table name, so like the collision
// detail it can promise its remedy's lead for a table name of any legal
// length. The read-back runs right after the CREATE TABLE, so the step
// clause is always the first step's, and the lead that must survive names
// which of the table's names the operator compares.
func TestCreateNamesUnverifiedRefusalLeadSurvivesStatusReasonClamp(t *testing.T) {
	const lead = "; compare the table's constraint-index and sequence names"
	longest := strings.Repeat("a", maxIdentifierLength)
	err := fmt.Errorf("execute: %w", &executor.SequenceStepError{
		Step: 1, Total: 3, Err: fmt.Errorf("%w: public.%s: %w", executor.ErrCreateNamesUnverified, longest, context.Canceled),
	})

	r := classifyRefusal(err, longest)

	require.NotNil(t, r)
	assert.Equal(t, "create-names-unverified", r.reason)
	at := strings.Index(r.detail, lead)
	require.GreaterOrEqual(t, at, 0, r.detail)
	assert.LessOrEqual(t, at+len(lead), statusReasonKeptWidth, r.detail)
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
	eng.claimProgress("task-a", progressResult(engine.StateRunning, "preflight", time.Now(), change, ""), tracker, slog.Default(), false, nil)
	req := &engine.ProgressRequest{ResumeState: &engine.ResumeState{MigrationContext: "task-a"}}

	before, err := eng.Progress(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, "1", before.Metadata["step"], "before the executor reports, the record keeps the planned first step")
	assert.Equal(t, "3", before.Metadata["steps_total"])
	assert.NotContains(t, before.Metadata, "statement")
	for _, key := range []string{"executor_operation", "server_phase", "attempt", "blocks_done", "blocks_total", "tuples_done", "tuples_total", "lockers_done", "lockers_total"} {
		assert.NotContains(t, before.Metadata, key)
	}

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

// TestProgressMetadataIsStableWhilePositionIsUnchanged pins the contract the
// driver's persistence relies on: two polls of a running apply whose executor
// position has not moved return identical metadata, so the driver's
// change detection sees no difference and writes nothing. A field derived
// from the wall clock would differ on every poll and defeat that check.
func TestProgressMetadataIsStableWhilePositionIsUnchanged(t *testing.T) {
	eng := New()
	change := nativeApply{namespace: "public", table: "widgets", sql: "CREATE TABLE public.widgets (id bigint PRIMARY KEY)", steps: 3}
	tracker := newTestTracker(t)
	tracker.Start(3, progress.OperationAdmitting)
	tracker.StartStep(2, progress.OperationBrief, "CREATE INDEX widgets_name_idx ON public.widgets (name)")
	eng.claimProgress("task-a", progressResult(engine.StateRunning, "preflight", time.Now().Add(-time.Minute), change, ""), tracker, slog.Default(), false, nil)
	req := &engine.ProgressRequest{ResumeState: &engine.ResumeState{MigrationContext: "task-a"}}

	first, err := eng.Progress(t.Context(), req)
	require.NoError(t, err)
	time.Sleep(2 * time.Millisecond)
	second, err := eng.Progress(t.Context(), req)
	require.NoError(t, err)

	assert.Equal(t, map[string]string{
		"phase": "preflight", "step": "2", "steps_total": "3",
		"statement":          "CREATE INDEX widgets_name_idx ON public.widgets (name)",
		"executor_operation": string(progress.OperationBrief),
	}, first.Metadata)
	assert.Equal(t, first.Metadata, second.Metadata)
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
	eng.claimProgress("task-a", progressResult(engine.StateRunning, "preflight", time.Now(), change, ""), tracker, slog.Default(), false, nil)
	tracker.Start(2, progress.OperationAdmitting)
	tracker.StartStep(2, progress.OperationBrief, "CREATE INDEX widgets_name_idx ON public.widgets (name)")
	tracker.Finish(errors.New("boom"))

	terminal := progressResult(engine.StateFailed, "failed", time.Now(), change, "detail")
	require.NoError(t, executorProgressMetadata(t.Context(), tracker, terminal))
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

// An executor cancellation is this apply's operator cancellation only when
// the engine recorded that it sent the signal. The invalid leftover remains
// named so the next drive can recover it.
func TestApplyClassifiesRequestedConcurrentBuildCancel(t *testing.T) {
	scripted := newScriptedExecutor(func(tracker *progress.Tracker) error {
		tracker.Finish(errors.New("cancelled"))
		return &executor.InvalidIndexError{
			Schema: "public", Index: "users_email_idx", Table: "users",
			Build: executor.ErrCancelledExternally, Cleanup: executor.ErrBuildLeftInvalidIndex,
		}
	})
	eng := New()
	const key = "task-a"
	applyAlterUsers(t, eng, scripted, key, slog.New(slog.NewTextHandler(io.Discard, nil)))
	scripted.tracker(t)
	eng.mu.Lock()
	eng.progress[key].cancelRequested = true
	eng.mu.Unlock()
	scripted.release()

	require.Eventually(t, func() bool {
		return pollProgress(t, eng, key).State.IsTerminal()
	}, backgroundApplyDeadline, 10*time.Millisecond)
	terminal := pollProgress(t, eng, key)
	assert.Equal(t, engine.StateCancelled, terminal.State)
	assert.Contains(t, terminal.ErrorMessage, "users_email_idx")
}

// A build cancelled from outside SchemaBot — an operator's pg_cancel_backend,
// a connection reaper — is not a cancel nobody asked for: with no cancel
// recorded on the apply it is a retryable failure whose leftover index the
// next drive recovers.
func TestApplyTreatsAnUnrequestedCancellationAsAFailure(t *testing.T) {
	scripted := newScriptedExecutor(func(tracker *progress.Tracker) error {
		tracker.Finish(errors.New("cancelled"))
		return &executor.InvalidIndexError{
			Schema: "public", Index: "users_email_idx", Table: "users",
			Build: executor.ErrCancelledExternally, Cleanup: executor.ErrBuildLeftInvalidIndex,
		}
	})
	eng := New()
	const key = "task-a"
	applyAlterUsers(t, eng, scripted, key, slog.New(slog.NewTextHandler(io.Discard, nil)))
	scripted.tracker(t)
	scripted.release()

	require.Eventually(t, func() bool {
		return pollProgress(t, eng, key).State.IsTerminal()
	}, backgroundApplyDeadline, 10*time.Millisecond)
	terminal := pollProgress(t, eng, key)
	assert.Equal(t, engine.StateFailed, terminal.State)
	assert.True(t, terminal.Retryable)
	assert.Contains(t, terminal.ErrorMessage, "users_email_idx")
}

const concurrentIndexDDL = "CREATE INDEX CONCURRENTLY users_email_idx ON public.users (email)"

// applyConcurrentIndex accepts one concurrent index build on public.users
// through applyChange.
func applyConcurrentIndex(t *testing.T, eng *Engine, scripted *scriptedExecutor, key string) {
	t.Helper()
	applyChange(t, eng, scripted, key, concurrentIndexDDL, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// A cancel issued while the drive has not yet reserved a build backend cannot
// signal anything, so the engine ends the drive's own context; the executor
// returns the cancellation, the drive publishes it as this cancel, and the
// cancel is answered from that outcome.
func TestCancelEndsAConcurrentBuildThroughTheApplyDrive(t *testing.T) {
	scripted := newScriptedExecutor(func(*progress.Tracker) error {
		return errors.New("the executor must be ended by its context, not released")
	})
	eng := New()
	const key = "task-a"
	applyConcurrentIndex(t, eng, scripted, key)
	scripted.tracker(t)

	result, err := eng.Cancel(t.Context(), &engine.ControlRequest{ResumeState: &engine.ResumeState{MigrationContext: key}})
	require.NoError(t, err)
	assert.True(t, result.Accepted)

	terminal := pollProgress(t, eng, key)
	assert.Equal(t, engine.StateCancelled, terminal.State)
	assert.Equal(t, "Concurrent index build cancelled", terminal.ErrorMessage)
}

// A halt for shutdown ends a running concurrent build without recording a
// cancel: the outcome the drive publishes is the executor's failure, so the
// apply stays active for another driver to claim and recover the leftover.
func TestHaltForShutdownEndsConcurrentBuildsWithoutRecordingACancel(t *testing.T) {
	scripted := newScriptedExecutor(func(*progress.Tracker) error {
		return errors.New("the executor must be ended by its context, not released")
	})
	eng := New()
	const key = "task-a"
	applyConcurrentIndex(t, eng, scripted, key)
	scripted.tracker(t)

	ctx, cancel := context.WithTimeout(t.Context(), backgroundApplyDeadline)
	defer cancel()
	require.NoError(t, eng.HaltForShutdown(ctx))

	terminal := pollProgress(t, eng, key)
	assert.True(t, terminal.State.IsTerminal(), "the halted drive must have published before the halt returned")
	assert.NotEqual(t, engine.StateCancelled, terminal.State, "a halt is not an operator cancel")
}

// A halt for shutdown leaves plain DDL to finish, so a statement that will not
// return within the shutdown budget is reported as still holding the target
// rather than interrupted.
func TestHaltForShutdownLeavesPlainDDLToFinish(t *testing.T) {
	scripted := newScriptedExecutor(func(tracker *progress.Tracker) error {
		tracker.Finish(nil)
		return nil
	})
	eng := New()
	const key = "task-a"
	applyAlterUsers(t, eng, scripted, key, slog.New(slog.NewTextHandler(io.Discard, nil)))
	scripted.tracker(t)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err := eng.HaltForShutdown(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Contains(t, err.Error(), "still running on the target")
	assert.Equal(t, engine.StateRunning, pollProgress(t, eng, key).State, "plain DDL is not interrupted by a halt")

	scripted.release()
	require.Eventually(t, func() bool {
		return pollProgress(t, eng, key).State.IsTerminal()
	}, backgroundApplyDeadline, 10*time.Millisecond)
	assert.Equal(t, engine.StateCompleted, pollProgress(t, eng, key).State)
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
	eng.claimProgress("task-a", progressResult(engine.StateRunning, "preflight", time.Now(), change, ""), tracker, logger, false, nil)

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
		eng.claimProgress("task-b", progressResult(engine.StateRunning, "preflight", time.Now(), change, ""), newTestTracker(t), logger, false, nil)
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
	eng.claimProgress("task-a", progressResult(engine.StateRunning, "preflight", time.Now(), change, ""), tracker, slog.Default(), false, nil)
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

// indexProgressRow is one row of pg_stat_progress_create_index as pg-sprite
// scans it: phase, blocks, tuples, lockers, then the current locker's PID.
type indexProgressRow struct {
	phase                     string
	blocksDone, blocksTotal   uint64
	tuplesDone, tuplesTotal   uint64
	lockersTotal, lockersDone uint64
	currentLockerPID          int32
}

func (r indexProgressRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.phase
	*(dest[1].(*uint64)) = r.blocksDone
	*(dest[2].(*uint64)) = r.blocksTotal
	*(dest[3].(*uint64)) = r.tuplesDone
	*(dest[4].(*uint64)) = r.tuplesTotal
	*(dest[5].(*uint64)) = r.lockersTotal
	*(dest[6].(*uint64)) = r.lockersDone
	pid := r.currentLockerPID
	*(dest[7].(**int32)) = &pid
	return nil
}

// heapScanRow is a btree build part-way through its first heap scan.
var heapScanRow = indexProgressRow{
	phase: "building index: scanning table", blocksDone: 25, blocksTotal: 40,
	tuplesDone: 12, tuplesTotal: 30, lockersTotal: 4, lockersDone: 2, currentLockerPID: 31337,
}

// publishedIndexProgressSession answers every progress read with the row it
// currently holds, or with no row at all when the build has left the view.
type publishedIndexProgressSession struct {
	row    *indexProgressRow
	onRead func()
}

func (s *publishedIndexProgressSession) QueryRow(context.Context, string, ...any) pgx.Row {
	if s.onRead != nil {
		s.onRead()
	}
	if s.row == nil {
		return failingRow{err: pgx.ErrNoRows}
	}
	return *s.row
}

func claimRunningIndexBuild(t *testing.T, eng *Engine, session *publishedIndexProgressSession) *progress.Tracker {
	t.Helper()
	change := nativeApply{namespace: "public", table: "widgets", sql: "CREATE INDEX widgets_name_idx ON public.widgets (name)", steps: 2}
	tracker := newTestTracker(t)
	tracker.Start(2, progress.OperationAdmitting)
	tracker.StartStep(2, progress.OperationConcurrentIndex, "CREATE INDEX CONCURRENTLY widgets_name_idx ON public.widgets (name)")
	tracker.SetAttempt(3)
	tracker.SetConcurrentBuild(session, 42)
	eng.claimProgress("task-a", progressResult(engine.StateRunning, "running", time.Now(), change, ""), tracker, slog.Default(), false, nil)
	return tracker
}

func pollRunningIndexBuild(t *testing.T, eng *Engine) *engine.ProgressResult {
	t.Helper()
	got, err := eng.Progress(t.Context(), &engine.ProgressRequest{ResumeState: &engine.ResumeState{MigrationContext: "task-a"}})
	require.NoError(t, err)
	return got
}

// TestProgressReportsExecutorDetailAndBuildPercent proves a poll during a
// concurrent index build carries the executor's operation class and attempt,
// the server's phase and every counter of its progress row, and a percent
// that places the phase's block position on the whole build's scale, on the
// apply and its table alike.
func TestProgressReportsExecutorDetailAndBuildPercent(t *testing.T) {
	eng := New()
	row := heapScanRow
	claimRunningIndexBuild(t, eng, &publishedIndexProgressSession{row: &row})

	got := pollRunningIndexBuild(t, eng)
	assert.Equal(t, "concurrent-index-build", got.Metadata["executor_operation"])
	assert.Equal(t, "building index: scanning table", got.Metadata["server_phase"])
	assert.Equal(t, "3", got.Metadata["attempt"])
	assert.Equal(t, "25", got.Metadata["blocks_done"])
	assert.Equal(t, "40", got.Metadata["blocks_total"])
	assert.Equal(t, "12", got.Metadata["tuples_done"])
	assert.Equal(t, "30", got.Metadata["tuples_total"])
	assert.Equal(t, "2", got.Metadata["lockers_done"])
	assert.Equal(t, "4", got.Metadata["lockers_total"])
	assert.NotContains(t, got.Metadata, "current_locker_pid")
	assert.Equal(t, 25, got.Tables[0].Progress, "25 of 40 heap blocks inside the 0–40 band of the first heap scan")
	assert.Equal(t, got.Tables[0].Progress, got.Progress, "the apply and its table report the same percent")
}

// TestProgressReportsHonestZerosWhileWaitingForWriters proves a phase that
// publishes no block work still reports every counter: a build parked behind
// writers reports zero lockers cleared and zero blocks, not missing keys, and
// its percent is the phase's position on the whole build, not a division of
// the zero block total.
func TestProgressReportsHonestZerosWhileWaitingForWriters(t *testing.T) {
	eng := New()
	row := indexProgressRow{phase: "waiting for writers before validation", lockersTotal: 3}
	claimRunningIndexBuild(t, eng, &publishedIndexProgressSession{row: &row})

	got := pollRunningIndexBuild(t, eng)
	assert.Equal(t, "0", got.Metadata["lockers_done"])
	assert.Equal(t, "3", got.Metadata["lockers_total"])
	assert.Equal(t, "0", got.Metadata["blocks_done"])
	assert.Equal(t, "0", got.Metadata["blocks_total"])
	assert.Equal(t, "0", got.Metadata["tuples_done"])
	assert.Equal(t, "0", got.Metadata["tuples_total"])
	assert.Equal(t, 60, got.Tables[0].Progress, "the first heap scan is behind it, validation ahead of it")
	assert.Equal(t, 60, got.Progress)
}

// TestProgressKeepsLastKnownPercentWithoutABuildRow proves the derived
// percent is part of the last-known position: once a poll has placed the
// build on the whole scale, a later poll that finds no progress row — the
// build between phases, its session released, or the view unreadable —
// answers with that percent instead of the record's pre-execution zero, and
// the next row moves it forward again.
func TestProgressKeepsLastKnownPercentWithoutABuildRow(t *testing.T) {
	eng := New()
	row := heapScanRow
	session := &publishedIndexProgressSession{row: &row}
	claimRunningIndexBuild(t, eng, session)
	require.Equal(t, 25, pollRunningIndexBuild(t, eng).Tables[0].Progress)

	session.row = nil
	got := pollRunningIndexBuild(t, eng)
	assert.Equal(t, 25, got.Tables[0].Progress, "no row keeps the last-known percent")
	assert.Equal(t, 25, got.Progress)
	assert.NotContains(t, got.Metadata, "blocks_done", "no row publishes no counters")

	validation := indexProgressRow{phase: "index validation: scanning table", blocksDone: 40, blocksTotal: 40}
	session.row = &validation
	got = pollRunningIndexBuild(t, eng)
	assert.Equal(t, 95, got.Tables[0].Progress, "a completed validation scan stays below 100 until the apply completes")
	assert.Equal(t, "40", got.Metadata["blocks_done"])
}

// TestProgressNeverRegressesWithinABuild proves a row that derives a lower
// percent than the record already carries is a stale reading — a build
// moves through its phases in one direction — so the poll answers with the
// record's percent and the record keeps it, rather than the earlier reading
// being pinned as the last-known position.
func TestProgressNeverRegressesWithinABuild(t *testing.T) {
	eng := New()
	validation := indexProgressRow{phase: "index validation: scanning table", blocksDone: 40, blocksTotal: 40}
	session := &publishedIndexProgressSession{row: &validation}
	claimRunningIndexBuild(t, eng, session)
	require.Equal(t, 95, pollRunningIndexBuild(t, eng).Tables[0].Progress)

	stale := heapScanRow
	session.row = &stale
	got := pollRunningIndexBuild(t, eng)
	assert.Equal(t, 95, got.Tables[0].Progress, "a row from an earlier phase does not pull the percent back")
	assert.Equal(t, 95, got.Progress)
	assert.Equal(t, "25", got.Metadata["blocks_done"], "the counters still report the row that was read")

	session.row = nil
	got = pollRunningIndexBuild(t, eng)
	assert.Equal(t, 95, got.Tables[0].Progress, "the record kept the later position")
	assert.Equal(t, 95, got.Progress)
}

func TestProgressStraddlingATerminalPublishDoesNotFloorToTheRetiredRecord(t *testing.T) {
	eng := New()
	validation := indexProgressRow{phase: "index validation: scanning table", blocksDone: 40, blocksTotal: 40}
	session := &publishedIndexProgressSession{row: &validation}
	claimRunningIndexBuild(t, eng, session)
	require.Equal(t, 95, pollRunningIndexBuild(t, eng).Progress)

	change := nativeApply{namespace: "public", table: "widgets", sql: "CREATE INDEX widgets_name_idx ON public.widgets (name)", steps: 2}
	heapScan := heapScanRow
	session.row = &heapScan
	session.onRead = func() {
		session.onRead = nil
		eng.publishProgress("task-a", progressResult(engine.StateCompleted, "completed", time.Now(), change, ""), slog.Default())
	}
	got := pollRunningIndexBuild(t, eng)
	assert.Equal(t, engine.StateRunning, got.State)
	assert.Equal(t, 25, got.Progress)
	assert.Equal(t, 25, got.Tables[0].Progress)

	got = pollRunningIndexBuild(t, eng)
	assert.Equal(t, engine.StateCompleted, got.State)
	assert.Equal(t, 100, got.Progress)
	assert.Equal(t, 100, got.Tables[0].Progress)
}

func TestProgressStraddlingAReclaimDoesNotFloorToTheRetiredRecord(t *testing.T) {
	eng := New()
	validation := indexProgressRow{phase: "index validation: scanning table", blocksDone: 40, blocksTotal: 40}
	session := &publishedIndexProgressSession{row: &validation}
	tracker := claimRunningIndexBuild(t, eng, session)
	require.Equal(t, 95, pollRunningIndexBuild(t, eng).Progress)

	change := nativeApply{namespace: "public", table: "widgets", sql: "CREATE INDEX widgets_name_idx ON public.widgets (name)", steps: 2}
	heapScan := heapScanRow
	session.row = &heapScan
	session.onRead = func() {
		session.onRead = nil
		eng.claimProgress("task-a", progressResult(engine.StateRunning, "running", time.Now(), change, ""), tracker, slog.Default(), false, nil)
	}
	got := pollRunningIndexBuild(t, eng)
	assert.Equal(t, engine.StateRunning, got.State)
	assert.Equal(t, 25, got.Progress)
	assert.Equal(t, 25, got.Tables[0].Progress)

	session.row = nil
	got = pollRunningIndexBuild(t, eng)
	assert.Equal(t, engine.StateRunning, got.State)
	assert.Equal(t, 0, got.Progress)
	assert.Equal(t, 0, got.Tables[0].Progress)
}

// TestProgressKeepsPercentWhenTheBuildRowReadFails proves a tolerated
// progress-view failure does not reset the percent: the poll answers with
// the last-known percent alongside the last-known step and statement.
func TestProgressKeepsPercentWhenTheBuildRowReadFails(t *testing.T) {
	eng := New()
	row := heapScanRow
	session := &publishedIndexProgressSession{row: &row}
	tracker := claimRunningIndexBuild(t, eng, session)
	require.Equal(t, 25, pollRunningIndexBuild(t, eng).Tables[0].Progress)

	tracker.SetConcurrentBuild(&indexProgressSession{err: errors.New("connection reset by peer")}, 42)
	got := pollRunningIndexBuild(t, eng)
	assert.Equal(t, engine.StateRunning, got.State)
	assert.Equal(t, 25, got.Tables[0].Progress)
	assert.Equal(t, "2", got.Metadata["step"])
}

// TestExecutorProgressMetadataKeepsTerminalPercent proves a terminal result's
// percent is decided by its state: a tracker still answering with a partial
// build position folds its counters into the metadata but never pulls a
// completed apply below 100.
func TestExecutorProgressMetadataKeepsTerminalPercent(t *testing.T) {
	change := nativeApply{namespace: "public", table: "widgets", sql: "CREATE INDEX widgets_name_idx ON public.widgets (name)", steps: 2}
	tracker := newTestTracker(t)
	tracker.Start(2, progress.OperationAdmitting)
	tracker.StartStep(2, progress.OperationConcurrentIndex, "CREATE INDEX CONCURRENTLY widgets_name_idx ON public.widgets (name)")
	row := heapScanRow
	tracker.SetConcurrentBuild(&publishedIndexProgressSession{row: &row}, 42)
	terminal := progressResult(engine.StateCompleted, "completed", time.Now(), change, "")

	require.NoError(t, executorProgressMetadata(t.Context(), tracker, terminal))

	assert.Equal(t, 100, terminal.Tables[0].Progress)
	assert.Equal(t, 100, terminal.Progress)
	assert.Equal(t, "25", terminal.Metadata["blocks_done"])
	assert.Equal(t, "40", terminal.Metadata["blocks_total"])
}

// TestExecutorProgressMetadataBoundsTheServerPhase proves the server phase
// gets the same single-line treatment as every other operator-facing value
// in the metadata, and that an unrecognised phase leaves the percent alone.
func TestExecutorProgressMetadataBoundsTheServerPhase(t *testing.T) {
	tracker := newTestTracker(t)
	tracker.Start(1, progress.OperationAdmitting)
	tracker.StartStep(1, progress.OperationConcurrentIndex, "CREATE INDEX CONCURRENTLY widgets_name_idx ON public.widgets (name)")
	row := indexProgressRow{phase: "future phase |\nwith layout", blocksDone: 5, blocksTotal: 10}
	tracker.SetConcurrentBuild(&publishedIndexProgressSession{row: &row}, 42)
	result := &engine.ProgressResult{State: engine.StateRunning, Tables: []engine.TableProgress{{Progress: 7}}}

	require.NoError(t, executorProgressMetadata(t.Context(), tracker, result))

	assert.Equal(t, "future phase / with layout", result.Metadata["server_phase"])
	assert.Equal(t, 7, result.Tables[0].Progress)
	assert.Equal(t, "1", result.Metadata["step"], "a nil metadata map is allocated rather than written through")
}

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

	err := executorProgressMetadata(t.Context(), tracker, &engine.ProgressResult{Metadata: metadata})

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

	err := executorProgressMetadata(ctx, tracker, &engine.ProgressResult{Metadata: map[string]string{}})

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
	eng.claimProgress("task-a", progressResult(engine.StateRunning, "preflight", time.Now(), change, ""), tracker, logger, false, nil)

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
		// The CREATE TABLE committed before the names read failed, and
		// SchemaBot's retry re-runs the whole plan rather than the read
		// alone, so every retry collides with the table this apply created.
		executor.CodeCreateNamesUnverified: "a retry re-runs the committed CREATE TABLE",
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
// not by the concurrent index envelope — with room under it for the whole
// retry path. The bound is measured from the executor's side, so it is the
// ceiling the engine actually grants that must hold the retry path, not the
// constant it is seeded from: a constructor that seeded a shorter ceiling
// would still sit under the constant and still clear the statement budget,
// but would cancel a lock-contended statement mid-retry and turn its typed
// budget verdict into an external cancellation.
func TestOrdinaryApplyRunsUnderTheFixedCeiling(t *testing.T) {
	scripted := newScriptedExecutor(func(*progress.Tracker) error { return nil })
	eng := NewWithOptions(0, 3*time.Hour)
	applyAlterUsers(t, eng, scripted, "task-a", slog.New(slog.DiscardHandler))

	env := scripted.envelope(t)
	granted := env.deadline.Sub(env.observed)
	assert.False(t, env.change.concurrentIndex)
	assert.Zero(t, env.change.concurrentIndexMaxDuration, "the build bound is stamped only on a concurrent index apply")
	assert.LessOrEqual(t, granted, optimisticApplyCeiling)
	assert.Greater(t, granted, retryPathWorstCase(t), "the retry path must fit under the ceiling the engine actually grants")
}

// retryPathWorstCase is the longest legitimate run of the native retry path:
// every attempt the default retry policy allows, each at its full statement
// limit, plus the longest backoff between attempts. The policy comes from
// pg-sprite, so a dependency bump that widens it moves this value.
func retryPathWorstCase(t *testing.T) time.Duration {
	t.Helper()
	policy := executor.DefaultRetryPolicy()
	require.Positive(t, policy.MaxAttempts)
	attempts := time.Duration(policy.MaxAttempts)
	return attempts*optimisticStatementLimit + (attempts-1)*policy.MaxBackoff
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
//
// The executor's own catalog reads ahead of the build — resolving the
// target, inspecting the requested name, listing quarantined debris — are
// not in the census: they run in the build session, whose statement_timeout
// is already the bound, so the bound rather than the pool's limit is what
// ends one that stalls, and the drive names the option for it.
func TestConcurrentIndexHeadroomCoversSessionSetup(t *testing.T) {
	setup := dbconn.DefaultConnectTimeout + // pool dial
		dbconn.DefaultStatementTimeout + // preflight.CheckPrivileges
		dbconn.DefaultStatementTimeout + // preflight.CheckTable
		dbconn.DefaultStatementTimeout // preflight.LookupTargetFacts
	assert.GreaterOrEqual(t, concurrentIndexHeadroom, setup)
}

// TestConcurrentIndexMaximumIsTheServerStatementTimeoutCeiling pins the
// accepted build bound to the range statement_timeout itself can hold: the
// largest value PostgreSQL accepts at the top, and one millisecond, its
// resolution, at the bottom, below which the server's timer rounds to zero
// and switches off. The constructor's normalization around that range: a
// bound above it is clamped, a positive bound below it is raised, and a
// bound that is not positive adopts the default instead of a deadline that
// would end every build on arrival.
func TestConcurrentIndexMaximumIsTheServerStatementTimeoutCeiling(t *testing.T) {
	assert.Equal(t, time.Duration(math.MaxInt32)*time.Millisecond, MaxConcurrentIndexMaxDuration)
	assert.Less(t, MaxConcurrentIndexMaxDuration+concurrentIndexHeadroom, time.Duration(math.MaxInt64),
		"the ceiling built on the maximum must not overflow")
	assert.Equal(t, time.Millisecond, MinConcurrentIndexMaxDuration)

	assert.Equal(t, MaxConcurrentIndexMaxDuration, NewWithOptions(0, MaxConcurrentIndexMaxDuration+1).ConcurrentIndexMaxDuration())
	assert.Equal(t, MinConcurrentIndexMaxDuration, NewWithOptions(0, 500*time.Microsecond).ConcurrentIndexMaxDuration())
	assert.Equal(t, MinConcurrentIndexMaxDuration, NewWithOptions(0, MinConcurrentIndexMaxDuration).ConcurrentIndexMaxDuration())
	assert.Equal(t, DefaultConcurrentIndexMaxDuration, NewWithOptions(0, 0).ConcurrentIndexMaxDuration())
	assert.Equal(t, DefaultConcurrentIndexMaxDuration, NewWithOptions(0, -time.Second).ConcurrentIndexMaxDuration())
	assert.Equal(t, 36*time.Hour, NewWithOptions(0, 36*time.Hour).ConcurrentIndexMaxDuration())
}

// TestConcurrentIndexBoundsMatchTheExecutorsBudgetRange proves the engine's
// floor and ceiling are the executor's own, not a copy of them: the executor
// refuses a served budget one step outside either bound as unbounded and
// accepts a budget exactly on it. The executor validates the budget before
// it touches the pool or the statement, so the probe needs neither a target
// nor a real build; the accepted cases use a plain CREATE INDEX so the call
// stops at admission, one step past validation, instead of reaching the
// pool. Asserting the refusal's wording pins the value the executor holds,
// so a dependency bump that moves either bound fails here rather than in a
// refused apply on a configuration this engine had accepted.
func TestConcurrentIndexBoundsMatchTheExecutorsBudgetRange(t *testing.T) {
	const concurrent = "CREATE INDEX CONCURRENTLY users_email_idx ON public.users (email)"
	const plain = "CREATE INDEX users_email_idx ON public.users (email)"
	probe := func(sql string, overall time.Duration) error {
		_, err := executor.BuildIndexConcurrentlyWithProgress(t.Context(), nil, sql, executor.ConcurrentBudget{Overall: overall}, newTestTracker(t))
		return err
	}

	err := probe(concurrent, MinConcurrentIndexMaxDuration-time.Nanosecond)
	require.ErrorIs(t, err, executor.ErrUnboundedBudget)
	assert.ErrorContains(t, err, "at least 1ms")

	err = probe(concurrent, MaxConcurrentIndexMaxDuration+time.Millisecond)
	require.ErrorIs(t, err, executor.ErrUnboundedBudget)
	assert.ErrorContains(t, err, "at most "+MaxConcurrentIndexMaxDuration.String())

	for _, bound := range []time.Duration{MinConcurrentIndexMaxDuration, MaxConcurrentIndexMaxDuration} {
		err = probe(plain, bound)
		require.ErrorIs(t, err, executor.ErrNotConcurrentIndexBuild, "the probe must stop at admission, past validation")
		assert.NotErrorIs(t, err, executor.ErrUnboundedBudget, "a bound the engine accepts must be one the executor serves")
	}
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

// concurrentIndexCall records the envelope one executor entry point was
// handed: the budget and the deadline of the context it ran under.
type concurrentIndexCall struct {
	budget      executor.ConcurrentBudget
	deadline    time.Time
	hasDeadline bool
}

type concurrentIndexCalls struct {
	build, rebuild []concurrentIndexCall
}

// scriptConcurrentIndex swaps the concurrent index executor for one whose
// build and recovery are scripted, restoring the executor's own functions
// when the test ends. Each call's envelope is recorded before the script
// answers.
func scriptConcurrentIndex(t *testing.T, build func() error, rebuild func() error) *concurrentIndexCalls {
	t.Helper()
	calls := &concurrentIndexCalls{}
	record := func(ctx context.Context, budget executor.ConcurrentBudget) concurrentIndexCall {
		deadline, ok := ctx.Deadline()
		return concurrentIndexCall{budget: budget, deadline: deadline, hasDeadline: ok}
	}
	previous := concurrentIndex
	concurrentIndex = concurrentIndexExecutor{
		build: func(ctx context.Context, _ *pgxpool.Pool, _ string, budget executor.ConcurrentBudget, _ *progress.Tracker) (executor.IndexBuildReport, error) {
			calls.build = append(calls.build, record(ctx, budget))
			return executor.IndexBuildReport{}, build()
		},
		rebuild: func(ctx context.Context, _ *pgxpool.Pool, _ string, budget executor.ConcurrentBudget) (executor.IndexRecoveryReport, error) {
			calls.rebuild = append(calls.rebuild, record(ctx, budget))
			return executor.IndexRecoveryReport{}, rebuild()
		},
	}
	t.Cleanup(func() { concurrentIndex = previous })
	return calls
}

func concurrentIndexChange(bound time.Duration) nativeApply {
	return nativeApply{namespace: "public", table: "users",
		sql:             "CREATE INDEX CONCURRENTLY users_email_idx ON public.users (email)",
		concurrentIndex: true, concurrentIndexMaxDuration: bound}
}

// TestBuildIndexConcurrentlyRunsTheBuildUnderTheServedBound proves the plain
// build is handed the configured bound as its served statement budget, so
// the server's own timer ends an over-long build and reports exhaustion as
// the executor's typed verdict, rather than running caller-owned with no
// server-side statement timeout.
func TestBuildIndexConcurrentlyRunsTheBuildUnderTheServedBound(t *testing.T) {
	const bound = 3 * time.Hour
	calls := scriptConcurrentIndex(t, func() error { return nil }, func() error {
		return errors.New("no recovery is expected for a build that succeeded")
	})

	err := buildIndexConcurrently(t.Context(), nil, concurrentIndexChange(bound), newTestTracker(t), slog.New(slog.DiscardHandler))

	require.NoError(t, err)
	require.Len(t, calls.build, 1)
	assert.Equal(t, executor.ConcurrentBudget{Overall: bound}, calls.build[0].budget)
	assert.Empty(t, calls.rebuild)
}

// TestBuildIndexConcurrentlyRecoversUnderItsOwnCallerOwnedDeadline proves the
// recovery over an abandoned invalid index runs as one caller-owned envelope
// bounded by a fresh deadline of the configured bound: caller-owned, because
// the executor's served mode would spend the bound once on the drops and
// again on the build; a deadline of its own, because the apply ceiling the
// caller holds is the bound plus the setup headroom, and the recovery must
// end within the bound the operator configured, not the ceiling above it.
func TestBuildIndexConcurrentlyRecoversUnderItsOwnCallerOwnedDeadline(t *testing.T) {
	const bound = 3 * time.Hour
	abandoned := &executor.InvalidIndexError{Schema: "public", Index: "users_email_idx", Table: "users",
		Cleanup: executor.ErrAbandonedInvalidIndex}
	require.True(t, abandonedBeforeBuild(abandoned), "the scripted verdict must be one the drive recovers")
	calls := scriptConcurrentIndex(t, func() error { return abandoned }, func() error { return nil })
	ctx, cancel := context.WithTimeout(t.Context(), bound+concurrentIndexHeadroom)
	defer cancel()
	ceiling, _ := ctx.Deadline()

	before := time.Now()
	err := buildIndexConcurrently(ctx, nil, concurrentIndexChange(bound), newTestTracker(t), slog.New(slog.DiscardHandler))

	require.NoError(t, err)
	require.Len(t, calls.build, 1)
	require.Len(t, calls.rebuild, 1)
	recovery := calls.rebuild[0]
	assert.Equal(t, executor.ConcurrentBudget{CallerOwned: true}, recovery.budget)
	require.True(t, recovery.hasDeadline, "the recovery must run under a deadline")
	assert.WithinDuration(t, before.Add(bound), recovery.deadline, concurrentIndexHeadroom/2,
		"the recovery starts with the configured bound ahead of it, not the caller's ceiling")
	assert.True(t, recovery.deadline.Before(ceiling), "the recovery's deadline must fall inside the caller's ceiling")
}

// TestBuildIndexConcurrentlyNamesTheBoundThatEndedTheBuild proves the option
// is named on the drive's own error path, for each way the bound ends a
// build: the served build's typed statement budget verdict, and a raw
// cancellation of the executor's in-session catalog reads once the bound
// has elapsed. Both classify as the same refusal with the same remedy.
func TestBuildIndexConcurrentlyNamesTheBoundThatEndedTheBuild(t *testing.T) {
	const bound = 5 * time.Millisecond
	cases := []struct {
		name  string
		build func() error
	}{
		{
			name: "served build exhausts its statement budget",
			build: func() error {
				return &executor.BudgetError{Cause: executor.CauseStatement, Budget: bound}
			},
		},
		{
			name: "pre-build catalog read is cancelled once the bound has elapsed",
			build: func() error {
				// The drive names the bound on a raw cancellation only once
				// the bound has elapsed on its own clock, so real time must
				// pass here; overshooting is the safe direction.
				time.Sleep(2 * bound)
				return fmt.Errorf("resolve target: %w", &pgconn.PgError{Code: sqlstateQueryCanceled})
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := scriptConcurrentIndex(t, tc.build, func() error {
				return errors.New("no recovery is expected for a build the bound ended")
			})

			err := buildIndexConcurrently(t.Context(), nil, concurrentIndexChange(bound), newTestTracker(t), slog.New(slog.DiscardHandler))

			require.Error(t, err)
			assert.Empty(t, calls.rebuild)
			r := classifyRefusal(err, "users")
			require.NotNil(t, r, "a bound-ended build that left nothing is a refusal")
			assert.Equal(t, "concurrent-index-bound-exceeded", r.reason)
			assert.Contains(t, r.detail, "postgres.concurrent_index_max_duration (5ms)")
			assert.Contains(t, r.detail, "raise postgres.concurrent_index_max_duration and re-run")
		})
	}
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

	// The executor's typed verdict is authoritative on its own; the elapsed
	// time is not consulted for it.
	named := nameConcurrentIndexBound(fmt.Errorf("build: %w", statementBudget), bound, time.Second)
	var boundErr *concurrentIndexBoundError
	require.ErrorAs(t, named, &boundErr)
	assert.Equal(t, "the concurrent index build ran past postgres.concurrent_index_max_duration (36h0m0s) and was cancelled", boundErr.Error())
	var budgetErr *executor.BudgetError
	require.ErrorAs(t, named, &budgetErr)
	assert.Same(t, statementBudget, budgetErr)

	// A raw server cancellation of one of the executor's catalog reads ahead
	// of the build is the bound's once the bound has elapsed; the server's
	// error stays reachable for callers that read the code.
	cancelled := &pgconn.PgError{Code: sqlstateQueryCanceled, Message: "canceling statement due to statement timeout"}
	named = nameConcurrentIndexBound(fmt.Errorf("resolve target: %w", cancelled), bound, bound)
	require.ErrorAs(t, named, &boundErr)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, named, &pgErr)
	assert.Same(t, cancelled, pgErr)

	passThrough := []struct {
		name    string
		err     error
		elapsed time.Duration
	}{
		{name: "lock budget", err: &executor.BudgetError{Cause: executor.CauseLock, Budget: time.Second}, elapsed: 2 * bound},
		{name: "cancellation", err: executor.ErrCancelledByCaller, elapsed: 2 * bound},
		{name: "abandoned entry without a build failure", elapsed: 2 * bound,
			err: &executor.InvalidIndexError{Schema: "public", Index: "big_ref_idx", Table: "users", Cleanup: executor.ErrAbandonedInvalidIndex}},
		{name: "raw cancellation before the bound could fire", err: fmt.Errorf("resolve target: %w", cancelled), elapsed: bound - time.Millisecond},
		{name: "raw cancellation the executor attributed to the caller", elapsed: 2 * bound,
			err: fmt.Errorf("%w (after 1h): %w", executor.ErrCancelledByCaller, cancelled)},
		{name: "raw cancellation the executor attributed to an outside party", elapsed: 2 * bound,
			err: fmt.Errorf("%w (after 1h of a 36h budget): %w", executor.ErrCancelledExternally, cancelled)},
		{name: "server error under another code after the bound", elapsed: 2 * bound,
			err: fmt.Errorf("resolve target: %w", &pgconn.PgError{Code: "55P03"})},
	}
	for _, tc := range passThrough {
		t.Run(tc.name, func(t *testing.T) {
			assert.Same(t, tc.err, nameConcurrentIndexBound(tc.err, bound, tc.elapsed))
		})
	}
}

// TestRetryPathFitsUnderApplyCeiling pins the constant the engine's ceiling
// is seeded from against the retry path: every attempt the default retry
// policy allows, each at its full statement limit, plus the longest backoff
// between them, must finish before the ceiling cancels the session —
// otherwise a lock-contended native statement would surface as an external
// cancellation instead of the typed budget verdict. A dependency bump that
// widens the policy fails here instead of in an apply;
// TestOrdinaryApplyRunsUnderTheFixedCeiling pins the same relationship on
// the ceiling the drive actually grants.
func TestRetryPathFitsUnderApplyCeiling(t *testing.T) {
	assert.Less(t, retryPathWorstCase(t), optimisticApplyCeiling)
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
	eng.claimProgress("task-a", progressResult(engine.StateCompleted, "completed", time.Now(), change, ""), newTestTracker(t), slog.Default(), false, nil)

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
	eng.claimProgress("task-a", progressResult(engine.StateRunning, "preflight", time.Now(), changeA, ""), newTestTracker(t), slog.Default(), false, nil)
	eng.claimProgress("task-b", progressResult(engine.StateRunning, "preflight", time.Now(), changeB, ""), newTestTracker(t), slog.Default(), false, nil)

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
	eng.claimProgress("task-settled", progressResult(engine.StateCompleted, "completed", time.Now(), settled, ""), newTestTracker(t), slog.Default(), false, nil)
	eng.claimProgress("task-running", progressResult(engine.StateRunning, "preflight", time.Now(), running, ""), newTestTracker(t), slog.Default(), false, nil)

	eng.claimProgress("task-fresh", progressResult(engine.StateRunning, "preflight", time.Now(), fresh, ""), newTestTracker(t), slog.Default(), false, nil)

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
	eng.claimProgress("task-b", progressResult(engine.StateRunning, "preflight", time.Now(), changeB, ""), newTestTracker(t), slog.Default(), false, nil)

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
	eng.claimProgress("task-a", progressResult(engine.StateCompleted, "completed", time.Now(), changeA, ""), newTestTracker(t), slog.Default(), false, nil)
	eng.claimProgress("task-b", progressResult(engine.StateRunning, "preflight", time.Now(), changeB, ""), newTestTracker(t), slog.Default(), false, nil)

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
