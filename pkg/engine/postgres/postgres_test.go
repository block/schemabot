package postgres

import (
	"errors"
	"fmt"
	"testing"

	pgplan "github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/planner"
	"github.com/block/pg-sprite/pkg/preflight"
	"github.com/block/pg-sprite/pkg/router"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
)

func TestExecutionVerdict(t *testing.T) {
	tests := []struct {
		name       string
		version    int
		statement  pgplan.Statement
		wantMode   string
		wantReason string
	}{
		{
			name:       "unrecognized plan contract",
			version:    pgplan.FormatVersion + 1,
			statement:  pgplan.Statement{Disposition: router.DispositionExecute},
			wantMode:   engine.ExecutionModeBlocked,
			wantReason: `statement for table "users" has an unrecognized plan contract`,
		},
		{
			name: "native safe",
			statement: pgplan.Statement{Route: planner.RouteNative, Backend: router.BackendNative,
				Disposition: router.DispositionExecute, ExecSQL: []string{"ALTER TABLE public.users ADD COLUMN email text"}},
		},
		{
			name: "copy and swap",
			statement: pgplan.Statement{Route: planner.RouteCopyAndSwap, Backend: router.BackendCopyAndSwap,
				Disposition: router.DispositionUnavailable},
			wantMode:   engine.ExecutionModeBlocked,
			wantReason: `statement for table "users" requires copy-and-swap, which is unavailable`,
		},
		{
			name: "unavailable without copy-and-swap backend",
			statement: pgplan.Statement{Route: planner.RouteNative, Backend: router.BackendNative,
				Disposition: router.DispositionUnavailable},
			wantMode:   engine.ExecutionModeBlocked,
			wantReason: `statement for table "users" requires an execution path SchemaBot's PostgreSQL support does not provide yet`,
		},
		{
			name:       "rewrite required",
			statement:  pgplan.Statement{Disposition: router.DispositionRewriteRequired},
			wantMode:   engine.ExecutionModeBlocked,
			wantReason: `statement for table "users" must be rewritten into a form the engine can execute natively, then re-planned`,
		},
		{
			name:       "refuse",
			statement:  pgplan.Statement{Route: planner.RouteRefuse, Disposition: router.DispositionRefuse},
			wantMode:   engine.ExecutionModeBlocked,
			wantReason: `statement for table "users" is refused: it cannot be executed safely as written`,
		},
		{
			name:       "unrecognized disposition",
			statement:  pgplan.Statement{Disposition: router.Disposition("future")},
			wantMode:   engine.ExecutionModeBlocked,
			wantReason: `statement for table "users" has an unrecognized planner verdict`,
		},
		{
			name: "execute without authoritative steps",
			statement: pgplan.Statement{Route: planner.RouteNative, Backend: router.BackendNative,
				Disposition: router.DispositionExecute},
			wantMode:   engine.ExecutionModeBlocked,
			wantReason: `statement for table "users" has an unrecognized planner verdict`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			version := tt.version
			if version == 0 {
				version = pgplan.FormatVersion
			}
			mode, reason := executionVerdict(version, tt.statement, "users")
			assert.Equal(t, tt.wantMode, mode)
			assert.Equal(t, tt.wantReason, reason)
		})
	}
}

func TestTableChangesMapsDestructiveSafety(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	report.Statements = []pgplan.Statement{
		{
			SQL:         "ALTER TABLE public.users DROP COLUMN legacy",
			Route:       planner.RouteNative,
			Backend:     router.BackendNative,
			Disposition: router.DispositionExecute,
			ExecSQL:     []string{"ALTER TABLE public.users DROP COLUMN legacy"},
			Destructive: true,
		},
		{
			SQL:         "ALTER TABLE public.users ADD COLUMN email text",
			Route:       planner.RouteNative,
			Backend:     router.BackendNative,
			Disposition: router.DispositionExecute,
			ExecSQL:     []string{"ALTER TABLE public.users ADD COLUMN email text"},
		},
	}

	changes, err := tableChanges(report, parser)
	require.NoError(t, err)
	require.Len(t, changes, 2)
	assert.True(t, changes[0].IsUnsafe)
	assert.NotEmpty(t, changes[0].UnsafeReason)
	assert.False(t, changes[1].IsUnsafe)
	assert.Empty(t, changes[1].UnsafeReason)
}

func TestTableChangesRendersOrderedExecutionSteps(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	report.Statements = []pgplan.Statement{{
		SQL:         "ALTER TABLE public.users ADD COLUMN email text",
		Route:       planner.RouteNative,
		Backend:     router.BackendNative,
		Disposition: router.DispositionExecute,
		ExecSQL: []string{
			"ALTER TABLE public.users ADD COLUMN email text",
			"CREATE INDEX CONCURRENTLY users_email_idx ON public.users (email)",
		},
	}}

	changes, err := tableChanges(report, parser)
	require.NoError(t, err)
	require.Len(t, changes, 2)
	assert.Equal(t, "ALTER TABLE public.users ADD COLUMN email text", changes[0].DDL)
	assert.Equal(t, ddl.StatementAlterTable, changes[0].Operation)
	assert.Equal(t, "CREATE INDEX CONCURRENTLY users_email_idx ON public.users (email)", changes[1].DDL)
	assert.Equal(t, ddl.StatementCreateIndex, changes[1].Operation)
	assert.Empty(t, changes[0].ExecutionMode)
	assert.Empty(t, changes[1].ExecutionMode)
}

func TestTableChangesMixedVerdictsFailClosedPerStatement(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	report.Statements = []pgplan.Statement{
		{SQL: "ALTER TABLE public.users ADD COLUMN email text", Route: planner.RouteNative,
			Backend: router.BackendNative, Disposition: router.DispositionExecute,
			ExecSQL: []string{"ALTER TABLE public.users ADD COLUMN email text"}},
		{SQL: "ALTER TABLE public.users ALTER COLUMN email TYPE bigint", Route: planner.RouteCopyAndSwap,
			Backend: router.BackendCopyAndSwap, Disposition: router.DispositionUnavailable},
	}

	changes, err := tableChanges(report, parser)
	require.NoError(t, err)
	require.Len(t, changes, 2)
	assert.Empty(t, changes[0].ExecutionMode)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[1].ExecutionMode)
	assert.NotContains(t, changes[1].ModeReason, changes[1].DDL)
}

func TestTableChangesRejectsUnparseablePlanSQL(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	report.Statements = []pgplan.Statement{{SQL: "not ddl", Disposition: router.DispositionRefuse}}

	_, err = tableChanges(report, parser)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `classify planned statement for table "users"`)
}

// TestPrivilegeBlockReason pins the boundary between typed preflight refusals
// (rendered as blocked plan verdicts with provisioning detail) and
// operational failures (the plan must fail instead of blocking the change).
func TestPrivilegeBlockReason(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		refused    bool
		wantReason []string
	}{
		{
			name: "privilege error blocks with grant, check, and hint",
			err: fmt.Errorf("check privileges: %w", &preflight.PrivilegeError{
				Tier:  preflight.TierAlterInPlace,
				Check: "pg_has_role(limited, app_owner, 'USAGE')",
				Grant: `GRANT "app_owner" TO "limited" WITH INHERIT TRUE`,
				Hint:  "membership must be inheritable",
			}),
			refused: true,
			wantReason: []string{
				"in-place ALTER TABLE",
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
				Grant: "GRANT \"app|owner\" TO \"evil\nrole\"\x1b[31m",
				Hint:  "membership\tmust be inheritable",
			}),
			refused: true,
			wantReason: []string{
				`GRANT "app/owner" TO "evil role"`,
				"pg_has_role(evil role, app/owner, 'USAGE')",
				"membership must be inheritable",
			},
		},
		{
			name:       "missing table blocks with a re-plan instruction",
			err:        fmt.Errorf("check privileges: %w", preflight.ErrTableNotFound),
			refused:    true,
			wantReason: []string{"re-plan against the current schema"},
		},
		{
			name:       "non-table relation blocks with the relation kind cause",
			err:        fmt.Errorf("check privileges: %w", preflight.ErrNotTable),
			refused:    true,
			wantReason: []string{"not an ordinary or partitioned table"},
		},
		{
			name: "untyped error is operational, not a refusal",
			err:  errors.New("dial tcp: connection refused"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, refused := privilegeBlockReason(tt.err, "users")
			assert.Equal(t, tt.refused, refused)
			for _, want := range tt.wantReason {
				assert.Contains(t, reason, want)
			}
		})
	}
}

// TestBlockMissingPrivilegesSkipsFullyBlockedPlans proves the privilege check
// never touches the target when the plan carries no executable steps: a
// blocked verdict already withholds apply, so there is no access to verify.
func TestBlockMissingPrivilegesSkipsFullyBlockedPlans(t *testing.T) {
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	changes := []engine.TableChange{{
		Table:         "users",
		DDL:           "CREATE TABLE public.users (id bigint PRIMARY KEY)",
		ExecutionMode: engine.ExecutionModeBlocked,
	}}

	require.NoError(t, blockMissingPrivileges(t.Context(), nil, report, changes))
	assert.Equal(t, engine.ExecutionModeBlocked, changes[0].ExecutionMode)
}

// TestBlockMissingPrivilegesRequiresTargetTable proves a report that carries
// executable steps without naming its target fails the plan closed: the
// privilege check cannot answer for an unnamed table, and an executable plan
// must never be produced while the answer is unknown.
func TestBlockMissingPrivilegesRequiresTargetTable(t *testing.T) {
	report := pgplan.NewReport(pgplan.SourceDiff)
	changes := []engine.TableChange{{
		Table: "users",
		DDL:   "ALTER TABLE public.users ADD COLUMN email text",
	}}

	err := blockMissingPrivileges(t.Context(), nil, report, changes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "names no target table")
}

func TestPlanRejectsInvalidInputsBeforeConnecting(t *testing.T) {
	eng := New()

	_, err := eng.Plan(t.Context(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "request is required")

	_, err = eng.Plan(t.Context(), &engine.PlanRequest{Database: "app"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DSN credentials are required")
}

// Every lifecycle control declines with a typed unsupported-operation error:
// PostgreSQL DDL runs each statement as a single transactional statement with
// no engine phase to pause, resume, swap, revert, or retune. The typed decline
// is what lets the durable control path resolve a request terminally instead
// of retrying a rejection forever while the schema change keeps executing.
func TestLifecycleControlsDeclineAsUnsupported(t *testing.T) {
	eng := New()

	tests := []struct {
		name string
		call func(t *testing.T) error
	}{
		{"stop", func(t *testing.T) error {
			result, err := eng.Stop(t.Context(), &engine.ControlRequest{})
			assert.Nil(t, result)
			return err
		}},
		{"cancel", func(t *testing.T) error {
			result, err := eng.Cancel(t.Context(), &engine.ControlRequest{})
			assert.Nil(t, result)
			return err
		}},
		{"start", func(t *testing.T) error {
			result, err := eng.Start(t.Context(), &engine.ControlRequest{})
			assert.Nil(t, result)
			return err
		}},
		{"cutover", func(t *testing.T) error {
			result, err := eng.Cutover(t.Context(), &engine.ControlRequest{})
			assert.Nil(t, result)
			return err
		}},
		{"revert", func(t *testing.T) error {
			result, err := eng.Revert(t.Context(), &engine.ControlRequest{})
			assert.Nil(t, result)
			return err
		}},
		{"skip-revert", func(t *testing.T) error {
			result, err := eng.SkipRevert(t.Context(), &engine.ControlRequest{})
			assert.Nil(t, result)
			return err
		}},
		{"volume", func(t *testing.T) error {
			result, err := eng.Volume(t.Context(), &engine.VolumeRequest{})
			assert.Nil(t, result)
			return err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call(t)
			require.Error(t, err)
			assert.True(t, engine.IsUnsupportedOperation(err),
				"the decline must be typed so durable control consumers resolve it terminally")
		})
	}
}
