package postgres

import (
	"testing"

	pgplan "github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/planner"
	"github.com/block/pg-sprite/pkg/preflight"
	"github.com/block/pg-sprite/pkg/router"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
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

	changes, _, err := tableChanges(report, parser)
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
	exists := true
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	report.TableExists = &exists
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

	changes, tiers, err := tableChanges(report, parser)
	require.NoError(t, err)
	require.Len(t, changes, 2)
	assert.Equal(t, "ALTER TABLE public.users ADD COLUMN email text", changes[0].DDL)
	assert.Equal(t, ddl.StatementAlterTable, changes[0].Operation)
	assert.Equal(t, "CREATE INDEX CONCURRENTLY users_email_idx ON public.users (email)", changes[1].DDL)
	assert.Equal(t, ddl.StatementCreateIndex, changes[1].Operation)
	assert.Empty(t, changes[0].ExecutionMode)
	assert.Empty(t, changes[1].ExecutionMode)
	require.Len(t, tiers, 2)
	assert.Equal(t, preflight.TierAlterInPlace, tiers[0],
		"an in-place ALTER needs only owner membership, never the index tier's schema CREATE")
	assert.Equal(t, preflight.TierIndexBuild, tiers[1])
}

func TestTableChangesCollapsesGreenfieldCreateSet(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	exists := false
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "widgets"
	report.TableExists = &exists
	report.Statements = []pgplan.Statement{
		{SQL: "CREATE TABLE public.widgets (id bigint PRIMARY KEY, name text)", Route: planner.RouteNative, Backend: router.BackendNative, Disposition: router.DispositionExecute, ExecSQL: []string{"CREATE TABLE public.widgets (id bigint PRIMARY KEY, name text)"}},
		{SQL: "CREATE UNIQUE INDEX widgets_name_key ON public.widgets (name)", Route: planner.RouteNative, Backend: router.BackendNative, Disposition: router.DispositionExecute, ExecSQL: []string{"CREATE UNIQUE INDEX CONCURRENTLY widgets_name_key ON public.widgets (name)"}},
		{SQL: "CREATE INDEX widgets_id_idx ON public.widgets (id)", Route: planner.RouteNative, Backend: router.BackendNative, Disposition: router.DispositionExecute, ExecSQL: []string{"CREATE INDEX CONCURRENTLY widgets_id_idx ON public.widgets (id)"}},
	}

	changes, tiers, err := tableChanges(report, parser)
	require.NoError(t, err)
	require.Len(t, changes, 1)
	assert.Equal(t, "CREATE TABLE public.widgets (id bigint PRIMARY KEY, name text);\nCREATE UNIQUE INDEX widgets_name_key ON public.widgets (name);\nCREATE INDEX widgets_id_idx ON public.widgets (id)", changes[0].DDL)
	assert.Equal(t, "widgets", changes[0].Table)
	assert.Equal(t, ddl.StatementCreateTable, changes[0].Operation)
	assert.False(t, changes[0].IsUnsafe)
	assert.Empty(t, changes[0].ExecutionMode)
	require.Equal(t, []preflight.Tier{preflight.TierCreateTable}, tiers)
}

func TestTableChangesKeepsGreenfieldVerdictsPerStatement(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	exists := false
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "widgets"
	report.TableExists = &exists
	report.Statements = []pgplan.Statement{
		{SQL: "CREATE TABLE public.widgets (id bigint PRIMARY KEY, name text)", Route: planner.RouteNative, Backend: router.BackendNative, Disposition: router.DispositionExecute, ExecSQL: []string{"CREATE TABLE public.widgets (id bigint PRIMARY KEY, name text)"}},
		{SQL: "CREATE INDEX widgets_name_idx ON public.widgets (name)", Route: planner.RouteCopyAndSwap, Backend: router.BackendCopyAndSwap, Disposition: router.DispositionUnavailable},
		{SQL: "CREATE INDEX widgets_id_idx ON public.widgets (id)", Route: planner.RouteNative, Backend: router.BackendNative, Disposition: router.DispositionExecute, ExecSQL: []string{"CREATE INDEX CONCURRENTLY widgets_id_idx ON public.widgets (id)"}},
	}

	changes, tiers, err := tableChanges(report, parser)
	require.NoError(t, err)
	require.Len(t, changes, 3)
	blockAbsentTableDependents(changes, tiers, report.Table)
	assert.Empty(t, changes[0].ExecutionMode)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[1].ExecutionMode)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[2].ExecutionMode)
	assert.Contains(t, changes[2].ModeReason, "depends on the statement that creates it")
}

func TestIsGreenfieldCreateSetRejectsUnsafeStatements(t *testing.T) {
	exists := false
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "widgets"
	report.TableExists = &exists
	report.Statements = []pgplan.Statement{{SQL: "CREATE TABLE public.widgets (id bigint PRIMARY KEY)"}}

	tests := []struct {
		name     string
		verdicts []string
		mutate   func(*pgplan.Report)
	}{
		{name: "destructive verdict", verdicts: []string{"destructive statement"}},
		{name: "destructive term", verdicts: []string{""}, mutate: func(report *pgplan.Report) {
			report.Statements[0].Destructive = true
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := report
			candidate.Statements = append([]pgplan.Statement(nil), report.Statements...)
			if tt.mutate != nil {
				tt.mutate(&candidate)
			}
			assert.False(t, isGreenfieldCreateSet(candidate, tt.verdicts))
		})
	}
}

func TestGreenfieldCreateSetRejectsNonCreateTier(t *testing.T) {
	err := ensureGreenfieldCreateTier("widgets", preflight.TierIndexBuild)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected create a new table")
}

func TestGreenfieldCreateSetRejectsIndexForAnotherTable(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "widgets"
	report.Statements = []pgplan.Statement{
		{SQL: "CREATE TABLE public.widgets (id bigint PRIMARY KEY)"},
		{SQL: "CREATE INDEX gadgets_id_idx ON public.gadgets (id)"},
	}

	_, _, err = greenfieldCreateSet(report, parser)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `creates an index on table "public"."gadgets", not CREATE TABLE target "public"."widgets"`)
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

	changes, _, err := tableChanges(report, parser)
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

	_, _, err = tableChanges(report, parser)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `classify planned statement for table "users"`)
}

// TestBlockMissingPrivilegesSkipsMissingTable proves that executable steps on
// a table the target provably does not have are blocked with their dependency
// on the table's creation as the reason — never with a privilege probe's
// "table not found", which reads as a re-plan instruction no re-plan can
// satisfy. The nil pool proves no probe runs.
func TestBlockMissingPrivilegesSkipsMissingTable(t *testing.T) {
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	exists := false
	report.TableExists = &exists
	changes := []engine.TableChange{
		{
			Table:         "users",
			DDL:           "CREATE TABLE public.users (id bigint PRIMARY KEY)",
			ExecutionMode: engine.ExecutionModeBlocked,
			ModeReason:    "creation shape verdict",
		},
		{
			Table: "users",
			DDL:   "CREATE INDEX CONCURRENTLY idx_users_email ON public.users (email)",
		},
	}

	changes, err := blockMissingPrivileges(t.Context(), nil, report, changes, []preflight.Tier{0, preflight.TierIndexBuild})
	require.NoError(t, err)
	assert.Equal(t, "creation shape verdict", changes[0].ModeReason)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[1].ExecutionMode)
	assert.Contains(t, changes[1].ModeReason, `table "users" does not exist on the target`)
	assert.Contains(t, changes[1].ModeReason, "depends on the statement that creates it")
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

	changes, err := blockMissingPrivileges(t.Context(), nil, report, changes, []preflight.Tier{0})
	require.NoError(t, err)
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

	_, err := blockMissingPrivileges(t.Context(), nil, report, changes, []preflight.Tier{preflight.TierAlterInPlace})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "names no target table")
}

// TestBlockMissingPrivilegesRequiresTargetTableWhenAbsent proves the unnamed-
// target guard fires before dependent-step blocking rewrites verdicts: a
// report that provably lacks its table but carries executable steps and no
// target name must fail the plan closed, never launder the missing name into
// a plan whose every step was blocked for a fabricated dependency.
func TestBlockMissingPrivilegesRequiresTargetTableWhenAbsent(t *testing.T) {
	report := pgplan.NewReport(pgplan.SourceDiff)
	exists := false
	report.TableExists = &exists
	changes := []engine.TableChange{
		{
			Table:         "users",
			DDL:           "CREATE TABLE public.users (id bigint PRIMARY KEY)",
			ExecutionMode: engine.ExecutionModeBlocked,
			ModeReason:    "creation shape verdict",
		},
		{
			Table: "users",
			DDL:   "CREATE INDEX CONCURRENTLY idx_users_email ON public.users (email)",
		},
	}

	_, err := blockMissingPrivileges(t.Context(), nil, report, changes, []preflight.Tier{0, preflight.TierIndexBuild})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "names no target table")
}

// TestBlockMissingPrivilegesRequiresMatchingTiers proves a tier slice that
// does not pair one-to-one with the planned changes fails the plan closed
// instead of guessing which step needs which access.
func TestBlockMissingPrivilegesRequiresMatchingTiers(t *testing.T) {
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	changes := []engine.TableChange{{
		Table: "users",
		DDL:   "ALTER TABLE public.users ADD COLUMN email text",
	}}

	_, err := blockMissingPrivileges(t.Context(), nil, report, changes, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "privilege tiers")
}

// TestBlockChangesAtTier proves a privilege refusal lands only on the steps
// that need the refused tier: steps at other tiers and steps already carrying
// a verdict keep their own reasons.
func TestBlockChangesAtTier(t *testing.T) {
	changes := []engine.TableChange{
		{Table: "users", DDL: "ALTER TABLE public.users ADD COLUMN email text"},
		{Table: "users", DDL: "CREATE INDEX CONCURRENTLY idx_users_email ON public.users (email)"},
		{Table: "users", DDL: "CREATE TABLE public.users (id bigint PRIMARY KEY)",
			ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "creation shape verdict"},
	}
	tiers := []preflight.Tier{preflight.TierAlterInPlace, preflight.TierIndexBuild, 0}

	blockChangesAtTier(changes, tiers, preflight.TierIndexBuild, "index tier refused")

	assert.Empty(t, changes[0].ExecutionMode, "a step at another tier must keep its own verdict")
	assert.Equal(t, engine.ExecutionModeBlocked, changes[1].ExecutionMode)
	assert.Equal(t, "index tier refused", changes[1].ModeReason)
	assert.Equal(t, "creation shape verdict", changes[2].ModeReason,
		"an existing verdict must never be overwritten")
}

// A fully blocked plan does not need a table-size answer. The nil pool proves
// the guard returns before catalog access while preserving the prior verdict.
func TestBlockOversizedTableSkipsFullyBlockedPlans(t *testing.T) {
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	changes := []engine.TableChange{{
		Table:         "users",
		DDL:           "CREATE TABLE public.users (id bigint PRIMARY KEY)",
		ExecutionMode: engine.ExecutionModeBlocked,
	}}

	changes, err := blockOversizedTable(t.Context(), nil, report, changes, 1)
	require.NoError(t, err)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[0].ExecutionMode)
}

// An executable step without a named target fails the plan closed because a
// size check cannot answer for an unidentified table.
func TestBlockOversizedTableRequiresTargetTable(t *testing.T) {
	report := pgplan.NewReport(pgplan.SourceDiff)
	changes := []engine.TableChange{{
		Table: "users",
		DDL:   "ALTER TABLE public.users ADD COLUMN email text",
	}}

	_, err := blockOversizedTable(t.Context(), nil, report, changes, 1)
	require.Error(t, err)
	assert.EqualError(t, err, "plan report carries executable steps but names no target table")
}

// An operational size-check failure fails the plan closed instead of leaving
// the step executable. A non-positive limit is rejected before catalog access.
func TestBlockOversizedTableFailsClosedOnCheckError(t *testing.T) {
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	changes := []engine.TableChange{{
		Table: "users",
		DDL:   "ALTER TABLE public.users ADD COLUMN email text",
	}}

	_, err := blockOversizedTable(t.Context(), nil, report, changes, -1)
	require.Error(t, err)
	assert.EqualError(t, err, `check size for table "users": size limit must be positive, got -1`)
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

// The PostgreSQL engine runs each statement in a goroutine of this process and
// claims the tracked progress before Apply returns, so it declares its work
// registration synchronous. A driver reads that declaration to decide whether
// a pending progress report about work it believes is in flight is conclusive.
func TestRegistersWorkSynchronously(t *testing.T) {
	eng := New()

	assert.True(t, eng.RegistersWorkSynchronously(),
		"the engine claims the tracked schema change before Apply returns")
	assert.True(t, engine.RegistersWorkSynchronously(eng),
		"the package helper resolves the engine's declaration")
}

// A zero ceiling means unset and adopts the default, so a zero-valued client
// config preserves the stock ceiling instead of disabling the size guard.
func TestNewWithTableSizeLimitTreatsZeroAsUnset(t *testing.T) {
	assert.Equal(t, DefaultNativeSafeTableSizeLimitBytes, NewWithTableSizeLimit(0).TableSizeLimit())
	assert.Equal(t, int64(42), NewWithTableSizeLimit(42).TableSizeLimit())
}

// The undeclared-table reason offers only remedies the operator can carry
// out, and which remedy applies depends on where the table's foreign key
// constraints live: none, on the table itself, on other tables, or both. Every
// constraint on either side is named so the operator knows what a drop or a
// hand-written file has to account for.
func TestUndeclaredTableReasonNamesRemedyPerForeignKeySide(t *testing.T) {
	const preamble = `table "orders" exists on the target but no schema file in namespace "public" declares it; converging would drop the table, which SchemaBot's PostgreSQL support never executes`
	tests := []struct {
		name string
		live liveTable
		want string
	}{
		{
			name: "no foreign keys on either side",
			live: liveTable{name: "orders"},
			want: preamble + " — declare the table in a schema file to keep it under management, or drop it through a separately reviewed process",
		},
		{
			name: "owns one foreign key",
			live: liveTable{name: "orders", foreignKeys: []string{"orders_user_id_fkey"}},
			want: preamble + `; the table cannot be declared while it carries foreign key constraint(s) "orders_user_id_fkey", which schema files do not support — drop the table, or remove its foreign keys before declaring it, through a separately reviewed process`,
		},
		{
			name: "owns several foreign keys",
			live: liveTable{name: "orders", foreignKeys: []string{"orders_shop_id_fkey", "orders_user_id_fkey"}},
			want: preamble + `; the table cannot be declared while it carries foreign key constraint(s) "orders_shop_id_fkey", "orders_user_id_fkey", which schema files do not support — drop the table, or remove its foreign keys before declaring it, through a separately reviewed process`,
		},
		{
			name: "referenced by foreign keys on other tables",
			live: liveTable{name: "orders", referencedBy: []string{"invoices_order_id_fkey", "shipments_order_id_fkey"}},
			want: preamble + `; foreign key constraint(s) "invoices_order_id_fkey", "shipments_order_id_fkey" on other tables reference it, and schema files do not support foreign keys, so the schema pull cannot write a file for it — declare the table by hand in a schema file to keep it under management, or drop it together with the referencing constraints through a separately reviewed process`,
		},
		{
			name: "owns and is referenced by foreign keys",
			live: liveTable{name: "orders", foreignKeys: []string{"orders_user_id_fkey"}, referencedBy: []string{"invoices_order_id_fkey"}},
			want: preamble + `; the table cannot be declared while it carries foreign key constraint(s) "orders_user_id_fkey", which schema files do not support, and foreign key constraint(s) "invoices_order_id_fkey" on other tables reference it — drop the table together with the referencing constraints, or remove its own foreign keys and declare it by hand, through a separately reviewed process`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, undeclaredTableReason("public", tt.live))
		})
	}
}

func TestPullNamespacesRejectsReservedSchema(t *testing.T) {
	_, err := pullNamespaces(t.Context(), nil, "pg_catalog")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `schema "pg_catalog" is reserved and cannot be pulled`)
}

func TestPullSchemaRequiresConfiguredCredentials(t *testing.T) {
	_, err := New().PullSchema(t.Context(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "request is required")

	_, err = New().PullSchema(t.Context(), &ternv1.PullSchemaRequest{Database: "orders"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DSN credentials are required")
}

func TestPullSchemaRejectsDetailedCatalog(t *testing.T) {
	_, err := New().PullSchema(t.Context(), &ternv1.PullSchemaRequest{
		Database:      "orders",
		CatalogDetail: ternv1.PullCatalogDetail_PULL_CATALOG_DETAIL_DETAILED,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "catalog detail")
	assert.Contains(t, err.Error(), "unsupported; use basic")
}
