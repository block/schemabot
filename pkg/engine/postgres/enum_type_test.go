package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/block/pg-sprite/pkg/executor"
	pgplan "github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/planner"
	"github.com/block/pg-sprite/pkg/preflight"
	"github.com/block/pg-sprite/pkg/router"
	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
)

// An enum type's plan renders only label additions to that type in its own
// schema. A statement of any other kind, or one that targets a different
// type or the same type name in another schema, fails the plan instead of
// reaching review under a name SchemaBot never checked.
func TestEnumTypeChangesFailsClosedOnUnexpectedStatements(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)

	t.Run("a statement that is not a label addition", func(t *testing.T) {
		report := pgplan.Report{Type: "order_status", Statements: []pgplan.Statement{
			{Kind: schemadiff.ChangeAddColumn, SQL: "ALTER TABLE public.orders ADD COLUMN note text"},
		}}
		_, err := enumTypeChanges("app", "public", report, parser)
		require.ErrorContains(t, err, `plan for enum type "order_status" holds a statement of kind add-column`)
	})

	t.Run("a label addition to another type", func(t *testing.T) {
		report := pgplan.Report{Type: "order_status", Statements: []pgplan.Statement{
			{Kind: schemadiff.ChangeAddEnumValue, SQL: "ALTER TYPE public.payment_state ADD VALUE 'refunded'"},
		}}
		_, err := enumTypeChanges("app", "public", report, parser)
		require.EqualError(t, err, `planned statement for enum type "order_status" classified as ALTER TYPE on "payment_state"`)
	})

	t.Run("a label addition to the same type name in another schema", func(t *testing.T) {
		report := pgplan.Report{Type: "order_status", Statements: []pgplan.Statement{
			{Kind: schemadiff.ChangeAddEnumValue, SQL: "ALTER TYPE billing.order_status ADD VALUE 'refunded'"},
		}}
		_, err := enumTypeChanges("app", "public", report, parser)
		require.EqualError(t, err, `planned statement for enum type "order_status" in schema "public" adds a label to "order_status" in schema "billing"`)
	})

	t.Run("a converged type renders nothing", func(t *testing.T) {
		changes, err := enumTypeChanges("app", "public", pgplan.Report{Type: "order_status"}, parser)
		require.NoError(t, err)
		assert.Empty(t, changes)
	})
}

// enumLabelStatement is a label addition as the planner routes it: native,
// executed as written.
func enumLabelStatement(sql string) pgplan.Statement {
	return pgplan.Statement{Kind: schemadiff.ChangeAddEnumValue, SQL: sql, ExecSQL: []string{sql},
		Route: planner.RouteNative, Backend: router.BackendNative, Disposition: router.DispositionExecute}
}

// A label addition the planner routes native is executable, in planned
// order. One routed anywhere else is blocked with a reason that names the
// type and the label, never applied.
func TestEnumTypeChangesTakeThePlannerVerdict(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)

	t.Run("native label additions are executable in order", func(t *testing.T) {
		report := pgplan.Report{FormatVersion: pgplan.FormatVersion, Schema: "public", Type: "order_status", Statements: []pgplan.Statement{
			enumLabelStatement("ALTER TYPE public.order_status ADD VALUE 'paid' BEFORE 'shipped'"),
			enumLabelStatement("ALTER TYPE public.order_status ADD VALUE 'delivered'"),
		}}
		changes, err := enumTypeChanges("app", "public", report, parser)
		require.NoError(t, err)
		require.Len(t, changes, 2)
		for i, want := range []string{
			"ALTER TYPE public.order_status ADD VALUE 'paid' BEFORE 'shipped'",
			"ALTER TYPE public.order_status ADD VALUE 'delivered'",
		} {
			assert.Equal(t, "order_status", changes[i].Table)
			assert.Equal(t, ddl.StatementAlterType, changes[i].Operation)
			assert.Equal(t, want, changes[i].DDL)
			assert.Empty(t, changes[i].ExecutionMode, "a native label addition runs on the engine's default path")
			assert.Empty(t, changes[i].ModeReason)
		}
	})

	t.Run("a label addition routed elsewhere is blocked", func(t *testing.T) {
		statement := enumLabelStatement("ALTER TYPE public.order_status ADD VALUE 'paid'")
		statement.Disposition = router.DispositionUnavailable
		report := pgplan.Report{FormatVersion: pgplan.FormatVersion, Schema: "public", Type: "order_status", Statements: []pgplan.Statement{statement}}
		changes, err := enumTypeChanges("app", "public", report, parser)
		require.NoError(t, err)
		require.Len(t, changes, 1)
		assert.Equal(t, engine.ExecutionModeBlocked, changes[0].ExecutionMode)
		assert.Equal(t, `adding label "paid" to enum type "order_status" needs an execution path SchemaBot's PostgreSQL support does not provide yet`, changes[0].ModeReason)
	})
}

// The plan proves the engine role may alter the type before it offers the
// label additions as executable. A missing grant blocks every executable
// step and names the grant; a type that is no longer an enum, or a check
// that cannot answer, fails the plan rather than leave the steps
// executable while access is unknown. A plan with nothing executable is
// not probed.
func TestBlockEnumTypeWithoutAccess(t *testing.T) {
	report := pgplan.Report{Schema: "public", Type: "order_status"}
	executable := func() []engine.TableChange {
		return []engine.TableChange{
			{Table: "order_status", Operation: ddl.StatementAlterType, DDL: "ALTER TYPE public.order_status ADD VALUE 'paid'"},
			{Table: "order_status", Operation: ddl.StatementAlterType, DDL: "ALTER TYPE public.order_status ADD VALUE 'delivered'"},
		}
	}

	t.Run("access proven keeps the steps executable", func(t *testing.T) {
		changes, err := blockEnumTypeWithoutAccess(t.Context(), "app", report, executable(), func(context.Context) error { return nil })
		require.NoError(t, err)
		for _, change := range changes {
			assert.Empty(t, change.ExecutionMode)
		}
	})

	t.Run("a missing grant blocks every step and names the grant", func(t *testing.T) {
		missing := &preflight.PrivilegeError{Tier: preflight.TierAlterInPlace,
			Check: "pg_has_role(app, owner, 'USAGE')", Grant: `GRANT "owner" TO "app"`}
		changes, err := blockEnumTypeWithoutAccess(t.Context(), "app", report, executable(), func(context.Context) error { return missing })
		require.NoError(t, err)
		require.Len(t, changes, 2)
		for _, change := range changes {
			assert.Equal(t, engine.ExecutionModeBlocked, change.ExecutionMode)
			assert.Equal(t, `the engine role lacks access to alter enum type "order_status"; provision with: GRANT "owner" TO "app"`, change.ModeReason)
		}
	})

	t.Run("a type that is no longer an enum fails the plan", func(t *testing.T) {
		gone := fmt.Errorf("%w: no type public.order_status; nothing was executed", preflight.ErrEnumTypeNotFound)
		_, err := blockEnumTypeWithoutAccess(t.Context(), "app", report, executable(), func(context.Context) error { return gone })
		require.ErrorIs(t, err, preflight.ErrEnumTypeNotFound)
		assert.ErrorContains(t, err, `check access to PostgreSQL enum type "public"."order_status"`)
	})

	t.Run("a check that cannot answer fails the plan", func(t *testing.T) {
		down := errors.New("connection reset")
		_, err := blockEnumTypeWithoutAccess(t.Context(), "app", report, executable(), func(context.Context) error { return down })
		require.ErrorIs(t, err, down)
	})

	t.Run("nothing executable is not probed", func(t *testing.T) {
		blocked := []engine.TableChange{{Table: "order_status", ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "earlier verdict"}}
		changes, err := blockEnumTypeWithoutAccess(t.Context(), "app", report, blocked, func(context.Context) error {
			assert.Fail(t, "a plan with nothing executable must not be probed")
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, "earlier verdict", changes[0].ModeReason)
	})
}

// enumApplyRequest is an apply request for one planned label addition.
func enumApplyRequest(namespace, typeName, statement string) *engine.ApplyRequest {
	return &engine.ApplyRequest{
		Database: "app",
		Changes: []engine.SchemaChange{{
			Namespace:    namespace,
			TableChanges: []engine.TableChange{{Table: typeName, Operation: ddl.StatementAlterType, DDL: statement}},
		}},
		Credentials: &engine.Credentials{DSN: "postgres://app@localhost/app"},
	}
}

// Apply admits a label addition only as it was planned: qualified with the
// schema the apply targets and named by the type it changes. Anything else
// is refused before work is queued.
func TestValidateOptimisticApplyEnumValueAdd(t *testing.T) {
	t.Run("a planned label addition is admitted", func(t *testing.T) {
		change, err := validateOptimisticApply(enumApplyRequest("public", "order_status", "ALTER TYPE public.order_status ADD VALUE 'paid' BEFORE 'shipped'"))
		require.NoError(t, err)
		assert.True(t, change.enumValueAdd)
		assert.False(t, change.concurrentIndex)
		assert.Equal(t, "public", change.namespace)
		assert.Equal(t, "order_status", change.table)
		assert.Equal(t, 1, change.steps)
	})

	refusals := []struct {
		name      string
		namespace string
		typeName  string
		statement string
		want      string
	}{
		{
			name: "another schema", namespace: "billing", typeName: "order_status",
			statement: "ALTER TYPE public.order_status ADD VALUE 'paid'",
			want:      `apply PostgreSQL enum type "order_status": the planned statement names schema "public" but the apply targets schema "billing"; the target's schema route changed after planning, so plan again`,
		},
		{
			name: "an unqualified type", namespace: "public", typeName: "order_status",
			statement: "ALTER TYPE order_status ADD VALUE 'paid'",
			want:      `apply PostgreSQL enum type "order_status": the planned statement does not name the type's schema, so plan again`,
		},
		{
			name: "another type", namespace: "public", typeName: "order_status",
			statement: "ALTER TYPE public.payment_state ADD VALUE 'refunded'",
			want:      `apply PostgreSQL enum type "order_status": the planned statement adds a label to type "payment_state"`,
		},
	}
	for _, tt := range refusals {
		t.Run(tt.name, func(t *testing.T) {
			_, err := validateOptimisticApply(enumApplyRequest(tt.namespace, tt.typeName, tt.statement))
			require.EqualError(t, err, tt.want)
		})
	}
}

// An enum step's failures reach the operator as refusals that name the
// type: a missing grant, a type that is gone, a label another change added
// after planning, a placement label another change removed, and a
// broken engine invariant, each with what to do next. A type name that moved
// to another type is pg-sprite's typed drift, refused by the shared
// mapping. None of them is retried.
func TestEnumTypeRefusals(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		reason string
		detail string
	}{
		{
			name: "missing grant",
			err: &enumTypeError{err: fmt.Errorf("check enum type: %w", &preflight.PrivilegeError{Tier: preflight.TierAlterInPlace,
				Check: "pg_has_role(app, owner, 'USAGE')", Grant: `GRANT "owner" TO "app"`, Hint: "if the owner is an administrative role, transfer the type instead"})},
			reason: "insufficient-privileges",
			detail: `the engine role lacks access to alter enum type "order_status"; provision with: GRANT "owner" TO "app"; if the owner is an administrative role, transfer the type instead`,
		},
		{
			name:   "type gone",
			err:    &enumTypeError{err: fmt.Errorf("check enum type: %w", preflight.ErrEnumTypeNotFound)},
			reason: "planned-object-drift",
			detail: `"order_status" is no longer an enum type on the target; re-plan against the current schema`,
		},
		{
			name: "label added after planning",
			err: &enumTypeError{label: "paid", err: fmt.Errorf("add label: %w: %w", executor.ErrEnumLabelExists,
				&pgconn.PgError{Code: "42710", Message: `enum label "paid" already exists`})},
			reason: "planned-object-drift",
			detail: `enum type "order_status" already has label "paid", which another change added after planning; re-plan against the current schema`,
		},
		{
			name: "placement label renamed after planning",
			err: &enumTypeError{label: "packed", neighbor: "shipped", err: fmt.Errorf("add label: %w: %w", executor.ErrEnumNeighborMissing,
				&pgconn.PgError{Code: "22023", Message: `"shipped" is not an existing enum label`})},
			reason: "planned-object-drift",
			detail: `"packed" is placed next to label "shipped", which another change renamed or removed from enum type "order_status" after planning; re-plan against the current schema`,
		},
		{
			name:   "engine invariant violated",
			err:    &enumTypeError{err: fmt.Errorf("add label: %w", executor.ErrInvariantViolation)},
			reason: "engine-invariant-violation",
			detail: `the engine's safety invariants did not hold while changing enum type "order_status"; inspect the target and server logs before re-running`,
		},
		{
			name:   "type name moved to another type",
			err:    &enumTypeError{err: fmt.Errorf("add label: %w", executor.ErrEnumTypeDrift)},
			reason: "planned-object-drift",
			detail: `the enum type the planned statement for "order_status" names is a different type than the one checked; re-plan against the current schema`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := classifyRefusal(tt.err, "order_status")
			require.NotNil(t, r, "an enum step's permanent outcome is a refusal, never a retry")
			assert.Equal(t, tt.reason, r.reason)
			assert.Equal(t, tt.detail, r.detail)
		})
	}

	t.Run("the server error stays in the log text", func(t *testing.T) {
		err := &enumTypeError{label: "paid", err: fmt.Errorf("add label: %w: %w", executor.ErrEnumLabelExists,
			&pgconn.PgError{Code: "42710", Message: `enum label "paid" already exists`})}
		assert.Contains(t, err.Error(), `enum label "paid" already exists`)
	})
}
