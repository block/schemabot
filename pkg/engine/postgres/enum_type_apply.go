package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/preflight"
	pgstatement "github.com/block/pg-sprite/pkg/statement"

	"github.com/block/schemabot/pkg/engine"
)

// isEnumValueAdd reports whether a planned step's statements are a single
// ALTER TYPE ... ADD VALUE, the one statement an enum types file plans.
func isEnumValueAdd(statements []string) (pgstatement.Statement, bool, error) {
	if len(statements) != 1 {
		return pgstatement.Statement{}, false, nil
	}
	st, err := pgstatement.ParseOne(statements[0])
	if err != nil {
		return pgstatement.Statement{}, false, fmt.Errorf("parse planned statement: %w", err)
	}
	return st, st.Kind() == pgstatement.KindAddEnumValue, nil
}

// validateEnumValueAdd admits one label addition planned from an enum types
// file. The plan qualified the statement with the schema it was made
// against and named the step by the type, so a statement that names another
// schema or another type is not the step that was reviewed and is refused
// before any work is queued.
func validateEnumValueAdd(req *engine.ApplyRequest, tc engine.TableChange, st pgstatement.Statement) (nativeApply, error) {
	namespace := req.Changes[0].Namespace
	ref := st.EnumType()
	if ref.Schema == "" {
		return nativeApply{}, fmt.Errorf("apply PostgreSQL enum type %q: the planned statement does not name the type's schema, so plan again", tc.Table)
	}
	if ref.Schema != namespace {
		return nativeApply{}, fmt.Errorf("apply PostgreSQL enum type %q: the planned statement names schema %q but the apply targets schema %q; the target's schema route changed after planning, so plan again", tc.Table, ref.Schema, namespace)
	}
	if ref.Name != tc.Table {
		return nativeApply{}, fmt.Errorf("apply PostgreSQL enum type %q: the planned statement adds a label to type %q", tc.Table, ref.Name)
	}
	return nativeApply{namespace: namespace, table: tc.Table, sql: tc.DDL, steps: 1, enumValueAdd: true}, nil
}

// executeEnumValueAdd adds one enum label. The type is resolved and the
// role's right to alter it is proven in the session that executes, because
// what held at plan time proves nothing now. pg-sprite then runs the
// statement under the apply's lock and statement budgets, in a transaction
// of its own that commits before the step ends, so the next step of the
// apply can use the label. Several labels for one type are separate steps,
// run in planned order, and each placement names a label the type already
// carried when the plan was made.
//
// The label is added only to the type the check resolved: pg-sprite's
// executor confirms the type by OID before its commit and rolls back
// otherwise (pg-sprite's ST-7, not an entry in SchemaBot's registry).
func executeEnumValueAdd(ctx context.Context, pool *pgxpool.Pool, change nativeApply) error {
	st, err := pgstatement.ParseOne(change.sql)
	if err != nil {
		return fmt.Errorf("parse planned label addition for enum type %q: %w", change.table, err)
	}
	target, err := preflight.CheckEnumTarget(ctx, pool, st.EnumType())
	if err != nil {
		return &enumTypeError{err: fmt.Errorf("check enum type %q.%q: %w", change.namespace, change.table, err)}
	}
	err = executor.ExecuteEnumValueAdd(ctx, pool, target, st, executor.Budget{
		LockTimeout: optimisticLockTimeout, StatementTimeout: optimisticStatementLimit,
	}, executor.DefaultRetryPolicy())
	if err != nil {
		return &enumTypeError{
			label:    st.AddedEnumValue(),
			neighbor: st.EnumValueNeighbor(),
			err:      fmt.Errorf("add label %q to enum type %q.%q: %w", st.AddedEnumValue(), change.namespace, change.table, err),
		}
	}
	return nil
}

// enumTypeError marks a failure of an enum label addition, so the refusal
// names the type instead of a table and the labels the step names. label is
// the label the step adds and neighbor the label it is placed BEFORE or
// AFTER; both are empty for a failure of the check that runs before the
// statement, and neighbor is empty when the step names no position.
type enumTypeError struct {
	label    string
	neighbor string
	err      error
}

func (e *enumTypeError) Error() string { return e.err.Error() }

func (e *enumTypeError) Unwrap() error { return e.err }

// enumTypeRefusal maps the outcomes only an enum step produces to permanent
// refusals that name the labels the step carries. A nil result leaves the
// failure to the shared mapping, which covers the budgets, the type drift
// pg-sprite reports, and the rest of the executor's vocabulary.
func enumTypeRefusal(err error, typeName string) *refusal {
	var enumErr *enumTypeError
	if !errors.As(err, &enumErr) {
		return nil
	}
	var privilegeErr *preflight.PrivilegeError
	if errors.As(err, &privilegeErr) {
		return &refusal{reason: "insufficient-privileges",
			cause:         fmt.Sprintf("the engine role lacks access to alter enum type %s", quotedTable(typeName)),
			remedy:        privilegeRemedy(privilegeErr),
			provisionable: true}
	}
	if errors.Is(err, preflight.ErrEnumTypeNotFound) {
		return &refusal{reason: "planned-object-drift",
			cause:  fmt.Sprintf("%s is no longer an enum type on the target", quotedTable(typeName)),
			remedy: replanRemedy}
	}
	if errors.Is(err, executor.ErrEnumLabelExists) {
		return &refusal{reason: "planned-object-drift",
			cause:  fmt.Sprintf("enum type %s already has label %s, which another change added after planning", quotedTable(typeName), quotedTable(enumErr.label)),
			remedy: replanRemedy}
	}
	if errors.Is(err, executor.ErrEnumNeighborMissing) {
		return &refusal{reason: "planned-object-drift",
			cause:  fmt.Sprintf("%s is placed next to label %s, which another change renamed or removed from enum type %s after planning", quotedTable(enumErr.label), quotedTable(enumErr.neighbor), quotedTable(typeName)),
			remedy: replanRemedy}
	}
	if executor.OutcomeCode(err) == executor.CodeInvariantViolation {
		// The shared arm words this for a table; an enum step names its type.
		return &refusal{reason: "engine-invariant-violation",
			cause:  fmt.Sprintf("the engine's safety invariants did not hold while changing enum type %s", quotedTable(typeName)),
			remedy: invariantViolationRemedy}
	}
	return nil
}
