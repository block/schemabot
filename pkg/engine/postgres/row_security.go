package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/block/pg-sprite/pkg/diffplan"
	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/preflight"
	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/block/pg-sprite/pkg/statement"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/block/schemabot/pkg/engine"
)

// Table-only definitions use the ordinary planner. Managed RLS definitions
// are handled separately as atomic operations before reaching this function.
// INV: RV-4 — refuse unsupported changes before apply admission.
func planPostgresDefinition(ctx context.Context, pool *pgxpool.Pool, namespace string, desired statement.DesiredSchema) (plan.Report, string, error) {
	// Table-only declarations manage structure, not row security. This keeps
	// existing files and pre-RLS rollback baselines compatible; the ordinary
	// planner leaves live policies and settings unchanged.
	report, err := diffplan.Plan(ctx, pool, diffplan.Request{Schema: namespace, Desired: desired})
	if err != nil {
		return plan.Report{}, desired.Table(), fmt.Errorf("plan PostgreSQL table %q: %w%s", desired.Table(), err, referencedTableRemedy(err))
	}
	return report, desired.Table(), nil
}

// planRowSecurityTableDefinition plans a file that declares row security
// when the live table's row security already matches it: the report changes
// only the table definition, so its statements run as in-place DDL that
// leaves the policies and settings as they are. matched is false when the
// row security differs or the table does not exist yet; either one is
// planned as a single row security operation instead. A table change the
// planner refuses keeps its own error.
func planRowSecurityTableDefinition(ctx context.Context, pool *pgxpool.Pool, namespace string, desired statement.DesiredWithRowSecurity) (report plan.Report, matched bool, err error) {
	report, err = diffplan.PlanWithRowSecurity(ctx, pool, namespace, desired)
	var review *diffplan.RowSecurityReviewRequired
	switch {
	case err == nil:
		return report, true, nil
	case errors.As(err, &review):
		slog.Debug("PostgreSQL row security differs from the schema file; planning it as one row security operation",
			"namespace", namespace, "table", desired.Table())
		return plan.Report{}, false, nil
	case errors.Is(err, schemadiff.ErrTableNotFound):
		slog.Debug("PostgreSQL table declared with row security does not exist; planning its creation as one row security operation",
			"namespace", namespace, "table", desired.Table())
		return plan.Report{}, false, nil
	default:
		return plan.Report{}, false, fmt.Errorf("plan PostgreSQL table %q: %w%s", desired.Table(), err, referencedTableRemedy(err))
	}
}

// parseDesiredFiles admits every schema file in a namespace once, through
// pg-sprite's typed parse, which picks the planner each file belongs to: the
// row-security planner for a file that declares row security, the ordinary
// planner otherwise. Every later step reads the admitted value, so the
// duplicate-table check and the plan agree on each file's table and planner by
// construction.
func parseDesiredFiles(namespace string, files map[string]string) (map[string]statement.DesiredFile, error) {
	parsed := make(map[string]statement.DesiredFile, len(files))
	for _, filename := range sortedKeys(files) {
		desired, err := statement.ParseDesiredFile(files[filename])
		if err != nil {
			// The parse reads only the file's text, so whatever it refuses is
			// fixed by editing the file: a statement the file may not hold, a
			// schema-qualified name, or a syntax error, which pg-sprite
			// locates at its line and column.
			return nil, desiredFileRefusal(namespace, filename, err)
		}
		parsed[filename] = desired
	}
	return parsed, nil
}

// desiredFileRefusal is the refusal for a schema file the parser does not
// admit. A syntax error is about one line of the file, so the refusal also
// lists it as a problem located at that line, for a renderer that points the
// file's author at it.
func desiredFileRefusal(namespace, filename string, err error) error {
	refusal := &engine.PlanRefusedError{Err: fmt.Errorf("plan PostgreSQL schema in %q/%q: %w", namespace, filename, err)}
	var syntaxErr *statement.SyntaxError
	if errors.As(err, &syntaxErr) {
		refusal.Problems = []engine.PlanProblem{{
			File:      namespace + "/" + filename,
			Line:      syntaxErr.Line,
			Message:   fmt.Sprintf("The statement has a syntax error at column %d: %s. Correct it in this file.", syntaxErr.Column, syntaxErr.Message),
			Statement: syntaxErr.SourceLine,
		}}
	}
	return refusal
}

// plannerRefusal wraps a failure to plan one schema file with the file it
// came from, and reports a planner's decline of the declared schema as a plan
// refusal. A planner reads the target as well as the files, so its errors mix
// declines with failures of the connection or the role, and only the declines
// are refusals. pg-sprite names each decline with a sentinel: a shape the
// planner refuses while it builds the table (a partition or inheritance
// declaration, a constraint repeated within one file, an expression whose text
// never settles), a change no ALTER can converge, a foreign key to a table the
// target lacks, policies that read each other back, and the parser's own
// refusals when a planner reaches them. Each is fixed by editing the schema
// files. A privilege the planning role lacks is a refusal too, fixed by a
// grant on the target: see planningPrivilegeRefusal. Any other failure, such
// as a lost connection, is returned as it came. A file the parser does not
// admit never reaches a planner: parseDesiredFiles refuses it first.
//
// pg-sprite keeps the closed set of these refusals in its migrate package,
// which also carries its schema change runner, so the sentinels are named here
// until a lighter pg-sprite package exports a predicate over that set.
func plannerRefusal(namespace, filename string, err error) error {
	wrapped := fmt.Errorf("plan PostgreSQL schema in %q/%q: %w", namespace, filename, err)
	switch {
	case errors.Is(err, statement.ErrMultipleCreateTables),
		errors.Is(err, statement.ErrDisallowedStatement),
		errors.Is(err, statement.ErrDesiredPartitions),
		errors.Is(err, statement.ErrEnumTypeAlone),
		errors.Is(err, statement.ErrEnumLabel),
		errors.Is(err, schemadiff.ErrDesiredPartitionOf),
		errors.Is(err, schemadiff.ErrDesiredInherits),
		errors.Is(err, schemadiff.ErrDesiredConstraintMerged),
		errors.Is(err, schemadiff.ErrExpressionUnsettled),
		errors.Is(err, schemadiff.ErrUnsupportedChange),
		errors.Is(err, schemadiff.ErrReferencedTableNotFound),
		errors.Is(err, schemadiff.ErrReferencedTableNotReadable),
		errors.Is(err, schemadiff.ErrReferencedTableNotReferenceable),
		errors.Is(err, executor.ErrPolicyRecursion),
		errors.As(err, new(*removedRowSecurityError)):
		return &engine.PlanRefusedError{Err: wrapped}
	}
	if refusal := planningPrivilegeRefusal(namespace, filename, wrapped); refusal != nil {
		return &engine.PlanRefusedError{Err: refusal}
	}
	return wrapped
}

// planningPrivilegeError is a schema file the planning role cannot plan for
// want of a privilege on the target. Planning again fails the same way until
// the grant is made, so it is a refusal about the target rather than the
// files, and its text ends in the grant to make.
type planningPrivilegeError struct {
	text string
	err  error
}

func (e *planningPrivilegeError) Error() string { return e.text }
func (e *planningPrivilegeError) Unwrap() error { return e.err }

// planningPrivilegeRefusal returns the refusal for a plan that failed on a
// privilege the planning role lacks, or nil for any other failure. When
// pg-sprite checked the privilege itself, its PrivilegeError carries the exact
// GRANT, and the refusal names it with the catalog check that verifies it.
// Otherwise the server's insufficient_privilege error names the object, and
// the refusal keeps that error and says to grant the privilege it names.
func planningPrivilegeRefusal(namespace, filename string, wrapped error) *planningPrivilegeError {
	if privilege, ok := errors.AsType[*preflight.PrivilegeError](wrapped); ok {
		remedy := fmt.Sprintf("provision with: %s (verified by: %s)", privilege.Grant, privilege.Check)
		if privilege.Hint != "" {
			remedy += "; " + privilege.Hint
		}
		return &planningPrivilegeError{
			text: fmt.Sprintf("plan PostgreSQL schema in %q/%q: SchemaBot's role lacks a privilege planning this file needs; %s; then re-plan", namespace, filename, remedy),
			err:  wrapped,
		}
	}
	if isInsufficientPrivilege(wrapped) {
		return &planningPrivilegeError{
			text: wrapped.Error() + "; SchemaBot's role lacks the privilege this error names, so grant it to that role, then re-plan",
			err:  wrapped,
		}
	}
	return nil
}

// referencedTableRemedy names the fix for a foreign key the planner cannot
// bind. The planner copies each referenced table's shape from the live
// database, so the referenced table must already exist on the target: a
// schema file that creates it in the same plan does not help. The planning
// role must also hold SELECT and REFERENCES on it. It returns an empty
// string for every other failure.
func referencedTableRemedy(err error) string {
	switch {
	case errors.Is(err, schemadiff.ErrReferencedTableNotFound):
		return "; a foreign key can reference only a table that already exists on the target, so apply the referenced table first, then add the foreign key to the file in a later change"
	case errors.Is(err, schemadiff.ErrReferencedTableNotReadable):
		return "; grant the planning role SELECT on the referenced table, then re-plan"
	case errors.Is(err, schemadiff.ErrReferencedTableNotReferenceable):
		return "; grant the planning role REFERENCES on the referenced table, then re-plan"
	}
	return ""
}
