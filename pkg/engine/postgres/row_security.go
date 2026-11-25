package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/block/pg-sprite/pkg/diffplan"
	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/plan"
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
			return nil, plannerRefusal(fmt.Errorf("plan PostgreSQL schema in %q/%q: %w", namespace, filename, err))
		}
		parsed[filename] = desired
	}
	return parsed, nil
}

// plannerRefusal reports a decline of the declared schema as a plan refusal.
// pg-sprite names each decline with a sentinel: a file its parser does not
// admit (two tables, a statement a desired file may not hold, partitions, an
// enum type mixed with other statements or carrying a bad label), a shape the
// planner refuses while it builds the table (a partition or inheritance
// declaration, a constraint repeated within one file, an expression whose text
// never settles), a change no ALTER can converge, a foreign key to a table the
// target lacks, and policies that read each other back. Each is fixed by
// editing the schema files. Any other failure, such as a syntax error, a lost
// connection, or a missing privilege, is returned as it came.
//
// pg-sprite keeps the closed set of these refusals in its migrate package,
// which also carries its schema change runner, so the sentinels are named here
// until a lighter pg-sprite package exports a predicate over that set.
func plannerRefusal(err error) error {
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
		errors.Is(err, executor.ErrPolicyRecursion):
		return &engine.PlanRefusedError{Err: err}
	}
	return err
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
