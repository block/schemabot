package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/block/pg-sprite/pkg/diffplan"
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
			return nil, fmt.Errorf("plan PostgreSQL schema in %q/%q: %w", namespace, filename, err)
		}
		parsed[filename] = desired
	}
	return parsed, nil
}

// plannerRefusal reports a planner decline of the declared schema as a plan
// refusal. pg-sprite names each decline with a sentinel: a change no ALTER can
// converge, a foreign key to a table the target lacks, a partition or
// inheritance declaration, and a constraint repeated within one file. Each is
// fixed by editing the schema files. Any other failure, such as a lost
// connection or a missing privilege, is returned as it came.
func plannerRefusal(err error) error {
	switch {
	case errors.Is(err, schemadiff.ErrUnsupportedChange),
		errors.Is(err, schemadiff.ErrReferencedTableNotFound),
		errors.Is(err, schemadiff.ErrDesiredPartitionOf),
		errors.Is(err, schemadiff.ErrDesiredInherits),
		errors.Is(err, schemadiff.ErrDesiredConstraintMerged):
		return &engine.PlanRefusedError{Err: err}
	}
	return err
}

// referencedTableRemedy names the fix for a foreign key the planner cannot
// bind. The planner copies each referenced table's shape from the live
// database, so the referenced table must already exist on the target: a
// schema file that creates it in the same plan does not help. It returns an
// empty string for every other failure.
func referencedTableRemedy(err error) string {
	switch {
	case errors.Is(err, schemadiff.ErrReferencedTableNotFound):
		return "; a foreign key can reference only a table that already exists on the target, so leave the foreign key out of the schema file, apply the referenced table first, then run the ADD CONSTRAINT outside SchemaBot and add the foreign key back to the file"
	case errors.Is(err, schemadiff.ErrReferencedTableNotReadable):
		return "; grant the planning role SELECT on the referenced table, then re-plan"
	}
	return ""
}
