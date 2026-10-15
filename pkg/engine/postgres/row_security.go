package postgres

import (
	"context"
	"fmt"

	"github.com/block/pg-sprite/pkg/diffplan"
	"github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/statement"
	"github.com/jackc/pgx/v5/pgxpool"
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
		return plan.Report{}, desired.Table(), fmt.Errorf("plan PostgreSQL table %q: %w", desired.Table(), err)
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
