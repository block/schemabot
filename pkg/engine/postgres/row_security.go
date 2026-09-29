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
func planPostgresDefinition(ctx context.Context, pool *pgxpool.Pool, namespace, sql string) (plan.Report, string, error) {
	desired, err := statement.ParseDesired(sql)
	if err != nil {
		return plan.Report{}, "", err
	}
	// Table-only declarations manage structure, not row security. This keeps
	// existing files and pre-RLS rollback baselines compatible; the ordinary
	// planner leaves live policies and settings unchanged.
	report, err := diffplan.Plan(ctx, pool, diffplan.Request{Schema: namespace, Desired: desired})
	if err != nil {
		return plan.Report{}, desired.Table(), fmt.Errorf("plan PostgreSQL table %q: %w", desired.Table(), err)
	}
	return report, desired.Table(), nil
}
