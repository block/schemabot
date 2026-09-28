package postgres

import (
	"context"
	"fmt"

	"github.com/block/pg-sprite/pkg/diffplan"
	"github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/statement"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RLS declarations use pg-sprite's separate comparison boundary. An unchanged
// definition can converge; a delta cannot become ordinary executable DDL.
// INV: RV-4 — refuse unsupported changes before apply admission.
func planPostgresDefinition(ctx context.Context, pool *pgxpool.Pool, namespace, sql string) (plan.Report, string, error) {
	hasRLS, err := statement.HasRowSecurityDeclaration(sql)
	if err != nil {
		return plan.Report{}, "", err
	}
	if hasRLS {
		desired, err := statement.ParseDesiredWithRowSecurity(sql)
		if err != nil {
			return plan.Report{}, "", err
		}
		report, err := diffplan.PlanWithRowSecurity(ctx, pool, namespace, desired)
		if err != nil {
			return plan.Report{}, desired.Table(), fmt.Errorf("compare row security for table %q; SchemaBot does not yet execute changes to RLS declarations: %w", desired.Table(), err)
		}
		return report, desired.Table(), nil
	}
	desired, err := statement.ParseDesired(sql)
	if err != nil {
		return plan.Report{}, "", err
	}
	report, err := diffplan.Plan(ctx, pool, diffplan.Request{Schema: namespace, Desired: desired})
	return report, desired.Table(), err
}
