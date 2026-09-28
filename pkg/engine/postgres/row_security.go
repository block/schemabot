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
			if errors.Is(err, schemadiff.ErrUnsupportedChange) {
				return plan.Report{}, desired.Table(), fmt.Errorf("compare row security for table %q; SchemaBot does not yet execute changes to RLS declarations: %w", desired.Table(), err)
			}
			return plan.Report{}, desired.Table(), fmt.Errorf("compare row security for table %q: %w", desired.Table(), err)
		}
		return report, desired.Table(), nil
	}
	desired, err := statement.ParseDesired(sql)
	if err != nil {
		return plan.Report{}, "", err
	}
	// An omitted declaration must not silently discard live access rules,
	// including policies on tables where RLS is currently disabled.
	var liveRLS bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = $2
			AND (c.relrowsecurity OR c.relforcerowsecurity
				OR EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = c.oid))
		)
	`, namespace, desired.Table()).Scan(&liveRLS)
	if err != nil {
		return plan.Report{}, desired.Table(), fmt.Errorf("inspect row security for table %q: %w", desired.Table(), err)
	}
	if liveRLS {
		return plan.Report{}, desired.Table(), fmt.Errorf("table %q has live row security settings or policies missing from its declaration: %w", desired.Table(), schemadiff.ErrUnsupportedChange)
	}
	report, err := diffplan.Plan(ctx, pool, diffplan.Request{Schema: namespace, Desired: desired})
	return report, desired.Table(), err
}
