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
	// Table-only declarations manage structure, not row security. This keeps
	// existing files and pre-RLS rollback baselines compatible; the ordinary
	// planner leaves live policies and settings unchanged.
	report, err := diffplan.Plan(ctx, pool, diffplan.Request{Schema: namespace, Desired: desired})
	if err != nil {
		return plan.Report{}, desired.Table(), fmt.Errorf("plan PostgreSQL table %q: %w", desired.Table(), err)
	}
	return report, desired.Table(), nil
}

// desiredTableName returns the table a schema file declares, admitted through
// the same pg-sprite parse planPostgresDefinition plans with, so the name is
// the one the plan keys the file's diff under.
func desiredTableName(sql string) (string, error) {
	hasRLS, err := statement.HasRowSecurityDeclaration(sql)
	if err != nil {
		return "", err
	}
	if hasRLS {
		desired, err := statement.ParseDesiredWithRowSecurity(sql)
		if err != nil {
			return "", err
		}
		return desired.Table(), nil
	}
	desired, err := statement.ParseDesired(sql)
	if err != nil {
		return "", err
	}
	return desired.Table(), nil
}
