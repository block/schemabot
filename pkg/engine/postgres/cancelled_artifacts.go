// cancelled_artifacts.go removes what a cancelled or failed PostgreSQL schema
// change left on a live table: the helper CHECK an online SET NOT NULL adds NOT VALID
// as its first step and drops as its last. A cancel that lands between the two,
// most often while the helper's validation fails on a NULL row and retries,
// leaves the helper behind, and it goes on rejecting every write that leaves
// its column NULL. A failure that ends the apply there leaves it the same way.
// The application never asked for that check, so the cancel or the failure
// removes it.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"

	"github.com/block/pg-sprite/pkg/diffplan"
	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/preflight"
	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/block/schemabot/pkg/engine"
)

// ReleaseCancelledArtifacts drops the SET NOT NULL helper constraints a
// cancelled schema change added and left on the named tables of one schema,
// as req.AddedConstraints names them, connecting
// from the request's credentials rather than from in-process state, since the
// cancel may land on an instance that never drove the apply.
//
// pg-sprite recognizes a helper by the name and the exact definition its
// sequence gives it, the rule its planner uses to resume one, and each drop is
// checked against the definition read: a constraint replaced under the name
// since is left alone. A helper the release finds and cannot drop is returned
// with the statement that drops it by hand, alongside the error.
func (e *Engine) ReleaseCancelledArtifacts(ctx context.Context, req *engine.ReleaseArtifactsRequest) (*engine.ReleaseArtifactsResult, error) {
	return releaseAddedHelpers(ctx, req, "cancelled")
}

// ReleaseFailedArtifacts drops the SET NOT NULL helper constraints an apply
// that failed for good added and left, most often because rows broke the
// helper's validation. The apply ends there, so the helper would otherwise go
// on rejecting writes for a change nobody is driving. It recognizes and drops
// helpers exactly as ReleaseCancelledArtifacts does.
func (e *Engine) ReleaseFailedArtifacts(ctx context.Context, req *engine.ReleaseArtifactsRequest) (*engine.ReleaseArtifactsResult, error) {
	return releaseAddedHelpers(ctx, req, "failed")
}

// releaseAddedHelpers drops the helpers req.AddedConstraints names on each of
// req.Tables. outcome names how the schema change ended, for the errors.
func releaseAddedHelpers(ctx context.Context, req *engine.ReleaseArtifactsRequest, outcome string) (*engine.ReleaseArtifactsResult, error) {
	if req == nil {
		return nil, fmt.Errorf("release %s PostgreSQL schema change artifacts: request is required", outcome)
	}
	if req.Credentials == nil || req.Credentials.DSN == "" {
		return nil, fmt.Errorf("release %s PostgreSQL schema change artifacts in schema %q: DSN credentials are required", outcome, req.Database)
	}
	if req.Database == "" {
		return nil, fmt.Errorf("release %s PostgreSQL schema change artifacts: schema is required", outcome)
	}
	if len(req.Tables) == 0 {
		return nil, fmt.Errorf("release %s PostgreSQL schema change artifacts in schema %q: tables are required", outcome, req.Database)
	}
	pool, closePool, err := openTargetPool(ctx, req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("release %s PostgreSQL schema change artifacts in schema %q: %w", outcome, req.Database, err)
	}
	defer closePool()

	result := &engine.ReleaseArtifactsResult{}
	var errs []error
	for _, table := range slices.Sorted(slices.Values(req.Tables)) {
		if err := releaseNotNullHelpers(ctx, pool, req.Database, table, req.AddedConstraints[table], outcome, result); err != nil {
			errs = append(errs, err)
		}
	}
	return result, errors.Join(errs...)
}

// releaseNotNullHelpers drops each helper a stopped SET NOT NULL left on
// schema.table that the stopped apply added, recording each one as removed
// or remaining on result. A helper the apply did not add was already on the
// table, left by an earlier run or put there by another one, and stays; it is
// recorded as preexisting, since it still checks writes.
func releaseNotNullHelpers(ctx context.Context, pool *pgxpool.Pool, schema, table string, added []string, outcome string, result *engine.ReleaseArtifactsResult) error {
	helpers, err := diffplan.LeftoverNotNullHelpers(ctx, pool, schema, table)
	if err != nil {
		return fmt.Errorf("find SET NOT NULL helper constraints on %s.%s: %w", schema, table, err)
	}
	var errs []error
	for _, helper := range helpers {
		constraint := engine.EngineConstraint{
			Table:  schema + "." + table,
			Name:   helper.Name,
			Effect: "rejects writes that break " + strings.TrimSuffix(helper.Definition, " NOT VALID"),
		}
		if !slices.Contains(added, helper.Name) {
			// The caller logs each recorded constraint with the apply's
			// identifiers, so this line is for tracing the engine alone.
			slog.Debug("left a SET NOT NULL helper constraint in place: the schema change did not add it",
				"outcome", outcome, "schema", schema, "table", table, "constraint", helper.Name)
			constraint.Removal = dropConstraintStatement(schema, table, helper.Name)
			result.PreexistingConstraints = append(result.PreexistingConstraints, constraint)
			continue
		}
		err := dropNotNullHelper(ctx, pool, schema, table, helper)
		switch {
		case err == nil:
			slog.Debug("dropped the SET NOT NULL helper constraint the schema change left on the table",
				"outcome", outcome, "schema", schema, "table", table, "constraint", helper.Name)
			result.RemovedConstraints = append(result.RemovedConstraints, constraint)
		case errors.Is(err, executor.ErrDroppedObjectDrift):
			// The constraint changed between the read and the drop's lock, so
			// what is under the name now is not the helper the run left.
			slog.Warn("left a constraint in place: it changed under a SET NOT NULL helper's name after the release read it, so it is not the helper",
				"outcome", outcome, "schema", schema, "table", table, "constraint", helper.Name, "error", err)
		default:
			constraint.Removal = dropConstraintStatement(schema, table, helper.Name)
			result.RemainingConstraints = append(result.RemainingConstraints, constraint)
			errs = append(errs, fmt.Errorf("drop SET NOT NULL helper constraint %q on %s.%s: %w", helper.Name, schema, table, err))
		}
	}
	return errors.Join(errs...)
}

// dropNotNullHelper drops one helper through pg-sprite's checked drop, under
// the same lock and statement limits as every other brief statement the engine
// runs. A drop changes only the catalog and reads no rows, so the table's size
// does not bound it.
func dropNotNullHelper(ctx context.Context, pool *pgxpool.Pool, schema, table string, helper schemadiff.DroppedObject) error {
	pt, err := preflight.CheckTable(ctx, pool, schema, table, math.MaxInt64)
	if err != nil {
		return fmt.Errorf("check table: %w", err)
	}
	budget := executor.SequenceBudget{
		Brief:      executor.Budget{LockTimeout: optimisticLockTimeout, StatementTimeout: optimisticStatementLimit},
		Concurrent: executor.ConcurrentBudget{Overall: optimisticStatementLimit},
		Validate:   executor.ValidateBudget{LockTimeout: optimisticLockTimeout, Overall: optimisticStatementLimit},
	}
	if _, err := executor.RunSequenceDropping(ctx, pool, pt, []string{dropConstraintStatement(schema, table, helper.Name)}, budget, executor.DefaultRetryPolicy(), helper); err != nil {
		return err
	}
	return nil
}

// dropConstraintStatement is the statement that drops constraint name from
// schema.table, with every identifier quoted.
func dropConstraintStatement(schema, table, name string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", pgx.Identifier{schema, table}.Sanitize(), pgx.Identifier{name}.Sanitize())
}
