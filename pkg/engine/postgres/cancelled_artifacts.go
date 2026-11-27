// cancelled_artifacts.go removes what a cancelled or failed PostgreSQL schema
// change left on a live table: the helper CHECK an online SET NOT NULL adds NOT VALID
// as its first step and drops as its last. A cancel that lands between the two,
// most often while the helper's validation fails on a NULL row and retries,
// leaves the helper behind, and it goes on rejecting every write that leaves
// its column NULL. A failure that ends the apply there leaves it the same way.
// The application never asked for that check, so the cancel or the failure
// removes it.
//
// A CHECK or foreign key the change adds NOT VALID and validates in a step of
// its own is left the same way when that validation fails for good or the
// cancel lands before it finishes: the constraint goes on rejecting new writes
// that break a rule the existing rows already break. The change that added it
// is not finishing, so the release removes that one too, and leaves one that
// was on the table before.
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

// ReleaseCancelledArtifacts drops the SET NOT NULL helper constraints, and the
// unvalidated constraints it was still validating, that a cancelled schema
// change added and left on the named tables of one schema, as
// req.AddedConstraints names them, connecting
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

// ReleaseFailedArtifacts drops the SET NOT NULL helper constraints, and the
// unvalidated constraints it was still validating, that an apply that failed
// for good added and left, most often because rows broke a validation. The
// apply ends there, so the constraint would otherwise go on rejecting writes
// for a change nobody is driving. It recognizes and drops them exactly as
// ReleaseCancelledArtifacts does.
func (e *Engine) ReleaseFailedArtifacts(ctx context.Context, req *engine.ReleaseArtifactsRequest) (*engine.ReleaseArtifactsResult, error) {
	return releaseAddedHelpers(ctx, req, "failed")
}

// releaseAddedHelpers drops the helpers and the unvalidated constraints
// req.AddedConstraints names on each of req.Tables. outcome names how the
// schema change ended, for the errors.
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
		helpers, err := diffplan.LeftoverNotNullHelpers(ctx, pool, req.Database, table)
		if err != nil {
			// Without the table's helpers known, an unvalidated constraint
			// cannot be told apart from one, so the table waits for a release
			// that can read them.
			errs = append(errs, fmt.Errorf("find SET NOT NULL helper constraints on %s.%s: %w", req.Database, table, err))
			continue
		}
		if err := releaseNotNullHelpers(ctx, pool, req.Database, table, helpers, req.AddedConstraints[table], outcome, result); err != nil {
			errs = append(errs, err)
		}
		validating := withoutHelpers(req.ValidatedConstraints[table], helpers)
		if err := releaseUnvalidatedConstraints(ctx, pool, req.Database, table, validating, req.AddedConstraints[table], outcome, result); err != nil {
			errs = append(errs, err)
		}
	}
	return result, errors.Join(errs...)
}

// withoutHelpers returns the names in validating that do not name one of
// helpers, which the helper release has already handled.
func withoutHelpers(validating []string, helpers []schemadiff.DroppedObject) []string {
	return slices.DeleteFunc(slices.Clone(validating), func(name string) bool {
		return slices.ContainsFunc(helpers, func(helper schemadiff.DroppedObject) bool { return helper.Name == name })
	})
}

// releaseUnvalidatedConstraints drops each CHECK or foreign key on
// schema.table that the stopped apply added NOT VALID to validate in a step
// of its own, and that is still unvalidated: the validation failed or never
// ran, so the constraint checks every new write against a rule the
// application's existing rows break, for a change nobody is driving. One the
// apply did not add was already on the table, and stays; it is recorded as
// preexisting, since it still checks writes.
//
// validating names the constraints the apply's plan validates. A constraint
// outside it is one the desired schema leaves unvalidated, or one the apply
// replaced in place, and the release does not look at it.
func releaseUnvalidatedConstraints(ctx context.Context, pool *pgxpool.Pool, schema, table string, validating, added []string, outcome string, result *engine.ReleaseArtifactsResult) error {
	if len(validating) == 0 {
		slog.Debug("no unvalidated constraint to release: the schema change validates none apart from its add",
			"outcome", outcome, "schema", schema, "table", table)
		return nil
	}
	constraints, err := unvalidatedConstraints(ctx, pool, schema, table, validating)
	if err != nil {
		return fmt.Errorf("find unvalidated constraints on %s.%s: %w", schema, table, err)
	}
	var errs []error
	for _, dropped := range constraints {
		constraint := engine.EngineConstraint{
			Table:  schema + "." + table,
			Name:   dropped.Name,
			Effect: "rejects writes that break " + strings.TrimSuffix(dropped.Definition, " NOT VALID"),
		}
		if !slices.Contains(added, dropped.Name) {
			slog.Debug("left an unvalidated constraint in place: the schema change did not add it",
				"outcome", outcome, "schema", schema, "table", table, "constraint", dropped.Name)
			constraint.Removal = dropConstraintStatement(schema, table, dropped.Name)
			result.PreexistingConstraints = append(result.PreexistingConstraints, constraint)
			continue
		}
		err := dropCheckedConstraint(ctx, pool, schema, table, dropped)
		switch {
		case err == nil:
			slog.Debug("dropped the unvalidated constraint the schema change left on the table",
				"outcome", outcome, "schema", schema, "table", table, "constraint", dropped.Name)
			result.RemovedConstraints = append(result.RemovedConstraints, constraint)
		case errors.Is(err, executor.ErrDroppedObjectDrift):
			// The constraint changed between the read and the drop's lock, so
			// what is under the name now is not the one the run left.
			slog.Warn("left a constraint in place: it changed after the release read it, so it is not the one the schema change added",
				"outcome", outcome, "schema", schema, "table", table, "constraint", dropped.Name, "error", err)
			constraint.Removal = dropConstraintStatement(schema, table, dropped.Name)
			result.PreexistingConstraints = append(result.PreexistingConstraints, constraint)
		default:
			constraint.Removal = dropConstraintStatement(schema, table, dropped.Name)
			result.RemainingConstraints = append(result.RemainingConstraints, constraint)
			errs = append(errs, fmt.Errorf("drop unvalidated constraint %q on %s.%s: %w", dropped.Name, schema, table, err))
		}
	}
	return errors.Join(errs...)
}

// unvalidatedConstraints reads each CHECK or foreign key among names on
// schema.table that is not validated, with the definition the drop checks
// again under its lock. A table that is gone has none.
func unvalidatedConstraints(ctx context.Context, pool *pgxpool.Pool, schema, table string, names []string) ([]schemadiff.DroppedObject, error) {
	rows, err := pool.Query(ctx, `
		SELECT c.conname, pg_get_constraintdef(c.oid)
		FROM pg_constraint c
		WHERE c.conrelid = to_regclass(format('%I.%I', $1::text, $2::text))
		  AND c.contype IN ('c', 'f')
		  AND NOT c.convalidated
		  AND c.conname = ANY($3)
		ORDER BY c.conname`, schema, table, names)
	if err != nil {
		return nil, err
	}
	constraints, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (schemadiff.DroppedObject, error) {
		constraint := schemadiff.DroppedObject{Kind: schemadiff.DroppedConstraint}
		err := row.Scan(&constraint.Name, &constraint.Definition)
		return constraint, err
	})
	if err != nil {
		return nil, err
	}
	return constraints, nil
}

// releaseNotNullHelpers drops each of helpers, the helpers a stopped SET NOT
// NULL left on schema.table, that the stopped apply added, recording each one as removed
// or remaining on result. A helper the apply did not add was already on the
// table, left by an earlier run or put there by another one, and stays; it is
// recorded as preexisting, since it still checks writes.
func releaseNotNullHelpers(ctx context.Context, pool *pgxpool.Pool, schema, table string, helpers []schemadiff.DroppedObject, added []string, outcome string, result *engine.ReleaseArtifactsResult) error {
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
		err := dropCheckedConstraint(ctx, pool, schema, table, helper)
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
			constraint.Removal = dropConstraintStatement(schema, table, helper.Name)
			result.PreexistingConstraints = append(result.PreexistingConstraints, constraint)
		default:
			constraint.Removal = dropConstraintStatement(schema, table, helper.Name)
			result.RemainingConstraints = append(result.RemainingConstraints, constraint)
			errs = append(errs, fmt.Errorf("drop SET NOT NULL helper constraint %q on %s.%s: %w", helper.Name, schema, table, err))
		}
	}
	return errors.Join(errs...)
}

// dropCheckedConstraint drops one constraint through pg-sprite's checked drop,
// which refuses when the definition under the name changed since it was read,
// under the same lock and statement limits as every other brief statement the engine
// runs. A drop changes only the catalog and reads no rows, so the table's size
// does not bound it.
func dropCheckedConstraint(ctx context.Context, pool *pgxpool.Pool, schema, table string, constraint schemadiff.DroppedObject) error {
	pt, err := preflight.CheckTable(ctx, pool, schema, table, math.MaxInt64)
	if err != nil {
		return fmt.Errorf("check table: %w", err)
	}
	budget := executor.SequenceBudget{
		Brief:      executor.Budget{LockTimeout: optimisticLockTimeout, StatementTimeout: optimisticStatementLimit},
		Concurrent: executor.ConcurrentBudget{Overall: optimisticStatementLimit},
		Validate:   executor.ValidateBudget{LockTimeout: optimisticLockTimeout, Overall: optimisticStatementLimit},
	}
	if _, err := executor.RunSequenceDropping(ctx, pool, pt, []string{dropConstraintStatement(schema, table, constraint.Name)}, budget, executor.DefaultRetryPolicy(), constraint); err != nil {
		return err
	}
	return nil
}

// dropConstraintStatement is the statement that drops constraint name from
// schema.table, with every identifier quoted.
func dropConstraintStatement(schema, table, name string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", pgx.Identifier{schema, table}.Sanitize(), pgx.Identifier{name}.Sanitize())
}
