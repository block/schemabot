package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/block/pg-sprite/pkg/statement"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
)

// A policy replacement is one reviewed operation and one transaction. Never
// split its statements into ordinary independently committed DDL tasks.
func planRowSecurityOperation(ctx context.Context, pool *pgxpool.Pool, namespace string, desired statement.DesiredWithRowSecurity) ([]engine.TableChange, error) {
	// Equality is a catalog comparison, not execution authority. Avoid taking
	// an exclusive target lock or requiring table ownership for unchanged files.
	live, err := schemadiff.Introspect(ctx, pool, namespace, desired.Table())
	if errors.Is(err, schemadiff.ErrTableNotFound) {
		return nil, fmt.Errorf("creating table %q.%q with managed row security is not supported: %w", namespace, desired.Table(), schemadiff.ErrUnsupportedChange)
	}
	if err != nil {
		return nil, err
	}
	wanted, err := schemadiff.IntrospectDesiredWithRowSecurity(ctx, pool, namespace, desired)
	if err != nil {
		return nil, err
	}
	if _, err := schemadiff.DiffWithRowSecurity(namespace, live, wanted); err == nil {
		return nil, nil
	} else if !errors.Is(err, schemadiff.ErrUnsupportedChange) {
		return nil, err
	}
	// A delta still needs the executor's locked admission and fresh comparison.
	preview, err := executor.PreviewRowSecurity(ctx, pool, namespace, desired, executor.Budget{
		LockTimeout: optimisticLockTimeout, StatementTimeout: optimisticStatementLimit,
	})
	if err != nil {
		return nil, err
	}
	if len(preview.Statements) == 0 {
		return nil, nil
	}
	return []engine.TableChange{{
		Table: desired.Table(), Operation: ddl.StatementAlterTable,
		DDL:      strings.Join(preview.Statements, ";\n"),
		IsUnsafe: true, UnsafeReason: "Row security changes alter who can access rows; review the complete policy and settings replacement",
	}}, nil
}

func validateRowSecurityApply(req *engine.ApplyRequest, operation statement.RowSecurityChange) (nativeApply, error) {
	tc := req.Changes[0].TableChanges[0]
	namespace := req.Changes[0].Namespace
	if operation.Schema() != namespace || operation.Table() != tc.Table {
		return nativeApply{}, fmt.Errorf("row security operation target differs: SQL targets %q.%q, apply targets %q.%q", operation.Schema(), operation.Table(), namespace, tc.Table)
	}
	if req.Options["defer_cutover"] == "true" {
		return nativeApply{}, fmt.Errorf("row security changes do not support deferred cutover")
	}
	files := req.SchemaFiles[namespace]
	if files == nil {
		return nativeApply{}, fmt.Errorf("row security apply requires the reviewed desired schema files")
	}
	var found *statement.DesiredWithRowSecurity
	for _, name := range sortedKeys(files.Files) {
		file, err := statement.ParseDesiredFile(files.Files[name])
		if err != nil {
			return nativeApply{}, fmt.Errorf("parse schema file %q: %w", name, err)
		}
		desired, ok := file.RowSecurity()
		if !ok || desired.Table() != tc.Table {
			continue
		}
		if found != nil {
			return nativeApply{}, fmt.Errorf("multiple row security declarations for table %q", tc.Table)
		}
		found = &desired
	}
	if found == nil {
		return nativeApply{}, fmt.Errorf("row security declaration for table %q is required", tc.Table)
	}
	return nativeApply{namespace: namespace, table: tc.Table, sql: tc.DDL, steps: 1,
		rowSecurity: found, reviewedSecurity: operation.Statements()}, nil
}
