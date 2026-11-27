package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	if mixed, err := tableDefinitionDiffers(namespace, live, wanted); err != nil {
		return nil, err
	} else if mixed {
		return nil, &mixedRowSecurityChangeError{table: desired.Table()}
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

// tableDefinitionDiffers reports whether the file changes the table itself,
// compared with its row security set aside on both sides. A row security
// operation runs in one transaction that changes only the settings and
// policies, so a table change beside a row security delta must be planned on
// its own.
func tableDefinitionDiffers(namespace string, live, wanted schemadiff.Model) (bool, error) {
	live.RowSecurity, wanted.RowSecurity = schemadiff.RowSecurity{}, schemadiff.RowSecurity{}
	changes, err := schemadiff.Diff(namespace, live, wanted)
	if errors.Is(err, schemadiff.ErrUnsupportedChange) {
		// The table changes in a way the planner refuses, which is still a
		// change to its definition. Split from the row security change, the
		// table's file plans on its own and names that refusal.
		slog.Debug("table definition change beside a row security change is one the planner refuses; treating the file as mixed",
			"namespace", namespace, "table", live.Table, "error", err)
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("compare table %q.%q without its row security: %w", namespace, live.Table, err)
	}
	return len(changes) > 0, nil
}

// mixedRowSecurityChangeError refuses a file that changes a table's row
// security and its definition together. It is a schemadiff.ErrUnsupportedChange
// to the planner's refusal handling, without that sentinel's text trailing
// the remedy the author reads.
type mixedRowSecurityChangeError struct {
	table string
}

func (e *mixedRowSecurityChangeError) Error() string {
	return fmt.Sprintf("table %q changes its row security and its definition in one file, which SchemaBot cannot run as one reviewed change; change the row security in one pull request and the table in another", e.table)
}

func (e *mixedRowSecurityChangeError) Is(target error) bool {
	return target == schemadiff.ErrUnsupportedChange
}
