package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/preflight"
	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/block/pg-sprite/pkg/statement"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
)

// A policy replacement is one reviewed operation and one transaction. Never
// split its statements into ordinary independently committed DDL tasks.
func planRowSecurityOperation(ctx context.Context, pool *pgxpool.Pool, namespace string, desired statement.DesiredWithRowSecurity, tableOwner string) ([]engine.TableChange, error) {
	// Equality is a catalog comparison, not execution authority. Avoid taking
	// an exclusive target lock or requiring table ownership for unchanged files.
	live, err := schemadiff.Introspect(ctx, pool, namespace, desired.Table())
	if errors.Is(err, schemadiff.ErrTableNotFound) {
		return planCreateWithRowSecurity(ctx, pool, namespace, desired, tableOwner)
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

// planCreateWithRowSecurity plans a table the target lacks together with its
// row security, as one statement set that runs in one transaction: no other
// session ever sees the table without its policies, so a role granted every
// new table by default never reaches its rows unprotected. The table is new
// and holds no rows, so the change is not unsafe.
//
// The preview runs the create on the target and rolls it back, so the
// statements are the ones an apply would run; the apply runs them only if it
// derives the same sequence again.
func planCreateWithRowSecurity(ctx context.Context, pool *pgxpool.Pool, namespace string, desired statement.DesiredWithRowSecurity, tableOwner string) ([]engine.TableChange, error) {
	preview, err := executor.PreviewDesiredCreateWithRowSecurity(ctx, pool, namespace, tableOwner, desired, executor.Budget{
		LockTimeout: optimisticLockTimeout, StatementTimeout: optimisticStatementLimit,
	})
	if errors.Is(err, preflight.ErrRelationExists) {
		return nil, fmt.Errorf("table %q.%q was created while it was planned; plan again: %w", namespace, desired.Table(), err)
	}
	if r := classifyRefusal(err, desired.Table()); r != nil && r.provisionable {
		// The target lacks something the create needs, such as the schema
		// that would hold the table: the same verdict a table without row
		// security gets, so the plan shows the create blocked with what to
		// provision instead of failing.
		return []engine.TableChange{blockedCreateWithRowSecurity(desired, r.detail)}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("preview creating table %q.%q with its row security: %w", namespace, desired.Table(), err)
	}
	return []engine.TableChange{{
		Table: desired.Table(), Operation: ddl.StatementCreateTable,
		DDL: strings.Join(preview.Statements, ";\n"),
	}}, nil
}

// blockedCreateWithRowSecurity is the create of desired, shown as the file
// declares it, blocked with reason. The preview never ran, so the plan has no
// derived statements to show; the file's own are what the operator wrote.
func blockedCreateWithRowSecurity(desired statement.DesiredWithRowSecurity, reason string) engine.TableChange {
	statements := desired.Statements()
	sql := make([]string, 0, len(statements))
	for _, stmt := range statements {
		sql = append(sql, stmt.SQL())
	}
	return engine.TableChange{
		Table: desired.Table(), Operation: ddl.StatementCreateTable,
		DDL:           strings.Join(sql, ";\n"),
		ExecutionMode: engine.ExecutionModeBlocked,
		ModeReason:    reason,
	}
}

// createsTableWithRowSecurity reports whether planned row security DDL
// creates its table: the plan renders that change as the CREATE TABLE
// followed by its security statements. It reads the DDL, not the change's
// operation, because every drive path carries the DDL and not every one
// carries the operation.
func createsTableWithRowSecurity(script string) (bool, error) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	if err != nil {
		return false, fmt.Errorf("select PostgreSQL statement parser: %w", err)
	}
	statements, err := parser.Split(script)
	if err != nil {
		return false, fmt.Errorf("split planned DDL: %w", err)
	}
	if len(statements) == 0 {
		return false, fmt.Errorf("planned DDL holds no statement")
	}
	stmtType, _, err := parser.Classify(statements[0])
	if err != nil {
		return false, fmt.Errorf("classify the first planned statement: %w", err)
	}
	return stmtType == ddl.StatementCreateTable, nil
}

// validateCreateWithRowSecurityApply admits a planned create of a table with
// its row security. The planned statements are the reviewed sequence: the
// executor re-derives the sequence from the reviewed desired file and runs it
// only if the two are identical, so nothing the review did not show runs.
func validateCreateWithRowSecurityApply(req *engine.ApplyRequest) (nativeApply, error) {
	tc := req.Changes[0].TableChanges[0]
	namespace := req.Changes[0].Namespace
	if req.Options["defer_cutover"] == "true" {
		return nativeApply{}, fmt.Errorf("creating table %q with row security does not support deferred cutover", tc.Table)
	}
	statements, err := statement.Split(tc.DDL)
	if err != nil {
		return nativeApply{}, fmt.Errorf("split the planned create of table %q with its row security: %w", tc.Table, err)
	}
	reviewed := make([]string, 0, len(statements))
	for _, stmt := range statements {
		reviewed = append(reviewed, stmt.SQL)
	}
	found, err := reviewedRowSecurityDeclaration(req, namespace, tc.Table)
	if err != nil {
		return nativeApply{}, err
	}
	return nativeApply{namespace: namespace, table: tc.Table, sql: tc.DDL, steps: 1,
		rowSecurity: found, reviewedSecurity: reviewed, createsTable: true}, nil
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
	found, err := reviewedRowSecurityDeclaration(req, namespace, tc.Table)
	if err != nil {
		return nativeApply{}, err
	}
	return nativeApply{namespace: namespace, table: tc.Table, sql: tc.DDL, steps: 1,
		rowSecurity: found, reviewedSecurity: operation.Statements()}, nil
}

// reviewedRowSecurityDeclaration returns the one row security declaration for
// table among the reviewed desired schema files of namespace.
func reviewedRowSecurityDeclaration(req *engine.ApplyRequest, namespace, table string) (*statement.DesiredWithRowSecurity, error) {
	files := req.SchemaFiles[namespace]
	if files == nil {
		return nil, fmt.Errorf("row security apply requires the reviewed desired schema files")
	}
	var found *statement.DesiredWithRowSecurity
	for _, name := range sortedKeys(files.Files) {
		file, err := statement.ParseDesiredFile(files.Files[name])
		if err != nil {
			return nil, fmt.Errorf("parse schema file %q: %w", name, err)
		}
		desired, ok := file.RowSecurity()
		if !ok || desired.Table() != table {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("multiple row security declarations for table %q", table)
		}
		found = &desired
	}
	if found == nil {
		return nil, fmt.Errorf("row security declaration for table %q is required", table)
	}
	return found, nil
}
