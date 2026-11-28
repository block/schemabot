package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/preflight"
	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/block/pg-sprite/pkg/statement"
	"github.com/jackc/pgx/v5/pgconn"
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
	if mixed, err := tableDefinitionDiffers(namespace, live, wanted); err != nil {
		return nil, err
	} else if mixed {
		return nil, &mixedRowSecurityChangeError{table: desired.Table()}
	}
	// A delta still needs the executor's locked admission and fresh comparison.
	preview, err := executor.PreviewRowSecurity(ctx, pool, namespace, desired, executor.Budget{
		LockTimeout: optimisticLockTimeout, StatementTimeout: optimisticStatementLimit,
	})
	if refusal, ok := unsupportedRowSecurityShape(namespace, live, err); ok {
		return nil, refusal
	}
	if err != nil {
		return nil, err
	}
	if len(preview.Statements) == 0 {
		return nil, nil
	}
	reason := "Row security changes alter who can access rows; review the complete policy and settings replacement"
	if deniesEveryRow(wanted.RowSecurity) {
		reason += ". " + denyAllReason
	}
	return []engine.TableChange{{
		Table: desired.Table(), Operation: ddl.StatementAlterTable,
		DDL:      strings.Join(preview.Statements, ";\n"),
		IsUnsafe: true, UnsafeReason: reason,
	}}, nil
}

// denyAllReason names what a table with row security on and no permissive
// policy does to the application, so the plan says it rather than leaving
// the reviewer to work it out from the statements.
const denyAllReason = "After this change row security is on with no policy that grants access, so every role subject to it reads no rows and every write fails; declare a permissive policy in the same file"

// deniesEveryRow reports row security that is on with no permissive
// policy. PostgreSQL grants a row only through a permissive policy, and
// restrictive policies only narrow what those grant, so such a table hides
// every row from every role subject to row security.
func deniesEveryRow(rs schemadiff.RowSecurity) bool {
	if !rs.Enabled {
		return false
	}
	for _, policy := range rs.Policies {
		if policy.Permissive {
			return false
		}
	}
	return true
}

// unsupportedRowSecurityShape turns the executor's refusal of a table shape
// the row security path cannot manage, such as a table in a foreign key
// relationship, into a plan refusal that names the table and the shape. The
// refusal stays until the file or the table changes, so planning again
// cannot clear it. Any other error, privileges included, is not one.
func unsupportedRowSecurityShape(namespace string, live schemadiff.Model, err error) (error, bool) {
	if !errors.Is(err, executor.ErrRowSecurityRefused) || errors.Is(err, executor.ErrRowSecurityPrivileges) {
		return nil, false
	}
	shape, ok := schemadiff.RenderRefusal(err)
	if !ok {
		return nil, false
	}
	why := ": " + shape.Error()
	if errors.Is(shape, schemadiff.ErrUnrenderableForeignKey) {
		why = ", because it is in a foreign key relationship (" + strings.Join(quoteAll(foreignKeyRelationships(namespace, live)), ", ") + ")"
	}
	return &engine.PlanRefusedError{Err: fmt.Errorf(
		"SchemaBot does not manage row security on table %q yet%s; remove the row security statements from its schema file, then plan again",
		live.Table, why)}, true
}

// foreignKeyRelationships names a table's own foreign keys and the foreign
// keys of other tables that reference it, as "table.constraint".
func foreignKeyRelationships(namespace string, m schemadiff.Model) []string {
	var keys []string
	for _, con := range m.Constraints {
		if con.ForeignKey {
			keys = append(keys, m.Table+"."+con.Name)
		}
	}
	for _, fk := range m.IncomingForeignKeys {
		keys = append(keys, foreignKeyName(namespace, fk))
	}
	return keys
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
	if missing, ok := readsMissingTable(err, desired.Table()); ok {
		return nil, &engine.PlanRefusedError{Err: missing}
	}
	if err != nil {
		return nil, fmt.Errorf("preview creating table %q.%q with its row security: %w", namespace, desired.Table(), err)
	}
	return []engine.TableChange{{
		Table: desired.Table(), Operation: ddl.StatementCreateTable,
		DDL: strings.Join(preview.Statements, ";\n"),
	}}, nil
}

// readsMissingTable reports a preview that failed because the file reads a
// table the target does not have, most often a policy that reads a table the
// same plan creates. The preview runs the file's statements on the target,
// where that table does not exist yet, so the create cannot be proven. The
// server's message names the relation the file itself named.
func readsMissingTable(err error, table string) (error, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != sqlstateUndefinedTable {
		return nil, false
	}
	return errors.New(sanitizeReasonText(fmt.Sprintf(
		"table %q reads a table the target does not have yet (%s). The plan proves the create on the target, so a table its statements read must already exist there: apply that table first, then add %q in a later change",
		table, pgErr.Message, table))), true
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
