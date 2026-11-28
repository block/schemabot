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
	reason := "Row security changes alter who can access rows, so review the complete policy and settings replacement"
	if denied := deniedCommands(wanted.RowSecurity); len(denied) > 0 {
		reason += "; " + denyReason(denied)
	}
	return []engine.TableChange{{
		Table: desired.Table(), Operation: ddl.StatementAlterTable,
		DDL:      strings.Join(preview.Statements, ";\n"),
		IsUnsafe: true, UnsafeReason: reason,
	}}, nil
}

// policyGrant is a command a row security policy can grant, with what a role
// subject to row security gets from that command when no permissive policy
// grants it.
type policyGrant struct {
	command schemadiff.PolicyCommand
	name    string
	denied  string
}

// policyGrants are the commands a policy can grant, in the order a reason
// names them.
var policyGrants = []policyGrant{
	{schemadiff.PolicySelect, "SELECT", "reads no rows"},
	{schemadiff.PolicyInsert, "INSERT", "has its inserts refused"},
	{schemadiff.PolicyUpdate, "UPDATE", "changes no rows with its updates"},
	{schemadiff.PolicyDelete, "DELETE", "changes no rows with its deletes"},
}

// deniedCommands returns the commands that row security that is on denies
// to every role subject to it: those no permissive policy grants, either
// for that command or for ALL. PostgreSQL grants a row only through a
// permissive policy, and restrictive policies only narrow what those grant,
// so a command with no permissive policy reaches no row.
func deniedCommands(rs schemadiff.RowSecurity) []policyGrant {
	if !rs.Enabled {
		return nil
	}
	var denied []policyGrant
	for _, grant := range policyGrants {
		if !grantsCommand(rs.Policies, grant.command) {
			denied = append(denied, grant)
		}
	}
	return denied
}

// grantsCommand reports whether a permissive policy grants command, either
// on its own or as part of ALL.
func grantsCommand(policies []schemadiff.Policy, command schemadiff.PolicyCommand) bool {
	for _, policy := range policies {
		if policy.Permissive && (policy.Command == command || policy.Command == schemadiff.PolicyAll) {
			return true
		}
	}
	return false
}

// denyReason names what row security that denies the given commands does to
// the application, so the plan says it rather than leaving the reviewer to
// work it out from the policies. It is one finding, so it carries no "; ",
// which separates the findings a reason lists.
func denyReason(denied []policyGrant) string {
	if len(denied) == len(policyGrants) {
		return "after this change row security is on with no policy that grants access, so every role subject to it reads no rows, its inserts are refused, and its updates and deletes change no rows: declare a permissive policy in the same file"
	}
	names := make([]string, len(denied))
	effects := make([]string, len(denied))
	for i, grant := range denied {
		names[i], effects[i] = grant.name, grant.denied
	}
	return fmt.Sprintf("after this change no permissive policy grants %s, so every role subject to row security %s: if the application runs %s, declare a permissive policy for %s in the same file",
		joinSeries(names, "or"), joinSeries(effects, "and"), plural(len(denied), "it", "them"), plural(len(denied), "it", "them"))
}

// joinSeries joins items as an English series with conjunction before the
// last one.
func joinSeries(items []string, conjunction string) string {
	switch len(items) {
	case 0:
		return ""
	case 1:
		return items[0]
	case 2:
		return items[0] + " " + conjunction + " " + items[1]
	}
	return strings.Join(items[:len(items)-1], ", ") + ", " + conjunction + " " + items[len(items)-1]
}

// unsupportedRowSecurityShape turns the executor's refusal of a table shape
// the row security path cannot manage, such as a table in a foreign key
// relationship, into a plan refusal that names the table and the shape. The
// refusal stays until the file or the table changes, so planning again
// cannot clear it. Any other error, privileges included, is not one.
//
// A table the target lacks has an empty live model: its keys are the file's
// own, so the refusal names the shape without listing them.
//
// A table that has no row security yet gets its other changes planned once
// the file declares none. A table that already has some keeps it whatever
// the file says, and a file that stops declaring row security SchemaBot
// applied is refused (RV-4), so there the author makes the file's
// statements match the table's settings and policies instead.
func unsupportedRowSecurityShape(namespace string, live schemadiff.Model, err error) (error, bool) {
	if !errors.Is(err, executor.ErrRowSecurityRefused) || errors.Is(err, executor.ErrRowSecurityPrivileges) {
		return nil, false
	}
	shape, ok := schemadiff.RenderRefusal(err)
	if !ok {
		return nil, false
	}
	why := rowSecurityShapePhrase(shape)
	if keys := foreignKeyRelationships(namespace, live); errors.Is(shape, schemadiff.ErrUnrenderableForeignKey) && len(keys) > 0 {
		why = "it is in a foreign key relationship (" + strings.Join(quoteAll(keys), ", ") + ")"
	}
	next := "remove the row security statements from its schema file, then plan again"
	if hasRowSecurity(live.RowSecurity) {
		next = "make the row security statements in its schema file match the table's current settings and policies, then plan again"
	}
	return &engine.PlanRefusedError{Err: &rowSecurityShapeError{table: live.Table, why: why, next: next}}, true
}

// rowSecurityShapeError is a table shape the row security path cannot
// manage. Each one is about its own table and file, so the plan reports
// every such table in a namespace at once rather than stopping at the
// first, unless another file in that namespace fails the plan first.
type rowSecurityShapeError struct {
	table, why, next string
}

func (e *rowSecurityShapeError) Error() string {
	return fmt.Sprintf("SchemaBot does not manage row security on table %q yet, because %s; %s", e.table, e.why, e.next)
}

// rowSecurityShapePhrases says, in the operator's terms, what about a table
// keeps the row security path from managing it. The planning library's own
// text speaks of rendering a desired schema, which the operator never sees.
var rowSecurityShapePhrases = []struct {
	shape  error
	phrase string
}{
	{schemadiff.ErrUnrenderableDefault, "it has a column whose default draws from a sequence other than the one a serial column declares"},
	{schemadiff.ErrMultipleOwnedSequences, "it has a column whose default uses more than one sequence the column owns"},
	{schemadiff.ErrUnrenderablePartition, "it is a partition of a partitioned table"},
	{schemadiff.ErrUnrenderableInheritance, "it uses table inheritance"},
	{schemadiff.ErrUnrenderableForeignKey, "it is in a foreign key relationship"},
	{schemadiff.ErrUnrenderableUnlogged, "it is unlogged"},
	{schemadiff.ErrUnrenderableCollation, "it has a column with an explicit collation"},
	{schemadiff.ErrUnrenderableVirtual, "it has a virtual generated column"},
	{schemadiff.ErrUnrenderableToastOptions, "it sets TOAST storage parameters"},
	{schemadiff.ErrUnrenderableSequenceOptions, "it owns a sequence with options other than the defaults"},
	{schemadiff.ErrUnrenderableMergedConstraints, "it has constraints that repeat each other on the same columns"},
	{schemadiff.ErrUnrenderableUnattachedIndex, "it is a partitioned table with an index that is not attached on every partition"},
	{schemadiff.ErrUnrenderablePartitionedRowSecurity, "it is a partitioned table"},
	{schemadiff.ErrUnrenderableRowSecurity, "its row security settings are of a kind SchemaBot cannot compare yet"},
}

// rowSecurityShapePhrase names the shape, or says only that the table's
// definition is the cause when the planning library reports a shape this
// build has no phrase for.
func rowSecurityShapePhrase(shape error) string {
	for _, p := range rowSecurityShapePhrases {
		if errors.Is(shape, p.shape) {
			return p.phrase
		}
	}
	slog.Warn("row security refusal names a table shape with no phrase; rendering the generic one", "shape", shape.Error())
	return "of a part of its definition SchemaBot cannot compare yet"
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
	if refusal, ok := unsupportedRowSecurityShape(namespace, schemadiff.Model{Table: desired.Table()}, err); ok {
		return nil, refusal
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
