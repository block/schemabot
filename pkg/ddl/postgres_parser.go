package ddl

import (
	"fmt"
	"regexp"
	"strings"

	pgproto "github.com/pganalyze/pg_query_go/v6"
	pgquery "github.com/wasilibs/go-pgquery"
)

// postgresStatementParser implements StatementParser over libpg_query — the
// PostgreSQL server's own parser, compiled to WebAssembly — so classification
// uses the real Postgres grammar rather than an approximation. Only the
// parse-tree protobuf types come from the pg_query_go module; parsing itself
// runs in-process through the Wasm build, which needs no cgo and contains a
// parser crash to the sandboxed module instead of the whole process.
type postgresStatementParser struct{}

// Split implements StatementParser. It parses the full content with the
// Postgres grammar and slices each statement's text out of the input using
// the parse tree's source offsets — verbatim source rather than a deparse —
// with surrounding whitespace trimmed from the input and from each statement.
func (postgresStatementParser) Split(content string) ([]string, error) {
	content = strings.TrimSpace(content)
	if content == "" {
		return nil, nil
	}
	result, err := pgquery.Parse(content)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL statements %q: %w", statementPreview(content), err)
	}
	var stmts []string
	for _, raw := range result.GetStmts() {
		span, err := statementSpan(content, raw)
		if err != nil {
			return nil, fmt.Errorf("failed to split SQL statements %q: %w", statementPreview(content), err)
		}
		stmt := strings.TrimSpace(span)
		if stmt != "" {
			stmts = append(stmts, stmt)
		}
	}
	return stmts, nil
}

// statementSpan returns the source text of one parsed statement using the
// parser's byte offsets into the original content. A zero length means the
// statement extends to the end of the input. Out-of-range offsets are an
// internal inconsistency between the parser and its input; they surface as an
// error rather than silently dropping a statement.
func statementSpan(content string, raw *pgproto.RawStmt) (string, error) {
	start := int(raw.GetStmtLocation())
	if start < 0 || start > len(content) {
		return "", fmt.Errorf("statement offset %d outside content of %d bytes", start, len(content))
	}
	end := len(content)
	if length := int(raw.GetStmtLen()); length > 0 {
		end = start + length
	}
	if end < start || end > len(content) {
		return "", fmt.Errorf("statement end offset %d outside content of %d bytes", end, len(content))
	}
	return content[start:end], nil
}

// Classify implements StatementParser. It mirrors the TiDB implementation's
// contract: exactly one statement per call, the bare (unqualified) name of the
// first referenced relation, and StatementUnknown without an error for
// statements that parse but fall outside the classified vocabulary.
func (postgresStatementParser) Classify(stmt string) (StatementType, string, error) {
	result, err := pgquery.Parse(stmt)
	if err != nil {
		return StatementUnknown, "", fmt.Errorf("classify statement %q: %w", statementPreview(stmt), err)
	}
	stmts := result.GetStmts()
	if len(stmts) == 0 {
		return StatementUnknown, "", fmt.Errorf("no classification result for statement %q", statementPreview(stmt))
	}
	if len(stmts) > 1 {
		return StatementUnknown, "", fmt.Errorf(
			"expected a single statement but %q parsed as %d statements; split with the parser's Split before classifying",
			statementPreview(stmt), len(stmts),
		)
	}
	stmtType, table := classifyPostgresNode(stmts[0].GetStmt())
	return stmtType, table, nil
}

// CreateTableColumns implements StatementParser using the parsed CREATE TABLE
// node, so quoted identifiers and PostgreSQL expressions follow the server
// grammar while table constraints are excluded by node type.
func (postgresStatementParser) CreateTableColumns(stmt string) ([]string, error) {
	result, err := pgquery.Parse(stmt)
	if err != nil {
		return nil, fmt.Errorf("parse CREATE TABLE %q: %w", statementPreview(stmt), err)
	}
	stmts := result.GetStmts()
	if len(stmts) != 1 {
		return nil, fmt.Errorf("expected one CREATE TABLE statement, got %d", len(stmts))
	}
	createNode, ok := stmts[0].GetStmt().GetNode().(*pgproto.Node_CreateStmt)
	if !ok {
		return nil, fmt.Errorf("expected CREATE TABLE statement")
	}
	columns := make([]string, 0, len(createNode.CreateStmt.GetTableElts()))
	for _, element := range createNode.CreateStmt.GetTableElts() {
		column, ok := element.GetNode().(*pgproto.Node_ColumnDef)
		if ok {
			columns = append(columns, column.ColumnDef.GetColname())
		}
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("CREATE TABLE has no columns")
	}
	return columns, nil
}

// postgresCreateTableColumn parses exactly one PostgreSQL CREATE TABLE
// statement and returns its parse result, the CREATE TABLE node, and the
// declaration node of the named column.
func postgresCreateTableColumn(createTableDDL, columnName string) (*pgproto.ParseResult, *pgproto.Node_CreateStmt, *pgproto.Node, error) {
	result, err := pgquery.Parse(createTableDDL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parse CREATE TABLE %q: %w", statementPreview(createTableDDL), err)
	}
	stmts := result.GetStmts()
	if len(stmts) != 1 {
		return nil, nil, nil, fmt.Errorf("expected one CREATE TABLE statement, got %d", len(stmts))
	}
	createNode, ok := stmts[0].GetStmt().GetNode().(*pgproto.Node_CreateStmt)
	if !ok {
		return nil, nil, nil, fmt.Errorf("expected CREATE TABLE statement")
	}
	for _, element := range createNode.CreateStmt.GetTableElts() {
		column, ok := element.GetNode().(*pgproto.Node_ColumnDef)
		if ok && column.ColumnDef.GetColname() == columnName {
			return result, createNode, element, nil
		}
	}
	return nil, nil, nil, fmt.Errorf("column %q not found in CREATE TABLE statement", columnName)
}

// SynthesizeAddColumn implements StatementParser. It grafts the column's
// ColumnDef parse node — type and column-level constraints intact — from the
// CREATE TABLE tree into a fresh ALTER TABLE ... ADD COLUMN tree and deparses
// it, so the output is libpg_query's normalized rendering rather than a
// textual slice of the input. Table-level constraints are separate TableElts
// nodes, not part of the ColumnDef, and are not carried.
func (postgresStatementParser) SynthesizeAddColumn(createTableDDL, columnName string) (string, error) {
	result, createNode, columnNode, err := postgresCreateTableColumn(createTableDDL, columnName)
	if err != nil {
		return "", err
	}

	result.Stmts = []*pgproto.RawStmt{{
		Stmt: &pgproto.Node{Node: &pgproto.Node_AlterTableStmt{AlterTableStmt: &pgproto.AlterTableStmt{
			Relation: createNode.CreateStmt.GetRelation(),
			Cmds: []*pgproto.Node{{Node: &pgproto.Node_AlterTableCmd{AlterTableCmd: &pgproto.AlterTableCmd{
				Subtype: pgproto.AlterTableType_AT_AddColumn,
				Def:     columnNode,
			}}}},
			Objtype: pgproto.ObjectType_OBJECT_TABLE,
		}}},
	}}
	ddl, err := pgquery.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("deparse ADD COLUMN for %q: %w", columnName, err)
	}
	return ddl, nil
}

// PostgresAddColumnManualReason reports why adding the named column from a
// CREATE TABLE declaration to a populated table needs manual remediation,
// or "" when the synthesized ADD COLUMN is safe to run automatically. The
// decision reads the column's parsed constraint list:
//
//   - Generated and identity columns rewrite the whole table under an
//     exclusive lock while PostgreSQL computes values for existing rows.
//   - NOT NULL without a DEFAULT needs a backfill — the server would reject
//     the ADD COLUMN outright on a populated table.
//   - A DEFAULT whose expression is not provably non-volatile (a constant,
//     a cast of a constant, or a SQL value function such as
//     CURRENT_TIMESTAMP) fails closed: the parse tree cannot see function
//     volatility, and a volatile default rewrites the whole table under an
//     exclusive lock.
//   - Constraint shapes not explicitly known to be safe fail closed.
func PostgresAddColumnManualReason(createTableDDL, columnName string) (string, error) {
	_, _, columnNode, err := postgresCreateTableColumn(createTableDDL, columnName)
	if err != nil {
		return "", err
	}
	column := columnNode.GetColumnDef()

	var notNull, hasDefault, constantDefault bool
	for _, node := range column.GetConstraints() {
		constraint := node.GetConstraint()
		switch constraint.GetContype() {
		case pgproto.ConstrType_CONSTR_NOTNULL:
			notNull = true
		case pgproto.ConstrType_CONSTR_DEFAULT:
			hasDefault = true
			constantDefault = postgresNonVolatileExpression(constraint.GetRawExpr())
		case pgproto.ConstrType_CONSTR_GENERATED, pgproto.ConstrType_CONSTR_IDENTITY:
			return "definition is generated or identity, which rewrites the whole table under an exclusive lock; add it manually", nil
		case pgproto.ConstrType_CONSTR_NULL, pgproto.ConstrType_CONSTR_UNIQUE, pgproto.ConstrType_CONSTR_FOREIGN:
			// These constraints are safe on a nullable new column.
		default:
			return fmt.Sprintf("definition has constraint %s, which is not safe for automatic convergence; add it manually", constraint.GetContype().String()), nil
		}
	}
	if hasDefault && !constantDefault {
		return "definition has a DEFAULT expression whose volatility cannot be proven from the statement alone, and a volatile default rewrites the whole table under an exclusive lock; add it manually or ship the column with a constant DEFAULT", nil
	}
	if notNull && !hasDefault {
		return "definition is NOT NULL without a DEFAULT; add it manually or ship the column with a DEFAULT", nil
	}
	return "", nil
}

// postgresNonVolatileExpression reports whether a DEFAULT expression is
// provably non-volatile from its parse tree: a constant, a cast whose
// argument is itself provably non-volatile, or a SQL value function
// (CURRENT_TIMESTAMP and friends, which PostgreSQL defines as STABLE).
// Function calls report false — the parse tree carries no volatility
// information, so even a stable function like now() cannot be proven safe
// without catalog access.
func postgresNonVolatileExpression(expr *pgproto.Node) bool {
	switch x := expr.GetNode().(type) {
	case *pgproto.Node_AConst:
		return true
	case *pgproto.Node_TypeCast:
		return postgresNonVolatileExpression(x.TypeCast.GetArg())
	case *pgproto.Node_SqlvalueFunction:
		return true
	default:
		return false
	}
}

// CreateIndex implements StatementParser using the parsed IndexStmt. Any
// standalone CREATE INDEX statement reports its index and table names, with
// unique carrying the UNIQUE declaration; other parsed statement types return
// an empty index name.
func (postgresStatementParser) CreateIndex(stmt string) (string, string, bool, error) {
	result, err := pgquery.Parse(stmt)
	if err != nil {
		return "", "", false, fmt.Errorf("parse CREATE INDEX %q: %w", statementPreview(stmt), err)
	}
	stmts := result.GetStmts()
	if len(stmts) != 1 {
		return "", "", false, fmt.Errorf("expected one statement, got %d", len(stmts))
	}
	indexNode, ok := stmts[0].GetStmt().GetNode().(*pgproto.Node_IndexStmt)
	if !ok {
		return "", "", false, nil
	}
	return indexNode.IndexStmt.GetIdxname(), indexNode.IndexStmt.GetRelation().GetRelname(), indexNode.IndexStmt.GetUnique(), nil
}

// CostScalesWithTableSize implements StatementParser. A standalone CREATE
// INDEX always scans the table. An ALTER TABLE scales when any command is not
// provably metadata-only: PRIMARY KEY, UNIQUE, and EXCLUDE adds build an
// index; FOREIGN KEY and CHECK adds validate every row unless declared NOT
// VALID; SET NOT NULL scans the table; a column type change can rewrite it;
// and a column add is metadata-only only while its DEFAULT (if any) is a
// constant. Plain drops, renames, and default changes report false.
func (postgresStatementParser) CostScalesWithTableSize(stmt string) (bool, error) {
	result, err := pgquery.Parse(stmt)
	if err != nil {
		return false, fmt.Errorf("parse statement %q: %w", statementPreview(stmt), err)
	}
	stmts := result.GetStmts()
	if len(stmts) != 1 {
		return false, fmt.Errorf(
			"expected a single statement but %q parsed as %d statements; split with the parser's Split first",
			statementPreview(stmt), len(stmts),
		)
	}
	switch node := stmts[0].GetStmt().GetNode().(type) {
	case *pgproto.Node_IndexStmt:
		return true, nil
	case *pgproto.Node_AlterTableStmt:
		for _, cmd := range node.AlterTableStmt.GetCmds() {
			if alterCmdScalesWithTableSize(cmd.GetAlterTableCmd()) {
				return true, nil
			}
		}
		return false, nil
	default:
		return false, nil
	}
}

// alterCmdScalesWithTableSize reports whether one ALTER TABLE command forces
// work proportional to the table's row count. The metadata-only cases are an
// allowlist: a command shape this function doesn't recognize is assumed to
// scale, so a new or exotic command over-reports size context rather than
// hiding it on an expensive change.
func alterCmdScalesWithTableSize(cmd *pgproto.AlterTableCmd) bool {
	switch cmd.GetSubtype() { //nolint:exhaustive
	case pgproto.AlterTableType_AT_DropColumn, pgproto.AlterTableType_AT_ColumnDefault,
		pgproto.AlterTableType_AT_DropNotNull, pgproto.AlterTableType_AT_DropConstraint:
		return false
	case pgproto.AlterTableType_AT_AddColumn:
		return addColumnScalesWithTableSize(cmd.GetDef().GetColumnDef())
	case pgproto.AlterTableType_AT_AddConstraint:
		constraint := cmd.GetDef().GetConstraint()
		switch constraint.GetContype() { //nolint:exhaustive
		case pgproto.ConstrType_CONSTR_PRIMARY, pgproto.ConstrType_CONSTR_UNIQUE,
			pgproto.ConstrType_CONSTR_EXCLUSION:
			// Index-backed constraints build their index regardless of NOT VALID.
			return true
		case pgproto.ConstrType_CONSTR_FOREIGN, pgproto.ConstrType_CONSTR_CHECK:
			// FOREIGN KEY and CHECK scan every row to validate unless the
			// constraint is declared NOT VALID.
			return !constraint.GetSkipValidation()
		default:
			return true
		}
	default:
		// AT_AlterColumnType can rewrite the table, AT_SetNotNull scans it,
		// and anything unrecognized is assumed expensive.
		return true
	}
}

// addColumnScalesWithTableSize reports whether adding this column forces a
// table rewrite or scan: an inline PRIMARY KEY or UNIQUE builds an index, a
// generated or identity column is computed for every existing row, and a
// non-constant DEFAULT is evaluated per row. A plain column with no DEFAULT
// (or a constant one) is metadata-only.
func addColumnScalesWithTableSize(col *pgproto.ColumnDef) bool {
	for _, c := range col.GetConstraints() {
		constraint := c.GetConstraint()
		switch constraint.GetContype() { //nolint:exhaustive
		case pgproto.ConstrType_CONSTR_PRIMARY, pgproto.ConstrType_CONSTR_UNIQUE,
			pgproto.ConstrType_CONSTR_GENERATED, pgproto.ConstrType_CONSTR_IDENTITY:
			return true
		case pgproto.ConstrType_CONSTR_DEFAULT:
			if !isConstantExpr(constraint.GetRawExpr()) {
				return true
			}
		}
	}
	return false
}

// isConstantExpr reports whether a default-value expression is a bare
// constant, possibly wrapped in type casts (DEFAULT 'x'::jsonb). Anything
// else — a function call, an operator expression — may be volatile, and a
// volatile default forces a full-table rewrite on ADD COLUMN.
func isConstantExpr(node *pgproto.Node) bool {
	switch expr := node.GetNode().(type) {
	case *pgproto.Node_AConst:
		return true
	case *pgproto.Node_TypeCast:
		return isConstantExpr(expr.TypeCast.GetArg())
	default:
		return false
	}
}

// classifyPostgresNode maps one parsed statement node onto the pkg/ddl-owned
// statement vocabulary and extracts the bare name of the first relation the
// statement targets. Postgres expresses a table rename as
// ALTER TABLE ... RENAME TO (a RenameStmt), so the rename forms classify by
// what they rename: the whole table is a rename, a column or constraint is an
// ordinary table alteration.
func classifyPostgresNode(node *pgproto.Node) (StatementType, string) {
	switch x := node.GetNode().(type) {
	case *pgproto.Node_CreateStmt:
		return StatementCreateTable, x.CreateStmt.GetRelation().GetRelname()
	case *pgproto.Node_AlterTableStmt:
		if x.AlterTableStmt.GetObjtype() == pgproto.ObjectType_OBJECT_TABLE {
			return StatementAlterTable, x.AlterTableStmt.GetRelation().GetRelname()
		}
		return StatementUnknown, ""
	case *pgproto.Node_DropStmt:
		switch x.DropStmt.GetRemoveType() {
		case pgproto.ObjectType_OBJECT_TABLE:
			return StatementDropTable, firstDropObjectName(x.DropStmt)
		case pgproto.ObjectType_OBJECT_INDEX:
			// DROP INDEX names only the index; unlike MySQL there is no
			// ON <table> clause, so no table name is available.
			return StatementDropIndex, ""
		default:
			return StatementUnknown, ""
		}
	case *pgproto.Node_RenameStmt:
		switch x.RenameStmt.GetRenameType() {
		case pgproto.ObjectType_OBJECT_TABLE:
			return StatementRenameTable, x.RenameStmt.GetRelation().GetRelname()
		case pgproto.ObjectType_OBJECT_COLUMN, pgproto.ObjectType_OBJECT_TABCONSTRAINT:
			return StatementAlterTable, x.RenameStmt.GetRelation().GetRelname()
		default:
			return StatementUnknown, ""
		}
	case *pgproto.Node_TruncateStmt:
		relations := x.TruncateStmt.GetRelations()
		if len(relations) == 0 {
			return StatementTruncateTable, ""
		}
		return StatementTruncateTable, relations[0].GetRangeVar().GetRelname()
	case *pgproto.Node_IndexStmt:
		return StatementCreateIndex, x.IndexStmt.GetRelation().GetRelname()
	case *pgproto.Node_ViewStmt:
		return StatementCreateView, x.ViewStmt.GetView().GetRelname()
	case *pgproto.Node_InsertStmt:
		return StatementInsert, x.InsertStmt.GetRelation().GetRelname()
	case *pgproto.Node_UpdateStmt:
		return StatementUpdate, x.UpdateStmt.GetRelation().GetRelname()
	case *pgproto.Node_DeleteStmt:
		return StatementDelete, x.DeleteStmt.GetRelation().GetRelname()
	default:
		return StatementUnknown, ""
	}
}

// firstDropObjectName returns the bare name of the first object a DROP
// statement targets. Drop targets are qualified-name component lists rather
// than relation references, so the last component is the object's own name.
func firstDropObjectName(drop *pgproto.DropStmt) string {
	for _, object := range drop.GetObjects() {
		items := object.GetList().GetItems()
		if len(items) == 0 {
			continue
		}
		return items[len(items)-1].GetString_().GetSval()
	}
	return ""
}

// Canonicalize implements StatementParser. It round-trips the statement
// through the Postgres parser's deparser, which normalizes formatting the way
// the server itself would render the statement. It returns the input
// unchanged when parsing or deparsing fails or when the input contains more
// than one statement, so canonicalization never truncates its input.
func (postgresStatementParser) Canonicalize(ddl string) string {
	result, err := pgquery.Parse(ddl)
	if err != nil || len(result.GetStmts()) != 1 {
		return ddl
	}
	canonical, err := pgquery.Deparse(result)
	if err != nil {
		return ddl
	}
	return restoreDropColumnKeyword(result.GetStmts()[0].GetStmt(), canonical)
}

// restoreDropColumnKeyword reinstates the optional COLUMN keyword the
// deparser elides from ALTER TABLE ... DROP COLUMN subcommands. The elided
// form is valid PostgreSQL, but the canonical statement is a review surface a
// human approves before a destructive apply, and "DROP legacy" next to an
// explicit "DROP CONSTRAINT" no longer says that a column is what is going;
// the explicit spelling is also the one pg_dump emits. The parse tree names
// exactly which columns are dropped, so only those clauses are rewritten and
// every other DROP subcommand (CONSTRAINT, DEFAULT, NOT NULL, IDENTITY,
// EXPRESSION) passes through untouched.
func restoreDropColumnKeyword(stmt *pgproto.Node, canonical string) string {
	for _, cmd := range stmt.GetAlterTableStmt().GetCmds() {
		alterCmd := cmd.GetAlterTableCmd()
		if alterCmd.GetSubtype() != pgproto.AlterTableType_AT_DropColumn {
			continue
		}
		// The deparser renders the column bare when it can and quoted when it
		// must; match either rendering, bounded so one column's clause can
		// never rewrite a longer name it prefixes.
		bare := regexp.QuoteMeta(alterCmd.GetName())
		quoted := regexp.QuoteMeta(`"` + strings.ReplaceAll(alterCmd.GetName(), `"`, `""`) + `"`)
		pattern := regexp.MustCompile(`\bDROP ((?:IF EXISTS )?(?:` + quoted + `|` + bare + `))($|[ ,])`)
		canonical = pattern.ReplaceAllString(canonical, "DROP COLUMN $1$2")
	}
	return canonical
}
