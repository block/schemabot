package ddl

import (
	"fmt"
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
	return canonical
}
