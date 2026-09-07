package ddl

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/statement"

	"github.com/block/schemabot/pkg/schema"
)

// StatementParser abstracts the dialect-specific, string-level DDL parsing
// operations that pkg/ddl exposes to its callers (schema discovery, planning,
// the CLI): splitting a schema file into statements, classifying a statement,
// inspecting CREATE TABLE columns, and canonicalizing a statement.
//
// The default implementation wraps the TiDB parser (via Spirit's statement
// package), so behavior for MySQL and Vitess is exactly what it has always
// been. A non-MySQL dialect (e.g. Postgres) can supply its own implementation
// without changing any pkg/ddl caller.
//
// The seam is intentionally scoped to these string-level operations. The
// TiDB-AST-based differ, validators, and linters remain dialect-specific by
// design — a non-MySQL engine supplies its own equivalents rather than reusing
// the MySQL ones.
type StatementParser interface {
	// Split divides SQL file content into individual DDL statement strings.
	// Empty content yields a nil slice and no error.
	Split(content string) ([]string, error)

	// Classify returns the statement type and table name for a single
	// statement. It rejects multi-statement input so a destructive statement
	// cannot hide behind the classification of the first one.
	Classify(stmt string) (StatementType, string, error)

	// CreateTableColumns returns the declared column names from exactly one
	// CREATE TABLE statement. Table-level constraints are not columns.
	CreateTableColumns(stmt string) ([]string, error)

	// CreateIndex returns the index and table names from exactly one
	// standalone CREATE INDEX statement, with unique reporting whether the
	// statement declares UNIQUE. Other statement shapes return an empty index
	// name without an error.
	CreateIndex(stmt string) (indexName, tableName string, unique bool, err error)

	// SynthesizeAddColumn builds an ALTER TABLE ... ADD COLUMN statement for
	// the named column of exactly one CREATE TABLE statement, carrying the
	// column's declaration verbatim at the parse-tree level: its type and all
	// column-level constraints. Table-level constraints (PRIMARY KEY (...),
	// UNIQUE (...), CHECK (...)) are not part of a column declaration and are
	// not carried. The result is the parser's normalized rendering of the
	// synthesized statement, not a textual slice of the input DDL.
	//
	// The seam is faithful by design and judges nothing: a NOT NULL column
	// without a DEFAULT synthesizes exactly as declared even though PostgreSQL
	// rejects that ALTER on a populated table. Whether the synthesized
	// statement can be applied is the caller's decision.
	//
	// columnName is matched against the parser-folded column name — the
	// values CreateTableColumns returns — so an unquoted "Email" in the DDL
	// is found as "email", and only a quoted identifier keeps its case.
	SynthesizeAddColumn(createTableDDL, columnName string) (string, error)

	// CostScalesWithTableSize reports whether exactly one DDL statement's
	// execution cost grows with the size of an existing table: an index
	// build, a table copy or rebuild, or a full-table scan to validate a
	// constraint. Whether a given ALTER runs as instant DDL is decided by the
	// server at execution time, so this is a conservative statement-level
	// judgement: it reports false only for clause shapes that are provably
	// metadata-only, and true for anything it cannot prove cheap. Statements
	// that don't touch an existing table's data (CREATE TABLE, DROP TABLE,
	// ...) report false without an error.
	CostScalesWithTableSize(stmt string) (bool, error)

	// Canonicalize normalizes a single DDL statement's formatting, returning
	// the input unchanged when it cannot be parsed.
	Canonicalize(ddl string) string
}

// defaultParser backs the package-level SplitStatements, ClassifyStatement, and
// Canonicalize helpers. It is the TiDB/Spirit implementation so existing MySQL
// and Vitess behavior is unchanged. Later work (Postgres support) selects a
// dialect-specific parser at the call sites that know the database type.
var defaultParser StatementParser = tidbStatementParser{}

// ParserForDialect returns the StatementParser for a database family. The
// MySQL family (MySQL, Vitess, Strata) shares the TiDB parser; Postgres gets
// the libpg_query parser. An unregistered dialect is an error rather than a
// silent fallback, so a mislabeled target can never have its DDL classified
// by another family's grammar. Callers holding a database_type derive the
// dialect with schema.DialectForDatabaseType.
func ParserForDialect(dialect schema.Dialect) (StatementParser, error) {
	switch dialect {
	case schema.DialectMySQL:
		return tidbStatementParser{}, nil
	case schema.DialectPostgres:
		return postgresStatementParser{}, nil
	default:
		return nil, fmt.Errorf("no statement parser registered for dialect %q", dialect)
	}
}

// CreateSet is a parsed greenfield create set, or a single statement of any
// type. Statements preserves the parser's statement order.
type CreateSet struct {
	Statements []string
	Type       StatementType
	Table      string
}

// StatementType reports the type of Statements[i] as ParseCreateSet admitted
// it: a lone statement carries its own classification, and every statement
// after the first in a multi-statement set is a CREATE INDEX, because
// admission refuses any other shape. Callers read the type from here instead
// of classifying the statement again.
func (s CreateSet) StatementType(i int) StatementType {
	if i == 0 {
		return s.Type
	}
	return StatementCreateIndex
}

// ParseCreateSet splits and classifies a DDL script, admitting either one
// statement or a greenfield create set: one CREATE TABLE followed only by
// CREATE INDEX statements on that same table.
//
// Multi-statement create sets are currently a PostgreSQL-parser capability.
// The MySQL parser's Split reports one entry per statement, but each entry
// that Spirit does not rewrite carries the whole script's text rather than
// that statement alone, so the entries cannot be classified one at a time;
// Classify rejects the first entry as multi-statement input and the script is
// refused with that cause.
func ParseCreateSet(p StatementParser, script string) (CreateSet, error) {
	statements, err := p.Split(script)
	if err != nil {
		return CreateSet{}, fmt.Errorf("split DDL script: %w", err)
	}
	if len(statements) == 0 {
		return CreateSet{}, fmt.Errorf("DDL script contains no statements")
	}

	firstType, table, err := p.Classify(statements[0])
	if err != nil && len(statements) == 1 {
		return CreateSet{}, fmt.Errorf("classify DDL statement: %w", err)
	}
	if err != nil {
		return CreateSet{}, fmt.Errorf("classify statement 1 of %d in multi-statement DDL script: %w", len(statements), err)
	}
	result := CreateSet{Statements: statements, Type: firstType, Table: table}
	if len(statements) == 1 {
		return result, nil
	}

	if firstType != StatementCreateTable {
		return CreateSet{}, fmt.Errorf("statement 1 is %s; a multi-statement DDL script must start with CREATE TABLE", firstType)
	}
	createTarget, err := createSetRelation(p, statements[0], table)
	if err != nil {
		return CreateSet{}, fmt.Errorf("identify CREATE TABLE target: %w", err)
	}
	for i, stmt := range statements[1:] {
		stmtType, indexTable, err := p.Classify(stmt)
		if err != nil {
			return CreateSet{}, fmt.Errorf("classify statement %d in multi-statement DDL script: %w", i+2, err)
		}
		if stmtType != StatementCreateIndex {
			return CreateSet{}, fmt.Errorf("statement %d is %s; a multi-statement DDL script must be a CREATE TABLE followed only by CREATE INDEX statements on that table", i+2, stmtType)
		}
		indexTarget, err := createSetRelation(p, stmt, indexTable)
		if err != nil {
			return CreateSet{}, fmt.Errorf("identify statement %d index target: %w", i+2, err)
		}
		if indexTarget.qualified() != createTarget.qualified() {
			return CreateSet{}, fmt.Errorf("statement %d creates an index on %s while CREATE TABLE targets %s; a create set cannot resolve an unqualified name against a search_path, so both must name the schema the same way", i+2, indexTarget, createTarget)
		}
		if indexTarget != createTarget {
			return CreateSet{}, fmt.Errorf("statement %d creates an index on table %s, not CREATE TABLE target %s", i+2, indexTarget, createTarget)
		}
	}
	return result, nil
}

// relationIdentity is the parsed (schema, name) pair naming a relation. The
// two parts are compared as a pair, never as one joined string, so a relation
// whose bare name contains a dot can never be mistaken for a schema-qualified
// one. Schema is empty when the statement did not qualify the name.
type relationIdentity struct {
	Schema string
	Name   string
}

func (r relationIdentity) qualified() bool { return r.Schema != "" }

// String renders the identity for error messages, quoting each part so that
// a dot inside a bare name reads differently from a schema qualifier.
func (r relationIdentity) String() string {
	if !r.qualified() {
		return fmt.Sprintf("%q", r.Name)
	}
	return fmt.Sprintf("%q.%q", r.Schema, r.Name)
}

// createSetRelationParser is implemented by a StatementParser whose grammar
// carries a schema qualifier on CREATE TABLE and CREATE INDEX targets.
type createSetRelationParser interface {
	createSetRelation(stmt string) (relationIdentity, error)
}

// createSetRelation identifies the relation a create-set statement targets.
// The identity is taken exactly as written — an unqualified name is never
// resolved against a search_path — so a create set is only admitted when
// every statement qualifies its target the same way. Every ParseCreateSet
// caller today hands it plan-emitted DDL, which the planner qualifies
// uniformly; an operator-authored set that mixes qualified and unqualified
// names is refused with a message naming that cause rather than guessed at.
// A parser without a schema-aware seam falls back to Classify's bare table
// name, which is the whole identity its grammar can express.
func createSetRelation(p StatementParser, stmt, classifiedTable string) (relationIdentity, error) {
	if relationParser, ok := p.(createSetRelationParser); ok {
		return relationParser.createSetRelation(stmt)
	}
	return relationIdentity{Name: classifiedTable}, nil
}

// CreateSetStatements has the same admission rules as ParseCreateSet. It is
// retained for callers that need only the split statements. Multi-statement
// create sets are a PostgreSQL-parser capability; see ParseCreateSet for why
// the MySQL parser refuses them.
func CreateSetStatements(p StatementParser, script string) ([]string, error) {
	createSet, err := ParseCreateSet(p, script)
	if err != nil {
		return nil, err
	}
	return createSet.Statements, nil
}

// tidbStatementParser implements StatementParser over the TiDB parser via
// Spirit's statement package — the behavior pkg/ddl has always had.
type tidbStatementParser struct{}

// Split implements StatementParser.
func (tidbStatementParser) Split(content string) ([]string, error) {
	content = strings.TrimSpace(content)
	if content == "" {
		return nil, nil
	}
	parsed, err := statement.NewWithOptions(content, statement.Options{
		AllowMixedStatementTypes: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL statements %q: %w", statementPreview(content), err)
	}
	var stmts []string
	for _, s := range parsed {
		stmt := strings.TrimSpace(s.Statement)
		if stmt != "" {
			stmts = append(stmts, stmt)
		}
	}
	return stmts, nil
}

// Classify implements StatementParser.
func (tidbStatementParser) Classify(stmt string) (StatementType, string, error) {
	results, err := statement.Classify(stmt)
	if err != nil {
		return StatementUnknown, "", fmt.Errorf("classify statement %q: %w", statementPreview(stmt), err)
	}
	if len(results) == 0 {
		return StatementUnknown, "", fmt.Errorf("no classification result for statement %q", statementPreview(stmt))
	}
	if len(results) > 1 {
		return StatementUnknown, "", fmt.Errorf(
			"expected a single statement but %q parsed as %d statements; split with SplitStatements before classifying",
			statementPreview(stmt), len(results),
		)
	}
	return statementTypeFromSpirit(results[0].Type), results[0].Table, nil
}

// CreateTableColumns implements StatementParser.
func (tidbStatementParser) CreateTableColumns(stmt string) ([]string, error) {
	createTable, err := statement.ParseCreateTable(stmt)
	if err != nil {
		return nil, fmt.Errorf("parse CREATE TABLE %q: %w", statementPreview(stmt), err)
	}
	columns := make([]string, 0, len(createTable.Columns))
	for _, column := range createTable.Columns {
		columns = append(columns, column.Name)
	}
	return columns, nil
}

// CreateIndex implements StatementParser. This inspection operation is
// currently needed only by the PostgreSQL storage bootstrapper.
func (tidbStatementParser) CreateIndex(string) (string, string, bool, error) {
	return "", "", false, fmt.Errorf("CREATE INDEX inspection is not supported by the MySQL statement parser")
}

// SynthesizeAddColumn implements StatementParser. This synthesis operation is
// currently needed only by the PostgreSQL storage bootstrapper; the MySQL
// bootstrapper diffs schemas with Spirit instead.
func (tidbStatementParser) SynthesizeAddColumn(string, string) (string, error) {
	return "", fmt.Errorf("ADD COLUMN synthesis is not supported by the MySQL statement parser")
}

// CostScalesWithTableSize implements StatementParser. A standalone CREATE
// INDEX always scans the table. An ALTER TABLE scales when any clause is not
// provably metadata-only: index-backed constraint adds build an index, FOREIGN
// KEY and CHECK adds validate every row, and column type changes, charset
// conversions, and table-option rebuilds copy the table. Clause shapes MySQL
// executes on metadata alone — plain column adds, renames, default changes,
// constraint and index drops, visibility toggles, partition drops and
// truncations, and comment-only option changes — report false.
func (tidbStatementParser) CostScalesWithTableSize(stmt string) (bool, error) {
	p := parser.New()
	stmtNodes, _, err := p.Parse(stmt, "", "")
	if err != nil {
		return false, fmt.Errorf("parse statement %q: %w", statementPreview(stmt), err)
	}
	if len(stmtNodes) != 1 {
		return false, fmt.Errorf(
			"expected a single statement but %q parsed as %d statements; split with SplitStatements first",
			statementPreview(stmt), len(stmtNodes),
		)
	}
	switch node := stmtNodes[0].(type) {
	case *ast.CreateIndexStmt:
		return true, nil
	case *ast.AlterTableStmt:
		if slices.ContainsFunc(node.Specs, alterSpecScalesWithTableSize) {
			return true, nil
		}
		return false, nil
	default:
		return false, nil
	}
}

// alterSpecScalesWithTableSize reports whether one ALTER TABLE clause forces
// work proportional to the table's row count. The metadata-only cases are an
// allowlist: a clause shape this function doesn't recognize is assumed to
// scale, so a new or exotic clause over-reports size context rather than
// hiding it on an expensive change.
//
// DROP CONSTRAINT is metadata-only whichever constraint it names: MySQL
// resolves it against the table's CHECK, FOREIGN KEY and UNIQUE constraints,
// and dropping any of the three is a metadata change.
func alterSpecScalesWithTableSize(spec *ast.AlterTableSpec) bool {
	switch spec.Tp { //nolint:exhaustive
	case ast.AlterTableRenameColumn, ast.AlterTableRenameTable,
		ast.AlterTableRenameIndex, ast.AlterTableAlterColumn, ast.AlterTableDropIndex,
		ast.AlterTableDropForeignKey, ast.AlterTableDropCheck,
		ast.AlterTableDropConstraint, ast.AlterTableIndexInvisible,
		ast.AlterTableDropPartition, ast.AlterTableTruncatePartition:
		return false
	case ast.AlterTableAddPartitions:
		// ADD PARTITION is two operations sharing one clause type, and the
		// statement shows which: with partition definitions
		// (ADD PARTITION (PARTITION p ...), the RANGE/LIST form) it attaches
		// a new empty partition on metadata alone, while without them
		// (ADD PARTITION PARTITIONS n, the HASH/KEY form) it changes the
		// partition count and redistributes every existing row.
		return len(spec.PartDefinitions) == 0
	case ast.AlterTableAddColumns:
		// A plain column add is metadata-only, but an inline PRIMARY KEY or
		// UNIQUE builds an index and a STORED generated column is computed
		// for every existing row.
		for _, col := range spec.NewColumns {
			for _, opt := range col.Options {
				switch opt.Tp { //nolint:exhaustive
				case ast.ColumnOptionPrimaryKey, ast.ColumnOptionUniqKey:
					return true
				case ast.ColumnOptionGenerated:
					if opt.Stored {
						return true
					}
				}
			}
		}
		return false
	case ast.AlterTableOption:
		// A table COMMENT change is metadata-only; other options (ENGINE=,
		// ROW_FORMAT=, CONVERT TO CHARACTER SET, ...) can rebuild the table.
		for _, opt := range spec.Options {
			if opt.Tp != ast.TableOptionComment {
				return true
			}
		}
		return false
	default:
		// Index-backed constraint adds build an index; FOREIGN KEY and CHECK
		// adds validate every existing row; MODIFY/CHANGE COLUMN can rebuild
		// (a VARCHAR widening that crosses the length-byte boundary copies the
		// table, and that boundary isn't visible from the statement alone).
		// DROP COLUMN lands here too: it is instant only on MySQL 8.0.29+,
		// only while the table's instant-change row-header budget lasts, not
		// on ROW_FORMAT=COMPRESSED, and not without rebuilding any index on
		// the column — none of which the statement shows.
		return true
	}
}

// statementTypeFromSpirit translates Spirit's parser-owned statement type into
// the pkg/ddl-owned vocabulary at the seam boundary, so Spirit's type never
// escapes the TiDB implementation.
func statementTypeFromSpirit(t statement.StatementType) StatementType {
	switch t {
	case statement.StatementAlterTable:
		return StatementAlterTable
	case statement.StatementCreateTable:
		return StatementCreateTable
	case statement.StatementDropTable:
		return StatementDropTable
	case statement.StatementRenameTable:
		return StatementRenameTable
	case statement.StatementTruncateTable:
		return StatementTruncateTable
	case statement.StatementCreateIndex:
		return StatementCreateIndex
	case statement.StatementDropIndex:
		return StatementDropIndex
	case statement.StatementCreateView:
		return StatementCreateView
	case statement.StatementInsert:
		return StatementInsert
	case statement.StatementUpdate:
		return StatementUpdate
	case statement.StatementDelete:
		return StatementDelete
	case statement.StatementUnknown:
		return StatementUnknown
	default:
		return StatementUnknown
	}
}

// Canonicalize implements StatementParser.
//
// For ALTER TABLE statements it reconstructs from Spirit's normalized Alter
// field; for CREATE TABLE and DROP TABLE it uses TiDB's Restore. It returns
// the original statement when parsing fails or when the input contains more
// than one statement, so canonicalization never truncates its input.
func (tidbStatementParser) Canonicalize(ddl string) string {
	stmts, err := statement.New(ddl)
	if err != nil || len(stmts) != 1 {
		return ddl
	}

	stmt := stmts[0]

	// For ALTER TABLE, reconstruct from the normalized Alter field.
	if stmt.Alter != "" {
		if stmt.Schema != "" {
			return fmt.Sprintf("ALTER TABLE %s.%s %s", sqlescape.EscapeIdentifier(stmt.Schema), sqlescape.EscapeIdentifier(stmt.Table), stmt.Alter)
		}
		return fmt.Sprintf("ALTER TABLE %s %s", sqlescape.EscapeIdentifier(stmt.Table), stmt.Alter)
	}

	// For CREATE TABLE and DROP TABLE, use TiDB's Restore for canonical format.
	return restoreCanonical(ddl)
}

// restoreCanonical uses TiDB parser to restore a statement in canonical
// format. It returns the input unchanged unless exactly one statement was
// parsed, so it never drops trailing statements from multi-statement input.
func restoreCanonical(ddl string) string {
	p := parser.New()
	stmtNodes, _, err := p.Parse(ddl, "", "")
	if err != nil || len(stmtNodes) != 1 {
		return ddl
	}

	node := stmtNodes[0]

	// Only canonicalize CREATE TABLE and DROP TABLE
	switch node.(type) {
	case *ast.CreateTableStmt, *ast.DropTableStmt:
		var sb strings.Builder
		rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
		if err := node.Restore(rCtx); err != nil {
			return ddl
		}
		return sb.String()
	default:
		return ddl
	}
}
