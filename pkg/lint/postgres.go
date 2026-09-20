package lint

import (
	"fmt"
	"sort"
	"strings"

	pgproto "github.com/pganalyze/pg_query_go/v6"
	pgquery "github.com/wasilibs/go-pgquery"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/schema"
)

// PostgreSQL schema-shape audit.
//
// Spirit's linters read the TiDB AST and are MySQL-specific by design (see
// pkg/ddl): a non-MySQL engine supplies its own equivalents rather than
// reusing them. This file is the PostgreSQL equivalent for the rules that have
// an unambiguous PostgreSQL analog — primary key presence and type, floating
// point columns, table name case, and redundant btree indexes. Rules about
// storage engines, character sets, zero dates, reserved words, and MySQL
// timestamp semantics have no PostgreSQL counterpart and are deliberately
// absent rather than approximated.
//
// The audit runs over pulled table definitions, so every finding is a warning:
// a pulled table is existing schema, and Spirit likewise reports shape defects
// on existing tables as warnings rather than errors.

// Linter names reported by the PostgreSQL audit. They match Spirit's names for
// the same rules so a `pull --lint` response reads the same across dialects.
const (
	postgresLinterPrimaryKey       = "primary_key"
	postgresLinterHasFloat         = "has_float"
	postgresLinterNameCase         = "name_case"
	postgresLinterRedundantIndexes = "redundant_indexes"
)

// UnlintableTableError reports a pulled table entry the PostgreSQL audit
// cannot cover: it must be a create set — one CREATE TABLE, optionally
// followed by CREATE INDEX statements on that same table. Anything else would
// leave part of the entry unaudited and turn the result into a partial one, so
// the caller fails the request instead.
type UnlintableTableError struct {
	Table  string
	Detail string
}

func (e *UnlintableTableError) Error() string {
	return fmt.Sprintf("table %q %s", e.Table, e.Detail)
}

// LintPostgresSchema audits pulled PostgreSQL table definitions, keyed by
// table name, for schema-shape defects. Each entry is a rendered table: one
// CREATE TABLE followed by that table's CREATE INDEX statements. Results are
// sorted by table, column, linter, and message so the output is stable
// regardless of map order.
func (l *Linter) LintPostgresSchema(tables map[string]string) ([]Result, error) {
	allowedPKTypes := splitCSV(l.config.AllowedPostgresPKTypes)
	if len(allowedPKTypes) == 0 {
		return nil, fmt.Errorf("postgres lint: at least one allowed primary key type must be configured")
	}
	ignored := make(map[string]bool, len(l.config.IgnoreTables))
	for _, name := range l.config.IgnoreTables {
		ignored[name] = true
	}

	var results []Result
	for tableName, content := range tables {
		create, indexes, err := parsePostgresTableEntry(tableName, content)
		if err != nil {
			return nil, err
		}
		if ignored[create.GetRelation().GetRelname()] {
			continue
		}
		results = append(results, lintPostgresCreateTable(create, allowedPKTypes)...)
		results = append(results, lintPostgresRedundantIndexes(create, indexes)...)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Table != results[j].Table {
			return results[i].Table < results[j].Table
		}
		if results[i].Column != results[j].Column {
			return results[i].Column < results[j].Column
		}
		if results[i].Linter != results[j].Linter {
			return results[i].Linter < results[j].Linter
		}
		return results[i].Message < results[j].Message
	})
	return results, nil
}

// parsePostgresTableEntry admits one pulled table entry as a create set and
// returns its CREATE TABLE and the CREATE INDEX statements that follow it.
// ddl.ParseCreateSet owns the shape rule — the first statement must be the
// CREATE TABLE, and every later statement must be a CREATE INDEX naming that
// same relation, qualified the same way — so an index that arrives before its
// table, or that targets another relation, is refused with the parser's own
// cause rather than passing on node kind alone. Anything that is not a create
// set is an UnlintableTableError.
func parsePostgresTableEntry(tableName, content string) (*pgproto.CreateStmt, []*pgproto.IndexStmt, error) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	if err != nil {
		return nil, nil, fmt.Errorf("postgres lint: %w", err)
	}
	set, err := ddl.ParseCreateSet(parser, content)
	if err != nil {
		return nil, nil, &UnlintableTableError{Table: tableName, Detail: fmt.Sprintf("is not a create set: %v", err)}
	}
	if set.Type != ddl.StatementCreateTable {
		return nil, nil, &UnlintableTableError{Table: tableName, Detail: fmt.Sprintf("is a %s statement, expected CREATE TABLE", set.Type)}
	}

	create, ok := parsePostgresSingleNode(set.Statements[0]).(*pgproto.Node_CreateStmt)
	if !ok {
		return nil, nil, &UnlintableTableError{Table: tableName, Detail: "is classified as CREATE TABLE but does not parse as one"}
	}
	indexes := make([]*pgproto.IndexStmt, 0, len(set.Statements)-1)
	for i, stmt := range set.Statements[1:] {
		index, ok := parsePostgresSingleNode(stmt).(*pgproto.Node_IndexStmt)
		if !ok {
			return nil, nil, &UnlintableTableError{Table: tableName, Detail: fmt.Sprintf("statement %d is classified as CREATE INDEX but does not parse as one", i+2)}
		}
		indexes = append(indexes, index.IndexStmt)
	}
	return create.CreateStmt, indexes, nil
}

// parsePostgresSingleNode parses one statement ParseCreateSet has already
// split and classified, returning its node or nil when the parse disagrees.
func parsePostgresSingleNode(stmt string) any {
	parsed, err := pgquery.Parse(stmt)
	if err != nil || len(parsed.GetStmts()) != 1 {
		return nil
	}
	return parsed.GetStmts()[0].GetStmt().GetNode()
}

// lintPostgresCreateTable applies the column and table shape rules to one
// CREATE TABLE.
func lintPostgresCreateTable(create *pgproto.CreateStmt, allowedPKTypes []string) []Result {
	tableName := create.GetRelation().GetRelname()
	var results []Result

	if tableName != strings.ToLower(tableName) {
		results = append(results, Result{
			Table:    tableName,
			Linter:   postgresLinterNameCase,
			Message:  fmt.Sprintf("table name %q is not lowercase", tableName),
			Severity: "warning",
		})
	}

	columnTypes := make(map[string]postgresColumnType)
	var pkColumns []string
	for _, element := range create.GetTableElts() {
		switch node := element.GetNode().(type) {
		case *pgproto.Node_ColumnDef:
			column := node.ColumnDef
			columnType := parsePostgresColumnType(column.GetTypeName())
			columnTypes[column.GetColname()] = columnType
			for _, c := range column.GetConstraints() {
				if c.GetConstraint().GetContype() == pgproto.ConstrType_CONSTR_PRIMARY {
					pkColumns = append(pkColumns, column.GetColname())
				}
			}
			if columnType.isFloat() {
				results = append(results, Result{
					Table:    tableName,
					Column:   column.GetColname(),
					Linter:   postgresLinterHasFloat,
					Message:  fmt.Sprintf("Column %q in table %q uses %q data type", column.GetColname(), tableName, columnType.display()),
					Severity: "warning",
				})
			}
		case *pgproto.Node_Constraint:
			if node.Constraint.GetContype() == pgproto.ConstrType_CONSTR_PRIMARY {
				for _, key := range node.Constraint.GetKeys() {
					pkColumns = append(pkColumns, key.GetString_().GetSval())
				}
			}
		}
	}

	if len(pkColumns) == 0 {
		results = append(results, Result{
			Table:    tableName,
			Linter:   postgresLinterPrimaryKey,
			Message:  "No primary key defined",
			Severity: "warning",
		})
		return results
	}
	for _, column := range pkColumns {
		columnType, ok := columnTypes[column]
		if !ok {
			// A table-level PRIMARY KEY naming a column the statement does
			// not define cannot come from a live pull; nothing to audit.
			continue
		}
		if columnType.allowed(allowedPKTypes) {
			continue
		}
		results = append(results, Result{
			Table:    tableName,
			Column:   column,
			Linter:   postgresLinterPrimaryKey,
			Message:  fmt.Sprintf("Primary key column %q in table %q uses %q; allowed types: %s", column, tableName, columnType.display(), strings.Join(allowedPKTypes, ", ")),
			Severity: "warning",
		})
	}
	return results
}

// postgresColumnType is a column's declared type as the grammar reports it.
// The grammar folds SQL-standard spellings to pg_catalog internal names
// (bigint → pg_catalog.int8, character(36) → pg_catalog.bpchar(36)) while
// serial pseudo-types, uuid, text, and extension or user types keep the name
// as written. Schema is empty for pg_catalog types and for unqualified names;
// a user type written with its own schema keeps that qualifier so that
// custom.bigint is never mistaken for the built-in bigint.
type postgresColumnType struct {
	Schema string
	Name   string
	Mods   []string
	Array  bool
}

func parsePostgresColumnType(typeName *pgproto.TypeName) postgresColumnType {
	names := typeName.GetNames()
	var t postgresColumnType
	switch {
	case len(names) == 0:
		return t
	case len(names) == 1:
		t.Name = names[0].GetString_().GetSval()
	case names[0].GetString_().GetSval() == "pg_catalog":
		t.Name = names[len(names)-1].GetString_().GetSval()
	default:
		t.Schema = names[0].GetString_().GetSval()
		t.Name = names[len(names)-1].GetString_().GetSval()
	}
	t.Array = len(typeName.GetArrayBounds()) > 0
	// Interval typmods begin with an internal field mask that has no useful SQL
	// spelling in audit messages, so report the unmodified type name.
	if strings.EqualFold(t.Name, "interval") && t.Schema == "" {
		return t
	}
	for _, mod := range typeName.GetTypmods() {
		ival, ok := mod.GetAConst().GetVal().(*pgproto.A_Const_Ival)
		if !ok {
			// Modifiers without an integer SQL spelling are omitted from messages.
			t.Mods = nil
			break
		}
		t.Mods = append(t.Mods, fmt.Sprint(ival.Ival.GetIval()))
	}
	return t
}

// postgresInternalTypeNames maps pg_catalog internal names back to the SQL
// spelling an adopter wrote and reads in psql.
var postgresInternalTypeNames = map[string]string{
	"int8":        "bigint",
	"int4":        "integer",
	"int2":        "smallint",
	"float4":      "real",
	"float8":      "double precision",
	"bool":        "boolean",
	"bpchar":      "character",
	"varchar":     "character varying",
	"varbit":      "bit varying",
	"timestamp":   "timestamp without time zone",
	"timestamptz": "timestamp with time zone",
	"time":        "time without time zone",
	"timetz":      "time with time zone",
}

// postgresSerialUnderlyingTypes maps serial pseudo-types to the integer type
// the column actually gets, so the primary key type rule judges the stored
// type rather than the shorthand.
var postgresSerialUnderlyingTypes = map[string]string{
	"serial":      "integer",
	"serial4":     "integer",
	"bigserial":   "bigint",
	"serial8":     "bigint",
	"smallserial": "smallint",
	"serial2":     "smallint",
}

// baseDisplay is the SQL spelling of the type without modifiers or array
// bounds: "bigint", "character varying", "custom.bigint".
func (t postgresColumnType) baseDisplay() string {
	if t.Schema != "" {
		return t.Schema + "." + t.Name
	}
	if display, ok := postgresInternalTypeNames[strings.ToLower(t.Name)]; ok {
		return display
	}
	return t.Name
}

// display renders the type for a message in its SQL spelling with modifiers
// and array bounds: "character(36)", "numeric(12,2)", "timestamp(3) with
// time zone", "bigint[]".
func (t postgresColumnType) display() string {
	base := t.baseDisplay()
	if len(t.Mods) > 0 {
		mods := "(" + strings.Join(t.Mods, ",") + ")"
		// Precision on the time types sits after the first word:
		// timestamp(3) with time zone.
		if head, tail, ok := strings.Cut(base, " with"); ok && t.Schema == "" {
			base = head + mods + " with" + tail
		} else {
			base += mods
		}
	}
	if t.Array {
		base += "[]"
	}
	return base
}

// allowed reports whether the type is in the allowed set. An entry matches
// the stored type's spelling with or without modifiers, so "bigint" admits
// every bigint key while "character varying(36)" admits only that width. A
// serial column is judged as the integer type it stores. A schema-qualified
// user type only matches an entry written with the same qualifier.
func (t postgresColumnType) allowed(allowed []string) bool {
	stored := t.baseDisplay()
	if underlying, ok := postgresSerialUnderlyingTypes[strings.ToLower(stored)]; ok && t.Schema == "" {
		stored = underlying
	}
	if t.Array {
		stored += "[]"
	}
	full := t.display()
	for _, candidate := range allowed {
		if strings.EqualFold(candidate, stored) || strings.EqualFold(candidate, full) {
			return true
		}
	}
	return false
}

// isFloat reports whether the grammar-folded column type is binary floating
// point. numeric/decimal are exact and pass; a user type that happens to be
// named float8 in its own schema is not the built-in.
func (t postgresColumnType) isFloat() bool {
	if t.Schema != "" {
		return false
	}
	switch strings.ToLower(t.Name) {
	case "float4", "float8":
		return true
	}
	return false
}

// postgresIndex is one btree key the redundancy rule compares: the primary
// key, a UNIQUE constraint, or a CREATE INDEX statement from the entry.
type postgresIndex struct {
	Name  string
	Type  string // "PRIMARY KEY", "UNIQUE", or "INDEX"
	Parts []postgresIndexPart
}

// postgresIndexPart is one plain column key part with its sort direction.
// NullsFirst carries the explicit or default NULLS ordering so that
// (a DESC) and (a DESC NULLS LAST) compare as different orderings.
type postgresIndexPart struct {
	Name       string
	Desc       bool
	NullsFirst bool
}

func (p postgresIndexPart) render() string {
	rendered := p.Name
	if p.Desc {
		rendered += " DESC"
	}
	return rendered
}

// lintPostgresRedundantIndexes reports btree indexes another index or the
// primary key already covers, following Spirit's redundant_indexes rule where
// PostgreSQL shares the semantics: the primary key is never redundant, an
// index with more key parts than another cannot be covered by it, a UNIQUE
// key is redundant only to a UNIQUE key or the primary key over exactly the
// same parts, and any other index is redundant when its parts are a leading
// prefix of another's. Spirit's InnoDB-specific checks (a secondary index
// that spells out the clustered primary key) have no PostgreSQL analog and
// are omitted. Only plain btree indexes over whole columns take part: a
// partial, expression, INCLUDE, opclass, collation, NULLS NOT DISTINCT, or
// non-btree index serves queries a plain index cannot, so it is neither
// reported nor treated as covering.
func lintPostgresRedundantIndexes(create *pgproto.CreateStmt, indexStmts []*pgproto.IndexStmt) []Result {
	tableName := create.GetRelation().GetRelname()
	indexes := postgresTableIndexes(create, indexStmts)

	var results []Result
	for i, index := range indexes {
		for j, other := range indexes {
			if i == j || !postgresIndexRedundantTo(index, other) {
				continue
			}
			results = append(results, Result{
				Table:    tableName,
				Linter:   postgresLinterRedundantIndexes,
				Message:  postgresRedundancyMessage(index, other),
				Severity: "warning",
			})
			// Report an index against the first index that covers it.
			break
		}
	}
	return results
}

// postgresTableIndexes collects the comparable keys of a table in statement
// order: the primary key and UNIQUE constraints from the CREATE TABLE, then
// the entry's CREATE INDEX statements.
func postgresTableIndexes(create *pgproto.CreateStmt, indexStmts []*pgproto.IndexStmt) []postgresIndex {
	var indexes []postgresIndex
	for _, element := range create.GetTableElts() {
		switch node := element.GetNode().(type) {
		case *pgproto.Node_ColumnDef:
			for _, c := range node.ColumnDef.GetConstraints() {
				if index, ok := postgresConstraintIndex(c.GetConstraint(), []string{node.ColumnDef.GetColname()}); ok {
					indexes = append(indexes, index)
				}
			}
		case *pgproto.Node_Constraint:
			var columns []string
			for _, key := range node.Constraint.GetKeys() {
				columns = append(columns, key.GetString_().GetSval())
			}
			if index, ok := postgresConstraintIndex(node.Constraint, columns); ok {
				indexes = append(indexes, index)
			}
		}
	}
	for _, stmt := range indexStmts {
		if index, ok := postgresIndexFromStatement(stmt); ok {
			indexes = append(indexes, index)
		}
	}
	return indexes
}

// postgresConstraintIndex models a PRIMARY KEY or UNIQUE constraint as the
// btree index that backs it. Other constraint kinds, INCLUDE columns, and
// NULLS NOT DISTINCT do not take part.
func postgresConstraintIndex(c *pgproto.Constraint, columns []string) (postgresIndex, bool) {
	var index postgresIndex
	switch c.GetContype() {
	case pgproto.ConstrType_CONSTR_PRIMARY:
		index.Type = "PRIMARY KEY"
	case pgproto.ConstrType_CONSTR_UNIQUE:
		index.Type = "UNIQUE"
	default:
		return postgresIndex{}, false
	}
	if len(columns) == 0 || len(c.GetIncluding()) > 0 || c.GetNullsNotDistinct() {
		return postgresIndex{}, false
	}
	index.Name = c.GetConname()
	if index.Name == "" {
		// An inline constraint carries no name until the server assigns one;
		// describe it by its definition so the message still identifies it.
		index.Name = index.Type + " (" + strings.Join(columns, ", ") + ")"
	}
	for _, column := range columns {
		index.Parts = append(index.Parts, postgresIndexPart{Name: column})
	}
	return index, true
}

// postgresIndexFromStatement models a CREATE INDEX statement when it is a
// plain btree index over whole columns.
func postgresIndexFromStatement(stmt *pgproto.IndexStmt) (postgresIndex, bool) {
	method := strings.ToLower(stmt.GetAccessMethod())
	if (method != "" && method != "btree") || stmt.GetWhereClause() != nil || len(stmt.GetIndexIncludingParams()) > 0 || stmt.GetNullsNotDistinct() {
		return postgresIndex{}, false
	}
	index := postgresIndex{Name: stmt.GetIdxname(), Type: "INDEX"}
	if stmt.GetUnique() {
		index.Type = "UNIQUE"
	}
	for _, param := range stmt.GetIndexParams() {
		elem := param.GetIndexElem()
		if elem.GetName() == "" || elem.GetExpr() != nil || len(elem.GetCollation()) > 0 || len(elem.GetOpclass()) > 0 {
			return postgresIndex{}, false
		}
		part := postgresIndexPart{Name: elem.GetName(), Desc: elem.GetOrdering() == pgproto.SortByDir_SORTBY_DESC}
		switch elem.GetNullsOrdering() {
		case pgproto.SortByNulls_SORTBY_NULLS_FIRST:
			part.NullsFirst = true
		case pgproto.SortByNulls_SORTBY_NULLS_LAST:
			part.NullsFirst = false
		default:
			// PostgreSQL sorts NULLS LAST for ascending keys and NULLS
			// FIRST for descending ones.
			part.NullsFirst = part.Desc
		}
		index.Parts = append(index.Parts, part)
	}
	if len(index.Parts) == 0 {
		return postgresIndex{}, false
	}
	return index, true
}

// postgresIndexRedundantTo reports whether index a is redundant to index b.
// Column names compare exactly: the grammar has already folded unquoted
// identifiers, and PostgreSQL treats "A" and a as different columns.
func postgresIndexRedundantTo(a, b postgresIndex) bool {
	if a.Type == "PRIMARY KEY" || len(a.Parts) > len(b.Parts) {
		return false
	}
	if a.Type == "UNIQUE" {
		if b.Type != "UNIQUE" && b.Type != "PRIMARY KEY" {
			return false
		}
		return len(a.Parts) == len(b.Parts) && postgresIndexPartsPrefix(a.Parts, b.Parts)
	}
	return postgresIndexPartsPrefix(a.Parts, b.Parts)
}

// postgresIndexPartsPrefix reports whether every key part of a matches the
// part at the same position in b.
func postgresIndexPartsPrefix(a, b []postgresIndexPart) bool {
	if len(a) > len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// postgresRedundancyMessage words a finding the way Spirit does so the two
// audits read alike.
func postgresRedundancyMessage(redundant, covering postgresIndex) string {
	redundantCols := postgresIndexColumnsPhrase(redundant)
	coveringCols := postgresIndexColumnsPhrase(covering)
	isDuplicate := len(redundant.Parts) == len(covering.Parts)
	toPK := covering.Type == "PRIMARY KEY"
	switch {
	case isDuplicate && toPK:
		return fmt.Sprintf("Index %q on %s is a duplicate of the PRIMARY KEY", redundant.Name, redundantCols)
	case isDuplicate:
		return fmt.Sprintf("Index %q on %s is a duplicate of index %q", redundant.Name, redundantCols, covering.Name)
	case toPK:
		return fmt.Sprintf("Index %q on %s is redundant - covered by PRIMARY KEY on %s", redundant.Name, redundantCols, coveringCols)
	default:
		return fmt.Sprintf("Index %q on %s is redundant - covered by index %q on %s", redundant.Name, redundantCols, covering.Name, coveringCols)
	}
}

// postgresIndexColumnsPhrase renders an index's key parts for message prose:
// `column "a"` or `columns ("a", "b DESC")`.
func postgresIndexColumnsPhrase(index postgresIndex) string {
	rendered := make([]string, 0, len(index.Parts))
	for _, part := range index.Parts {
		rendered = append(rendered, fmt.Sprintf("%q", part.render()))
	}
	if len(rendered) == 1 {
		return "column " + rendered[0]
	}
	return "columns (" + strings.Join(rendered, ", ") + ")"
}

func splitCSV(s string) []string {
	var out []string
	for part := range strings.SplitSeq(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
