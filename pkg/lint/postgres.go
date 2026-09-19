package lint

import (
	"fmt"
	"sort"
	"strings"

	pgproto "github.com/pganalyze/pg_query_go/v6"
	pgquery "github.com/wasilibs/go-pgquery"
)

// PostgreSQL schema-shape audit.
//
// Spirit's linters read the TiDB AST and are MySQL-specific by design (see
// pkg/ddl): a non-MySQL engine supplies its own equivalents rather than
// reusing them. This file is the PostgreSQL equivalent for the rules that have
// an unambiguous PostgreSQL analog — primary key presence and type, floating
// point columns, and table name case. Rules about storage engines, character
// sets, zero dates, and MySQL timestamp semantics have no PostgreSQL
// counterpart and are deliberately absent rather than approximated.
//
// The audit runs over pulled table definitions, so every finding is a warning:
// a pulled table is existing schema, and Spirit likewise reports shape defects
// on existing tables as warnings rather than errors.

// Linter names reported by the PostgreSQL audit. They match Spirit's names for
// the same rules so a `pull --lint` response reads the same across dialects.
const (
	postgresLinterPrimaryKey = "primary_key"
	postgresLinterHasFloat   = "has_float"
	postgresLinterNameCase   = "name_case"
)

// UnlintableTableError reports a pulled table entry the PostgreSQL audit
// cannot cover: it must hold exactly one CREATE TABLE, optionally followed by
// CREATE INDEX statements. Anything else would leave part of the entry
// unaudited and turn the result into a partial one, so the caller fails the
// request instead.
type UnlintableTableError struct {
	Table  string
	Detail string
}

func (e *UnlintableTableError) Error() string {
	return fmt.Sprintf("table %q %s", e.Table, e.Detail)
}

// LintPostgresSchema audits pulled PostgreSQL table definitions, keyed by
// table name, for schema-shape defects. Each entry is a rendered table: one
// CREATE TABLE followed by that table's CREATE INDEX statements, which carry
// no shape the audit inspects and are skipped. Results are sorted by table,
// column, linter, and message so the output is stable regardless of map order.
func (l *Linter) LintPostgresSchema(tables map[string]string) ([]Result, error) {
	allowedPKTypes := splitCSV(l.config.AllowedPostgresPKTypes)
	ignored := make(map[string]bool, len(l.config.IgnoreTables))
	for _, name := range l.config.IgnoreTables {
		ignored[name] = true
	}

	var results []Result
	for tableName, content := range tables {
		create, err := parsePostgresTableEntry(tableName, content)
		if err != nil {
			return nil, err
		}
		if ignored[create.GetRelation().GetRelname()] {
			continue
		}
		results = append(results, lintPostgresCreateTable(create, allowedPKTypes)...)
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

// parsePostgresTableEntry parses one pulled table entry with the PostgreSQL
// grammar and returns its single CREATE TABLE statement. CREATE INDEX
// statements are accepted and skipped; any other statement kind, more than one
// CREATE TABLE, or no CREATE TABLE at all is an UnlintableTableError.
func parsePostgresTableEntry(tableName, content string) (*pgproto.CreateStmt, error) {
	parsed, err := pgquery.Parse(content)
	if err != nil {
		return nil, &UnlintableTableError{Table: tableName, Detail: fmt.Sprintf("cannot be parsed as PostgreSQL DDL: %v", err)}
	}
	var create *pgproto.CreateStmt
	for _, raw := range parsed.GetStmts() {
		switch node := raw.GetStmt().GetNode().(type) {
		case *pgproto.Node_CreateStmt:
			if create != nil {
				return nil, &UnlintableTableError{Table: tableName, Detail: "contains more than one CREATE TABLE statement"}
			}
			create = node.CreateStmt
		case *pgproto.Node_IndexStmt:
			// Index definitions carry no shape the audit inspects.
		default:
			return nil, &UnlintableTableError{Table: tableName, Detail: fmt.Sprintf("contains a %s statement, expected CREATE TABLE", postgresStatementKind(raw.GetStmt()))}
		}
	}
	if create == nil {
		return nil, &UnlintableTableError{Table: tableName, Detail: "contains no CREATE TABLE statement"}
	}
	return create, nil
}

// postgresStatementKind names a parse node for an error message using the
// grammar's own statement name (AlterTable, DropStmt → Drop). It is
// diagnostic text, not a classification.
func postgresStatementKind(node *pgproto.Node) string {
	name := fmt.Sprintf("%T", node.GetNode())
	name = name[strings.LastIndex(name, ".")+1:]
	name = strings.TrimPrefix(name, "Node_")
	return strings.TrimSuffix(name, "Stmt")
}

// lintPostgresCreateTable applies the shape rules to one CREATE TABLE.
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

	columnTypes := make(map[string]*pgproto.TypeName)
	var pkColumns []string
	for _, element := range create.GetTableElts() {
		switch node := element.GetNode().(type) {
		case *pgproto.Node_ColumnDef:
			column := node.ColumnDef
			columnTypes[column.GetColname()] = column.GetTypeName()
			for _, c := range column.GetConstraints() {
				if c.GetConstraint().GetContype() == pgproto.ConstrType_CONSTR_PRIMARY {
					pkColumns = append(pkColumns, column.GetColname())
				}
			}
			if postgresTypeIsFloat(column.GetTypeName()) {
				results = append(results, Result{
					Table:    tableName,
					Column:   column.GetColname(),
					Linter:   postgresLinterHasFloat,
					Message:  fmt.Sprintf("Column %q in table %q uses %q data type", column.GetColname(), tableName, postgresTypeDisplayName(column.GetTypeName())),
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
		typeName, ok := columnTypes[column]
		if !ok {
			// A table-level PRIMARY KEY naming a column the statement does
			// not define cannot come from a live pull; nothing to audit.
			continue
		}
		if postgresTypeAllowed(typeName, allowedPKTypes) {
			continue
		}
		results = append(results, Result{
			Table:    tableName,
			Column:   column,
			Linter:   postgresLinterPrimaryKey,
			Message:  fmt.Sprintf("Primary key column %q in table %q uses %q; allowed types: %s", column, tableName, postgresTypeDisplayName(typeName), strings.Join(allowedPKTypes, ", ")),
			Severity: "warning",
		})
	}
	return results
}

// postgresTypeBaseName returns the unqualified type name as the grammar
// reports it: SQL-standard spellings fold to pg_catalog internal names
// (bigint → int8, double precision → float8, float(10) → float4) while
// serial pseudo-types and extension types keep their spelling.
func postgresTypeBaseName(typeName *pgproto.TypeName) string {
	names := typeName.GetNames()
	if len(names) == 0 {
		return ""
	}
	return strings.ToLower(names[len(names)-1].GetString_().GetSval())
}

// postgresInternalTypeNames maps pg_catalog internal names back to the SQL
// spelling an adopter wrote and reads in psql.
var postgresInternalTypeNames = map[string]string{
	"int8":   "bigint",
	"int4":   "integer",
	"int2":   "smallint",
	"float4": "real",
	"float8": "double precision",
	"bool":   "boolean",
}

// postgresTypeDisplayName renders a type for a message in its SQL spelling.
func postgresTypeDisplayName(typeName *pgproto.TypeName) string {
	base := postgresTypeBaseName(typeName)
	if display, ok := postgresInternalTypeNames[base]; ok {
		return display
	}
	return base
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

// postgresTypeAllowed reports whether a column type is in the allowed set,
// comparing the SQL spelling of the stored type against each allowed entry.
func postgresTypeAllowed(typeName *pgproto.TypeName, allowed []string) bool {
	stored := postgresTypeDisplayName(typeName)
	if underlying, ok := postgresSerialUnderlyingTypes[stored]; ok {
		stored = underlying
	}
	for _, candidate := range allowed {
		if strings.EqualFold(strings.TrimSpace(candidate), stored) {
			return true
		}
	}
	return false
}

// postgresTypeIsFloat reports whether a column is a binary floating point type
// (real, double precision, float(n)). numeric/decimal are exact and pass.
func postgresTypeIsFloat(typeName *pgproto.TypeName) bool {
	switch postgresTypeBaseName(typeName) {
	case "float4", "float8", "real", "float":
		return true
	}
	return false
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
