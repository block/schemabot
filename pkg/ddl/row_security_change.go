package ddl

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	pgproto "github.com/pganalyze/pg_query_go/v6"
	pgquery "github.com/wasilibs/go-pgquery"
)

// ErrRowSecurityChange identifies SQL outside a table-local RLS operation.
var ErrRowSecurityChange = errors.New("invalid row security operation")

// RowSecurityChange is an ordered SQL operation for comparison, not execution
// authority. Parsing checks statement kinds and one fully qualified target; it
// does not check privileges, policy dependencies, or whether access widens.
// It deliberately stays separate from StatementParser and ordinary DDL tasks.
// The PostgreSQL executor must still admit and execute the operation atomically.
type RowSecurityChange struct {
	schema     string
	table      string
	statements []string
}

// Schema returns the physical schema named by every target statement.
func (c RowSecurityChange) Schema() string { return c.schema }

// Table returns the table named by every target statement.
func (c RowSecurityChange) Table() string { return c.table }

// Statements returns a copy of the canonical statements in their original order.
func (c RowSecurityChange) Statements() []string { return slices.Clone(c.statements) }

// CanonicalSQL preserves order and duplicates for a single operation's comparison
// key. It preserves physical schema qualifiers, roles, predicates, and comments;
// namespace mapping is a separate concern. A zero value is not a parsed operation.
func (c RowSecurityChange) CanonicalSQL() string { return strings.Join(c.statements, ";\n") }

// ParseRowSecurityChange admits the SQL shapes emitted by a complete atomic RLS
// replacement: policy drops/creates/comments and RLS settings on one table.
// Every entry must contain exactly one statement. Empty input is not a change.
// General policy DDL remains outside the ordinary task and drift vocabulary.
func ParseRowSecurityChange(schema, table string, statements []string) (RowSecurityChange, error) {
	if schema == "" || table == "" || len(statements) == 0 {
		return RowSecurityChange{}, fmt.Errorf("%w: target and statements are required", ErrRowSecurityChange)
	}
	result := RowSecurityChange{schema: schema, table: table}
	for i, sql := range statements {
		parsed, err := pgquery.Parse(sql)
		if err != nil {
			return RowSecurityChange{}, fmt.Errorf("%w: statement %d: %w", ErrRowSecurityChange, i+1, err)
		}
		if len(parsed.GetStmts()) != 1 {
			return RowSecurityChange{}, fmt.Errorf("%w: statement %d must contain one statement", ErrRowSecurityChange, i+1)
		}
		if !rowSecurityStatementTarget(parsed.Stmts[0].Stmt, schema, table) {
			return RowSecurityChange{}, fmt.Errorf("%w: statement %d has an unsupported shape or target", ErrRowSecurityChange, i+1)
		}
		canonical, err := pgquery.Deparse(parsed)
		if err != nil {
			return RowSecurityChange{}, fmt.Errorf("%w: deparse statement %d: %w", ErrRowSecurityChange, i+1, err)
		}
		result.statements = append(result.statements, canonical)
	}
	return result, nil
}

func rowSecurityStatementTarget(node *pgproto.Node, schema, table string) bool {
	switch {
	case node.GetCreatePolicyStmt() != nil:
		return rowSecurityRelationMatches(node.GetCreatePolicyStmt().GetTable(), schema, table)
	case node.GetDropStmt() != nil:
		drop := node.GetDropStmt()
		if drop.GetRemoveType() != pgproto.ObjectType_OBJECT_POLICY || drop.GetMissingOk() {
			return false
		}
		if drop.GetBehavior() != pgproto.DropBehavior_DROP_RESTRICT || len(drop.GetObjects()) != 1 {
			return false
		}
		return rowSecurityPolicyMatches(drop.Objects[0], schema, table)
	case node.GetCommentStmt() != nil:
		comment := node.GetCommentStmt()
		return comment.GetObjtype() == pgproto.ObjectType_OBJECT_POLICY && rowSecurityPolicyMatches(comment.GetObject(), schema, table)
	case node.GetAlterTableStmt() != nil:
		alter := node.GetAlterTableStmt()
		if !rowSecurityRelationMatches(alter.GetRelation(), schema, table) {
			return false
		}
		if alter.GetObjtype() != pgproto.ObjectType_OBJECT_TABLE || alter.GetMissingOk() || len(alter.GetCmds()) != 1 {
			return false
		}
		switch alter.Cmds[0].GetAlterTableCmd().GetSubtype() {
		case pgproto.AlterTableType_AT_EnableRowSecurity, pgproto.AlterTableType_AT_DisableRowSecurity,
			pgproto.AlterTableType_AT_ForceRowSecurity, pgproto.AlterTableType_AT_NoForceRowSecurity:
			return true
		}
	}
	return false
}

func rowSecurityRelationMatches(rel *pgproto.RangeVar, schema, table string) bool {
	if rel.GetCatalogname() != "" {
		return false
	}
	return rel.GetSchemaname() == schema && rel.GetRelname() == table
}

func rowSecurityPolicyMatches(node *pgproto.Node, schema, table string) bool {
	names := node.GetList().GetItems()
	if len(names) != 3 {
		return false
	}
	if names[0].GetString_().GetSval() != schema || names[1].GetString_().GetSval() != table {
		return false
	}
	return names[2].GetString_().GetSval() != ""
}
