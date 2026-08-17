// drop.go parses DROP TABLE statements into their targets and executes them
// directly against the target, for the deployments that drop tables outright
// rather than quarantining them in the pending drops database.
package spirit

import (
	"context"
	"fmt"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/mysqlconn"
)

// dropTarget is one table named by a DROP TABLE statement, with the statement's
// database filled in when the name is unqualified.
type dropTarget struct {
	schema string
	table  string
}

// parseDropTableStatement parses a single DROP TABLE statement into its AST
// node. DROP VIEW and DROP TEMPORARY TABLE parse into the same node; callers
// distinguish them with isNonTableDrop.
func parseDropTableStatement(stmt string) (*ast.DropTableStmt, error) {
	parsed, err := statement.New(stmt)
	if err != nil {
		return nil, fmt.Errorf("parse DROP TABLE statement: %w", err)
	}
	if len(parsed) != 1 {
		return nil, fmt.Errorf("expected exactly 1 parsed DROP TABLE statement, got %d", len(parsed))
	}
	dropStmt, ok := (*parsed[0].StmtNode).(*ast.DropTableStmt)
	if !ok {
		return nil, fmt.Errorf("statement is not DROP TABLE: %s", stmt)
	}
	return dropStmt, nil
}

// isNonTableDrop reports whether the statement drops a view or a temporary
// table rather than a base table. Neither holds recoverable table data, and
// neither participates in the pending drops quarantine or the existence check
// on the direct path, so both are executed as written.
func isNonTableDrop(dropStmt *ast.DropTableStmt) bool {
	return dropStmt.IsView || dropStmt.TemporaryKeyword != ast.TemporaryNone
}

// dropTableTargets returns the tables the statement names, qualifying any
// unqualified name with the database the statement runs against.
func dropTableTargets(dropStmt *ast.DropTableStmt, database string) []dropTarget {
	targets := make([]dropTarget, 0, len(dropStmt.Tables))
	for _, table := range dropStmt.Tables {
		schema := table.Schema.String()
		if schema == "" {
			schema = database
		}
		targets = append(targets, dropTarget{schema: schema, table: table.Name.String()})
	}
	return targets
}

// executeDropDirectly drops each table the statement names, skipping the ones
// that are already absent.
//
// The declarative differ emits a bare DROP TABLE, and the DROP phase re-runs
// from its first statement every time an apply resumes, so a phase that dropped
// some of its tables before being stopped would otherwise fail on the second
// attempt with "unknown table" and never reach the tables still standing.
// Skipping an absent table converges on exactly the state the plan asked for:
// the table is gone. The skip is logged because on a first attempt it means
// something outside this apply removed the table.
func (e *Engine) executeDropDirectly(ctx context.Context, host, username, password, database, stmt string) error {
	dropStmt, err := parseDropTableStatement(stmt)
	if err != nil {
		return err
	}
	// A view or temporary table has no base-table row in information_schema.
	// tables to check, and MySQL's own IF EXISTS already tolerates a missing
	// target, so both go straight through as written.
	if isNonTableDrop(dropStmt) || dropStmt.IfExists {
		return e.executeSingleStatement(ctx, host, username, password, database, stmt)
	}

	db, err := mysqlconn.Open(targetDSN(host, username, password, database))
	if err != nil {
		return fmt.Errorf("open database %s: %w", database, err)
	}
	defer utils.CloseAndLog(db)
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping database %s: %w", database, err)
	}

	for _, target := range dropTableTargets(dropStmt, database) {
		exists, err := tableExistsInSchema(ctx, db, target.schema, target.table)
		if err != nil {
			return fmt.Errorf("check table `%s`.`%s` exists: %w", target.schema, target.table, err)
		}
		if !exists {
			e.logger.Warn("DROP TABLE target is already absent, leaving it dropped",
				"database", target.schema,
				"table", target.table,
			)
			e.emitTableLog(target.table, "table was already absent; nothing to drop")
			continue
		}
		// IF EXISTS carries the convergence through the window between the
		// check above and the drop itself: a table another actor removes in
		// that window has already reached the state the plan asked for, and
		// the check is what decides whether to report the table as absent.
		drop := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s",
			sqlescape.EscapeIdentifier(target.schema), sqlescape.EscapeIdentifier(target.table))
		if err := e.executeSingleStatement(ctx, host, username, password, database, drop); err != nil {
			return fmt.Errorf("drop table `%s`.`%s`: %w", target.schema, target.table, err)
		}
	}
	return nil
}
