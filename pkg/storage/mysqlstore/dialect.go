package mysqlstore

import "strings"

// Dialect abstracts the SQL-syntax differences between database families (MySQL
// and, in the future, Postgres) that the state store depends on, so the same
// store logic can target either engine. It currently lives here alongside its
// MySQLDialect implementation; when a Postgres store is introduced this
// interface likely moves to a shared package so neither store depends on the
// other.
type Dialect interface {
	// UpsertClause returns the trailing conflict-resolution clause that turns an
	// INSERT into an upsert. conflictColumns names the unique key that defines a
	// conflict; MySQL infers the key from the table and ignores it, while
	// Postgres requires it for the ON CONFLICT target. assignments lists the
	// columns to overwrite when a conflicting row already exists.
	UpsertClause(conflictColumns []string, assignments []UpsertAssignment) string
	// ExcludedValue references the value from the row that failed to insert, for
	// use inside an UpsertAssignment expression (MySQL: VALUES(col); Postgres:
	// EXCLUDED.col).
	ExcludedValue(column string) string
}

// UpsertAssignment describes how one column is updated when an upsert matches an
// existing row. Expr is the raw SQL update expression; when empty, the column is
// set to its excluded (to-be-inserted) value.
type UpsertAssignment struct {
	Column string
	Expr   string
}

// MySQLDialect implements Dialect for MySQL and MySQL-protocol engines.
type MySQLDialect struct{}

// ExcludedValue returns the MySQL reference to the proposed row value.
func (MySQLDialect) ExcludedValue(column string) string {
	return "VALUES(" + column + ")"
}

// UpsertClause builds a MySQL ON DUPLICATE KEY UPDATE clause. conflictColumns is
// unused because MySQL resolves conflicts against every unique key on the table.
func (d MySQLDialect) UpsertClause(_ []string, assignments []UpsertAssignment) string {
	sets := make([]string, len(assignments))
	for i, a := range assignments {
		expr := a.Expr
		if expr == "" {
			expr = d.ExcludedValue(a.Column)
		}
		sets[i] = a.Column + " = " + expr
	}
	return "ON DUPLICATE KEY UPDATE " + strings.Join(sets, ", ")
}
