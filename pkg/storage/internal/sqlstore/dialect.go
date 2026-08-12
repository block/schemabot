package sqlstore

import (
	"fmt"
	"strconv"
	"strings"
)

// Dialect abstracts the SQL-syntax differences between database families (MySQL
// and, in the future, Postgres) that the state store depends on. This is an
// incremental seam: the store still emits MySQL-style "?" placeholders, and
// only the family-varying syntax (upsert clause, current-time and
// relative-time expressions) routes through here. When a Postgres backend is
// introduced its parameterized SQL will need a "?"-to-"$n" rebind at the store
// boundary.
type Dialect interface {
	// InsertIfAbsent returns the syntax that makes an INSERT a no-op when the
	// named unique-key columns conflict. Modifier is placed between INSERT and
	// INTO; Suffix is placed after the VALUES clause. A successful insert must
	// report one affected row, while a conflict must report zero affected rows.
	InsertIfAbsent(conflictColumns []string) InsertIfAbsentSyntax
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
	// CurrentTimestamp returns the expression for the current time at the given
	// fractional-second precision (MySQL: NOW() / NOW(6)).
	CurrentTimestamp(precision TimestampPrecision) string
	// RelativeTime returns an expression for the current time offset by an
	// interval, for staleness and lease-expiry predicates. amount is either a
	// build-time literal or a bound query parameter; a parameterized amount emits
	// exactly one placeholder that the caller supplies as a query argument.
	RelativeTime(precision TimestampPrecision, direction RelativeTimeDirection, amount IntervalAmount, unit IntervalUnit) string
	// JSONBooleanIsTrue returns a predicate that is true only when the JSON value
	// at path is the boolean true. Missing paths, JSON null, SQL NULL, strings,
	// numbers, objects, and arrays must all yield false. Path contains unescaped
	// object keys, ordered from the root to the value.
	JSONBooleanIsTrue(expression string, path []string) string
	// IndexHint returns an optional table-level hint that directs lookups through
	// index. The returned text includes any required leading whitespace and is
	// placed immediately after a table name or alias; dialects without index-hint
	// syntax return an empty string.
	IndexHint(index string) string
}

// InsertIfAbsentSyntax contains the dialect-specific fragments surrounding an
// INSERT's portable INTO, column-list, and VALUES body.
type InsertIfAbsentSyntax struct {
	Modifier string
	Suffix   string
}

// TimestampPrecision selects the fractional-second precision of a current-time
// expression. Only the values the store actually uses are representable.
type TimestampPrecision uint8

const (
	// TimestampPrecisionDefault is whole-second precision (MySQL NOW()).
	TimestampPrecisionDefault TimestampPrecision = iota
	// TimestampPrecisionMicrosecond is microsecond precision (MySQL NOW(6)).
	TimestampPrecisionMicrosecond
)

// RelativeTimeDirection selects whether an interval is subtracted from or added
// to the current time.
type RelativeTimeDirection uint8

const (
	// BeforeCurrentTime yields current time minus the interval.
	BeforeCurrentTime RelativeTimeDirection = iota
	// AfterCurrentTime yields current time plus the interval.
	AfterCurrentTime
)

// IntervalUnit is the unit of a relative-time interval.
type IntervalUnit uint8

const (
	// IntervalMicrosecond is a microsecond interval unit.
	IntervalMicrosecond IntervalUnit = iota
	// IntervalSecond is a second interval unit.
	IntervalSecond
	// IntervalMinute is a minute interval unit.
	IntervalMinute
	// IntervalHour is an hour interval unit.
	IntervalHour
	// IntervalDay is a day interval unit.
	IntervalDay
)

// IntervalAmount is the magnitude of a relative-time interval: either a literal
// value known when the query is built or a bound query parameter. Construct it
// with LiteralIntervalAmount or ParameterIntervalAmount so the parameterized
// form always emits exactly one placeholder.
type IntervalAmount struct {
	literal       uint64
	parameterized bool
}

// LiteralIntervalAmount is an interval magnitude fixed at query-build time.
func LiteralIntervalAmount(value uint64) IntervalAmount {
	return IntervalAmount{literal: value}
}

// ParameterIntervalAmount is an interval magnitude bound as a query parameter;
// the caller supplies the value as a query argument.
func ParameterIntervalAmount() IntervalAmount {
	return IntervalAmount{parameterized: true}
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

// InsertIfAbsent uses MySQL's INSERT IGNORE modifier. MySQL infers conflicts
// from every unique key on the table, so conflictColumns is not rendered.
func (MySQLDialect) InsertIfAbsent(_ []string) InsertIfAbsentSyntax {
	return InsertIfAbsentSyntax{Modifier: " IGNORE"}
}

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

// CurrentTimestamp returns MySQL's NOW() at the requested precision.
func (MySQLDialect) CurrentTimestamp(precision TimestampPrecision) string {
	switch precision {
	case TimestampPrecisionDefault:
		return "NOW()"
	case TimestampPrecisionMicrosecond:
		return "NOW(6)"
	default:
		panic(fmt.Sprintf("sqlstore: unknown timestamp precision %d", precision))
	}
}

// RelativeTime builds a MySQL current-time-plus-or-minus-interval expression.
// Subtraction uses the INTERVAL operator (NOW() - INTERVAL n UNIT); addition
// uses DATE_ADD so it composes with an explicit-precision NOW(6). A
// parameterized amount emits a single "?" placeholder.
func (d MySQLDialect) RelativeTime(precision TimestampPrecision, direction RelativeTimeDirection, amount IntervalAmount, unit IntervalUnit) string {
	magnitude := "?"
	if !amount.parameterized {
		magnitude = strconv.FormatUint(amount.literal, 10)
	}
	interval := "INTERVAL " + magnitude + " " + mysqlIntervalUnit(unit)
	now := d.CurrentTimestamp(precision)

	switch direction {
	case BeforeCurrentTime:
		return now + " - " + interval
	case AfterCurrentTime:
		return "DATE_ADD(" + now + ", " + interval + ")"
	default:
		panic(fmt.Sprintf("sqlstore: unknown relative-time direction %d", direction))
	}
}

// JSONBooleanIsTrue compares the extracted MySQL JSON value with JSON true
// using null-safe equality so absent and null values do not match.
func (MySQLDialect) JSONBooleanIsTrue(expression string, path []string) string {
	jsonPath := "$"
	for _, key := range path {
		jsonPath += "." + strconv.Quote(key)
	}
	jsonPath = strings.ReplaceAll(jsonPath, "'", "''")
	return "(JSON_EXTRACT(" + expression + ", '" + jsonPath + "') <=> CAST('true' AS JSON))"
}

// IndexHint returns a MySQL FORCE INDEX hint for the named index.
func (MySQLDialect) IndexHint(index string) string {
	return " FORCE INDEX (`" + strings.ReplaceAll(index, "`", "``") + "`)"
}

func mysqlIntervalUnit(unit IntervalUnit) string {
	switch unit {
	case IntervalMicrosecond:
		return "MICROSECOND"
	case IntervalSecond:
		return "SECOND"
	case IntervalMinute:
		return "MINUTE"
	case IntervalHour:
		return "HOUR"
	case IntervalDay:
		return "DAY"
	default:
		panic(fmt.Sprintf("sqlstore: unknown interval unit %d", unit))
	}
}
