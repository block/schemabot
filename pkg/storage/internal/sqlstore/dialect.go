package sqlstore

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Dialect abstracts the SQL-syntax differences between database families (MySQL
// and, in the future, Postgres) that the state store depends on. This is an
// incremental seam: the store still emits MySQL-style "?" placeholders, and
// family-varying syntax routes through here. When a Postgres backend is introduced
// its parameterized SQL will need a "?"-to-"$n" rebind at the store boundary.
type Dialect interface {
	// InsertIfAbsent returns the syntax that makes an INSERT a no-op when the
	// named unique-key columns conflict. Modifier is placed between INSERT and
	// INTO; Suffix is placed after the VALUES clause. A successful insert must
	// report one affected row, while a conflict must report zero affected rows.
	// The dialects diverge on non-conflict failures: MySQL's INSERT IGNORE
	// downgrades other errors (value truncation, NOT NULL violations) to
	// warnings, while PostgreSQL's ON CONFLICT DO NOTHING suppresses only the
	// named conflict and surfaces everything else as an error. Callers must
	// bind values that are valid for the schema so the two behave identically.
	InsertIfAbsent(conflictColumns []string) InsertIfAbsentSyntax
	// UpsertClause returns the trailing conflict-resolution clause that turns an
	// INSERT into an upsert. conflictColumns names the unique key that defines a
	// conflict; MySQL infers the key from the table and ignores it, while
	// Postgres requires it for the ON CONFLICT target. assignments lists the
	// columns to overwrite when a conflicting row already exists.
	UpsertClause(conflictColumns []string, assignments []UpsertAssignment) string
	// ExcludedValue references the value from the row that failed to insert, for
	// use inside an UpsertAssignment expression (MySQL: VALUES(col); Postgres:
	// EXCLUDED.col). Custom expressions must table-qualify references to the
	// target row because Postgres also has the EXCLUDED row in scope.
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
	// numbers, objects, and arrays must all yield false. Path contains plain
	// identifier object keys (letters, digits, and underscores), ordered from the
	// root to the value. Implementations panic when a key is not a plain identifier.
	JSONBooleanIsTrue(expression string, path []string) string
	// IndexHint returns an optional table-level hint that directs lookups through
	// index. The returned text includes any required leading whitespace and is
	// placed immediately after a table name or alias; dialects without index-hint
	// syntax return an empty string.
	IndexHint(index string) string
	// BinaryEquals returns a predicate that is true only when column's stored
	// value equals the single bound placeholder byte-for-byte. Opaque identity
	// values such as lock owners compare byte-wise by contract, so dialects
	// whose default column collation folds case or accents must force a binary
	// comparison rather than inherit the column's collation.
	BinaryEquals(column string) string
	// JoinedUpdate returns an UPDATE that changes target rows selected through a
	// join. Aliases qualify join and predicate expressions, while assignments to
	// target columns must be unqualified so the statement is valid across
	// dialects. joinCondition and predicate remain separate so dialects can place
	// them in the clauses their syntax requires. Because that placement differs
	// per dialect, joinCondition must not contain bind placeholders: placeholders
	// are permitted only in assignment expressions and predicate, and callers
	// supply their arguments in that order. Implementations panic when
	// assignments is empty, joinCondition contains a placeholder, or an
	// assignment column is qualified.
	JoinedUpdate(targetTable, targetAlias, joinTable, joinAlias, joinCondition string, assignments []JoinedUpdateAssignment, predicate string) string
	// JoinedDelete returns a DELETE that removes target rows selected through a
	// join. Aliases qualify join and predicate expressions. joinCondition and
	// predicate remain separate so dialects can place them in the clauses their
	// syntax requires; because that placement differs per dialect, joinCondition
	// must not contain bind placeholders — placeholders are permitted only in
	// predicate. Implementations panic when joinCondition contains a
	// placeholder.
	JoinedDelete(targetTable, targetAlias, joinTable, joinAlias, joinCondition, predicate string) string
	// LeaseTokenFence returns a predicate fragment, for use inside a
	// JoinedUpdate or JoinedDelete predicate, that passes only while the joined
	// row — reached through joinAlias over joinTable and identified by its
	// idColumn primary key — still carries the lease token bound to the
	// fragment's single placeholder. The fence must serialize against a
	// concurrent lease steal: a steal that commits while the guarded statement
	// runs must fail the fence rather than let a stale token check pass, and a
	// fence that wins the race must block the steal until the guarded write
	// commits. Dialects whose joined DML record-locks the scanned joined rows
	// achieve this with plain token equality; dialects whose joined DML reads
	// the joined table without locks must lock the row explicitly.
	LeaseTokenFence(joinTable, joinAlias, idColumn, tokenColumn string) string
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

// JoinedUpdateAssignment describes one target-table column update. Column is
// unqualified; Expr is the raw SQL expression assigned to it.
type JoinedUpdateAssignment struct {
	Column string
	Expr   string
}

// MySQLDialect implements Dialect for MySQL and MySQL-protocol engines.
type MySQLDialect struct{}

var plainJSONIdentifier = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// InsertIfAbsent uses MySQL's INSERT IGNORE modifier. MySQL infers conflicts
// from every unique key on the table, so conflictColumns is not rendered.
// IGNORE suppresses more than duplicate-key conflicts — it also downgrades
// value truncation and NOT NULL violations to warnings — so callers must bind
// schema-valid values (see the Dialect interface contract).
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
	var jsonPath strings.Builder
	jsonPath.WriteString("$")
	for _, key := range path {
		if !plainJSONIdentifier.MatchString(key) {
			panic(fmt.Sprintf("sqlstore: JSON path key %q is not a plain identifier", key))
		}
		jsonPath.WriteString(`."` + key + `"`)
	}
	return "(JSON_EXTRACT(" + expression + ", '" + jsonPath.String() + "') <=> CAST('true' AS JSON))"
}

// IndexHint returns a MySQL FORCE INDEX hint for the named index.
func (MySQLDialect) IndexHint(index string) string {
	return " FORCE INDEX (`" + strings.ReplaceAll(index, "`", "``") + "`)"
}

// BinaryEquals compares under the binary collation: the schema's table default
// is accent- and case-insensitive utf8mb4_0900_ai_ci, under which
// differently-spelled opaque values would match.
func (MySQLDialect) BinaryEquals(column string) string {
	return column + " COLLATE utf8mb4_0900_bin = ?"
}

// JoinedUpdate builds a MySQL multi-table UPDATE statement.
func (MySQLDialect) JoinedUpdate(targetTable, targetAlias, joinTable, joinAlias, joinCondition string, assignments []JoinedUpdateAssignment, predicate string) string {
	if len(assignments) == 0 {
		panic("sqlstore: JoinedUpdate requires at least one assignment")
	}
	if strings.Contains(joinCondition, "?") {
		panic("sqlstore: JoinedUpdate joinCondition must not contain bind placeholders")
	}
	sets := make([]string, len(assignments))
	for i, assignment := range assignments {
		if strings.Contains(assignment.Column, ".") {
			panic("sqlstore: JoinedUpdate assignment columns must be unqualified")
		}
		sets[i] = targetAlias + "." + assignment.Column + " = " + assignment.Expr
	}
	return "UPDATE " + targetTable + " " + targetAlias +
		" JOIN " + joinTable + " " + joinAlias + " ON " + joinCondition +
		" SET " + strings.Join(sets, ", ") +
		" WHERE " + predicate
}

// JoinedDelete builds a MySQL multi-table DELETE statement that removes only
// the target alias's rows.
func (MySQLDialect) JoinedDelete(targetTable, targetAlias, joinTable, joinAlias, joinCondition, predicate string) string {
	if strings.Contains(joinCondition, "?") {
		panic("sqlstore: JoinedDelete joinCondition must not contain bind placeholders")
	}
	return "DELETE " + targetAlias +
		" FROM " + targetTable + " " + targetAlias +
		" JOIN " + joinTable + " " + joinAlias + " ON " + joinCondition +
		" WHERE " + predicate
}

// LeaseTokenFence renders a plain token-equality check: MySQL's multi-table
// UPDATE and DELETE record-lock the scanned rows of the joined table, so the
// equality alone serializes against a concurrent lease steal.
func (MySQLDialect) LeaseTokenFence(_, joinAlias, _, tokenColumn string) string {
	return joinAlias + "." + tokenColumn + " = ?"
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

// PostgresDialect implements Dialect for PostgreSQL.
type PostgresDialect struct{}

// Rebind rewrites store-native question-mark placeholders to PostgreSQL's
// ordinal placeholders. Question marks inside SQL string literals are data,
// not placeholders, and are left untouched.
func (PostgresDialect) Rebind(query string) string {
	var b strings.Builder
	b.Grow(len(query))
	ordinal := 1
	quote := byte(0)
	dollarQuote := ""
	for i := 0; i < len(query); i++ {
		if dollarQuote != "" {
			if strings.HasPrefix(query[i:], dollarQuote) {
				b.WriteString(dollarQuote)
				i += len(dollarQuote) - 1
				dollarQuote = ""
			} else {
				b.WriteByte(query[i])
			}
			continue
		}
		if quote != 0 {
			b.WriteByte(query[i])
			if query[i] == quote && i+1 < len(query) && query[i+1] == quote {
				i++
				b.WriteByte(query[i])
				continue
			}
			if query[i] == quote {
				quote = 0
			}
			continue
		}
		if query[i] == '\'' || query[i] == '"' {
			quote = query[i]
			b.WriteByte(query[i])
			continue
		}
		// A dollar sign that continues an identifier (PostgreSQL permits $
		// inside identifiers) never opens a dollar-quoted string.
		if query[i] == '$' && (i == 0 || !isPostgresIdentifierChar(query[i-1])) {
			if end := strings.IndexByte(query[i+1:], '$'); end >= 0 {
				delimiter := query[i : i+end+2]
				tag := delimiter[1 : len(delimiter)-1]
				// A dollar-quote tag follows identifier rules: letters,
				// digits, and underscores, but never a leading digit — the
				// server lexes a digit-leading run like $1 as a parameter
				// placeholder, not a quote delimiter.
				valid := true
				for j := range len(tag) {
					c := tag[j]
					isLetter := c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
					isDigit := c >= '0' && c <= '9'
					if (!isLetter && !isDigit) || (j == 0 && isDigit) {
						valid = false
						break
					}
				}
				if valid {
					dollarQuote = delimiter
					b.WriteString(delimiter)
					i += len(delimiter) - 1
					continue
				}
			}
		}
		if query[i] == '-' && i+1 < len(query) && query[i+1] == '-' {
			end := strings.IndexByte(query[i:], '\n')
			if end < 0 {
				b.WriteString(query[i:])
				break
			}
			b.WriteString(query[i : i+end])
			i += end - 1
			continue
		}
		// Block comments nest in PostgreSQL; question marks inside them are
		// prose, not placeholders. An unterminated comment consumes the rest of
		// the query, matching the server's tokenization.
		if query[i] == '/' && i+1 < len(query) && query[i+1] == '*' {
			depth := 1
			end := i + 2
			for end < len(query) && depth > 0 {
				switch {
				case query[end] == '/' && end+1 < len(query) && query[end+1] == '*':
					depth++
					end += 2
				case query[end] == '*' && end+1 < len(query) && query[end+1] == '/':
					depth--
					end += 2
				default:
					end++
				}
			}
			b.WriteString(query[i:end])
			i = end - 1
			continue
		}
		if query[i] == '?' {
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(ordinal))
			ordinal++
			continue
		}
		b.WriteByte(query[i])
	}
	return b.String()
}

// InsertIfAbsent uses PostgreSQL's ON CONFLICT DO NOTHING clause targeting the
// named unique-key columns. A skipped conflicting insert reports zero affected
// rows, matching the interface contract.
func (PostgresDialect) InsertIfAbsent(conflictColumns []string) InsertIfAbsentSyntax {
	return InsertIfAbsentSyntax{Suffix: " ON CONFLICT (" + strings.Join(conflictColumns, ", ") + ") DO NOTHING"}
}

// JSONBooleanIsTrue extracts the JSONB value at path and compares it with JSON
// true using null-safe equality, so missing paths, JSON null, and SQL NULL all
// yield false rather than NULL.
func (PostgresDialect) JSONBooleanIsTrue(expression string, path []string) string {
	keys := make([]string, len(path))
	for i, key := range path {
		if !plainJSONIdentifier.MatchString(key) {
			panic(fmt.Sprintf("sqlstore: JSON path key %q is not a plain identifier", key))
		}
		keys[i] = "'" + key + "'"
	}
	return "(jsonb_extract_path(" + expression + ", " + strings.Join(keys, ", ") + ") IS NOT DISTINCT FROM 'true'::jsonb)"
}

// IndexHint returns an empty string: PostgreSQL has no index-hint syntax and
// relies on the planner to choose the access path.
func (PostgresDialect) IndexHint(string) string { return "" }

// BinaryEquals returns plain equality: PostgreSQL's deterministic collations
// already compare equality byte-for-byte.
func (PostgresDialect) BinaryEquals(column string) string {
	return column + " = ?"
}

// JoinedUpdate builds a PostgreSQL UPDATE … FROM statement. The join condition
// moves into the WHERE clause alongside the residual predicate; SET
// placeholders still precede predicate placeholders, so the placeholder-free
// join condition the interface requires keeps argument order identical to the
// MySQL rendering.
//
// Locking of the joined rows differs from MySQL: a MySQL multi-table UPDATE
// record-locks the scanned rows of the joined table, so a lease-token
// predicate on the joined table serializes against a concurrent lease steal.
// UPDATE … FROM reads the FROM rows from the MVCC snapshot without locking
// them, so under READ COMMITTED a write guarded only by a joined-table token
// check can pass the check and commit after a steal commits. Guards that
// require steal serialization must build their token check with
// LeaseTokenFence, whose PostgreSQL rendering locks the joined row, rather
// than embed a plain token equality in the predicate.
func (PostgresDialect) JoinedUpdate(targetTable, targetAlias, joinTable, joinAlias, joinCondition string, assignments []JoinedUpdateAssignment, predicate string) string {
	if len(assignments) == 0 {
		panic("sqlstore: JoinedUpdate requires at least one assignment")
	}
	if strings.Contains(joinCondition, "?") {
		panic("sqlstore: JoinedUpdate joinCondition must not contain bind placeholders")
	}
	sets := make([]string, len(assignments))
	for i, assignment := range assignments {
		if strings.Contains(assignment.Column, ".") {
			panic("sqlstore: JoinedUpdate assignment columns must be unqualified")
		}
		sets[i] = assignment.Column + " = " + assignment.Expr
	}
	return "UPDATE " + targetTable + " " + targetAlias +
		" SET " + strings.Join(sets, ", ") +
		" FROM " + joinTable + " " + joinAlias +
		" WHERE (" + joinCondition + ") AND (" + predicate + ")"
}

// JoinedDelete builds a PostgreSQL DELETE … USING statement. The join
// condition moves into the WHERE clause alongside the residual predicate
// (DELETE … USING has no ON clause), preserving the MySQL rendering's argument
// order. USING rows share UPDATE … FROM's locking semantics — read from the
// MVCC snapshot without locks — so JoinedUpdate's lease-fencing caveat applies
// here too: build steal-serializing token checks with LeaseTokenFence.
func (PostgresDialect) JoinedDelete(targetTable, targetAlias, joinTable, joinAlias, joinCondition, predicate string) string {
	if strings.Contains(joinCondition, "?") {
		panic("sqlstore: JoinedDelete joinCondition must not contain bind placeholders")
	}
	return "DELETE FROM " + targetTable + " " + targetAlias +
		" USING " + joinTable + " " + joinAlias +
		" WHERE (" + joinCondition + ") AND (" + predicate + ")"
}

// LeaseTokenFence locks the joined row while checking its token. UPDATE … FROM
// and DELETE … USING read the joined table from the MVCC snapshot without
// locks, so a plain token equality could pass with a stale value after a
// concurrent steal commits. The correlated FOR UPDATE subquery record-locks
// the joined row instead: under READ COMMITTED a lock wait re-evaluates the
// subquery's predicate against the latest committed row version, so a steal
// that commits first fails the fence, and a fence that locks first blocks the
// steal until the guarded write commits — the serialization MySQL's
// multi-table row locking provides.
func (PostgresDialect) LeaseTokenFence(joinTable, joinAlias, idColumn, tokenColumn string) string {
	return joinAlias + "." + idColumn +
		" = (SELECT fence." + idColumn +
		" FROM " + joinTable + " fence" +
		" WHERE fence." + idColumn + " = " + joinAlias + "." + idColumn +
		" AND fence." + tokenColumn + " = ? FOR UPDATE)"
}

// ExcludedValue returns the PostgreSQL reference to the proposed row value.
func (PostgresDialect) ExcludedValue(column string) string { return "EXCLUDED." + column }

// UpsertClause builds a PostgreSQL ON CONFLICT DO UPDATE clause.
func (d PostgresDialect) UpsertClause(conflictColumns []string, assignments []UpsertAssignment) string {
	sets := make([]string, len(assignments))
	for i, a := range assignments {
		expr := a.Expr
		if expr == "" {
			expr = d.ExcludedValue(a.Column)
		}
		sets[i] = a.Column + " = " + expr
	}
	return "ON CONFLICT (" + strings.Join(conflictColumns, ", ") + ") DO UPDATE SET " + strings.Join(sets, ", ")
}

// CurrentTimestamp returns PostgreSQL's transaction timestamp. PostgreSQL
// timestamps retain microsecond precision without a precision argument.
func (PostgresDialect) CurrentTimestamp(precision TimestampPrecision) string {
	switch precision {
	case TimestampPrecisionDefault, TimestampPrecisionMicrosecond:
		return "now()"
	default:
		panic(fmt.Sprintf("sqlstore: unknown timestamp precision %d", precision))
	}
}

// RelativeTime builds PostgreSQL interval arithmetic while retaining the
// store-native placeholder for the execution-boundary binder.
func (d PostgresDialect) RelativeTime(precision TimestampPrecision, direction RelativeTimeDirection, amount IntervalAmount, unit IntervalUnit) string {
	magnitude := "?"
	if !amount.parameterized {
		magnitude = strconv.FormatUint(amount.literal, 10)
	}
	op := " - "
	if direction == AfterCurrentTime {
		op = " + "
	} else if direction != BeforeCurrentTime {
		panic(fmt.Sprintf("sqlstore: unknown relative-time direction %d", direction))
	}
	return d.CurrentTimestamp(precision) + op + magnitude + " * interval '1 " + postgresIntervalUnit(unit) + "'"
}

// isPostgresIdentifierChar reports whether c can appear inside a PostgreSQL
// identifier body (letters, digits, underscores, and dollar signs).
func isPostgresIdentifierChar(c byte) bool {
	return c == '_' || c == '$' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func postgresIntervalUnit(unit IntervalUnit) string {
	switch unit {
	case IntervalMicrosecond:
		return "microsecond"
	case IntervalSecond:
		return "second"
	case IntervalMinute:
		return "minute"
	case IntervalHour:
		return "hour"
	case IntervalDay:
		return "day"
	default:
		panic(fmt.Sprintf("sqlstore: unknown interval unit %d", unit))
	}
}
