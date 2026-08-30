package ddl

import (
	"log/slog"
	"regexp"
	"strings"

	"github.com/block/schemabot/pkg/schema"
)

// FormatDDL formats a DDL statement for better readability.
// It first canonicalizes using Spirit's parser, then formats:
//   - ALTER statements: each clause on its own line
//   - CREATE TABLE statements: each column/index on its own line
//   - Data types, functions, and charset/collate values are lowercased
//     while SQL keywords remain uppercase (PlanetScale style).
func FormatDDL(ddl string) string {
	// Canonicalize first
	ddl = Canonicalize(ddl)

	result := lowercaseTypes(layoutDDL(ddl))

	// Ensure trailing semicolon
	result = strings.TrimRight(result, "; ")
	return result + ";"
}

// layoutDDL line-breaks a canonicalized statement for readability: a CREATE
// TABLE gets each column/index and table option on its own line, and a
// multi-clause ALTER TABLE gets each clause on its own line. The layout is
// plain string splitting on the statement's own text, so it applies to any
// dialect's canonical form. Other statement types are returned unchanged.
func layoutDDL(ddl string) string {
	upperDDL := strings.ToUpper(ddl)

	switch {
	case strings.HasPrefix(upperDDL, "CREATE TABLE"):
		return formatCreateTable(ddl)
	case strings.HasPrefix(upperDDL, "ALTER TABLE"):
		clauses := splitAlterClauses(ddl)
		if len(clauses) <= 1 {
			return ddl
		}
		// Extract table header (ALTER TABLE `name`) from first clause
		tableEnd := findTableNameEnd(clauses[0])
		tableHeader := strings.TrimSpace(clauses[0][:tableEnd-1]) // -1 to remove trailing space
		firstClause := strings.TrimSpace(clauses[0][tableEnd:])

		// Format with header on first line, each clause on its own indented line
		var sb strings.Builder
		sb.WriteString(tableHeader)
		sb.WriteString("\n    ")
		sb.WriteString(firstClause)
		for i := 1; i < len(clauses); i++ {
			sb.WriteString(",\n    ")
			sb.WriteString(clauses[i])
		}
		return sb.String()
	default:
		return ddl
	}
}

// FormatDDLForDialect formats a DDL statement for display under the dialect's
// own grammar. The MySQL family gets FormatDDL's full treatment; any other
// dialect renders its own parser's canonical form with the same line-break
// layout — so a statement is never canonicalized, or judged unparseable,
// under another family's grammar — but skips the MySQL-specific lowercasing
// pass. Like FormatDDL, this is a best-effort display formatter: a statement
// the dialect's parser rejects — or any statement of a dialect with no
// registered parser (logged, since it means a database type reached the
// display layer without a parser) — renders unformatted, with only
// surrounding whitespace trimmed and a trailing semicolon enforced.
func FormatDDLForDialect(dialect schema.Dialect, stmt string) string {
	if dialect == schema.DialectMySQL {
		return FormatDDL(stmt)
	}
	result := strings.TrimSpace(stmt)
	if p, err := ParserForDialect(dialect); err != nil {
		slog.Warn("DDL display formatting has no parser for this dialect; statements will render unformatted",
			"dialect", dialect, "error", err)
	} else {
		result = layoutDDL(p.Canonicalize(result))
	}
	return strings.TrimRight(result, "; ") + ";"
}

// dataTypePattern matches SQL data types that should be lowercased.
// Ordered longest-first to avoid partial matches (e.g., MEDIUMINT before INT).
// Uses negative lookbehind to avoid matching SET in "CHARACTER SET".
var dataTypePattern = regexp.MustCompile(`\b(` +
	`BIGINT|MEDIUMINT|SMALLINT|TINYINT|INT|INTEGER|` +
	`VARBINARY|VARCHAR|BINARY|CHAR|` +
	`TINYTEXT|MEDIUMTEXT|LONGTEXT|TEXT|` +
	`TINYBLOB|MEDIUMBLOB|LONGBLOB|BLOB|` +
	`TIMESTAMP|DATETIME|DATE|TIME|YEAR|` +
	`DECIMAL|NUMERIC|FLOAT|DOUBLE|` +
	`BOOLEAN|BOOL|BIT|JSON|ENUM|` +
	`UNSIGNED|SIGNED` +
	`)\b`)

// funcPattern matches SQL function names that should be lowercased.
var funcPattern = regexp.MustCompile(`\b(CURRENT_TIMESTAMP|CURRENT_DATE|CURRENT_TIME|NOW|UUID)\b`)

// charsetCollatePattern matches charset/collate values after their keywords.
// Matches: CHARACTER SET = UTF8MB4, CHARSET utf8mb4, COLLATE UTF8MB4_0900_AI_CI
var charsetCollatePattern = regexp.MustCompile(
	`((?:CHARACTER SET|CHARSET|COLLATE)\s*=?\s*)([A-Z][A-Z0-9_]+)`)

// charsetLiteralPattern matches _CHARSET'string' prefixes like _UTF8MB4'pending'.
// These are stripped entirely since the column charset makes them redundant.
var charsetLiteralPattern = regexp.MustCompile(`_(UTF8MB4|UTF8|LATIN1|ASCII|BINARY)(')`)

// lowercaseTypes post-processes canonicalized DDL to lowercase data types,
// function names, and charset/collate values while keeping SQL keywords uppercase.
func lowercaseTypes(ddl string) string {
	// Lowercase charset/collate values first (before data types, to avoid
	// matching SET in "CHARACTER SET" as the SET data type)
	ddl = charsetCollatePattern.ReplaceAllStringFunc(ddl, func(match string) string {
		loc := charsetCollatePattern.FindStringSubmatchIndex(match)
		if loc == nil {
			return match
		}
		prefix := match[loc[2]:loc[3]]
		value := match[loc[4]:loc[5]]
		return prefix + strings.ToLower(value)
	})

	// Strip _CHARSET'...' introducers (redundant with column charset)
	ddl = charsetLiteralPattern.ReplaceAllString(ddl, "$2")

	// Strip redundant DEFAULT NULL (implied for nullable columns)
	ddl = strings.ReplaceAll(ddl, " DEFAULT NULL", "")

	// Lowercase data types
	ddl = dataTypePattern.ReplaceAllStringFunc(ddl, strings.ToLower)

	// Lowercase function names
	ddl = funcPattern.ReplaceAllStringFunc(ddl, strings.ToLower)

	return ddl
}

// formatCreateTable formats a CREATE TABLE statement with line breaks.
func formatCreateTable(ddl string) string {
	// Find the opening parenthesis
	openParen := strings.Index(ddl, "(")
	if openParen == -1 {
		return ddl
	}

	// Find the matching closing parenthesis (for the column definitions)
	closeParen := findMatchingParen(ddl, openParen)
	if closeParen == -1 {
		return ddl
	}

	header := ddl[:openParen+1]           // "CREATE TABLE `name` ("
	body := ddl[openParen+1 : closeParen] // column definitions
	footer := ddl[closeParen:]            // ") ENGINE = ..."

	// Split the body by commas (respecting parentheses for things like VARCHAR(255))
	parts := splitByComma(body)

	// Format table options
	options := strings.TrimSpace(footer[1:]) // Skip the ")"
	options, partition := splitPartitionClause(options)

	if len(parts) <= 1 {
		// Single column — no line-break formatting for columns,
		// but still format table options if present
		if options != "" || partition != "" {
			return header + strings.TrimSpace(body) + ")" + formatFooter(options, partition)
		}
		return ddl
	}

	// Format with each part on its own line (4 spaces, PlanetScale style)
	var sb strings.Builder
	sb.WriteString(header)
	sb.WriteString("\n")
	for i, part := range parts {
		sb.WriteString("    ")
		sb.WriteString(strings.TrimSpace(part))
		if i < len(parts)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}
	sb.WriteString(")")
	sb.WriteString(formatFooter(options, partition))

	return sb.String()
}

// splitPartitionClause separates the trailing PARTITION BY clause, if any,
// from a CREATE TABLE options footer. Quoted strings are skipped so a COMMENT
// mentioning PARTITION BY is not mistaken for the clause.
func splitPartitionClause(options string) (opts, partition string) {
	const clause = "PARTITION BY"
	inQuote := false
	for i := 0; i+len(clause) <= len(options); i++ {
		if options[i] == '\'' {
			inQuote = !inQuote
			continue
		}
		if inQuote {
			continue
		}
		if strings.EqualFold(options[i:i+len(clause)], clause) {
			return strings.TrimSpace(options[:i]), strings.TrimSpace(options[i:])
		}
	}
	return options, ""
}

// formatFooter renders what follows the column list's closing parenthesis:
// table options each on their own line (2-space indent, PlanetScale style),
// then the PARTITION BY clause on its own line.
func formatFooter(options, partition string) string {
	var sb strings.Builder
	if options != "" {
		sb.WriteString(" ")
		sb.WriteString(formatTableOptions(options))
	}
	if partition != "" {
		if options != "" {
			sb.WriteString("\n  ")
		} else {
			sb.WriteString(" ")
		}
		sb.WriteString(partition)
	}
	return sb.String()
}

// tableOptionPattern matches individual table options in the options string.
// TiDB restores options as: ENGINE = InnoDB DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_0900_AI_CI COMMENT = '...'
var tableOptionPattern = regexp.MustCompile(
	`(?:DEFAULT\s+)?(?:ENGINE|CHARACTER SET|CHARSET|COLLATE|COMMENT|AUTO_INCREMENT|ROW_FORMAT|COMPRESSION|KEY_BLOCK_SIZE|STATS_PERSISTENT|STATS_AUTO_RECALC|PACK_KEYS)\s*=?\s*(?:'[^']*'|\S+)`)

// formatTableOptions splits table options onto separate indented lines.
// Input: "ENGINE = InnoDB DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_0900_AI_CI"
// Output: "ENGINE InnoDB,\n  CHARSET utf8mb4,\n  COLLATE utf8mb4_0900_ai_ci"
func formatTableOptions(options string) string {
	matches := tableOptionPattern.FindAllString(options, -1)
	if len(matches) == 0 {
		return options
	}

	// Normalize each option: strip "DEFAULT ", strip " = " → " "
	var normalized []string
	for _, m := range matches {
		m = strings.TrimSpace(m)
		// Remove leading "DEFAULT " prefix
		upper := strings.ToUpper(m)
		if strings.HasPrefix(upper, "DEFAULT ") {
			m = m[8:]
		}
		// Shorten "CHARACTER SET" to "CHARSET"
		if strings.HasPrefix(strings.ToUpper(m), "CHARACTER SET") {
			m = "CHARSET" + m[13:]
		}
		// Remove " = " → " "
		m = strings.Replace(m, " = ", " ", 1)
		normalized = append(normalized, m)
	}

	if len(normalized) == 1 {
		return normalized[0]
	}

	// First option on same line as ")", rest indented (2 spaces, PlanetScale style)
	var sb strings.Builder
	sb.WriteString(normalized[0])
	for i := 1; i < len(normalized); i++ {
		sb.WriteString(",\n  ")
		sb.WriteString(normalized[i])
	}
	return sb.String()
}

// findMatchingParen finds the index of the closing parenthesis that matches
// the opening parenthesis at the given position.
func findMatchingParen(s string, openPos int) int {
	depth := 0
	for i := openPos; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// splitByComma splits a string by commas, respecting parentheses.
func splitByComma(s string) []string {
	var parts []string
	var current strings.Builder
	depth := 0

	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '(':
			depth++
			current.WriteByte(c)
		case ')':
			depth--
			current.WriteByte(c)
		case ',':
			if depth == 0 {
				parts = append(parts, current.String())
				current.Reset()
			} else {
				current.WriteByte(c)
			}
		default:
			current.WriteByte(c)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}

// splitAlterClauses splits an ALTER TABLE statement into individual clauses.
func splitAlterClauses(ddl string) []string {
	// Find the end of "ALTER TABLE `tablename`" or "ALTER TABLE tablename"
	tableEnd := findTableNameEnd(ddl)
	if tableEnd >= len(ddl) {
		return []string{ddl}
	}

	tablePart := ddl[:tableEnd]
	clausesPart := ddl[tableEnd:]

	// Split on ", ADD ", ", DROP ", ", MODIFY ", ", CHANGE "
	// Track parentheses to avoid splitting inside column definitions
	var clauses []string
	var current strings.Builder
	parenDepth := 0

	for i := 0; i < len(clausesPart); i++ {
		c := clausesPart[i]
		switch c {
		case '(':
			parenDepth++
		case ')':
			parenDepth--
		}

		if parenDepth == 0 && c == ',' && i+1 < len(clausesPart) {
			// Check if this is followed by a clause keyword
			rest := strings.TrimLeft(clausesPart[i+1:], " ")
			if isClauseKeyword(rest) {
				clauses = append(clauses, current.String())
				current.Reset()
				i++ // Skip the comma
				// Skip leading spaces
				for i+1 < len(clausesPart) && clausesPart[i+1] == ' ' {
					i++
				}
				continue
			}
		}
		current.WriteByte(c)
	}
	if current.Len() > 0 {
		clauses = append(clauses, current.String())
	}

	if len(clauses) == 0 {
		return []string{ddl}
	}

	// Prepend the table part to the first clause
	clauses[0] = tablePart + clauses[0]
	return clauses
}

// findTableNameEnd finds the end position of "ALTER TABLE tablename " in the DDL.
func findTableNameEnd(ddl string) int {
	upperDDL := strings.ToUpper(ddl)

	// Try to find backtick-quoted table name: ALTER TABLE `tablename`
	if idx := strings.Index(ddl, "` "); idx != -1 && idx > 12 {
		return idx + 2
	}

	// Try to find unquoted table name: ALTER TABLE tablename
	// Skip "ALTER TABLE " (12 chars) and find the next space
	if len(ddl) > 12 && strings.HasPrefix(upperDDL, "ALTER TABLE ") {
		rest := ddl[12:]
		if spaceIdx := strings.Index(rest, " "); spaceIdx != -1 {
			return 12 + spaceIdx + 1
		}
	}

	return len(ddl)
}

// isClauseKeyword checks if the string starts with an ALTER TABLE clause keyword.
func isClauseKeyword(s string) bool {
	upper := strings.ToUpper(s)
	return strings.HasPrefix(upper, "ADD ") ||
		strings.HasPrefix(upper, "DROP ") ||
		strings.HasPrefix(upper, "MODIFY ") ||
		strings.HasPrefix(upper, "CHANGE ") ||
		strings.HasPrefix(upper, "RENAME ") ||
		strings.HasPrefix(upper, "ALTER ") ||
		strings.HasPrefix(upper, "CONVERT ") ||
		strings.HasPrefix(upper, "ENABLE ") ||
		strings.HasPrefix(upper, "DISABLE ") ||
		strings.HasPrefix(upper, "ORDER ")
}

// Canonicalize converts a DDL statement to canonical format.
// It delegates to the package's default StatementParser (the TiDB/Spirit
// implementation): ALTER TABLE statements are normalized with proper quoting
// and formatting, while CREATE TABLE and DROP TABLE use TiDB's Restore.
// Returns the original statement if parsing fails.
func Canonicalize(ddl string) string {
	return defaultParser.Canonicalize(ddl)
}
