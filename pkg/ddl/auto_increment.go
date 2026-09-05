package ddl

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/statement"
)

// autoIncrementKeyword is the keyword shared by the table-level counter option
// and the column-level attribute. Only the table option carries a value.
const autoIncrementKeyword = "AUTO_INCREMENT"

// StripTableAutoIncrement removes the table-level AUTO_INCREMENT=N counter from
// a MySQL-family CREATE TABLE statement and returns every other byte exactly as
// it was given. The counter records how far one instance's sequence has
// advanced, so it is noise in a declarative schema file and a spurious diff when
// the same table is read from two instances.
//
// Only the table option is removed. The column-level AUTO_INCREMENT attribute is
// preserved: dropping it would produce a table whose ids no longer generate. A
// statement carrying no counter is returned untouched, so the word appearing in a
// column default, a column comment or an identifier is never rewritten.
//
// The statement is parsed to decide whether a counter is present, and the result
// is parsed again to confirm that removing it changed nothing else. A statement
// that cannot be parsed, or whose counter cannot be located and removed cleanly,
// is an error rather than a passthrough: returning the input would write one
// instance's counter into a schema file.
func StripTableAutoIncrement(stmt string) (string, error) {
	ct, err := statement.ParseCreateTable(stmt)
	if err != nil {
		return "", fmt.Errorf("parse CREATE TABLE to strip its auto-increment counter: %w", err)
	}
	if ct == nil || ct.Raw == nil {
		return "", fmt.Errorf("parse CREATE TABLE to strip its auto-increment counter: parser returned no statement")
	}
	if !slices.ContainsFunc(ct.Raw.Options, isAutoIncrementOption) {
		return stmt, nil
	}
	start, end, ok := tableAutoIncrementSpan(stmt)
	if !ok {
		return "", fmt.Errorf("locate the auto-increment counter in CREATE TABLE %s: parsed as a table option but absent from the statement text", ct.TableName)
	}
	stripped := stmt[:start] + stmt[end:]
	if err := verifyOnlyCounterRemoved(ct.Raw, stripped); err != nil {
		return "", fmt.Errorf("strip the auto-increment counter from CREATE TABLE %s: %w", ct.TableName, err)
	}
	return stripped, nil
}

func isAutoIncrementOption(opt *ast.TableOption) bool {
	return opt != nil && opt.Tp == ast.TableOptionAutoIncrement
}

// verifyOnlyCounterRemoved reports whether stripped is the parsed original with
// its counter dropped and nothing else touched. Both sides are compared in the
// parser's own canonical form, so the check is on the statement's meaning rather
// than on the bytes the removal happened to leave behind. original is consumed:
// its counter option is deleted in place to build the expected form.
func verifyOnlyCounterRemoved(original *ast.CreateTableStmt, stripped string) error {
	strippedCT, err := statement.ParseCreateTable(stripped)
	if err != nil {
		return fmt.Errorf("re-parse the stripped statement: %w", err)
	}
	if strippedCT == nil || strippedCT.Raw == nil {
		return fmt.Errorf("re-parse the stripped statement: parser returned no statement")
	}
	original.Options = slices.DeleteFunc(original.Options, isAutoIncrementOption)
	want, err := restoreCreateTable(original)
	if err != nil {
		return fmt.Errorf("restore the parsed statement without its counter: %w", err)
	}
	got, err := restoreCreateTable(strippedCT.Raw)
	if err != nil {
		return fmt.Errorf("restore the stripped statement: %w", err)
	}
	if want != got {
		return fmt.Errorf("removing the counter changed the rest of the statement")
	}
	return nil
}

func restoreCreateTable(stmt *ast.CreateTableStmt) (string, error) {
	var sb strings.Builder
	if err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		return "", err
	}
	return sb.String(), nil
}

// tableAutoIncrementSpan returns the byte range of the table-level counter,
// extended backwards over the spacing that separates it from the option before
// it. The scan steps over quoted identifiers, string literals and comments, so
// the keyword inside a default, a comment or an identifier is never matched, and
// it only considers text outside the definition list, which is where the
// column-level attribute lives. ok is false when no such span exists.
func tableAutoIncrementSpan(stmt string) (start, end int, ok bool) {
	depth := 0
	for i := 0; i < len(stmt); {
		switch c := stmt[i]; {
		case c == '`' || c == '\'' || c == '"':
			i = skipQuoted(stmt, i)
			continue
		case c == '/' && i+1 < len(stmt) && stmt[i+1] == '*':
			i = skipBlockComment(stmt, i)
			continue
		case c == '#' || (c == '-' && i+1 < len(stmt) && stmt[i+1] == '-'):
			i = skipLineComment(stmt, i)
			continue
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && (c == 'A' || c == 'a') && isKeywordAt(stmt, i, autoIncrementKeyword):
			if valueEnd, hasValue := counterValueEnd(stmt, i+len(autoIncrementKeyword)); hasValue {
				return trimSpacingBefore(stmt, i), valueEnd, true
			}
		}
		i++
	}
	return 0, 0, false
}

// skipQuoted returns the index just past the quoted run starting at i. A quote
// inside the run is escaped either by doubling it or, for the string quotes,
// with a backslash; SHOW CREATE TABLE emits the backslash form.
func skipQuoted(s string, i int) int {
	quote := s[i]
	for j := i + 1; j < len(s); j++ {
		switch s[j] {
		case '\\':
			if quote != '`' {
				j++
			}
		case quote:
			if j+1 < len(s) && s[j+1] == quote {
				j++
				continue
			}
			return j + 1
		}
	}
	return len(s)
}

func skipBlockComment(s string, i int) int {
	if end := strings.Index(s[i+2:], "*/"); end >= 0 {
		return i + 2 + end + 2
	}
	return len(s)
}

func skipLineComment(s string, i int) int {
	if end := strings.IndexByte(s[i:], '\n'); end >= 0 {
		return i + end + 1
	}
	return len(s)
}

// isKeywordAt reports whether the complete word at i is keyword, compared
// case-insensitively. A keyword embedded in a longer identifier does not match.
func isKeywordAt(s string, i int, keyword string) bool {
	if i+len(keyword) > len(s) || !strings.EqualFold(s[i:i+len(keyword)], keyword) {
		return false
	}
	if i > 0 && isWordByte(s[i-1]) {
		return false
	}
	end := i + len(keyword)
	return end == len(s) || !isWordByte(s[end])
}

// counterValueEnd returns the index just past the counter's digits when the text
// at i is the table option's optional `=` and its value. It reports false for the
// bare column-level attribute, which carries no value.
func counterValueEnd(s string, i int) (int, bool) {
	j := skipWhitespace(s, i)
	if j < len(s) && s[j] == '=' {
		j = skipWhitespace(s, j+1)
	}
	digits := j
	for digits < len(s) && s[digits] >= '0' && s[digits] <= '9' {
		digits++
	}
	if digits == j {
		return 0, false
	}
	return digits, true
}

func skipWhitespace(s string, i int) int {
	for i < len(s) && isWhitespaceByte(s[i]) {
		i++
	}
	return i
}

// trimSpacingBefore walks back over the blanks preceding i so the removal takes
// the separator with the option. Line breaks are left in place to preserve the
// statement's layout.
func trimSpacingBefore(s string, i int) int {
	for i > 0 && isSpacingByte(s[i-1]) {
		i--
	}
	return i
}

func isSpacingByte(c byte) bool {
	return c == ' ' || c == '\t'
}

func isWhitespaceByte(c byte) bool {
	return isSpacingByte(c) || c == '\n' || c == '\r'
}

func isWordByte(c byte) bool {
	return c == '_' || c == '$' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}
