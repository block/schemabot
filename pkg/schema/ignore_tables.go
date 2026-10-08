package schema

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

// ignoreTablePatternDelimiter wraps a pattern entry. A table name cannot
// carry it, since a plain entry containing a path separator is refused, so an
// entry it wraps is unambiguously a pattern and every other entry keeps
// meaning the one table it names.
const ignoreTablePatternDelimiter = "/"

// ValidateIgnoreTables rejects ignore_tables entries that cannot match a live
// table: blank entries, entries padded with whitespace (a target spells a
// table name without padding, so such an entry would silently withhold
// nothing), entries with path separators, which name a file rather than a
// table, and pattern entries whose expression does not compile.
//
// A plain entry is a literal table name. Unlike ignore_namespaces there is no
// $ENV substitution: an entry is matched against the target's own catalog,
// exactly and case-sensitively. An entry wrapped in slashes is a pattern (see
// CompileIgnoreTablePattern).
func ValidateIgnoreTables(tables []string) error {
	for _, name := range tables {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("ignore_tables entries must not be blank")
		}
		if name != strings.TrimSpace(name) {
			return fmt.Errorf("ignore_tables entry %s must not have leading or trailing whitespace", QuoteIgnoreTablesEntry(name))
		}
		if IsIgnoreTablePattern(name) {
			if _, err := CompileIgnoreTablePattern(name, false); err != nil {
				return err
			}
			continue
		}
		if strings.ContainsAny(name, `/\`) {
			return fmt.Errorf("ignore_tables entry %s must be a table name, not a path. To match a family of tables, wrap a regular expression in slashes, such as \"/^events_[0-9]+$/\"", QuoteIgnoreTablesEntry(name))
		}
	}
	return nil
}

// IsIgnoreTablePattern reports whether an ignore_tables entry is a pattern: a
// regular expression wrapped in slashes. A lone slash is not one; it is
// refused as a path.
func IsIgnoreTablePattern(entry string) bool {
	return len(entry) >= 2 &&
		strings.HasPrefix(entry, ignoreTablePatternDelimiter) &&
		strings.HasSuffix(entry, ignoreTablePatternDelimiter)
}

// CompileIgnoreTablePattern compiles a pattern entry into a regular
// expression that matches a table name only in full: the expression is
// anchored at both ends whether or not it is written with anchors, so a
// pattern meant for one family of tables cannot reach a table that merely
// contains a matching run of characters. The syntax is Go's RE2, which has no
// backtracking, so no pattern can stall a plan.
//
// The expression is compiled on its own before it is anchored, which is what
// makes the anchoring hold: an expression that only parses once wrapped, such
// as `a)|(b`, would otherwise close the anchoring group early and match any
// name starting with a or ending with b.
//
// foldCase compiles the pattern to ignore case. Withholding never does (see
// engine.IgnoredTables); only the refusal of a pattern that reaches a declared
// table does.
//
// The error names the entry as written, so an operator can find it in
// schemabot.yaml.
func CompileIgnoreTablePattern(entry string, foldCase bool) (*regexp.Regexp, error) {
	if !IsIgnoreTablePattern(entry) {
		return nil, fmt.Errorf("ignore_tables entry %s is not a pattern: a pattern is a regular expression wrapped in slashes", QuoteIgnoreTablesEntry(entry))
	}
	expr := entry[len(ignoreTablePatternDelimiter) : len(entry)-len(ignoreTablePatternDelimiter)]
	if expr == "" {
		return nil, fmt.Errorf("ignore_tables entry %s is an empty pattern: write a regular expression between the slashes", QuoteIgnoreTablesEntry(entry))
	}
	if _, err := regexp.Compile(expr); err != nil {
		return nil, fmt.Errorf("ignore_tables entry %s is not a valid regular expression: %w", QuoteIgnoreTablesEntry(entry), err)
	}
	anchored := `\A(?:` + expr + `)\z`
	if foldCase {
		anchored = `(?i)` + anchored
	}
	re, err := regexp.Compile(anchored)
	if err != nil {
		return nil, fmt.Errorf("ignore_tables entry %s is not a valid regular expression: %w", QuoteIgnoreTablesEntry(entry), err)
	}
	return re, nil
}

// QuoteIgnoreTablesEntry renders an entry for an operator-facing message as it
// is written in schemabot.yaml, so a pattern reads with the backslashes its
// author typed rather than with each one escaped. An entry holding a quote or
// a character that does not print is quoted with escapes instead, so where it
// starts and ends stays unambiguous.
func QuoteIgnoreTablesEntry(entry string) string {
	for _, r := range entry {
		if r == '"' || !unicode.IsPrint(r) {
			return strconv.Quote(entry)
		}
	}
	return `"` + entry + `"`
}
