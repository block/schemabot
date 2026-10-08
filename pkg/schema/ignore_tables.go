package schema

import (
	"cmp"
	"fmt"
	"regexp"
	"regexp/syntax"
	"slices"
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
// table does. The folding is applied to the parsed expression rather than by
// prefixing a flag, because an expression can turn a flag back off: a folded
// `(?-i)relay_[0-9]+_feed` must still match Relay_1_Feed, or the refusal would
// miss a declared table the target folds to the same name.
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
	if foldCase {
		parsed, err := syntax.Parse(expr, syntax.Perl)
		if err != nil {
			return nil, fmt.Errorf("ignore_tables entry %s is not a valid regular expression: %w", QuoteIgnoreTablesEntry(entry), err)
		}
		foldRegexpCase(parsed)
		expr = parsed.String()
	}
	anchored := `\A(?:` + expr + `)\z`
	re, err := regexp.Compile(anchored)
	if err != nil {
		return nil, fmt.Errorf("ignore_tables entry %s is not a valid regular expression: %w", QuoteIgnoreTablesEntry(entry), err)
	}
	return re, nil
}

// foldRegexpCase makes a parsed expression match a name when any spelling of
// it that differs only in case matches the expression as written, whatever
// flags the expression sets. That is the name a case-folding target would
// treat as the table the pattern withholds. Each literal and character class
// is closed under case folding; a negated class is folded after negation, so
// `[^a-z]`, which as written matches A, also matches a.
func foldRegexpCase(re *syntax.Regexp) {
	switch re.Op {
	case syntax.OpLiteral:
		re.Flags |= syntax.FoldCase
	case syntax.OpCharClass:
		re.Rune = foldedRanges(re.Rune)
	}
	for _, sub := range re.Sub {
		foldRegexpCase(sub)
	}
}

// foldedRanges adds to a character class's rune ranges every rune that a rune
// in them folds to. Only runes inside unicode.CaseRanges have another case, so
// a range is walked only where it overlaps them.
func foldedRanges(ranges []rune) []rune {
	minFold := rune(unicode.CaseRanges[0].Lo)
	maxFold := rune(unicode.CaseRanges[len(unicode.CaseRanges)-1].Hi)
	out := slices.Clone(ranges)
	for i := 0; i+1 < len(ranges); i += 2 {
		for c := max(ranges[i], minFold); c <= min(ranges[i+1], maxFold); c++ {
			for f := unicode.SimpleFold(c); f != c; f = unicode.SimpleFold(f) {
				out = append(out, f, f)
			}
		}
	}
	return mergeRuneRanges(out)
}

// mergeRuneRanges sorts lo, hi rune pairs and merges the ones that overlap or
// touch, which is the form a parsed character class holds.
func mergeRuneRanges(ranges []rune) []rune {
	pairs := make([][2]rune, 0, len(ranges)/2)
	for i := 0; i+1 < len(ranges); i += 2 {
		pairs = append(pairs, [2]rune{ranges[i], ranges[i+1]})
	}
	slices.SortFunc(pairs, func(a, b [2]rune) int { return cmp.Compare(a[0], b[0]) })
	merged := make([]rune, 0, len(ranges))
	for _, p := range pairs {
		if n := len(merged); n > 0 && p[0] <= merged[n-1]+1 {
			merged[n-1] = max(merged[n-1], p[1])
			continue
		}
		merged = append(merged, p[0], p[1])
	}
	return merged
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
