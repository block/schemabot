package schema

import (
	"cmp"
	"fmt"
	"regexp"
	"regexp/syntax"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

// ignoreTablePatternDelimiter wraps a pattern entry. A table name cannot
// carry it, since a plain entry containing a path separator is refused, so an
// entry it wraps is unambiguously a pattern and every other entry keeps
// meaning the one table it names.
const ignoreTablePatternDelimiter = "/"

// maxIgnoreTablePatternBytes caps a pattern entry's length, slashes included.
// RE2 matches in linear time, but compiling an expression costs time in
// proportion to its character classes, and one written to ignore case visits
// every code point a class spans, so an uncapped entry could stall every plan
// that reads it. A table name is at most 64 characters, so a pattern that
// describes one never needs to come close to the cap.
const maxIgnoreTablePatternBytes = 256

// maxIgnoreTablePatternsTotalBytes caps the length of a config's distinct
// pattern entries taken together, so the compilation work a plan does before
// it reads the target is bounded however many entries the config lists.
const maxIgnoreTablePatternsTotalBytes = 1024

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
	if err := CheckIgnoreTablePatternBudget(tables); err != nil {
		return err
	}
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

// CheckIgnoreTablePatternBudget refuses a config whose distinct pattern
// entries, taken together, are longer than maxIgnoreTablePatternsTotalBytes.
// It runs before any pattern compiles, so an over-budget config costs nothing
// to refuse.
func CheckIgnoreTablePatternBudget(entries []string) error {
	seen := make(map[string]bool)
	total := 0
	for _, entry := range entries {
		if !IsIgnoreTablePattern(entry) || seen[entry] {
			continue
		}
		seen[entry] = true
		total += len(entry)
	}
	if total > maxIgnoreTablePatternsTotalBytes {
		return fmt.Errorf("ignore_tables pattern entries total %d bytes, over the %d-byte limit for all patterns together: remove or shorten pattern entries", total, maxIgnoreTablePatternsTotalBytes)
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
// backtracking, and an entry longer than maxIgnoreTablePatternBytes is refused,
// so no pattern can stall a plan.
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
	if len(entry) > maxIgnoreTablePatternBytes {
		return nil, fmt.Errorf("ignore_tables entry %s is longer than %d bytes: split it into several pattern entries", QuoteIgnoreTablesEntry(entry), maxIgnoreTablePatternBytes)
	}
	expr := entry[len(ignoreTablePatternDelimiter) : len(entry)-len(ignoreTablePatternDelimiter)]
	if expr == "" {
		return nil, fmt.Errorf("ignore_tables entry %s is an empty pattern: write a regular expression between the slashes", QuoteIgnoreTablesEntry(entry))
	}
	if _, err := regexp.Compile(expr); err != nil {
		return nil, fmt.Errorf("ignore_tables entry %s is not a valid regular expression: %w", QuoteIgnoreTablesEntry(entry), err)
	}
	// The anchors wrap the parsed expression's canonical form, not the text as
	// written: a quote opened by \Q runs to the end of the text, so wrapped
	// as written it would swallow the closing group and anchor.
	parsed, err := syntax.Parse(expr, syntax.Perl)
	if err != nil {
		return nil, fmt.Errorf("ignore_tables entry %s is not a valid regular expression: %w", QuoteIgnoreTablesEntry(entry), err)
	}
	if foldCase {
		foldRegexpCase(parsed)
	}
	anchored := `\A(?:` + parsed.String() + `)\z`
	re, err := regexp.Compile(anchored)
	if err != nil {
		return nil, fmt.Errorf("ignore_tables entry %s is not a valid regular expression: %w", QuoteIgnoreTablesEntry(entry), err)
	}
	return re, nil
}

// foldRegexpCase makes a parsed expression match every name that a spelling
// equivalent to it under CaseFoldKey would match as written, whatever flags the
// expression sets: those are the names a case-folding target could treat as a
// table the pattern withholds. Each literal rune and character class is widened
// to every rune case-equivalent to one it holds; a negated class is widened
// after negation, so `[^a-z]`, which as written matches A, also matches a.
//
// A word boundary is treated as always holding. Whether one holds depends on
// which spelling of the neighboring runes the name uses, since only ASCII runes
// count as word characters, so `\bK\b` written would match the Kelvin sign's
// ASCII spelling K but a folded `\b` would reject the Kelvin sign itself. Dropping
// the assertion can only widen the match, and the folded expression only decides
// what the declared-table refusal refuses, so it errs toward refusing.
func foldRegexpCase(re *syntax.Regexp) {
	switch re.Op {
	case syntax.OpLiteral:
		classes := make([]*syntax.Regexp, 0, len(re.Rune))
		for _, r := range re.Rune {
			classes = append(classes, &syntax.Regexp{Op: syntax.OpCharClass, Rune: equivalentRanges([]rune{r, r})})
		}
		if len(classes) == 1 {
			*re = *classes[0]
			return
		}
		*re = syntax.Regexp{Op: syntax.OpConcat, Sub: classes}
		return
	case syntax.OpCharClass:
		re.Rune = equivalentRanges(re.Rune)
	case syntax.OpWordBoundary, syntax.OpNoWordBoundary:
		re.Op = syntax.OpEmptyMatch
	}
	for _, sub := range re.Sub {
		foldRegexpCase(sub)
	}
}

// caseEquivalence partitions the runes that have another case into classes
// SchemaBot treats as one table-name character wherever a target may fold
// case. Two runes share a class when one folds to the other under Unicode
// simple case folding or when both lower-case to the same rune, and classes
// are closed under both. Neither relation alone covers the other: the dotted
// capital I lower-cases to i without folding to it, and the long s folds to s
// without lower-casing to it. Refusing a contradiction costs a plan, while
// missing one costs an apply, so the refusal takes the union.
type caseEquivalence struct {
	// key maps each rune in a class to the class's smallest rune.
	key map[rune]rune
	// members maps a class's smallest rune to the class, sorted.
	members map[rune][]rune
	// runes lists every rune in a class, sorted.
	runes []rune
}

// caseEquivalents builds the classes once per process. Only runes between the
// first and last entries of unicode.CaseRanges fold or change case, so the scan
// that builds them is bounded.
var caseEquivalents = sync.OnceValue(func() caseEquivalence {
	parent := map[rune]rune{}
	var find func(r rune) rune
	find = func(r rune) rune {
		p, ok := parent[r]
		if !ok || p == r {
			return r
		}
		root := find(p)
		parent[r] = root
		return root
	}
	union := func(a, b rune) {
		ra, rb := find(a), find(b)
		if ra == rb {
			return
		}
		parent[max(ra, rb)] = min(ra, rb)
		parent[min(ra, rb)] = min(ra, rb)
	}
	minCase := rune(unicode.CaseRanges[0].Lo)
	maxCase := rune(unicode.CaseRanges[len(unicode.CaseRanges)-1].Hi)
	for c := minCase; c <= maxCase; c++ {
		if f := unicode.SimpleFold(c); f != c {
			union(c, f)
		}
		if l := unicode.ToLower(c); l != c {
			union(c, l)
		}
	}
	eq := caseEquivalence{key: make(map[rune]rune, len(parent)), members: map[rune][]rune{}}
	for r := range parent {
		root := find(r)
		eq.key[r] = root
		eq.members[root] = append(eq.members[root], r)
		eq.runes = append(eq.runes, r)
	}
	for _, members := range eq.members {
		slices.Sort(members)
	}
	slices.Sort(eq.runes)
	return eq
})

// CaseFoldKey returns the form two table names share when SchemaBot treats
// them as one table on a target that folds case: each rune is replaced by the
// smallest rune case-equivalent to it. It is the one rule both the plain
// ignore_tables entries and the patterns refuse declared tables by, so the two
// kinds of entry can never disagree about which spellings collide.
func CaseFoldKey(name string) string {
	eq := caseEquivalents()
	return strings.Map(func(r rune) rune {
		if key, ok := eq.key[r]; ok {
			return key
		}
		return r
	}, name)
}

// equivalentRanges adds to a character class's rune ranges every rune
// case-equivalent to a rune in them. It visits only the runes that have
// another case inside each range, so a class costs at most one visit per such
// rune however wide it is.
func equivalentRanges(ranges []rune) []rune {
	eq := caseEquivalents()
	out := slices.Clone(ranges)
	for i := 0; i+1 < len(ranges); i += 2 {
		start, _ := slices.BinarySearch(eq.runes, ranges[i])
		for _, c := range eq.runes[start:] {
			if c > ranges[i+1] {
				break
			}
			for _, m := range eq.members[eq.key[c]] {
				out = append(out, m, m)
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
