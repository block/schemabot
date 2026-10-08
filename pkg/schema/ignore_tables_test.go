package schema

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateIgnoreTables(t *testing.T) {
	assert.NoError(t, ValidateIgnoreTables(nil))
	assert.NoError(t, ValidateIgnoreTables([]string{"flyway_schema_history", "legacy_audit_log"}))

	// $ENV is not substituted in table entries, so an entry carrying it is a
	// literal name like any other rather than a rejected one.
	assert.NoError(t, ValidateIgnoreTables([]string{"events_$ENV"}))

	err := ValidateIgnoreTables([]string{""})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not be blank")

	err = ValidateIgnoreTables([]string{"  "})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not be blank")

	err = ValidateIgnoreTables([]string{"flyway_schema_history "})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "whitespace")

	err = ValidateIgnoreTables([]string{" flyway_schema_history"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "whitespace")

	err = ValidateIgnoreTables([]string{"app/flyway_schema_history"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a path")

	err = ValidateIgnoreTables([]string{`app\flyway_schema_history`})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a path")
}

// A pattern entry is validated where the config is read, so a malformed one
// stops at schemabot.yaml rather than at the first plan, and the error names
// the entry as written.
func TestValidateIgnoreTablesPatterns(t *testing.T) {
	assert.NoError(t, ValidateIgnoreTables([]string{`/^relay_\d+_feed$/`, "legacy_audit_log"}))
	assert.NoError(t, ValidateIgnoreTables([]string{`/relay_[0-9]+_(feed|cursor)/`}))

	err := ValidateIgnoreTables([]string{`/relay_(\d+_feed/`})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `ignore_tables entry "/relay_(\d+_feed/" is not a valid regular expression`)

	err = ValidateIgnoreTables([]string{"//"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `ignore_tables entry "//" is an empty pattern`)

	// Only an entry wrapped in slashes at both ends is a pattern; one with a
	// slash at one end is still a path.
	for _, entry := range []string{"/", "/relay_feed", "relay_feed/"} {
		err = ValidateIgnoreTables([]string{entry})
		require.Error(t, err, entry)
		assert.Contains(t, err.Error(), "must be a table name, not a path", entry)
		assert.Contains(t, err.Error(), "wrap a regular expression in slashes", entry)
	}

	// Whitespace padding is refused before the pattern is read.
	err = ValidateIgnoreTables([]string{` /relay_\d+_feed/`})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not have leading or trailing whitespace")
}

func TestIsIgnoreTablePattern(t *testing.T) {
	assert.True(t, IsIgnoreTablePattern(`/relay_\d+_feed/`))
	assert.True(t, IsIgnoreTablePattern("//"))
	assert.False(t, IsIgnoreTablePattern("/"))
	assert.False(t, IsIgnoreTablePattern("relay_feed"))
	assert.False(t, IsIgnoreTablePattern("/relay_feed"))
	assert.False(t, IsIgnoreTablePattern("relay_feed/"))
}

// The compiled pattern matches whole names only, and folds case only when
// asked to.
func TestCompileIgnoreTablePattern(t *testing.T) {
	re, err := CompileIgnoreTablePattern(`/relay_\d+_feed/`, false)
	require.NoError(t, err)
	assert.True(t, re.MatchString("relay_12_feed"))
	assert.False(t, re.MatchString("relay_12_feed_old"))
	assert.False(t, re.MatchString("old_relay_12_feed"))
	assert.False(t, re.MatchString("Relay_12_Feed"))
	assert.False(t, re.MatchString("relay_12_feed\n"), "a trailing newline is not the end of the name")

	folded, err := CompileIgnoreTablePattern(`/relay_\d+_feed/`, true)
	require.NoError(t, err)
	assert.True(t, folded.MatchString("Relay_12_Feed"))
	assert.False(t, folded.MatchString("Relay_12_Feed_old"))

	_, err = CompileIgnoreTablePattern("relay_feed", false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `ignore_tables entry "relay_feed" is not a pattern`)
}

// The refusal of a pattern that reaches a declared table ignores case on every
// pattern, so an expression cannot opt its own literals or classes back out of
// the folding with an embedded flag. Withholding keeps the flags as written.
func TestCompileIgnoreTablePatternFoldsCaseDespiteEmbeddedFlags(t *testing.T) {
	for _, tc := range []struct {
		entry   string
		matches []string
		misses  []string
	}{
		{entry: `/(?-i)relay_[0-9]+_feed/`, matches: []string{"relay_1_feed", "Relay_1_Feed", "RELAY_1_FEED"}, misses: []string{"Relay_1_Feed_old"}},
		{entry: `/(?i)relay_(?-i)feed/`, matches: []string{"RELAY_FEED", "relay_FEED"}},
		{entry: `/(?-i:Relay)_[a-c]+/`, matches: []string{"relay_ABC", "RELAY_abc"}, misses: []string{"relay_abd"}},
		// Written, the class withholds AB_feed, which is ab_feed on a target
		// that folds names, so the folded pattern matches both spellings.
		{entry: `/[^a-z]+_feed/`, matches: []string{"12_FEED", "AB_feed", "ab_feed"}, misses: []string{"ab_fee"}},
		{entry: `/(?-i)a|b/`, matches: []string{"A", "B"}, misses: []string{"AB", "aB"}},
		// The Kelvin sign folds to K but is not an ASCII word character, so a
		// word boundary cannot hold the way it does around K.
		{entry: `/\bK\b/`, matches: []string{"K", "k", "\u212a"}, misses: []string{"KK"}},
	} {
		folded, err := CompileIgnoreTablePattern(tc.entry, true)
		require.NoError(t, err, tc.entry)
		for _, name := range tc.matches {
			assert.True(t, folded.MatchString(name), "%s folded must match %q", tc.entry, name)
		}
		for _, name := range tc.misses {
			assert.False(t, folded.MatchString(name), "%s folded must not match %q", tc.entry, name)
		}
	}

	asWritten, err := CompileIgnoreTablePattern(`/(?-i)relay_[0-9]+_feed/`, false)
	require.NoError(t, err)
	assert.True(t, asWritten.MatchString("relay_1_feed"))
	assert.False(t, asWritten.MatchString("Relay_1_Feed"), "withholding keeps the expression's own case rules")
}

// caseEquivalents only scans the runes between the first and last entries of
// unicode.CaseRanges, which holds only while no rune outside them folds or
// changes case.
func TestCaseRangesBoundEveryFold(t *testing.T) {
	minCase := rune(unicode.CaseRanges[0].Lo)
	maxCase := rune(unicode.CaseRanges[len(unicode.CaseRanges)-1].Hi)
	for c := rune(0); c <= unicode.MaxRune; c++ {
		if c >= minCase && c <= maxCase {
			continue
		}
		require.Equal(t, c, unicode.SimpleFold(c), "rune %U folds but lies outside unicode.CaseRanges", c)
		require.Equal(t, c, unicode.ToLower(c), "rune %U lower-cases but lies outside unicode.CaseRanges", c)
	}
}

// CaseFoldKey equates every rune with the rune it folds to and with its lower
// case, the two rules a target may fold table names by.
func TestCaseFoldKeyCoversFoldingAndLowerCasing(t *testing.T) {
	for c := rune(0); c <= unicode.MaxRune; c++ {
		key := CaseFoldKey(string(c))
		require.Equal(t, key, CaseFoldKey(string(unicode.SimpleFold(c))), "rune %U and its fold", c)
		require.Equal(t, key, CaseFoldKey(string(unicode.ToLower(c))), "rune %U and its lower case", c)
	}
	assert.Equal(t, CaseFoldKey("orders"), CaseFoldKey("ORDERS"))
	assert.Equal(t, CaseFoldKey("i"), CaseFoldKey("\u0130"), "the dotted capital I lower-cases to i")
	assert.Equal(t, CaseFoldKey("s"), CaseFoldKey("\u017f"), "the long s folds to s")
	assert.NotEqual(t, CaseFoldKey("orders"), CaseFoldKey("order"))
}

// A folded pattern collides with exactly the spellings a plain entry collides
// with: a pattern naming one rune matches every rune CaseFoldKey equates with it.
func TestFoldedPatternMatchesEveryCaseEquivalent(t *testing.T) {
	eq := caseEquivalents()
	for _, r := range eq.runes {
		folded, err := CompileIgnoreTablePattern("/"+regexp.QuoteMeta(string(r))+"/", true)
		require.NoError(t, err)
		for _, m := range eq.members[eq.key[r]] {
			require.True(t, folded.MatchString(string(m)), "folded pattern for %U must match %U", r, m)
		}
	}
	for _, entry := range []string{`/i/`, `/[h-j]/`} {
		folded, err := CompileIgnoreTablePattern(entry, true)
		require.NoError(t, err)
		assert.True(t, folded.MatchString("\u0130"), "%s folded must match the dotted capital I", entry)
	}
}

// A pattern's length is capped, since compiling one costs time in proportion
// to its character classes. A pattern at the cap made entirely of classes that
// span the code space still compiles, folded and as written.
func TestCompileIgnoreTablePatternLengthCap(t *testing.T) {
	atCap := "/" + strings.Repeat(`[^_]`, (maxIgnoreTablePatternBytes-2)/4) + "/"
	require.LessOrEqual(t, len(atCap), maxIgnoreTablePatternBytes)
	for _, foldCase := range []bool{false, true} {
		re, err := CompileIgnoreTablePattern(atCap, foldCase)
		require.NoError(t, err)
		assert.True(t, re.MatchString(strings.Repeat("A", (maxIgnoreTablePatternBytes-2)/4)))
	}

	tooLong := "/" + strings.Repeat("a", maxIgnoreTablePatternBytes-1) + "/"
	_, err := CompileIgnoreTablePattern(tooLong, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is longer than 256 bytes: split it into several pattern entries")
	err = ValidateIgnoreTables([]string{tooLong})
	require.Error(t, err, "config validation refuses an over-long pattern")
	assert.Contains(t, err.Error(), "is longer than 256 bytes")
}

// Patterns are budgeted together as well as one by one, so a config cannot
// bound each entry and still list enough of them to stall a plan. A repeated
// entry counts once, and plain entries do not count at all.
func TestCheckIgnoreTablePatternBudget(t *testing.T) {
	pattern := func(i int) string {
		return fmt.Sprintf("/^relay_%03d_[0-9]+_feed_%s$/", i, strings.Repeat("x", 30))
	}
	var withinBudget []string
	for i := 0; len(strings.Join(withinBudget, ""))+len(pattern(i)) <= maxIgnoreTablePatternsTotalBytes; i++ {
		withinBudget = append(withinBudget, pattern(i))
	}
	require.NoError(t, CheckIgnoreTablePatternBudget(withinBudget))
	require.NoError(t, CheckIgnoreTablePatternBudget(append(slices.Clone(withinBudget), withinBudget[0], strings.Repeat("t", 2000))),
		"a repeated pattern and a plain entry do not count against the budget")

	overBudget := append(slices.Clone(withinBudget), pattern(len(withinBudget)))
	err := CheckIgnoreTablePatternBudget(overBudget)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "over the 1024-byte limit for all patterns together")
	err = ValidateIgnoreTables(overBudget)
	require.Error(t, err, "config validation refuses an over-budget config")
	assert.Contains(t, err.Error(), "over the 1024-byte limit")
}

func TestQuoteIgnoreTablesEntry(t *testing.T) {
	assert.Equal(t, `"legacy_audit_log"`, QuoteIgnoreTablesEntry("legacy_audit_log"))
	assert.Equal(t, `"/^relay_\d+_feed$/"`, QuoteIgnoreTablesEntry(`/^relay_\d+_feed$/`), "backslashes read as written")
	assert.Equal(t, `"say \"hi\""`, QuoteIgnoreTablesEntry(`say "hi"`))
	assert.Equal(t, `"tab\there"`, QuoteIgnoreTablesEntry("tab\there"))
}
