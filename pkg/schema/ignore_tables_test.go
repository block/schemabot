package schema

import (
	"testing"

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

func TestQuoteIgnoreTablesEntry(t *testing.T) {
	assert.Equal(t, `"legacy_audit_log"`, QuoteIgnoreTablesEntry("legacy_audit_log"))
	assert.Equal(t, `"/^relay_\d+_feed$/"`, QuoteIgnoreTablesEntry(`/^relay_\d+_feed$/`), "backslashes read as written")
	assert.Equal(t, `"say \"hi\""`, QuoteIgnoreTablesEntry(`say "hi"`))
	assert.Equal(t, `"tab\there"`, QuoteIgnoreTablesEntry("tab\there"))
}
