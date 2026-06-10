package pendingdrops

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableName(t *testing.T) {
	now := time.Date(2026, 6, 10, 14, 30, 22, 123*int(time.Millisecond), time.UTC)

	name := TableName("testdb", "users", now)
	assert.Equal(t, "20260610143022123_users", name)

	parsed, ok := ParseTimestamp(name)
	require.True(t, ok)
	assert.Equal(t, now, parsed)
}

func TestTableNameUsesUTC(t *testing.T) {
	loc := time.FixedZone("UTC+5", 5*3600)
	local := time.Date(2026, 6, 10, 19, 30, 22, 0, loc) // 14:30:22 UTC

	name := TableName("testdb", "users", local)
	assert.Equal(t, "20260610143022000_users", name)

	parsed, ok := ParseTimestamp(name)
	require.True(t, ok)
	assert.True(t, parsed.Equal(local), "parsed instant %v should equal original %v", parsed, local)
}

func TestTableNameCapsAtMySQLLimit(t *testing.T) {
	now := time.Date(2026, 6, 10, 14, 30, 22, 0, time.UTC)
	longTable := strings.Repeat("a", 80)

	name := TableName("testdb", longTable, now)
	assert.Len(t, name, 64)
	assert.True(t, strings.HasPrefix(name, "20260610143022000_"))

	parsed, ok := ParseTimestamp(name)
	require.True(t, ok)
	assert.Equal(t, now, parsed)
}

func TestParseTimestampRejectsInvalidNames(t *testing.T) {
	invalid := []string{
		"",
		"users",
		"20260610143022123users",     // missing underscore separator
		"2026061014302212x_users",    // non-numeric milliseconds
		"20261310143022123_users",    // month 13
		"abcdefghijklmn123_users",    // non-numeric date
		"_users_old_20260610_143022", // Spirit old-table naming, not quarantine naming
		"20260610143022123_",         // valid prefix, empty table name is still parseable
	}
	for _, name := range invalid[:len(invalid)-1] {
		_, ok := ParseTimestamp(name)
		assert.False(t, ok, "expected %q to be rejected", name)
	}

	// A bare prefix is parseable: age is known even if the original name was truncated away.
	_, ok := ParseTimestamp("20260610143022123_")
	assert.True(t, ok)
}

func TestTargetLockNameIncludesTargetIdentity(t *testing.T) {
	base := targetLockName(Target{Database: "orders", Environment: "staging"})
	same := targetLockName(Target{Database: "orders", Environment: "staging"})
	differentDatabase := targetLockName(Target{Database: "customers", Environment: "staging"})
	differentEnvironment := targetLockName(Target{Database: "orders", Environment: "production"})

	assert.Equal(t, same, base)
	assert.NotEqual(t, differentDatabase, base)
	assert.NotEqual(t, differentEnvironment, base)
	assert.LessOrEqual(t, len(base), 64)
	assert.True(t, strings.HasPrefix(base, lockNamePrefix))
}
