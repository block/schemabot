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

func TestDestinationsAreUniqueWithinOneMove(t *testing.T) {
	now := time.Date(2026, 6, 10, 14, 30, 22, 123*int(time.Millisecond), time.UTC)
	// 44 characters: Spirit's "_<t>_new" and "_<t>_old" differ only after the
	// 46th character, so plain truncation gives both the same name.
	longTable := strings.Repeat("t", 44)

	tests := []struct {
		name   string
		tables []TableMove
	}{
		{
			name: "shadow and cutover original of a long table name",
			tables: []TableMove{
				{SchemaName: "app", TableName: "_" + longTable + "_new"},
				{SchemaName: "app", TableName: "_" + longTable + "_old"},
			},
		},
		{
			name: "two tables sharing their first 46 characters",
			tables: []TableMove{
				{SchemaName: "app", TableName: strings.Repeat("a", 46) + "_archive"},
				{SchemaName: "app", TableName: strings.Repeat("a", 46) + "_backup"},
			},
		},
		{
			name: "same table name in two schemas",
			tables: []TableMove{
				{SchemaName: "s1", TableName: "users"},
				{SchemaName: "s2", TableName: "users"},
			},
		},
		{
			name: "same table name in two schemas differing only in case",
			tables: []TableMove{
				{SchemaName: "s1", TableName: "Users"},
				{SchemaName: "s2", TableName: "users"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			moved := Destinations(tt.tables, now)
			require.Len(t, moved, len(tt.tables))

			seen := make(map[string]string, len(moved))
			for _, table := range moved {
				key := strings.ToLower(table.QuarantineTable)
				prior, dup := seen[key]
				assert.False(t, dup, "%s.%s and %s share quarantine name %q",
					table.SchemaName, table.TableName, prior, table.QuarantineTable)
				seen[key] = table.SchemaName + "." + table.TableName

				assert.LessOrEqual(t, len(table.QuarantineTable), 64)
				assert.Equal(t, Database, table.QuarantineSchema)
				parsed, ok := ParseTimestamp(table.QuarantineTable)
				require.True(t, ok, "cleaner must recognize %q", table.QuarantineTable)
				assert.Equal(t, now, parsed)
			}

			assert.Equal(t, moved, Destinations(tt.tables, now), "destinations must be deterministic")
		})
	}
}

func TestDestinationsKeepPlainNameWhenNoCollision(t *testing.T) {
	now := time.Date(2026, 6, 10, 14, 30, 22, 123*int(time.Millisecond), time.UTC)
	moved := Destinations([]TableMove{
		{SchemaName: "app", TableName: "users"},
		{SchemaName: "app", TableName: "orders"},
		{SchemaName: "app", TableName: strings.Repeat("c", 46)},
	}, now)
	require.Len(t, moved, 3)
	assert.Equal(t, "20260610143022123_users", moved[0].QuarantineTable)
	assert.Equal(t, "20260610143022123_orders", moved[1].QuarantineTable)
	assert.Equal(t, "20260610143022123_"+strings.Repeat("c", 46), moved[2].QuarantineTable)
}

func TestTableNameDisambiguatesTruncatedNamesBySource(t *testing.T) {
	now := time.Date(2026, 6, 10, 14, 30, 22, 0, time.UTC)
	longTable := strings.Repeat("a", 80)

	first := TableName("s1", longTable, now)
	otherSchema := TableName("s2", longTable, now)
	otherTable := TableName("s1", longTable+"b", now)

	assert.NotEqual(t, first, otherSchema)
	assert.NotEqual(t, first, otherTable)
	assert.Equal(t, first, TableName("s1", longTable, now))
	for _, name := range []string{first, otherSchema, otherTable} {
		assert.Len(t, name, 64)
		assert.True(t, strings.HasPrefix(name, "20260610143022000_aaaa"), name)
	}
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
