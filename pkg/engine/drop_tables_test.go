package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Dropping the cutover sentinel runs a deferred cutover nobody asked for, so an
// entry naming it is refused in any case, while a name that only resembles it
// is an ordinary entry.
func TestNewDroppedTablesRefusesTheCutoverSentinelInAnyCase(t *testing.T) {
	for _, entry := range []string{"_spirit_sentinel", "_SPIRIT_SENTINEL", "_Spirit_Sentinel"} {
		_, err := NewDroppedTables([]string{"_jobs_old", entry})
		require.Error(t, err, "entry %q", entry)
		assert.Contains(t, err.Error(), `drop_tables entry "`+entry+`" is the table that holds a deferred cutover`)
	}
	drops, err := NewDroppedTables([]string{"_spirit_sentinel_old", "_spirit_sentinels", "spirit_sentinel"})
	require.NoError(t, err)
	assert.Equal(t, []string{"_spirit_sentinel_old", "_spirit_sentinels", "spirit_sentinel"}, drops.Entries())
}

// drop_tables takes exact names only, so a pattern, a path or a padded entry
// fails the plan instead of dropping tables nobody listed by name.
func TestNewDroppedTablesRefusesAnythingButAnExactName(t *testing.T) {
	for entry, want := range map[string]string{
		`/^_.*_old$/`: "must be an exact table name",
		"db/_jobs":    "must be an exact table name",
		"\t_jobs_old": "must not have leading or trailing whitespace",
		"":            "must not be blank",
	} {
		_, err := NewDroppedTables([]string{entry})
		require.Error(t, err, "entry %q", entry)
		assert.Contains(t, err.Error(), want, "entry %q", entry)
	}

	drops, err := NewDroppedTables([]string{"_jobs_old", "_jobs_old", "orders_archive_2019"})
	require.NoError(t, err)
	assert.Equal(t, []string{"_jobs_old", "orders_archive_2019"}, drops.Entries())
	assert.True(t, drops.Names("_jobs_old"))
	assert.False(t, drops.Names("_JOBS_OLD"), "matching is exact and case-sensitive")
	assert.False(t, drops.Empty())
	assert.True(t, DroppedTables{}.Empty(), "the zero value names nothing")

	drops.Entries()[0] = "orders"
	assert.Equal(t, []string{"_jobs_old", "orders_archive_2019"}, drops.Entries(),
		"a caller that edits the returned entries does not change the list")
}

// An entry that names no live table drops nothing, so it is reported: a typo
// or a wrong case would otherwise read as a table the plan chose to leave.
func TestDroppedTablesUnmatchedReportsEntriesThatNameNoLiveTable(t *testing.T) {
	drops, err := NewDroppedTables([]string{"_jobs_old", "never_existed", "_Other_old"})
	require.NoError(t, err)

	assert.Equal(t, []string{"never_existed", "_Other_old"},
		drops.Unmatched([]string{"executions", "_jobs_old", "_other_old"}),
		"entries are reported in config order, and a live table in another case is not a match")
	assert.Empty(t, drops.Unmatched([]string{"_jobs_old", "never_existed", "_Other_old"}))
	assert.Empty(t, DroppedTables{}.Unmatched([]string{"executions"}))
}

// An entry that ignore_tables also withholds, or that a schema file declares,
// asks for two contradictory things, so each is refused naming the entry. A
// declaration is compared ignoring case, and the error gives the file's
// spelling when it differs from the entry's.
func TestDroppedTablesRefusesEntriesTheConfigContradicts(t *testing.T) {
	drops, err := NewDroppedTables([]string{"_jobs_old", "orders"})
	require.NoError(t, err)

	ignored, err := NewIgnoredTables([]string{`/^_jobs_.*$/`})
	require.NoError(t, err)
	err = drops.RefuseIgnored(ignored)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `drop_tables entry "_jobs_old" is also withheld by ignore_tables`)
	require.NoError(t, drops.RefuseIgnored(IgnoredTables{}))

	err = drops.RefuseDeclared("app", []string{"Orders"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `drop_tables entry "orders" (the file spells it "Orders") is also declared by a schema file in namespace "app"`)
	err = drops.RefuseDeclared("app", []string{"orders"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `drop_tables entry "orders" is also declared by a schema file in namespace "app"`)
	require.NoError(t, drops.RefuseDeclared("app", []string{"users"}))
	require.NoError(t, DroppedTables{}.RefuseDeclared("app", []string{"orders"}))
}

// An engine that does not honor drop_tables refuses a plan carrying it, so the
// listed tables are never silently left in place behind a clean plan.
func TestRefuseDropTablesOnUnsupportedEngines(t *testing.T) {
	require.NoError(t, RefuseDropTables("PostgreSQL", nil))
	err := RefuseDropTables("PostgreSQL", []string{"_jobs_old"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "drop_tables is supported only on MySQL, and this is a PostgreSQL target")
}
