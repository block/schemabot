package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
}

// An engine that does not honor drop_tables refuses a plan carrying it, so the
// listed tables are never silently left in place behind a clean plan.
func TestRefuseDropTablesOnUnsupportedEngines(t *testing.T) {
	require.NoError(t, RefuseDropTables("PostgreSQL", nil))
	err := RefuseDropTables("PostgreSQL", []string{"_jobs_old"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "drop_tables is supported only on MySQL, and this is a PostgreSQL target")
}
