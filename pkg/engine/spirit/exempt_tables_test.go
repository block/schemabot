package spirit

import (
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// mustIgnoredTables indexes ignore_tables entries the test knows are valid.
func mustIgnoredTables(t *testing.T, entries []string) engine.IgnoredTables {
	t.Helper()
	ignored, err := engine.NewIgnoredTables(entries)
	require.NoError(t, err)
	return ignored
}

// Every engine leaves the same undeclared archive tables in place. The MySQL
// engine relies on Spirit's loader for that exclusion and engines that do not
// load through Spirit use engine.IsArchiveTable, so the two predicates must
// agree on every name, including the ones just outside the convention.
func TestIsArchiveTable_MatchesSpiritLoaderConvention(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"orders_archive_2024", true},
		{"orders_archive_2024_01", true},
		{"orders_archive_2024_01_15", true},
		{"_archive_2024", true},
		{"orders_archive", false},
		{"orders_archive_24", false},
		{"orders_archive_2024_1", false},
		{"orders_archive_2024_01_15_09", false},
		{"orders_archive_2024x", false},
		{"orders_archived_2024", false},
		{"archive_2024", false},
		{"orders_ARCHIVE_2024", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, engine.IsArchiveTable(tc.name), "engine.IsArchiveTable")
			assert.Equal(t, tc.want, table.IsArchiveTable(tc.name), "Spirit table.IsArchiveTable")
		})
	}
}

// The loader drops an archive table before reading its definition. Moving that
// exclusion to this side would cost one SHOW CREATE TABLE per archive table on
// every plan, and would fail a plan outright on an archive-named object that
// cannot be read — neither of which a repository configuring nothing should
// pay for.
func TestLiveSchemaFilterOptions_ArchiveExclusionStaysWithTheLoader(t *testing.T) {
	opts := liveSchemaFilterOptions(mustIgnoredTables(t, nil))
	assert.Contains(t, opts, table.WithoutArchiveTables)
	assert.Contains(t, opts, table.WithoutUnderscoreTables)
	assert.Contains(t, opts, table.WithStrippedAutoIncrement)
}

// A config that withholds an ordinary table says nothing about archive tables,
// so the loader keeps that exclusion and the plan reads no archive table.
func TestLiveSchemaFilterOptions_UnrelatedEntriesKeepTheLoaderExclusion(t *testing.T) {
	opts := liveSchemaFilterOptions(mustIgnoredTables(t, []string{"flyway_schema_history"}))
	assert.Contains(t, opts, table.WithoutArchiveTables)
}

// An entry naming a table the archive convention also excludes is the one case
// where the two orderings disagree: the config has to resolve first for the
// table to be disclosed as withheld rather than reported as matching nothing,
// so the loader is not asked for that exclusion and this side applies it.
func TestLiveSchemaFilterOptions_AnArchiveNamedEntryTakesTheExclusionBack(t *testing.T) {
	require.True(t, table.IsArchiveTable("orders_archive_2024"), "fixture must be archive-shaped")

	opts := liveSchemaFilterOptions(mustIgnoredTables(t, []string{"orders_archive_2024"}))
	assert.NotContains(t, opts, table.WithoutArchiveTables)
	assert.Contains(t, opts, table.WithoutUnderscoreTables)
}
