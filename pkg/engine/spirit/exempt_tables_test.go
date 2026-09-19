package spirit

import (
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// The loader drops an archive table before reading its definition. Moving that
// exclusion to this side would cost one SHOW CREATE TABLE per archive table on
// every plan, and would fail a plan outright on an archive-named object that
// cannot be read — neither of which a repository configuring nothing should
// pay for.
func TestLiveSchemaFilterOptions_ArchiveExclusionStaysWithTheLoader(t *testing.T) {
	opts := liveSchemaFilterOptions(engine.NewIgnoredTables(nil))
	assert.Contains(t, opts, table.WithoutArchiveTables)
	assert.Contains(t, opts, table.WithoutUnderscoreTables)
	assert.Contains(t, opts, table.WithStrippedAutoIncrement)
}

// A config that withholds an ordinary table says nothing about archive tables,
// so the loader keeps that exclusion and the plan reads no archive table.
func TestLiveSchemaFilterOptions_UnrelatedEntriesKeepTheLoaderExclusion(t *testing.T) {
	opts := liveSchemaFilterOptions(engine.NewIgnoredTables([]string{"flyway_schema_history"}))
	assert.Contains(t, opts, table.WithoutArchiveTables)
}

// An entry naming a table the archive convention also excludes is the one case
// where the two orderings disagree: the config has to resolve first for the
// table to be disclosed as withheld rather than reported as matching nothing,
// so the loader is not asked for that exclusion and this side applies it.
func TestLiveSchemaFilterOptions_AnArchiveNamedEntryTakesTheExclusionBack(t *testing.T) {
	require.True(t, table.IsArchiveTable("orders_archive_2024"), "fixture must be archive-shaped")

	opts := liveSchemaFilterOptions(engine.NewIgnoredTables([]string{"orders_archive_2024"}))
	assert.NotContains(t, opts, table.WithoutArchiveTables)
	assert.Contains(t, opts, table.WithoutUnderscoreTables)
}
