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
		{"orders_archive_2024_01_", false},
		{"orders_archive_202401", false},
		{"orders_archive_2024-01", false},
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

// TestIsArchiveTable_MatchesSpiritLoaderConvention checks a sample of names, so
// a change to either convention that keeps those verdicts would pass it. This
// test compares the two predicates on every generated name instead.
func TestIsArchiveTable_AgreesWithSpiritAcrossGeneratedNames(t *testing.T) {
	var archives, others int
	var disagreements []string
	for _, name := range generatedArchiveNameCandidates() {
		ours := engine.IsArchiveTable(name)
		if ours {
			archives++
		} else {
			others++
		}
		if ours != table.IsArchiveTable(name) && len(disagreements) < 10 {
			disagreements = append(disagreements, name)
		}
	}

	assert.Empty(t, disagreements, "engine.IsArchiveTable and Spirit's table.IsArchiveTable disagree on these names")
	assert.Positive(t, archives, "the generated names include archive names")
	assert.Positive(t, others, "the generated names include names outside the convention")
}

// generatedArchiveNameCandidates builds table names on both sides of the
// archive convention from the parts a change to it would most plausibly touch:
// the marker's spelling and letter case, how many digits each date part has,
// what separates the parts, and what follows them.
func generatedArchiveNameCandidates() []string {
	prefixes := []string{"", "orders", "orders_archive_2020"}
	markers := []string{"_archive_", "_ARCHIVE_", "archive_", "_archive", "_archives_"}
	years := []string{"", "24", "2024", "20245", "2O24", "２０２４"}
	tails := []string{"", "_", "_09", "\n"}

	// A month or a day is either absent or one separator followed by one part.
	optionalParts := []string{""}
	for _, separator := range []string{"_", "-", ""} {
		for _, part := range []string{"1", "01", "001"} {
			optionalParts = append(optionalParts, separator+part)
		}
	}

	var dates []string
	for _, year := range years {
		for _, month := range optionalParts {
			for _, day := range optionalParts {
				dates = append(dates, year+month+day)
			}
		}
	}

	var names []string
	for _, prefix := range prefixes {
		for _, marker := range markers {
			for _, date := range dates {
				for _, tail := range tails {
					names = append(names, prefix+marker+date+tail)
				}
			}
		}
	}
	return names
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
