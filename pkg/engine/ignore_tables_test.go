package engine

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mustIgnoredTables indexes entries the test knows are valid.
func mustIgnoredTables(t *testing.T, entries ...string) IgnoredTables {
	t.Helper()
	ignored, err := NewIgnoredTables(entries)
	require.NoError(t, err)
	return ignored
}

func TestIgnoredTablesWithholds(t *testing.T) {
	assert.True(t, mustIgnoredTables(t).Empty())
	assert.True(t, mustIgnoredTables(t, []string{}...).Empty())
	assert.True(t, IgnoredTables{}.Empty())
	assert.False(t, IgnoredTables{}.Withholds("users"))

	ignored := mustIgnoredTables(t, "flyway_schema_history", "legacy_audit_log")
	assert.False(t, ignored.Empty())
	assert.True(t, ignored.Withholds("flyway_schema_history"))
	assert.True(t, ignored.Withholds("legacy_audit_log"))
	assert.False(t, ignored.Withholds("users"))

	// Matching is exact and case-sensitive: a near miss withholds nothing, so
	// the table stays visible to the plan rather than being silently dropped
	// from the desired-vs-live comparison.
	assert.False(t, ignored.Withholds("Flyway_Schema_History"))
	assert.False(t, ignored.Withholds("flyway_schema_history_2"))
	assert.False(t, ignored.Withholds("app.flyway_schema_history"))
}

// A table the config withholds from the planner that a schema file also
// declares is the one shape in which ignoring could invert into creating or
// managing the table, so the plan fails instead of resolving the contradiction.
func TestIgnoredTablesRefuseDeclared(t *testing.T) {
	ignored := mustIgnoredTables(t, "flyway_schema_history", "legacy_audit_log")

	assert.NoError(t, mustIgnoredTables(t).RefuseDeclared("app", []string{"flyway_schema_history"}))
	assert.NoError(t, ignored.RefuseDeclared("app", nil))
	assert.NoError(t, ignored.RefuseDeclared("app", []string{"users", "orders"}))

	err := ignored.RefuseDeclared("app", []string{"users", "flyway_schema_history"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"flyway_schema_history"`)
	assert.Contains(t, err.Error(), `namespace "app"`)
	assert.Contains(t, err.Error(), "Remove the entry or the schema file")
	assert.NotContains(t, err.Error(), "users")

	// Every colliding table is named, sorted and deduplicated, so one plan
	// failure tells an operator the whole set to reconcile.
	err = ignored.RefuseDeclared("app", []string{"legacy_audit_log", "flyway_schema_history", "legacy_audit_log"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"flyway_schema_history", "legacy_audit_log"`)
}

// Whether two spellings of a name are one table is the target's answer: where
// a database folds identifiers to lower case, a file declaring one spelling
// and an entry naming another are the same table, and honoring both would
// leave the plan proposing to create a table that already exists. The
// contradiction is refused whatever the case, and the error names both
// spellings so an operator can find the entry they wrote.
func TestIgnoredTablesRefuseDeclaredIgnoresCase(t *testing.T) {
	ignored := mustIgnoredTables(t, "flyway_schema_history")

	err := ignored.RefuseDeclared("app", []string{"Flyway_Schema_History"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"flyway_schema_history" (the file spells it "Flyway_Schema_History")`)
	assert.Contains(t, err.Error(), "Remove the entry or the schema file")

	// An exact collision reads as one name, not as an entry respelled as itself.
	err = ignored.RefuseDeclared("app", []string{"flyway_schema_history"})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "the file spells it")

	// Ignoring case in the refusal does not widen what an entry withholds: a
	// differently-cased live table stays visible to the plan, and the entry
	// that matched nothing is reported to the operator instead.
	assert.False(t, ignored.Withholds("Flyway_Schema_History"))

	// A name that merely shares a prefix or suffix is still a different table.
	assert.NoError(t, ignored.RefuseDeclared("app", []string{"flyway_schema_history_archive", "old_flyway_schema_history"}))
}

// A config can spell one folded name several ways, and on a target that folds
// identifiers all of them name the declared table. Resolving the contradiction
// then means removing every one of them, so the error names every one of them
// rather than sending an operator to delete a single entry and re-plan into the
// same refusal.
func TestIgnoredTablesRefuseDeclaredNamesEverySpelling(t *testing.T) {
	ignored := mustIgnoredTables(t, "Orders", "orders", "ORDERS")

	err := ignored.RefuseDeclared("app", []string{"Orders"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"ORDERS" (the file spells it "Orders")`)
	assert.Contains(t, err.Error(), `"Orders"`)
	assert.Contains(t, err.Error(), `"orders" (the file spells it "Orders")`)

	// Only the entry spelled as the file declares it reads as one name; the
	// other two are reported against the spelling that collided with them.
	assert.Equal(t, 2, strings.Count(err.Error(), "the file spells it"))

	// Each of the three still withholds only the table it names.
	assert.True(t, ignored.Withholds("orders"))
	assert.True(t, ignored.Withholds("Orders"))
	assert.False(t, ignored.Withholds("oRdErS"))
}

func TestIgnoredTablesExemption(t *testing.T) {
	ignored := mustIgnoredTables(t, "flyway_schema_history", "legacy_audit_log")

	assert.Nil(t, ignored.Exemption("app", nil))
	assert.Nil(t, ignored.Exemption("app", []string{}))

	exemption := ignored.Exemption("app", []string{"legacy_audit_log", "flyway_schema_history"})
	require.NotNil(t, exemption)
	assert.Equal(t, "app", exemption.Namespace)
	assert.Equal(t, []string{"flyway_schema_history", "legacy_audit_log"}, exemption.Tables)
	assert.Equal(t, ExemptReasonIgnoreTables, exemption.Reason)
	assert.Equal(t, "ignore_tables", exemption.Reason, "the disclosure names the config key a reviewer reads the decision in")

	// The caller's slice is not reordered by building the disclosure.
	withheld := []string{"legacy_audit_log", "flyway_schema_history"}
	ignored.Exemption("app", withheld)
	assert.Equal(t, []string{"legacy_audit_log", "flyway_schema_history"}, withheld)
}

// An application that creates one table per configured trigger produces an
// unbounded family of names, and a pattern entry withholds the whole family,
// including members created after the config was written. Plain entries keep
// meaning exactly the one table they name.
func TestIgnoredTablesWithholdsPatternFamily(t *testing.T) {
	ignored := mustIgnoredTables(t, `/^relay_\d+_feed$/`, "legacy_audit_log")
	assert.False(t, ignored.Empty())
	assert.True(t, ignored.HasPatterns())

	assert.True(t, ignored.Withholds("relay_1_feed"))
	assert.True(t, ignored.Withholds("relay_20481_feed"))
	assert.True(t, ignored.Withholds("legacy_audit_log"))

	assert.False(t, ignored.Withholds("relay__feed"), "the pattern needs at least one digit")
	assert.False(t, ignored.Withholds("relay_x_feed"))
	assert.False(t, ignored.Withholds("orders"))

	// A plain entry is never read as a pattern, even when it holds characters
	// a regular expression would treat as operators.
	literal := mustIgnoredTables(t, "relay.feed")
	assert.False(t, literal.HasPatterns())
	assert.True(t, literal.Withholds("relay.feed"))
	assert.False(t, literal.Withholds("relayXfeed"))
}

// A pattern matches a table name only in full, whether or not it is written
// with anchors, so an entry meant for one family cannot reach a table that
// merely contains a matching run of characters.
func TestIgnoredTablesPatternMatchesWholeName(t *testing.T) {
	unanchored := mustIgnoredTables(t, `/relay_\d+_feed/`)
	assert.True(t, unanchored.Withholds("relay_7_feed"))
	assert.False(t, unanchored.Withholds("orders_relay_7_feed"), "no implicit leading wildcard")
	assert.False(t, unanchored.Withholds("relay_7_feed_backup"), "no implicit trailing wildcard")

	// Alternation stays inside the anchors: each branch must match the whole
	// name, not just its start or its end.
	alternation := mustIgnoredTables(t, `/relay_\d+_feed|relay_\d+_cursor/`)
	assert.True(t, alternation.Withholds("relay_3_feed"))
	assert.True(t, alternation.Withholds("relay_3_cursor"))
	assert.False(t, alternation.Withholds("relay_3_feed_old"))
	assert.False(t, alternation.Withholds("old_relay_3_cursor"))

	// An expression that only parses once wrapped would close the anchoring
	// group early, so it is refused rather than compiled into a pattern that
	// matches any name starting with "a" or ending with "b".
	_, err := NewIgnoredTables([]string{`/a)|(b/`})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `ignore_tables entry "/a)|(b/" is not a valid regular expression`)

	// A pattern spelling out its own anchors means the same thing.
	anchored := mustIgnoredTables(t, `/^relay_\d+_feed$/`)
	assert.True(t, anchored.Withholds("relay_7_feed"))
	assert.False(t, anchored.Withholds("orders_relay_7_feed"))
}

// Withholding by pattern is case-sensitive, the same as a plain entry, unless
// the expression itself asks to ignore case.
func TestIgnoredTablesPatternCase(t *testing.T) {
	ignored := mustIgnoredTables(t, `/relay_\d+_feed/`)
	assert.True(t, ignored.Withholds("relay_1_feed"))
	assert.False(t, ignored.Withholds("Relay_1_Feed"))

	folded := mustIgnoredTables(t, `/(?i)relay_\d+_feed/`)
	assert.True(t, folded.Withholds("Relay_1_Feed"))
	assert.False(t, folded.Withholds("Relay_1_Feed_old"), "a case flag does not widen the anchoring")
}

// A malformed pattern fails the plan or apply that asked for it. Skipping it
// would withhold nothing, and the tables it was written to protect would come
// back as DROP TABLE proposals.
func TestNewIgnoredTablesRefusesInvalidPattern(t *testing.T) {
	_, err := NewIgnoredTables([]string{"legacy_audit_log", `/relay_(\d+_feed/`})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `ignore_tables entry "/relay_(\d+_feed/" is not a valid regular expression`)
	assert.Contains(t, err.Error(), "missing closing )")

	_, err = NewIgnoredTables([]string{"//"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `ignore_tables entry "//" is an empty pattern`)
}

// A pattern written for a family of undeclared tables that also reaches a
// declared table is the same contradiction as a plain entry naming one, and is
// refused the same way, ignoring case. The error names the pattern with every
// declared table it matches, since narrowing the pattern is the remedy.
func TestIgnoredTablesRefuseDeclaredPattern(t *testing.T) {
	ignored := mustIgnoredTables(t, `/relay_\d+_feed/`)

	assert.NoError(t, ignored.RefuseDeclared("app", []string{"orders", "relay_feed_settings"}))

	err := ignored.RefuseDeclared("app", []string{"orders", "relay_1_feed"})
	require.Error(t, err)
	assert.Equal(t,
		`ignore_tables entry "/relay_\d+_feed/" matches "relay_1_feed", which a schema file in namespace "app" declares. Narrow the pattern so it no longer matches it, or remove the schema file`,
		err.Error())

	err = ignored.RefuseDeclared("app", []string{"relay_2_feed", "Relay_1_Feed"})
	require.Error(t, err)
	assert.Equal(t,
		`ignore_tables entry "/relay_\d+_feed/" matches "Relay_1_Feed", "relay_2_feed", which schema files in namespace "app" declare. Narrow the pattern so it no longer matches them, or remove the schema files`,
		err.Error(), "the refusal ignores case, as it does for plain entries")
	assert.False(t, ignored.Withholds("Relay_1_Feed"), "ignoring case in the refusal does not widen what the pattern withholds")

	// A flag inside the expression cannot switch the refusal's case folding off.
	flagged := mustIgnoredTables(t, `/(?-i)relay_\d+_feed/`)
	err = flagged.RefuseDeclared("app", []string{"Relay_1_Feed"})
	require.Error(t, err)
	assert.Equal(t,
		`ignore_tables entry "/(?-i)relay_\d+_feed/" matches "Relay_1_Feed", which a schema file in namespace "app" declares. Narrow the pattern so it no longer matches it, or remove the schema file`,
		err.Error())
	assert.False(t, flagged.Withholds("Relay_1_Feed"))

	// A pattern colliding alongside a plain entry is listed with it.
	mixed := mustIgnoredTables(t, "legacy_audit_log", `/relay_\d+_feed/`)
	err = mixed.RefuseDeclared("app", []string{"legacy_audit_log", "relay_1_feed"})
	require.Error(t, err)
	assert.Equal(t,
		`ignore_tables entries "/relay_\d+_feed/" (matching "relay_1_feed"), "legacy_audit_log" are also declared by schema files in namespace "app". Remove the entries or the schema files, narrowing a pattern instead where it still has tables to withhold`,
		err.Error())
}

func TestIgnoredTablesUnmatched(t *testing.T) {
	assert.Nil(t, mustIgnoredTables(t).Unmatched(nil))
	assert.Nil(t, mustIgnoredTables(t, "flyway_schema_history").Unmatched([]string{"flyway_schema_history"}))

	// A configured entry the plan never withheld is reported; matching is
	// exact and case-sensitive, so a case mismatch withholds nothing.
	assert.Equal(t, []string{"typo"},
		mustIgnoredTables(t, "flyway_schema_history", "typo").Unmatched([]string{"flyway_schema_history"}))
	assert.Equal(t, []string{"Flyway_Schema_History"},
		mustIgnoredTables(t, "Flyway_Schema_History").Unmatched([]string{"flyway_schema_history"}))

	// withheld is the union across namespaces, so an entry that matched in one
	// namespace and not another still withheld a table and is not reported.
	assert.Nil(t, mustIgnoredTables(t, "audit_log").Unmatched([]string{"audit_log", "flyway_schema_history"}))

	// A pattern is matched when it withheld any table, and reported as written
	// when it withheld none. Entries come back in the order the config lists
	// them.
	ignored := mustIgnoredTables(t, `/relay_\d+_feed/`, "typo", `/relay_\d+_cursor/`)
	assert.Equal(t, []string{"typo", `/relay_\d+_cursor/`}, ignored.Unmatched([]string{"relay_4_feed"}))
	assert.Equal(t, []string{"typo"}, ignored.Unmatched([]string{"relay_4_feed", "relay_4_cursor"}))
}

// A plain entry answers MayWithhold by its name. A pattern always answers yes,
// since which names it reaches is only known once the target's catalog is read.
func TestIgnoredTablesMayWithhold(t *testing.T) {
	isArchive := func(name string) bool { return strings.HasSuffix(name, "_archive_2024") }
	assert.False(t, mustIgnoredTables(t).MayWithhold(isArchive))
	assert.False(t, mustIgnoredTables(t, "flyway_schema_history").MayWithhold(isArchive))
	assert.True(t, mustIgnoredTables(t, "orders_archive_2024").MayWithhold(isArchive))
	assert.True(t, mustIgnoredTables(t, `/relay_\d+_feed/`).MayWithhold(isArchive))
}
