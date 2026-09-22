package engine

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIgnoredTablesWithholds(t *testing.T) {
	assert.True(t, NewIgnoredTables(nil).Empty())
	assert.True(t, NewIgnoredTables([]string{}).Empty())
	assert.True(t, IgnoredTables{}.Empty())
	assert.False(t, IgnoredTables{}.Withholds("users"))

	ignored := NewIgnoredTables([]string{"flyway_schema_history", "legacy_audit_log"})
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
	ignored := NewIgnoredTables([]string{"flyway_schema_history", "legacy_audit_log"})

	assert.NoError(t, NewIgnoredTables(nil).RefuseDeclared("app", []string{"flyway_schema_history"}))
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
	ignored := NewIgnoredTables([]string{"flyway_schema_history"})

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
	ignored := NewIgnoredTables([]string{"Orders", "orders", "ORDERS"})

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
	ignored := NewIgnoredTables([]string{"flyway_schema_history", "legacy_audit_log"})

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
