package templates

import (
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/stretchr/testify/assert"
)

// An ignore_tables entry that withheld nothing is the case an operator most
// needs told: the table they wrote down is still visible to the plan, which is
// free to propose dropping it.
func TestWriteUnmatchedIgnoreTables(t *testing.T) {
	assert.Empty(t, captureStdout(t, func() {
		WriteUnmatchedIgnoreTables(nil)
	}), "a config whose every entry withheld a table earns no warning")

	output := captureStdout(t, func() {
		WriteUnmatchedIgnoreTables([]string{"typo", "Flyway_Schema_History"})
	})
	assert.Contains(t, output, `ignore_tables entry "typo" matched no live table and withheld nothing`)
	assert.Contains(t, output, `ignore_tables entry "Flyway_Schema_History" matched no live table and withheld nothing`)
}

// The tables an entry did withhold are disclosed on the plan itself, naming
// the config key so a reader knows which diff records the decision.
func TestWriteExemptTablesNamesTheConfigKey(t *testing.T) {
	output := captureStdout(t, func() {
		WriteExemptTables([]*apitypes.ExemptTablesResponse{{
			Namespace: "app",
			Tables:    []string{"flyway_schema_history", "legacy_audit_log"},
			Reason:    apitypes.ExemptReasonIgnoreTables,
		}})
	})
	assert.Contains(t, output, "Tables in namespace app exempt from the undeclared-table verdict (ignore_tables): flyway_schema_history, legacy_audit_log")
}
