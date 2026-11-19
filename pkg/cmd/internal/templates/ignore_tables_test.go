package templates

import (
	"fmt"
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/stretchr/testify/assert"
)

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
	assert.Contains(t, output, "Ignored tables in namespace app (ignore_tables): flyway_schema_history, legacy_audit_log")
}

// The CLI names the same first tables the PR comment does and counts the
// rest, so a target with thousands of archive tables prints one line.
func TestWriteExemptTablesCountsTablesPastTheListLimit(t *testing.T) {
	names := make([]string, 60)
	for i := range names {
		names[i] = fmt.Sprintf("orders_archive_2025_%02d", i)
	}
	output := captureStdout(t, func() {
		WriteExemptTables([]*apitypes.ExemptTablesResponse{{Namespace: "app", Tables: names, Reason: "archive naming"}})
	})
	assert.Contains(t, output, "orders_archive_2025_49, and 10 more\n")
	assert.NotContains(t, output, "orders_archive_2025_50")
}
