package templates

import (
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
	assert.Contains(t, output, "Tables in namespace app that no schema file declares, left in place (ignore_tables): flyway_schema_history, legacy_audit_log")
}
