package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The CLI unsafe-change list mirrors the PR comment rendering: one line per
// single-finding table, nested bullets when the engine joined several
// violations into one reason, and the change type as the fallback line when
// no reason parses.
func TestWriteUnsafeChangesWarning_SplitsJoinedReasons(t *testing.T) {
	out := captureStdout(t, func() {
		WriteUnsafeChangesWarning([]UnsafeChange{
			{
				Table:  "orders",
				Reason: `[ERROR] unsafe: DROP COLUMN removes data; [ERROR] has_timestamp: Column "created_at" uses TIMESTAMP which overflows on 2038-01-19.`,
			},
			{Table: "users", Reason: "DROP TABLE removes all data"},
			{Table: "audit_log", Reason: "", ChangeType: "drop"},
		})
	})

	assert.Contains(t, out, "  • orders:\n")
	assert.Contains(t, out, "      - DROP COLUMN removes data\n")
	assert.Contains(t, out, "      - Column \"created_at\" uses TIMESTAMP which overflows on 2038-01-19.\n")
	assert.Contains(t, out, "  • users: DROP TABLE removes all data\n")
	assert.Contains(t, out, "  • audit_log: drop\n")
	assert.NotContains(t, out, "data; ")
}
