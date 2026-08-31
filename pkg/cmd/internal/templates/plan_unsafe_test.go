package templates

import (
	"testing"

	"github.com/block/schemabot/pkg/glyph"
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
				Reason: `[ERROR] unsafe: DROP COLUMN removes data; [ERROR] has_timestamp: Column "created_at" uses "TIMESTAMP" which overflows on 2038-01-19.`,
			},
			{Table: "users", Reason: "DROP TABLE removes all data"},
			{Table: "audit_log", Reason: "", ChangeType: "drop"},
		})
	})

	assert.Contains(t, out, "  • orders:\n")
	assert.Contains(t, out, "      - DROP COLUMN removes data\n")
	assert.Contains(t, out, "      - Column \"created_at\" uses \"TIMESTAMP\" which overflows on 2038-01-19.\n")
	assert.Contains(t, out, "  • users: DROP TABLE removes all data\n")
	assert.Contains(t, out, "  • audit_log: drop\n")
	assert.NotContains(t, out, "data; ")
}

// Plan-time unsafe changes await consent, so the heading carries Attention;
// a blocked apply is a refusal, so its heading carries Refused and names the
// refusal itself, with the Escalation instruction for granting consent.
func TestUnsafeChangeHeadings_PlanWarnsApplyRefuses(t *testing.T) {
	changes := []UnsafeChange{{Table: "users", Reason: "DROP TABLE removes all data"}}

	planOut := captureStdout(t, func() { WriteUnsafeChangesWarning(changes) })
	assert.Contains(t, planOut, glyph.Attention+" Unsafe Changes Detected:")

	applyOut := captureStdout(t, func() { WriteUnsafeChangesBlocked(changes, "testapp", "staging", ".") })
	assert.Contains(t, applyOut, glyph.Refused+" Apply blocked: 1 unsafe change(s) detected")
	assert.Contains(t, applyOut, glyph.Escalation+" To proceed with these destructive changes, re-run with --allow-unsafe:")
}
