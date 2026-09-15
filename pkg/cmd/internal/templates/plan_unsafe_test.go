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

	assert.Contains(t, out, "  1. orders: DROP COLUMN removes data\n")
	assert.Contains(t, out, "  2. orders: Column \"created_at\" uses \"TIMESTAMP\" which overflows on 2038-01-19.\n")
	assert.Contains(t, out, "  3. users: DROP TABLE removes all data\n")
	assert.Contains(t, out, "  4. audit_log: drop\n")
	assert.NotContains(t, out, "data; ")
}

// Plan-time unsafe changes await consent, so the heading carries Attention;
// a blocked apply is a refusal, so its heading carries Refused and names the
// refusal itself, with the Escalation instruction for granting consent.
func TestUnsafeChangeHeadings_PlanWarnsApplyRefuses(t *testing.T) {
	changes := []UnsafeChange{{Table: "users", Reason: "DROP TABLE removes all data"}}

	planOut := captureStdout(t, func() { WriteUnsafeChangesWarning(changes) })
	assert.Contains(t, planOut, glyph.Attention+" Unsafe Changes Detected:")

	applyOut := captureStdout(t, func() { WriteUnsafeChangesBlocked(changes, "apply -s . -e staging --allow-unsafe") })
	assert.Contains(t, applyOut, glyph.Refused+" Apply blocked: 1 unsafe change(s) detected")
	assert.Contains(t, applyOut, glyph.Escalation+" To proceed with these destructive changes, re-run with --allow-unsafe:")
}

// A producer that separated its own findings is taken at its word: the list
// prints them as given and the heading counts them, so a reason written as one
// sentence stays one finding. Splitting it at its semicolon would number the
// remedy for a problem as a second problem to fix.
func TestWriteUnsafeChanges_DeclaredReasonsAreNotSplit(t *testing.T) {
	sentence := "definition is NOT NULL without a DEFAULT; add it manually or ship the column with a DEFAULT"
	declared := []UnsafeChange{{Table: "checks", Reason: sentence, Reasons: []string{sentence}}}

	out := captureStdout(t, func() { WriteUnsafeChangesBlocked(declared, "storage apply --allow-unsafe") })
	assert.Contains(t, out, "Apply blocked: 1 unsafe change(s) detected",
		"the heading counts what the list shows, so both read the findings the same way")
	assert.Contains(t, out, "  1. checks: "+sentence+"\n")
	assert.NotContains(t, out, "2. checks:")

	// The engine-reported form is still split, which is what a plan of a user's
	// database needs: its reason is a concatenation of findings.
	joined := []UnsafeChange{{Table: "checks", Reason: sentence}}
	out = captureStdout(t, func() { WriteUnsafeChangesBlocked(joined, "apply -s . -e staging --allow-unsafe") })
	assert.Contains(t, out, "Apply blocked: 2 unsafe change(s) detected")
	assert.Contains(t, out, "  2. checks: add it manually or ship the column with a DEFAULT\n")
}
