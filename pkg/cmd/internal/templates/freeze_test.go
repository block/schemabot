package templates

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

// The refusal prints a freeze's end in UTC, and its reason and the override
// request ID come from other people, so they print on their own line with
// control runes removed.
func TestChangeGateRefusedText(t *testing.T) {
	zone := time.FixedZone("PST", -8*60*60)
	ends := time.Date(2026, 11, 22, 2, 0, 0, 0, zone)
	lines := ChangeGateRefusedText(ChangeGateRefusedData{
		Environment: "production",
		Operation:   "apply",
		Blockers: []apitypes.ChangeGateBlocker{
			{ID: "freeze-abc", Reason: "SEV-1234 " + lineAttack, EndsAt: &ends},
		},
		OverrideRequest: &apitypes.FreezeOverrideRequest{ID: "ovr-" + lineAttack},
		OverrideAsked:   true,
	})

	assert.Len(t, lines, 3)
	assert.Equal(t, "✗ production is frozen. This change did not start.", lines[0])
	assert.Equal(t, "   freeze-abc  SEV-1234 "+lineAttackCleaned+"  (until 2026-11-22T10:00:00Z)", lines[1])
	assert.True(t, strings.HasPrefix(lines[2], "Override requested: ovr-"+lineAttackCleaned+". "))
	assertNoTerminalControls(t, strings.Join(lines, "\n"))
}

// A pending request keeps its first reason, so the refusal shows the one the
// approver sees, even when the operator asked again with another.
func TestChangeGateRefusedTextShowsTheRecordedReason(t *testing.T) {
	lines := ChangeGateRefusedText(ChangeGateRefusedData{
		Environment:     "production",
		Operation:       "apply",
		Blockers:        []apitypes.ChangeGateBlocker{{ID: "freeze-abc", Reason: "SEV-1234"}},
		OverrideRequest: &apitypes.FreezeOverrideRequest{ID: "ovr-123", Reason: "first reason " + lineAttack},
		OverrideAsked:   true,
	})

	require.Len(t, lines, 4)
	assert.Equal(t, "Reason the approver sees: first reason "+lineAttackCleaned, lines[3])
	assertNoTerminalControls(t, strings.Join(lines, "\n"))
}

// rollback has no --override, so its refusal does not suggest one.
func TestChangeGateRefusedTextForARollbackSaysToWait(t *testing.T) {
	lines := ChangeGateRefusedText(ChangeGateRefusedData{
		Environment: "production",
		Operation:   "rollback",
		Blockers:    []apitypes.ChangeGateBlocker{{ID: "freeze-abc", Reason: "SEV-1234"}},
	})

	require.Len(t, lines, 3)
	assert.Equal(t, "Run the rollback again once these freezes end or are lifted.", lines[2])
	assert.NotContains(t, strings.Join(lines, "\n"), "--override")
}

// An override asked for and not recorded says why, in the server's words.
func TestChangeGateRefusedTextSaysWhyNoRequestWasRecorded(t *testing.T) {
	lines := ChangeGateRefusedText(ChangeGateRefusedData{
		Environment:   "production",
		Operation:     "start",
		Blockers:      []apitypes.ChangeGateBlocker{{ID: "freeze-abc", Reason: "SEV-1234"}},
		OverrideAsked: true,
		Message:       "production is frozen; asking for one needs a verified caller " + lineAttack,
	})

	require.Len(t, lines, 3)
	assert.Equal(t, "No override request was recorded. SchemaBot said: production is frozen; asking for one needs a verified caller "+lineAttackCleaned, lines[2])
	assertNoTerminalControls(t, strings.Join(lines, "\n"))
}

// Status says which freeze stopped an apply and how it resumes, and says
// nothing of a freeze for an apply an operator stopped. The freeze's text is
// printed safe for a terminal.
func TestStoppedByFreezeText(t *testing.T) {
	freeze := &apitypes.FreezeStopRef{ID: "freeze-1", Reason: "SEV-1234\x1b[31m"}
	text := StoppedByFreezeText(freeze, "apply-1", "staging")
	assert.Contains(t, text, "Stopped by freeze freeze-1 (SEV-1234")
	assert.NotContains(t, text, "\x1b", "the reason cannot write escape sequences")
	assert.Contains(t, text, "'schemabot start apply-1 -e staging' after the freeze ends or is lifted")
	assert.Contains(t, text, `add --override "<reason>" to ask a freeze approver to let it through`)

	assert.Empty(t, StoppedByFreezeText(nil, "apply-1", "staging"))
	assert.Equal(t, "Stopped by freeze freeze-1. Run 'schemabot start apply-1 -e staging' after the freeze ends or is lifted, or add --override \"<reason>\" to ask a freeze approver to let it through.",
		StoppedByFreezeText(&apitypes.FreezeStopRef{ID: "freeze-1"}, "apply-1", "staging"))
}
