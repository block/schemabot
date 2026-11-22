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

// The watch TUI's keys carry no override reason, so its refusal names the
// exact command that asks for one instead of saying to rerun with --override.
func TestChangeGateRefusedTextNamesTheOverrideCommand(t *testing.T) {
	lines := ChangeGateRefusedText(ChangeGateRefusedData{
		Environment:     "production",
		Operation:       "cutover",
		Blockers:        []apitypes.ChangeGateBlocker{{ID: "freeze-abc", Reason: "SEV-1234"}},
		OverrideCommand: "schemabot cutover apply-" + lineAttack + " -e production",
	})

	require.Len(t, lines, 4)
	assert.Equal(t, "✗ production is frozen. This cutover did not start.", lines[0])
	assert.Equal(t, "To ask for an override, run:", lines[2])
	assert.Equal(t, `  schemabot cutover apply-`+lineAttackCleaned+` -e production --override "<reason>"`, lines[3])
	assertNoTerminalControls(t, strings.Join(lines, "\n"))
}
