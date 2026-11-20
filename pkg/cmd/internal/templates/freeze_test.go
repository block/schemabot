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
