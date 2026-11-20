package webhook

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

func TestChangeGateRefusalDetail(t *testing.T) {
	blocked := func() *api.ChangeGateRefusedError {
		return &api.ChangeGateRefusedError{
			Environment: "production",
			Blockers:    []apitypes.ChangeGateBlocker{{Gate: "freeze", ID: "frz-1", Reason: "SEV-1234, page @payments-oncall"}},
		}
	}
	const freezeLine = "blocked in production by freeze frz-1 (SEV-1234, page `@payments-oncall`, until lifted); try again after it ends or is lifted."

	t.Run("a command that takes --override is told how to ask", func(t *testing.T) {
		for _, command := range []string{action.Apply, action.ApplyConfirm, action.Start, action.Cutover} {
			assert.Equal(t, freezeLine+" To ask a freeze approver to let it through, run the same command again with `--override` followed by your reason in quotes.",
				changeGateRefusalDetail(blocked(), command, ""), command)
		}
	})

	t.Run("a command without --override only waits", func(t *testing.T) {
		for _, command := range []string{action.RollbackConfirm, action.Revert} {
			assert.Equal(t, freezeLine, changeGateRefusalDetail(blocked(), command, ""), command)
		}
	})

	t.Run("a recorded request names its reason, the approve command and who runs again", func(t *testing.T) {
		refused := blocked()
		refused.OverrideRequest = &storage.FreezeOverrideRequest{
			Identifier: "ovr-0123456789abcdef", RequestedBy: "github:carol", PRAuthor: "github:alice",
			Reason: "hotfix for @payments-oncall",
		}
		assert.Equal(t, freezeLine+
			" Override request `ovr-0123456789abcdef` was recorded. Reason the approver sees: hotfix for `@payments-oncall`."+
			" A freeze approver other than @carol and other than the PR author (@alice) must comment `schemabot freeze approve ovr-0123456789abcdef --tenant acme`."+
			" Then @carol runs this command again.",
			changeGateRefusalDetail(refused, action.Apply, "acme"))
	})

	t.Run("an author asking on their own pull request is named once", func(t *testing.T) {
		refused := blocked()
		refused.OverrideRequest = &storage.FreezeOverrideRequest{
			Identifier: "ovr-0123456789abcdef", RequestedBy: "github:alice", PRAuthor: "github:Alice", Reason: "hotfix.",
		}
		refused.OverrideReasonKept = true
		assert.Equal(t, freezeLine+
			" Override request `ovr-0123456789abcdef` was recorded. Reason the approver sees: hotfix."+
			" You asked for it earlier, so it keeps the reason you gave then."+
			" A freeze approver other than @alice, who wrote this pull request, must comment `schemabot freeze approve ovr-0123456789abcdef`."+
			" Then @alice runs this command again.",
			changeGateRefusalDetail(refused, action.Start, ""))
	})

	t.Run("an unrecorded request says to retry", func(t *testing.T) {
		refused := blocked()
		refused.OverrideUnrecorded = true
		assert.Contains(t, changeGateRefusalDetail(refused, action.Apply, ""), "No override request was recorded")
		assert.NotContains(t, changeGateRefusalDetail(refused, action.Apply, ""), "To ask a freeze approver")
	})

	// However long the reasons and however many freezes, the request and its
	// instructions fit inside the bound the refusal is posted under.
	t.Run("long reasons and many freezes keep the instructions", func(t *testing.T) {
		long := strings.Repeat("@x&", 400)
		refused := &api.ChangeGateRefusedError{Environment: "production"}
		for i := range 6 {
			refused.Blockers = append(refused.Blockers, apitypes.ChangeGateBlocker{Gate: "freeze", ID: fmt.Sprintf("frz-%d", i), Reason: long})
		}
		refused.OverrideRequest = &storage.FreezeOverrideRequest{
			Identifier: "ovr-0123456789abcdef", RequestedBy: "github:" + strings.Repeat("c", 39), PRAuthor: "github:" + strings.Repeat("a", 39),
			Reason: strings.Repeat("@y&", 170),
		}
		refused.OverrideReasonKept = true
		detail := "This change did not start: it is " + changeGateRefusalDetail(refused, action.Apply, strings.Repeat("t", 63))
		assert.LessOrEqual(t, utf8.RuneCountInString(detail), templates.MaxChangeGateRefusalLen)
		assert.Contains(t, detail, " 3 more freezes also block it.")
		assert.NotContains(t, detail, "frz-3")
		assert.True(t, strings.HasSuffix(detail, " runs this command again."), detail)
	})
}
