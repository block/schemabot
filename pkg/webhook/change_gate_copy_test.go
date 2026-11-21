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
	const frozen = "production is frozen by freeze frz-1 until someone lifts it. Reason: SEV-1234, page `@payments-oncall`."
	const freezeLine = "This change did not start: " + frozen

	t.Run("a command that takes --override is told how to ask", func(t *testing.T) {
		for command, outcome := range map[string]string{action.Apply: "This change", action.Start: "This change", action.Cutover: "This cutover"} {
			assert.Equal(t, outcome+" did not start: "+frozen+" To ask a freeze approver to let it through, run the same command again with `--override` followed by your reason in quotes.",
				changeGateRefusalDetail(blocked(), command, ""), command)
		}
	})

	// A refused apply-confirm has released its pending confirmation, so
	// running apply-confirm again would find nothing to confirm.
	t.Run("apply-confirm is told to run apply first", func(t *testing.T) {
		assert.Equal(t, freezeLine+" The pending confirmation was released. To ask a freeze approver to let it through, run `apply` again, then run `apply-confirm` with `--override` followed by your reason in quotes.",
			changeGateRefusalDetail(blocked(), action.ApplyConfirm, ""))

		refused := blocked()
		refused.OverrideRequest = &storage.FreezeOverrideRequest{
			Identifier: "ovr-0123456789abcdef", RequestedBy: "github:carol", PRAuthor: "github:alice", Reason: "hotfix",
		}
		assert.True(t, strings.HasSuffix(changeGateRefusalDetail(refused, action.ApplyConfirm, ""), " Then @carol runs `apply` again, then `apply-confirm`."))

		refused = blocked()
		refused.OverrideUnrecorded = true
		assert.Contains(t, changeGateRefusalDetail(refused, action.ApplyConfirm, ""), "Run `apply` again, then `apply-confirm` with `--override`; if this persists")
	})

	t.Run("a reason that ends a sentence gets no extra period", func(t *testing.T) {
		for reason, shown := range map[string]string{
			"hotfix!":                "hotfix!",
			"why?":                   "why?",
			strings.Repeat("a", 400): strings.Repeat("a", maxShownOverrideReason-1) + "…",
		} {
			refused := blocked()
			refused.OverrideRequest = &storage.FreezeOverrideRequest{
				Identifier: "ovr-0123456789abcdef", RequestedBy: "github:carol", PRAuthor: "github:alice", Reason: reason,
			}
			assert.Contains(t, changeGateRefusalDetail(refused, action.Apply, ""), " Reason the approver sees: "+shown+" A freeze approver", reason)
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
	// instructions survive the comment's own sanitizing, which escapes text and
	// redacts anything that looks like an endpoint ("a:12" grows to
	// "[endpoint redacted]").
	t.Run("long reasons and many freezes keep the instructions", func(t *testing.T) {
		for name, unit := range map[string]string{"escaped": "@x&", "redacted": "a:12 "} {
			refused := &api.ChangeGateRefusedError{Environment: "production"}
			for i := range 6 {
				refused.Blockers = append(refused.Blockers, apitypes.ChangeGateBlocker{Gate: "freeze", ID: fmt.Sprintf("frz-%d", i), Reason: strings.Repeat(unit, 400)})
			}
			refused.OverrideRequest = &storage.FreezeOverrideRequest{
				Identifier: "ovr-0123456789abcdef", RequestedBy: "github:" + strings.Repeat("c", 39), PRAuthor: "github:" + strings.Repeat("a", 39),
				Reason: strings.Repeat(unit, 170),
			}
			refused.OverrideReasonKept = true
			for command, ending := range map[string]string{
				action.Apply:        " runs this command again.",
				action.ApplyConfirm: " runs `apply` again, then `apply-confirm`.",
			} {
				detail := changeGateRefusalDetail(refused, command, strings.Repeat("t", 63))
				assert.LessOrEqual(t, utf8.RuneCountInString(detail), templates.MaxChangeGateRefusalLen, name, command)
				assert.Contains(t, detail, " 3 more freezes also block it.", name, command)
				assert.NotContains(t, detail, "frz-3", name, command)
				assert.True(t, strings.HasSuffix(detail, ending), detail)

				rendered := templates.RenderGenericError(templates.SchemaErrorData{
					CommandName: command, Environment: "production", ErrorDetail: detail, DetailLimit: templates.MaxChangeGateRefusalLen,
				})
				assert.Contains(t, rendered, "Then @"+strings.Repeat("c", 39)+ending, name, command)
			}
		}
	})
}
