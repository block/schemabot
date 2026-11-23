package templates

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/webhook/action"
)

func refusalData(command string, freezes ...RefusingFreeze) ChangeGateRefusalData {
	if len(freezes) == 0 {
		freezes = []RefusingFreeze{{ID: "frz-1", Reason: "SEV-1234, page @payments-oncall"}}
	}
	return ChangeGateRefusalData{
		CommandName: command, Environment: "production", RequestedBy: "carol", Timestamp: "2026-01-15 14:30:00", Freezes: freezes,
		Approvers:     []string{"acme/freeze-approvers"},
		TakesOverride: command == action.Apply || command == action.ApplyConfirm || command == action.Start || command == action.Cutover,
	}
}

// A refusal names what is frozen, lists each freeze with when it ends and
// why, and says what the commenter can do next.
func TestRenderChangeGateRefusal(t *testing.T) {
	assert.Equal(t, "## ❄️ Apply Blocked by a Freeze\n\n"+
		"**Environment**: `production`\n\n"+
		"*Requested by @carol at 2026-01-15 14:30:00 UTC*\n\n"+
		"`production` is frozen, so this command did not run. Nothing changed.\n\n"+
		"| Freeze | Holds | Reason |\n| :-- | :-- | :-- |\n"+
		"| `frz-1` | until lifted | SEV-1234, page `@payments-oncall` |\n\n"+
		"### What you can do\n\n"+
		"- **Wait for the freeze to end**, then run the same command again.\n"+
		"- **Ask for an override** if this change can't wait. Run the same command with `--override \"<why it can't wait>\"`. "+
		"A freeze approver other than you and the PR author must approve it: a member of `acme/freeze-approvers`.\n"+
		supportChannelOfferMarker+"\n",
		RenderChangeGateRefusal(refusalData(action.Apply)))
}

func TestRenderChangeGateRefusalNextSteps(t *testing.T) {
	t.Run("start and cutover can ask for an override", func(t *testing.T) {
		for _, command := range []string{action.Start, action.Cutover} {
			assert.Contains(t, RenderChangeGateRefusal(refusalData(command)), "Run the same command with `--override", command)
		}
	})

	t.Run("a command without --override only waits", func(t *testing.T) {
		for _, command := range []string{action.RollbackConfirm, action.Revert} {
			rendered := RenderChangeGateRefusal(refusalData(command))
			assert.Contains(t, rendered, "- **Wait for the freeze to end**, then run the same command again.\n", command)
			assert.NotContains(t, rendered, "override", command)
		}
	})

	// A refused apply-confirm has released its pending confirmation, so
	// running apply-confirm again would find nothing to confirm.
	t.Run("apply-confirm starts again with apply", func(t *testing.T) {
		rendered := RenderChangeGateRefusal(refusalData(action.ApplyConfirm))
		assert.Contains(t, rendered, "## ❄️ Apply-confirm Blocked by a Freeze\n")
		assert.Contains(t, rendered, "Nothing was applied, and the pending confirmation was released.")
		assert.Contains(t, rendered, "- **Wait for the freeze to end**, then run `apply` again, then `apply-confirm`.\n")
		assert.Contains(t, rendered, "Run `apply` again, then run `apply-confirm` with `--override")
	})

	// Approver teams are named, not mentioned: nobody has asked them for
	// anything yet.
	t.Run("the approver teams are named without a mention", func(t *testing.T) {
		data := refusalData(action.Apply)
		data.Approvers = []string{"acme/freeze-approvers", "acme/sre"}
		rendered := RenderChangeGateRefusal(data)
		assert.Contains(t, rendered, "must approve it: a member of `acme/freeze-approvers` or `acme/sre`.\n")
		assert.NotContains(t, rendered, "@acme/")
	})

	t.Run("with no approver team there is no override to ask for", func(t *testing.T) {
		data := refusalData(action.Apply)
		data.Approvers = nil
		rendered := RenderChangeGateRefusal(data)
		assert.Contains(t, rendered, "- **Wait for the freeze to end**, then run the same command again.\n")
		assert.NotContains(t, rendered, "override")
	})

	t.Run("an unrecorded request says to retry instead", func(t *testing.T) {
		data := refusalData(action.Apply)
		data.OverrideUnrecorded = true
		rendered := RenderChangeGateRefusal(data)
		assert.Contains(t, rendered, "### Override not recorded\n\nSchemaBot could not confirm who is asking and who wrote this pull request, so it recorded no override request. Run the command again.")
		assert.NotContains(t, rendered, "What you can do")

		data.CommandName = action.ApplyConfirm
		assert.Contains(t, RenderChangeGateRefusal(data), "Run `apply` again, then `apply-confirm` with `--override`. If this keeps happening")
	})
}

func TestRenderChangeGateRefusalFreezes(t *testing.T) {
	t.Run("several freezes each get a row", func(t *testing.T) {
		ends := time.Date(2026, 1, 15, 10, 0, 0, 0, time.FixedZone("PST", -8*60*60))
		rendered := RenderChangeGateRefusal(refusalData(action.Cutover,
			RefusingFreeze{ID: "frz-1", Reason: "SEV-1234"},
			RefusingFreeze{ID: "frz-2", Reason: " ", EndsAt: &ends},
		))
		assert.Contains(t, rendered, "## ❄️ Cutover Blocked by Freezes\n")
		assert.Contains(t, rendered, "| `frz-1` | until lifted | SEV-1234 |\n| `frz-2` | until 2026-01-15T18:00:00Z | — |\n")
		assert.Contains(t, rendered, "- **Wait for the freezes to end**")
	})

	t.Run("freezes past the listed ones are counted", func(t *testing.T) {
		var freezes []RefusingFreeze
		for i := range maxListedFreezes + 2 {
			freezes = append(freezes, RefusingFreeze{ID: fmt.Sprintf("frz-%d", i), Reason: strings.Repeat("r", 400)})
		}
		rendered := RenderChangeGateRefusal(refusalData(action.Apply, freezes...))
		assert.Contains(t, rendered, fmt.Sprintf("`frz-%d`", maxListedFreezes-1))
		assert.NotContains(t, rendered, fmt.Sprintf("`frz-%d`", maxListedFreezes))
		assert.Contains(t, rendered, "\n2 more freezes also hold it.\n")
		assert.Contains(t, rendered, "| "+strings.Repeat("r", maxListedFreezeReason-1)+"… |")
	})

	// A reason is a person's text: it stays on its row, renders no markup and
	// notifies nobody.
	t.Run("a hostile reason stays in its cell", func(t *testing.T) {
		for raw, cell := range map[string]string{"a|b": `a\|b`, `a\|b`: `a\|b`, `a\\|b`: `a\\\|b`, "`a|b`": "`a\\|b`"} {
			assert.Equal(t, cell, tableCellText(raw), raw)
		}
		rendered := RenderChangeGateRefusal(refusalData(action.Apply,
			RefusingFreeze{ID: "frz|1", Reason: "a | b\n## injected <img src=x> @org/team"}))
		row := ""
		for line := range strings.SplitSeq(rendered, "\n") {
			if strings.HasPrefix(line, "| `frz") {
				row = line
			}
		}
		assert.Equal(t, 4, strings.Count(row, "|")-strings.Count(row, `\|`), "three cells, so four unescaped separators: %s", row)
		assert.NotContains(t, rendered, "\n## injected")
		assert.NotContains(t, rendered, " <img", "an unescaped tag would render")
		assert.NotContains(t, rendered, " @org/team")
		assert.Contains(t, row, "`frz\\|1`")
	})
}

func TestRenderChangeGateRefusalOverrideRequested(t *testing.T) {
	data := refusalData(action.Apply)
	data.Tenant = "acme"
	data.Override = &RefusalOverrideRequest{ID: "ovr-0123456789abcdef", Requester: "carol", Author: "alice", Reason: "hotfix for @payments-oncall"}
	rendered := RenderChangeGateRefusal(data)
	assert.Contains(t, rendered, "### Override requested\n\n"+
		"Override request `ovr-0123456789abcdef` is waiting for a freeze approver.\n\n"+
		"**Reason the approver sees:** hotfix for `@payments-oncall`.\n\n"+
		"**Freeze approvers**:\n- @acme/freeze-approvers\n\n"+
		"1. A freeze approver other than @carol and other than the PR author (@alice) comments:\n"+
		"   ```\n   schemabot freeze approve ovr-0123456789abcdef --tenant acme\n   ```\n"+
		"2. Then @carol runs this command again.\n")
	assert.NotContains(t, rendered, "What you can do")

	t.Run("an author asking on their own pull request is named once", func(t *testing.T) {
		data := refusalData(action.ApplyConfirm)
		data.Override = &RefusalOverrideRequest{ID: "ovr-1", Requester: "alice", Author: "Alice", Reason: "hotfix!", ReasonKept: true}
		rendered := RenderChangeGateRefusal(data)
		assert.Contains(t, rendered, "**Reason the approver sees:** hotfix! You asked for it earlier, so it keeps the reason you gave then.\n")
		assert.Contains(t, rendered, "1. A freeze approver other than @alice, who wrote this pull request, comments:\n")
		assert.Contains(t, rendered, "2. Then @alice runs `apply` again, then `apply-confirm`.\n")
	})

	t.Run("with no approver team nobody is waited on", func(t *testing.T) {
		data := refusalData(action.ApplyConfirm)
		data.Approvers = nil
		data.Override = &RefusalOverrideRequest{ID: "ovr-1", Requester: "carol", Author: "alice", Reason: "hotfix"}
		rendered := RenderChangeGateRefusal(data)
		assert.Contains(t, rendered, "### Override requested\n\nSchemaBot recorded override request `ovr-1`, but no team can approve an override from a pull request, so nobody can approve it here. "+
			"Wait for the freeze to end, then run `apply` again, then `apply-confirm`.\n")
		assert.NotContains(t, rendered, "freeze approve")
		assert.NotContains(t, rendered, "waiting")
		assert.NotContains(t, rendered, "Freeze approvers")
	})

	t.Run("a long reason is clamped", func(t *testing.T) {
		data := refusalData(action.Apply)
		data.Override = &RefusalOverrideRequest{ID: "ovr-1", Requester: "carol", Author: "alice", Reason: strings.Repeat("a", 400)}
		assert.Contains(t, RenderChangeGateRefusal(data), "**Reason the approver sees:** "+strings.Repeat("a", maxShownOverrideReason-1)+"…\n")
	})
}
