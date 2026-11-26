package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// A stopped apply's PR comments say which freeze stopped it and how it
// resumes, in every layout, and say nothing of a freeze when an operator
// stopped it (AZ-11).
func TestStoppedComments_NameTheFreezeThatStoppedTheApply(t *testing.T) {
	stoppedApply := func() *storage.Apply {
		apply := runningApply()
		apply.State = state.Apply.Stopped
		return apply
	}
	stoppedOp := func(id int64, deployment, key string) *storage.ApplyOperation {
		return &storage.ApplyOperation{
			ID: id, ApplyID: 1, Deployment: deployment, OperationKey: key,
			State: state.ApplyOperation.Stopped, CutoverPolicy: storage.CutoverPolicyBarrier, OnFailure: storage.OnFailureHalt,
		}
	}
	layouts := []struct {
		name string
		ops  []*storage.ApplyOperation
	}{
		{"Single", nil},
		{"MultiDeployment", []*storage.ApplyOperation{stoppedOp(1, "eu", ""), stoppedOp(2, "us", "")}},
		{"Sharded", []*storage.ApplyOperation{
			stoppedOp(1, "cake", "payments_sharded/-80/mutes"),
			stoppedOp(2, "cake", "payments_sharded/80-/mutes"),
		}},
	}
	freeze := &apitypes.FreezeStopRef{ID: "freeze-1", Reason: "SEV-1234 ask @alice"}
	render := map[string]func(*storage.Apply, []*storage.ApplyOperation, *apitypes.FreezeStopRef) string{
		"Status": func(apply *storage.Apply, ops []*storage.ApplyOperation, freeze *apitypes.FreezeStopRef) string {
			return formatApplyStatusComment(apply, ops, false, nil, nil, nil, nil, "", "", freeze, 0)
		},
		"Summary": func(apply *storage.Apply, ops []*storage.ApplyOperation, freeze *apitypes.FreezeStopRef) string {
			return formatApplySummaryComment(apply, ops, false, nil, nil, nil, nil, "", "", freeze)
		},
	}
	for _, layout := range layouts {
		for kind, render := range render {
			t.Run(layout.name+kind, func(t *testing.T) {
				out := render(stoppedApply(), layout.ops, freeze)
				assert.Contains(t, out, "> ⏹️ Stopped by freeze `freeze-1` (SEV-1234 ask `@alice`).")
				assert.Contains(t, out, "Start it again after the freeze ends or is lifted.")
				assert.Contains(t, out, "add `--override` and your reason to the start command")
				assert.Contains(t, out, "schemabot start apply-1 -e production", "the resume command stays")
				assert.NotContains(t, out, " @alice", "a mention in the reason notifies nobody")

				operator := render(stoppedApply(), layout.ops, nil)
				assert.NotContains(t, operator, "Stopped by freeze", "an operator's stop names no freeze")
				assert.Contains(t, operator, "schemabot start apply-1 -e production")
			})
		}
	}
}

// A freeze's stop that withdraws a queued start says the freeze stopped it,
// and how the change resumes.
func TestRenderStartWithdrawn_NamesTheFreeze(t *testing.T) {
	out := templates.RenderStartWithdrawn(templates.StartWithdrawnData{
		ApplyID: "apply-1", Environment: "staging", StoppedByFreeze: "freeze-1",
	})
	assert.Contains(t, out, "**Stopped by**: freeze `freeze-1`")
	assert.Contains(t, out, "A freeze that stops schema changes in this environment took effect before the start requested earlier did")
	assert.Contains(t, out, "add `--override` and your reason to the start command")

	operator := templates.RenderStartWithdrawn(templates.StartWithdrawnData{
		ApplyID: "apply-1", Environment: "staging", StoppedBy: "alice",
	})
	assert.Contains(t, operator, "**Stopped by**: @alice")
	assert.NotContains(t, operator, "freeze")
}
