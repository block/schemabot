package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

const (
	wantDeferredCutoverLine = "\n**Options**: ⏸️ Defer Cutover\n"
	wantBothOptionsLine     = "\n**Options**: ⏸️ Defer Cutover | ⏩ Skip Revert\n"
	wantSkipRevertLine      = "\n**Options**: ⏩ Skip Revert\n"
)

func deferredCutoverApplyData(applyState, taskState string, deferCutover bool) ApplyStatusCommentData {
	return ApplyStatusCommentData{
		ApplyID:      "apply-abc123",
		Database:     "testapp",
		Environment:  "production",
		RequestedBy:  "aparajon",
		State:        applyState,
		Engine:       storage.EngineSpirit,
		DeferCutover: deferCutover,
		Tables: []TableProgressData{
			{TableName: "sub_sweeps", Status: taskState, RowsCopied: 120, RowsTotal: 1000, PercentComplete: 12},
		},
	}
}

// An apply started with --defer-cutover that is still copying rows or
// checksumming tells the PR reader, right above the status that changes on
// every update, that it was started with cutover deferred, so a long copy is
// not read as a change that will finish on its own.
func TestRenderApplyStatusComment_DeferredCutoverLineBeforeCutover(t *testing.T) {
	for name, tc := range map[string]struct {
		applyState, taskState, status string
	}{
		"copying rows": {state.Apply.Running, state.Task.Running, "In Progress"},
		"checksumming": {state.Apply.Checksumming, state.Task.Checksumming, "Checksumming"},
	} {
		t.Run(name, func(t *testing.T) {
			out := RenderApplyStatusComment(deferredCutoverApplyData(tc.applyState, tc.taskState, true))

			assert.Contains(t, out, wantDeferredCutoverLine+"\n**Status**: "+tc.status+"\n", out)
			assert.Equal(t, 1, strings.Count(out, "Defer Cutover"), out)
			assert.Contains(t, out, "To stop this schema change:\n```\nschemabot stop apply-abc123 -e production\n```\n", "the footer still offers stop")
		})
	}
}

// An apply started with --skip-revert lists that option too, as the plan
// did, so an option the operator chose never looks dropped between the plan
// and the progress comment.
func TestRenderApplyStatusComment_OptionsLineListsSkipRevert(t *testing.T) {
	both := deferredCutoverApplyData(state.Apply.Running, state.Task.Running, true)
	both.SkipRevert = true
	out := RenderApplyStatusComment(both)
	assert.Contains(t, out, wantBothOptionsLine+"\n**Status**: In Progress\n", out)
	assert.Equal(t, 1, strings.Count(out, "**Options**"), out)

	skipOnly := deferredCutoverApplyData(state.Apply.Running, state.Task.Running, false)
	skipOnly.SkipRevert = true
	out = RenderApplyStatusComment(skipOnly)
	assert.Contains(t, out, wantSkipRevertLine+"\n**Status**: In Progress\n", out)
	assert.NotContains(t, out, "Defer Cutover", out)

	done := deferredCutoverApplyData(state.Apply.Completed, state.Task.Completed, true)
	done.SkipRevert = true
	assert.NotContains(t, RenderApplyStatusComment(done), "**Options**", "after cutover the options have nothing left to say")
}

// An apply that cuts over on its own has nothing to wait for, so its copy and
// checksum carry no deferred-cutover line.
func TestRenderApplyStatusComment_NoDeferredCutoverLineWhenNotDeferred(t *testing.T) {
	for _, tc := range []struct{ applyState, taskState string }{
		{state.Apply.Running, state.Task.Running},
		{state.Apply.Checksumming, state.Task.Checksumming},
	} {
		out := RenderApplyStatusComment(deferredCutoverApplyData(tc.applyState, tc.taskState, false))
		assert.NotContains(t, out, "Defer Cutover", "state %s:\n%s", tc.applyState, out)
	}
}

// Once a deferred apply waits for cutover the footer gives the command itself,
// and after cutover the deferral has nothing left to say, so neither repeats
// the line.
func TestRenderApplyStatusComment_NoDeferredCutoverLineAtOrAfterCutover(t *testing.T) {
	waiting := RenderApplyStatusComment(deferredCutoverApplyData(state.Apply.WaitingForCutover, state.Task.WaitingForCutover, true))
	assert.NotContains(t, waiting, "Defer Cutover", waiting)
	assert.Contains(t, waiting, "To proceed with cutover:\n```\nschemabot cutover apply-abc123 -e production\n```\n")

	for _, applyState := range []string{
		state.Apply.CuttingOver, state.Apply.Completed, state.Apply.Failed,
		state.Apply.Stopped, state.Apply.Cancelled,
	} {
		out := RenderApplyStatusComment(deferredCutoverApplyData(applyState, state.Task.Completed, true))
		assert.NotContains(t, out, "Defer Cutover", "state %s:\n%s", applyState, out)
	}
}

// The comment posted the moment a deferred apply is accepted already says it
// will wait for the operator, whether the apply starts at once or is queued.
func TestRenderApplyStarted_DeferredCutoverLine(t *testing.T) {
	for _, applyState := range []string{state.Apply.Running, state.Apply.Pending} {
		data := ApplyStatusCommentData{ApplyID: "apply-abc123", Database: "testapp", Environment: "production", State: applyState, Engine: storage.EngineSpirit, DeferCutover: true}
		assert.Contains(t, RenderApplyStarted(data), wantDeferredCutoverLine, "state %s", applyState)

		data.SkipRevert = true
		assert.Contains(t, RenderApplyStarted(data), wantBothOptionsLine, "state %s", applyState)

		data.DeferCutover, data.SkipRevert = false, false
		assert.NotContains(t, RenderApplyStarted(data), "**Options**", "state %s", applyState)
	}
}

// The progress response carries the apply's options, and the comment built
// from it keeps the defer-cutover and skip-revert options.
func TestApplyStatusFromProgress_Options(t *testing.T) {
	deferred := ApplyStatusFromProgress(&apitypes.ProgressResponse{State: state.Apply.Checksumming, Options: map[string]string{"defer_cutover": "true"}}, "")
	assert.True(t, deferred.DeferCutover)
	assert.False(t, deferred.SkipRevert)

	skipping := ApplyStatusFromProgress(&apitypes.ProgressResponse{State: state.Apply.Checksumming, Options: map[string]string{"skip_revert": "true"}}, "")
	assert.False(t, skipping.DeferCutover)
	assert.True(t, skipping.SkipRevert)

	automatic := ApplyStatusFromProgress(&apitypes.ProgressResponse{State: state.Apply.Checksumming}, "")
	assert.False(t, automatic.DeferCutover)
	assert.False(t, automatic.SkipRevert)
}

// A multi-deployment rollout states the deferral once at the rollout level,
// not in each deployment's section, and drops it once a deployment waits for
// cutover and the footer gives the command.
func TestRenderMultiDeploymentApplyComment_DeferredCutoverLineOnce(t *testing.T) {
	render := func(ops []presentation.Operation, deferCutover, skipRevert bool) string {
		return RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
			Model:       presentation.Derive(ops),
			ApplyID:     "apply-123",
			Environment: "production",
			Details: []*ApplyStatusCommentData{
				{Database: "orders_us", State: ops[0].State, ApplyID: "apply-123", Environment: "production", Engine: storage.EngineSpirit, DeferCutover: deferCutover, SkipRevert: skipRevert},
				{Database: "orders_eu", State: ops[1].State, ApplyID: "apply-123", Environment: "production", Engine: storage.EngineSpirit, DeferCutover: deferCutover, SkipRevert: skipRevert},
			},
		})
	}
	copying := []presentation.Operation{barrierOp("us", so.Running), barrierOp("eu", so.Running)}

	out := render(copying, true, false)
	require.Equal(t, 1, strings.Count(out, "Defer Cutover"), out)
	assert.Less(t, strings.Index(out, wantDeferredCutoverLine), strings.Index(out, "<details"), "the line sits above the deployment sections:\n%s", out)

	assert.NotContains(t, render(copying, false, false), "Defer Cutover")

	both := render(copying, true, true)
	assert.Equal(t, 1, strings.Count(both, "**Options**"), both)
	assert.Contains(t, both, wantBothOptionsLine, both)

	ready := render([]presentation.Operation{barrierOp("us", so.WaitingForCutover), barrierOp("eu", so.Running)}, true, false)
	assert.NotContains(t, ready, "Defer Cutover", ready)
	assert.Contains(t, ready, "schemabot cutover apply-123 -e production")

	// A degraded rollout may have a deployment already past cutover, so it
	// carries no promise of a pause still ahead.
	for _, past := range []string{so.CuttingOver, so.RevertWindow} {
		ops := []presentation.Operation{barrierOp("us", so.Failed), barrierOp("eu", past)}
		require.Equal(t, state.Apply.RunningDegraded, presentation.Derive(ops).State, "a failed deployment beside one in %s derives a degraded rollout", past)
		degraded := render(ops, true, false)
		assert.NotContains(t, degraded, "Defer Cutover", degraded)
	}
}
