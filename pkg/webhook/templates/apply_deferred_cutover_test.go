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

const wantDeferredCutoverLine = "\n**Options**: ⏸️ Defer Cutover — pauses before cutover and waits for `schemabot cutover`\n"

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
// checksumming tells the PR reader, right under its status, that it will pause
// before cutover and wait for `schemabot cutover`, so a long copy is not read
// as a change that will finish on its own.
func TestRenderApplyStatusComment_DeferredCutoverLineBeforeCutover(t *testing.T) {
	for name, tc := range map[string]struct {
		applyState, taskState, status string
	}{
		"copying rows": {state.Apply.Running, state.Task.Running, "In Progress"},
		"checksumming": {state.Apply.Checksumming, state.Task.Checksumming, "Checksumming"},
	} {
		t.Run(name, func(t *testing.T) {
			out := RenderApplyStatusComment(deferredCutoverApplyData(tc.applyState, tc.taskState, true))

			assert.Contains(t, out, "\n**Status**: "+tc.status+"\n"+wantDeferredCutoverLine, out)
			assert.Equal(t, 1, strings.Count(out, "Defer Cutover"), out)
			assert.Contains(t, out, "To stop this schema change:\n```\nschemabot stop apply-abc123 -e production\n```\n", "the footer still offers stop")
		})
	}
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

		data.DeferCutover = false
		assert.NotContains(t, RenderApplyStarted(data), "Defer Cutover", "state %s", applyState)
	}
}

// The progress response carries the apply's options, and the comment built
// from it keeps the defer-cutover option.
func TestApplyStatusFromProgress_DeferCutoverOption(t *testing.T) {
	deferred := ApplyStatusFromProgress(&apitypes.ProgressResponse{State: state.Apply.Checksumming, Options: map[string]string{"defer_cutover": "true"}}, "")
	assert.True(t, deferred.DeferCutover)

	automatic := ApplyStatusFromProgress(&apitypes.ProgressResponse{State: state.Apply.Checksumming}, "")
	assert.False(t, automatic.DeferCutover)
}

// A multi-deployment rollout states the deferral once at the rollout level,
// not in each deployment's section, and drops it once a deployment waits for
// cutover and the footer gives the command.
func TestRenderMultiDeploymentApplyComment_DeferredCutoverLineOnce(t *testing.T) {
	render := func(ops []presentation.Operation, deferCutover bool) string {
		return RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
			Model:       presentation.Derive(ops),
			ApplyID:     "apply-123",
			Environment: "production",
			Details: []*ApplyStatusCommentData{
				{Database: "orders_us", State: ops[0].State, ApplyID: "apply-123", Environment: "production", Engine: storage.EngineSpirit, DeferCutover: deferCutover},
				{Database: "orders_eu", State: ops[1].State, ApplyID: "apply-123", Environment: "production", Engine: storage.EngineSpirit, DeferCutover: deferCutover},
			},
		})
	}
	copying := []presentation.Operation{barrierOp("us", so.Running), barrierOp("eu", so.Running)}

	out := render(copying, true)
	require.Equal(t, 1, strings.Count(out, "Defer Cutover"), out)
	assert.Less(t, strings.Index(out, wantDeferredCutoverLine), strings.Index(out, "<details"), "the line sits above the deployment sections:\n%s", out)

	assert.NotContains(t, render(copying, false), "Defer Cutover")

	ready := render([]presentation.Operation{barrierOp("us", so.WaitingForCutover), barrierOp("eu", so.Running)}, true)
	assert.NotContains(t, ready, "Defer Cutover", ready)
	assert.Contains(t, ready, "schemabot cutover apply-123 -e production")
}
