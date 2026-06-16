package templates

import (
	"testing"

	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var so = state.ApplyOperation

func barrierOp(dep, st string) presentation.Operation {
	return presentation.Operation{Deployment: dep, State: st, Barrier: true, HaltOnFailure: true}
}

func rollingOp(dep, st string) presentation.Operation {
	return presentation.Operation{Deployment: dep, State: st, HaltOnFailure: true}
}

// A barrier rollout mid-flight (one deployment parked ready for cutover, one
// copying, two queued behind it) renders an aggregate header with the running
// title, a per-status count line, a single cutover next-action, an at-a-glance
// per-deployment summary, and a <details> block per deployment in resolved order.
func TestRenderMultiDeploymentApplyComment_BarrierInProgress(t *testing.T) {
	withTemplateTimestamp(t, "2026-06-16 19:43:00 UTC")
	model := presentation.Derive([]presentation.Operation{
		barrierOp("eu", so.WaitingForCutover),
		barrierOp("us", so.Running),
		barrierOp("au", so.Pending),
		barrierOp("ca", so.Pending),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
	})

	assert.Contains(t, out, "## Schema Change In Progress")
	assert.Contains(t, out, "**Environment**: `production`")
	assert.Contains(t, out, "**Apply ID**: `apply-123`")
	assert.Contains(t, out, "_Last updated: <relative-time datetime=\"2026-06-16T19:43:00Z\">2026-06-16 19:43:00 UTC</relative-time> (2026-06-16 19:43:00 UTC)_")
	assert.NotContains(t, out, "**Last updated**")
	assert.Contains(t, out, "**Deployments**: 1 ready for cutover, 1 running, 2 waiting")

	// Single next-action points at the cutover-ready deployment, even though the
	// aggregate is still running. The command is the executable apply-ID form the
	// CLI accepts today (no --deployment flag yet).
	assert.Contains(t, out, "To cut over `eu`:")
	assert.Contains(t, out, "schemabot cutover apply-123")
	assert.NotContains(t, out, "--deployment")

	// Per-deployment summary lines, in resolved order, with derived labels.
	assert.Contains(t, out, "- 🟢 eu — ready for cutover — next in order")
	assert.Contains(t, out, "- 🔄 us — running table copy")
	assert.Contains(t, out, "- ⏳ au — waiting for us")
	assert.Contains(t, out, "- ⏳ ca — waiting for us")

	// Active/ready deployments default open; queued ones default collapsed.
	assert.Contains(t, out, "<details open>\n<summary>🟢 eu — ready for cutover — next in order</summary>")
	assert.Contains(t, out, "<details open>\n<summary>🔄 us — running table copy</summary>")
	assert.Contains(t, out, "<details>\n<summary>⏳ au — waiting for us</summary>")
	assert.Contains(t, out, "<details>\n<summary>⏳ ca — waiting for us</summary>")
}

// A halt-on-failure rollout with a failed deployment keeps the aggregate failed,
// offers retry as the next action, and marks the never-started deployments as
// halted (and open, since halted explains the next action).
func TestRenderMultiDeploymentApplyComment_FailedHalt(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("eu", so.WaitingForCutover),
		rollingOp("us", so.Failed),
		rollingOp("au", so.Pending),
		rollingOp("ca", so.Pending),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
	})

	assert.Contains(t, out, "## ❌ Schema Change Failed")
	assert.Contains(t, out, "**Deployments**: 1 ready for cutover, 2 halted, 1 failed")
	// The recovery path for a failed apply is retry, matching the single-deployment
	// footer. revert is only for a deployment in its post-cutover revert window.
	assert.Contains(t, out, "To retry:")
	assert.Contains(t, out, "schemabot apply -e production")
	assert.NotContains(t, out, "schemabot revert")
	assert.Contains(t, out, "- ❌ us — failed")
	assert.Contains(t, out, "- ⏸ au — halted — us failed")
	assert.Contains(t, out, "<details open>\n<summary>⏸ au — halted — us failed</summary>")
	// With no error detail on the failed operation, the first-failure line names
	// the deployment without a reason.
	assert.Contains(t, out, "> ⚠️ **First failure:** <code>us</code>\n")
}

func TestRenderMultiDeploymentApplyComment_UsesOneRenderTimestamp(t *testing.T) {
	original := TimestampFunc
	timestamps := []string{"2026-06-16 19:43:00 UTC", "2026-06-16 19:43:01 UTC"}
	TimestampFunc = func() string {
		ts := timestamps[0]
		timestamps = timestamps[1:]
		return ts
	}
	t.Cleanup(func() { TimestampFunc = original })

	model := presentation.Derive([]presentation.Operation{
		rollingOp("us", so.Running),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
		RequestedBy: "aparajon",
		Details: map[string]ApplyStatusCommentData{
			"us": {
				Database:    "payments_us",
				Environment: "production",
				State:       state.Apply.Running,
				RequestedBy: "aparajon",
			},
		},
	})

	assert.Contains(t, out, "*Applied by @aparajon at 2026-06-16 19:43:00 UTC*")
	assert.Contains(t, out, "<relative-time datetime=\"2026-06-16T19:43:00Z\">2026-06-16 19:43:00 UTC</relative-time>")
	assert.NotContains(t, out, "2026-06-16 19:43:01 UTC")
}

// firstFailingOp builds a rolling, continue-policy operation carrying an error,
// so a fan-out can fail one deployment and keep going.
func firstFailingOp(dep, st, errMsg string) presentation.Operation {
	return presentation.Operation{Deployment: dep, State: st, HaltOnFailure: false, Error: errMsg}
}

// A failed deployment's reason is lifted to the aggregate header so an operator
// sees what failed without expanding that deployment's section. Under on_failure
// continue a later deployment is still copying, so the reason surfaces on the
// in-progress status comment, and only the first failure in resolved order shows.
func TestRenderMultiDeploymentApplyComment_FirstFailureSurfacesError(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		firstFailingOp("us", so.Failed, "Error 1061: Duplicate key name idx"),
		firstFailingOp("eu", so.Failed, "second failure"),
		{Deployment: "au", State: so.Running, HaltOnFailure: false},
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
	})

	// A later deployment is still running even though the aggregate is fail-closed.
	assert.Contains(t, out, "- 🔄 au — running table copy")
	assert.Contains(t, out, "> ⚠️ **First failure:** <code>us</code> — Error 1061: Duplicate key name idx\n")
	// Only the earliest failure is lifted to the header.
	assert.NotContains(t, out, "First failure:** <code>eu</code>")
}

// A healthy rollout (no failed deployment) renders no first-failure line.
func TestRenderMultiDeploymentApplyComment_NoFirstFailureWhenHealthy(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("eu", so.Completed),
		rollingOp("us", so.Running),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
	})

	assert.NotContains(t, out, "First failure:")
}

// The terminal summary comment surfaces the same first-failure line, and escapes
// HTML in the error so a failure message can never inject markup.
func TestRenderMultiDeploymentApplySummaryComment_FirstFailureSurfacesError(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("eu", so.Completed),
		{Deployment: "us", State: so.Failed, HaltOnFailure: true, Error: "boom <script>"},
		rollingOp("au", so.Pending),
	})
	out := RenderMultiDeploymentApplySummaryComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
	})

	assert.Contains(t, out, "> ⚠️ **First failure:** <code>us</code> — boom &lt;script&gt;\n")
}

// A deployment name with HTML-significant characters is escaped inside the
// <code> element, so it renders correctly rather than leaking entities the way
// an escaped Markdown code span would.
func TestRenderMultiDeploymentApplyComment_FirstFailureEscapesName(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		{Deployment: "us&ca", State: so.Failed, HaltOnFailure: true, Error: "boom"},
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
	})

	assert.Contains(t, out, "> ⚠️ **First failure:** <code>us&amp;ca</code> — boom\n")
}

// Each deployment's <details> body is rendered by the single-deployment renderer,
// so per-table progress and the deployment's own database are preserved.
func TestRenderMultiDeploymentApplyComment_DetailsReuseSingleRenderer(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("eu", so.Completed),
		rollingOp("us", so.Running),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
		Details: map[string]ApplyStatusCommentData{
			"us": {
				Database: "payments_us",
				State:    state.Apply.Running,
				Tables: []TableProgressData{
					{TableName: "orders", Status: state.Task.Running, PercentComplete: 42, RowsCopied: 420, RowsTotal: 1000},
				},
			},
		},
	})

	// The us section carries the single-deployment body: its database and table.
	assert.Contains(t, out, "**Database**: `payments_us`")
	assert.Contains(t, out, "orders")
	// Completed deployment with no detail still renders its summary line + section,
	// with a placeholder body rather than an empty <details>.
	assert.Contains(t, out, "- ✅ eu — completed")
	assert.Contains(t, out, "<details>\n<summary>✅ eu — completed</summary>")
	assert.Contains(t, out, "_No details available yet._")
}

// A completed multi-deployment summary uses the same aggregate header, count
// line, and per-deployment summary list as the status comment, but each
// <details> body is the deployment's terminal summary (the single-deployment
// summary renderer) — so it carries the "applied successfully" outcome text
// rather than in-progress table bars.
func TestRenderMultiDeploymentApplySummaryComment_CompletedReusesSummaryRenderer(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("eu", so.Completed),
		rollingOp("us", so.Completed),
	})
	out := RenderMultiDeploymentApplySummaryComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
		Details: map[string]ApplyStatusCommentData{
			"eu": {
				Database:    "payments_eu",
				Environment: "production",
				State:       state.Apply.Completed,
				Tables: []TableProgressData{
					{TableName: "orders", Status: state.Task.Completed},
				},
			},
			"us": {
				Database:    "payments_us",
				Environment: "production",
				State:       state.Apply.Completed,
				Tables: []TableProgressData{
					{TableName: "orders", Status: state.Task.Completed},
				},
			},
		},
	})

	// Aggregate terminal header and per-deployment summary list, as the status
	// comment, so an operator sees rollout outcome at a glance.
	assert.Contains(t, out, "## ✅ Schema Change Applied")
	assert.Contains(t, out, "- ✅ eu — completed")
	assert.Contains(t, out, "- ✅ us — completed")

	// Each <details> body is the summary renderer's output, not the status one.
	assert.Contains(t, out, "Schema change applied successfully — your changes are live!")
	assert.Contains(t, out, "**Database**: `payments_eu`")
	assert.Contains(t, out, "**Database**: `payments_us`")
}

// A failed multi-deployment summary keeps the aggregate failed header and routes
// each deployment's terminal summary into its section, so the failed deployment's
// retry guidance appears in its <details> body.
func TestRenderMultiDeploymentApplySummaryComment_FailedDeploymentSummary(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("eu", so.Completed),
		rollingOp("us", so.Failed),
	})
	out := RenderMultiDeploymentApplySummaryComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
		Details: map[string]ApplyStatusCommentData{
			"us": {
				Database:     "payments_us",
				Environment:  "production",
				State:        state.Apply.Failed,
				ErrorMessage: "lock wait timeout",
				Tables: []TableProgressData{
					{TableName: "orders", Status: state.Task.Failed},
				},
			},
		},
	})

	assert.Contains(t, out, "## ❌ Schema Change Failed")
	// The failed deployment's section carries the single-deployment summary's
	// error and retry guidance.
	assert.Contains(t, out, "lock wait timeout")
	assert.Contains(t, out, "To retry:")
}

// A deployment in an unrecognized engine state still renders a summary line and
// section without a leading space where the glyph would be.
func TestRenderMultiDeploymentApplyComment_UnknownStateNoGlyph(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("eu", so.Running),
		rollingOp("us", "some_engine_state"),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{Model: model, ApplyID: "apply-123", Environment: "staging"})
	require.Len(t, model.Deployments, 2)
	assert.Contains(t, out, "- us — some_engine_state")
	assert.NotContains(t, out, "-  us")
}

// When the rollup has no pending operator action, no next-action block is written.
func TestRenderMultiDeploymentApplyComment_NoNextActionWhenCompleted(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("eu", so.Completed),
		rollingOp("us", so.Completed),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{Model: model, ApplyID: "apply-123", Environment: "production"})
	assert.Contains(t, out, "## ✅ Schema Change Applied")
	assert.NotContains(t, out, "schemabot cutover")
	assert.NotContains(t, out, "schemabot revert")
	assert.NotContains(t, out, "To resume:")
	assert.NotContains(t, out, "To retry:")
	assert.NotContains(t, out, "Last updated")
}

// Deployment names and labels come from configuration/engine state, so they are
// HTML-escaped before being interpolated into the <summary> tags — a name with
// markup characters must not break the comment HTML.
func TestRenderMultiDeploymentApplyComment_EscapesSummaryHTML(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("eu<b>", so.Running),
		rollingOp("us&ca", so.Running),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{Model: model, ApplyID: "apply-123", Environment: "production"})
	assert.Contains(t, out, "eu&lt;b&gt;")
	assert.Contains(t, out, "us&amp;ca")
	assert.NotContains(t, out, "<summary>🔄 eu<b>")
}
