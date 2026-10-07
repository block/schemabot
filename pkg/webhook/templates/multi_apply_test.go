package templates

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/ui"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var so = state.ApplyOperation

func barrierOp(dep, st string) presentation.Operation {
	return presentation.Operation{Deployment: dep, State: st, Barrier: true}
}

func rollingOp(dep, st string) presentation.Operation {
	return presentation.Operation{Deployment: dep, State: st}
}

func continuingOp(dep, st string) presentation.Operation {
	return presentation.Operation{Deployment: dep, State: st, ContinueOnFailure: true}
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

	assert.Contains(t, out, "## Schema Change Status")
	assert.Contains(t, out, "— Production")
	assert.Contains(t, out, "**Apply ID**: `apply-123`")
	assert.Contains(t, out, "_Last updated: <relative-time datetime=\"2026-06-16T19:43:00Z\">2026-06-16 19:43:00 UTC</relative-time> (2026-06-16 19:43:00 UTC)_")
	assert.NotContains(t, out, "**Last updated**")
	assert.Contains(t, out, "**Deployments**: 1 ready for cutover, 1 running, 2 waiting")

	// Single next-action points at the cutover-ready deployment, even though the
	// aggregate is still running. The apply was not deferred, so SchemaBot cuts
	// eu over itself and the comment offers no command to run.
	assert.Contains(t, out, "SchemaBot will cut over `eu` next — no action needed.")
	assert.NotContains(t, out, "To cut over")
	assert.NotContains(t, out, "schemabot cutover")

	// Per-deployment summary lines, in resolved order, with derived labels.
	assert.Contains(t, out, "- 🟢 `eu` — ready for cutover — next in order")
	assert.Contains(t, out, "- 🔄 `us` — running table copy")
	assert.Contains(t, out, "- ⏳ `au` — waiting for us")
	assert.Contains(t, out, "- ⏳ `ca` — waiting for us")

	// Active/ready deployments default open; queued ones default collapsed.
	assert.Contains(t, out, "<details open>\n<summary>🟢 eu — ready for cutover — next in order</summary>")
	assert.Contains(t, out, "<details open>\n<summary>🔄 us — running table copy</summary>")
	assert.Contains(t, out, "<details>\n<summary>⏳ au — waiting for us</summary>")
	assert.Contains(t, out, "<details>\n<summary>⏳ ca — waiting for us</summary>")
}

// A halt-on-failure rollout whose failure sits beside a deployment still
// waiting for cutover has not settled, so a new apply would be refused: the
// footer offers stop, not retry, and says when retry opens up and that it
// resumes. The never-started deployments read as halted (and open, since
// halted explains the next action).
func TestRenderMultiDeploymentApplyComment_FailedHalt(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("eu", so.WaitingForCutover),
		rollingOp("us", so.Failed),
		rollingOp("au", so.Pending),
		rollingOp("ca", so.Pending),
	})
	require.False(t, state.IsTerminalApplyState(model.State), "the rollout has not settled: %s", model.State)
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
	})

	assert.Contains(t, out, "## Schema Change Status")
	assert.Contains(t, out, "**Deployments**: 1 ready for cutover, 2 halted, 1 failed")
	footer := out[strings.LastIndex(out, "\n---\n"):]
	assert.Contains(t, footer, "To stop this schema change:\n```\nschemabot stop apply-123 -e production\n```\n\n"+presentation.RetryOnceSettledNote+"\n")
	assert.NotContains(t, out, "schemabot apply", "a new apply is refused until this one settles")
	assert.NotContains(t, out, "schemabot revert")
	assert.Contains(t, out, "- ❌ `us` — failed")
	assert.Contains(t, out, "- ⏸️ `au` — halted — us failed")
	assert.Contains(t, out, "<details open>\n<summary>⏸️ au — halted — us failed</summary>")
	// With no error detail on the failed operation, the first-failure line names
	// the deployment without a reason.
	assert.Contains(t, out, "> ❌ **First failure:** <code>us</code>\n")
}

// Once a halted rollout is terminal, its footer is the retry, and the label
// says the new apply resumes. The recovery path for a failed apply is retry,
// matching the single-deployment footer; revert is only for a deployment in
// its post-cutover revert window, and a terminal apply refuses stop.
func TestRenderMultiDeploymentApplyComment_TerminalFailureOffersRetry(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("eu", so.Completed),
		rollingOp("us", so.Failed),
		rollingOp("au", so.Pending),
	})
	require.True(t, state.IsTerminalApplyState(model.State), "the rollout has settled: %s", model.State)
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
	})

	footer := out[strings.LastIndex(out, "\n---\n"):]
	assert.Contains(t, footer, "To retry once the failure above is resolved — a new apply reprocesses only the tables that haven't completed:\n```\nschemabot apply -e production\n```\n")
	assert.NotContains(t, out, "schemabot stop")
	assert.NotContains(t, out, "schemabot revert")
	assert.NotContains(t, out, presentation.RetryOnceSettledNote)
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
		Details: []*ApplyStatusCommentData{
			{
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

// The aggregate comment uses the apply start time for no-requester attribution
// while keeping its own last-updated footer anchored to the render timestamp.
func TestRenderMultiDeploymentApplyComment_StartedAtUsesApplyStart(t *testing.T) {
	original := TimestampFunc
	TimestampFunc = func() string { return "2026-06-16 20:00:00 UTC" }
	t.Cleanup(func() { TimestampFunc = original })

	model := presentation.Derive([]presentation.Operation{
		rollingOp("us", so.Running),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
		StartedAt:   "2026-06-16T19:42:00Z",
	})

	assert.Contains(t, out, "*Started at 2026-06-16 19:42:00 UTC*")
	assert.Contains(t, out, "<relative-time datetime=\"2026-06-16T20:00:00Z\">2026-06-16 20:00:00 UTC</relative-time>")
	assert.NotContains(t, out, "*Started at 2026-06-16 20:00:00 UTC*")
}

// firstFailingOp builds a rolling, continue-policy operation carrying an error,
// so a fan-out can fail one deployment and keep going.
func firstFailingOp(dep, st, errMsg string) presentation.Operation {
	return presentation.Operation{Deployment: dep, State: st, ContinueOnFailure: true, Error: errMsg}
}

// A failed deployment's reason is lifted to the aggregate header so an operator
// sees what failed without expanding that deployment's section. Under on_failure
// continue a later deployment is still copying, so the rollout is running_degraded
// (still in progress) and the reason surfaces on the in-progress status comment,
// with only the first failure in resolved order shown.
func TestRenderMultiDeploymentApplyComment_FirstFailureSurfacesError(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		firstFailingOp("us", so.Failed, "Error 1061: Duplicate key name idx"),
		firstFailingOp("eu", so.Failed, "second failure"),
		continuingOp("au", so.Running),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
	})

	// The rollout is still in progress (running_degraded), not headlined as failed.
	assert.Contains(t, out, "## Schema Change Status")
	assert.NotContains(t, out, "Schema Change Failed")
	// A later deployment is still running while siblings have failed.
	assert.Contains(t, out, "- 🔄 `au` — running table copy")
	assert.Contains(t, out, "> ❌ **First failure:** <code>us</code> — Error 1061: Duplicate key name idx\n")
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
		{Deployment: "us", State: so.Failed, Error: "boom <script>"},
		rollingOp("au", so.Pending),
	})
	out := RenderMultiDeploymentApplySummaryComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
	})

	assert.Contains(t, out, "> ❌ **First failure:** <code>us</code> — boom &lt;script&gt;\n")
}

// A deployment name with HTML-significant characters is escaped inside the
// <code> element, so it renders correctly rather than leaking entities the way
// an escaped Markdown code span would.
func TestRenderMultiDeploymentApplyComment_FirstFailureEscapesName(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		{Deployment: "us&ca", State: so.Failed, Error: "boom"},
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
	})

	assert.Contains(t, out, "> ❌ **First failure:** <code>us&amp;ca</code> — boom\n")
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
		Details: []*ApplyStatusCommentData{
			// eu has no detail yet, so its section renders the placeholder.
			nil,
			{
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
	assert.Contains(t, out, "- ✅ `eu` — completed")
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
		Details: []*ApplyStatusCommentData{
			{
				Database:    "payments_eu",
				Environment: "production",
				State:       state.Apply.Completed,
				Tables: []TableProgressData{
					{TableName: "orders", Status: state.Task.Completed},
				},
			},
			{
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
	assert.Contains(t, out, "- ✅ `eu` — completed")
	assert.Contains(t, out, "- ✅ `us` — completed")

	// Each <details> body is the summary renderer's output, not the status one.
	assert.Contains(t, out, "Applied successfully — your schema change is live!")
	assert.Contains(t, out, "**Database**: `payments_eu`")
	assert.Contains(t, out, "**Database**: `payments_us`")
}

// The comment's headline appears once, on the aggregate header: each
// deployment's <details> body drops the headline the single-deployment renderer
// would write, since the <summary> line already names the deployment and
// repeating the title in every expanded section is noise. The rest of the body
// (database, tables) is untouched.
func TestRenderMultiDeploymentApplyComment_DetailsOmitDuplicateHeadline(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("eu", so.Completed),
		rollingOp("us", so.Running),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
		Details: []*ApplyStatusCommentData{
			{
				Database:    "payments_eu",
				Environment: "production",
				State:       state.Apply.Completed,
				Tables:      []TableProgressData{{TableName: "orders", Status: state.Task.Completed}},
			},
			{
				Database:    "payments_us",
				Environment: "production",
				State:       state.Apply.Running,
				Tables:      []TableProgressData{{TableName: "orders", Status: state.Task.Running}},
			},
		},
	})

	assert.Equal(t, 1, strings.Count(out, "## Schema Change Status"),
		"the headline must appear once, on the aggregate header, not inside each deployment's section")
	assert.True(t, strings.HasPrefix(out, "## Schema Change Status — Production"))
	// The section bodies keep their per-deployment content.
	assert.Contains(t, out, "**Database**: `payments_eu`")
	assert.Contains(t, out, "**Database**: `payments_us`")
}

// The terminal summary comment deduplicates the same way: the state-specific
// headline appears once on the aggregate header, not inside each deployment's
// expanded terminal summary.
func TestRenderMultiDeploymentApplySummaryComment_DetailsOmitDuplicateHeadline(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("eu", so.Completed),
		rollingOp("us", so.Completed),
	})
	out := RenderMultiDeploymentApplySummaryComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
		Details: []*ApplyStatusCommentData{
			{
				Database:    "payments_eu",
				Environment: "production",
				State:       state.Apply.Completed,
				Tables:      []TableProgressData{{TableName: "orders", Status: state.Task.Completed}},
			},
			{
				Database:    "payments_us",
				Environment: "production",
				State:       state.Apply.Completed,
				Tables:      []TableProgressData{{TableName: "orders", Status: state.Task.Completed}},
			},
		},
	})

	assert.Equal(t, 1, strings.Count(out, "## ✅ Schema Change Applied"),
		"the terminal headline must appear once, on the aggregate header, not inside each deployment's section")
	assert.Contains(t, out, "Applied successfully — your schema change is live!")
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
		Details: []*ApplyStatusCommentData{
			// eu carries no summary detail; only the failed member's is asserted.
			nil,
			{
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
	assert.Contains(t, out, presentation.RetryLabel+":")
}

// When the first deployment's engine rejects the change before copying a
// single row (e.g. a failed preflight check) and halts the rest of the
// rollout, the failed deployment's detail must not render a 0% progress bar —
// nothing ran, so the row reads as a failed check, not a stalled copy.
func TestRenderMultiDeploymentApplyComment_PreflightFailureHasNoProgressBar(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("apse2", so.Failed),
		rollingOp("euwe1", so.Pending),
		rollingOp("usea1", so.Pending),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "qa",
		Details: []*ApplyStatusCommentData{
			{
				Database:    "profiles_db",
				Environment: "qa",
				State:       state.Apply.Failed,
				Tables: []TableProgressData{
					{TableName: "profiles", Status: state.Task.Failed},
				},
			},
		},
	})

	assert.Contains(t, out, "**`profiles`**: ❌ Failed (before row copy started)")
	assert.NotContains(t, out, "🟥")
	assert.NotContains(t, out, "⬜")
}

// A deployment held by an earlier sibling's failure is "halted" — a derived
// presentation; its persisted operation state is still pending. The <details>
// body's Status line must carry the derived status so it agrees with its own
// <summary> line, never the raw-state "Starting" gloss.
func TestRenderMultiDeploymentApplyComment_HaltedDetailUsesDerivedStatus(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("apse2", so.Failed),
		rollingOp("euwe1", so.Pending),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "qa",
		Details: []*ApplyStatusCommentData{
			{Database: "app_db", Environment: "qa", State: state.Apply.Failed},
			{Database: "app_db", Environment: "qa", State: state.Apply.Pending},
		},
	})

	assert.Contains(t, out, "**Status**: ⏸️ Halted — apse2 failed")
	assert.NotContains(t, out, "**Status**: Starting")
	// The failed deployment keeps its raw-state gloss.
	assert.Contains(t, out, "**Status**: Failed")
}

// Pending deployments that are genuinely still coming — next in order, or
// waiting on an earlier copy — also carry their derived status in the body, so
// the body never shows the raw-state "Starting" gloss for a deployment that
// has not been claimed.
func TestRenderMultiDeploymentApplyComment_PendingDetailUsesDerivedStatus(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("eu", so.Running),
		rollingOp("us", so.Pending),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "staging",
		Details: []*ApplyStatusCommentData{
			{Database: "app_db", Environment: "staging", State: state.Apply.Running},
			{Database: "app_db", Environment: "staging", State: state.Apply.Pending},
		},
	})

	assert.Contains(t, out, "**Status**: ⏳ Waiting for eu")
	assert.NotContains(t, out, "**Status**: Starting")
	// The running deployment keeps its raw-state gloss and suffix behavior.
	assert.Contains(t, out, "**Status**: In Progress")
}

// A derived-status label interpolates deployment names, and the detail body
// lives inside raw <details> HTML — a name carrying markup must be escaped in
// the body's Status line, matching the <summary> line's escaping.
func TestRenderMultiDeploymentApplyComment_DerivedStatusEscapesLabel(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("eu<b>", so.Running),
		rollingOp("us", so.Pending),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "staging",
		Details: []*ApplyStatusCommentData{
			{Database: "app_db", Environment: "staging", State: state.Apply.Running},
			{Database: "app_db", Environment: "staging", State: state.Apply.Pending},
		},
	})

	assert.Contains(t, out, "**Status**: ⏳ Waiting for eu&lt;b&gt;")
	assert.NotContains(t, out, "**Status**: ⏳ Waiting for eu<b>")
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
	assert.Contains(t, out, "- `us` — some_engine_state")
	assert.NotContains(t, out, "-  `us`")
}

// When the rollup has no pending operator action, no next-action block is written.
func TestRenderMultiDeploymentApplyComment_NoNextActionWhenCompleted(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("eu", so.Completed),
		rollingOp("us", so.Completed),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{Model: model, ApplyID: "apply-123", Environment: "production"})
	assert.Contains(t, out, "## Schema Change Status")
	assert.NotContains(t, out, "schemabot cutover")
	assert.NotContains(t, out, "schemabot revert")
	assert.NotContains(t, out, "To resume:")
	assert.NotContains(t, out, "To retry")
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

// A rollback apply that fans out across deployments carries rollback vocabulary
// on the aggregate headline of both the in-place status comment and the
// terminal summary, matching the single-deployment comments.
func TestRenderMultiDeploymentApplyComment_Rollback(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("eu", so.Running),
		rollingOp("us", so.Pending),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model: model, ApplyID: "apply-123", Environment: "production", Rollback: true,
	})
	assert.Contains(t, out, "## Rollback Status")
	assert.NotContains(t, out, "Schema Change Status")

	completedModel := presentation.Derive([]presentation.Operation{
		rollingOp("eu", so.Completed),
		rollingOp("us", so.Completed),
	})
	summary := RenderMultiDeploymentApplySummaryComment(MultiDeploymentApplyData{
		Model: completedModel, ApplyID: "apply-123", Environment: "production", Rollback: true,
	})
	assert.Contains(t, summary, "## ⏪ Rollback Complete")
	assert.NotContains(t, summary, "Schema Change Applied")
}

// A failed deployment's raw engine error can carry internal endpoints and
// newlines. The lifted first-failure line must redact endpoints and stay on one
// Markdown line so the error cannot escape the blockquote.
func TestRenderMultiDeploymentApplyComment_FirstFailureErrorSanitized(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		firstFailingOp("us", so.Failed, "dial tcp db-primary.internal:3306: refused\nsecond line"),
		continuingOp("eu", so.Running),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
	})

	assert.NotContains(t, out, "db-primary.internal", "internal endpoints are redacted")
	assert.Contains(t, out, "> ❌ **First failure:** <code>us</code> — dial tcp [endpoint redacted]: refused second line\n",
		"the first-failure line stays on one line")
}

// renderTargets renders model's progress comment with one detail per member.
func renderTargets(model presentation.Apply, details ...*ApplyStatusCommentData) string {
	return RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{Model: model, ApplyID: "apply-123", Environment: "production", Details: details})
}

// parallelTarget is one target of a multi-target deployment under a parallel,
// continue-on-failure rollout, where every target copies at once and a failed
// target holds none of the others back.
func parallelTarget(dep, target, st string) presentation.Operation {
	return presentation.Operation{Deployment: dep, Target: target, State: st, Parallel: true, ContinueOnFailure: true}
}

// targetDetail is one target's comment data, running one change on orders.
func targetDetail(database, taskStatus, ddl string, copied int64) *ApplyStatusCommentData {
	return &ApplyStatusCommentData{
		Database: database, State: state.Apply.Running, ApplyID: "apply-123", Environment: "production", Engine: storage.EngineSpirit,
		Tables: []TableProgressData{{TableName: "orders", DDL: ddl, Status: taskStatus, RowsCopied: copied, RowsTotal: 1000, ETASeconds: copied}},
	}
}

const addNote = "ALTER TABLE `orders` ADD COLUMN `note` text"

// A deployment that addresses several targets renders as one section, the way
// the sharded comment renders a keyspace: the <summary> line counts its targets
// by status, each table is one line aggregated across the targets, and the DDL
// they share renders once. A single-target deployment beside it keeps its own
// comment body.
func TestRenderMultiDeploymentApplyComment_RollsUpADeploymentsTargets(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		parallelTarget("primary", "testapp-001", so.Completed),
		parallelTarget("primary", "testapp-002", so.Running),
		parallelTarget("primary", "testapp-003", so.Running),
		parallelTarget("eu-west", "orders-eu", so.Running),
	})
	out := renderTargets(model,
		targetDetail("testapp_001", state.Task.Completed, addNote, 1000),
		targetDetail("testapp_002", state.Task.Running, addNote, 420),
		targetDetail("testapp_003", state.Task.Running, addNote, 180),
		targetDetail("orders_eu", state.Task.Running, addNote, 500),
	)

	assert.Contains(t, out, "**Targets**: 1 completed, 3 running\n")
	assert.Contains(t, out, "- 🔄 `primary` — 1 completed, 2 running (3 targets)\n- 🔄 `eu-west` — running table copy\n")
	assert.Contains(t, out, "<details open>\n<summary>🔄 primary — 1 completed, 2 running (3 targets)</summary>\n<dl><dd>\n\n")
	assert.Contains(t, out, "**`orders`**: "+ui.ProgressBarRowCopy(53)+" 53% · 1 complete, 2 running\n- Rows: 1,600 / 3,000 · ETA: "+ui.FormatETA(420)+"\n- Running: `testapp-002`, `testapp-003`\n")

	// The shared DDL renders once for primary's three targets and once in
	// eu-west's own body; no target gets a body of its own.
	assert.Equal(t, 2, strings.Count(out, "ADD COLUMN `note`"))
	assert.NotContains(t, out, "`testapp_00")
	assert.Contains(t, out, "**Database**: `orders_eu`")
}

// Targets that run different changes are split by change, each group named
// above its own table lines and DDL, the way the plan comment splits them. A
// target whose detail has not arrived joins no group: its missing tables are
// not a different change.
func TestRenderMultiDeploymentApplyComment_RolledUpTargetsDivergeByChange(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		parallelTarget("primary", "testapp-001", so.Running),
		parallelTarget("primary", "testapp-002", so.Running),
		parallelTarget("primary", "testapp-003", so.Running),
		parallelTarget("primary", "testapp-004", so.Pending),
	})
	out := renderTargets(model,
		targetDetail("testapp_001", state.Task.Running, addNote, 500),
		targetDetail("testapp_002", state.Task.Running, "ALTER TABLE `orders` ADD INDEX `idx_note`(`note`)", 500),
		targetDetail("testapp_003", state.Task.Running, addNote, 500),
		nil,
	)

	assert.Contains(t, out, "#### 2 of 4 targets\n\n`testapp-001`, `testapp-003`\n\n**`orders`**: ")
	assert.Contains(t, out, "#### Target `testapp-002`\n\n**`orders`**: ")
	assert.Equal(t, 1, strings.Count(out, "ADD COLUMN `note`"))
	assert.Equal(t, 1, strings.Count(out, "ADD INDEX `idx_note`"))
	assert.NotContains(t, out, "testapp-004`**", "a target without detail is not a change of its own")
}

// Rolled-up targets that run different plans each point their cut DDL at the
// stored plan of the group's first target, and a group of several says the
// rest of the group runs the same DDL, the way the plan comment's target
// groups do.
func TestRenderMultiDeploymentApplyComment_RolledUpCutDDLNamesEachGroupsStoredPlan(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		parallelTarget("primary", "testapp-001", so.Running),
		parallelTarget("primary", "testapp-002", so.Running),
		parallelTarget("primary", "testapp-003", so.Running),
	})
	longDDL := func(column string) string {
		return "ALTER TABLE `orders` ADD COLUMN `" + column + "` text" + strings.Repeat(", ADD COLUMN `"+column+"_x` text", 2000)
	}
	withPlan := func(detail *ApplyStatusCommentData, planID string) *ApplyStatusCommentData {
		detail.PlanID, detail.CLIName = planID, "acme schemabot"
		return detail
	}
	out := renderTargets(model,
		withPlan(targetDetail("testapp_001", state.Task.Running, longDDL("note"), 500), "plan_001"),
		withPlan(targetDetail("testapp_002", state.Task.Running, longDDL("memo"), 500), "plan_002"),
		withPlan(targetDetail("testapp_003", state.Task.Running, longDDL("note"), 500), "plan_003"),
	)

	assert.LessOrEqual(t, len(out), commentBodyLimit-applyCommentAppendReserve)
	assert.Contains(t, out, "the full plan for `testapp-001` is available from the CLI with `acme schemabot list-plans -e production plan_001` (every target in this group runs the same DDL).")
	assert.Contains(t, out, "the full plan for this target is available from the CLI with `acme schemabot list-plans -e production plan_002`.")
	assert.NotContains(t, out, "plan_003", "a group names only its first target's plan")
}

// Rolled-up targets that all report one change render with no group heading,
// so a cut block's pointer names the target whose stored plan it is and says
// the other targets run the same DDL only of the targets that have reported:
// a target that has not reported is not known to run it.
func TestRenderMultiDeploymentApplyComment_SoleGroupCutDDLSpeaksOnlyForReportingTargets(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		parallelTarget("primary", "testapp-001", so.Running),
		parallelTarget("primary", "testapp-002", so.Running),
		parallelTarget("primary", "testapp-003", so.Running),
	})
	longDDL := "ALTER TABLE `orders` ADD COLUMN `note` text" + strings.Repeat(", ADD COLUMN `note_x` text", 2000)
	reporting := func(database, planID string) *ApplyStatusCommentData {
		detail := targetDetail(database, state.Task.Running, longDDL, 500)
		detail.PlanID, detail.CLIName = planID, "acme schemabot"
		return detail
	}
	silent := func(database string) *ApplyStatusCommentData {
		return &ApplyStatusCommentData{Database: database, State: state.Apply.Running, ApplyID: "apply-123", Environment: "production", Engine: storage.EngineSpirit}
	}
	const pointer = "_DDL truncated to fit GitHub's comment size limit; the full plan for `testapp-001` is available from the CLI with `acme schemabot list-plans -e production plan_001`"

	t.Run("every target reports", func(t *testing.T) {
		out := renderTargets(model, reporting("testapp_001", "plan_001"), reporting("testapp_002", "plan_002"), reporting("testapp_003", "plan_003"))

		assert.Contains(t, out, pointer+" (every target runs the same DDL)._\n")
	})

	t.Run("a silent target is not claimed", func(t *testing.T) {
		out := renderTargets(model, reporting("testapp_001", "plan_001"), reporting("testapp_002", "plan_002"), silent("testapp_003"))

		assert.Contains(t, out, pointer+" (every target that has reported runs the same DDL)._\n")
		assert.NotContains(t, out, "every target runs the same DDL")
		assert.NotContains(t, out, "every target in this group")
	})

	t.Run("one reporting target is named and speaks for no other", func(t *testing.T) {
		out := renderTargets(model, reporting("testapp_001", "plan_001"), silent("testapp_002"), silent("testapp_003"))

		assert.Contains(t, out, pointer+"._\n")
		assert.NotContains(t, out, "for this target", "no heading names the target, so the pointer does")
		assert.NotContains(t, out, "runs the same DDL")
	})
}

// A failed target is named with its error in a status table, the way the
// sharded comment names a failed shard. The table is capped so a deployment
// whose every target failed still fits in one comment; the <summary> counts
// carry the total.
func TestRenderMultiDeploymentApplyComment_RolledUpFailedTargetsAreNamed(t *testing.T) {
	var ops []presentation.Operation
	var details []*ApplyStatusCommentData
	for i := range failedTargetRowLimit + 5 {
		target := fmt.Sprintf("testapp-%03d", i)
		ops = append(ops, presentation.Operation{Deployment: "primary", Target: target, State: so.Failed, Parallel: true, ContinueOnFailure: true, Error: "Error 1062: Duplicate entry | for key"})
		details = append(details, targetDetail(target, state.Task.Failed, addNote, 0))
	}
	out := renderTargets(presentation.Derive(ops), details...)

	assert.Contains(t, out, "\n❌ Rolled out to no targets, 25 failed\n")
	assert.Contains(t, out, "\n| Target | Status |\n| --- | --- |\n| `testapp-000` | ❌ failed — Error 1062: Duplicate entry / for key |\n")
	assert.Equal(t, failedTargetRowLimit, strings.Count(out, "| ❌ failed"))
	assert.Contains(t, out, "\n…and 5 more failed targets.\n")
}

// A table copying on several targets shows its planned size summed across
// them, since each target copies its own data. The total is left off when any
// target has no estimate or has reported nothing, rather than understating it.
// A failed target's rows are left out of the sum, but its data is still part
// of the table, so beside the partial rows the size names every target.
func TestRenderMultiDeploymentApplyComment_RolledUpTableSizeTotalsTheTargets(t *testing.T) {
	sized := func(detail *ApplyStatusCommentData, bytes int64) *ApplyStatusCommentData {
		detail.Tables[0].EstimatedBytes = &bytes
		return detail
	}
	ops := []presentation.Operation{
		parallelTarget("primary", "testapp-001", so.Running),
		parallelTarget("primary", "testapp-002", so.Running),
	}

	out := renderTargets(presentation.Derive(ops),
		sized(targetDetail("testapp_001", state.Task.Running, addNote, 500), 1_500_000_000),
		sized(targetDetail("testapp_002", state.Task.Running, addNote, 250), 2_000_000_000),
	)
	assert.Contains(t, out, "- Rows: 750 / 2,000 · ~3.5 GB · ETA: "+ui.FormatETA(500)+"\n")

	out = renderTargets(presentation.Derive(ops),
		sized(targetDetail("testapp_001", state.Task.Running, addNote, 500), 1_500_000_000),
		targetDetail("testapp_002", state.Task.Running, addNote, 250),
	)
	assert.Contains(t, out, "- Rows: 750 / 2,000 · ETA: "+ui.FormatETA(500)+"\n")

	withFailed := append(slices.Clone(ops), parallelTarget("primary", "testapp-003", so.Failed))
	out = renderTargets(presentation.Derive(withFailed),
		sized(targetDetail("testapp_001", state.Task.Running, addNote, 500), 1_500_000_000),
		sized(targetDetail("testapp_002", state.Task.Running, addNote, 250), 1_500_000_000),
		sized(targetDetail("testapp_003", state.Task.Failed, addNote, 0), 2_000_000_000),
	)
	assert.Contains(t, out, "- Rows: 750 / 2,000 across 2 of 3 targets · ~5.0 GB across all 3 targets · ETA: "+ui.FormatETA(500)+"\n")
}

func TestTargetsTableBytes(t *testing.T) {
	one, two := int64(1_500_000_000), int64(2_000_000_000)
	cells := []TableProgressData{{EstimatedBytes: &one}, {EstimatedBytes: &two}}

	total := targetsTableBytes(cells, 0)
	require.NotNil(t, total)
	assert.Equal(t, int64(3_500_000_000), *total)
	assert.Nil(t, targetsTableBytes(cells, 1), "a silent target has no estimate to add")
	assert.Nil(t, targetsTableBytes([]TableProgressData{{EstimatedBytes: &one}, {}}, 0), "a target without an estimate leaves the total unknown")
	assert.Nil(t, targetsTableBytes(nil, 0))
}

// One failed target among several still copying does not hide their progress:
// the table keeps its bar from the targets that are copying or done, leaves the
// failed target's rows out of it, and counts the failure beside the coverage.
func TestRenderMultiDeploymentApplyComment_RolledUpFailureKeepsTheOthersProgress(t *testing.T) {
	ops := []presentation.Operation{
		parallelTarget("primary", "testapp-001", so.Completed),
		parallelTarget("primary", "testapp-002", so.Running),
		parallelTarget("primary", "testapp-003", so.Failed),
	}
	out := renderTargets(presentation.Derive(ops),
		targetDetail("testapp_001", state.Task.Completed, addNote, 1000),
		targetDetail("testapp_002", state.Task.Running, addNote, 500),
		targetDetail("testapp_003", state.Task.Failed, addNote, 900),
	)

	assert.Contains(t, out, "**`orders`**: "+ui.ProgressBarRowCopy(75)+" 75% · 1 complete, 1 running, 1 failed\n- Rows: 1,500 / 2,000 across 2 of 3 targets · ETA: "+ui.FormatETA(500)+"\n- Running: `testapp-002`\n")
	assert.Contains(t, out, "| `testapp-003` | ❌ failed |")

	// A target that has yet to report its rows can only add time, so while one
	// is queued the ETA is a floor. The failed target never adds any.
	queued := targetDetail("testapp_004", state.Task.Pending, addNote, 0)
	queued.Tables[0].RowsTotal = 0
	out = renderTargets(presentation.Derive(append(ops, parallelTarget("primary", "testapp-004", so.Pending))),
		targetDetail("testapp_001", state.Task.Completed, addNote, 1000),
		targetDetail("testapp_002", state.Task.Running, addNote, 500),
		targetDetail("testapp_003", state.Task.Failed, addNote, 900),
		queued,
	)
	assert.Contains(t, out, "across 2 of 4 targets · ETA: ≥ "+ui.FormatETA(500)+"\n")

	// A target with no detail at all is still one of the deployment's targets:
	// the rows cover fewer than all of them and the ETA is a floor.
	out = renderTargets(presentation.Derive(append(ops, parallelTarget("primary", "testapp-004", so.Pending))),
		targetDetail("testapp_001", state.Task.Completed, addNote, 1000),
		targetDetail("testapp_002", state.Task.Running, addNote, 500),
		targetDetail("testapp_003", state.Task.Failed, addNote, 900),
		nil,
	)
	assert.Contains(t, out, "across 2 of 4 targets · ETA: ≥ "+ui.FormatETA(500)+"\n")
}

// A target retrying its table on its own is not a failure: the table's line
// counts it as retrying, the word the target's own status uses.
func TestRenderMultiDeploymentApplyComment_RolledUpRetryingTargetIsNotFailed(t *testing.T) {
	out := renderTargets(presentation.Derive([]presentation.Operation{
		parallelTarget("primary", "testapp-001", so.Running),
		parallelTarget("primary", "testapp-002", so.FailedRetryable),
	}),
		targetDetail("testapp_001", state.Task.Running, addNote, 500),
		targetDetail("testapp_002", state.Task.FailedRetryable, addNote, 900),
	)

	assert.Contains(t, out, "**`orders`**: "+ui.ProgressBarRowCopy(50)+" 50% · 1 running, 1 retrying\n")
	assert.NotContains(t, out, "1 failed")
}

// A target that completed a table with no rows to copy, such as an empty
// table, has reported: the line covers every target and its ETA is not a floor.
func TestRenderMultiDeploymentApplyComment_RolledUpCompletedEmptyTableHasReported(t *testing.T) {
	empty := targetDetail("testapp_001", state.Task.Completed, addNote, 0)
	empty.Tables[0].RowsTotal = 0
	out := renderTargets(presentation.Derive([]presentation.Operation{
		parallelTarget("primary", "testapp-001", so.Completed),
		parallelTarget("primary", "testapp-002", so.Running),
	}),
		empty,
		targetDetail("testapp_002", state.Task.Running, addNote, 500),
	)

	assert.Contains(t, out, "- Rows: 500 / 1,000 · ETA: "+ui.FormatETA(500)+"\n")
	assert.NotContains(t, out, "across")
}

// When a deployment's targets run different changes, a target that has not
// reported is not known to run either one, so each change's line counts only
// its own targets.
func TestRenderMultiDeploymentApplyComment_DivergedTargetsCountOnlyTheirOwnTargets(t *testing.T) {
	const addIndex = "ALTER TABLE `orders` ADD INDEX `idx_note`(`note`)"
	out := renderTargets(presentation.Derive([]presentation.Operation{
		parallelTarget("primary", "testapp-001", so.Running),
		parallelTarget("primary", "testapp-002", so.Running),
		parallelTarget("primary", "testapp-003", so.Running),
		parallelTarget("primary", "testapp-004", so.Pending),
	}),
		targetDetail("testapp_001", state.Task.Running, addNote, 400),
		targetDetail("testapp_002", state.Task.Running, addNote, 600),
		targetDetail("testapp_003", state.Task.Running, addIndex, 300),
		nil,
	)

	assert.Contains(t, out, "- Rows: 1,000 / 2,000 · ETA: "+ui.FormatETA(600)+"\n")
	assert.Contains(t, out, "- Rows: 300 / 1,000 · ETA: "+ui.FormatETA(300)+"\n")
	assert.NotContains(t, out, "across")
}

// A table changed by two statements on each target gets a line per statement,
// each aggregating only that statement's progress.
func TestRenderMultiDeploymentApplyComment_RolledUpTableWithTwoStatements(t *testing.T) {
	const addIndex = "ALTER TABLE `orders` ADD INDEX `idx_note`(`note`)"
	twoStatements := func(database string, noteCopied, indexCopied int64) *ApplyStatusCommentData {
		d := targetDetail(database, state.Task.Running, addNote, noteCopied)
		d.Tables = append(d.Tables, TableProgressData{TableName: "orders", DDL: addIndex, Status: state.Task.Running, RowsCopied: indexCopied, RowsTotal: 1000, ETASeconds: indexCopied})
		return d
	}
	out := renderTargets(presentation.Derive([]presentation.Operation{
		parallelTarget("primary", "testapp-001", so.Running),
		parallelTarget("primary", "testapp-002", so.Running),
	}),
		twoStatements("testapp_001", 800, 100),
		twoStatements("testapp_002", 600, 300),
	)

	assert.Contains(t, out, "**`orders`**: "+ui.ProgressBarRowCopy(70)+" 70% · 2 running\n- Rows: 1,400 / 2,000 · ETA: "+ui.FormatETA(800)+"\n")
	assert.Contains(t, out, "**`orders`**: "+ui.ProgressBarRowCopy(20)+" 20% · 2 running\n- Rows: 400 / 2,000 · ETA: "+ui.FormatETA(300)+"\n")
}

// A deployment can address hundreds of targets. The comment then grows with
// the distinct changes and the failures, not with the targets, so a rollout of
// two deployments with 256 targets each renders whole, well inside the room
// GitHub gives one comment, rather than being replaced by the notice for a
// comment too large to post.
func TestRenderMultiDeploymentApplyComment_ManyTargetsFitOneComment(t *testing.T) {
	var ops []presentation.Operation
	var details []*ApplyStatusCommentData
	for _, dep := range []string{"us", "eu"} {
		for i := range 256 {
			target := fmt.Sprintf("testapp_%03d", i)
			ops = append(ops, parallelTarget(dep, target, so.Running))
			details = append(details, targetDetail(target, state.Task.Running, addNote, int64(i)))
		}
	}
	out := renderTargets(presentation.Derive(ops), details...)

	assert.Less(t, len(out), commentBodyLimit-applyCommentAppendReserve)
	assert.Equal(t, 2, strings.Count(out, "<details"), "one section per deployment")
	assert.Equal(t, 2, strings.Count(out, "ADD COLUMN `note`"), "each deployment's change renders once")
	assert.Contains(t, out, "<summary>🔄 us — 256 running (256 targets)</summary>")
	assert.NotContains(t, out, "- Running:", "with every target running, the count already says which")
}

// deferredCutoverDetails is the member detail of an apply started with
// --defer-cutover. The first member with detail speaks for the whole apply.
func deferredCutoverDetails() []*ApplyStatusCommentData {
	return []*ApplyStatusCommentData{{ApplyID: "apply-123", Environment: "production", State: state.Apply.WaitingForCutover, DeferCutover: true}}
}

// An apply started with --defer-cutover waits for an operator at each cutover,
// so the next action offers the command for the member whose turn it is. The
// command is the executable apply-ID form the CLI accepts today (no --deployment
// flag yet).
func TestRenderMultiDeploymentApplyComment_DeferredCutoverOffersCommand(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		barrierOp("eu", so.WaitingForCutover),
		barrierOp("us", so.Running),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
		Details:     deferredCutoverDetails(),
	})

	assert.Contains(t, out, "To cut over `eu`:\n```\nschemabot cutover apply-123 -e production\n```")
	assert.NotContains(t, out, "--deployment")
	assert.NotContains(t, out, "no action needed")
}

// A cutover suggestion names the member it applies to, so an operator reading it
// on a deployment with several targets knows which one is parked at the barrier.
func TestRenderMultiDeploymentApplyComment_NextActionNamesMultiTargetMember(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		{Deployment: "primary", Target: "testapp-001", State: so.Completed, Barrier: true},
		{Deployment: "primary", Target: "testapp-002", State: so.WaitingForCutover, Barrier: true},
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
		Details:     deferredCutoverDetails(),
	})

	assert.Contains(t, out, "To cut over `primary/testapp-002`:")
}

// A member name is assembled from server config, so it reaches the comment as
// text SchemaBot did not choose. The summary list is plain markdown and the
// section header is HTML, and a name has to be unable to write structure of its
// own into either: an operator reads this comment to decide whether to cut over
// or cancel a change that is already touching a database.
func TestRenderMultiDeploymentApplyComment_HostileMemberNamesCannotWriteMarkdown(t *testing.T) {
	hostile := "us`\n## Injected [click](https://example.invalid)"
	model := presentation.Derive([]presentation.Operation{
		rollingOp("eu", so.Running),
		rollingOp(hostile, so.Running),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
	})

	// Flattening runs at both surfaces, so no name reaches the start of a line:
	// it can neither open a heading nor break the list item or the <summary> tag
	// it sits in.
	assert.NotContains(t, out, "\n## Injected", "a name must not start a heading of its own")

	// The summary list is markdown, so the name is fenced in a backtick run
	// longer than any run it carries — inside the span the heading marker and
	// the link are text, not structure.
	assert.Contains(t, out, "- 🔄 `` us` ## Injected [click](https://example.invalid) `` — running table copy")

	// The section header is read as HTML, so the name is escaped rather than
	// fenced, matching how this comment names a member everywhere else in a tag.
	assert.Contains(t, out, "<summary>🔄 us` ## Injected [click](https://example.invalid) — running table copy</summary>")

	// The next-action line names a member in markdown prose too, and is the line
	// an operator reads to decide which member to cut over.
	cutover := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model: presentation.Derive([]presentation.Operation{
			{Deployment: hostile, State: so.WaitingForCutover, Barrier: true},
		}),
		ApplyID:     "apply-123",
		Environment: "production",
		Details:     deferredCutoverDetails(),
	})
	assert.NotContains(t, cutover, "\n## Injected")
	assert.Contains(t, cutover, "To cut over `` us` ## Injected [click](https://example.invalid) ``:")

	// The automatic form names the member in the same prose position.
	automatic := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model: presentation.Derive([]presentation.Operation{
			{Deployment: hostile, State: so.WaitingForCutover, Barrier: true},
		}),
		ApplyID:     "apply-123",
		Environment: "production",
	})
	assert.NotContains(t, automatic, "\n## Injected")
	assert.Contains(t, automatic, "SchemaBot will cut over `` us` ## Injected [click](https://example.invalid) `` next — no action needed.")
}

// Every control command addresses the whole apply, so a rollout writes its
// commands once, in one footer at the bottom of the comment, and no member's
// section carries one: a command under one member's name would read as acting
// on that member alone. This holds for every state the members can be in, on
// every engine, in the progress comment and the terminal summary.
func TestRenderMultiDeploymentApplyComment_WritesItsOneCommandAtTheBottom(t *testing.T) {
	for _, field := range reflect.ValueOf(state.Apply).Fields() {
		applyState := field.String()
		for _, engine := range []string{storage.EngineSpirit, storage.EnginePlanetScale} {
			member := func(database string) *ApplyStatusCommentData {
				return &ApplyStatusCommentData{Database: database, State: applyState, ApplyID: "apply-123", Environment: "production", Engine: engine, DeferCutover: true}
			}
			data := MultiDeploymentApplyData{
				Model:       presentation.Derive([]presentation.Operation{rollingOp("us", applyState), rollingOp("eu", applyState)}),
				ApplyID:     "apply-123",
				Environment: "production",
				Details:     []*ApplyStatusCommentData{member("orders_us"), member("orders_eu")},
			}
			renders := map[string]string{
				"status":  RenderMultiDeploymentApplyComment(data),
				"summary": RenderMultiDeploymentApplySummaryComment(data),
			}
			for surface, out := range renders {
				t.Run(applyState+"/"+engine+"/"+surface, func(t *testing.T) {
					lastSection := strings.LastIndex(out, "</details>")
					require.GreaterOrEqual(t, lastSection, 0, out)
					assert.NotContains(t, out[:lastSection], "---\n", "a member section carries a footer:\n%s", out)
					assert.LessOrEqual(t, strings.Count(out, "\n---\n"), 1, "the rollout writes one footer:\n%s", out)
				})
			}
		}
	}
}

// A running rollout's footer is the stop command, and cancel on an engine whose
// control command is cancel.
func TestRenderMultiDeploymentApplyComment_RunningRolloutFooterStopsIt(t *testing.T) {
	for engine, want := range map[string]string{
		storage.EngineSpirit:      "To stop this schema change:\n```\nschemabot stop apply-123 -e production\n```\n",
		storage.EnginePlanetScale: "To cancel this schema change:\n```\nschemabot cancel apply-123 -e production\n```\n",
	} {
		t.Run(engine, func(t *testing.T) {
			out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
				Model:       presentation.Derive([]presentation.Operation{rollingOp("us", so.Running), rollingOp("eu", so.Pending)}),
				ApplyID:     "apply-123",
				Environment: "production",
				Details: []*ApplyStatusCommentData{
					{Database: "orders_us", State: state.Apply.Running, ApplyID: "apply-123", Environment: "production", Engine: engine},
					{Database: "orders_eu", State: state.Apply.Pending, ApplyID: "apply-123", Environment: "production", Engine: engine},
				},
			})

			footer := out[strings.LastIndex(out, "</details>"):]
			assert.Contains(t, footer, "\n---\n\n"+want)
			assert.Equal(t, 1, strings.Count(out, "schemabot "), "the command renders once:\n%s", out)
		})
	}
}

// A pending rollup action does not take stop away from a member that is still
// writing to its target: a cutover ready beside a sibling still copying and a
// revert window beside a running sibling each lead with their own line and
// then offer stop, in the same footer. A deferred cutover leads with its
// command; an automatic one says SchemaBot will run it.
func TestRenderMultiDeploymentApplyComment_LiveMemberKeepsStopBesideThePendingAction(t *testing.T) {
	const stop = "To stop this schema change:\n```\nschemabot stop apply-123 -e production\n```\n"
	for name, tc := range map[string]struct {
		ops          []presentation.Operation
		deferCutover bool
		lead         string
	}{
		"deferred cutover ready beside a copying sibling": {
			ops:          []presentation.Operation{barrierOp("us", so.WaitingForCutover), barrierOp("eu", so.Running)},
			deferCutover: true,
			lead:         "To cut over `us`:\n```\nschemabot cutover apply-123 -e production\n```\n",
		},
		"automatic cutover ready beside a copying sibling": {
			ops:  []presentation.Operation{barrierOp("us", so.WaitingForCutover), barrierOp("eu", so.Running)},
			lead: "SchemaBot will cut over `us` next — no action needed.\n",
		},
		"revert window beside a running sibling": {
			ops:  []presentation.Operation{rollingOp("us", so.RevertWindow), rollingOp("eu", so.Running)},
			lead: "To revert:\n```\nschemabot revert apply-123 -e production\n```\n",
		},
	} {
		t.Run(name, func(t *testing.T) {
			out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
				Model:       presentation.Derive(tc.ops),
				ApplyID:     "apply-123",
				Environment: "production",
				Details: []*ApplyStatusCommentData{
					{Database: "orders_us", State: tc.ops[0].State, ApplyID: "apply-123", Environment: "production", Engine: storage.EngineSpirit, DeferCutover: tc.deferCutover},
					{Database: "orders_eu", State: tc.ops[1].State, ApplyID: "apply-123", Environment: "production", Engine: storage.EngineSpirit, DeferCutover: tc.deferCutover},
				},
			})

			footer := out[strings.LastIndex(out, "</details>"):]
			assert.Contains(t, footer, tc.lead+"\n"+stop, "the pending action leads and stop follows:\n%s", out)
			assert.Equal(t, 1, strings.Count(out, "schemabot stop "), "stop renders once:\n%s", out)
			assert.Equal(t, 1, strings.Count(out, "\n---\n"), "the commands share one footer:\n%s", out)
		})
	}
}

// A halting failure beside a sibling a driver already started leaves the
// rollout active, and a new apply is refused until it settles: the footer
// offers stop first and says retry opens once the apply finishes or is
// stopped, rather than offering an apply that would be rejected.
func TestRenderMultiDeploymentApplyComment_HaltedWithLiveSiblingOffersStopFirst(t *testing.T) {
	ops := []presentation.Operation{{Deployment: "us", State: so.Failed, Parallel: true}, {Deployment: "eu", State: so.Running, Parallel: true}}
	model := presentation.Derive(ops)
	require.Equal(t, presentation.NextActionReviewFailure, model.NextAction.Kind)
	require.False(t, state.IsTerminalApplyState(model.State), "the sibling keeps the rollout active: %s", model.State)
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
		Details: []*ApplyStatusCommentData{
			{Database: "orders_us", State: state.Apply.Failed, ApplyID: "apply-123", Environment: "production", Engine: storage.EngineSpirit},
			{Database: "orders_eu", State: state.Apply.Running, ApplyID: "apply-123", Environment: "production", Engine: storage.EngineSpirit},
		},
	})

	footer := out[strings.LastIndex(out, "</details>"):]
	assert.Contains(t, footer, "\n---\n\nTo stop this schema change:\n```\nschemabot stop apply-123 -e production\n```\n\n"+presentation.RetryOnceSettledNote+"\n")
	assert.NotContains(t, out, "schemabot apply", "a new apply is refused until this one settles")
	assert.Equal(t, 1, strings.Count(out, "\n---\n"), "the commands share one footer:\n%s", out)
}

// The rollout decides whether stop still has to follow its footer from the
// states the single-deployment footer offers stop in, so the two must agree on
// every apply state; otherwise stop is either lost or written twice.
func TestOffersStopMatchesTheApplyFooter(t *testing.T) {
	for _, field := range reflect.ValueOf(state.Apply).Fields() {
		applyState := field.String()
		t.Run(applyState, func(t *testing.T) {
			var sb strings.Builder
			writeApplyFooter(&sb, ApplyStatusCommentData{State: applyState, ApplyID: "apply-123", Environment: "production", Engine: storage.EngineSpirit})
			assert.Equal(t, strings.Contains(sb.String(), "schemabot stop "), presentation.OffersStop(applyState), sb.String())
		})
	}
}

// A rollout paused after a failure waits for a human to choose between letting
// the held deployments proceed and stopping the apply, so its footer names
// both commands, with the tenant flag the other footers carry.
func TestRenderMultiDeploymentApplyComment_PausedRolloutOffersReleaseAndStop(t *testing.T) {
	pausing := func(dep, st string) presentation.Operation {
		return presentation.Operation{Deployment: dep, State: st, PauseOnFailure: true}
	}
	model := presentation.Derive([]presentation.Operation{pausing("us", so.Failed), pausing("eu", so.Pending)})
	require.Equal(t, state.Apply.Paused, model.State)

	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-123",
		Environment: "production",
		Tenant:      "acme",
		Details: []*ApplyStatusCommentData{
			{Database: "orders_us", State: state.Apply.Failed, ApplyID: "apply-123", Environment: "production", Engine: storage.EngineSpirit},
			{Database: "orders_eu", State: state.Apply.Pending, ApplyID: "apply-123", Environment: "production", Engine: storage.EngineSpirit},
		},
	})

	footer := out[strings.LastIndex(out, "</details>"):]
	assert.Contains(t, footer, "\n---\n\nPaused after a failure — to let the held deployments proceed:\n```\nschemabot release apply-123 -e production --tenant acme\n```\n"+
		"\nTo stop this schema change:\n```\nschemabot stop apply-123 -e production --tenant acme\n```\n", out)
}

// A rollout paused after a failure while another deployment waits for its
// cutover names the cutover, release, and stop together in one footer, so the
// operator reads every choice under a single separator.
func TestRenderMultiDeploymentApplyComment_PausedRolloutWithPendingCutoverSharesOneFooter(t *testing.T) {
	pausing := func(dep, st string) presentation.Operation {
		return presentation.Operation{Deployment: dep, State: st, PauseOnFailure: true, Parallel: true}
	}
	model := presentation.Derive([]presentation.Operation{pausing("us", so.WaitingForCutover), pausing("eu", so.Failed), pausing("ap", so.Pending)})
	require.Equal(t, state.Apply.Paused, model.State)
	require.Equal(t, presentation.NextActionCutover, model.NextAction.Kind)

	for _, tc := range []struct {
		name         string
		deferCutover bool
		cutoverLine  string
	}{
		{name: "deferred", deferCutover: true, cutoverLine: "To cut over `us`:\n```\nschemabot cutover apply-123 -e production\n```\n"},
		{name: "automatic", cutoverLine: "SchemaBot will cut over `us` next — no action needed.\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
				Model:       model,
				ApplyID:     "apply-123",
				Environment: "production",
				Details: []*ApplyStatusCommentData{
					{Database: "orders_us", State: state.Apply.WaitingForCutover, ApplyID: "apply-123", Environment: "production", Engine: storage.EngineSpirit, DeferCutover: tc.deferCutover},
					{Database: "orders_eu", State: state.Apply.Failed, ApplyID: "apply-123", Environment: "production", Engine: storage.EngineSpirit, DeferCutover: tc.deferCutover},
					{Database: "orders_ap", State: state.Apply.Pending, ApplyID: "apply-123", Environment: "production", Engine: storage.EngineSpirit, DeferCutover: tc.deferCutover},
				},
			})

			footer := out[strings.LastIndex(out, "</details>"):]
			assert.Contains(t, footer, "\n---\n\n"+tc.cutoverLine+
				"\nPaused after a failure — to let the held deployments proceed:\n```\nschemabot release apply-123 -e production\n```\n"+
				"\nTo stop this schema change:\n```\nschemabot stop apply-123 -e production\n```\n", out)
			assert.Equal(t, 1, strings.Count(out, "\n---\n"), "the commands share one footer:\n%s", out)
		})
	}
}

// A terminal apply refuses stop, and refuses cancel in every terminal state
// but stopped, so the rollout footer offers neither once the aggregate is
// terminal, even while a member is still writing to its target: a cancelled
// sibling ranks the aggregate cancelled over a sibling still copying. A stopped
// PlanetScale aggregate offers no cancel either, since PlanetScale refuses stop
// and so never reaches that state.
func TestRenderMultiDeploymentApplyComment_TerminalAggregateNeverOffersStop(t *testing.T) {
	ops := []presentation.Operation{rollingOp("us", so.Cancelled), rollingOp("eu", so.Running)}
	require.Equal(t, state.Apply.Cancelled, presentation.Derive(ops).State)

	terminal := 0
	for _, field := range reflect.ValueOf(state.Apply).Fields() {
		aggregate := field.String()
		if !state.IsTerminalApplyState(aggregate) {
			continue
		}
		terminal++
		for _, engine := range []string{storage.EngineSpirit, storage.EnginePlanetScale} {
			t.Run(aggregate+"/"+engine, func(t *testing.T) {
				model := presentation.Derive(ops)
				model.State = aggregate
				out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
					Model:       model,
					ApplyID:     "apply-123",
					Environment: "production",
					Details: []*ApplyStatusCommentData{
						{Database: "orders_us", State: state.Apply.Cancelled, ApplyID: "apply-123", Environment: "production", Engine: engine},
						{Database: "orders_eu", State: state.Apply.Running, ApplyID: "apply-123", Environment: "production", Engine: engine},
					},
				})
				assert.NotContains(t, out, "schemabot stop ", out)
				assert.NotContains(t, out, "schemabot cancel ", out)
			})
		}
	}
	require.Positive(t, terminal, "the apply state registry names terminal states")
}

// A table retrying on any member makes the rollout's one footer the retry
// guidance, not the plain stop command.
func TestRenderMultiDeploymentApplyComment_RetryingMemberTableGetsRetryFooter(t *testing.T) {
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       presentation.Derive([]presentation.Operation{parallelTarget("primary", "testapp-001", so.Running), parallelTarget("primary", "testapp-002", so.Running)}),
		ApplyID:     "apply-123",
		Environment: "production",
		Details: []*ApplyStatusCommentData{
			targetDetail("testapp_001", state.Task.Running, addNote, 500),
			targetDetail("testapp_002", state.Task.FailedRetryable, addNote, 500),
		},
	})

	footerStart := strings.LastIndex(out, "\n---\n")
	require.GreaterOrEqual(t, footerStart, 0, "the rollout has a footer:\n%s", out)
	footer := out[footerStart:]
	assert.Contains(t, footer, "SchemaBot retries automatically and marks it failed if retries are exhausted. To stop retrying:\n```\nschemabot stop apply-123 -e production\n```\n")
}

// When the round produced more than one plan, each member's section names the
// one it runs, beside the database and apply identifiers it already carries, so
// a running member can be tied back to the block that was reviewed.
func TestRenderMultiDeploymentApplyComment_MemberSectionNamesItsPlan(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("primary", so.Running),
		rollingOp("eu-west", so.Pending),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-7f3a",
		Environment: "production",
		Details: []*ApplyStatusCommentData{
			{Database: "orders", ApplyID: "apply-7f3a", State: state.Apply.Running, PlanID: "plan_reviewed"},
			{Database: "orders_eu", ApplyID: "apply-7f3a", State: state.Apply.Pending, PlanID: "plan_3344"},
		},
	})

	assert.Contains(t, out, "**Database**: `orders` | **Plan**: `plan_reviewed`\n")
	assert.Contains(t, out, "**Database**: `orders_eu` | **Plan**: `plan_3344`\n")
	assertRolloutHeaderNotRepeated(t, out)
}

// assertRolloutHeaderNotRepeated checks that the apply ID and who applied it
// appear once, in the rollout header, and not again in each member's section.
func assertRolloutHeaderNotRepeated(t *testing.T, out string) {
	t.Helper()
	assert.Equal(t, 1, strings.Count(out, "**Apply ID**"), "only the rollout header names the apply")
	assert.NotContains(t, out, "_Apply ID:", "no member section names the apply again")
	assert.Equal(t, 1, strings.Count(out, "*Started at")+strings.Count(out, "*Applied by"), "only the rollout header says who applied it")
}

// A rollout whose members all run the same plan names none of them: the
// identifier would be identical under every member and name nothing.
func TestRenderMultiDeploymentApplyComment_ConvergedRolloutNamesNoPlan(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("primary", so.Running),
		rollingOp("eu-west", so.Pending),
	})
	out := RenderMultiDeploymentApplyComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-7f3a",
		Environment: "production",
		Details: []*ApplyStatusCommentData{
			{Database: "orders", ApplyID: "apply-7f3a", State: state.Apply.Running},
			{Database: "orders_eu", ApplyID: "apply-7f3a", State: state.Apply.Pending},
		},
	})

	assert.Contains(t, out, "**Database**: `orders`\n")
	assert.NotContains(t, out, "**Plan**:")
	assertRolloutHeaderNotRepeated(t, out)
}

// The terminal summary names it too, on every member: which plan a failed member
// ran is the first thing triage needs, and for every member it is the record
// that ties the outcome back to a reviewed block.
func TestRenderMultiDeploymentApplySummaryComment_MemberSectionNamesItsPlan(t *testing.T) {
	model := presentation.Derive([]presentation.Operation{
		rollingOp("primary", so.Completed),
		rollingOp("eu-west", so.Failed),
	})
	out := RenderMultiDeploymentApplySummaryComment(MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     "apply-7f3a",
		Environment: "production",
		Details: []*ApplyStatusCommentData{
			{Database: "orders", ApplyID: "apply-7f3a", State: state.Apply.Completed, PlanID: "plan_reviewed"},
			{Database: "orders_eu", ApplyID: "apply-7f3a", State: state.Apply.Failed, PlanID: "plan_3344"},
		},
	})

	assert.Contains(t, out, "**Database**: `orders_eu` | **Plan**: `plan_3344`\n")
	assert.Contains(t, out, "**Database**: `orders` | **Plan**: `plan_reviewed`\n")
	assertRolloutHeaderNotRepeated(t, out)
}

// A target that already held the change is settled completed without a driver
// ever starting it, so it ran nothing and reports no table progress. The
// deployment counts it as already having the change and says so once, and the
// table lines cover only the targets that ran. A target settled without
// starting but not marked as already holding the change, as a reaper settles
// one to its apply's outcome, counts as completed.
func TestRenderMultiDeploymentApplyComment_TargetThatAlreadyHadTheChangeIsCountedApart(t *testing.T) {
	converged := parallelTarget("primary", "testapp-004", so.Completed)
	converged.NeverStarted = true
	converged.AlreadyConverged = true

	t.Run("finished", func(t *testing.T) {
		out := renderTargets(presentation.Derive([]presentation.Operation{
			parallelTarget("primary", "testapp-001", so.Completed),
			parallelTarget("primary", "testapp-002", so.Completed),
			parallelTarget("primary", "testapp-003", so.Completed),
			converged,
		}),
			targetDetail("testapp_001", state.Task.Completed, addNote, 1000),
			targetDetail("testapp_002", state.Task.Completed, addNote, 1000),
			targetDetail("testapp_003", state.Task.Completed, addNote, 1000),
			nil,
		)

		assert.Contains(t, out, "\n✅ Rolled out to 3 of 4 targets (1 already had it)\n")
		assert.NotContains(t, out, "across", "the rows cover every target that ran")
	})

	t.Run("finished with a target that failed before reporting", func(t *testing.T) {
		out := renderTargets(presentation.Derive([]presentation.Operation{
			parallelTarget("primary", "testapp-001", so.Completed),
			parallelTarget("primary", "testapp-002", so.Completed),
			parallelTarget("primary", "testapp-003", so.Failed),
			converged,
		}),
			targetDetail("testapp_001", state.Task.Completed, addNote, 1000),
			targetDetail("testapp_002", state.Task.Completed, addNote, 1000),
			nil,
			nil,
		)

		assert.Contains(t, out, "| `testapp-003` | ❌ failed |")
	})

	t.Run("running", func(t *testing.T) {
		out := renderTargets(presentation.Derive([]presentation.Operation{
			parallelTarget("primary", "testapp-001", so.Completed),
			parallelTarget("primary", "testapp-002", so.Running),
			parallelTarget("primary", "testapp-003", so.Pending),
			converged,
		}),
			targetDetail("testapp_001", state.Task.Completed, addNote, 1000),
			targetDetail("testapp_002", state.Task.Running, addNote, 500),
			nil,
			nil,
		)

		assert.Contains(t, out, "\n🔄 Rolling out: 1 of 4 targets done, 1 running, 1 queued (1 already had it)\n")
	})

	t.Run("every target already had it", func(t *testing.T) {
		ops := make([]presentation.Operation, 0, 4)
		for _, name := range []string{"testapp-001", "testapp-002", "testapp-003", "testapp-004"} {
			op := parallelTarget("primary", name, so.Completed)
			op.NeverStarted = true
			op.AlreadyConverged = true
			ops = append(ops, op)
		}
		out := renderTargets(presentation.Derive(ops), nil, nil, nil, nil)

		assert.Contains(t, out, "\n✅ All 4 targets already had this schema\n")
		assert.NotContains(t, out, "Rolled out", "nothing ran, so the line claims no rollout:\n%s", out)
		assert.NotContains(t, out, "No details available yet", "nothing ran, so no target is still to report details:\n%s", out)
	})

	t.Run("settled without starting", func(t *testing.T) {
		reaped := parallelTarget("primary", "testapp-004", so.Completed)
		reaped.NeverStarted = true
		out := renderTargets(presentation.Derive([]presentation.Operation{
			parallelTarget("primary", "testapp-001", so.Completed),
			parallelTarget("primary", "testapp-002", so.Completed),
			parallelTarget("primary", "testapp-003", so.Completed),
			reaped,
		}),
			targetDetail("testapp_001", state.Task.Completed, addNote, 1000),
			targetDetail("testapp_002", state.Task.Completed, addNote, 1000),
			targetDetail("testapp_003", state.Task.Completed, addNote, 1000),
			nil,
		)

		assert.Contains(t, out, "\n✅ Rolled out to all 4 targets\n")
		assert.NotContains(t, out, "already had", "a target settled without the mark is not one that already had the change:\n%s", out)
	})
}

// An apply whose one deployment addresses several targets has no other
// deployment to tell it apart from, so it states its status once, with no
// counts line, no per-deployment list, and no section wrapping the table
// lines: they sit under the status line and stay visible once the rollout
// finishes. Beside a second deployment, each deployment keeps its section, and
// the one that had targets with the change already says so in its body.
func TestRenderMultiDeploymentApplyComment_SoleMultiTargetDeploymentHasNoWrapper(t *testing.T) {
	converged := parallelTarget("primary", "testapp-003", so.Completed)
	converged.NeverStarted = true
	converged.AlreadyConverged = true
	ops := []presentation.Operation{
		parallelTarget("primary", "testapp-001", so.Completed),
		parallelTarget("primary", "testapp-002", so.Completed),
		converged,
	}
	details := []*ApplyStatusCommentData{
		targetDetail("testapp_001", state.Task.Completed, addNote, 1000),
		targetDetail("testapp_002", state.Task.Completed, addNote, 1000),
		nil,
	}
	data := MultiDeploymentApplyData{Model: presentation.Derive(ops), ApplyID: "apply-123", Environment: "production", Details: details}

	for name, out := range map[string]string{
		"status":  RenderMultiDeploymentApplyComment(data),
		"summary": RenderMultiDeploymentApplySummaryComment(data),
	} {
		t.Run(name, func(t *testing.T) {
			assert.Contains(t, out, "\n✅ Rolled out to 2 of 3 targets (1 already had it)\n\n**`orders`**: ✅ Complete (2 targets)\n")
			assert.Equal(t, 1, strings.Count(out, "already had"), "the status is stated once:\n%s", out)
			assert.NotContains(t, out, "<details")
			assert.NotContains(t, out, "**Targets**:")
			assert.NotContains(t, out, "- ✅ `primary`")
		})
	}

	withSibling := data
	withSibling.Model = presentation.Derive(append(slices.Clone(ops), presentation.Operation{Deployment: "eu", Target: "orders-eu", State: so.Completed, Parallel: true, ContinueOnFailure: true}))
	withSibling.Details = append(slices.Clone(details), targetDetail("orders_eu", state.Task.Completed, addNote, 1000))
	out := RenderMultiDeploymentApplySummaryComment(withSibling)
	assert.Contains(t, out, "**Targets**: 3 completed, 1 already had it\n")
	assert.Contains(t, out, "<summary>✅ primary — 2 completed, 1 already had it (3 targets)</summary>")
	assert.Contains(t, out, "\n_1 of 3 targets already had this schema; nothing ran there._\n")
	assert.NotContains(t, out, "Rolled out to")
}

// A rollback across one deployment's targets states its status as a rollback
// on both the live comment and the summary, and a stopped rollout reads as
// still in progress rather than finished, since its targets run again once it
// resumes.
func TestRenderMultiDeploymentApplyComment_SoleMultiTargetStatusFollowsTheApply(t *testing.T) {
	completed := MultiDeploymentApplyData{
		Model: presentation.Derive([]presentation.Operation{
			parallelTarget("primary", "testapp-001", so.Completed),
			parallelTarget("primary", "testapp-002", so.Completed),
		}),
		ApplyID: "apply-123", Environment: "production", Rollback: true,
		Details: []*ApplyStatusCommentData{
			targetDetail("testapp_001", state.Task.Completed, addNote, 1000),
			targetDetail("testapp_002", state.Task.Completed, addNote, 1000),
		},
	}
	for name, out := range map[string]string{
		"status":  RenderMultiDeploymentApplyComment(completed),
		"summary": RenderMultiDeploymentApplySummaryComment(completed),
	} {
		t.Run("rollback "+name, func(t *testing.T) {
			assert.Contains(t, out, "\n✅ Rolled back on all 2 targets\n")
			assert.NotContains(t, out, "Rolled out")
		})
	}

	stopped := completed
	stopped.Rollback = false
	stopped.Model = presentation.Derive([]presentation.Operation{
		parallelTarget("primary", "testapp-001", so.Completed),
		parallelTarget("primary", "testapp-002", so.Stopped),
	})
	require.Equal(t, state.Apply.Stopped, stopped.Model.State)
	out := RenderMultiDeploymentApplyComment(stopped)
	assert.Contains(t, out, "Rolling out: 1 of 2 targets done, 1 stopped")
	assert.NotContains(t, out, "Rolled out to")
}

// An apply settles as cancelled or reverted while one of its targets is still
// running. The status line follows the target that is still going, in the
// present tense, rather than calling the rollout finished beside a running
// count.
func TestRenderMultiDeploymentApplyComment_SoleMultiTargetStatusWaitsForARunningTarget(t *testing.T) {
	for _, tc := range []struct {
		ended    string
		rollback bool
		want     string
	}{
		{so.Cancelled, false, "Rolling out: 0 of 2 targets done, 1 running, 1 cancelled"},
		{so.Reverted, true, "Rolling back: 0 of 2 targets done, 1 running, 1 reverted"},
	} {
		data := MultiDeploymentApplyData{
			Model: presentation.Derive([]presentation.Operation{
				parallelTarget("primary", "testapp-001", tc.ended),
				parallelTarget("primary", "testapp-002", so.Running),
			}),
			ApplyID: "apply-123", Environment: "production", Rollback: tc.rollback,
			Details: []*ApplyStatusCommentData{nil, targetDetail("testapp_002", state.Task.Running, addNote, 500)},
		}
		require.True(t, state.IsState(data.Model.State, state.SettledApplyStates...), "the apply settled as %s with a target still running", data.Model.State)
		for name, out := range map[string]string{
			"status":  RenderMultiDeploymentApplyComment(data),
			"summary": RenderMultiDeploymentApplySummaryComment(data),
		} {
			t.Run(tc.ended+" "+name, func(t *testing.T) {
				assert.Contains(t, out, tc.want)
				assert.NotContains(t, out, "Rolled out to")
				assert.NotContains(t, out, "Rolled back on")
			})
		}
	}
}

func TestTargetRolloutStatus(t *testing.T) {
	counts := func(pairs ...any) []presentation.StateCount {
		var out []presentation.StateCount
		for i := 0; i < len(pairs); i += 2 {
			out = append(out, presentation.StateCount{Label: pairs[i].(string), Count: pairs[i+1].(int)})
		}
		return out
	}
	for name, tc := range map[string]struct {
		progress presentation.TargetProgress
		settled  bool
		rollback bool
		want     string
	}{
		"running": {
			presentation.TargetProgress{Total: 4, Done: 1, AlreadyHad: 1, Others: counts("running", 1, "queued", 1), Unsettled: 2}, false, false,
			"Rolling out: 1 of 4 targets done, 1 running, 1 queued (1 already had it)",
		},
		"running, none done yet": {
			presentation.TargetProgress{Total: 3, Others: counts("running", 3), Unsettled: 3}, false, false,
			"Rolling out: 0 of 3 targets done, 3 running",
		},
		"every target ran": {
			presentation.TargetProgress{Total: 4, Done: 4}, true, false,
			"Rolled out to all 4 targets",
		},
		"one already had it": {
			presentation.TargetProgress{Total: 4, Done: 3, AlreadyHad: 1}, true, false,
			"Rolled out to 3 of 4 targets (1 already had it)",
		},
		"one failed": {
			presentation.TargetProgress{Total: 4, Done: 2, AlreadyHad: 1, Others: counts("failed", 1)}, true, false,
			"Rolled out to 2 of 4 targets, 1 failed (1 already had it)",
		},
		"every target failed": {
			presentation.TargetProgress{Total: 2, Others: counts("failed", 2)}, true, false,
			"Rolled out to no targets, 2 failed",
		},
		"rollback running": {
			presentation.TargetProgress{Total: 3, Done: 1, Others: counts("running", 2), Unsettled: 2}, false, true,
			"Rolling back: 1 of 3 targets done, 2 running",
		},
		"settled with a target still running": {
			presentation.TargetProgress{Total: 2, Others: counts("running", 1, "cancelled", 1), Unsettled: 1}, true, false,
			"Rolling out: 0 of 2 targets done, 1 running, 1 cancelled",
		},
		"rollback finished": {
			presentation.TargetProgress{Total: 3, Done: 3}, true, true,
			"Rolled back on all 3 targets",
		},
		"rollback with a failure": {
			presentation.TargetProgress{Total: 3, Done: 2, Others: counts("failed", 1)}, true, true,
			"Rolled back on 2 of 3 targets, 1 failed",
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, targetRolloutStatus(tc.progress, tc.settled, tc.rollback))
		})
	}
}
