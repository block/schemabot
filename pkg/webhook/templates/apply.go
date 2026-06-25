package templates

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

// TableProgressData represents progress for a single table in a PR comment.
type TableProgressData struct {
	TaskID          string
	Namespace       string
	TableName       string
	DDL             string
	Status          string // canonical lowercase: "pending", "running", "completed", etc.
	RowsCopied      int64
	RowsTotal       int64
	PercentComplete int
	ETASeconds      int64
	// Checksum phase progress: rows verified so far and total to verify.
	// Non-zero only while the table is checksumming (verifying copied data).
	ChecksumRowsChecked int64
	ChecksumRowsTotal   int64
	IsInstant           bool
	ReadyToComplete     bool

	// ErrorMessage is the task's last error. Rendered for states where the
	// per-table error explains what the user is seeing (e.g. a retrying task).
	ErrorMessage string

	// Shards is the per-shard breakdown for a sharded table. Empty for unsharded
	// engines. Rendered as a single compact summary line
	// while the table is in flight (see renderShardSummary); detailed per-shard
	// row counts/ETAs stay in the CLI, not the PR comment.
	Shards []ShardProgressData
}

// ShardProgressData is the high-level status of one shard, for the compact
// per-shard summary in the PR comment. It intentionally carries only state +
// percent (no row counts/ETA) to keep the comment quiet.
type ShardProgressData struct {
	Shard           string
	Status          string // canonical lowercase shard/task state
	PercentComplete int
}

// ApplyStatusCommentData contains all data needed to render an apply status PR comment.
type ApplyStatusCommentData struct {
	ApplyID      string
	Database     string
	Environment  string
	RequestedBy  string
	State        string // canonical lowercase apply state
	Engine       string
	ErrorMessage string
	StartedAt    string // RFC3339 format
	CompletedAt  string // RFC3339 format
	Tables       []TableProgressData

	// VSchemaChanges holds per-keyspace VSchema application state, surfaced from
	// the engine's display metadata rather than as a per-table task. Empty when
	// the apply carries no VSchema change.
	VSchemaChanges []apitypes.VSchemaChange

	// DeployRequestURL links the PlanetScale deploy request driving this apply
	// (Vitess/PlanetScale only). It is the operator's entry point into the deploy
	// request's own progress, which the comment does not otherwise surface. Empty
	// for engines without a deploy request or before one is created.
	DeployRequestURL string
}

// RenderApplyStatusComment renders a PR comment for the current apply status.
// When Tables is populated, per-table progress bars are shown.
// When Tables is empty, a simple status message is rendered.
func RenderApplyStatusComment(data ApplyStatusCommentData) string {
	return renderApplyStatusComment(data, true, currentTimestamp())
}

func renderApplyStatusComment(data ApplyStatusCommentData, includeLastUpdated bool, renderedAt string) string {
	var sb strings.Builder

	// Header varies by state
	writeApplyHeader(&sb, data)

	// Metadata line
	writeApplyMetadata(&sb, data, renderedAt)
	writeApplyStatusDetail(&sb, data.State)

	// Deploy-request link (PlanetScale) — the operator's entry point into the
	// deploy request's own progress, which the comment does not otherwise surface.
	writeDeployRequestLink(&sb, data)

	// Cutover readiness summary
	if data.State == state.Apply.WaitingForCutover || data.State == state.Apply.CuttingOver {
		writeCutoverSummary(&sb, data.Tables)
	}

	// Per-table progress section
	if len(data.Tables) > 0 {
		writeTableProgressSection(&sb, data)
	}

	// VSchema application status, surfaced from engine metadata rather than as a
	// per-table task (a VSchema-only apply has no tables at all).
	writeVSchemaStatus(&sb, data)

	// Error message for apply states that need operator triage.
	if state.IsState(data.State, state.Apply.Failed, state.Apply.Stopped) && data.ErrorMessage != "" {
		writeErrorBlock(&sb, data.ErrorMessage)
	}

	// Footer with next actions
	writeApplyFooter(&sb, data)
	if includeLastUpdated && !state.IsTerminalApplyState(data.State) {
		writeLastUpdatedFooter(&sb, renderedAt)
	}

	return sb.String()
}

// writeApplyHeader writes the comment header with a state-specific title.
func writeApplyHeader(sb *strings.Builder, data ApplyStatusCommentData) {
	switch data.State {
	case state.Apply.Pending,
		state.Apply.Running,
		state.Apply.RunningDegraded,
		state.Apply.FailedRetryable,
		state.Apply.WaitingForDeploy,
		state.Apply.WaitingForCutover,
		state.Apply.Recovering,
		state.Apply.Resuming,
		state.Apply.CuttingOver,
		state.Apply.PreparingBranch,
		state.Apply.ApplyingBranchChanges,
		state.Apply.ValidatingBranch,
		state.Apply.CreatingDeployRequest,
		state.Apply.ValidatingDeployRequest:
		// running_degraded is a continue rollout still in flight past a failed
		// sibling: the change is still in progress, and the failed deployment is
		// surfaced in the per-deployment breakdown, not the headline.
		writeEnvironmentTitle(sb, "Schema Change In Progress", data.Environment)
	case state.Apply.RevertWindow:
		writeEnvironmentTitle(sb, "Schema Change Applied (Pending Revert)", data.Environment)
	case state.Apply.Completed:
		writeEnvironmentTitle(sb, "✅ Schema Change Applied", data.Environment)
	case state.Apply.Failed:
		writeEnvironmentTitle(sb, "❌ Schema Change Failed", data.Environment)
	case state.Apply.Stopped:
		writeEnvironmentTitle(sb, "⏹️ Schema Change Stopped", data.Environment)
	case state.Apply.Reverted:
		writeEnvironmentTitle(sb, "↩️ Schema Change Reverted", data.Environment)
	case state.Apply.Cancelled:
		writeEnvironmentTitle(sb, "🚫 Schema Change Cancelled", data.Environment)
	default:
		if state.IsTerminalApplyState(data.State) {
			writeEnvironmentTitle(sb, fmt.Sprintf("Schema Change: %s", humanizeState(data.State)), data.Environment)
		} else {
			writeEnvironmentTitle(sb, "Schema Change In Progress", data.Environment)
		}
	}
}

// writeApplyMetadata writes the database, apply ID, and requester info.
func writeApplyMetadata(sb *strings.Builder, data ApplyStatusCommentData, renderedAt string) {
	var parts []string
	parts = append(parts, fmt.Sprintf("**Database**: `%s`", data.Database))
	if data.ApplyID != "" {
		parts = append(parts, fmt.Sprintf("**Apply ID**: `%s`", data.ApplyID))
	}
	fmt.Fprintf(sb, "%s\n", strings.Join(parts, " | "))
	attributionAt := renderedAt
	if data.RequestedBy == "" {
		attributionAt = startedAtDisplay(data.StartedAt, renderedAt)
	}
	writeAppliedByOrTimestampAt(sb, data.RequestedBy, attributionAt)
}

func startedAtDisplay(startedAt, fallback string) string {
	if startedAt == "" {
		return fallback
	}
	t, err := time.Parse(time.RFC3339, startedAt)
	if err != nil {
		return fallback
	}
	return t.UTC().Format("2006-01-02 15:04:05 UTC")
}

func writeApplyStatusDetail(sb *strings.Builder, applyState string) {
	detail := applyStatusDetail(applyState)
	if detail == "" {
		return
	}
	fmt.Fprintf(sb, "\n**Status**: %s\n", detail)
}

func applyStatusDetail(applyState string) string {
	switch applyState {
	case state.Apply.Pending:
		return "Starting"
	case state.Apply.WaitingForDeploy:
		return "Waiting for Deploy"
	case state.Apply.WaitingForCutover:
		return "Waiting for Cutover"
	case state.Apply.Recovering:
		return "Recovering"
	case state.Apply.Resuming:
		return "Resuming"
	case state.Apply.CuttingOver:
		return "Cutting Over"
	case state.Apply.PreparingBranch:
		return "Preparing Branch"
	case state.Apply.ApplyingBranchChanges:
		return "Applying Branch Changes"
	case state.Apply.ValidatingBranch:
		return "Validating Branch"
	case state.Apply.CreatingDeployRequest:
		return "Creating Deploy Request"
	case state.Apply.ValidatingDeployRequest:
		return "Validating Deploy Request"
	default:
		if applyState == "" || state.IsTerminalApplyState(applyState) || state.IsState(applyState, state.Apply.Running, state.Apply.RunningDegraded, state.Apply.FailedRetryable) {
			return ""
		}
		return humanizeState(applyState)
	}
}

// writeDeployRequestLink writes a link to the PlanetScale deploy request driving
// the apply, when one is known. For a Vitess/PlanetScale apply the meaningful
// in-flight work happens in the deploy request, so this gives the operator a way
// to follow it directly rather than hunting for it in the PlanetScale UI.
func writeDeployRequestLink(sb *strings.Builder, data ApplyStatusCommentData) {
	if data.DeployRequestURL == "" {
		return
	}
	fmt.Fprintf(sb, "\n🔗 **Deploy request**: %s\n", data.DeployRequestURL)
}

// writeCutoverSummary writes a readiness summary for cutover states,
// showing how many tables are ready to complete vs still catching up.
func writeCutoverSummary(sb *strings.Builder, tables []TableProgressData) {
	ready := 0
	total := len(tables)
	for _, t := range tables {
		if t.ReadyToComplete {
			ready++
		}
	}
	if total == 0 {
		return
	}
	if ready == total {
		fmt.Fprintf(sb, "\n**%d/%d** table(s) ready for cutover\n", ready, total)
	} else {
		fmt.Fprintf(sb, "\n**%d/%d** table(s) ready for cutover — waiting on %d\n", ready, total, total-ready)
	}
}

// writeProgressSummary writes a one-line progress summary before the per-table breakdown.
// For multi-table applies, shows "X/N complete · Y running (Z%) · ..." at a glance.
// For single-table applies, the summary is skipped — the header and progress bar
// already communicate the state, making the summary line redundant.
func writeProgressSummary(sb *strings.Builder, tables []TableProgressData) {
	total := len(tables)
	if total <= 1 {
		return
	}

	var completed, running, checksumming, queued, failed, retrying, stopped, waiting, recovering, cutting, cancelled int
	var runningPct int
	var runningEstimateExceeded bool

	for _, t := range tables {
		switch state.NormalizeTaskStatus(t.Status) {
		case state.Task.Completed:
			completed++
		case state.Task.Running:
			running++
			if ui.EstimateExceeded(t.RowsCopied, t.RowsTotal) {
				runningEstimateExceeded = true
			} else {
				runningPct = ui.RowCopyDisplayPercent(t.PercentComplete, t.RowsCopied)
			}
		case state.Task.Checksumming:
			checksumming++
		case state.Task.Pending:
			queued++
		case state.Task.WaitingForCutover:
			waiting++
		case state.Task.Recovering:
			recovering++
		case state.Task.CuttingOver:
			cutting++
		case state.Task.Failed:
			failed++
		case state.Task.FailedRetryable:
			retrying++
		case state.Task.Stopped:
			stopped++
		case state.Task.Cancelled:
			cancelled++
		}
	}

	multi := total > 1
	var parts []string

	// For multi-table: "2/3 complete · 1 running (45%) · 1 queued"
	// For single-table: "running (45%)" or "waiting for cutover" (no fractions)
	if completed > 0 && multi {
		parts = append(parts, fmt.Sprintf("%d/%d complete", completed, total))
	}
	if running > 0 {
		label := "running"
		if multi {
			label = fmt.Sprintf("%d running", running)
		}
		if runningEstimateExceeded {
			label += " (Active)"
		} else if runningPct > 0 {
			label += fmt.Sprintf(" (%d%%)", runningPct)
		}
		parts = append(parts, label)
	}
	if checksumming > 0 {
		if multi {
			parts = append(parts, fmt.Sprintf("%d checksumming", checksumming))
		} else {
			parts = append(parts, "checksumming")
		}
	}
	if queued > 0 && multi {
		parts = append(parts, fmt.Sprintf("%d queued", queued))
	}
	if waiting > 0 {
		if multi {
			parts = append(parts, fmt.Sprintf("%d waiting for cutover", waiting))
		} else {
			parts = append(parts, "waiting for cutover")
		}
	}
	if recovering > 0 {
		if multi {
			parts = append(parts, fmt.Sprintf("%d recovering", recovering))
		} else {
			parts = append(parts, "recovering")
		}
	}
	if cutting > 0 {
		if multi {
			parts = append(parts, fmt.Sprintf("%d cutting over", cutting))
		} else {
			parts = append(parts, "cutting over")
		}
	}
	if failed > 0 {
		if multi {
			parts = append(parts, fmt.Sprintf("%d failed", failed))
		} else {
			parts = append(parts, "failed")
		}
	}
	if retrying > 0 {
		if multi {
			parts = append(parts, fmt.Sprintf("%d retrying", retrying))
		} else {
			parts = append(parts, "retrying")
		}
	}
	if stopped > 0 {
		if multi {
			parts = append(parts, fmt.Sprintf("%d stopped", stopped))
		} else {
			parts = append(parts, "stopped")
		}
	}
	if cancelled > 0 && multi {
		parts = append(parts, fmt.Sprintf("%d cancelled", cancelled))
	}

	if len(parts) > 0 {
		fmt.Fprintf(sb, "\n📊 %s\n", strings.Join(parts, " · "))
	}
}

// writeVSchemaStatus renders the VSchema application status and diff surfaced
// from the engine's display metadata. It is shown as its own section because
// VSchema application is not a per-table task. Renders nothing when the apply
// carries no VSchema change.
func writeVSchemaStatus(sb *strings.Builder, data ApplyStatusCommentData) {
	if len(data.VSchemaChanges) == 0 {
		return
	}
	sb.WriteString("\n### VSchema\n\n")
	for _, c := range data.VSchemaChanges {
		fmt.Fprintf(sb, "**`%s`**: %s\n\n", c.Namespace, ui.VSchemaStatusLabel(c.Status))
		if c.Diff != "" {
			fmt.Fprintf(sb, "```diff\n%s\n```\n\n", c.Diff)
		}
	}
}

// writeTableProgressSection writes the per-table progress breakdown.
// Tables are sorted: active/running first, then pending, then completed/terminal last.
func writeTableProgressSection(sb *strings.Builder, data ApplyStatusCommentData) {
	// During the resume window the per-table percents are indeterminate (the data
	// plane has not reported continuation vs fresh copy yet), so the aggregate
	// running-percent summary would surface stale pre-stop numbers. The per-table
	// "Resuming…" lines below convey state without it.
	if data.State != state.Apply.Resuming {
		writeProgressSummary(sb, data.Tables)
	}
	sb.WriteString("\n### Table Progress\n\n")

	sorted := make([]TableProgressData, len(data.Tables))
	copy(sorted, data.Tables)
	sort.SliceStable(sorted, func(i, j int) bool {
		return tableStatePriority(sorted[i].Status) < tableStatePriority(sorted[j].Status)
	})

	for _, table := range sorted {
		// While the apply is resuming, the data plane has not yet reported whether
		// the schema change continues from its checkpoint or restarts from scratch,
		// so the row-copy percent is indeterminate. Render state-only until the
		// apply transitions to running and real progress is known.
		if data.State == state.Apply.Resuming && !state.IsTerminalTaskState(state.NormalizeTaskStatus(table.Status)) {
			renderResumingTable(sb, table)
			continue
		}
		renderTableProgress(sb, table)
	}
}

// renderResumingTable renders a table while the apply is resuming, before the
// data plane reports whether the change continues from its checkpoint or restarts
// from scratch. The percent is intentionally omitted during this window.
func renderResumingTable(sb *strings.Builder, table TableProgressData) {
	fmt.Fprintf(sb, "**`%s`**: \U0001f504 Resuming…\n", table.TableName)
	writeDDLLine(sb, table.DDL)
	sb.WriteString("\n")
}

// tableStatePriority returns a sort key: lower = rendered first (active on top, completed on bottom).
func tableStatePriority(tableStatus string) int {
	return ui.TableStatePriority(state.NormalizeTaskStatus(tableStatus))
}

// renderTableProgress renders a single table's progress as markdown.
// Mirrors the CLI's writeTableProgressWithState logic but outputs markdown instead of ANSI.
func renderTableProgress(sb *strings.Builder, table TableProgressData) {
	// Normalize to canonical Task state for consistent matching.
	status := state.NormalizeTaskStatus(table.Status)

	switch status {
	case state.Task.Pending:
		fmt.Fprintf(sb, "**`%s`**: \u23f3 Queued\n", table.TableName)
		writeDDLLine(sb, table.DDL)

	case state.Task.Completed:
		bar := ui.ProgressBarComplete()
		fmt.Fprintf(sb, "**`%s`**: %s \u2713 Complete\n", table.TableName, bar)
		writeDDLLine(sb, table.DDL)

	case state.Task.Checksumming:
		// Row copy is complete; the engine is verifying the copied data against
		// the source before cutover. On a large table this can run for hours, so
		// show how far the verify has progressed once Spirit reports a total.
		if table.ChecksumRowsTotal > 0 {
			pct := ui.ClampPercent(int(table.ChecksumRowsChecked * 100 / table.ChecksumRowsTotal))
			fmt.Fprintf(sb, "**`%s`**: %s \U0001f50d Checksumming to verify data (%d%%)\n", table.TableName, ui.ProgressBarRowCopy(pct), pct)
			writeDDLLine(sb, table.DDL)
			fmt.Fprintf(sb, "Rows verified: %s / %s\n",
				ui.FormatNumber(ui.ClampRows(table.ChecksumRowsChecked, table.ChecksumRowsTotal)), ui.FormatNumber(table.ChecksumRowsTotal))
		} else {
			fmt.Fprintf(sb, "**`%s`**: %s \U0001f50d Checksumming to verify data...\n", table.TableName, ui.ProgressBarRowCopy(100))
			writeDDLLine(sb, table.DDL)
		}

	case state.Task.WaitingForCutover:
		bar := ui.ProgressBarWaitingCutover()
		if table.ReadyToComplete {
			fmt.Fprintf(sb, "**`%s`**: %s \u2705 Ready for cutover\n", table.TableName, bar)
		} else {
			fmt.Fprintf(sb, "**`%s`**: %s Waiting for cutover\n", table.TableName, bar)
		}
		writeDDLLine(sb, table.DDL)

	case state.Task.Recovering:
		if recoveringIsCopyingRows(table) {
			pct := ui.RowCopyDisplayPercent(table.PercentComplete, table.RowsCopied)
			bar := ui.ProgressBarRowCopy(pct)
			fmt.Fprintf(sb, "**`%s`**: %s Row copy in progress (%d%%)\n", table.TableName, bar, pct)
			writeDDLLine(sb, table.DDL)
			writeRowsAndETA(sb, table)
			break
		}
		bar := ui.ProgressBarWaitingCutover()
		fmt.Fprintf(sb, "**`%s`**: %s Recovering state...\n", table.TableName, bar)
		writeDDLLine(sb, table.DDL)

	case state.Task.CuttingOver:
		bar := ui.ProgressBarWaitingCutover()
		fmt.Fprintf(sb, "**`%s`**: %s \U0001f504 Cutting over...\n", table.TableName, bar)
		writeDDLLine(sb, table.DDL)

	case state.Task.Failed:
		bar := ui.ProgressBarFailed(ui.RowCopyDisplayPercent(table.PercentComplete, table.RowsCopied))
		fmt.Fprintf(sb, "**`%s`**: %s \u274c Failed\n", table.TableName, bar)
		writeDDLLine(sb, table.DDL)

	case state.Task.FailedRetryable:
		bar := ui.ProgressBarStopped(ui.RowCopyDisplayPercent(table.PercentComplete, table.RowsCopied))
		fmt.Fprintf(sb, "**`%s`**: %s \U0001f504 Interrupted — retrying automatically\n", table.TableName, bar)
		writeDDLLine(sb, table.DDL)
		if table.ErrorMessage != "" {
			writeTableErrorLine(sb, table.ErrorMessage)
		}

	case state.Task.Cancelled:
		fmt.Fprintf(sb, "**`%s`**: \u2298 Cancelled (not started)\n", table.TableName)
		writeDDLLine(sb, table.DDL)

	case state.Task.RevertWindow:
		bar := ui.ProgressBarWaitingCutover()
		fmt.Fprintf(sb, "**`%s`**: %s \u2713 Complete (pending revert)\n", table.TableName, bar)
		writeDDLLine(sb, table.DDL)

	case state.Task.Stopped:
		renderStoppedTable(sb, table)

	default:
		// Running / in-progress
		renderRunningTable(sb, table)
	}

	renderShardSummary(sb, table)

	sb.WriteString("\n")
}

// renderShardSummary appends a single compact per-shard status line for a
// sharded table, only while it is in flight. It keeps the PR
// comment quiet: at most one extra line per table. With few shards it lists each
// shard's state (and percent for actively-copying shards); with many it collapses
// to per-state counts plus the slowest copying shard, so even hundreds of shards
// fit on one line. Detailed per-shard rows/ETAs stay in the CLI.
func renderShardSummary(sb *strings.Builder, table TableProgressData) {
	if len(table.Shards) <= 1 {
		return
	}
	switch state.NormalizeTaskStatus(table.Status) {
	case state.Task.Running, state.Task.Checksumming, state.Task.Recovering, state.Task.CuttingOver, state.Task.WaitingForCutover:
		// in flight — a breakdown adds signal
	default:
		return // completed/pending/cancelled/failed: no breakdown, stay quiet
	}

	const inlineLimit = 8
	if len(table.Shards) <= inlineLimit {
		parts := make([]string, 0, len(table.Shards))
		for _, sh := range table.Shards {
			if isCopyingShardStatus(sh.Status) && sh.PercentComplete > 0 {
				parts = append(parts, fmt.Sprintf("%s %s %d%%", shardGlyph(sh.Status), sh.Shard, sh.PercentComplete))
			} else {
				parts = append(parts, fmt.Sprintf("%s %s", shardGlyph(sh.Status), sh.Shard))
			}
		}
		fmt.Fprintf(sb, "  └ shards: %s\n", strings.Join(parts, " · "))
		return
	}

	var complete, copying, ready, failed, queued, other int
	slowestShard := ""
	slowestPct := -1
	for _, sh := range table.Shards {
		switch state.NormalizeShardStatus(sh.Status) {
		case state.Task.Completed:
			complete++
		case state.Task.WaitingForCutover:
			ready++
		case state.Task.Failed, state.Task.FailedRetryable:
			failed++
		case state.Task.Pending:
			queued++
		default:
			if isCopyingShardStatus(sh.Status) {
				copying++
				if slowestPct < 0 || sh.PercentComplete < slowestPct {
					slowestPct = sh.PercentComplete
					slowestShard = sh.Shard
				}
			} else {
				other++
			}
		}
	}
	var buckets []string
	if complete > 0 {
		buckets = append(buckets, fmt.Sprintf("%d ✓", complete))
	}
	if copying > 0 {
		buckets = append(buckets, fmt.Sprintf("%d ◐ copying", copying))
	}
	if ready > 0 {
		buckets = append(buckets, fmt.Sprintf("%d ● ready", ready))
	}
	if queued > 0 {
		buckets = append(buckets, fmt.Sprintf("%d ⏳", queued))
	}
	if failed > 0 {
		buckets = append(buckets, fmt.Sprintf("%d ✗ failed", failed))
	}
	if other > 0 {
		buckets = append(buckets, fmt.Sprintf("%d …", other))
	}
	line := fmt.Sprintf("  └ %d shards: %s", len(table.Shards), strings.Join(buckets, " · "))
	if slowestShard != "" && slowestPct >= 0 {
		line += fmt.Sprintf(" · slowest %s %d%%", slowestShard, slowestPct)
	}
	sb.WriteString(line + "\n")
}

// shardGlyph maps a shard's status to its compact summary glyph.
func shardGlyph(status string) string {
	switch state.NormalizeShardStatus(status) {
	case state.Task.Completed:
		return "✓" // ✓
	case state.Task.WaitingForCutover:
		return "●" // ●
	case state.Task.Failed, state.Task.FailedRetryable:
		return "✗" // ✗
	case state.Task.Pending:
		return "⏳" // ⏳
	default:
		if isCopyingShardStatus(status) {
			return "◐" // ◐
		}
		return "•" // •
	}
}

// isCopyingShardStatus reports whether a shard is actively doing copy/cutover work.
func isCopyingShardStatus(status string) bool {
	switch state.NormalizeShardStatus(status) {
	case state.Task.Running, state.Task.Recovering, state.Task.CuttingOver:
		return true
	default:
		return false
	}
}

// renderRunningTable renders a table that is actively copying rows.
func renderRunningTable(sb *strings.Builder, table TableProgressData) {
	if table.RowsTotal > 0 {
		if ui.EstimateExceeded(table.RowsCopied, table.RowsTotal) {
			fmt.Fprintf(sb, "**`%s`**: %s Active\n", table.TableName, ui.ProgressBarActivity())
			writeDDLLine(sb, table.DDL)
			fmt.Fprintf(sb, "- Rows copied: %s so far\n", ui.FormatNumber(table.RowsCopied))
			fmt.Fprintf(sb, "- ℹ️ _%s_\n", ui.EstimateExceededTooltip)
			return
		}

		pct := ui.RowCopyDisplayPercent(table.PercentComplete, table.RowsCopied)
		bar := ui.ProgressBarRowCopy(pct)
		fmt.Fprintf(sb, "**`%s`**: %s %d%%\n", table.TableName, bar, pct)
		writeDDLLine(sb, table.DDL)
		writeRowsAndETA(sb, table)
	} else {
		// No row data yet (initializing or instant DDL)
		fmt.Fprintf(sb, "**`%s`**: Running...\n", table.TableName)
		writeDDLLine(sb, table.DDL)
	}
}

func recoveringIsCopyingRows(table TableProgressData) bool {
	return table.RowsTotal > 0 && table.PercentComplete < 100
}

func recoveringCopyPercent(tables []TableProgressData) (int, bool) {
	percent := 100
	found := false
	for _, table := range tables {
		if state.NormalizeTaskStatus(table.Status) != state.Task.Recovering || !recoveringIsCopyingRows(table) {
			continue
		}
		percent = min(percent, ui.RowCopyDisplayPercent(table.PercentComplete, table.RowsCopied))
		found = true
	}
	return percent, found
}

// renderStoppedTable renders a table in the stopped state.
func renderStoppedTable(sb *strings.Builder, table TableProgressData) {
	switch {
	case table.PercentComplete >= 100:
		bar := ui.ProgressBarStopped(100)
		fmt.Fprintf(sb, "**`%s`**: %s \u23f9\ufe0f Stopped (was waiting for cutover)\n", table.TableName, bar)
	case table.PercentComplete > 0 || table.RowsCopied > 0:
		pct := ui.RowCopyDisplayPercent(table.PercentComplete, table.RowsCopied)
		bar := ui.ProgressBarStopped(pct)
		fmt.Fprintf(sb, "**`%s`**: %s \u23f9\ufe0f Stopped at %d%%\n", table.TableName, bar, pct)
	default:
		fmt.Fprintf(sb, "**`%s`**: \u23f9\ufe0f Stopped (not started)\n", table.TableName)
	}

	writeDDLLine(sb, table.DDL)

	// Show rows (no ETA) for stopped tables with progress
	if table.RowsTotal > 0 && (table.PercentComplete > 0 || table.RowsCopied > 0) {
		fmt.Fprintf(sb, "Rows: %s / %s\n",
			ui.FormatNumber(ui.ClampRows(table.RowsCopied, table.RowsTotal)),
			ui.FormatNumber(table.RowsTotal))
	}
}

// writeDDLLine writes the DDL statement as a sql code block below the table name.
func writeDDLLine(sb *strings.Builder, rawDDL string) {
	if rawDDL != "" {
		fmt.Fprintf(sb, "\n```sql\n%s\n```\n", ddl.FormatDDL(rawDDL))
	}
}

// writeRowsAndETA writes the rows copied / total line with optional ETA.
func writeRowsAndETA(sb *strings.Builder, table TableProgressData) {
	if table.RowsTotal <= 0 {
		return
	}
	copied := ui.ClampRows(table.RowsCopied, table.RowsTotal)
	if table.ETASeconds > 0 {
		fmt.Fprintf(sb, "Rows: %s / %s \u00b7 ETA: %s\n",
			ui.FormatNumber(copied),
			ui.FormatNumber(table.RowsTotal),
			ui.FormatETA(table.ETASeconds))
	} else {
		fmt.Fprintf(sb, "Rows: %s / %s\n",
			ui.FormatNumber(copied),
			ui.FormatNumber(table.RowsTotal))
	}
}

// writeApplyFooter writes a state-specific footer with the next operator action.
// Most actionable states render a "<label>:" line plus a command. Terminal states
// with no recovery command (a cancelled change cannot be resumed) instead render
// explanatory guidance pointing at the right next step.
func writeApplyFooter(sb *strings.Builder, data ApplyStatusCommentData) {
	switch data.State {
	case state.Apply.WaitingForDeploy:
		writeFooterAction(sb, "To deploy:", fmt.Sprintf("schemabot cutover %s -e %s", data.ApplyID, data.Environment))
	case state.Apply.WaitingForCutover:
		writeFooterAction(sb, "To proceed with cutover:", fmt.Sprintf("schemabot cutover %s -e %s", data.ApplyID, data.Environment))
	case state.Apply.Recovering:
		sb.WriteString("\n---\n\n")
		if pct, ok := recoveringCopyPercent(data.Tables); ok {
			fmt.Fprintf(sb, "Recovering after restart. Row copy is in progress (%d%%); once recovery completes, progress returns to the normal row-copy view.\n", pct)
		} else {
			sb.WriteString("Recovering after restart. Cutover will be available once recovery completes.\n")
		}
	case state.Apply.CuttingOver:
		sb.WriteString("\n---\n\n")
		sb.WriteString("Cutover in progress — typically completes within seconds.\n")
	case state.Apply.Running, state.Apply.RunningDegraded:
		writeFooterAction(sb, "To stop this schema change:", fmt.Sprintf("schemabot stop %s -e %s", data.ApplyID, data.Environment))
	case state.Apply.PreparingBranch,
		state.Apply.ApplyingBranchChanges,
		state.Apply.ValidatingBranch,
		state.Apply.CreatingDeployRequest,
		state.Apply.ValidatingDeployRequest:
		// PlanetScale's branch and deploy-request phases are active, stoppable
		// work — the operator can halt the change before cutover just as during
		// row copy.
		writeFooterAction(sb, "To stop this schema change:", fmt.Sprintf("schemabot stop %s -e %s", data.ApplyID, data.Environment))
	case state.Apply.FailedRetryable:
		writeFooterAction(sb, "An error interrupted this schema change. SchemaBot retries automatically and marks it failed if retries are exhausted. To stop retrying:", fmt.Sprintf("schemabot stop %s -e %s", data.ApplyID, data.Environment))
	case state.Apply.Stopped:
		writeFooterAction(sb, "Paused — to resume from where it stopped:", fmt.Sprintf("schemabot start %s -e %s", data.ApplyID, data.Environment))
	case state.Apply.Cancelled:
		sb.WriteString("\n---\n\n")
		sb.WriteString("This schema change was cancelled and cannot be resumed. Open a new schema change to apply it again.\n")
	case state.Apply.Failed:
		writeFooterAction(sb, "To retry:", fmt.Sprintf("schemabot apply -e %s", data.Environment))
	case state.Apply.RevertWindow:
		writeFooterAction(sb, "To revert:", fmt.Sprintf("schemabot revert %s -e %s", data.ApplyID, data.Environment))
		fmt.Fprintf(sb, "\nTo skip revert and keep changes:\n```\nschemabot skip-revert %s -e %s\n```\n", data.ApplyID, data.Environment)
	}
}

// writeFooterAction writes a --- separator followed by an action label and command.
func writeFooterAction(sb *strings.Builder, label, command string) {
	sb.WriteString("\n---\n\n")
	fmt.Fprintf(sb, "%s\n```\n%s\n```\n", label, command)
}

// RenderApplySummaryComment renders a final summary comment for a terminal apply state.
// This is posted as a new comment separate from the progress comment, providing a
// concise outcome record with apply ID and table results.
func RenderApplySummaryComment(data ApplyStatusCommentData) string {
	var sb strings.Builder

	completedCount, failedCount := countTableOutcomes(data.Tables)
	totalTables := len(data.Tables)

	switch data.State {
	case state.Apply.Completed:
		writeSummaryCompleted(&sb, data, totalTables)
	case state.Apply.Failed:
		writeSummaryFailed(&sb, data, completedCount, failedCount, totalTables)
	case state.Apply.Stopped:
		writeSummaryStopped(&sb, data, completedCount, totalTables)
	case state.Apply.Cancelled:
		writeSummaryCancelled(&sb, data, completedCount, totalTables)
	default:
		writeEnvironmentTitle(&sb, fmt.Sprintf("Schema Change: %s", humanizeState(data.State)), data.Environment)
		writeSummaryMetadata(&sb, data)
	}

	return sb.String()
}

// countTableOutcomes counts completed and failed tables.
func countTableOutcomes(tables []TableProgressData) (completed, failed int) {
	for _, t := range tables {
		switch state.NormalizeTaskStatus(t.Status) {
		case state.Task.Completed:
			completed++
		case state.Task.Failed:
			failed++
		}
	}
	return
}

func writeSummaryCompleted(sb *strings.Builder, data ApplyStatusCommentData, totalTables int) {
	writeApplyHeader(sb, data)
	writeSummaryCompletedMetadata(sb, data)
	var msg string
	switch {
	case totalTables == 0 && len(data.VSchemaChanges) > 0:
		// VSchema-only apply: no per-table tasks, so report the VSchema outcome.
		msg = "VSchema applied successfully — your changes are live!"
	case totalTables == 0:
		msg = "Schema change applied successfully — your changes are live!"
	case totalTables == 1:
		msg = "Schema change applied successfully — your changes are live!"
	default:
		msg = fmt.Sprintf("All %d tables applied successfully — your schema changes are live!", totalTables)
	}
	writeSuccessBlock(sb, msg)
	writeSummaryTableList(sb, data)
	writeVSchemaStatus(sb, data)
	if data.ApplyID != "" {
		if !strings.HasSuffix(sb.String(), "\n\n") {
			sb.WriteString("\n")
		}
		fmt.Fprintf(sb, "_Apply ID: `%s`_\n", data.ApplyID)
	}
}

// writeSummaryCompletedMetadata writes a clean metadata line for completed applies.
// Only shows database — environment is already in the title, and apply ID plus
// duration are operational details that add clutter without value for most users.
func writeSummaryCompletedMetadata(sb *strings.Builder, data ApplyStatusCommentData) {
	writeDBLine(sb, data.Database)
	sb.WriteString("\n")
}

func writeSummaryFailed(sb *strings.Builder, data ApplyStatusCommentData, completedCount, _, totalTables int) {
	writeApplyHeader(sb, data)
	writeSummaryMetadata(sb, data)

	if data.ErrorMessage != "" {
		writeErrorBlock(sb, data.ErrorMessage)
	}

	if completedCount > 0 {
		fmt.Fprintf(sb, "\n%d of %d %s completed before failure.\n", completedCount, totalTables, pluralize("table", totalTables))
	}

	writeSummaryTableList(sb, data)
	writeFooterAction(sb, "To retry:", fmt.Sprintf("schemabot apply -e %s", data.Environment))
}

func writeSummaryStopped(sb *strings.Builder, data ApplyStatusCommentData, completedCount int, totalTables int) {
	writeApplyHeader(sb, data)
	writeSummaryMetadata(sb, data)

	if completedCount > 0 {
		fmt.Fprintf(sb, "\n%d of %d %s completed before stop.\n", completedCount, totalTables, pluralize("table", totalTables))
	}

	writeSummaryTableList(sb, data)
	writeFooterAction(sb, "Paused — to resume from where it stopped:", fmt.Sprintf("schemabot start %s -e %s", data.ApplyID, data.Environment))
}

// writeSummaryCancelled renders the terminal summary for a cancelled schema
// change. Unlike a stopped change, a cancelled one is permanent (e.g. a
// PlanetScale deploy request that was cancelled), so the summary offers no resume
// command and directs the operator to open a new schema change.
func writeSummaryCancelled(sb *strings.Builder, data ApplyStatusCommentData, completedCount int, totalTables int) {
	writeApplyHeader(sb, data)
	writeSummaryMetadata(sb, data)

	if completedCount > 0 {
		fmt.Fprintf(sb, "\n%d of %d %s completed before cancellation.\n", completedCount, totalTables, pluralize("table", totalTables))
	}

	writeSummaryTableList(sb, data)
	sb.WriteString("\n---\n\n")
	sb.WriteString("This schema change was cancelled and cannot be resumed. Open a new schema change to apply it again.\n")
}

func writeSummaryMetadata(sb *strings.Builder, data ApplyStatusCommentData) {
	// Combine database, apply ID, and duration on one metadata line.
	var parts []string
	parts = append(parts, fmt.Sprintf("**Database**: `%s`", data.Database))
	if data.ApplyID != "" {
		parts = append(parts, fmt.Sprintf("**Apply ID**: `%s`", data.ApplyID))
	}
	if data.StartedAt != "" && data.CompletedAt != "" {
		startTime, err1 := time.Parse(time.RFC3339, data.StartedAt)
		endTime, err2 := time.Parse(time.RFC3339, data.CompletedAt)
		if err1 == nil && err2 == nil {
			parts = append(parts, fmt.Sprintf("**Duration**: %s", formatDuration(endTime.Sub(startTime))))
		}
	}
	fmt.Fprintf(sb, "%s\n", strings.Join(parts, " | "))
	writeAppliedByOrTimestampAt(sb, data.RequestedBy, startedAtDisplay(data.StartedAt, currentTimestamp()))
}

// formatDuration formats a time.Duration as a human-readable string.
func formatDuration(d time.Duration) string {
	d = d.Truncate(time.Second)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		m := int(d.Minutes())
		s := int(d.Seconds()) % 60
		if s > 0 {
			return fmt.Sprintf("%dm %ds", m, s)
		}
		return fmt.Sprintf("%dm", m)
	}
	totalHours := int(d.Hours())
	if totalHours < 24 {
		m := int(d.Minutes()) % 60
		if m > 0 {
			return fmt.Sprintf("%dh %dm", totalHours, m)
		}
		return fmt.Sprintf("%dh", totalHours)
	}
	totalDays := totalHours / 24
	hours := totalHours % 24
	m := int(d.Minutes()) % 60
	var parts []string
	if totalDays >= 7 {
		weeks := totalDays / 7
		days := totalDays % 7
		parts = append(parts, fmt.Sprintf("%dw", weeks))
		if days > 0 {
			parts = append(parts, fmt.Sprintf("%dd", days))
		}
	} else {
		parts = append(parts, fmt.Sprintf("%dd", totalDays))
	}
	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%dh", hours))
	}
	if m > 0 {
		parts = append(parts, fmt.Sprintf("%dm", m))
	}
	return strings.Join(parts, " ")
}

// writeSummaryTableList writes table outcomes with inline DDL, grouped by namespace.
// Failed/stopped tables are listed first within each group.
// For 6+ tables, each namespace group is collapsible.
func writeSummaryTableList(sb *strings.Builder, data ApplyStatusCommentData) {
	if len(data.Tables) == 0 {
		return
	}

	// Order: failed/stopped/reverted first, then completed, then cancelled, then any remaining
	included := make(map[int]bool)
	var ordered []TableProgressData
	for i, t := range data.Tables {
		n := state.NormalizeTaskStatus(t.Status)
		if n == state.Task.Failed || n == state.Task.Stopped || n == "reverted" {
			ordered = append(ordered, t)
			included[i] = true
		}
	}
	for i, t := range data.Tables {
		if included[i] {
			continue
		}
		n := state.NormalizeTaskStatus(t.Status)
		if n == state.Task.Completed {
			ordered = append(ordered, t)
			included[i] = true
		}
	}
	for i, t := range data.Tables {
		if included[i] {
			continue
		}
		n := state.NormalizeTaskStatus(t.Status)
		if n == state.Task.Cancelled {
			ordered = append(ordered, t)
			included[i] = true
		}
	}
	// Catch-all: append any tables not yet included (unknown/unexpected states)
	for i, t := range data.Tables {
		if !included[i] {
			ordered = append(ordered, t)
		}
	}

	// Group by namespace
	type nsGroup struct {
		namespace string
		tables    []TableProgressData
	}
	var groups []nsGroup
	seen := make(map[string]int)
	for _, t := range ordered {
		ns := t.Namespace
		if idx, ok := seen[ns]; ok {
			groups[idx].tables = append(groups[idx].tables, t)
		} else {
			seen[ns] = len(groups)
			groups = append(groups, nsGroup{namespace: ns, tables: []TableProgressData{t}})
		}
	}

	collapsed := len(data.Tables) > 5
	// Skip namespace header when there's only one group and it's "default" or
	// matches the database name — the header is redundant with the metadata line.
	singleGroup := len(groups) == 1
	for _, g := range groups {
		skipHeader := singleGroup && (g.namespace == "" || g.namespace == "default" || g.namespace == data.Database)
		groupCollapsed := collapsed && !skipHeader

		if !skipHeader {
			header := g.namespace
			if header == "" || header == "default" {
				header = data.Database
			}

			groupEmoji := groupStateEmoji(g.tables)

			if groupCollapsed {
				sb.WriteString("\n<details><summary>")
				fmt.Fprintf(sb, "%s <strong>%s</strong> (%d tables)</summary>\n\n", groupEmoji, header, len(g.tables))
			} else {
				fmt.Fprintf(sb, "\n### %s %s\n\n", groupEmoji, header)
			}
		} else {
			sb.WriteString("\n")
		}

		for _, t := range g.tables {
			writeSummaryTableEntry(sb, t)
		}

		if groupCollapsed {
			sb.WriteString("</details>\n")
		}
	}
}

// groupStateEmoji returns the aggregate emoji for a group of tables.
func groupStateEmoji(tables []TableProgressData) string {
	states := make(map[string]bool)
	for _, t := range tables {
		states[state.NormalizeTaskStatus(t.Status)] = true
	}

	if states[state.Task.Failed] {
		return "❌"
	}
	if states["reverted"] {
		return "↩️"
	}
	if states[state.Task.Stopped] {
		return "⏹️"
	}
	if states[state.Task.Cancelled] && !states[state.Task.Completed] {
		return "⊘"
	}
	return "✅"
}

// writeSummaryTableEntry writes a single table with DDL block.
// No emoji — the header carries the group state. Non-success tables get a text label.
func writeSummaryTableEntry(sb *strings.Builder, t TableProgressData) {
	normalized := state.NormalizeTaskStatus(t.Status)

	switch normalized {
	case state.Task.Completed:
		fmt.Fprintf(sb, "**`%s`**\n", t.TableName)
	case state.Task.Failed:
		label := "Failed"
		if t.PercentComplete > 0 || t.RowsCopied > 0 {
			label = fmt.Sprintf("Failed at %d%%", ui.RowCopyDisplayPercent(t.PercentComplete, t.RowsCopied))
		}
		fmt.Fprintf(sb, "**`%s`** — %s\n", t.TableName, label)
	case state.Task.Stopped:
		label := "Stopped"
		if t.PercentComplete > 0 || t.RowsCopied > 0 {
			label = fmt.Sprintf("Stopped at %d%%", ui.RowCopyDisplayPercent(t.PercentComplete, t.RowsCopied))
		}
		fmt.Fprintf(sb, "**`%s`** — %s\n", t.TableName, label)
	case "reverted":
		fmt.Fprintf(sb, "**`%s`** — Reverted\n", t.TableName)
	case state.Task.Cancelled:
		fmt.Fprintf(sb, "**`%s`** — Cancelled\n", t.TableName)
	default:
		fmt.Fprintf(sb, "**`%s`**\n", t.TableName)
	}

	if t.DDL != "" {
		fmt.Fprintf(sb, "```sql\n%s\n```\n\n", ddl.FormatDDL(t.DDL))
	} else {
		sb.WriteString("\n")
	}
}

// ApplyStatusFromProgress converts a ProgressResponse to ApplyStatusCommentData.
func ApplyStatusFromProgress(resp *apitypes.ProgressResponse, requestedBy string) ApplyStatusCommentData {
	data := ApplyStatusCommentData{
		Database:     resp.Database,
		Environment:  resp.Environment,
		RequestedBy:  requestedBy,
		State:        state.NormalizeState(resp.State),
		Engine:       resp.Engine,
		ApplyID:      resp.ApplyID,
		ErrorMessage: resp.ErrorMessage,
		StartedAt:    resp.StartedAt,
		CompletedAt:  resp.CompletedAt,
	}

	if changes, err := apitypes.ParseVSchemaChanges(resp.Metadata); err != nil {
		slog.Warn("failed to parse VSchema changes from progress metadata", "apply_id", resp.ApplyID, "error", err)
	} else {
		data.VSchemaChanges = changes
	}

	for _, t := range resp.Tables {
		if t.TableName == "" {
			continue
		}
		data.Tables = append(data.Tables, TableProgressData{
			TableName:       t.TableName,
			DDL:             t.DDL,
			Status:          state.NormalizeState(t.Status),
			RowsCopied:      t.RowsCopied,
			RowsTotal:       t.RowsTotal,
			PercentComplete: int(t.PercentComplete),
			ETASeconds:      t.ETASeconds,
			IsInstant:       t.IsInstant,
		})
	}

	return data
}
