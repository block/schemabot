package templates

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/ui"
)

// TableProgressData is one progress row in a PR comment: one task, which runs
// one DDL statement against one table. A plan with several statements on the
// same table produces several rows carrying that table's name, so rows are
// counted by progressUnit rather than assumed to be distinct tables.
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
	// The engine's throttler is pausing this table's active phase (row copy
	// or checksum verify). ThrottleReason names the signal for display and is
	// empty when Throttled is false.
	Throttled      bool
	ThrottleReason string
	IsInstant      bool

	// ErrorMessage is the task's last error. Rendered for states where the
	// per-table error explains what the user is seeing (e.g. a retrying or
	// failed task).
	ErrorMessage string

	// Shards is the per-shard breakdown for a sharded table. Empty for unsharded
	// engines. Rendered as a single compact summary line
	// while the table is in flight (see renderShardSummary); detailed per-shard
	// row counts/ETAs stay in the CLI, not the PR comment.
	Shards []ShardProgressData
}

// TaskStatusReadyForCutover reports whether a table's task status marks it as
// ready for the operator to cut over. A task parked at the cutover barrier has
// finished its copy and verification phases — the remaining binlog catch-up
// happens under cutover itself — so the barrier state is what makes the table
// ready. Engines fold any internal readiness signal into the task status they
// report, so this is the single predicate every render path uses to decide
// readiness. Accepts raw engine statuses (Spirit "waitingOnSentinelTable",
// Vitess "ready_to_complete") as well as canonical task states.
func TaskStatusReadyForCutover(status string) bool {
	return state.NormalizeTaskStatus(status) == state.Task.WaitingForCutover
}

// tableKey identifies one table across namespaces. The separator is a byte no
// identifier can contain, so two distinct (namespace, table) pairs never share
// a key even when their names concatenate to the same string.
func tableKey(namespace, table string) string {
	return namespace + "\x00" + table
}

// progressUnit names what one row counts as in the comment's totals. Every row
// is one DDL statement, so the totals say "table" only while each row is a
// distinct (namespace, table) — the shape of any plan that emits one statement
// per table, whatever the engine. Once a table contributes several rows, the
// totals say "statement", so a count never claims more tables than the plan
// touches. Both nouns hold in the first case; only "statement" holds in the
// second, so the choice never overstates.
func progressUnit(tables []TableProgressData) string {
	distinct := make(map[string]struct{}, len(tables))
	for _, t := range tables {
		distinct[tableKey(t.Namespace, t.TableName)] = struct{}{}
	}
	if len(distinct) == len(tables) {
		return "table"
	}
	return "statement"
}

// countReadyForCutover returns how many rows are parked at the cutover
// barrier, per TaskStatusReadyForCutover.
func countReadyForCutover(tables []TableProgressData) int {
	ready := 0
	for _, t := range tables {
		if TaskStatusReadyForCutover(t.Status) {
			ready++
		}
	}
	return ready
}

// ShardProgressData is the high-level status of one shard, for the compact
// per-shard summary in the PR comment. It intentionally carries only state +
// percent (no row counts/ETA) to keep the comment quiet.
type ShardProgressData struct {
	Shard           string
	Status          string // canonical lowercase shard/task state
	PercentComplete int
	RowsCopied      int64
	RowsTotal       int64
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

	// DerivedStatus, when set, replaces the raw-state **Status** line in the
	// rendered body. Multi-deployment sections set it for deployments whose
	// render-time presentation is derived from earlier siblings (queued,
	// waiting, halted, paused): the raw operation state for all of those is
	// pending, which alone cannot express them, so the body's status must come
	// from the derived model to agree with its <summary> line.
	DerivedStatus string

	// Attempt is the apply's operator redispatch count so far; the retry the
	// comment announces is Attempt+1 of storage.MaxRecoveryAttempts.
	Attempt     int
	StartedAt   string // RFC3339 format
	CompletedAt string // RFC3339 format
	Tables      []TableProgressData

	// VSchemaChanges holds per-keyspace VSchema application state, surfaced from
	// the engine's display metadata rather than as a per-table task. Empty when
	// the apply carries no VSchema change.
	VSchemaChanges []apitypes.VSchemaChange

	// DeployRequestURL links the PlanetScale deploy request driving this apply
	// (Vitess/PlanetScale only). It is the operator's entry point into the deploy
	// request's own progress, which the comment does not otherwise surface. Empty
	// for engines without a deploy request or before one is created.
	DeployRequestURL string

	// RevertExpiresAt is the RFC3339 deadline when the revert window closes
	// (PlanetScale only). When set and the apply is in its revert window, the
	// comment shows the time remaining before the change becomes permanent.
	// Empty outside the revert window or for engines without one.
	RevertExpiresAt string

	// Tenant is the deployment's tenant identity, appended as --tenant to every
	// pasteable command hint so copied commands address this deployment in
	// tenant mode. Empty on single-tenant deployments, leaving hints unchanged.
	Tenant string

	// Rollback reports whether this apply reverts a previously applied schema
	// change (the apply's durable rollback option). It switches the headline and
	// status vocabulary from "apply" to "rollback" so a completed rollback is
	// announced as "Rollback Complete", never as a freshly applied schema change.
	Rollback bool

	// DeferCutover reports whether cutover waits on an explicit operator
	// trigger (the apply's durable defer-cutover option). It selects the
	// waiting-for-cutover footer: deferred applies get the pasteable
	// `schemabot cutover` command, while non-deferred applies get a note that
	// the drive triggers cutover automatically — surfacing the command there
	// would tell the operator to act when no action is needed.
	DeferCutover bool
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
	writeApplyStatusHeader(&sb, data)

	// Metadata line
	writeApplyMetadata(&sb, data, renderedAt)

	// Status line. For the revert window it carries the countdown to permanence
	// inline (e.g. "Revert Window | Closes in 28m 30s") so the operator sees the
	// state and its deadline in one place rather than on a separate line.
	writeApplyStatusDetail(&sb, data)

	// Deploy-request link (PlanetScale) — the operator's entry point into the
	// deploy request's own progress, which the comment does not otherwise surface.
	writeDeployRequestLink(&sb, data)

	// Cutover readiness summary. Only while the apply is parked at the barrier —
	// readiness answers "can I cut over yet?", a question that no longer exists
	// once cutover is running, when the per-table rows carry the state.
	if data.State == state.Apply.WaitingForCutover {
		writeCutoverSummary(&sb, data.Tables)
	}

	// Per-table progress section
	if len(data.Tables) > 0 {
		writeTableProgressSection(&sb, data)
	}

	// VSchema application status, surfaced from engine metadata rather than as a
	// per-table task (a VSchema-only apply has no tables at all).
	writeVSchemaStatus(&sb, data.VSchemaChanges)

	// Error message for apply states that need operator attention. A failed
	// apply gets the failure glyph — the system stopped and triage is due; a
	// stopped apply gets the attention glyph — the heading already says the
	// operator paused it, and the error is context, not a fresh failure.
	if data.ErrorMessage != "" {
		switch {
		case state.IsState(data.State, state.Apply.Failed):
			writeErrorBlock(&sb, glyph.Failed, data.ErrorMessage)
		case state.IsState(data.State, state.Apply.Stopped):
			writeErrorBlock(&sb, glyph.Attention, data.ErrorMessage)
		}
	}

	// Footer with next actions
	writeApplyFooter(&sb, data)
	if includeLastUpdated && !state.IsTerminalApplyState(data.State) {
		writeLastUpdatedFooter(&sb, renderedAt)
	}

	return sb.String()
}

// writeApplyStatusHeader writes the headline for an in-place apply status
// comment. The title is intentionally state-independent — "Schema Change
// Status — <env>" (or "Rollback Status — <env>" for a rollback apply, which is
// fixed for the apply's lifetime) — so the headline stays stable as the apply
// moves through its states (running → revert window → applied); the current
// state is conveyed by the Status line and the per-table progress, not the
// headline. The single-, multi-deployment, and sharded status comments all use
// this so their headline vocabulary is identical. Terminal summary/notification
// comments use writeApplyHeader, which keeps a state-specific title.
func writeApplyStatusHeader(sb *strings.Builder, data ApplyStatusCommentData) {
	title := "Schema Change Status"
	if data.Rollback {
		title = "Rollback Status"
	}
	writeEnvironmentTitle(sb, title, data.Environment)
}

// writeApplyHeader writes the state-specific title for a terminal summary or
// notification comment (the separate comment posted when an apply reaches a
// terminal state), distinct from the stable in-place status headline.
func writeApplyHeader(sb *strings.Builder, data ApplyStatusCommentData) {
	if data.Rollback {
		writeRollbackHeader(sb, data)
		return
	}
	switch data.State {
	case state.Apply.Completed:
		writeEnvironmentTitle(sb, "✅ Schema Change Applied", data.Environment)
	case state.Apply.Failed:
		writeEnvironmentTitle(sb, glyph.Failed+" Schema Change Failed", data.Environment)
		writeSupportChannelOffer(sb)
	case state.Apply.Stopped:
		writeEnvironmentTitle(sb, "⏹️ Schema Change Stopped", data.Environment)
	case state.Apply.Reverted:
		writeEnvironmentTitle(sb, "↩️ Schema Change Reverted", data.Environment)
	case state.Apply.Cancelled:
		writeEnvironmentTitle(sb, "🚫 Schema Change Cancelled", data.Environment)
	default:
		writeEnvironmentTitle(sb, fmt.Sprintf("Schema Change: %s", humanizeState(data.State)), data.Environment)
	}
}

// writeRollbackHeader writes the terminal title for a rollback apply. A
// completed rollback is announced with rollback vocabulary and a rewind emoji —
// never the green-check "Schema Change Applied" — so a PR reader cannot mistake
// the revert for the schema change landing.
func writeRollbackHeader(sb *strings.Builder, data ApplyStatusCommentData) {
	switch data.State {
	case state.Apply.Completed:
		writeEnvironmentTitle(sb, "⏪ Rollback Complete", data.Environment)
	case state.Apply.Failed:
		writeEnvironmentTitle(sb, glyph.Failed+" Rollback Failed", data.Environment)
		writeSupportChannelOffer(sb)
	case state.Apply.Stopped:
		writeEnvironmentTitle(sb, "⏹️ Rollback Stopped", data.Environment)
	case state.Apply.Cancelled:
		writeEnvironmentTitle(sb, "🚫 Rollback Cancelled", data.Environment)
	default:
		writeEnvironmentTitle(sb, fmt.Sprintf("Rollback: %s", humanizeState(data.State)), data.Environment)
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

func writeApplyStatusDetail(sb *strings.Builder, data ApplyStatusCommentData) {
	// A sibling-derived status carries the whole story ("Halted — eu failed");
	// the raw-state gloss and its running-state suffixes do not apply to the
	// pending-derived presentations that set it.
	if data.DerivedStatus != "" {
		fmt.Fprintf(sb, "\n**Status**: %s\n", data.DerivedStatus)
		return
	}
	detail := applyStatusDetail(data.State)
	if state.IsRunningApplyState(data.State) && hasRetryingTable(data.Tables) {
		// A remote data plane retries its failures on its own: the stored
		// apply stays active through the pause, so the retry is only visible
		// on the task rows. Surface it as the status so the operator sees the
		// same Retrying view a locally parked apply gets.
		detail = "Retrying"
	}
	if data.Rollback && state.IsState(data.State, state.Apply.Completed) {
		detail = "Rolled Back"
	}
	if detail == "" {
		return
	}
	if state.IsState(data.State, state.Apply.RevertWindow) {
		if countdown := revertWindowCountdown(data.RevertExpiresAt); countdown != "" {
			detail += " | " + countdown
		}
	}
	fmt.Fprintf(sb, "\n**Status**: %s\n", detail)
	if state.IsState(data.State, state.Apply.Failed) {
		writeSupportChannelOffer(sb)
	}
}

func applyStatusDetail(applyState string) string {
	applyState = state.NormalizeState(applyState)
	switch applyState {
	case state.Apply.Completed:
		return "Applied"
	case state.Apply.Failed:
		return "Failed"
	case state.Apply.Stopped:
		return "Stopped"
	case state.Apply.Reverted:
		return "Reverted"
	case state.Apply.Cancelled:
		return "Cancelled"
	case state.Apply.RevertWindow:
		return "Revert Window"
	case state.Apply.Reverting:
		return "Reverting"
	case state.Apply.Pending:
		return "Starting"
	case state.Apply.Running, state.Apply.RunningDegraded:
		return "In Progress"
	case state.Apply.CatchingUp:
		return "Catching Up"
	case state.Apply.Checksumming:
		return "Checksumming"
	case state.Apply.PostChecksum:
		return "Applying Final Changes"
	case state.Apply.FailedRetryable:
		return "Retrying"
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
		if applyState == "" || state.IsTerminalApplyState(applyState) {
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
	fmt.Fprintf(sb, "\nDeploy request: %s\n", data.DeployRequestURL)
}

func stopOrCancelCommand(data ApplyStatusCommentData) string {
	if strings.EqualFold(data.Engine, storage.EnginePlanetScale) {
		return "cancel"
	}
	return "stop"
}

func writeStopOrCancelFooterAction(sb *strings.Builder, data ApplyStatusCommentData, stopPrefix, cancelPrefix string) {
	command := stopOrCancelCommand(data)
	prefix := stopPrefix
	if command == "cancel" {
		prefix = cancelPrefix
	}
	writeFooterAction(sb, prefix, appendTenantFlag(fmt.Sprintf("schemabot %s %s -e %s", command, data.ApplyID, data.Environment), data.Tenant))
}

// revertWindowCountdown returns the time remaining before the revert window
// closes, phrased for the status line (e.g. "Closes in 28m 30s"). It returns ""
// when the deadline is unset, unparseable, or already past, so the status line
// never shows a stale or negative countdown.
func revertWindowCountdown(revertExpiresAt string) string {
	if revertExpiresAt == "" {
		return ""
	}
	expires, err := time.Parse(time.RFC3339, revertExpiresAt)
	if err != nil {
		return ""
	}
	// NowFunc (not time.Now) so previews and tests render a deterministic countdown.
	remaining := expires.Sub(NowFunc())
	if remaining <= 0 {
		return ""
	}
	return fmt.Sprintf("Closes in %s", formatDuration(remaining))
}

// writeCutoverSummary writes a readiness summary for cutover states, showing
// how many rows are ready for cutover vs not yet ready, counted in the
// comment's progressUnit.
func writeCutoverSummary(sb *strings.Builder, tables []TableProgressData) {
	ready := countReadyForCutover(tables)
	total := len(tables)
	if total == 0 {
		return
	}
	unit := progressUnit(tables)
	if ready == total {
		fmt.Fprintf(sb, "\n**%d/%d** %s(s) ready for cutover\n", ready, total, unit)
	} else {
		fmt.Fprintf(sb, "\n**%d/%d** %s(s) ready for cutover — waiting on %d\n", ready, total, unit, total-ready)
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

	var completed, running, catchingUp, checksumming, queued, waiting, failed, retrying, stopped, recovering, cutting, cancelled, other int
	var runningPct int
	var runningPctText string
	var runningEstimateExceeded bool

	for _, t := range tables {
		// Tables parked at the cutover barrier count through the shared
		// readiness predicate, keeping this summary consistent with the
		// cutover summary.
		if TaskStatusReadyForCutover(t.Status) {
			waiting++
			continue
		}
		switch state.NormalizeTaskStatus(t.Status) {
		case state.Task.Completed:
			completed++
		case state.Task.Running:
			running++
			if ui.EstimateExceeded(t.RowsCopied, t.RowsTotal) {
				runningEstimateExceeded = true
			} else {
				runningPct = ui.RowCopyDisplayPercent(t.PercentComplete, t.RowsCopied)
				runningPctText = ui.FormatRowCopyPercent(t.PercentComplete, t.RowsCopied, t.RowsTotal)
			}
		case state.Task.CatchingUp, state.Task.PostChecksum:
			// Both binlog drains (before and after the verify) read as
			// "catching up" in the compact summary; the per-table rows
			// distinguish them.
			catchingUp++
		case state.Task.Checksumming:
			checksumming++
		case state.Task.Pending:
			queued++
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
		default:
			// Statuses without a dedicated bucket (waiting for deploy, the
			// revert family, future states) surface as "other" rather than
			// silently vanishing from the summary line.
			other++
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
			label += " (finalizing copy)"
		} else if runningPct > 0 {
			label += fmt.Sprintf(" (%s)", runningPctText)
		}
		parts = append(parts, label)
	}
	if catchingUp > 0 {
		if multi {
			parts = append(parts, fmt.Sprintf("%d catching up", catchingUp))
		} else {
			parts = append(parts, "catching up")
		}
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
	if other > 0 {
		if multi {
			parts = append(parts, fmt.Sprintf("%d in other states", other))
		} else {
			parts = append(parts, "in another state")
		}
	}

	if len(parts) > 0 {
		fmt.Fprintf(sb, "\n📊 %s\n", strings.Join(parts, " · "))
	}
}

// writeVSchemaStatus renders the VSchema application status and, when
// available, the diff. It is shown as its own section because VSchema
// application is not a per-table task. Renders nothing when the apply carries
// no VSchema change.
func writeVSchemaStatus(sb *strings.Builder, changes []apitypes.VSchemaChange) {
	if len(changes) == 0 {
		return
	}
	sb.WriteString("\n### VSchema\n\n")
	// The diff budget is per comment, not per keyspace: split it across the
	// entries that carry a diff so a multi-keyspace apply stays bounded.
	diffCount := 0
	for _, c := range changes {
		if c.Diff != "" {
			diffCount++
		}
	}
	budget := vschemaDiffBudget(diffCount)
	for _, c := range changes {
		fmt.Fprintf(sb, "**`%s`**: %s\n\n", c.Namespace, ui.VSchemaStatusLabel(c.Status))
		if c.Diff != "" {
			writeVSchemaDiffFence(sb, c.Diff, budget)
		}
	}
}

// writeTableProgressSection writes the per-row progress breakdown, grouped by
// namespace so the operator can see which schema (MySQL) or keyspace
// (Vitess/PlanetScale, Strata) each table belongs to. Within a namespace, rows
// are ordered by sortProgressRows: active/running first, then pending, then
// completed/terminal last, with a table's rows kept together.
func writeTableProgressSection(sb *strings.Builder, data ApplyStatusCommentData) {
	// During the resume window the per-table percents are indeterminate (the data
	// plane has not reported continuation vs fresh copy yet), so the aggregate
	// running-percent summary would surface stale pre-stop numbers. The per-table
	// "Resuming…" lines below convey state without it.
	if data.State != state.Apply.Resuming {
		writeProgressSummary(sb, data.Tables)
	}
	sb.WriteString("\n")

	dialect := dialectForEngine(data.Engine, data.ApplyID)

	for _, group := range groupTablesByNamespace(data.Tables) {
		// Label the group by namespace when one is set. The bold metadata-style
		// header and the row block below it stand in for a generic section header.
		if group.namespace != "" {
			fmt.Fprintf(sb, "**%s `%s`**\n\n", namespaceLabel(data.Engine), group.namespace)
		}

		for _, table := range sortProgressRows(group.tables) {
			// While the apply is resuming, the data plane has not yet reported whether
			// the schema change continues from its checkpoint or restarts from scratch,
			// so the row-copy percent is indeterminate. Render state-only until the
			// apply transitions to running and real progress is known.
			if data.State == state.Apply.Resuming && !state.IsTerminalTaskState(state.NormalizeTaskStatus(table.Status)) {
				renderResumingTable(sb, dialect, table)
				continue
			}
			renderTableProgress(sb, dialect, table, data.State, data.Attempt, data.ErrorMessage)
		}
	}
}

// namespaceTableGroup is a set of tables that share a namespace, in the order the
// namespace first appears among the tables.
type namespaceTableGroup struct {
	namespace string
	tables    []TableProgressData
}

// groupTablesByNamespace groups tables by namespace, preserving the order in
// which each namespace first appears. The empty namespace and the "default"
// placeholder both mean "no specific namespace": they are folded together and
// render without a header, so a comment never shows a meaningless
// "Schema `default`" / "Keyspace `default`" header or splits the same logical
// no-namespace tables into separate groups.
// hasRetryingTable reports whether any table sits in a retryable pause. An
// active apply with a retrying table means a data plane is recovering the
// failure on its own; the pause never reaches the stored apply state, so
// status and footer rendering derive it from the task rows.
func hasRetryingTable(tables []TableProgressData) bool {
	for _, table := range tables {
		if state.IsState(state.NormalizeTaskStatus(table.Status), state.Task.FailedRetryable) {
			return true
		}
	}
	return false
}

func groupTablesByNamespace(tables []TableProgressData) []namespaceTableGroup {
	var groups []namespaceTableGroup
	index := make(map[string]int)
	for _, t := range tables {
		ns := t.Namespace
		if ns == "default" {
			ns = ""
		}
		if i, ok := index[ns]; ok {
			groups[i].tables = append(groups[i].tables, t)
			continue
		}
		index[ns] = len(groups)
		groups = append(groups, namespaceTableGroup{namespace: ns, tables: []TableProgressData{t}})
	}
	return groups
}

// namespaceLabel returns the operator-facing word for a table namespace: a
// keyspace for the Vitess-family engines (PlanetScale, Strata), a schema for
// MySQL (Spirit).
func namespaceLabel(engine string) string {
	if strings.EqualFold(engine, storage.EnginePlanetScale) || strings.EqualFold(engine, storage.EngineStrata) {
		return "Keyspace"
	}
	return "Schema"
}

// renderResumingTable renders a table while the apply is resuming, before the
// data plane reports whether the change continues from its checkpoint or restarts
// from scratch. The percent is intentionally omitted during this window.
func renderResumingTable(sb *strings.Builder, dialect schema.Dialect, table TableProgressData) {
	fmt.Fprintf(sb, "**`%s`**: \U0001f504 Resuming…\n", table.TableName)
	writeDDLLine(sb, dialect, table.DDL)
	sb.WriteString("\n")
}

// tableStatePriority returns a sort key: lower = rendered first (active on top, completed on bottom).
func tableStatePriority(tableStatus string) int {
	return ui.TableStatePriority(state.NormalizeTaskStatus(tableStatus))
}

// summaryStatePriority returns the sort key the terminal summary lists rows
// in: what went wrong first, then what landed, then what never ran, then
// anything unexpected. Lower renders first.
func summaryStatePriority(tableStatus string) int {
	switch state.NormalizeTaskStatus(tableStatus) {
	case state.Task.Failed, state.Task.Stopped, state.Task.Reverted:
		return 0
	case state.Task.Completed:
		return 1
	case state.Task.Cancelled:
		return 2
	default:
		return 3
	}
}

// sortProgressRows orders one namespace's rows for the progress comment, with
// active tables on top and terminal ones at the bottom. See sortRowsByTable.
func sortProgressRows(rows []TableProgressData) []TableProgressData {
	return sortRowsByTable(rows, tableStatePriority)
}

// sortRowsByTable orders rows for display without modifying the input. Tables
// are ranked by the most urgent state among their rows under statePriority,
// and a table's rows stay adjacent — ordered by their own state — so a table
// whose plan produced several statements reads as one block rather than
// scattering across the list by state. Tables with the same rank keep plan
// order. When every row is a distinct table this is exactly a stable sort by
// statePriority.
func sortRowsByTable(rows []TableProgressData, statePriority func(tableStatus string) int) []TableProgressData {
	type tableRank struct {
		priority   int
		firstIndex int
	}
	ranks := make(map[string]tableRank, len(rows))
	for i, row := range rows {
		key := tableKey(row.Namespace, row.TableName)
		priority := statePriority(row.Status)
		rank, seen := ranks[key]
		if !seen {
			ranks[key] = tableRank{priority: priority, firstIndex: i}
			continue
		}
		if priority < rank.priority {
			rank.priority = priority
			ranks[key] = rank
		}
	}

	sorted := make([]TableProgressData, len(rows))
	copy(sorted, rows)
	sort.SliceStable(sorted, func(i, j int) bool {
		ri := ranks[tableKey(sorted[i].Namespace, sorted[i].TableName)]
		rj := ranks[tableKey(sorted[j].Namespace, sorted[j].TableName)]
		if ri.priority != rj.priority {
			return ri.priority < rj.priority
		}
		if ri.firstIndex != rj.firstIndex {
			return ri.firstIndex < rj.firstIndex
		}
		return statePriority(sorted[i].Status) < statePriority(sorted[j].Status)
	})
	return sorted
}

// renderTableProgress renders a single table's progress as markdown.
// Mirrors the CLI's writeTableProgressWithState logic but outputs markdown
// instead of ANSI. applyError is the apply-level error message the comment
// renders as its own block, so a failed table's identical error is not
// repeated below the row.
func renderTableProgress(sb *strings.Builder, dialect schema.Dialect, table TableProgressData, applyState string, applyAttempt int, applyError string) {
	// Normalize to canonical Task state for consistent matching.
	status := state.NormalizeTaskStatus(table.Status)

	switch status {
	case state.Task.Pending:
		fmt.Fprintf(sb, "**`%s`**: \u23f3 Queued\n", table.TableName)
		writeDDLLine(sb, dialect, table.DDL)

	case state.Task.Completed:
		bar := ui.ProgressBarComplete()
		fmt.Fprintf(sb, "**`%s`**: %s \u2705 Complete\n", table.TableName, bar)
		writeDDLLine(sb, dialect, table.DDL)

	case state.Task.CatchingUp:
		// Row copy is complete; the engine is draining the changes that
		// accumulated on the source while the copy ran. On a busy table this
		// catch-up can run for hours, so name the phase instead of rendering a
		// serene completed copy.
		fmt.Fprintf(sb, "**`%s`**: %s ⏩ Catching up on accumulated changes...\n", table.TableName, ui.ProgressBarRowCopy(100))
		writeDDLLine(sb, dialect, table.DDL)
		if table.RowsCopied > 0 {
			fmt.Fprintf(sb, "- Rows copied: %s\n", ui.FormatNumber(table.RowsCopied))
		}

	case state.Task.Checksumming:
		// Row copy is complete; the engine is verifying the copied data against
		// the source before cutover. On a large table this can run for hours, so
		// show how far the verify has progressed once Spirit reports a total.
		if table.ChecksumRowsTotal > 0 {
			pct := ui.ClampPercent(int(table.ChecksumRowsChecked * 100 / table.ChecksumRowsTotal))
			fmt.Fprintf(sb, "**`%s`**: %s \U0001f50d Checksumming to verify data (%s)%s\n", table.TableName, ui.ProgressBarRowCopy(pct),
				ui.FormatRowCopyPercent(pct, table.ChecksumRowsChecked, table.ChecksumRowsTotal), throttledSuffix(table))
			writeDDLLine(sb, dialect, table.DDL)
			fmt.Fprintf(sb, "- Rows verified: %s / %s\n",
				ui.FormatNumber(ui.ClampRows(table.ChecksumRowsChecked, table.ChecksumRowsTotal)), ui.FormatNumber(table.ChecksumRowsTotal))
		} else {
			fmt.Fprintf(sb, "**`%s`**: %s \U0001f50d Checksumming to verify data...%s\n", table.TableName, ui.ProgressBarRowCopy(100), throttledSuffix(table))
			writeDDLLine(sb, dialect, table.DDL)
		}
		writeThrottleTooltip(sb, table)

	case state.Task.PostChecksum:
		// The verify passed and the engine is draining the changes that
		// accumulated on the source while it ran. Named separately from the
		// pre-checksum catch-up so the row doesn't rewind to an earlier phase.
		fmt.Fprintf(sb, "**`%s`**: %s ⏩ Data verified, applying final changes...\n", table.TableName, ui.ProgressBarRowCopy(100))
		writeDDLLine(sb, dialect, table.DDL)
		if table.RowsCopied > 0 {
			fmt.Fprintf(sb, "- Rows copied: %s\n", ui.FormatNumber(table.RowsCopied))
		}

	case state.Task.WaitingForCutover:
		fmt.Fprintf(sb, "**`%s`**: %s Waiting for cutover\n", table.TableName, ui.ProgressBarWaitingCutover())
		writeDDLLine(sb, dialect, table.DDL)

	case state.Task.Recovering:
		if recoveringIsCopyingRows(table) {
			pct := ui.RowCopyDisplayPercent(table.PercentComplete, table.RowsCopied)
			bar := ui.ProgressBarRowCopy(pct)
			fmt.Fprintf(sb, "**`%s`**: %s Row copy in progress (%s)\n", table.TableName, bar,
				ui.FormatRowCopyPercent(table.PercentComplete, table.RowsCopied, table.RowsTotal))
			writeDDLLine(sb, dialect, table.DDL)
			writeRowsAndETA(sb, table)
			break
		}
		// Blue activity bar: the engine is working on its own with no
		// meaningful percentage — yellow is reserved for states where the
		// operator holds the next move.
		bar := ui.ProgressBarActivity()
		fmt.Fprintf(sb, "**`%s`**: %s Recovering state...\n", table.TableName, bar)
		writeDDLLine(sb, dialect, table.DDL)

	case state.Task.CuttingOver:
		bar := ui.ProgressBarActivity() // blue — automatic work, no operator action
		fmt.Fprintf(sb, "**`%s`**: %s \U0001f504 Cutting over...\n", table.TableName, bar)
		writeDDLLine(sb, dialect, table.DDL)

	case state.Task.Failed:
		// A progress bar asserts that row copy happened. When the engine failed
		// before copying a single row (e.g. a preflight check rejected the
		// change), a red 0% bar reads as a copy that started and stalled — render
		// the failure without one. An instant DDL change has no row copy phase
		// at all, so its failure label does not mention one.
		switch pct := ui.RowCopyDisplayPercent(table.PercentComplete, table.RowsCopied); {
		case pct > 0:
			fmt.Fprintf(sb, "**`%s`**: %s "+glyph.Failed+" Failed\n", table.TableName, ui.ProgressBarFailed(pct))
		case table.IsInstant:
			fmt.Fprintf(sb, "**`%s`**: "+glyph.Failed+" Failed\n", table.TableName)
		default:
			fmt.Fprintf(sb, "**`%s`**: "+glyph.Failed+" Failed (before row copy started)\n", table.TableName)
		}
		writeDDLLine(sb, dialect, table.DDL)
		if taskErrorAddsDetail(table.ErrorMessage, applyError) {
			writeTableErrorLine(sb, glyph.Failed, table.ErrorMessage)
		}

	case state.Task.FailedRetryable:
		bar := ui.ProgressBarStopped(ui.RowCopyDisplayPercent(table.PercentComplete, table.RowsCopied))
		if state.IsState(applyState, state.Apply.FailedRetryable) {
			fmt.Fprintf(sb, "**`%s`**: %s \U0001f504 Interrupted — retrying automatically (attempt %d/%d)\n",
				table.TableName, bar, applyAttempt+1, storage.MaxRecoveryAttempts)
		} else {
			// The apply is paused by a data plane that retries on its own; its
			// attempt count does not cross the wire, so announce the retry
			// without inventing a number.
			fmt.Fprintf(sb, "**`%s`**: %s \U0001f504 Interrupted — retrying automatically\n",
				table.TableName, bar)
		}
		writeDDLLine(sb, dialect, table.DDL)
		if table.ErrorMessage != "" {
			// The row above says SchemaBot is retrying on its own, so the
			// error is context for the operator, not a failure to triage.
			writeTableErrorLine(sb, glyph.Attention, table.ErrorMessage)
		}

	case state.Task.Cancelled:
		fmt.Fprintf(sb, "**`%s`**: 🚫 Cancelled (not started)\n", table.TableName)
		writeDDLLine(sb, dialect, table.DDL)

	case state.Task.RevertWindow:
		// Deliberately no checkmark: the change is applied but not final while
		// the revert window is open, and a checkmark reads as "done, walk away".
		bar := ui.ProgressBarWaitingCutover()
		fmt.Fprintf(sb, "**`%s`**: %s Complete (revert window open)\n", table.TableName, bar)
		writeDDLLine(sb, dialect, table.DDL)

	case state.Task.Reverting:
		bar := ui.ProgressBarWaitingCutover()
		fmt.Fprintf(sb, "**`%s`**: %s \u21a9\ufe0f Reverting\n", table.TableName, bar)
		writeDDLLine(sb, dialect, table.DDL)

	case state.Task.Stopped:
		renderStoppedTable(sb, dialect, table)

	default:
		// Running / in-progress
		renderRunningTable(sb, dialect, table)
	}

	renderShardSummary(sb, table)

	sb.WriteString("\n")
}

// shardSummaryBreakdownState reports whether a table's aggregate status is
// in-flight work whose line carries the compact per-shard breakdown.
func shardSummaryBreakdownState(status string) bool {
	switch state.NormalizeTaskStatus(status) {
	case state.Task.Running, state.Task.CatchingUp, state.Task.Checksumming, state.Task.PostChecksum, state.Task.Recovering, state.Task.CuttingOver, state.Task.WaitingForCutover:
		return true
	default:
		return false
	}
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
	if !shardSummaryBreakdownState(table.Status) {
		return // completed/pending/cancelled/failed: no breakdown, stay quiet
	}

	if len(table.Shards) <= shardNamesInlineLimit {
		parts := make([]string, 0, len(table.Shards))
		for _, sh := range table.Shards {
			if isCopyingShardStatus(sh.Status) && (sh.PercentComplete > 0 || sh.RowsCopied > 0) {
				parts = append(parts, fmt.Sprintf("%s %s %s", shardGlyph(sh.Status), sh.Shard,
					ui.FormatRowCopyPercent(sh.PercentComplete, sh.RowsCopied, sh.RowsTotal)))
				continue
			}
			part := fmt.Sprintf("%s %s", shardGlyph(sh.Status), sh.Shard)
			// Glyphs whose meaning isn't self-evident carry the same word the
			// bucketed form uses, so the line reads without a legend.
			if word := shardStatusWord(sh.Status); word != "" {
				part += " " + word
			}
			parts = append(parts, part)
		}
		fmt.Fprintf(sb, "  └ shards: %s\n", strings.Join(parts, " · "))
		return
	}

	var complete, copying, ready, failed, queued, other int
	slowestShard, slowestText := "", ""
	slowestFraction := -1.0
	for _, sh := range table.Shards {
		// Shards parked at the cutover barrier count through the shared
		// readiness predicate, keeping the shard buckets consistent with the
		// table summaries.
		if TaskStatusReadyForCutover(sh.Status) {
			ready++
			continue
		}
		switch state.NormalizeShardStatus(sh.Status) {
		case state.Task.Completed:
			complete++
		case state.Task.Failed, state.Task.FailedRetryable:
			failed++
		case state.Task.Pending:
			queued++
		default:
			if isCopyingShardStatus(sh.Status) {
				copying++
				if frac := ui.RowCopyFraction(sh.PercentComplete, sh.RowsCopied, sh.RowsTotal); slowestFraction < 0 || frac < slowestFraction {
					slowestFraction = frac
					slowestShard = sh.Shard
					slowestText = ui.FormatRowCopyPercent(sh.PercentComplete, sh.RowsCopied, sh.RowsTotal)
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
	if slowestShard != "" && slowestFraction >= 0 {
		line += fmt.Sprintf(" · slowest %s %s", slowestShard, slowestText)
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

// shardStatusWord returns the word the bucketed summary pairs with a shard's
// glyph, for glyphs a reader can't decode on sight. Self-evident glyphs
// (✓ complete, ⏳ queued) return "", and so does the unknown-status catch-all
// — the bucketed form keeps its "…" bucket bare too. Copying shards return
// "copying", which the caller replaces with a percent when one is available.
func shardStatusWord(status string) string {
	switch state.NormalizeShardStatus(status) {
	case state.Task.WaitingForCutover:
		return "ready"
	case state.Task.Failed, state.Task.FailedRetryable:
		return "failed"
	default:
		if isCopyingShardStatus(status) {
			return "copying"
		}
		return ""
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
func renderRunningTable(sb *strings.Builder, dialect schema.Dialect, table TableProgressData) {
	defer writeThrottleTooltip(sb, table)
	if table.RowsTotal > 0 {
		if ui.EstimateExceeded(table.RowsCopied, table.RowsTotal) {
			fmt.Fprintf(sb, "**`%s`**: %s Finalizing copy%s\n", table.TableName, ui.ProgressBarActivity(), throttledSuffix(table))
			writeDDLLine(sb, dialect, table.DDL)
			fmt.Fprintf(sb, "- Rows copied: %s so far\n", ui.FormatNumber(table.RowsCopied))
			fmt.Fprintf(sb, "- "+glyph.Info+" _%s_\n", ui.EstimateExceededTooltip)
			return
		}

		pct := ui.RowCopyDisplayPercent(table.PercentComplete, table.RowsCopied)
		if pct == 0 {
			// Row total is known but the copy hasn't reported progress yet
			// (VReplication / Spirit ramp-up). A 0% bar reads as stuck, so show
			// a starting indicator and the row total instead.
			fmt.Fprintf(sb, "**`%s`**: ⏳ Starting copy...%s\n", table.TableName, throttledSuffix(table))
			writeDDLLine(sb, dialect, table.DDL)
			writeRowsAndETA(sb, table)
			return
		}
		bar := ui.ProgressBarRowCopy(pct)
		fmt.Fprintf(sb, "**`%s`**: %s %s%s\n", table.TableName, bar,
			ui.FormatRowCopyPercent(table.PercentComplete, table.RowsCopied, table.RowsTotal), throttledSuffix(table))
		writeDDLLine(sb, dialect, table.DDL)
		writeRowsAndETA(sb, table)
	} else {
		// No row data yet (initializing or instant DDL)
		fmt.Fprintf(sb, "**`%s`**: Running...%s\n", table.TableName, throttledSuffix(table))
		writeDDLLine(sb, dialect, table.DDL)
	}
}

// throttledSuffix annotates a paced-phase header when the engine's throttler
// is holding the phase back, so a slow bar reads as deliberate backpressure —
// slowed, not stopped — right where the eye checks progress, and never to be
// confused with an operator stop. The drive clears the stored flag when the
// throttle lifts, so the annotation disappears on the next refresh.
func throttledSuffix(table TableProgressData) string {
	if !table.Throttled {
		return ""
	}
	return " (throttled)"
}

// writeThrottleTooltip explains the header's "(throttled)" annotation with the
// engine's reason, using the same tooltip idiom as the estimate-exceeded note.
// The reason is sanitized at the engine boundary before it is stored, so it is
// safe to render in markdown. When the engine reports throttled without a
// reason, the header annotation stands alone.
func writeThrottleTooltip(sb *strings.Builder, table TableProgressData) {
	if !table.Throttled || table.ThrottleReason == "" {
		return
	}
	// The raw reason names the engine signal; the tip says what the pause
	// protects, and links the reference doc for remediation prose. A reason
	// whose signal has no tip renders alone so a new engine signal degrades
	// to raw text rather than a wrong explanation.
	if tip := ui.ThrottleTip(table.ThrottleReason); tip != "" {
		fmt.Fprintf(sb, "- "+glyph.Info+" _Throttled: %s · %s ([docs](%s))_\n", escapeInlineMarkdown(table.ThrottleReason), tip, ui.ThrottleDocURL)
		return
	}
	fmt.Fprintf(sb, "- "+glyph.Info+" _Throttled: %s_\n", escapeInlineMarkdown(table.ThrottleReason))
}

func recoveringIsCopyingRows(table TableProgressData) bool {
	return table.RowsTotal > 0 && table.PercentComplete < 100
}

// recoveringCopyPercent returns the least-progressed recovering table's copy
// percent as display text, so the recovery summary never overstates how far
// the slowest table has come.
func recoveringCopyPercent(tables []TableProgressData) (string, bool) {
	fraction := 0.0
	text := ""
	found := false
	for _, table := range tables {
		if state.NormalizeTaskStatus(table.Status) != state.Task.Recovering || !recoveringIsCopyingRows(table) {
			continue
		}
		if frac := ui.RowCopyFraction(table.PercentComplete, table.RowsCopied, table.RowsTotal); !found || frac < fraction {
			fraction = frac
			text = ui.FormatRowCopyPercent(table.PercentComplete, table.RowsCopied, table.RowsTotal)
		}
		found = true
	}
	return text, found
}

// renderStoppedTable renders a table in the stopped state.
func renderStoppedTable(sb *strings.Builder, dialect schema.Dialect, table TableProgressData) {
	switch {
	case table.PercentComplete >= 100:
		bar := ui.ProgressBarStopped(100)
		fmt.Fprintf(sb, "**`%s`**: %s \u23f9\ufe0f Stopped (was waiting for cutover)\n", table.TableName, bar)
	case table.PercentComplete > 0 || table.RowsCopied > 0:
		pct := ui.RowCopyDisplayPercent(table.PercentComplete, table.RowsCopied)
		bar := ui.ProgressBarStopped(pct)
		fmt.Fprintf(sb, "**`%s`**: %s \u23f9\ufe0f Stopped at %s\n", table.TableName, bar,
			ui.FormatRowCopyPercent(table.PercentComplete, table.RowsCopied, table.RowsTotal))
	default:
		fmt.Fprintf(sb, "**`%s`**: \u23f9\ufe0f Stopped (not started)\n", table.TableName)
	}

	writeDDLLine(sb, dialect, table.DDL)

	// Show rows (no ETA) for stopped tables with progress
	if table.RowsTotal > 0 && (table.PercentComplete > 0 || table.RowsCopied > 0) {
		fmt.Fprintf(sb, "- Rows: %s / %s\n",
			ui.FormatNumber(ui.ClampRows(table.RowsCopied, table.RowsTotal)),
			ui.FormatNumber(table.RowsTotal))
	}
}

// dialectForEngine maps an apply's engine value to the DDL dialect its
// statements are written in, for display formatting. The engine field on
// comment data carries engine names, database types, and display-cased
// variants ("postgres", "PostgreSQL", "Spirit", "vitess"), so the match is
// case-insensitive and accepts both Postgres spellings; every other value
// drives a MySQL-protocol target. MySQL is also the fallback for an empty
// value — rows that predate the engine field — and for a value outside the
// known set, which warns with the apply identifier: an unset or corrupted
// engine on a Postgres apply renders its DDL under the wrong grammar, and
// that must be triageable from logs. This is a display concern —
// system-schema classification uses schema.DialectForDatabaseType, which
// treats unknown values conservatively.
func dialectForEngine(engine, applyID string) schema.Dialect {
	switch strings.ToLower(strings.TrimSpace(engine)) {
	case storage.EnginePostgres, "postgresql":
		return schema.DialectPostgres
	case storage.EngineSpirit, storage.EnginePlanetScale, storage.EngineStrata,
		storage.DatabaseTypeMySQL, storage.DatabaseTypeVitess, "":
		return schema.DialectMySQL
	}
	slog.Warn("unrecognized engine value on apply comment; DDL will render under the MySQL grammar",
		"engine", engine, "apply_id", applyID)
	return schema.DialectMySQL
}

// writeDDLLine writes the DDL statement as a sql code block below the table name.
func writeDDLLine(sb *strings.Builder, dialect schema.Dialect, rawDDL string) {
	if rawDDL != "" {
		fmt.Fprintf(sb, "\n```sql\n%s\n```\n", ddl.FormatDDLForDialect(dialect, rawDDL))
	}
}

// writeRowsAndETA writes the rows copied / total line with optional ETA.
func writeRowsAndETA(sb *strings.Builder, table TableProgressData) {
	if table.RowsTotal <= 0 {
		return
	}
	copied := ui.ClampRows(table.RowsCopied, table.RowsTotal)
	if table.ETASeconds > 0 {
		fmt.Fprintf(sb, "- Rows: %s / %s \u00b7 ETA: %s\n",
			ui.FormatNumber(copied),
			ui.FormatNumber(table.RowsTotal),
			ui.FormatETA(table.ETASeconds))
	} else {
		fmt.Fprintf(sb, "- Rows: %s / %s\n",
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
		writeFooterAction(sb, "To deploy:", appendTenantFlag(fmt.Sprintf("schemabot cutover %s -e %s", data.ApplyID, data.Environment), data.Tenant))
	case state.Apply.WaitingForCutover:
		if data.DeferCutover {
			writeFooterAction(sb, "To proceed with cutover:", appendTenantFlag(fmt.Sprintf("schemabot cutover %s -e %s", data.ApplyID, data.Environment), data.Tenant))
		} else {
			sb.WriteString("\n---\n\n")
			sb.WriteString("SchemaBot triggers cutover automatically — no action needed.\n")
		}
	case state.Apply.Recovering:
		sb.WriteString("\n---\n\n")
		if pct, ok := recoveringCopyPercent(data.Tables); ok {
			fmt.Fprintf(sb, "Recovering after restart. Row copy is in progress (%s); once recovery completes, progress returns to the normal row-copy view.\n", pct)
		} else {
			sb.WriteString("Recovering after restart. Cutover will be available once recovery completes.\n")
		}
	case state.Apply.Running, state.Apply.RunningDegraded,
		state.Apply.CatchingUp, state.Apply.Checksumming, state.Apply.PostChecksum:
		if hasRetryingTable(data.Tables) {
			// The apply is active but a data plane is retrying a failed table
			// on its own; give the operator the same retry guidance a locally
			// parked apply gets.
			writeStopOrCancelFooterAction(sb, data,
				"An error interrupted this schema change. SchemaBot retries automatically and marks it failed if retries are exhausted. To stop retrying:",
				"An error interrupted this schema change. SchemaBot retries automatically and marks it failed if retries are exhausted. To cancel it:")
			return
		}
		writeStopOrCancelFooterAction(sb, data, "To stop this schema change:", "To cancel this schema change:")
	case state.Apply.PreparingBranch,
		state.Apply.ApplyingBranchChanges,
		state.Apply.ValidatingBranch,
		state.Apply.CreatingDeployRequest,
		state.Apply.ValidatingDeployRequest:
		writeStopOrCancelFooterAction(sb, data, "To stop this schema change:", "To cancel this schema change:")
	case state.Apply.FailedRetryable:
		writeStopOrCancelFooterAction(sb, data,
			"An error interrupted this schema change. SchemaBot retries automatically and marks it failed if retries are exhausted. To stop retrying:",
			"An error interrupted this schema change. SchemaBot retries automatically and marks it failed if retries are exhausted. To cancel it:")
	case state.Apply.Stopped:
		writeFooterAction(sb, "Paused — to resume from where it stopped:", appendTenantFlag(fmt.Sprintf("schemabot start %s -e %s", data.ApplyID, data.Environment), data.Tenant))
	case state.Apply.Cancelled:
		sb.WriteString("\n---\n\n")
		sb.WriteString("This schema change was cancelled and cannot be resumed. Open a new schema change to apply it again.\n")
	case state.Apply.Failed:
		writeFooterAction(sb, "To retry:", appendTenantFlag(fmt.Sprintf("schemabot apply -e %s", data.Environment), data.Tenant))
	case state.Apply.RevertWindow:
		// Skip-revert (finalize) is the common path, so it leads; revert (undo) follows.
		writeFooterAction(sb, "To skip revert and keep changes:", appendTenantFlag(fmt.Sprintf("schemabot skip-revert %s -e %s", data.ApplyID, data.Environment), data.Tenant))
		fmt.Fprintf(sb, "\nTo revert:\n```\n%s\n```\n", appendTenantFlag(fmt.Sprintf("schemabot revert %s -e %s", data.ApplyID, data.Environment), data.Tenant))
	case state.Apply.SkippingRevert:
		sb.WriteString("\n---\n\n")
		sb.WriteString("Skip-revert was requested — closing the revert window and making this schema change permanent. This can no longer be reverted.\n")
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
	// A VSchema update counts as a schema change alongside table changes, so the
	// singular/plural wording reflects the total operation count.
	writeSuccessBlock(sb, completedOutcomeMessage(totalTables+len(data.VSchemaChanges) == 1, data.Rollback))
	writeCompletedSummaryDetails(sb, data)
}

// completedOutcomeMessage is the completed summary's outcome line, shared by
// every apply shape so the terminal vocabulary stays identical.
func completedOutcomeMessage(singular, rollback bool) string {
	if rollback {
		if singular {
			return "Rolled back successfully — the schema change has been reverted."
		}
		return "Rolled back successfully — the schema changes have been reverted."
	}
	if singular {
		return "Applied successfully — your schema change is live!"
	}
	return "Applied successfully — your schema changes are live!"
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
		writeErrorBlock(sb, glyph.Failed, data.ErrorMessage)
	}

	if completedCount > 0 {
		fmt.Fprintf(sb, "\n%d of %d %s completed before failure.\n", completedCount, totalTables, pluralize(progressUnit(data.Tables), totalTables))
	}

	writeSummaryTableList(sb, data)
	writeFooterAction(sb, "To retry:", appendTenantFlag(fmt.Sprintf("schemabot apply -e %s", data.Environment), data.Tenant))
}

func writeSummaryStopped(sb *strings.Builder, data ApplyStatusCommentData, completedCount int, totalTables int) {
	writeApplyHeader(sb, data)
	writeSummaryMetadata(sb, data)

	if completedCount > 0 {
		fmt.Fprintf(sb, "\n%d of %d %s completed before stop.\n", completedCount, totalTables, pluralize(progressUnit(data.Tables), totalTables))
	}

	writeSummaryTableList(sb, data)
	writeFooterAction(sb, "Paused — to resume from where it stopped:", appendTenantFlag(fmt.Sprintf("schemabot start %s -e %s", data.ApplyID, data.Environment), data.Tenant))
}

// writeSummaryCancelled renders the terminal summary for a cancelled schema
// change. Unlike a stopped change, a cancelled one is permanent (e.g. a
// PlanetScale deploy request that was cancelled), so the summary offers no resume
// command and directs the operator to open a new schema change.
func writeSummaryCancelled(sb *strings.Builder, data ApplyStatusCommentData, completedCount int, totalTables int) {
	writeApplyHeader(sb, data)
	writeSummaryMetadata(sb, data)

	if completedCount > 0 {
		fmt.Fprintf(sb, "\n%d of %d %s completed before cancellation.\n", completedCount, totalTables, pluralize(progressUnit(data.Tables), totalTables))
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
	if d := durationDisplay(data.StartedAt, data.CompletedAt); d != "" {
		parts = append(parts, fmt.Sprintf("**Duration**: %s", d))
	}
	fmt.Fprintf(sb, "%s\n", strings.Join(parts, " | "))
	writeAppliedByOrTimestampAt(sb, data.RequestedBy, startedAtDisplay(data.StartedAt, currentTimestamp()))
}

// durationDisplay formats the elapsed time between two RFC3339 timestamps for a
// summary metadata line, or "" when either timestamp is missing or unparseable
// — the duration is decoration, so a bad timestamp drops it rather than failing
// the render.
func durationDisplay(startedAt, completedAt string) string {
	if startedAt == "" || completedAt == "" {
		return ""
	}
	startTime, err1 := time.Parse(time.RFC3339, startedAt)
	endTime, err2 := time.Parse(time.RFC3339, completedAt)
	if err1 != nil || err2 != nil {
		return ""
	}
	if endTime.Before(startTime) {
		return ""
	}
	return formatDuration(endTime.Sub(startTime))
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

func writeCompletedSummaryDetails(sb *strings.Builder, data ApplyStatusCommentData) {
	if len(data.Tables) == 0 && len(data.VSchemaChanges) == 0 {
		// No per-operation detail to collapse (e.g. a task-less apply that found
		// no changes). Still surface the Apply ID so the summary stays auditable.
		if data.ApplyID != "" {
			fmt.Fprintf(sb, "\n_Apply ID: `%s`_\n", data.ApplyID)
		}
		return
	}

	fmt.Fprintf(sb, "\n<details><summary>%s</summary>\n\n", completedSummaryDetailsLabel(data))
	if data.ApplyID != "" {
		fmt.Fprintf(sb, "_Apply ID: `%s`_\n\n", data.ApplyID)
	}
	writeCompletedNamespaceSummary(sb, data)
	if len(data.Tables) > 0 {
		writeSummaryTableListWithOptions(sb, data, false)
	}
	writeVSchemaStatus(sb, data.VSchemaChanges)
	sb.WriteString("</details>\n")
}

func completedSummaryDetailsLabel(data ApplyStatusCommentData) string {
	var parts []string
	if len(data.Tables) > 0 {
		parts = append(parts, fmt.Sprintf("%d %s", len(data.Tables), pluralize(progressUnit(data.Tables), len(data.Tables))))
	}
	if len(data.VSchemaChanges) > 0 {
		parts = append(parts, fmt.Sprintf("%d VSchema %s", len(data.VSchemaChanges), pluralize("update", len(data.VSchemaChanges))))
	}
	return fmt.Sprintf("Apply details (%s)", strings.Join(parts, ", "))
}

type completedNamespaceSummary struct {
	namespace      string
	tableCount     int
	vschemaUpdates int
}

func writeCompletedNamespaceSummary(sb *strings.Builder, data ApplyStatusCommentData) {
	summaries := completedNamespaceSummaries(data)
	if len(summaries) <= 1 {
		return
	}

	// One unit for the whole comment: a per-namespace choice would let one
	// line say "tables" and the next "statements" for the same apply.
	unit := progressUnit(data.Tables)
	sb.WriteString("Applied by namespace:\n\n")
	for _, summary := range summaries {
		var parts []string
		if summary.tableCount > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", summary.tableCount, pluralize(unit, summary.tableCount)))
		}
		if summary.vschemaUpdates > 0 {
			parts = append(parts, fmt.Sprintf("%d VSchema %s", summary.vschemaUpdates, pluralize("update", summary.vschemaUpdates)))
		}
		fmt.Fprintf(sb, "- `%s`: %s\n", summary.namespace, strings.Join(parts, ", "))
	}
	sb.WriteString("\n")
}

func completedNamespaceSummaries(data ApplyStatusCommentData) []completedNamespaceSummary {
	seen := make(map[string]int)
	var summaries []completedNamespaceSummary
	for _, table := range data.Tables {
		namespace := displayNamespace(table.Namespace, data.Database)
		idx, ok := seen[namespace]
		if !ok {
			idx = len(summaries)
			seen[namespace] = idx
			summaries = append(summaries, completedNamespaceSummary{namespace: namespace})
		}
		summaries[idx].tableCount++
	}
	for _, change := range data.VSchemaChanges {
		namespace := displayNamespace(change.Namespace, data.Database)
		idx, ok := seen[namespace]
		if !ok {
			idx = len(summaries)
			seen[namespace] = idx
			summaries = append(summaries, completedNamespaceSummary{namespace: namespace})
		}
		summaries[idx].vschemaUpdates++
	}
	return summaries
}

func displayNamespace(namespace, database string) string {
	if namespace == "" || namespace == "default" {
		return database
	}
	return namespace
}

// writeSummaryTableList writes table outcomes with inline DDL, grouped by namespace.
// Failed/stopped tables are listed first within each group.
// For 6+ tables, each namespace group is collapsible.
func writeSummaryTableList(sb *strings.Builder, data ApplyStatusCommentData) {
	writeSummaryTableListWithOptions(sb, data, true)
}

func writeSummaryTableListWithOptions(sb *strings.Builder, data ApplyStatusCommentData, collapseNamespaceGroups bool) {
	if len(data.Tables) == 0 {
		return
	}

	dialect := dialectForEngine(data.Engine, data.ApplyID)

	// What went wrong leads, then what landed, then what never ran — and a
	// table's rows stay together so the reader meets each table once.
	ordered := sortRowsByTable(data.Tables, summaryStatePriority)

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

	collapsed := collapseNamespaceGroups && len(data.Tables) > 5
	// Collapsed group headers count in the comment-wide unit, not a per-group
	// one, so a group of distinct tables next to a multi-statement group does
	// not read "3 tables" above "3 statements" for the same apply.
	unit := progressUnit(data.Tables)
	// On an unsuccessful apply, each completed table is labeled so the reader
	// can tell which tables made it before the failure/stop/cancellation.
	labelCompleted := !state.IsState(data.State, state.Apply.Completed)
	// Per-namespace status emojis only carry information when outcomes differ
	// across namespaces (some failed, some succeeded). When every namespace
	// succeeded, the repeated ✅ is noise, so the headers omit the emoji.
	showGroupEmoji := false
	for _, g := range groups {
		if groupStateEmoji(g.tables) != "✅" {
			showGroupEmoji = true
			break
		}
	}
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

			emojiPrefix := ""
			if showGroupEmoji {
				emojiPrefix = groupStateEmoji(g.tables) + " "
			}

			if groupCollapsed {
				sb.WriteString("\n<details><summary>")
				fmt.Fprintf(sb, "%s<strong>%s</strong> (%d %s)</summary>\n\n", emojiPrefix, header, len(g.tables), pluralize(unit, len(g.tables)))
			} else {
				fmt.Fprintf(sb, "\n### %s%s\n\n", emojiPrefix, header)
			}
		} else {
			sb.WriteString("\n")
		}

		for _, t := range g.tables {
			writeSummaryTableEntry(sb, dialect, t, labelCompleted)
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
		return glyph.Failed
	}
	if states["reverted"] {
		return "↩️"
	}
	if states[state.Task.Stopped] {
		return "⏹️"
	}
	if states[state.Task.Cancelled] && !states[state.Task.Completed] {
		return "🚫"
	}
	return "✅"
}

// writeSummaryTableEntry writes a single table with a text outcome label and
// DDL block. No emoji — the header carries the group state. labelCompleted
// controls whether completed tables get a label too: on a summary for an
// unsuccessful apply, each row must answer "did this table make it?", so
// completed tables are labeled explicitly. On a successful apply the header
// already says every table completed, so the label would be noise.
func writeSummaryTableEntry(sb *strings.Builder, dialect schema.Dialect, t TableProgressData, labelCompleted bool) {
	normalized := state.NormalizeTaskStatus(t.Status)

	switch normalized {
	case state.Task.Completed:
		if labelCompleted {
			fmt.Fprintf(sb, "**`%s`** — Completed\n", t.TableName)
		} else {
			fmt.Fprintf(sb, "**`%s`**\n", t.TableName)
		}
	case state.Task.Failed:
		label := "Failed"
		if t.PercentComplete > 0 || t.RowsCopied > 0 {
			label = fmt.Sprintf("Failed at %s", ui.FormatRowCopyPercent(t.PercentComplete, t.RowsCopied, t.RowsTotal))
		}
		fmt.Fprintf(sb, "**`%s`** — %s\n", t.TableName, label)
	case state.Task.Stopped:
		label := "Stopped"
		if t.PercentComplete > 0 || t.RowsCopied > 0 {
			label = fmt.Sprintf("Stopped at %s", ui.FormatRowCopyPercent(t.PercentComplete, t.RowsCopied, t.RowsTotal))
		}
		fmt.Fprintf(sb, "**`%s`** — %s\n", t.TableName, label)
	case "reverted":
		fmt.Fprintf(sb, "**`%s`** — Reverted\n", t.TableName)
	case state.Task.Cancelled:
		fmt.Fprintf(sb, "**`%s`** — Cancelled\n", t.TableName)
	default:
		// Unknown or in-flight statuses still get a visible label — a bare
		// table name reads as success, which is wrong for anything but
		// completed.
		fmt.Fprintf(sb, "**`%s`** — %s\n", t.TableName, taskOutcomeLabel(normalized))
	}

	if t.DDL != "" {
		fmt.Fprintf(sb, "```sql\n%s\n```\n\n", ddl.FormatDDLForDialect(dialect, t.DDL))
	} else {
		sb.WriteString("\n")
	}
}

// taskOutcomeLabel says where a table was left when the apply stopped running.
// Most states read correctly once humanized, but three do not: two are internal
// vocabulary the operator never sees elsewhere, and revert_window hides the fact
// an operator most needs from a terminal record, that the change is already
// applied and only the window is still open. Those get an explicit label; every
// other state keeps the humanized constant this file uses by default.
func taskOutcomeLabel(normalized string) string {
	switch normalized {
	case state.Task.RevertWindow:
		return "Completed (revert window open)"
	case state.Task.PostChecksum:
		return "Data verified, not cut over"
	case state.Task.FailedRetryable:
		return "Interrupted"
	}
	return humanizeState(normalized)
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
	data.RevertExpiresAt = resp.Metadata["revert_expires_at"]

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
