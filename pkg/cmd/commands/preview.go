package commands

import (
	"fmt"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/cmd/templates"
	webhooktemplates "github.com/block/schemabot/pkg/webhook/templates"
)

// PreviewCmd previews CLI output templates without running schema changes.
type PreviewCmd struct {
	Type string `arg:"" optional:"" help:"Preview type (run 'schemabot preview' for valid types)"`
}

// Run executes the preview command.
func (cmd *PreviewCmd) Run(g *Globals) error {
	// Use fixed timestamps so TEMPLATES.md doesn't change on every regeneration.
	webhooktemplates.TimestampFunc = func() string { return "2026-01-01 00:00:00 UTC" }
	webhooktemplates.NowFunc = func() time.Time { return time.Date(2026, 3, 15, 14, 30, 0, 0, time.UTC) }
	templates.SetPreviewMode()

	if cmd.Type == "" {
		printPreviewUsage()
		return nil
	}

	previewType := templates.PreviewType(cmd.Type)
	switch previewType {
	// Basic types
	case templates.PreviewPlan, templates.PreviewProgress, templates.PreviewWaitingForCutover,
		templates.PreviewCuttingOver, templates.PreviewCompleted, templates.PreviewFailed,
		templates.PreviewStopped, templates.PreviewStates:
		templates.PreviewCLIOutput(previewType)
	// Lock types
	case templates.PreviewLockAcquired, templates.PreviewLockConflict,
		templates.PreviewLockConflictCLI, templates.PreviewNoLockFound,
		templates.PreviewUnlockNotOwned,
		templates.PreviewLockReleased, templates.PreviewLocksList:
		templates.PreviewCLIOutput(previewType)
	// Sequential mode types
	case templates.PreviewSeqPending, templates.PreviewSeqFirstRun, templates.PreviewSeqSecondRun,
		templates.PreviewSeqThirdRun, templates.PreviewSeqAllDone, templates.PreviewSeqFirstFail,
		templates.PreviewSeqMidFail, templates.PreviewSeqStopped, templates.PreviewSequentialAll:
		templates.PreviewCLIOutput(previewType)
	// Defer cutover types (atomic mode)
	case templates.PreviewDeferRunning, templates.PreviewDeferWaiting, templates.PreviewDeferSingle,
		templates.PreviewDeferSeqWait, templates.PreviewDeferStopped, templates.PreviewDeferDetached,
		templates.PreviewDeferCuttingAll, templates.PreviewDeferAll:
		templates.PreviewCLIOutput(previewType)
	// Apply watch mode types
	case templates.PreviewApplyWatch, templates.PreviewApplyStopped:
		templates.PreviewCLIOutput(previewType)
	// Stop/Start command types
	case templates.PreviewStopCommand, templates.PreviewStartCommand:
		templates.PreviewCLIOutput(previewType)
	// Volume control types
	case templates.PreviewVolumeMode, templates.PreviewVolumeBar:
		templates.PreviewCLIOutput(previewType)
	// Status types
	case templates.PreviewStatusList, templates.PreviewStatusHistory:
		templates.PreviewCLIOutput(previewType)
	// Lint and unsafe types
	case templates.PreviewLintWarnings, templates.PreviewUnsafeBlocked,
		templates.PreviewUnsafeAllowed, templates.PreviewLintAll:
		templates.PreviewCLIOutput(previewType)
	// Comment template types
	case templates.PreviewCommentPlan, templates.PreviewCommentPlanEmpty,
		templates.PreviewCommentMultiEnv, templates.PreviewCommentMultiEnvDiff,
		templates.PreviewCommentMultiEnvLint,
		templates.PreviewCommentVitessPlan, templates.PreviewCommentVitessApplyPlan,
		templates.PreviewCommentMySQLMultiSchema,
		templates.PreviewCommentHelp,
		templates.PreviewCommentErrors, templates.PreviewCommentUnsafeBlocked,
		templates.PreviewCommentApplyPlan, templates.PreviewCommentApplyPlanOptions,
		templates.PreviewCommentApplyPlanUnsafe,
		templates.PreviewCommentApplyProgress, templates.PreviewCommentApplyCompleted,
		templates.PreviewCommentApplyFailed, templates.PreviewCommentApplyStopped,
		templates.PreviewCommentApplyWaitingCutover, templates.PreviewCommentApplyCuttingOver,
		templates.PreviewCommentSingleProgress, templates.PreviewCommentSingleComplete,
		templates.PreviewCommentSingleFailed, templates.PreviewCommentSingleStopped,
		templates.PreviewCommentSummaryCompleted, templates.PreviewCommentSummaryFailed,
		templates.PreviewCommentSummaryStopped,
		templates.PreviewCommentSummaryCompletedLarge, templates.PreviewCommentSummaryFailedLarge,
		templates.PreviewCommentAll:
		templates.PreviewCLIOutput(previewType)
	// Apply command comment types
	case templates.PreviewCommentApplyStartedType,
		templates.PreviewCommentApplyWaiting, templates.PreviewCommentUnlock,
		templates.PreviewCommentApplyBlocked, templates.PreviewCommentApplyBlockedCLI,
		templates.PreviewCommentApplyActive,
		templates.PreviewCommentApplyNoLock, templates.PreviewCommentApplyAllType:
		templates.PreviewCLIOutput(previewType)
	// Paired aggregate types (PR + CLI subsections)
	case templates.PreviewCommentPlanAll, templates.PreviewCommentLockingAll,
		templates.PreviewCommentApplyFlowAll, templates.PreviewCLIPlanAll,
		templates.PreviewCLILockingAll, templates.PreviewCLIApplyAll:
		templates.PreviewCLIOutput(previewType)
	// Log output mode types (-o log)
	case templates.PreviewLogSmall, templates.PreviewLogLarge, templates.PreviewLogFailed,
		templates.PreviewLogStopped, templates.PreviewLogMulti, templates.PreviewLogAll,
		templates.PreviewLogCutover, templates.PreviewLogDetailed:
		previewLogOutput(previewType)
	// Meta
	case templates.PreviewAll:
		templates.PreviewCLIOutput(previewType)
		fmt.Println()
		fmt.Println("=" + strings.Repeat("=", 70))
		fmt.Println("LOG OUTPUT MODE PREVIEWS (-o log)")
		fmt.Println("=" + strings.Repeat("=", 70))
		previewLogAll()
	default:
		return fmt.Errorf("unknown preview type: %s (run 'schemabot preview' for valid types)", cmd.Type)
	}

	return nil
}

func printPreviewUsage() {
	fmt.Println(`Preview CLI output templates without running schema changes.

Usage:
  schemabot preview <type>

Basic Types:
  plan                  Show sample plan output
  progress              Show sample progress output (running state)
  waiting_for_cutover   Show sample waiting for cutover output
  cutting_over          Show sample cutting over output
  completed             Show sample completed output
  failed                Show sample failed output
  stopped               Show sample stopped output (mid-apply stop)
  states                Show state display formatting

Lock Types:
  lock_acquired         Show lock acquired message
  lock_conflict         Show lock conflict error
  lock_conflict_cli     Show lock conflict by CLI session
  lock_released         Show lock released messages
  locks_list            Show locks list output
  no_lock_found         Show no lock found message
  unlock_not_owned      Show unlock not owned message

Sequential Mode (multi-table, one at a time):
  seq_pending           All tables pending (just started, shows queued DDLs)
  seq_first_run         First table running, others queued
  seq_second_run        First complete, second running
  seq_third_run         First two complete, third running
  seq_all_done          All tables completed
  seq_first_fail        First table failed, others never started
  seq_mid_fail          Middle table failed
  seq_stopped           User stopped mid-apply
  sequential_all        Show all sequential mode previews

Atomic Mode (--defer-cutover flag):
  defer_running         All tables copy rows, cutover together
  defer_waiting         Multiple tables waiting for cutover
  defer_single          Single table waiting for cutover
  defer_seq_wait        Sequential: first complete, second waiting
  defer_stopped         User stopped mid-apply (all tables stopped)
  defer_detached        Detached state with reconnect instructions
  defer_cutting         Cutting over in progress
  defer_all             Show all defer cutover previews

Apply Watch Mode:
  apply_watch           Running with footer controls
  apply_stopped         Stopped by user

Stop/Start Commands:
  stop_command          Output when user runs 'schemabot stop'
  start_command         Output when user runs 'schemabot start'

Volume Control:
  volume_bar            Volume bar at different levels
  volume_mode           Volume adjustment mode (press 'v' during apply)

Status:
  status_list           List of active schema changes
  status_history        Database apply history

Lint and Unsafe:
  lint_warnings         Lint warnings output
  unsafe_blocked        Unsafe changes blocked (need --allow-unsafe)
  unsafe_allowed        Unsafe changes with --allow-unsafe enabled
  lint_all              Show all lint/unsafe previews

Log Output Mode (-o log):
  log_small             Small/instant tables (start + complete, no progress noise)
  log_large             Large table with 30s row copy heartbeats
  log_failed            Failed apply with error details
  log_stopped           Stopped mid-apply
  log_multi             Mixed scenario: small + large tables
  log_cutover           Waiting for cutover + cutting over
  log_detailed          All fields including task_id
  log_all               Show all log output previews

Comment Templates (GitHub PR comments):
  comment_plan                  Plan comment with DDL changes + lint warnings
  comment_plan_empty            Plan comment with no changes
  comment_multi_env             Multi-env plan (identical plans, deduplicated)
  comment_multi_env_diff        Multi-env plan (different plans per environment)
  comment_multi_env_lint        Multi-env plan with lint warnings
  comment_help                  Help command reference comment
  comment_errors                All error comment templates
  comment_unsafe_blocked        Unsafe changes blocked (no --allow-unsafe)
  comment_single_progress       Single table: running (most common case)
  comment_single_complete       Single table: completed
  comment_single_failed         Single table: failed
  comment_single_stopped        Single table: stopped
  comment_apply_progress        Multi-table: in progress (per-table progress bars)
  comment_apply_completed       Multi-table: completed (all tables done)
  comment_apply_failed          Multi-table: failed (with error and cancelled tables)
  comment_apply_stopped         Multi-table: stopped (partial progress)
  comment_apply_waiting_cutover Waiting for cutover
  comment_apply_cutting_over    Cutting over
  comment_summary_completed       Summary: apply completed
  comment_summary_failed          Summary: apply failed (with error)
  comment_summary_stopped         Summary: apply stopped
  comment_summary_completed_large Summary: completed (8 tables, rollup format)
  comment_summary_failed_large    Summary: failed (8 tables, rollup format)
  comment_all                   Show all comment template previews

Apply Command Comments (GitHub PR apply commands):
  comment_apply_plan            Apply plan with lock + confirmation footer
  comment_apply_plan_options    Apply plan with options (defer cutover, revert)
  comment_apply_plan_unsafe     Apply plan with unsafe changes warning

Apply Command Comments (GitHub PR apply commands):
  comment_apply_plan            Apply plan with lock + confirmation footer
  comment_apply_started         Apply started notification
  comment_apply_waiting         Waiting for cutover notification
  comment_unlock                Lock released confirmation
  comment_apply_blocked         Apply blocked by another PR
  comment_apply_blocked_cli     Apply blocked by CLI session
  comment_apply_active          Apply already in progress
  comment_apply_no_lock         No lock found (need apply first)
  comment_apply_all             Show all apply command previews

Aggregate Types (grouped PR + CLI pairs):
  comment_plan_all              PR plan & status comments
  cli_plan_all                  CLI plan & status output
  comment_locking_all           PR locking comments
  cli_locking_all               CLI locking output
  comment_apply_flow_all        PR apply flow comments
  cli_apply_all                 CLI apply flow output

Meta:
  all                   Show all preview types

Examples:
  schemabot preview plan
  schemabot preview sequential_all
  schemabot preview comment_plan_all
  schemabot preview all`)
}
