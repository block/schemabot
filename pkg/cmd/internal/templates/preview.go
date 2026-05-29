package templates

import (
	"time"

	"github.com/block/schemabot/pkg/ui"
)

// previewTime is a fixed reference time used in all preview output so that
// TEMPLATES.md doesn't produce diffs on every regeneration.
var previewTime = time.Date(2026, 1, 15, 14, 30, 0, 0, time.UTC)

// SetPreviewMode configures the package to use fixed timestamps for deterministic output.
func SetPreviewMode() {
	nowFunc = func() time.Time { return previewTime }
	ui.NowFunc = func() time.Time { return previewTime }
}

// PreviewType represents the type of preview to show.
type PreviewType string

const (
	PreviewPlan              PreviewType = "plan"
	PreviewProgress          PreviewType = "progress"
	PreviewWaitingForDeploy  PreviewType = "waiting_for_deploy"
	PreviewWaitingForCutover PreviewType = "waiting_for_cutover"
	PreviewCuttingOver       PreviewType = "cutting_over"
	PreviewCompleted         PreviewType = "completed"
	PreviewFailed            PreviewType = "failed"
	PreviewStopped           PreviewType = "stopped"
	PreviewStates            PreviewType = "states"
	PreviewLockAcquired      PreviewType = "lock_acquired"
	PreviewLockConflict      PreviewType = "lock_conflict"
	PreviewLockConflictCLI   PreviewType = "lock_conflict_cli"
	PreviewLockReleased      PreviewType = "lock_released"
	PreviewLocksList         PreviewType = "locks_list"
	PreviewNoLockFound       PreviewType = "no_lock_found"
	PreviewUnlockNotOwned    PreviewType = "unlock_not_owned"
	PreviewAll               PreviewType = "all"

	// Paired aggregate previews (PR + CLI subsections under shared headings)
	PreviewCommentPlanAll      PreviewType = "comment_plan_all"       // PR: plan, multi-env, help, errors
	PreviewCommentLockingAll   PreviewType = "comment_locking_all"    // PR: blocked, unlock, no lock, in progress
	PreviewCommentApplyFlowAll PreviewType = "comment_apply_flow_all" // PR: apply plan, started, progress, completed, etc.
	PreviewCLIPlanAll          PreviewType = "cli_plan_all"           // CLI: plan, progress states, status
	PreviewCLILockingAll       PreviewType = "cli_locking_all"        // CLI: lock acquired, conflict, released, list
	PreviewCLIApplyAll         PreviewType = "cli_apply_all"          // CLI: apply watch, stop/start, volume

	// Apply watch mode previews
	PreviewApplyWatch    PreviewType = "apply_watch"    // Running with footer controls
	PreviewApplyStopped  PreviewType = "apply_stopped"  // Stopped by user
	PreviewBranchRefresh PreviewType = "branch_refresh" // Reusing branch with --branch flag

	// Sequential mode previews (multi-table, one at a time)
	PreviewSeqPending    PreviewType = "seq_pending"    // All tables pending (just started)
	PreviewSeqFirstRun   PreviewType = "seq_first_run"  // First table running, others pending
	PreviewSeqSecondRun  PreviewType = "seq_second_run" // First complete, second running
	PreviewSeqThirdRun   PreviewType = "seq_third_run"  // First two complete, third running
	PreviewSeqAllDone    PreviewType = "seq_all_done"   // All tables completed
	PreviewSeqFirstFail  PreviewType = "seq_first_fail" // First table failed, others never started
	PreviewSeqMidFail    PreviewType = "seq_mid_fail"   // Middle table failed
	PreviewSeqStopped    PreviewType = "seq_stopped"    // User stopped mid-apply
	PreviewSequentialAll PreviewType = "sequential_all" // Show all sequential mode previews

	// Defer cutover previews (--defer-cutover / atomic mode)
	PreviewDeferRunning    PreviewType = "defer_running"  // All tables copy rows, cutover together
	PreviewDeferSingle     PreviewType = "defer_single"   // Single table waiting for cutover
	PreviewDeferWaiting    PreviewType = "defer_waiting"  // Multiple tables waiting for cutover
	PreviewDeferSeqWait    PreviewType = "defer_seq_wait" // Sequential: first complete, second waiting
	PreviewDeferStopped    PreviewType = "defer_stopped"  // Stopped mid-apply (all tables show stopped)
	PreviewDeferDetached   PreviewType = "defer_detached" // Detached state with reconnect instructions
	PreviewDeferCuttingAll PreviewType = "defer_cutting"  // Cutting over in progress
	PreviewDeferAll        PreviewType = "defer_all"      // Show all defer cutover previews

	// Stop/Start command output previews
	PreviewStopCommand  PreviewType = "stop_command"  // Output when user runs 'schemabot stop'
	PreviewStartCommand PreviewType = "start_command" // Output when user runs 'schemabot start'

	// Volume control previews
	PreviewVolumeBar  PreviewType = "volume_bar"  // Volume bar at different levels
	PreviewVolumeMode PreviewType = "volume_mode" // Volume adjustment mode

	// Status previews
	PreviewStatusList    PreviewType = "status_list"    // List of active schema changes
	PreviewStatusHistory PreviewType = "status_history" // Database apply history

	// Lint and unsafe previews
	PreviewLintViolations PreviewType = "lint_violations" // Lint violations output
	PreviewUnsafeBlocked  PreviewType = "unsafe_blocked"  // Unsafe changes blocked
	PreviewUnsafeAllowed  PreviewType = "unsafe_allowed"  // Unsafe changes with --allow-unsafe
	PreviewLintAll        PreviewType = "lint_all"        // All lint/unsafe previews

	// Log output mode previews (-o log)
	PreviewLogSmall    PreviewType = "log_small"    // Small/instant tables (start + complete only)
	PreviewLogLarge    PreviewType = "log_large"    // Large table with row copy heartbeats
	PreviewLogFailed   PreviewType = "log_failed"   // Failed apply with error
	PreviewLogStopped  PreviewType = "log_stopped"  // Stopped mid-apply
	PreviewLogMulti    PreviewType = "log_multi"    // Mixed: small + large tables
	PreviewLogAll      PreviewType = "log_all"      // Show all log output previews
	PreviewLogCutover  PreviewType = "log_cutover"  // Waiting for cutover + cutover
	PreviewLogDetailed PreviewType = "log_detailed" // Detailed with task_id and all fields

	// Exit context previews (printed on TUI exit)
	PreviewExitDetachMySQL  PreviewType = "exit_detach_mysql"  // MySQL apply: user detaches mid-progress
	PreviewExitDetachVitess PreviewType = "exit_detach_vitess" // Vitess apply: user detaches mid-progress
	PreviewExitErrorMySQL   PreviewType = "exit_error_mysql"   // MySQL apply: connection lost during progress
	PreviewExitErrorVitess  PreviewType = "exit_error_vitess"  // Vitess apply: connection lost during progress
	PreviewExitAll          PreviewType = "exit_all"           // Show all exit context previews

	// Comment template previews (GitHub PR comments)
	PreviewCommentPlan                PreviewType = "comment_plan"                  // Plan comment with DDL changes + lint violations
	PreviewCommentPlanEmpty           PreviewType = "comment_plan_empty"            // Plan comment with no changes
	PreviewCommentMultiEnv            PreviewType = "comment_multi_env"             // Multi-env plan (identical, deduplicated)
	PreviewCommentMultiEnvDiff        PreviewType = "comment_multi_env_diff"        // Multi-env plan (different per env)
	PreviewCommentMultiEnvLint        PreviewType = "comment_multi_env_lint"        // Multi-env plan with lint violations
	PreviewCommentVitessPlan          PreviewType = "comment_vitess_plan"           // Vitess plan with keyspaces + VSchema
	PreviewCommentVitessApplyPlan     PreviewType = "comment_vitess_apply_plan"     // Locked Vitess apply-plan with options
	PreviewCommentMySQLMultiSchema    PreviewType = "comment_mysql_multi_schema"    // MySQL plan with multiple schema names
	PreviewCommentHelp                PreviewType = "comment_help"                  // Help command reference comment
	PreviewCommentErrors              PreviewType = "comment_errors"                // All error comment templates
	PreviewCommentUnsafeBlocked       PreviewType = "comment_unsafe_blocked"        // Unsafe changes blocked (no --allow-unsafe)
	PreviewCommentApplyPlan           PreviewType = "comment_apply_plan"            // Locked apply-plan comment
	PreviewCommentApplyPlanOptions    PreviewType = "comment_apply_plan_options"    // Locked apply-plan with options
	PreviewCommentApplyPlanUnsafe     PreviewType = "comment_apply_plan_unsafe"     // Locked apply-plan with unsafe warning
	PreviewCommentApplyProgress       PreviewType = "comment_apply_progress"        // Apply in progress (1 done, 1 running, 1 pending)
	PreviewCommentApplyCompleted      PreviewType = "comment_apply_completed"       // Apply completed (all tables done)
	PreviewCommentApplyFailed         PreviewType = "comment_apply_failed"          // Apply failed (1 done, 1 failed, 1 cancelled)
	PreviewCommentApplyStopped        PreviewType = "comment_apply_stopped"         // Apply stopped (1 done, 1 stopped)
	PreviewCommentApplyWaitingCutover PreviewType = "comment_apply_waiting_cutover" // Waiting for cutover
	PreviewCommentApplyCuttingOver    PreviewType = "comment_apply_cutting_over"    // Cutting over

	// Single-table apply comment previews (most common case)
	PreviewCommentSingleProgress          PreviewType = "comment_single_progress"            // Single table running
	PreviewCommentSingleComplete          PreviewType = "comment_single_complete"            // Single table completed
	PreviewCommentSingleFailed            PreviewType = "comment_single_failed"              // Single table failed
	PreviewCommentSingleStopped           PreviewType = "comment_single_stopped"             // Single table stopped
	PreviewCommentSummaryCompleted        PreviewType = "comment_summary_completed"          // Summary: completed
	PreviewCommentSummaryFailed           PreviewType = "comment_summary_failed"             // Summary: failed
	PreviewCommentSummaryStopped          PreviewType = "comment_summary_stopped"            // Summary: stopped
	PreviewCommentSummaryCompletedLarge   PreviewType = "comment_summary_completed_large"    // Summary: completed (8 tables, rollup)
	PreviewCommentSummaryFailedLarge      PreviewType = "comment_summary_failed_large"       // Summary: failed (8 tables, rollup)
	PreviewCommentSummaryMultiNSFailed    PreviewType = "comment_summary_multi_ns_failed"    // Summary: failed (multi-namespace)
	PreviewCommentSummaryMultiNSCompleted PreviewType = "comment_summary_multi_ns_completed" // Summary: completed (multi-namespace)
	PreviewCommentAll                     PreviewType = "comment_all"                        // Show all comment template previews

	// Apply command comment previews (GitHub PR apply commands)
	PreviewCommentApplyStartedType     PreviewType = "comment_apply_started"                // Apply started notification
	PreviewCommentApplyWaiting         PreviewType = "comment_apply_waiting"                // Waiting for cutover notification
	PreviewCommentUnlock               PreviewType = "comment_unlock"                       // Lock released confirmation
	PreviewCommentApplyBlocked         PreviewType = "comment_apply_blocked"                // Apply blocked by another PR
	PreviewCommentApplyBlockedCLI      PreviewType = "comment_apply_blocked_cli"            // Apply blocked by CLI session
	PreviewCommentApplyActive          PreviewType = "comment_apply_active"                 // Apply already in progress
	PreviewCommentApplyNoLock          PreviewType = "comment_apply_no_lock"                // No lock found
	PreviewCommentBlockedByPriorEnv    PreviewType = "comment_blocked_prior_env"            // Blocked by staging (pending)
	PreviewCommentBlockedByPriorFailed PreviewType = "comment_blocked_prior_env_failed"     // Blocked by staging (failed)
	PreviewCommentBlockedByPriorInProg PreviewType = "comment_blocked_prior_env_inprogress" // Blocked by staging (in progress)
	PreviewCommentReviewRequired       PreviewType = "comment_review_required"              // Review gate: approval needed
	PreviewCommentReviewGateError      PreviewType = "comment_review_gate_error"            // Review gate: fail-closed error
	PreviewCommentChecksGateFailing    PreviewType = "comment_checks_gate_failing"          // Checks gate: failing CI/lint
	PreviewCommentChecksGateInProgress PreviewType = "comment_checks_gate_in_progress"      // Checks gate: CI still running
	PreviewCommentActorNotAuthorized   PreviewType = "comment_actor_not_authorized"         // Actor authorization: user is not allowed
	PreviewCommentActorAuthUnavailable PreviewType = "comment_actor_auth_unavailable"       // Actor authorization: fail-closed error
	PreviewCommentApplyAllType         PreviewType = "comment_apply_all"                    // Show all apply comment previews
)
