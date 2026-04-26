package templates

import (
	"fmt"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
	webhooktemplates "github.com/block/schemabot/pkg/webhook/templates"
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
	PreviewApplyWatch   PreviewType = "apply_watch"   // Running with footer controls
	PreviewApplyStopped PreviewType = "apply_stopped" // Stopped by user

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
	PreviewCommentReviewRequired       PreviewType = "comment_review_required"              // Review gate: CODEOWNERS approval needed
	PreviewCommentReviewGateError      PreviewType = "comment_review_gate_error"            // Review gate: fail-closed error
	PreviewCommentApplyAllType         PreviewType = "comment_apply_all"                    // Show all apply comment previews
)

// PreviewCLIOutput prints sample CLI output for the given preview type.
func PreviewCLIOutput(previewType PreviewType) {
	switch previewType {
	case PreviewPlan:
		fmt.Println("=== MySQL Plan ===")
		fmt.Println()
		previewPlanOutput()
		fmt.Println()
		fmt.Println("=== Vitess Plan ===")
		fmt.Println()
		previewVitessPlanOutput()
	case PreviewProgress:
		previewProgressOutput()
	case PreviewWaitingForCutover:
		previewWaitingForCutoverOutput()
	case PreviewCuttingOver:
		previewCuttingOverOutput()
	case PreviewCompleted:
		previewCompletedOutput()
	case PreviewFailed:
		previewFailedOutput()
	case PreviewStopped:
		previewStoppedOutput()
	case PreviewStates:
		previewStatesOutput()
	case PreviewLockAcquired:
		previewLockAcquiredOutput()
	case PreviewLockConflict:
		previewLockConflictOutput()
	case PreviewLockConflictCLI:
		previewLockConflictByCLIOutput()
	case PreviewNoLockFound:
		previewNoLockFoundOutput()
	case PreviewUnlockNotOwned:
		previewUnlockNotOwnedOutput()
	case PreviewLockReleased:
		previewLockReleasedOutput()
	case PreviewLocksList:
		previewLocksListOutput()
	case PreviewApplyWatch:
		previewApplyWatchOutput()
	case PreviewApplyStopped:
		previewApplyStoppedOutput()
	case PreviewSeqPending:
		previewSeqPendingOutput()
	case PreviewSeqFirstRun:
		previewSeqFirstRunOutput()
	case PreviewSeqSecondRun:
		previewSeqSecondRunOutput()
	case PreviewSeqThirdRun:
		previewSeqThirdRunOutput()
	case PreviewSeqAllDone:
		previewSeqAllDoneOutput()
	case PreviewSeqFirstFail:
		previewSeqFirstFailOutput()
	case PreviewSeqMidFail:
		previewSeqMidFailOutput()
	case PreviewSeqStopped:
		previewSeqStoppedOutput()
	case PreviewSequentialAll:
		previewSequentialAllOutput()
	case PreviewDeferRunning:
		previewDeferRunningOutput()
	case PreviewDeferSingle:
		previewDeferSingleOutput()
	case PreviewDeferWaiting:
		previewDeferWaitingOutput()
	case PreviewDeferSeqWait:
		previewDeferSeqWaitOutput()
	case PreviewDeferStopped:
		previewDeferStoppedOutput()
	case PreviewDeferDetached:
		previewDeferDetachedOutput()
	case PreviewDeferCuttingAll:
		previewDeferCuttingOutput()
	case PreviewDeferAll:
		previewDeferAllOutput()
	case PreviewStopCommand:
		previewStopCommandOutput()
	case PreviewStartCommand:
		previewStartCommandOutput()
	case PreviewVolumeBar:
		previewVolumeBarOutput()
	case PreviewVolumeMode:
		previewVolumeModeOutput()
	case PreviewStatusList:
		previewStatusListOutput()
	case PreviewStatusHistory:
		previewStatusHistoryOutput()
	case PreviewLintViolations:
		previewLintViolationsOutput()
	case PreviewUnsafeBlocked:
		previewUnsafeBlockedOutput()
	case PreviewUnsafeAllowed:
		previewUnsafeAllowedOutput()
	case PreviewLintAll:
		previewLintAllOutput()
	// Comment template previews
	case PreviewCommentPlan:
		fmt.Print(webhooktemplates.PreviewCommentPlan())
	case PreviewCommentPlanEmpty:
		fmt.Print(webhooktemplates.PreviewCommentPlanNoChanges())
	case PreviewCommentMultiEnv:
		fmt.Print(webhooktemplates.PreviewCommentMultiEnvPlan())
	case PreviewCommentMultiEnvDiff:
		fmt.Print(webhooktemplates.PreviewCommentMultiEnvPlanDiff())
	case PreviewCommentMultiEnvLint:
		fmt.Print(webhooktemplates.PreviewCommentMultiEnvPlanLint())
	case PreviewCommentVitessPlan:
		fmt.Print(webhooktemplates.PreviewCommentVitessPlan())
	case PreviewCommentVitessApplyPlan:
		fmt.Print(webhooktemplates.PreviewCommentVitessApplyPlan())
	case PreviewCommentMySQLMultiSchema:
		fmt.Print(webhooktemplates.PreviewCommentMySQLMultiSchema())
	case PreviewCommentHelp:
		fmt.Print(webhooktemplates.PreviewCommentHelp())
	case PreviewCommentErrors:
		previewCommentErrorsOutput()
	case PreviewCommentUnsafeBlocked:
		fmt.Print(webhooktemplates.PreviewCommentUnsafeBlocked())
	case PreviewCommentApplyPlan:
		fmt.Print(webhooktemplates.PreviewCommentApplyPlan())
	case PreviewCommentApplyPlanOptions:
		fmt.Print(webhooktemplates.PreviewCommentApplyPlanOptions())
	case PreviewCommentApplyPlanUnsafe:
		fmt.Print(webhooktemplates.PreviewCommentApplyPlanUnsafe())
	case PreviewCommentApplyProgress:
		fmt.Print(webhooktemplates.PreviewCommentApplyProgress())
	case PreviewCommentApplyCompleted:
		fmt.Print(webhooktemplates.PreviewCommentApplyCompleted())
	case PreviewCommentApplyFailed:
		fmt.Print(webhooktemplates.PreviewCommentApplyFailed())
	case PreviewCommentApplyStopped:
		fmt.Print(webhooktemplates.PreviewCommentApplyStopped())
	case PreviewCommentApplyWaitingCutover:
		fmt.Print(webhooktemplates.PreviewCommentApplyWaitingForCutover())
	case PreviewCommentApplyCuttingOver:
		fmt.Print(webhooktemplates.PreviewCommentApplyCuttingOver())
	case PreviewCommentSingleProgress:
		fmt.Print(webhooktemplates.PreviewCommentApplySingleProgress())
	case PreviewCommentSingleComplete:
		fmt.Print(webhooktemplates.PreviewCommentApplySingleCompleted())
	case PreviewCommentSingleFailed:
		fmt.Print(webhooktemplates.PreviewCommentApplySingleFailed())
	case PreviewCommentSingleStopped:
		fmt.Print(webhooktemplates.PreviewCommentApplySingleStopped())
	case PreviewCommentSummaryCompleted:
		fmt.Print(webhooktemplates.PreviewCommentSummaryCompleted())
	case PreviewCommentSummaryFailed:
		fmt.Print(webhooktemplates.PreviewCommentSummaryFailed())
	case PreviewCommentSummaryStopped:
		fmt.Print(webhooktemplates.PreviewCommentSummaryStopped())
	case PreviewCommentSummaryCompletedLarge:
		fmt.Print(webhooktemplates.PreviewCommentSummaryCompletedLarge())
	case PreviewCommentSummaryFailedLarge:
		fmt.Print(webhooktemplates.PreviewCommentSummaryFailedLarge())
	case PreviewCommentSummaryMultiNSFailed:
		fmt.Print(webhooktemplates.PreviewCommentSummaryMultiNamespaceFailed())
	case PreviewCommentSummaryMultiNSCompleted:
		fmt.Print(webhooktemplates.PreviewCommentSummaryMultiNamespaceCompleted())
	case PreviewCommentAll:
		previewCommentAllOutput()
	// Apply command comment previews
	case PreviewCommentApplyStartedType:
		fmt.Print(webhooktemplates.PreviewCommentApplyStarted())
	case PreviewCommentApplyWaiting:
		fmt.Print(webhooktemplates.PreviewCommentApplyWaitingForCutover())
	case PreviewCommentUnlock:
		fmt.Print(webhooktemplates.PreviewCommentUnlockSuccess())
	case PreviewCommentApplyBlocked:
		fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByOtherPR())
	case PreviewCommentApplyBlockedCLI:
		fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByCLI())
	case PreviewCommentApplyActive:
		fmt.Print(webhooktemplates.PreviewCommentApplyInProgress())
	case PreviewCommentApplyNoLock:
		fmt.Print(webhooktemplates.PreviewCommentApplyConfirmNoLock())
	case PreviewCommentBlockedByPriorEnv:
		fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByPriorEnv())
	case PreviewCommentBlockedByPriorFailed:
		fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByPriorEnvFailed())
	case PreviewCommentBlockedByPriorInProg:
		fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByPriorEnvInProgress())
	case PreviewCommentReviewRequired:
		fmt.Print(webhooktemplates.PreviewCommentReviewRequired())
	case PreviewCommentReviewGateError:
		fmt.Print(webhooktemplates.PreviewCommentReviewGateError())
	case PreviewCommentApplyAllType:
		previewApplyCommandAllOutput()
	// Paired aggregate previews (PR + CLI subsections)
	case PreviewCommentPlanAll:
		previewCommentPlanAllOutput()
	case PreviewCommentLockingAll:
		previewCommentLockingAllOutput()
	case PreviewCommentApplyFlowAll:
		previewCommentApplyFlowAllOutput()
	case PreviewCLIPlanAll:
		previewCLIPlanAllOutput()
	case PreviewCLILockingAll:
		previewCLILockingAllOutput()
	case PreviewCLIApplyAll:
		previewCLIApplyAllOutput()
	case PreviewAll:
		fmt.Println("=" + strings.Repeat("=", 70))
		fmt.Println("PLAN OUTPUT")
		fmt.Println("=" + strings.Repeat("=", 70))
		previewPlanOutput()
		fmt.Println()

		fmt.Println("=" + strings.Repeat("=", 70))
		fmt.Println("PROGRESS OUTPUT (RUNNING)")
		fmt.Println("=" + strings.Repeat("=", 70))
		previewProgressOutput()
		fmt.Println()

		fmt.Println("=" + strings.Repeat("=", 70))
		fmt.Println("WAITING FOR CUTOVER OUTPUT")
		fmt.Println("=" + strings.Repeat("=", 70))
		previewWaitingForCutoverOutput()
		fmt.Println()

		fmt.Println("=" + strings.Repeat("=", 70))
		fmt.Println("CUTTING OVER OUTPUT")
		fmt.Println("=" + strings.Repeat("=", 70))
		previewCuttingOverOutput()
		fmt.Println()

		fmt.Println("=" + strings.Repeat("=", 70))
		fmt.Println("COMPLETED OUTPUT")
		fmt.Println("=" + strings.Repeat("=", 70))
		previewCompletedOutput()
		fmt.Println()

		fmt.Println("=" + strings.Repeat("=", 70))
		fmt.Println("FAILED OUTPUT")
		fmt.Println("=" + strings.Repeat("=", 70))
		previewFailedOutput()
		fmt.Println()

		fmt.Println("=" + strings.Repeat("=", 70))
		fmt.Println("STATE FORMATTING")
		fmt.Println("=" + strings.Repeat("=", 70))
		previewStatesOutput()
		fmt.Println()

		fmt.Println("=" + strings.Repeat("=", 70))
		fmt.Println("LOCK ACQUIRED OUTPUT")
		fmt.Println("=" + strings.Repeat("=", 70))
		previewLockAcquiredOutput()
		fmt.Println()

		fmt.Println("=" + strings.Repeat("=", 70))
		fmt.Println("LOCK CONFLICT OUTPUT")
		fmt.Println("=" + strings.Repeat("=", 70))
		previewLockConflictOutput()
		fmt.Println()

		fmt.Println("=" + strings.Repeat("=", 70))
		fmt.Println("LOCK RELEASED OUTPUT")
		fmt.Println("=" + strings.Repeat("=", 70))
		previewLockReleasedOutput()
		fmt.Println()

		fmt.Println("=" + strings.Repeat("=", 70))
		fmt.Println("LOCKS LIST OUTPUT")
		fmt.Println("=" + strings.Repeat("=", 70))
		previewLocksListOutput()
		fmt.Println()

		fmt.Println("=" + strings.Repeat("=", 70))
		fmt.Println("SEQUENTIAL MODE PREVIEWS")
		fmt.Println("=" + strings.Repeat("=", 70))
		previewSequentialAllOutput()
		fmt.Println()

		fmt.Println("=" + strings.Repeat("=", 70))
		fmt.Println("DEFER CUTOVER PREVIEWS")
		fmt.Println("=" + strings.Repeat("=", 70))
		previewDeferAllOutput()
		fmt.Println()

		fmt.Println("=" + strings.Repeat("=", 70))
		fmt.Println("APPLY WATCH MODE PREVIEWS")
		fmt.Println("=" + strings.Repeat("=", 70))
		previewApplyWatchOutput()
		fmt.Println()
		previewApplyStoppedOutput()
		fmt.Println()

		fmt.Println("=" + strings.Repeat("=", 70))
		fmt.Println("LINT AND UNSAFE PREVIEWS")
		fmt.Println("=" + strings.Repeat("=", 70))
		previewLintAllOutput()
		fmt.Println()

		fmt.Println("=" + strings.Repeat("=", 70))
		fmt.Println("COMMENT TEMPLATE PREVIEWS")
		fmt.Println("=" + strings.Repeat("=", 70))
		previewCommentAllOutput()
	}
}

// samplePlanChanges returns reusable DDL changes for plan preview functions.
func samplePlanChanges() []DDLChange {
	return []DDLChange{
		{ChangeType: "CREATE", TableName: "users", DDL: "CREATE TABLE `users` (`id` bigint NOT NULL AUTO_INCREMENT, `email` varchar(255) NOT NULL, `created_at` timestamp DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (`id`), INDEX `idx_email` (`email`))"},
		{ChangeType: "CREATE", TableName: "orders", DDL: "CREATE TABLE `orders` (`id` bigint NOT NULL AUTO_INCREMENT, `user_id` bigint NOT NULL, `total_cents` bigint NOT NULL, `status` varchar(50) NOT NULL DEFAULT 'pending', PRIMARY KEY (`id`), INDEX `idx_user_id` (`user_id`))"},
		{ChangeType: "ALTER", TableName: "products", DDL: "ALTER TABLE `products` ADD INDEX `idx_category_price` (`category`, `price`)"},
	}
}

// samplePlanLintViolations returns reusable lint violations for plan preview functions.
func samplePlanLintViolations() []apitypes.LintViolationResponse {
	return []apitypes.LintViolationResponse{
		{Message: "has_float: New column uses floating-point data type", Table: "orders", Linter: "has_float"},
		{Message: "no_default: Column added without DEFAULT value", Table: "users", Linter: "no_default"},
	}
}

func previewPlanOutput() {
	WritePlanHeader(PlanHeaderData{
		Database:    "testapp",
		SchemaName:  "testapp",
		Environment: "staging",
		IsMySQL:     true,
	})

	changes := samplePlanChanges()
	WriteSQLChanges(changes)
	WriteLintViolations(samplePlanLintViolations())
	WritePlanSummary(changes)
	WriteOptions(true, false) // Show defer cutover option
}

func previewVitessPlanOutput() {
	WritePlanHeader(PlanHeaderData{
		Database:    "commerce",
		SchemaName:  "commerce",
		Environment: "staging",
		IsMySQL:     false,
	})

	namespaces := []NamespaceChange{
		{
			Namespace: "commerce",
			Changes: []DDLChange{
				{TableName: "orders", DDL: "ALTER TABLE `orders` ADD COLUMN `region` varchar(50) NOT NULL DEFAULT '', ADD INDEX `idx_region` (`region`)", ChangeType: "alter"},
			},
			VSchemaChanged: true,
			VSchemaDiff: `--- a/commerce.json
+++ b/commerce.json
@@ -12,6 +12,10 @@
       "auto_increment": {
         "column": "id",
         "sequence": "orders_seq"
+      },
+      "column_vindexes": [
+        { "column": "region", "name": "region_map" }
+      ]
       }
     }
   }`,
		},
		{
			Namespace: "customer",
			Changes: []DDLChange{
				{TableName: "addresses", DDL: "CREATE TABLE `addresses` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `customer_id` bigint NOT NULL,\n  `street` varchar(255) NOT NULL,\n  `city` varchar(100) NOT NULL,\n  PRIMARY KEY (`id`),\n  INDEX `idx_customer_id` (`customer_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", ChangeType: "create"},
			},
		},
	}
	WriteNamespaceChanges(namespaces, false, "commerce")

	// Flat summary across all namespaces
	var allChanges []DDLChange
	for _, ns := range namespaces {
		allChanges = append(allChanges, ns.Changes...)
	}
	WritePlanSummary(allChanges)
}

func previewPlanNoChangesOutput() {
	WritePlanHeader(PlanHeaderData{
		Database:    "testapp",
		SchemaName:  "testapp",
		Environment: "staging",
		IsMySQL:     true,
	})
	WriteNoChanges()
}

func previewMultiEnvPlanOutput() {
	// Multi-env identical: no environment in header, plans deduplicated
	WritePlanHeader(PlanHeaderData{
		Database:   "testapp",
		SchemaName: "testapp",
		IsMySQL:    true,
	})
	changes := samplePlanChanges()
	WriteSQLChanges(changes)
	WritePlanSummary(changes)
}

func previewMultiEnvPlanDiffOutput() {
	// Multi-env different: separate per-environment sections
	WritePlanHeader(PlanHeaderData{
		Database:   "testapp",
		SchemaName: "testapp",
		IsMySQL:    true,
	})
	WriteEnvironmentHeader("staging")
	WriteNoChanges()
	WriteEnvironmentHeader("production")
	changes := samplePlanChanges()
	WriteSQLChanges(changes)
	WritePlanSummary(changes)
}

func previewMultiEnvPlanLintOutput() {
	// Multi-env identical with lint violations
	WritePlanHeader(PlanHeaderData{
		Database:   "testapp",
		SchemaName: "testapp",
		IsMySQL:    true,
	})
	changes := samplePlanChanges()
	WriteSQLChanges(changes)
	WriteLintViolations(samplePlanLintViolations())
	WritePlanSummary(changes)
}

func previewLockAcquiredOutput() {
	WriteLockAcquired(LockData{
		Database:     "testapp",
		DatabaseType: "mysql",
		Owner:        "cli:aparajon@macbook",
		CreatedAt:    previewTime,
	})
}

func previewLockConflictOutput() {
	WriteLockConflict(LockConflictData{
		Database:     "testapp",
		DatabaseType: "mysql",
		Owner:        "block/schemabot#123",
		Repository:   "block/schemabot",
		PullRequest:  123,
		CreatedAt:    previewTime.Add(-2 * time.Hour),
	})
}

func previewLockReleasedOutput() {
	WriteLockReleased("testapp", "mysql")
	fmt.Println()
	fmt.Println("--- Force release ---")
	fmt.Println()
	WriteLockForceReleased("testapp", "mysql", "block/schemabot#123")
}

func previewLocksListOutput() {
	locks := []LockData{
		{
			Database:     "testapp",
			DatabaseType: "mysql",
			Owner:        "cli:aparajon@macbook",
			CreatedAt:    previewTime.Add(-30 * time.Minute),
			UpdatedAt:    previewTime.Add(-5 * time.Minute),
		},
		{
			Database:     "payments",
			DatabaseType: "vitess",
			Owner:        "block/payments-api#456",
			Repository:   "block/payments-api",
			PullRequest:  456,
			CreatedAt:    previewTime.Add(-3 * time.Hour),
		},
	}
	WriteLocksList(locks)
	fmt.Println("--- No locks ---")
	fmt.Println()
	WriteLocksList(nil)
}

func previewLockConflictByCLIOutput() {
	WriteLockConflict(LockConflictData{
		Database:     "testapp",
		DatabaseType: "mysql",
		Owner:        "cli:deploy@prod-host.example.com",
		CreatedAt:    previewTime.Add(-45 * time.Minute),
	})
}

func previewNoLockFoundOutput() {
	WriteNoLockFound("testapp", "mysql")
}

func previewUnlockNotOwnedOutput() {
	WriteUnlockNotOwned("testapp", "mysql", "block/schemabot#123")
}

func previewStatesOutput() {
	// Show all state formatting
	states := []string{
		"STATE_PENDING",
		state.Apply.Running,
		state.Apply.WaitingForCutover,
		"STATE_CUTTING_OVER",
		state.Apply.Completed,
		state.Apply.Failed,
		"STATE_IDLE",
		"STATE_NO_ACTIVE_CHANGE",
	}

	fmt.Println("State Display Formatting:")
	fmt.Println()
	for _, s := range states {
		fmt.Printf("  %-30s → %s\n", s, FormatProgressState(s))
	}
}

// =============================================================================
// Sequential Mode Previews
// =============================================================================

// Common DDLs used in sequential mode examples
var seqDDLs = []struct {
	table string
	ddl   string
}{
	{"users", "ALTER TABLE `users` ADD INDEX `idx_email_created` (`email`, `created_at`)"},
	{"orders", "ALTER TABLE `orders` ADD INDEX `idx_user_status` (`user_id`, `status`)"},
	{"products", "ALTER TABLE `products` ADD COLUMN `weight_grams` INT DEFAULT 0"},
}

func previewStatusListOutput() {
	WriteStatusList(StatusListData{
		ActiveCount: 3,
		Applies: []ActiveApplyData{
			{
				ApplyID:     "apply_abc123",
				Database:    "orders-db",
				Environment: "staging",
				State:       state.Apply.Running,
				Engine:      "Spirit",
				StartedAt:   previewTime.Add(-15 * time.Minute).Format(time.RFC3339),
				UpdatedAt:   previewTime.Add(-30 * time.Second).Format(time.RFC3339),
				Volume:      4,
			},
			{
				ApplyID:     "apply_def456",
				Database:    "users-db",
				Environment: "production",
				State:       state.Apply.WaitingForCutover,
				Engine:      "Spirit",
				StartedAt:   previewTime.Add(-45 * time.Minute).Format(time.RFC3339),
				UpdatedAt:   previewTime.Add(-1 * time.Minute).Format(time.RFC3339),
				Volume:      6,
			},
			{
				ApplyID:     "apply_ghi789",
				Database:    "analytics",
				Environment: "staging",
				State:       state.Apply.Stopped,
				Engine:      "Spirit",
				StartedAt:   previewTime.Add(-2 * time.Hour).Format(time.RFC3339),
				UpdatedAt:   previewTime.Add(-30 * time.Minute).Format(time.RFC3339),
			},
		},
	})

	fmt.Println()
	fmt.Println("No active applies:")
	fmt.Println()
	WriteStatusList(StatusListData{
		ActiveCount: 0,
		Applies:     nil,
	})
}

func previewStatusHistoryOutput() {
	WriteDatabaseHistory(DatabaseHistoryData{
		Database: "orders-db",
		Applies: []ApplyHistoryData{
			{
				ApplyID:     "apply_abc123",
				Environment: "staging",
				State:       state.Apply.Completed,
				Engine:      "Spirit",
				Caller:      "cli",
				StartedAt:   previewTime.Add(-1 * time.Hour).Format(time.RFC3339),
				CompletedAt: previewTime.Add(-45 * time.Minute).Format(time.RFC3339),
			},
			{
				ApplyID:     "apply_def456",
				Environment: "staging",
				State:       state.Apply.Running,
				Engine:      "Spirit",
				Caller:      "PR 42",
				StartedAt:   previewTime.Add(-15 * time.Minute).Format(time.RFC3339),
			},
			{
				ApplyID:     "apply_ghi789",
				Environment: "production",
				State:       state.Apply.Failed,
				Engine:      "Spirit",
				Caller:      "PR 42",
				StartedAt:   previewTime.Add(-3 * time.Hour).Format(time.RFC3339),
				CompletedAt: previewTime.Add(-2*time.Hour - 30*time.Minute).Format(time.RFC3339),
				Error:       "lock timeout exceeded",
			},
			{
				ApplyID:     "apply_jkl012",
				Environment: "production",
				State:       state.Apply.Completed,
				Engine:      "Spirit",
				Caller:      "cli",
				StartedAt:   previewTime.Add(-24 * time.Hour).Format(time.RFC3339),
				CompletedAt: previewTime.Add(-23*time.Hour - 30*time.Minute).Format(time.RFC3339),
			},
		},
	})

	fmt.Println()
	fmt.Println("Empty database:")
	fmt.Println()
	WriteDatabaseHistory(DatabaseHistoryData{
		Database: "new-db",
		Applies:  nil,
	})
}

// =============================================================================
// Lint and Unsafe Previews
// =============================================================================

func previewLintViolationsOutput() {
	fmt.Println("Lint violations: Non-blocking warnings during plan/apply")
	fmt.Println()

	warnings := []apitypes.LintViolationResponse{
		{Message: "has_float: New column uses floating-point data type", Table: "orders", Linter: "has_float"},
		{Message: "no_default: Column added without DEFAULT value", Table: "users", Linter: "no_default"},
	}
	WriteLintViolations(warnings)
}

func previewUnsafeBlockedOutput() {
	fmt.Println("Unsafe blocked: Destructive changes require --allow-unsafe")
	fmt.Println()

	changes := []UnsafeChange{
		{Table: "users", Reason: "DROP COLUMN email", ChangeType: "DROP COLUMN"},
		{Table: "orders", Reason: "DROP TABLE", ChangeType: "DROP TABLE"},
		{Table: "products", Reason: "MODIFY COLUMN price_cents: INT → SMALLINT (potential data loss); DROP INDEX idx_category", ChangeType: "MODIFY COLUMN"},
	}
	WriteUnsafeChangesBlocked(changes, "testapp", "staging", "./schema/testapp")
}

func previewUnsafeAllowedOutput() {
	fmt.Println("Unsafe allowed: Proceeding with --allow-unsafe flag")
	fmt.Println()

	changes := []UnsafeChange{
		{Table: "users", Reason: "DROP COLUMN email", ChangeType: "DROP COLUMN"},
		{Table: "orders", Reason: "DROP TABLE", ChangeType: "DROP TABLE"},
	}
	WriteUnsafeWarningAllowed(changes)
}

func previewLintAllOutput() {
	sections := []struct {
		name string
		fn   func()
	}{
		{"LINT WARNINGS", previewLintViolationsOutput},
		{"UNSAFE CHANGES BLOCKED", previewUnsafeBlockedOutput},
		{"UNSAFE CHANGES ALLOWED", previewUnsafeAllowedOutput},
	}

	for i, s := range sections {
		if i > 0 {
			fmt.Println()
		}
		fmt.Println("---", s.name, strings.Repeat("-", 50-len(s.name)))
		fmt.Println()
		s.fn()
	}
}

// =============================================================================
// Comment Template Previews (GitHub PR comments)
// =============================================================================

func previewCommentErrorsOutput() {
	sections := []struct {
		name string
		fn   func() string
	}{
		{"NO CONFIG (no -d flag)", webhooktemplates.PreviewCommentErrorNoConfig},
		{"MULTIPLE DATABASES", webhooktemplates.PreviewCommentErrorMultiple},
		{"DATABASE NOT FOUND", webhooktemplates.PreviewCommentErrorNotFound},
		{"INVALID CONFIG", webhooktemplates.PreviewCommentErrorInvalid},
		{"GENERIC ERROR", webhooktemplates.PreviewCommentErrorGeneric},
		{"MISSING -e FLAG", webhooktemplates.PreviewCommentMissingEnv},
		{"INVALID COMMAND", webhooktemplates.PreviewCommentInvalidCmd},
	}

	for i, s := range sections {
		if i > 0 {
			fmt.Println()
		}
		fmt.Println("---", s.name, strings.Repeat("-", 50-len(s.name)))
		fmt.Println()
		fmt.Print(s.fn())
		fmt.Println()
	}
}

func previewCommentAllOutput() {
	sections := []struct {
		name string
		fn   func()
	}{
		{"PLAN COMMENT", func() { fmt.Print(webhooktemplates.PreviewCommentPlan()) }},
		{"PLAN COMMENT (NO CHANGES)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanNoChanges()) }},
		{"APPLY PLAN (LOCKED)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyPlan()) }},
		{"APPLY PLAN (UNSAFE + ALLOWED)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyPlanUnsafe()) }},
		{"UNSAFE CHANGES BLOCKED", func() { fmt.Print(webhooktemplates.PreviewCommentUnsafeBlocked()) }},
		{"MULTI-ENV PLAN (IDENTICAL)", func() { fmt.Print(webhooktemplates.PreviewCommentMultiEnvPlan()) }},
		{"MULTI-ENV PLAN (DIFFERENT)", func() { fmt.Print(webhooktemplates.PreviewCommentMultiEnvPlanDiff()) }},
		{"MULTI-ENV PLAN (ERROR)", func() { fmt.Print(webhooktemplates.PreviewCommentMultiEnvPlanError()) }},
		{"MULTI-ENV PLAN (LINT WARNINGS)", func() { fmt.Print(webhooktemplates.PreviewCommentMultiEnvPlanLint()) }},
		{"HELP COMMENT", func() { fmt.Print(webhooktemplates.PreviewCommentHelp()) }},
		{"NO CONFIG (NO -D FLAG)", func() { fmt.Print(webhooktemplates.PreviewCommentErrorNoConfig()) }},
		{"MULTIPLE DATABASES", func() { fmt.Print(webhooktemplates.PreviewCommentErrorMultiple()) }},
		{"DATABASE NOT FOUND", func() { fmt.Print(webhooktemplates.PreviewCommentErrorNotFound()) }},
		{"INVALID CONFIG", func() { fmt.Print(webhooktemplates.PreviewCommentErrorInvalid()) }},
		{"GENERIC ERROR", func() { fmt.Print(webhooktemplates.PreviewCommentErrorGeneric()) }},
		{"MISSING -E FLAG", func() { fmt.Print(webhooktemplates.PreviewCommentMissingEnv()) }},
		{"INVALID COMMAND", func() { fmt.Print(webhooktemplates.PreviewCommentInvalidCmd()) }},
		{"APPLY IN PROGRESS", func() { fmt.Print(webhooktemplates.PreviewCommentApplyProgress()) }},
		{"APPLY COMPLETED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyCompleted()) }},
		{"APPLY FAILED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyFailed()) }},
		{"APPLY STOPPED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyStopped()) }},
		{"APPLY WAITING FOR CUTOVER", func() { fmt.Print(webhooktemplates.PreviewCommentApplyWaitingForCutover()) }},
		{"APPLY CUTTING OVER", func() { fmt.Print(webhooktemplates.PreviewCommentApplyCuttingOver()) }},
		{"SUMMARY: COMPLETED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryCompleted()) }},
		{"SUMMARY: FAILED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryFailed()) }},
		{"SUMMARY: STOPPED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryStopped()) }},
		{"SUMMARY: COMPLETED (LARGE)", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryCompletedLarge()) }},
		{"SUMMARY: FAILED (LARGE)", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryFailedLarge()) }},
		{"SUMMARY: MULTI-NAMESPACE FAILED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryMultiNamespaceFailed()) }},
		{"SUMMARY: MULTI-NAMESPACE COMPLETED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryMultiNamespaceCompleted()) }},
	}

	for i, s := range sections {
		if i > 0 {
			fmt.Println()
		}
		fmt.Println("---", s.name, strings.Repeat("-", 50-len(s.name)))
		fmt.Println()
		s.fn()
		fmt.Println()
	}
}

func previewApplyCommandAllOutput() {
	sections := []struct {
		name string
		fn   func()
	}{
		{"APPLY PLAN (LOCK + CONFIRM)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyPlan()) }},
		{"APPLY PLAN (WITH OPTIONS)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyPlanOptions()) }},
		{"APPLY STARTED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyStarted()) }},
		{"UNLOCK SUCCESS", func() { fmt.Print(webhooktemplates.PreviewCommentUnlockSuccess()) }},
		{"APPLY BLOCKED BY OTHER PR", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByOtherPR()) }},
		{"APPLY BLOCKED BY CLI", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByCLI()) }},
		{"APPLY ALREADY IN PROGRESS", func() { fmt.Print(webhooktemplates.PreviewCommentApplyInProgress()) }},
		{"NO LOCK FOUND", func() { fmt.Print(webhooktemplates.PreviewCommentApplyConfirmNoLock()) }},
		{"BLOCKED BY PRIOR ENV (PENDING)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByPriorEnv()) }},
		{"BLOCKED BY PRIOR ENV (FAILED)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByPriorEnvFailed()) }},
		{"BLOCKED BY PRIOR ENV (IN PROGRESS)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByPriorEnvInProgress()) }},
		{"REVIEW REQUIRED (CODEOWNERS)", func() { fmt.Print(webhooktemplates.PreviewCommentReviewRequired()) }},
		{"REVIEW GATE ERROR (FAIL-CLOSED)", func() { fmt.Print(webhooktemplates.PreviewCommentReviewGateError()) }},
	}

	for i, s := range sections {
		if i > 0 {
			fmt.Println()
		}
		fmt.Println("---", s.name, strings.Repeat("-", 50-len(s.name)))
		fmt.Println()
		s.fn()
		fmt.Println()
	}
}

// =============================================================================
// Paired Aggregate Previews (used by update-templates.sh for grouped sections)
// =============================================================================

func previewCommentPlanAllOutput() {
	sections := []struct {
		name string
		fn   func()
	}{
		{"MYSQL PLAN", func() { fmt.Print(webhooktemplates.PreviewCommentPlan()) }},
		{"MYSQL PLAN (NO CHANGES)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanNoChanges()) }},
		{"VITESS PLAN", func() { fmt.Print(webhooktemplates.PreviewCommentVitessPlan()) }},
		{"VITESS APPLY PLAN (LOCKED + OPTIONS)", func() { fmt.Print(webhooktemplates.PreviewCommentVitessApplyPlan()) }},
		{"MYSQL MULTI-SCHEMA PLAN", func() { fmt.Print(webhooktemplates.PreviewCommentMySQLMultiSchema()) }},
		{"MULTI-ENV PLAN (IDENTICAL)", func() { fmt.Print(webhooktemplates.PreviewCommentMultiEnvPlan()) }},
		{"MULTI-ENV PLAN (DIFFERENT)", func() { fmt.Print(webhooktemplates.PreviewCommentMultiEnvPlanDiff()) }},
		{"MULTI-ENV PLAN (ERROR)", func() { fmt.Print(webhooktemplates.PreviewCommentMultiEnvPlanError()) }},
		{"MULTI-ENV PLAN (LINT WARNINGS)", func() { fmt.Print(webhooktemplates.PreviewCommentMultiEnvPlanLint()) }},
		{"HELP COMMENT", func() { fmt.Print(webhooktemplates.PreviewCommentHelp()) }},
		{"NO CONFIG (NO -D FLAG)", func() { fmt.Print(webhooktemplates.PreviewCommentErrorNoConfig()) }},
		{"MULTIPLE DATABASES", func() { fmt.Print(webhooktemplates.PreviewCommentErrorMultiple()) }},
		{"DATABASE NOT FOUND", func() { fmt.Print(webhooktemplates.PreviewCommentErrorNotFound()) }},
		{"INVALID CONFIG", func() { fmt.Print(webhooktemplates.PreviewCommentErrorInvalid()) }},
		{"GENERIC ERROR", func() { fmt.Print(webhooktemplates.PreviewCommentErrorGeneric()) }},
		{"MISSING -E FLAG", func() { fmt.Print(webhooktemplates.PreviewCommentMissingEnv()) }},
		{"INVALID COMMAND", func() { fmt.Print(webhooktemplates.PreviewCommentInvalidCmd()) }},
	}
	printSections(sections)
}

func previewCommentLockingAllOutput() {
	sections := []struct {
		name string
		fn   func()
	}{
		{"APPLY BLOCKED BY OTHER PR", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByOtherPR()) }},
		{"APPLY BLOCKED BY CLI", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByCLI()) }},
		{"UNLOCK SUCCESS", func() { fmt.Print(webhooktemplates.PreviewCommentUnlockSuccess()) }},
		{"APPLY ALREADY IN PROGRESS", func() { fmt.Print(webhooktemplates.PreviewCommentApplyInProgress()) }},
		{"NO LOCK FOUND", func() { fmt.Print(webhooktemplates.PreviewCommentApplyConfirmNoLock()) }},
		{"BLOCKED BY PRIOR ENV (PENDING)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByPriorEnv()) }},
		{"BLOCKED BY PRIOR ENV (FAILED)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByPriorEnvFailed()) }},
		{"BLOCKED BY PRIOR ENV (IN PROGRESS)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByPriorEnvInProgress()) }},
		{"REVIEW REQUIRED (CODEOWNERS)", func() { fmt.Print(webhooktemplates.PreviewCommentReviewRequired()) }},
		{"REVIEW GATE ERROR (FAIL-CLOSED)", func() { fmt.Print(webhooktemplates.PreviewCommentReviewGateError()) }},
	}
	printSections(sections)
}

func previewCommentApplyFlowAllOutput() {
	sections := []struct {
		name string
		fn   func()
	}{
		{"MYSQL APPLY PLAN (LOCK + CONFIRM)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyPlan()) }},
		{"MYSQL APPLY PLAN (WITH OPTIONS)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyPlanOptions()) }},
		{"VITESS APPLY PLAN (LOCKED + OPTIONS)", func() { fmt.Print(webhooktemplates.PreviewCommentVitessApplyPlan()) }},
		{"APPLY STARTED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyStarted()) }},
		// Single-table (most common case)
		{"SINGLE TABLE: RUNNING", func() { fmt.Print(webhooktemplates.PreviewCommentApplySingleProgress()) }},
		{"SINGLE TABLE: COMPLETED", func() { fmt.Print(webhooktemplates.PreviewCommentApplySingleCompleted()) }},
		{"SINGLE TABLE: FAILED", func() { fmt.Print(webhooktemplates.PreviewCommentApplySingleFailed()) }},
		{"SINGLE TABLE: STOPPED", func() { fmt.Print(webhooktemplates.PreviewCommentApplySingleStopped()) }},
		// Multi-table sequential progression
		{"ALL PENDING", func() { fmt.Print(webhooktemplates.PreviewCommentApplyAllPending()) }},
		{"FIRST TABLE RUNNING", func() { fmt.Print(webhooktemplates.PreviewCommentApplyFirstRunning()) }},
		{"SECOND TABLE RUNNING", func() { fmt.Print(webhooktemplates.PreviewCommentApplyProgress()) }},
		{"THIRD TABLE RUNNING", func() { fmt.Print(webhooktemplates.PreviewCommentApplyThirdRunning()) }},
		{"ALL COMPLETED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyCompleted()) }},
		{"FIRST TABLE FAILED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyFirstFailed()) }},
		{"MIDDLE TABLE FAILED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyFailed()) }},
		{"STOPPED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyStopped()) }},
		// Cutover states
		{"WAITING FOR CUTOVER", func() { fmt.Print(webhooktemplates.PreviewCommentApplyWaitingForCutover()) }},
		{"CUTTING OVER", func() { fmt.Print(webhooktemplates.PreviewCommentApplyCuttingOver()) }},
		// Summaries
		{"SUMMARY: COMPLETED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryCompleted()) }},
		{"SUMMARY: FAILED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryFailed()) }},
		{"SUMMARY: STOPPED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryStopped()) }},
		{"SUMMARY: COMPLETED (LARGE)", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryCompletedLarge()) }},
		{"SUMMARY: FAILED (LARGE)", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryFailedLarge()) }},
		{"SUMMARY: MULTI-NAMESPACE FAILED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryMultiNamespaceFailed()) }},
		{"SUMMARY: MULTI-NAMESPACE COMPLETED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryMultiNamespaceCompleted()) }},
	}
	printSections(sections)
}

func previewCLIPlanAllOutput() {
	sections := []struct {
		name string
		fn   func()
	}{
		{"PLAN (MYSQL)", previewPlanOutput},
		{"PLAN (NO CHANGES)", previewPlanNoChangesOutput},
		{"PLAN (VITESS)", previewVitessPlanOutput},
		{"MULTI-ENV PLAN (IDENTICAL)", previewMultiEnvPlanOutput},
		{"MULTI-ENV PLAN (DIFFERENT)", previewMultiEnvPlanDiffOutput},
		{"MULTI-ENV PLAN (LINT WARNINGS)", previewMultiEnvPlanLintOutput},
	}
	printSections(sections)
}

func previewCLILockingAllOutput() {
	sections := []struct {
		name string
		fn   func()
	}{
		{"LOCK ACQUIRED", previewLockAcquiredOutput},
		{"LOCK CONFLICT (PR)", previewLockConflictOutput},
		{"LOCK CONFLICT (CLI)", previewLockConflictByCLIOutput},
		{"LOCK RELEASED", previewLockReleasedOutput},
		{"NO LOCK FOUND", previewNoLockFoundOutput},
		{"UNLOCK NOT OWNED", previewUnlockNotOwnedOutput},
		{"LOCKS LIST", previewLocksListOutput},
	}
	printSections(sections)
}

func previewCLIApplyAllOutput() {
	sections := []struct {
		name string
		fn   func()
	}{
		// MySQL: single table
		{"MYSQL: SINGLE TABLE RUNNING", previewProgressOutput},
		{"MYSQL: SINGLE TABLE COMPLETED", previewCompletedOutput},
		{"MYSQL: SINGLE TABLE FAILED", previewFailedOutput},
		{"MYSQL: SINGLE TABLE STOPPED", previewStoppedOutput},
		{"MYSQL: SINGLE TABLE WAITING FOR CUTOVER", previewWaitingForCutoverOutput},
		{"MYSQL: SINGLE TABLE CUTTING OVER", previewCuttingOverOutput},
		// MySQL: multi-table sequential
		{"MYSQL: MULTI-TABLE ALL PENDING", previewSeqPendingOutput},
		{"MYSQL: MULTI-TABLE FIRST TABLE RUNNING", previewSeqFirstRunOutput},
		{"MYSQL: MULTI-TABLE SECOND TABLE RUNNING", previewSeqSecondRunOutput},
		{"MYSQL: MULTI-TABLE THIRD TABLE RUNNING", previewSeqThirdRunOutput},
		{"MYSQL: MULTI-TABLE ALL COMPLETED", previewSeqAllDoneOutput},
		{"MYSQL: MULTI-TABLE FIRST TABLE FAILED", previewSeqFirstFailOutput},
		{"MYSQL: MULTI-TABLE MIDDLE TABLE FAILED", previewSeqMidFailOutput},
		{"MYSQL: MULTI-TABLE STOPPED", previewSeqStoppedOutput},
		// Vitess: PlanetScale lifecycle
		{"VITESS: CREATING BRANCH", previewCreatingBranchOutput},
		{"VITESS: APPLYING BRANCH CHANGES", previewApplyingBranchChangesOutput},
		{"VITESS: CREATING DEPLOY REQUEST", previewCreatingDeployRequestOutput},
		{"VITESS: STAGING SCHEMA CHANGES (0% with shards)", previewVitessStagingOutput},
		{"VITESS: RUNNING", previewVitessRunningOutput},
		{"VITESS: COMPLETED", previewVitessCompletedOutput},
		{"VITESS: MULTI-KEYSPACE COMPLETED WATCH", previewVitessMultiKeyspaceCompletedWatchOutput},
		{"VITESS: FAILED", previewVitessFailedOutput},
		{"VITESS: WAITING FOR CUTOVER", previewVitessWaitingForCutoverOutput},
		{"VITESS: CUTTING OVER", previewVitessCuttingOverOutput},
		{"VITESS: CANCELLED", previewVitessCancelledOutput},
		{"VITESS: LARGE SHARD COUNT (256 shards)", previewVitessLargeShardCountOutput},
		{"VITESS: MANY KEYSPACES (33 keyspaces)", previewVitessManyKeyspacesOutput},
		{"VITESS: VSCHEMA-ONLY UPDATE", previewVitessVSchemaOnlyOutput},
		{"VITESS: MULTI-KEYSPACE", previewVitessMultiKeyspaceOutput},
		{"VITESS: DDL + VSCHEMA", previewVitessDDLWithVSchemaOutput},
		{"VITESS: SHARD PROGRESS", previewVitessShardProgressOutput},
		{"VITESS: CUTOVER RETRY", previewVitessCutoverRetryOutput},
		// Vitess: plan rendering
		{"VITESS: PLAN (DDL + VSCHEMA)", previewVSchemaPlanOutput},
		{"VITESS: PLAN (VSCHEMA-ONLY)", previewVSchemaOnlyOutput},
		{"VITESS: PLAN (MULTI-KEYSPACE)", previewMultiKeyspacePlanOutput},
		{"VITESS: INSTANT DDL", previewVitessInstantDDLOutput},
		{"VITESS: REVERT WINDOW", previewVitessRevertWindowOutput},
		// CLI-only: interactive commands
		{"APPLY WATCH MODE", previewApplyWatchOutput},
		{"STOP COMMAND", previewStopCommandOutput},
		{"START COMMAND", previewStartCommandOutput},
		{"VOLUME MODE", previewVolumeModeOutput},
		{"STATUS LIST", previewStatusListOutput},
		{"STATUS HISTORY", previewStatusHistoryOutput},
	}
	printSections(sections)
}

// printSections renders a list of named sections with --- separators.
func printSections(sections []struct {
	name string
	fn   func()
}) {
	for i, s := range sections {
		if i > 0 {
			fmt.Println()
		}
		fmt.Println("---", s.name, strings.Repeat("-", 50-len(s.name)))
		fmt.Println()
		s.fn()
	}
}
