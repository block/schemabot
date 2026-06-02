package templates

import (
	"fmt"
	"strings"

	webhooktemplates "github.com/block/schemabot/pkg/webhook/templates"
)

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
		{"CUTOVER COMMAND ACCEPTED", func() { fmt.Print(webhooktemplates.PreviewCommentCutoverCommandAccepted()) }},
		{"CUTOVER COMMAND ALREADY IN PROGRESS", func() { fmt.Print(webhooktemplates.PreviewCommentCutoverCommandAlreadyInProgress()) }},
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
		{"REVIEW REQUIRED", func() { fmt.Print(webhooktemplates.PreviewCommentReviewRequired()) }},
		{"REVIEW GATE ERROR (FAIL-CLOSED)", func() { fmt.Print(webhooktemplates.PreviewCommentReviewGateError()) }},
		{"ACTOR AUTHORIZATION: NOT AUTHORIZED", func() { fmt.Print(webhooktemplates.PreviewCommentPRCommandNotAuthorized()) }},
		{"ACTOR AUTHORIZATION: UNAVAILABLE", func() { fmt.Print(webhooktemplates.PreviewCommentPRCommandAuthorizationUnavailable()) }},
		{"CUTOVER COMMAND ACCEPTED", func() { fmt.Print(webhooktemplates.PreviewCommentCutoverCommandAccepted()) }},
		{"CUTOVER COMMAND ALREADY IN PROGRESS", func() { fmt.Print(webhooktemplates.PreviewCommentCutoverCommandAlreadyInProgress()) }},
		{"CHECKS GATE: FAILING", func() {
			fmt.Print(webhooktemplates.RenderApplyBlockedByFailingChecks("staging", []webhooktemplates.BlockingCheck{
				{Name: "CI / unit-tests", State: "failure"},
				{Name: "CI / lint", State: "timed_out"},
			}))
		}},
		{"CHECKS GATE: IN PROGRESS", func() {
			fmt.Print(webhooktemplates.RenderApplyBlockedByInProgressChecks("staging", []webhooktemplates.BlockingCheck{
				{Name: "CI / unit-tests", State: "in_progress"},
				{Name: "CI / integration-tests", State: "queued"},
			}))
		}},
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
		{"REVIEW REQUIRED", func() { fmt.Print(webhooktemplates.PreviewCommentReviewRequired()) }},
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
		{"CUTOVER COMMAND ACCEPTED", func() { fmt.Print(webhooktemplates.PreviewCommentCutoverCommandAccepted()) }},
		{"CUTOVER COMMAND ALREADY IN PROGRESS", func() { fmt.Print(webhooktemplates.PreviewCommentCutoverCommandAlreadyInProgress()) }},
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
		{"VITESS: PREPARING BRANCH", previewPreparingBranchOutput},
		{"VITESS: REFRESHING BRANCH (--branch)", previewRefreshingBranchOutput},
		{"VITESS: APPLYING BRANCH CHANGES", previewApplyingBranchChangesOutput},
		{"VITESS: VALIDATING BRANCH", previewValidatingBranchOutput},
		{"VITESS: CREATING DEPLOY REQUEST", previewCreatingDeployRequestOutput},
		{"VITESS: VALIDATING DEPLOY REQUEST", previewValidatingDeployRequestOutput},
		{"VITESS: STAGING SCHEMA CHANGES (0% with shards)", previewVitessStagingOutput},
		{"VITESS: RUNNING", previewVitessRunningOutput},
		{"VITESS: COMPLETED", previewVitessCompletedOutput},
		{"VITESS: MULTI-KEYSPACE COMPLETED WATCH", previewVitessMultiKeyspaceCompletedWatchOutput},
		{"VITESS: FAILED", previewVitessFailedOutput},
		{"VITESS: WAITING FOR DEPLOY", previewVitessWaitingForDeployOutput},
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
