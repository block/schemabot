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
		{"UNMANAGED SCHEMA CONFIGS NOTICE", webhooktemplates.PreviewCommentUnmanagedSchemaConfigsNotice},
		{"GENERIC ERROR", webhooktemplates.PreviewCommentErrorGeneric},
		{"AUTO-PLAN: GENERIC ERROR", webhooktemplates.PreviewCommentErrorGenericAutoPlan},
		{"MISSING -e FLAG", webhooktemplates.PreviewCommentMissingEnv},
		{"INVALID COMMAND", webhooktemplates.PreviewCommentInvalidCmd},
	}

	for i, s := range sections {
		if i > 0 {
			fmt.Println()
		}
		fmt.Println("---", s.name, strings.Repeat("-", max(50-len(s.name), 3)))
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
		{"PLAN COMMENT (IGNORED NAMESPACES)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanIgnoredNamespaces()) }},
		{"PLAN COMMENT (MANY LINT WARNINGS)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanManyLintWarnings()) }},
		{"PLAN COMMENT (ENGINE-BLOCKED CHANGE)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanBlocked()) }},
		{"PLAN COMMENT (DIRECT-EXECUTION CHANGE)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanDirect()) }},
		{"PLAN COMMENT (EXISTING COPY DISCARDED)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanCopyDiscarded()) }},
		{"PLAN COMMENT (EXISTING COPY DISCARDED, APPLYING)", func() {
			fmt.Print(webhooktemplates.PreviewCommentPlanCopyDiscardedApplying())
		}},
		{"PLAN COMMENT (EXISTING COPY DISCARDED, PAUSED)", func() {
			fmt.Print(webhooktemplates.PreviewCommentPlanCopyDiscardedPaused())
		}},
		{"PLAN COMMENT (EXISTING COPY DISCARDED, CONFIRM STOPPED)", func() {
			fmt.Print(webhooktemplates.PreviewCommentPlanCopyDiscardedStopped())
		}},
		{"PLAN COMMENT (EXISTING COPY ADOPTED)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanCopyAdopted()) }},
		{"PLAN COMMENT (EXISTING COPY RUNNING)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanCopyRunning()) }},
		{"APPLY REJECTED (ENGINE-BLOCKED CHANGES)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedRejected()) }},
		{"PLAN COMMENT (TENANT TARGET)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanTenant()) }},
		{"PLAN COMMENT (NO CHANGES)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanNoChanges()) }},
		{"NO MANAGED SCHEMA CHANGES", func() { fmt.Print(webhooktemplates.PreviewCommentNoManagedSchemaChanges()) }},
		{"NO MANAGED SCHEMA CHANGES (CHECKS REFRESHED)", func() { fmt.Print(webhooktemplates.PreviewCommentNoManagedSchemaChangesChecksRefreshed()) }},
		{"NO MANAGED SCHEMA CHANGES (GATED ON TENANTS)", func() {
			fmt.Print(webhooktemplates.PreviewCommentNoManagedSchemaChangesChecksRefreshedGatedOnTenants())
		}},
		{"RECONCILIATION REQUIRED (IN PROGRESS)", func() { fmt.Print(webhooktemplates.PreviewCommentSchemaReconciliationInProgress()) }},
		{"RECONCILIATION REQUIRED (COMPLETED)", func() { fmt.Print(webhooktemplates.PreviewCommentSchemaReconciliationCompleted()) }},
		{"SCHEMA CHANGE APPLY (AUTOMATIC)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyPlan()) }},
		{"SCHEMA CHANGE APPLY (DOWNGRADED)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyPlanDowngraded()) }},
		{"SCHEMA CHANGE APPLY (UNSAFE + ALLOWED)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyPlanUnsafe()) }},
		{"UNSAFE CHANGES BLOCKED", func() { fmt.Print(webhooktemplates.PreviewCommentUnsafeBlocked()) }},
		{"DROP COLUMN BLOCKED", func() { fmt.Print(webhooktemplates.PreviewCommentDropColumnBlocked()) }},
		{"DROP INDEX BLOCKED", func() { fmt.Print(webhooktemplates.PreviewCommentDropIndexBlocked()) }},
		{"SCHEMA LINT ERRORS BLOCKED", func() { fmt.Print(webhooktemplates.PreviewCommentLintErrorsBlocked()) }},
		{"MULTI-ENV PLAN (IDENTICAL)", func() { fmt.Print(webhooktemplates.PreviewCommentMultiEnvPlan()) }},
		{"MULTI-ENV PLAN (DIFFERENT)", func() { fmt.Print(webhooktemplates.PreviewCommentMultiEnvPlanDiff()) }},
		{"MULTI-ENV PLAN (ERROR)", func() { fmt.Print(webhooktemplates.PreviewCommentMultiEnvPlanError()) }},
		{"MULTI-ENV PLAN (LINT WARNINGS)", func() { fmt.Print(webhooktemplates.PreviewCommentMultiEnvPlanLint()) }},
		{"DEPLOYMENT DRIFT (CLEAN)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanDriftClean()) }},
		{"DEPLOYMENT DRIFT (DETECTED)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanDriftDetected()) }},
		{"DEPLOYMENT DRIFT (COULD NOT VERIFY)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanDriftUnverified()) }},
		{"HELP COMMENT", func() { fmt.Print(webhooktemplates.PreviewCommentHelp()) }},
		{"SUPPORT CHANNEL FOOTER", func() { fmt.Print(webhooktemplates.PreviewCommentSupportChannel()) }},
		{"AGENT HINT FOOTER", func() { fmt.Print(webhooktemplates.PreviewCommentAgentHint()) }},
		{"NO CONFIG (NO -D FLAG)", func() { fmt.Print(webhooktemplates.PreviewCommentErrorNoConfig()) }},
		{"MULTIPLE DATABASES", func() { fmt.Print(webhooktemplates.PreviewCommentErrorMultiple()) }},
		{"DATABASE NOT FOUND", func() { fmt.Print(webhooktemplates.PreviewCommentErrorNotFound()) }},
		{"INVALID CONFIG", func() { fmt.Print(webhooktemplates.PreviewCommentErrorInvalid()) }},
		{"UNMANAGED SCHEMA CONFIGS NOTICE", func() { fmt.Print(webhooktemplates.PreviewCommentUnmanagedSchemaConfigsNotice()) }},
		{"GENERIC ERROR", func() { fmt.Print(webhooktemplates.PreviewCommentErrorGeneric()) }},
		{"AUTO-PLAN: GENERIC ERROR", func() { fmt.Print(webhooktemplates.PreviewCommentErrorGenericAutoPlan()) }},
		{"MISSING -E FLAG", func() { fmt.Print(webhooktemplates.PreviewCommentMissingEnv()) }},
		{"INVALID COMMAND", func() { fmt.Print(webhooktemplates.PreviewCommentInvalidCmd()) }},
		{"APPLY IN PROGRESS", func() { fmt.Print(webhooktemplates.PreviewCommentApplyProgress()) }},
		{"APPLY ESTIMATE EXCEEDED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyEstimateExceeded()) }},
		{"APPLY COMPLETED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyCompleted()) }},
		{"APPLY RETRYING", func() { fmt.Print(webhooktemplates.PreviewCommentApplyRetrying()) }},
		{"APPLY RETRYING (REMOTE DATA-PLANE PAUSE)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyRemoteRetryablePause()) }},
		{"APPLY FAILED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyFailed()) }},
		{"APPLY FAILED (BEFORE ROW COPY)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyFailedBeforeRowCopy()) }},
		{"APPLY STOPPED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyStopped()) }},
		{"APPLY WAITING FOR CUTOVER", func() { fmt.Print(webhooktemplates.PreviewCommentApplyWaitingForCutover()) }},
		{"APPLY WAITING FOR CUTOVER (AUTOMATIC)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyWaitingForCutoverAutomatic()) }},
		{"APPLY CUTTING OVER", func() { fmt.Print(webhooktemplates.PreviewCommentApplyCuttingOver()) }},
		{"CUTOVER COMMAND ACCEPTED", func() { fmt.Print(webhooktemplates.PreviewCommentCutoverCommandAccepted()) }},
		{"CUTOVER COMMAND ALREADY IN PROGRESS", func() { fmt.Print(webhooktemplates.PreviewCommentCutoverCommandAlreadyInProgress()) }},
		{"RESUMED: SUPERSEDED PROGRESS COMMENT", func() { fmt.Print(webhooktemplates.PreviewCommentResumeSupersededProgress()) }},
		{"REVERT: SUPERSEDED PROGRESS COMMENT", func() { fmt.Print(webhooktemplates.PreviewCommentRevertSupersededProgress()) }},
		{"SKIP REVERT: SUPERSEDED PROGRESS COMMENT", func() { fmt.Print(webhooktemplates.PreviewCommentSkipRevertSupersededProgress()) }},
		{"CUTOVER COMPLETE: SUPERSEDED CUTOVER PROMPT", func() { fmt.Print(webhooktemplates.PreviewCommentCutoverSuperseded()) }},
		{"RETRY: SUPERSEDED PROGRESS COMMENT (GENERIC)", func() { fmt.Print(webhooktemplates.PreviewCommentSupersededProgress()) }},
		{"SUMMARY: COMPLETED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryCompleted()) }},
		{"SUMMARY: FAILED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryFailed()) }},
		{"SUMMARY: STOPPED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryStopped()) }},
		{"SUMMARY: COMPLETED (LARGE)", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryCompletedLarge()) }},
		{"SUMMARY: VITESS DDL + VSCHEMA", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryCompletedVitessDDLWithVSchema()) }},
		{"SUMMARY: VITESS VSCHEMA ONLY", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryCompletedVitessVSchemaOnly()) }},
		{"SUMMARY: FAILED (LARGE)", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryFailedLarge()) }},
		{"SUMMARY: MULTI-NAMESPACE FAILED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryMultiNamespaceFailed()) }},
		{"SUMMARY: MULTI-NAMESPACE COMPLETED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryMultiNamespaceCompleted()) }},
		{"ROLLBACK STATUS: RUNNING", func() { fmt.Print(webhooktemplates.PreviewCommentRollbackStatus()) }},
		{"SUMMARY: ROLLBACK COMPLETE", func() { fmt.Print(webhooktemplates.PreviewCommentRollbackSummaryCompleted()) }},
	}

	for i, s := range sections {
		if i > 0 {
			fmt.Println()
		}
		fmt.Println("---", s.name, strings.Repeat("-", max(50-len(s.name), 3)))
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
		{"MYSQL PLAN (IGNORED NAMESPACES)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanIgnoredNamespaces()) }},
		{"MYSQL PLAN (MANY LINT WARNINGS)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanManyLintWarnings()) }},
		{"MYSQL PLAN (ENGINE-BLOCKED CHANGE)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanBlocked()) }},
		{"MYSQL PLAN (DIRECT-EXECUTION CHANGE)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanDirect()) }},
		{"MYSQL PLAN (CHANGE ATTRIBUTED TO ANOTHER PR)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanAttributedChange()) }},
		{"MYSQL PLAN (EXISTING COPY DISCARDED)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanCopyDiscarded()) }},
		{"MYSQL PLAN (EXISTING COPY DISCARDED, APPLYING)", func() {
			fmt.Print(webhooktemplates.PreviewCommentPlanCopyDiscardedApplying())
		}},
		{"MYSQL PLAN (EXISTING COPY DISCARDED, PAUSED)", func() {
			fmt.Print(webhooktemplates.PreviewCommentPlanCopyDiscardedPaused())
		}},
		{"MYSQL PLAN (EXISTING COPY DISCARDED, CONFIRM STOPPED)", func() {
			fmt.Print(webhooktemplates.PreviewCommentPlanCopyDiscardedStopped())
		}},
		{"MYSQL PLAN (EXISTING COPY ADOPTED)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanCopyAdopted()) }},
		{"MYSQL PLAN (EXISTING COPY RUNNING)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanCopyRunning()) }},
		{"APPLY REJECTED (ENGINE-BLOCKED CHANGES)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedRejected()) }},
		{"MYSQL PLAN (TENANT TARGET)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanTenant()) }},
		{"MYSQL PLAN (NO CHANGES)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanNoChanges()) }},
		{"NO MANAGED SCHEMA CHANGES", func() { fmt.Print(webhooktemplates.PreviewCommentNoManagedSchemaChanges()) }},
		{"NO MANAGED SCHEMA CHANGES (CHECKS REFRESHED)", func() { fmt.Print(webhooktemplates.PreviewCommentNoManagedSchemaChangesChecksRefreshed()) }},
		{"NO MANAGED SCHEMA CHANGES (GATED ON TENANTS)", func() {
			fmt.Print(webhooktemplates.PreviewCommentNoManagedSchemaChangesChecksRefreshedGatedOnTenants())
		}},
		{"RECONCILIATION REQUIRED (IN PROGRESS)", func() { fmt.Print(webhooktemplates.PreviewCommentSchemaReconciliationInProgress()) }},
		{"RECONCILIATION REQUIRED (COMPLETED)", func() { fmt.Print(webhooktemplates.PreviewCommentSchemaReconciliationCompleted()) }},
		{"VITESS PLAN", func() { fmt.Print(webhooktemplates.PreviewCommentVitessPlan()) }},
		{"VITESS PLAN: VSCHEMA REMOVAL (UNSAFE)", func() { fmt.Print(webhooktemplates.PreviewCommentVitessPlanVSchemaRemoval()) }},
		{"POSTGRES PLAN", func() { fmt.Print(webhooktemplates.PreviewCommentPostgresPlan()) }},
		{"SCHEMA CHANGE APPLY (LOCKED + OPTIONS)", func() { fmt.Print(webhooktemplates.PreviewCommentVitessApplyPlan()) }},
		{"MYSQL MULTI-SCHEMA PLAN", func() { fmt.Print(webhooktemplates.PreviewCommentMySQLMultiSchema()) }},
		{"MULTI-ENV PLAN (IDENTICAL)", func() { fmt.Print(webhooktemplates.PreviewCommentMultiEnvPlan()) }},
		{"MULTI-ENV PLAN (DIFFERENT)", func() { fmt.Print(webhooktemplates.PreviewCommentMultiEnvPlanDiff()) }},
		{"MULTI-ENV PLAN (ERROR)", func() { fmt.Print(webhooktemplates.PreviewCommentMultiEnvPlanError()) }},
		{"MULTI-ENV PLAN (LINT WARNINGS)", func() { fmt.Print(webhooktemplates.PreviewCommentMultiEnvPlanLint()) }},
		{"DEPLOYMENT DRIFT (CLEAN)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanDriftClean()) }},
		{"DEPLOYMENT DRIFT (DETECTED)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanDriftDetected()) }},
		{"DEPLOYMENT DRIFT (COULD NOT VERIFY)", func() { fmt.Print(webhooktemplates.PreviewCommentPlanDriftUnverified()) }},
		{"DROP COLUMN BLOCKED", func() { fmt.Print(webhooktemplates.PreviewCommentDropColumnBlocked()) }},
		{"DROP INDEX BLOCKED", func() { fmt.Print(webhooktemplates.PreviewCommentDropIndexBlocked()) }},
		{"SCHEMA LINT ERRORS BLOCKED", func() { fmt.Print(webhooktemplates.PreviewCommentLintErrorsBlocked()) }},
		{"HELP COMMENT", func() { fmt.Print(webhooktemplates.PreviewCommentHelp()) }},
		{"SUPPORT CHANNEL FOOTER", func() { fmt.Print(webhooktemplates.PreviewCommentSupportChannel()) }},
		{"AGENT HINT FOOTER", func() { fmt.Print(webhooktemplates.PreviewCommentAgentHint()) }},
		{"NO CONFIG (NO -D FLAG)", func() { fmt.Print(webhooktemplates.PreviewCommentErrorNoConfig()) }},
		{"MULTIPLE DATABASES", func() { fmt.Print(webhooktemplates.PreviewCommentErrorMultiple()) }},
		{"DATABASE NOT FOUND", func() { fmt.Print(webhooktemplates.PreviewCommentErrorNotFound()) }},
		{"INVALID CONFIG", func() { fmt.Print(webhooktemplates.PreviewCommentErrorInvalid()) }},
		{"UNMANAGED SCHEMA CONFIGS NOTICE", func() { fmt.Print(webhooktemplates.PreviewCommentUnmanagedSchemaConfigsNotice()) }},
		{"GENERIC ERROR", func() { fmt.Print(webhooktemplates.PreviewCommentErrorGeneric()) }},
		{"AUTO-PLAN: GENERIC ERROR", func() { fmt.Print(webhooktemplates.PreviewCommentErrorGenericAutoPlan()) }},
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
		{"UNLOCK SUCCESS", func() { fmt.Print(webhooktemplates.PreviewCommentUnlockSuccess()) }},
		{"NO LOCK FOUND", func() { fmt.Print(webhooktemplates.PreviewCommentApplyConfirmNoLock()) }},
	}
	printSections(sections)
}

func previewCommentApplyFlowAllOutput() {
	sections := []struct {
		name string
		fn   func()
	}{
		{"SCHEMA CHANGE APPLY (AUTOMATIC)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyPlan()) }},
		{"SCHEMA CHANGE APPLY (WITH OPTIONS)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyPlanOptions()) }},
		{"SCHEMA CHANGE APPLY (DOWNGRADED)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyPlanDowngraded()) }},
		{"SCHEMA CHANGE APPLY (VITESS + OPTIONS)", func() { fmt.Print(webhooktemplates.PreviewCommentVitessApplyPlan()) }},
		{"APPLY STARTED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyStarted()) }},
		// Blocked applies: the apply command was rejected before starting
		{"APPLY BLOCKED BY OTHER PR", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByOtherPR()) }},
		{"APPLY BLOCKED BY CLI", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByCLI()) }},
		{"APPLY BLOCKED: ALREADY IN PROGRESS", func() { fmt.Print(webhooktemplates.PreviewCommentApplyInProgress()) }},
		{"APPLY BLOCKED: PR CLOSED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedClosedPR()) }},
		{"APPLY BLOCKED: PR MERGED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedMergedPR()) }},
		{"APPLY BLOCKED: BASE SCHEMA CHANGED SINCE PR DIVERGED", func() { fmt.Print(webhooktemplates.PreviewCommentBaseSchemaFreshnessRejected()) }},
		{"APPLY BLOCKED: SCHEMA STALE (NEW COMMITS)", func() { fmt.Print(webhooktemplates.PreviewCommentStaleSchemaRejected()) }},
		{"APPLY BLOCKED: CONFIRMED PLAN STALE", func() { fmt.Print(webhooktemplates.PreviewCommentStalePlanRejected()) }},
		{"APPLY BLOCKED BY PRIOR ENV (PENDING)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByPriorEnv()) }},
		{"APPLY BLOCKED BY PRIOR ENV (FAILED)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByPriorEnvFailed()) }},
		{"APPLY BLOCKED BY PRIOR ENV (IN PROGRESS)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByPriorEnvInProgress()) }},
		{"APPLY BLOCKED: PRIOR ENV CHECK MISSING", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByMissingPriorEnvCheck()) }},
		{"APPLY BLOCKED: PRIOR ENV CHECK READ ERROR", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByPriorEnvCheckError()) }},
		{"APPLY BLOCKED: PRIOR ENV CHECK UNTRUSTED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByUntrustedPriorEnvCheck()) }},
		{"APPLY BLOCKED: ENVIRONMENT NOT IN PROMOTION ORDER", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByUnlistedEnvironment()) }},
		{"APPLY BLOCKED: REVIEW REQUIRED", func() { fmt.Print(webhooktemplates.PreviewCommentReviewRequired()) }},
		{"APPLY BLOCKED: REVIEW REQUIRED (NO OPERATORS)", func() { fmt.Print(webhooktemplates.PreviewCommentReviewRequiredNoOperators()) }},
		{"APPLY BLOCKED: REVIEW GATE ERROR (FAIL-CLOSED)", func() { fmt.Print(webhooktemplates.PreviewCommentReviewGateError()) }},
		{"APPLY BLOCKED: CHECKS NOT PASSING", func() {
			fmt.Print(webhooktemplates.RenderApplyBlockedByNonPassingChecks("staging", []webhooktemplates.BlockingCheck{
				{Name: "CI / unit-tests", State: "failure"},
				{Name: "CI / lint", State: "timed_out"},
			}))
		}},
		{"APPLY BLOCKED: CHECKS IN PROGRESS", func() {
			fmt.Print(webhooktemplates.RenderApplyBlockedByInProgressChecks("staging", []webhooktemplates.BlockingCheck{
				{Name: "CI / unit-tests", State: "in_progress"},
				{Name: "CI / integration-tests", State: "queued"},
			}, nil))
		}},
		{"APPLY BLOCKED: CHECK STATUS READ ERROR", func() { fmt.Print(webhooktemplates.PreviewCommentApplyBlockedByCheckStatusError()) }},
		{"APPLY BLOCKED: ACTOR NOT AUTHORIZED", func() { fmt.Print(webhooktemplates.PreviewCommentPRCommandNotAuthorized()) }},
		{"ACTOR AUTHORIZATION: UNAVAILABLE", func() { fmt.Print(webhooktemplates.PreviewCommentPRCommandAuthorizationUnavailable()) }},
		{"ACTOR AUTHORIZATION: DATABASE NOT CONFIGURED", func() { fmt.Print(webhooktemplates.PreviewCommentPRCommandDatabaseNotConfigured()) }},
		// Single-table (most common case)
		{"SINGLE TABLE: RUNNING", func() { fmt.Print(webhooktemplates.PreviewCommentApplySingleProgress()) }},
		{"SINGLE TABLE: COMPLETED", func() { fmt.Print(webhooktemplates.PreviewCommentApplySingleCompleted()) }},
		{"SINGLE TABLE: FAILED", func() { fmt.Print(webhooktemplates.PreviewCommentApplySingleFailed()) }},
		{"SINGLE TABLE: STOPPED", func() { fmt.Print(webhooktemplates.PreviewCommentApplySingleStopped()) }},
		// Multi-table sequential progression
		{"ALL PENDING", func() { fmt.Print(webhooktemplates.PreviewCommentApplyAllPending()) }},
		{"FIRST TABLE RUNNING", func() { fmt.Print(webhooktemplates.PreviewCommentApplyFirstRunning()) }},
		{"SECOND TABLE RUNNING", func() { fmt.Print(webhooktemplates.PreviewCommentApplyProgress()) }},
		{"SECOND TABLE ESTIMATE EXCEEDED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyEstimateExceeded()) }},
		{"SECOND TABLE THROTTLED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyThrottled()) }},
		{"SECOND TABLE CATCHING UP", func() { fmt.Print(webhooktemplates.PreviewCommentApplyCatchingUp()) }},
		{"SECOND TABLE CHECKSUMMING", func() { fmt.Print(webhooktemplates.PreviewCommentApplyChecksumming()) }},
		{"SECOND TABLE POST-CHECKSUM", func() { fmt.Print(webhooktemplates.PreviewCommentApplyPostChecksum()) }},
		{"THIRD TABLE RUNNING", func() { fmt.Print(webhooktemplates.PreviewCommentApplyThirdRunning()) }},
		// PostgreSQL: several statements on one table, each its own row
		{"POSTGRESQL: MULTI-STATEMENT TABLE RUNNING", func() { fmt.Print(webhooktemplates.PreviewCommentApplyPostgresMultiStatement()) }},
		// Sharded tables: compact per-shard summary (inline for few, collapsed for many)
		{"SHARDED: SHARD PROGRESS", func() { fmt.Print(webhooktemplates.PreviewCommentApplyShardProgress()) }},
		{"SHARDED: MANY SHARDS (256)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyManyShardProgress()) }},
		{"ALL COMPLETED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyCompleted()) }},
		{"VITESS: VSCHEMA ONLY", func() { fmt.Print(webhooktemplates.PreviewCommentApplyVitessVSchemaOnly()) }},
		{"VITESS: DDL + VSCHEMA", func() { fmt.Print(webhooktemplates.PreviewCommentApplyVitessDDLWithVSchema()) }},
		{"VITESS: MULTI-KEYSPACE VSCHEMA", func() { fmt.Print(webhooktemplates.PreviewCommentApplyVitessMultiKeyspaceVSchema()) }},
		{"MIDDLE TABLE RETRYING", func() { fmt.Print(webhooktemplates.PreviewCommentApplyRetrying()) }},
		{"MIDDLE TABLE RETRYING (REMOTE DATA-PLANE PAUSE)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyRemoteRetryablePause()) }},
		{"FIRST TABLE FAILED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyFirstFailed()) }},
		{"MIDDLE TABLE FAILED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyFailed()) }},
		{"FAILED BEFORE ROW COPY (PREFLIGHT)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyFailedBeforeRowCopy()) }},
		{"STOPPED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyStopped()) }},
		{"RESUMING", func() { fmt.Print(webhooktemplates.PreviewCommentApplyResuming()) }},
		{"CANCELLED", func() { fmt.Print(webhooktemplates.PreviewCommentApplyCancelled()) }},
		// Cutover states
		{"WAITING FOR CUTOVER", func() { fmt.Print(webhooktemplates.PreviewCommentApplyWaitingForCutover()) }},
		{"WAITING FOR CUTOVER (AUTOMATIC)", func() { fmt.Print(webhooktemplates.PreviewCommentApplyWaitingForCutoverAutomatic()) }},
		{"CUTTING OVER", func() { fmt.Print(webhooktemplates.PreviewCommentApplyCuttingOver()) }},
		// Revert-window states (PlanetScale): deployed-but-revertable, then finalizing.
		{"REVERT WINDOW", func() { fmt.Print(webhooktemplates.PreviewCommentApplyRevertWindow()) }},
		{"SKIPPING REVERT", func() { fmt.Print(webhooktemplates.PreviewCommentApplySkippingRevert()) }},
		{"REVERTING", func() { fmt.Print(webhooktemplates.PreviewCommentApplyReverting()) }},
		{"START COMMAND ACCEPTED", func() { fmt.Print(webhooktemplates.PreviewCommentStartCommandAccepted()) }},
		{"START COMMAND ALREADY PENDING", func() { fmt.Print(webhooktemplates.PreviewCommentStartCommandAlreadyRequested()) }},
		{"CUTOVER COMMAND ACCEPTED", func() { fmt.Print(webhooktemplates.PreviewCommentCutoverCommandAccepted()) }},
		{"CUTOVER COMMAND ALREADY IN PROGRESS", func() { fmt.Print(webhooktemplates.PreviewCommentCutoverCommandAlreadyInProgress()) }},
		{"RESUMED: SUPERSEDED PROGRESS COMMENT", func() { fmt.Print(webhooktemplates.PreviewCommentResumeSupersededProgress()) }},
		{"REVERT: SUPERSEDED PROGRESS COMMENT", func() { fmt.Print(webhooktemplates.PreviewCommentRevertSupersededProgress()) }},
		{"SKIP REVERT: SUPERSEDED PROGRESS COMMENT", func() { fmt.Print(webhooktemplates.PreviewCommentSkipRevertSupersededProgress()) }},
		{"CUTOVER COMPLETE: SUPERSEDED CUTOVER PROMPT", func() { fmt.Print(webhooktemplates.PreviewCommentCutoverSuperseded()) }},
		{"RETRY: SUPERSEDED PROGRESS COMMENT (GENERIC)", func() { fmt.Print(webhooktemplates.PreviewCommentSupersededProgress()) }},
		// Summaries
		{"SUMMARY: COMPLETED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryCompleted()) }},
		{"SUMMARY: FAILED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryFailed()) }},
		{"SUMMARY: STOPPED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryStopped()) }},
		{"SUMMARY: CANCELLED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryCancelled()) }},
		{"SUMMARY: POSTGRESQL MULTI-STATEMENT TABLE FAILED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryPostgresMultiStatementFailed()) }},
		{"SUMMARY: COMPLETED (LARGE)", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryCompletedLarge()) }},
		{"SUMMARY: VITESS DDL + VSCHEMA", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryCompletedVitessDDLWithVSchema()) }},
		{"SUMMARY: VITESS VSCHEMA ONLY", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryCompletedVitessVSchemaOnly()) }},
		{"SUMMARY: FAILED (LARGE)", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryFailedLarge()) }},
		{"SUMMARY: MULTI-NAMESPACE FAILED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryMultiNamespaceFailed()) }},
		{"SUMMARY: MULTI-NAMESPACE COMPLETED", func() { fmt.Print(webhooktemplates.PreviewCommentSummaryMultiNamespaceCompleted()) }},
		{"ROLLBACK STATUS: RUNNING", func() { fmt.Print(webhooktemplates.PreviewCommentRollbackStatus()) }},
		{"SUMMARY: ROLLBACK COMPLETE", func() { fmt.Print(webhooktemplates.PreviewCommentRollbackSummaryCompleted()) }},
	}
	printSections(sections)
}

func previewCommentMultiDeployAllOutput() {
	sections := []struct {
		name string
		fn   func()
	}{
		{"BARRIER ROLLOUT IN PROGRESS", func() { fmt.Print(webhooktemplates.PreviewCommentMultiDeploymentApplyInProgress()) }},
		{"HALT ON FAILURE (ONE DEPLOYMENT FAILED)", func() { fmt.Print(webhooktemplates.PreviewCommentMultiDeploymentApplyFailed()) }},
		{"ALL DEPLOYMENTS COMPLETED", func() { fmt.Print(webhooktemplates.PreviewCommentMultiDeploymentApplyCompleted()) }},
		{"SUMMARY: ALL DEPLOYMENTS COMPLETED", func() { fmt.Print(webhooktemplates.PreviewCommentMultiDeploymentApplySummaryCompleted()) }},
		{"SUMMARY: HALT ON FAILURE (ONE DEPLOYMENT FAILED)", func() { fmt.Print(webhooktemplates.PreviewCommentMultiDeploymentApplySummaryFailed()) }},
	}
	printSections(sections)
}

func previewCommentShardedAllOutput() {
	sections := []struct {
		name string
		fn   func()
	}{
		{"PLAN: DIVERGENT SHARDS", func() { fmt.Print(webhooktemplates.PreviewCommentShardedPlanDivergent()) }},
		{"PLAN: MANY SHARDS (32)", func() { fmt.Print(webhooktemplates.PreviewCommentShardedPlanManyShards()) }},
		{"PLAN: PARTIALLY APPLIED SHARDS", func() { fmt.Print(webhooktemplates.PreviewCommentShardedPlanPartiallyApplied()) }},
		{"PLAN: UNSAFE CHANGE ON ONE SHARD", func() { fmt.Print(webhooktemplates.PreviewCommentShardedPlanUnsafe()) }},
		{"APPLY IN PROGRESS", func() { fmt.Print(webhooktemplates.PreviewCommentShardedApplyInProgress()) }},
		{"APPLY FAILED (ONE SHARD FAILED)", func() { fmt.Print(webhooktemplates.PreviewCommentShardedApplyFailed()) }},
		{"APPLY WITH DIVERGENT SHARDS", func() { fmt.Print(webhooktemplates.PreviewCommentShardedApplyDivergent()) }},
		{"APPLY ACROSS MULTIPLE KEYSPACES", func() { fmt.Print(webhooktemplates.PreviewCommentShardedApplyMultiKeyspace()) }},
		{"SUMMARY: ALL SHARDS COMPLETED", func() { fmt.Print(webhooktemplates.PreviewCommentShardedSummaryCompleted()) }},
		{"SUMMARY: HALT ON FAILURE (ONE SHARD FAILED)", func() { fmt.Print(webhooktemplates.PreviewCommentShardedSummaryFailed()) }},
		{"SUMMARY: CANCELLED AFTER PARTIAL LANDING", func() { fmt.Print(webhooktemplates.PreviewCommentShardedSummaryCancelledPartial()) }},
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
		{"LOCK EXISTS UNDER OTHER TYPE", previewLockExistsUnderOtherTypeOutput},
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
		{"MYSQL: MULTI-TABLE SECOND TABLE CATCHING UP", previewSeqCatchingUpOutput},
		{"MYSQL: MULTI-TABLE SECOND TABLE THROTTLED", previewSeqThrottledOutput},
		{"MYSQL: MULTI-TABLE SECOND TABLE CHECKSUMMING", previewSeqChecksummingOutput},
		{"MYSQL: MULTI-TABLE SECOND TABLE POST-CHECKSUM", previewSeqPostChecksumOutput},
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
		{"VITESS: VSCHEMA ONLY UPDATE", previewVitessVSchemaOnlyOutput},
		{"VITESS: MULTI-KEYSPACE VSCHEMA", previewVitessMultiKeyspaceVSchemaOutput},
		{"VITESS: MULTI-KEYSPACE", previewVitessMultiKeyspaceOutput},
		{"VITESS: DDL + VSCHEMA", previewVitessDDLWithVSchemaOutput},
		{"VITESS: SHARD PROGRESS", previewVitessShardProgressOutput},
		{"VITESS: CUTOVER RETRY", previewVitessCutoverRetryOutput},
		// Vitess: plan rendering
		{"VITESS: PLAN (DDL + VSCHEMA)", previewVSchemaPlanOutput},
		{"VITESS: PLAN (VSCHEMA ONLY)", previewVSchemaOnlyOutput},
		{"VITESS: PLAN (MULTI-KEYSPACE)", previewMultiKeyspacePlanOutput},
		{"VITESS: INSTANT DDL", previewVitessInstantDDLOutput},
		{"VITESS: REVERT WINDOW", previewVitessRevertWindowOutput},
		// CLI-only: interactive commands
		{"APPLY WATCH MODE", previewApplyWatchOutput},
		{"STOP COMMAND", previewStopCommandOutput},
		{"START COMMAND", previewStartCommandOutput},
		{"STATUS LIST", previewStatusListOutput},
		{"STATUS FOR DEPLOYMENT", previewStatusDeploymentOutput},
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
		// Clamp the trailing rule so a section name longer than the target width
		// doesn't pass a negative count to strings.Repeat (which panics and would
		// abort the whole TEMPLATES.md regeneration). Keep at least a few dashes
		// so the section-marker line still matches the wrapper's pattern.
		dashes := max(50-len(s.name), 3)
		fmt.Println("---", s.name, strings.Repeat("-", dashes))
		fmt.Println()
		s.fn()
	}
}
