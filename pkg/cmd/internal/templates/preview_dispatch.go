package templates

import (
	"fmt"
	"strings"

	webhooktemplates "github.com/block/schemabot/pkg/webhook/templates"
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
	case PreviewWaitingForDeploy:
		previewVitessWaitingForDeployOutput()
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
	case PreviewBranchRefresh:
		previewRefreshingBranchOutput()
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
	case PreviewCommentNoManagedSchema:
		fmt.Print(webhooktemplates.PreviewCommentNoManagedSchemaChanges())
	case PreviewCommentReconcileInProgress:
		fmt.Print(webhooktemplates.PreviewCommentSchemaReconciliationInProgress())
	case PreviewCommentReconcileCompleted:
		fmt.Print(webhooktemplates.PreviewCommentSchemaReconciliationCompleted())
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
	case PreviewCommentSupportChannel:
		fmt.Print(webhooktemplates.PreviewCommentSupportChannel())
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
	case PreviewCommentApplyEstimateExceeded:
		fmt.Print(webhooktemplates.PreviewCommentApplyEstimateExceeded())
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
	case PreviewCommentMultiDeployInProgress:
		fmt.Print(webhooktemplates.PreviewCommentMultiDeploymentApplyInProgress())
	case PreviewCommentMultiDeployFailed:
		fmt.Print(webhooktemplates.PreviewCommentMultiDeploymentApplyFailed())
	case PreviewCommentMultiDeployCompleted:
		fmt.Print(webhooktemplates.PreviewCommentMultiDeploymentApplyCompleted())
	case PreviewCommentMultiDeployAll:
		previewCommentMultiDeployAllOutput()
	case PreviewCLIMultiDeployInProgress:
		previewCLIMultiDeploymentApplyInProgress()
	case PreviewCLIMultiDeployFailed:
		previewCLIMultiDeploymentApplyFailed()
	case PreviewCLIMultiDeployCompleted:
		previewCLIMultiDeploymentApplyCompleted()
	case PreviewCLIMultiDeployAll:
		previewCLIMultiDeployAllOutput()
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
	case PreviewCommentChecksGateNotPassing:
		fmt.Print(webhooktemplates.RenderApplyBlockedByNonPassingChecks("staging", []webhooktemplates.BlockingCheck{
			{Name: "CI / unit-tests", State: "failure"},
			{Name: "CI / lint", State: "timed_out"},
		}))
	case PreviewCommentChecksGateInProgress:
		fmt.Print(webhooktemplates.RenderApplyBlockedByInProgressChecks("staging", []webhooktemplates.BlockingCheck{
			{Name: "CI / unit-tests", State: "in_progress"},
			{Name: "CI / integration-tests", State: "queued"},
		}, nil))
	case PreviewCommentActorNotAuthorized:
		fmt.Print(webhooktemplates.PreviewCommentPRCommandNotAuthorized())
	case PreviewCommentActorAuthUnavailable:
		fmt.Print(webhooktemplates.PreviewCommentPRCommandAuthorizationUnavailable())
	case PreviewCommentDatabaseNotConfigured:
		fmt.Print(webhooktemplates.PreviewCommentPRCommandDatabaseNotConfigured())
	case PreviewCommentStartAccepted:
		fmt.Print(webhooktemplates.PreviewCommentStartCommandAccepted())
	case PreviewCommentStartPending:
		fmt.Print(webhooktemplates.PreviewCommentStartCommandAlreadyRequested())
	case PreviewCommentCutoverAccepted:
		fmt.Print(webhooktemplates.PreviewCommentCutoverCommandAccepted())
	case PreviewCommentCutoverActive:
		fmt.Print(webhooktemplates.PreviewCommentCutoverCommandAlreadyInProgress())
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
