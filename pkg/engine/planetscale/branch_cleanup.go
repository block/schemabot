package planetscale

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/psclient"
)

// branchDeleteTimeout bounds the cleanup delete so a slow or unreachable
// PlanetScale API cannot hold the apply's failure path open.
const branchDeleteTimeout = 30 * time.Second

// deleteOwnedBranch removes a branch this apply created and no deploy request
// took ownership of, so a failure while preparing the branch does not strand
// quota. cause is the failure that triggered the cleanup; it is logged with the
// outcome so the two are readable together. logger carries the caller's triage
// identity for the schema change, so the outcome names the apply it belongs to.
//
// The delete runs under ctx with its own deadline. A caller deleting a branch
// that the apply's stored state names, and that another driver could therefore
// resume from, passes the drive's context, so a lost lease abandons the delete
// mid-request instead of tearing down a branch a successor may be preparing. A
// caller deleting a branch no drive can resume passes a context detached from
// the drive, so a cancelled apply still cleans up after itself. A branch that
// could not be deleted is logged at error level with the identifiers needed to
// remove it by hand — an undeletable branch must be visible, not silently
// retried.
func (e *Engine) deleteOwnedBranch(ctx context.Context, logger *slog.Logger, client psclient.PSClient, org, database, branch string, cause error) {
	alreadyGone, err := deleteBranch(ctx, client, org, database, branch)
	if err != nil && ctx.Err() != nil {
		logger.Warn("drive ended while deleting the branch; abandoned the delete request, which may already have landed; a resuming driver starts fresh if the branch is gone",
			"organization", org, "planetscale_database", database, "branch", branch,
			"apply_error", cause, "error", err)
		return
	}
	if err != nil {
		logger.Error("failed to delete the branch left behind by a failed apply; delete it manually to reclaim branch quota",
			"organization", org, "planetscale_database", database, "branch", branch,
			"apply_error", cause, "error", err)
		return
	}
	if alreadyGone {
		logger.Info("the branch created for an apply that failed before its deploy request was already gone",
			"organization", org, "planetscale_database", database, "branch", branch, "apply_error", cause)
		return
	}
	logger.Info("deleted the branch created for an apply that failed before its deploy request",
		"organization", org, "planetscale_database", database, "branch", branch, "apply_error", cause)
}

// deleteBranch sends one branch delete under its own deadline and reports a
// branch PlanetScale no longer has as already gone rather than as a failure.
// A branch can be gone for reasons the engine cannot tell apart: an earlier
// delete of the same branch landed, the branch was never created because the
// apply ended between persisting its name and creating it, or an operator
// removed it by hand. In each case there is nothing left to delete.
func deleteBranch(ctx context.Context, client psclient.PSClient, org, database, branch string) (alreadyGone bool, err error) {
	deleteCtx, cancel := context.WithTimeout(ctx, branchDeleteTimeout)
	defer cancel()

	err = client.DeleteBranch(deleteCtx, &ps.DeleteDatabaseBranchRequest{
		Organization: org,
		Database:     database,
		Branch:       branch,
	})
	if err != nil && isNotFound(err) {
		return true, nil
	}
	return false, err
}

// deployRequestCloseTimeout bounds the read and the close that retire the
// deploy request of a failed apply, so a slow PlanetScale API cannot hold the
// apply's failure path open.
const deployRequestCloseTimeout = 30 * time.Second

// deployRequestNeverDeployed reports whether a deploy request is provably one
// whose deploy never began: it carries no deploy timestamp, and its deployment
// state precedes a deploy (pending, ready) or records that PlanetScale refused
// the deploy request before any deploy (error).
func deployRequestNeverDeployed(dr *ps.DeployRequest) bool {
	if dr.DeployedAt != nil {
		return false
	}
	switch dr.DeploymentState {
	case deployState.Pending, deployState.Ready, deployState.Error:
		return true
	default:
		return false
	}
}

// retireDeployRequestOfFailedApply closes the deploy request a failed apply
// created and deletes the branch the apply created for it, when the deploy
// request provably never deployed. The deploy request's auto_delete_branch
// only fires when a deploy finishes, so without this a failure between
// creating the deploy request and starting its deploy leaves the deploy
// request open, still deployable from the PlanetScale UI after SchemaBot
// reported the apply failed, and leaves the branch holding quota.
//
// Nothing that may have started deploying is touched: the deploy request is
// re-read first, and only one that reads as never deployed is closed, and the
// branch is deleted only once the close response also reads as never deployed.
// ownedBranch is empty for an operator-supplied branch, which is closed out of
// its deploy request but kept for the operator. Each outcome is logged with the
// identifiers needed to finish the cleanup by hand; the caller returns cause
// unchanged whatever happens here.
//
// ctx is the drive's context. Until the drive returns, the apply is not settled
// failed, and a drive whose context ends is handed back for a successor to
// resume against this deploy request and branch, so the cleanup stops issuing
// requests as soon as ctx ends.
func (e *Engine) retireDeployRequestOfFailedApply(ctx context.Context, logger *slog.Logger, client psclient.PSClient, org, database string, number uint64, ownedBranch string, cause error) {
	attrs := []any{"organization", org, "planetscale_database", database, "deploy_request", number, "branch", ownedBranch, "apply_error", cause}
	driveEnded := func(during string) bool {
		if ctx.Err() == nil {
			return false
		}
		logger.Warn("drive ended "+during+"; stopping the cleanup of a failed apply's deploy request and leaving the deploy request and branch to the driver that takes over the apply",
			append(attrs, "error", ctx.Err())...)
		return true
	}
	closeCtx, cancel := context.WithTimeout(ctx, deployRequestCloseTimeout)
	defer cancel()

	dr, err := e.getDeployRequest(closeCtx, client, org, database, number)
	if driveEnded("while reading the deploy request") {
		return
	}
	if err != nil {
		logger.Error("failed to read the deploy request of a failed apply; close it manually if it never deployed, and delete its branch to reclaim branch quota",
			append(attrs, "error", err)...)
		return
	}
	if !deployRequestNeverDeployed(dr) {
		logger.Warn("the deploy request of a failed apply reports a deploy may have begun; leaving it and its branch in place",
			append(attrs, "deployment_state", dr.DeploymentState, "deployed_at", formatDeployedAt(dr.DeployedAt))...)
		return
	}
	if dr.State == deployRequestClosed {
		logger.Info("the deploy request of a failed apply was already closed before it deployed", attrs...)
	} else {
		closed, closeErr := client.CloseDeployRequest(closeCtx, &ps.CloseDeployRequestRequest{
			Organization: org,
			Database:     database,
			Number:       number,
		})
		if ctx.Err() != nil {
			endedAttrs := append(slices.Clone(attrs), "error", ctx.Err())
			if closeErr != nil {
				endedAttrs = append(endedAttrs, "close_error", closeErr)
			}
			logger.Warn("drive ended while closing the deploy request of a failed apply; the close may already have landed; leaving the branch to the driver that takes over the apply",
				endedAttrs...)
			return
		}
		if closeErr != nil {
			logger.Error("failed to close the undeployed deploy request of a failed apply; close it manually, and delete its branch to reclaim branch quota",
				append(attrs, "deployment_state", dr.DeploymentState, "error", closeErr)...)
			return
		}
		if closed == nil {
			logger.Error("closing the undeployed deploy request of a failed apply returned no deploy request; check it is closed, and delete its branch to reclaim branch quota",
				attrs...)
			return
		}
		if !deployRequestNeverDeployed(closed) {
			logger.Error("closing the deploy request of a failed apply reported a deploy; leaving its branch in place",
				append(attrs, "deployment_state", closed.DeploymentState, "deployed_at", formatDeployedAt(closed.DeployedAt))...)
			return
		}
		logger.Info("closed the undeployed deploy request of a failed apply",
			append(attrs, "deployment_state", dr.DeploymentState)...)
	}
	if ownedBranch == "" {
		logger.Info("the failed apply ran on an operator-supplied branch; keeping it for the operator", attrs...)
		return
	}
	if driveEnded("before deleting the branch") {
		return
	}
	alreadyGone, err := deleteBranch(ctx, client, org, database, ownedBranch)
	switch {
	case err != nil && ctx.Err() != nil:
		logger.Warn("drive ended while deleting the branch of a failed apply after closing its deploy request; abandoned the delete request, which may already have landed",
			append(attrs, "error", err)...)
	case err != nil:
		logger.Error("failed to delete the branch of a failed apply after closing its deploy request; delete it manually to reclaim branch quota",
			append(attrs, "error", err)...)
	case alreadyGone:
		logger.Info("the branch of a failed apply was already gone after closing its deploy request", attrs...)
	default:
		logger.Info("deleted the branch of a failed apply after closing its undeployed deploy request", attrs...)
	}
}

var _ engine.CancelledResumeStateReclaimer = (*Engine)(nil)

// ReclaimCancelledResumeState deletes the branch the stored resume state names
// when no deploy request exists to own its teardown and SchemaBot created it.
// It serves the cancel that lands between drives: the drive that persisted the
// branch has exited, and the drive consuming the cancel stands down before it
// resumes, so no drive's own cleanup ever sees the branch.
//
// A state naming a deploy request is left alone: the deploy request tears the
// branch down (auto_delete_branch), and the engine's Cancel has already
// addressed the deploy request itself. An operator-supplied branch is left to
// the operator. Each kept branch is logged with the reason on the schema
// change's logger, so a branch left behind is explained rather than silent. A
// branch PlanetScale no longer has is reported as already gone, not as a
// failure, without naming a cause the engine cannot know (see deleteBranch).
func (e *Engine) ReclaimCancelledResumeState(ctx context.Context, req *engine.ReclaimResumeStateRequest) (*engine.ReclaimResumeStateResult, error) {
	logger := e.logger
	if req.Logger != nil {
		logger = req.Logger
	}
	result := &engine.ReclaimResumeStateResult{}
	if req.ResumeState == nil || req.ResumeState.Metadata == "" {
		logger.Debug("cancelled apply persisted no engine state, so it holds no branch to reclaim",
			"planetscale_database", req.Database)
		return result, nil
	}
	meta, err := decodePSMetadata(req.ResumeState.Metadata)
	if err != nil {
		return nil, fmt.Errorf("decode stored resume state of the cancelled apply on %s: %w", req.Database, err)
	}
	org := credOrg(req.Credentials)
	branch := "branch " + meta.BranchName
	switch {
	case meta.BranchName == "":
		logger.Debug("cancelled apply's stored state names no branch, so there is nothing to reclaim",
			"organization", org, "planetscale_database", req.Database)
		return result, nil
	case meta.DeployRequestID != 0:
		logger.Info("cancelled apply's branch is owned by its deploy request, which tears the branch down",
			"organization", org, "planetscale_database", req.Database, "branch", meta.BranchName, "deploy_request", meta.DeployRequestID)
		return result, nil
	case !deployRequestDeletesBranch(req.Options):
		logger.Info("cancelled apply ran on an operator-supplied branch; keeping it for the operator",
			"organization", org, "planetscale_database", req.Database, "branch", meta.BranchName)
		result.Kept = []string{branch}
		result.KeptReason = "the operator supplied it and it is theirs to keep"
		return result, nil
	}

	client, err := e.getClient(req.Credentials)
	if err != nil {
		result.Kept = []string{branch}
		result.KeptReason = "the PlanetScale API could not be reached; delete it by hand to reclaim branch quota"
		return result, fmt.Errorf("reclaim branch %s of the cancelled apply on %s: %w", meta.BranchName, req.Database, err)
	}
	alreadyGone, err := deleteBranch(ctx, client, org, req.Database, meta.BranchName)
	if err != nil {
		result.Kept = []string{branch}
		result.KeptReason = "the delete failed; delete it by hand to reclaim branch quota"
		return result, fmt.Errorf("delete branch %s of the cancelled apply on %s: %w", meta.BranchName, req.Database, err)
	}
	if alreadyGone {
		logger.Info("cancelled apply's branch was already gone; nothing to reclaim",
			"organization", org, "planetscale_database", req.Database, "branch", meta.BranchName)
		return result, nil
	}
	logger.Info("deleted the branch of a cancelled apply that never reached its deploy request",
		"organization", org, "planetscale_database", req.Database, "branch", meta.BranchName)
	result.Reclaimed = []string{branch}
	return result, nil
}

// reclaimBranchAfterFailedResume deletes the branch a resumed drive was
// preparing when that drive fails before it creates the deploy request. Until
// a deploy request exists nothing else owns the branch's teardown, and the
// fresh drive's own cleanup does not cover a resumed drive, so a failure here
// would otherwise strand the branch against quota.
//
// Any error the drive returns fails the apply: the Vitess drive does not
// retry engine errors, classified or not, and recovery from a failed apply is
// a fresh plan and apply on a new branch (ST-2). So the failure's
// classification does not decide the branch's fate. The branch is kept only
// when another drive may still need it, or when it is not SchemaBot's to
// delete: an operator-supplied branch belongs to the operator, and a drive
// whose context ended is handing the apply to the driver that resumes it from
// this branch, unless an operator cancelled it, since no driver resumes a
// cancelled apply. Each kept branch is logged with the reason, so a branch left
// behind is explained rather than silent.
//
// The stored state names the resumed branch, so the delete stays under the
// drive's context for the whole request: a lease lost after the checks below,
// even while the delete is in flight, abandons it.
//
// The caller returns cause unchanged whatever happens here: a failed delete is
// logged by deleteOwnedBranch and never replaces the failure that ended the
// drive.
func (e *Engine) reclaimBranchAfterFailedResume(ctx context.Context, client psclient.PSClient, org string, req *engine.ApplyRequest, branch string, cause error) {
	logger := e.applyLogger(req)
	switch {
	case !deployRequestDeletesBranch(req.Options):
		logger.Info("resumed apply failed before its deploy request; keeping the operator-supplied branch",
			"organization", org, "planetscale_database", req.Database, "branch", branch, "apply_error", cause)
	case engine.CancelledByOperator(ctx):
		// No driver resumes a cancelled apply, so the branch has no one left
		// to need it, and the delete must outlive the cancelled context.
		e.deleteOwnedBranch(context.WithoutCancel(ctx), logger, client, org, req.Database, branch, cause)
	case ctx.Err() != nil:
		logger.Info("resumed drive ended before its deploy request; keeping the branch for the driver that resumes the apply",
			"organization", org, "planetscale_database", req.Database, "branch", branch, "apply_error", cause)
	default:
		e.deleteOwnedBranch(ctx, logger, client, org, req.Database, branch, cause)
	}
}
