package planetscale

import (
	"context"
	"fmt"
	"log/slog"
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

var _ engine.CancelledResumeStateReclaimer = (*Engine)(nil)

// ReclaimCancelledResumeState deletes the branch the stored resume state names
// when no deploy request will tear it down and SchemaBot created it. It serves
// the cancel that lands between drives: the drive that persisted the branch has
// exited, and the drive consuming the cancel stands down before it resumes, so
// no drive's own cleanup ever sees the branch.
//
// An operator-supplied branch is left to the operator. A deploy request tears
// its branch down (auto_delete_branch) only when its deploy finishes, so a
// state naming a deploy request is left to it unless the deploy request was
// closed before it deployed — the engine's Cancel closes a deferred deploy
// request that way — in which case nothing else removes the branch and it is
// deleted here. A deploy request that cannot be read is left to own the
// branch, since only a read proves it will never deploy. Each kept branch is
// logged with the reason on the schema change's logger, so a branch left
// behind is explained rather than silent. A branch PlanetScale no longer has is
// reported as already gone, not as a failure, without naming a cause the
// engine cannot know (see deleteBranch).
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
	case !deployRequestDeletesBranch(req.Options):
		logger.Info("cancelled apply ran on an operator-supplied branch; keeping it for the operator",
			"organization", org, "planetscale_database", req.Database, "branch", meta.BranchName, "deploy_request", meta.DeployRequestID)
		result.Kept = []string{branch}
		result.KeptReason = "the operator supplied it and it is theirs to keep"
		return result, nil
	}

	client, err := e.getClient(req.Credentials)
	if err != nil && meta.DeployRequestID != 0 {
		logger.Warn("could not reach PlanetScale to read the cancelled apply's deploy request; leaving the branch to the deploy request",
			"organization", org, "planetscale_database", req.Database, "branch", meta.BranchName, "deploy_request", meta.DeployRequestID, "error", err)
		return result, nil
	}
	if err != nil {
		result.Kept = []string{branch}
		result.KeptReason = "the PlanetScale API could not be reached; delete it by hand to reclaim branch quota"
		return result, fmt.Errorf("reclaim branch %s of the cancelled apply on %s: %w", meta.BranchName, req.Database, err)
	}
	if meta.DeployRequestID != 0 {
		dr, err := client.GetDeployRequest(ctx, &ps.GetDeployRequestRequest{
			Organization: org,
			Database:     req.Database,
			Number:       meta.DeployRequestID,
		})
		if err != nil {
			logger.Warn("could not read the cancelled apply's deploy request; leaving the branch to the deploy request",
				"organization", org, "planetscale_database", req.Database, "branch", meta.BranchName, "deploy_request", meta.DeployRequestID, "error", err)
			return result, nil
		}
		if !deployRequestClosedUndeployed(dr) {
			logger.Info("cancelled apply's branch is owned by its deploy request, which tears the branch down when its deploy finishes",
				"organization", org, "planetscale_database", req.Database, "branch", meta.BranchName, "deploy_request", meta.DeployRequestID,
				"deploy_request_state", dr.State, "deployment_state", dr.DeploymentState)
			return result, nil
		}
		logger.Info("cancelled apply's deploy request was closed before it deployed, so it will not tear the branch down; deleting the branch",
			"organization", org, "planetscale_database", req.Database, "branch", meta.BranchName, "deploy_request", meta.DeployRequestID,
			"deployment_state", dr.DeploymentState)
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
	logger.Info("deleted the branch of a cancelled apply that no deploy request tears down",
		"organization", org, "planetscale_database", req.Database, "branch", meta.BranchName, "deploy_request", meta.DeployRequestID)
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
