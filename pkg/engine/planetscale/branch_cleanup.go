package planetscale

import (
	"context"
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
	deleteCtx, cancel := context.WithTimeout(ctx, branchDeleteTimeout)
	defer cancel()

	err := client.DeleteBranch(deleteCtx, &ps.DeleteDatabaseBranchRequest{
		Organization: org,
		Database:     database,
		Branch:       branch,
	})
	if err != nil && ctx.Err() != nil {
		logger.Warn("drive ended while deleting the branch; abandoned the delete and left the branch for the driver that resumes the apply",
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
	logger.Info("deleted the branch created for an apply that failed before its deploy request",
		"organization", org, "planetscale_database", database, "branch", branch, "apply_error", cause)
}

// reclaimBranchAfterFailedResume deletes the branch a resumed drive was
// preparing when that drive fails for good before it creates the deploy
// request. Until a deploy request exists nothing else owns the branch's
// teardown, and the fresh drive's own cleanup does not cover a resumed drive,
// so a terminal failure here would otherwise strand the branch against quota.
//
// The branch is kept whenever another drive may still need it, or when it is
// not SchemaBot's to delete: an operator-supplied branch belongs to the
// operator; a drive whose context ended is handing the apply to the driver
// that resumes it from this branch; and a retryable failure leaves the branch
// as the starting point for the retry. Each kept branch is logged with the
// reason, so a branch left behind is explained rather than silent.
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
	case ctx.Err() != nil:
		logger.Info("resumed drive ended before its deploy request; keeping the branch for the driver that resumes the apply",
			"organization", org, "planetscale_database", req.Database, "branch", branch, "apply_error", cause)
	case engine.IsRetryable(cause):
		logger.Info("resumed apply failed with a retryable error before its deploy request; keeping the branch for the retry to resume on",
			"organization", org, "planetscale_database", req.Database, "branch", branch, "apply_error", cause)
	default:
		e.deleteOwnedBranch(ctx, logger, client, org, req.Database, branch, cause)
	}
}
