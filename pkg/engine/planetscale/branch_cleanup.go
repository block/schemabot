package planetscale

import (
	"context"
	"time"

	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/block/schemabot/pkg/psclient"
)

// branchDeleteTimeout bounds the cleanup delete so a slow or unreachable
// PlanetScale API cannot hold the apply's failure path open.
const branchDeleteTimeout = 30 * time.Second

// deleteOwnedBranch removes a branch this apply created and no deploy request
// took ownership of, so a failure while preparing the branch does not strand
// quota. cause is the failure that triggered the cleanup; it is logged with the
// outcome so the two are readable together.
//
// The delete runs on its own deadline, detached from the apply's context, so a
// cancelled or timed-out apply still cleans up after itself. A branch that could
// not be deleted is logged at error level with the identifiers needed to remove
// it by hand — an undeletable branch must be visible, not silently retried.
func (e *Engine) deleteOwnedBranch(ctx context.Context, client psclient.PSClient, org, database, branch string, cause error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), branchDeleteTimeout)
	defer cancel()

	err := client.DeleteBranch(ctx, &ps.DeleteDatabaseBranchRequest{
		Organization: org,
		Database:     database,
		Branch:       branch,
	})
	if err != nil {
		e.logger.Error("failed to delete the branch left behind by a failed apply; delete it manually to reclaim branch quota",
			"organization", org, "database", database, "branch", branch,
			"apply_error", cause, "error", err)
		return
	}
	e.logger.Info("deleted the branch created for an apply that failed before its deploy request",
		"organization", org, "database", database, "branch", branch, "apply_error", cause)
}
