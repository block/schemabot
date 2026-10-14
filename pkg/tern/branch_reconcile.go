package tern

import (
	"context"
	"errors"
	"fmt"
	"time"

	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/block/schemabot/pkg/engine/planetscale"
	"github.com/block/schemabot/pkg/storage"
)

// branchReconcileDeleteTimeout bounds each branch delete, so one slow request
// cannot hold the rest of a pass.
const branchReconcileDeleteTimeout = 30 * time.Second

// BranchReconcileOptions tunes one ReconcileOrphanedBranches pass.
type BranchReconcileOptions struct {
	// Grace is how old a branch must be before it can be reclaimed, so a
	// branch is never judged on the first moments of its life.
	Grace time.Duration
	// Delete deletes the orphaned branches. When false the pass only reports
	// which branches it would delete.
	Delete bool
	// Now is the time the pass measures branch age against.
	Now time.Time
}

// BranchReconcileResult counts what one pass found.
type BranchReconcileResult struct {
	// Database is the PlanetScale database the pass listed.
	Database string
	// Candidates is how many branches had the shape SchemaBot generates.
	Candidates int
	// Kept is how many of them were kept, for any reason.
	Kept int
	// Orphaned is how many were found orphaned: deleted, or, without Delete,
	// reported as the ones a deleting pass would delete.
	Orphaned int
	// Deleted is how many orphaned branches were deleted.
	Deleted int
}

// ReconcileOrphanedBranches finds the branches SchemaBot created for this
// PlanetScale database that no drive can resume or finish, and deletes them
// when opts.Delete is set.
//
// Every failure before a deploy request already deletes its branch, so this
// pass covers only the residue: a branch delete abandoned after a lost lease,
// or one the API refused, leaves a branch that no apply will resume. A branch
// is reclaimed only when all of these hold, and is kept otherwise:
//
//   - it has the shape SchemaBot generates for the database, so an operator's
//     branch, the main branch, and production branches are never touched;
//   - it is older than opts.Grace;
//   - no in-progress apply's stored resume state or branch option names it,
//     read once before judging and again immediately before the delete, since
//     an apply re-run for the same plan reuses the branch of that name;
//   - no open deploy request uses it.
//
// The apply records its branch name before creating the branch, so a branch
// SchemaBot is using is always named by an in-progress apply in this storage.
// The pass therefore only judges branches of databases this storage alone
// applies to.
//
// Anything it cannot read fails toward keeping branches: a storage error ends
// the pass before anything is deleted, and a deploy request lookup that fails
// keeps that branch. Each decision is logged with the branch name. Errors on
// individual branches are joined and returned after the pass finishes.
func (c *LocalClient) ReconcileOrphanedBranches(ctx context.Context, opts BranchReconcileOptions) (BranchReconcileResult, error) {
	if c.config.Type != storage.DatabaseTypeVitess {
		return BranchReconcileResult{}, fmt.Errorf("reconcile branches for database %s: not a PlanetScale database (type %s)", c.config.Database, c.config.Type)
	}
	target, err := c.planetScalePullClient()
	if err != nil {
		return BranchReconcileResult{}, fmt.Errorf("reconcile branches: %w", err)
	}
	logger := c.logger.With("database", c.config.Database, "organization", target.org, "planetscale_database", target.database)
	result := BranchReconcileResult{Database: target.database}

	branches, err := target.client.ListBranches(ctx, &ps.ListDatabaseBranchesRequest{Organization: target.org, Database: target.database})
	if err != nil {
		return result, fmt.Errorf("reconcile branches: list branches of %s/%s: %w", target.org, target.database, err)
	}
	var candidates []*ps.DatabaseBranch
	for _, branch := range branches {
		if branch.Production || branch.Name == target.branch || !planetscale.IsGeneratedBranchName(target.database, branch.Name) {
			continue
		}
		candidates = append(candidates, branch)
	}
	result.Candidates = len(candidates)
	if len(candidates) == 0 {
		logger.Debug("branch reconciliation found no SchemaBot branches")
		return result, nil
	}

	// Read the branches in use after listing, so an apply that recorded a
	// branch while the listing ran is seen as using it.
	inUse, err := c.branchesInUse(ctx)
	if err != nil {
		return result, fmt.Errorf("reconcile branches of %s/%s: %w", target.org, target.database, err)
	}

	var errs []error
	for _, branch := range candidates {
		keep, reason, err := c.keepBranch(ctx, target, branch, inUse, opts)
		if err != nil {
			errs = append(errs, err)
		}
		attrs := []any{"branch", branch.Name, "created_at", branch.CreatedAt}
		if keep {
			result.Kept++
			if err != nil {
				logger.Warn("keeping SchemaBot branch: "+reason, append(attrs, "error", err)...)
			} else {
				logger.Debug("keeping SchemaBot branch: "+reason, attrs...)
			}
			continue
		}
		if !opts.Delete {
			result.Orphaned++
			logger.Info("branch reconciliation dry run: SchemaBot branch is orphaned and would be deleted", attrs...)
			continue
		}
		claimed, err := c.branchClaimedSince(ctx, branch.Name)
		if err != nil {
			result.Kept++
			logger.Warn("keeping SchemaBot branch: in-progress applies could not be re-read before the delete", append(attrs, "error", err)...)
			errs = append(errs, err)
			continue
		}
		if claimed {
			result.Kept++
			logger.Info("keeping SchemaBot branch: an apply that started during the pass names it", attrs...)
			continue
		}
		result.Orphaned++
		if err := c.deleteOrphanedBranch(ctx, target, branch.Name); err != nil {
			logger.Error("failed to delete orphaned SchemaBot branch; it is retried on the next pass, or delete it manually to reclaim branch quota",
				append(attrs, "error", err)...)
			errs = append(errs, err)
			continue
		}
		result.Deleted++
		logger.Info("deleted orphaned SchemaBot branch: no in-progress apply names it and no open deploy request uses it", attrs...)
	}
	logger.Info("branch reconciliation pass complete",
		"delete", opts.Delete, "candidates", result.Candidates, "kept", result.Kept,
		"orphaned", result.Orphaned, "deleted", result.Deleted)
	return result, errors.Join(errs...)
}

// keepBranch decides whether a SchemaBot branch must be kept, and why. A
// returned error is a lookup that failed, and the branch is then kept.
func (c *LocalClient) keepBranch(ctx context.Context, target *planetScalePullTarget, branch *ps.DatabaseBranch, inUse map[string]bool, opts BranchReconcileOptions) (bool, string, error) {
	if inUse[branch.Name] {
		return true, "an in-progress apply names it", nil
	}
	if branch.CreatedAt.IsZero() {
		return true, "its creation time is unknown, so its age cannot be judged", nil
	}
	if age := opts.Now.Sub(branch.CreatedAt); age < opts.Grace {
		return true, fmt.Sprintf("younger than the grace period (%s)", opts.Grace), nil
	}
	open, err := target.client.ListDeployRequests(ctx, &ps.ListDeployRequestsRequest{
		Organization: target.org, Database: target.database, Branch: branch.Name, State: "open",
	})
	if err != nil {
		return true, "its deploy requests could not be read", fmt.Errorf("list open deploy requests for branch %s of %s/%s: %w", branch.Name, target.org, target.database, err)
	}
	if len(open) > 0 {
		return true, fmt.Sprintf("open deploy request #%d uses it", open[0].Number), nil
	}
	return false, "", nil
}

// branchesInUse returns every branch an in-progress PlanetScale apply's stored
// resume state names, together with any branch an operator supplied for one.
// It reads across all databases, so a branch is never judged unused because
// the apply naming it was registered under another database identifier.
func (c *LocalClient) branchesInUse(ctx context.Context) (map[string]bool, error) {
	applies, err := c.storage.Applies().GetInProgress(ctx)
	if err != nil {
		return nil, fmt.Errorf("read in-progress applies: %w", err)
	}
	inUse := make(map[string]bool)
	for _, apply := range applies {
		if apply.DatabaseType != storage.DatabaseTypeVitess {
			continue
		}
		if branch := apply.GetOptions().Branch; branch != "" {
			inUse[branch] = true
		}
		ops, err := c.storage.ApplyOperations().ListByApply(ctx, apply.ID)
		if err != nil {
			return nil, fmt.Errorf("read operations of apply %s: %w", apply.ApplyIdentifier, err)
		}
		for _, op := range ops {
			resumeState, err := c.storage.ApplyOperations().GetEngineResumeState(ctx, op.ID)
			if err != nil {
				return nil, fmt.Errorf("read resume state of apply %s operation %d: %w", apply.ApplyIdentifier, op.ID, err)
			}
			if resumeState == nil {
				continue
			}
			branch, err := planetscale.BranchNameFromResumeMetadata(resumeState.Metadata)
			if err != nil {
				return nil, fmt.Errorf("apply %s operation %d: %w", apply.ApplyIdentifier, op.ID, err)
			}
			if branch != "" {
				inUse[branch] = true
			}
		}
	}
	return inUse, nil
}

// branchClaimedSince re-reads the in-progress applies and reports whether one
// now names branch, so an apply that started after the pass first read them
// keeps the branch it is about to reuse.
func (c *LocalClient) branchClaimedSince(ctx context.Context, branch string) (bool, error) {
	inUse, err := c.branchesInUse(ctx)
	if err != nil {
		return false, fmt.Errorf("re-read branches in use before deleting branch %s: %w", branch, err)
	}
	return inUse[branch], nil
}

func (c *LocalClient) deleteOrphanedBranch(ctx context.Context, target *planetScalePullTarget, branch string) error {
	deleteCtx, cancel := context.WithTimeout(ctx, branchReconcileDeleteTimeout)
	defer cancel()
	if err := target.client.DeleteBranch(deleteCtx, &ps.DeleteDatabaseBranchRequest{
		Organization: target.org, Database: target.database, Branch: branch,
	}); err != nil {
		return fmt.Errorf("delete branch %s of %s/%s: %w", branch, target.org, target.database, err)
	}
	return nil
}
