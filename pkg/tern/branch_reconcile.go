package tern

import (
	"context"
	"errors"
	"fmt"
	"time"

	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/block/schemabot/pkg/engine/planetscale"
	"github.com/block/schemabot/pkg/inventory"
	"github.com/block/schemabot/pkg/storage"
)

// branchReconcileHoldTimeout bounds how long one delete holds the apply
// target: the re-read of the applies, the delete request, and the wait for the
// lock itself. It is half of what a driver waits for the target, so a driver
// that queued behind the hold still gets the lock inside its own wait, and one
// slow delete cannot hold the rest of a pass.
const branchReconcileHoldTimeout = storage.ApplyTargetLockWait / 2

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
	// Environment is the environment this database is registered under. With
	// the database it names the apply target the pass holds while it deletes
	// a branch, so it is required when Delete is set.
	Environment string
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
//   - it has the shape SchemaBot generates for the database, so the main
//     branch, production branches, and a branch an operator named are never
//     touched;
//   - it is older than opts.Grace;
//   - no unsettled apply uses it. An apply uses the branch its stored resume
//     state names, the branch an operator supplied with the branch option, and
//     the branch its plan names, since a generated name is a function of the
//     database and the plan identifier (planetscale.GenerateBranchName). That
//     last one covers an apply that exists but has not recorded a branch yet,
//     and the stopped applies are read with the running ones because a driver
//     may still resume them;
//   - no open deploy request uses it.
//
// The delete holds the apply target this database is registered under
// (storage.ApplyStore.WithTargetHeld, keyed by the database, its type, and
// opts.Environment) across a final read of the applies and the delete itself.
// No apply is created for, claimed on, or advanced on the target while it is
// held, so the applies the read saw are the applies when the branch goes: an
// apply re-run for the same plan either already exists, and keeps its branch,
// or is created after the delete and makes a new one. Every drive on the
// target waits behind the hold, which is why it is bounded by
// branchReconcileHoldTimeout and a hold that runs out keeps the branch. The
// hold covers this registration's target alone, so the pass only judges
// branches of databases this storage alone applies to.
//
// Anything it cannot read fails toward keeping branches: a storage error ends
// the pass before anything is deleted, and a deploy request lookup that fails
// keeps that branch. Each decision is logged with the branch name. Errors on
// individual branches are joined and returned after the pass finishes.
func (c *LocalClient) ReconcileOrphanedBranches(ctx context.Context, opts BranchReconcileOptions) (BranchReconcileResult, error) {
	if c.config.Type != storage.DatabaseTypeVitess {
		return BranchReconcileResult{}, fmt.Errorf("reconcile branches for database %s: not a PlanetScale database (type %s)", c.config.Database, c.config.Type)
	}
	if opts.Delete && opts.Environment == "" {
		return BranchReconcileResult{}, fmt.Errorf("reconcile branches for database %s: an environment is required to hold the apply target while deleting", c.config.Database)
	}
	target, err := c.planetScalePullClient()
	if err != nil {
		return BranchReconcileResult{}, fmt.Errorf("reconcile branches: %w", err)
	}
	logger := c.logger.With("database", c.config.Database, "environment", opts.Environment, "organization", target.org, "planetscale_database", target.database)
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
	inUse, err := c.branchesInUse(ctx, target.database)
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
		outcome, err := c.reclaimBranch(ctx, target, branch.Name, opts.Environment)
		if err != nil {
			errs = append(errs, err)
		}
		switch outcome {
		case branchKeptInUse:
			result.Kept++
			logger.Info("keeping SchemaBot branch: an apply that started during the pass uses it", attrs...)
		case branchKeptUnjudged:
			result.Kept++
			logger.Warn("keeping SchemaBot branch: the apply target could not be held and re-read before the delete", append(attrs, "error", err)...)
		case branchDeleteFailed:
			result.Orphaned++
			logger.Error("failed to delete orphaned SchemaBot branch; it is retried on the next pass, or delete it manually to reclaim branch quota",
				append(attrs, "error", err)...)
		case branchDeleted:
			result.Orphaned++
			result.Deleted++
			logger.Info("deleted orphaned SchemaBot branch: no unsettled apply uses it and no open deploy request uses it", attrs...)
		}
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
		return true, "an unsettled apply uses it", nil
	}
	if branch.CreatedAt.IsZero() {
		return true, "its creation time is unknown, so its age cannot be judged", nil
	}
	if age := opts.Now.Sub(branch.CreatedAt); age <= opts.Grace {
		return true, fmt.Sprintf("not older than the grace period (%s)", opts.Grace), nil
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

// branchesInUse returns every branch an unsettled PlanetScale apply uses: the
// branch its stored resume state names, the branch an operator supplied with
// the branch option, and the branch generated for its plan on database. It
// reads across all registered databases, so a branch is never judged unused
// because the apply using it was registered under another identifier.
func (c *LocalClient) branchesInUse(ctx context.Context, database string) (map[string]bool, error) {
	applies, err := c.storage.Applies().GetUnsettled(ctx)
	if err != nil {
		return nil, fmt.Errorf("read unsettled applies: %w", err)
	}
	planIdentifiers := make(map[int64]string)
	planIdentifier := func(planID int64) (string, error) {
		if identifier, ok := planIdentifiers[planID]; ok {
			return identifier, nil
		}
		plan, err := c.storage.Plans().GetByID(ctx, planID)
		if err != nil {
			return "", fmt.Errorf("read plan %d: %w", planID, err)
		}
		if plan == nil {
			return "", fmt.Errorf("plan %d does not exist", planID)
		}
		planIdentifiers[planID] = plan.PlanIdentifier
		return plan.PlanIdentifier, nil
	}

	inUse := make(map[string]bool)
	for _, apply := range applies {
		if apply.DatabaseType != storage.DatabaseTypeVitess {
			continue
		}
		if branch := apply.GetOptions().Branch; branch != "" {
			inUse[branch] = true
		}
		if apply.PlanID != 0 {
			identifier, err := planIdentifier(apply.PlanID)
			if err != nil {
				return nil, fmt.Errorf("name the branch of apply %s: %w", apply.ApplyIdentifier, err)
			}
			inUse[planetscale.GenerateBranchName(database, identifier)] = true
		}
		ops, err := c.storage.ApplyOperations().ListByApply(ctx, apply.ID)
		if err != nil {
			return nil, fmt.Errorf("read operations of apply %s: %w", apply.ApplyIdentifier, err)
		}
		for _, op := range ops {
			planID, err := storage.PlanIDForOperation(apply, op)
			if err != nil {
				return nil, fmt.Errorf("name the branch of apply %s operation %d: %w", apply.ApplyIdentifier, op.ID, err)
			}
			identifier, err := planIdentifier(planID)
			if err != nil {
				return nil, fmt.Errorf("name the branch of apply %s operation %d: %w", apply.ApplyIdentifier, op.ID, err)
			}
			inUse[planetscale.GenerateBranchName(database, identifier)] = true

			resumeState, err := c.storage.ApplyOperations().GetEngineResumeState(ctx, op.ID)
			if errors.Is(err, storage.ErrEngineResumeStateNotFound) {
				c.logger.Debug("apply operation has recorded no branch yet; its plan names the branch it will use",
					append(apply.LogAttrs(), "operation_id", op.ID)...)
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("read resume state of apply %s operation %d: %w", apply.ApplyIdentifier, op.ID, err)
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

// branchReclaimOutcome is what became of one orphaned branch the pass set out
// to delete.
type branchReclaimOutcome int

const (
	// branchKeptUnjudged: the apply target could not be held, or the applies
	// could not be re-read under it, so the branch was not judged again and
	// is kept.
	branchKeptUnjudged branchReclaimOutcome = iota
	// branchKeptInUse: an apply that started after the first read uses the
	// branch, so it is kept.
	branchKeptInUse
	// branchDeleteFailed: the branch is orphaned but the delete failed.
	branchDeleteFailed
	// branchDeleted: the branch was deleted.
	branchDeleted
)

// reclaimBranch deletes an orphaned branch while holding the apply target the
// database is registered under in environment, after a final read of the
// applies under that hold. Only the outcome says what happened to the branch:
// an error is the failure behind it, when there is one.
func (c *LocalClient) reclaimBranch(ctx context.Context, target *planetScalePullTarget, branch, environment string) (branchReclaimOutcome, error) {
	holdCtx, cancel := context.WithTimeout(ctx, branchReconcileHoldTimeout)
	defer cancel()

	outcome := branchKeptUnjudged
	err := c.storage.Applies().WithTargetHeld(holdCtx, c.config.Database, storage.DatabaseTypeVitess, environment, func(ctx context.Context) error {
		inUse, err := c.branchesInUse(ctx, target.database)
		if err != nil {
			return fmt.Errorf("re-read the applies under the held target: %w", err)
		}
		if inUse[branch] {
			outcome = branchKeptInUse
			return nil
		}
		if err := target.client.DeleteBranch(ctx, &ps.DeleteDatabaseBranchRequest{
			Organization: target.org, Database: target.database, Branch: branch,
		}); err != nil {
			outcome = branchDeleteFailed
			return fmt.Errorf("delete branch %s of %s/%s: %w", branch, target.org, target.database, err)
		}
		outcome = branchDeleted
		return nil
	})
	if err != nil && outcome == branchKeptUnjudged {
		return outcome, fmt.Errorf("hold apply target %s/%s/%s to delete branch %s: %w", c.config.Database, storage.DatabaseTypeVitess, environment, branch, err)
	}
	return outcome, err
}

// RecentTargetWindow is how far past the grace period a data plane looks back
// for the PlanetScale targets it applied to, measured from each apply's last
// write. A branch is orphaned no later than its apply settles, and settling
// writes the apply, so the target stays listed for at least this long after
// the branch first becomes reclaimable: a month of hourly passes to reclaim
// it.
const RecentTargetWindow = 30 * 24 * time.Hour

// RecentTargetsReconcileResult counts what one TargetRouter pass did.
type RecentTargetsReconcileResult struct {
	// Targets is how many recently applied targets the pass visited.
	Targets int
	// Deleted is how many orphaned branches it deleted across them.
	Deleted int
	// Unreachable is how many of the visited targets this data plane can no
	// longer reach (see branchReconcileTargetUnreachable). Their branches are
	// kept, and they are not counted as failures.
	Unreachable int
}

// ReconcileRecentTargetBranches runs LocalClient.ReconcileOrphanedBranches on
// every PlanetScale target this data plane's applies were written for within
// the grace period plus RecentTargetWindow.
//
// A data plane that resolves targets per request has no static list of
// databases, so the targets come from its own storage: each distinct
// (database, environment, target) its apply operations ran against. Each is
// reached through the same route an apply on it takes, so the reconcile runs
// with the client, and holds the apply target, that a drive of the apply would
// use. opts.Environment is ignored: each target is reconciled under the
// environment its applies were stored with.
//
// A target that cannot be resolved or reconciled is logged and the pass moves
// on; the failures are joined and returned after every target is visited. A
// target this data plane can no longer reach at all, because the inventory no
// longer has it or its client has no PlanetScale client, is logged at warn and
// not returned as a failure: it would otherwise fail every pass until it left
// the window, and hide a failure on a target that can be reconciled.
func (r *TargetRouter) ReconcileRecentTargetBranches(ctx context.Context, opts BranchReconcileOptions) (RecentTargetsReconcileResult, error) {
	lookback := opts.Grace + RecentTargetWindow
	targets, err := r.storage.ApplyOperations().ListRecentTargets(ctx, storage.DatabaseTypeVitess, lookback)
	if err != nil {
		return RecentTargetsReconcileResult{}, fmt.Errorf("reconcile branches: list PlanetScale targets applied to within %s: %w", lookback, err)
	}
	result := RecentTargetsReconcileResult{Targets: len(targets)}
	var errs []error
	for _, target := range targets {
		deleted, err := r.reconcileTargetBranches(ctx, target, opts)
		result.Deleted += deleted
		switch {
		case err == nil:
		case branchReconcileTargetUnreachable(err):
			result.Unreachable++
			r.logger.Warn("branch reconcile skipped a recently applied target this data plane can no longer reach; its branches are kept and are not reconciled from here",
				"database", target.Database, "environment", target.Environment, "target", target.Target, "error", err)
		default:
			r.logger.Error("branch reconcile on this target was incomplete; branches it could not judge or delete are kept and retried on the next pass",
				"database", target.Database, "environment", target.Environment, "target", target.Target, "deleted", deleted, "error", err)
			errs = append(errs, err)
		}
	}
	return result, errors.Join(errs...)
}

// branchReconcileTargetUnreachable reports whether err says the target cannot
// be reached at all, rather than that reaching it failed: the inventory
// answered and no longer has the target, or the target's client has no
// PlanetScale client. A resolver that cannot read the inventory, a missing
// organization, and every PlanetScale API or storage error are not this.
func branchReconcileTargetUnreachable(err error) bool {
	return errors.Is(err, inventory.ErrTargetNotFound) || errors.Is(err, errPlanetScaleClientNotConfigured)
}

// reconcileTargetBranches reconciles one recently applied target on its
// route's current client and returns how many branches it deleted.
func (r *TargetRouter) reconcileTargetBranches(ctx context.Context, target storage.AppliedTarget, opts BranchReconcileOptions) (int, error) {
	gen, err := r.clientOnlyForTarget(ctx, target.Target, storage.DatabaseTypeVitess, target.Environment, target.Database)
	if err != nil {
		return 0, fmt.Errorf("reconcile branches of %s/%s on target %q: %w", target.Database, target.Environment, target.Target, err)
	}
	defer r.release(gen)
	reconciler, ok := gen.client.(interface {
		ReconcileOrphanedBranches(context.Context, BranchReconcileOptions) (BranchReconcileResult, error)
	})
	if !ok {
		return 0, fmt.Errorf("reconcile branches of %s/%s on target %q: client %T cannot reconcile branches", target.Database, target.Environment, target.Target, gen.client)
	}
	opts.Environment = target.Environment
	reconciled, err := reconciler.ReconcileOrphanedBranches(ctx, opts)
	if err != nil {
		return reconciled.Deleted, fmt.Errorf("reconcile branches of %s/%s on target %q: %w", target.Database, target.Environment, target.Target, err)
	}
	return reconciled.Deleted, nil
}
