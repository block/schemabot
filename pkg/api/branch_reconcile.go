package api

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// BranchReconcileInterval is how often the branch reconcile pass runs. A
// branch it can reclaim has already outlived the grace period, so a slow
// cadence finds everything a fast one would, a little later, while listing
// each database's branches far less often.
const BranchReconcileInterval = 1 * time.Hour

// branchReconcileSweep identifies the branch reconcile pass to the outcome
// handling it shares with the reaper's sweeps.
var branchReconcileSweep = reapSweep{
	busy:          storage.ErrBranchReconcileBusy,
	subject:       "orphaned engine branches",
	failureReason: "branch_reconcile_error",
}

// branchReconciler is the tern client capability the pass drives. The local
// client for a PlanetScale database has it; a remote client does not, because
// the deployment it routes to owns that database's branches.
type branchReconciler interface {
	ReconcileOrphanedBranches(ctx context.Context, opts tern.BranchReconcileOptions) (tern.BranchReconcileResult, error)
}

// branchReconcileTarget is one PlanetScale database environment this process
// executes against itself.
type branchReconcileTarget struct {
	database    string
	environment string
}

// branchReconcilePass returns the pass to run on the reaper's cadence, or
// false when the pass should not start. Each reason it declines is logged
// separately, because each asks something different of the operator.
//
// The pass is a reaper (OW-10): it deletes branches that belong to applies no
// driver is coming back for, so it runs with the operator and one elected
// instance runs it per tick.
func (s *Service) branchReconcilePass() (func(context.Context), bool) {
	if !s.config.BranchReconcile.Enabled {
		s.logger.Debug("branch reconcile pass not started because it is disabled; orphaned PlanetScale branches are left until deleted by hand")
		return nil, false
	}
	grace, err := s.config.BranchReconcileGrace()
	if err != nil {
		// Validate() rejects an invalid grace before the server starts, so
		// this guards direct embedders that skip config validation.
		s.logger.Error("branch reconcile pass not started because its grace period is invalid; orphaned PlanetScale branches are left until the config is fixed", "error", err)
		return nil, false
	}
	targets := s.branchReconcileTargets()
	if len(targets) == 0 {
		s.logger.Info("branch reconcile pass not started because no local PlanetScale database targets are configured; the deployment that executes against each database reconciles its branches")
		return nil, false
	}
	deleteOrphans := s.config.BranchReconcile.Delete
	s.logger.Info("branch reconcile pass enabled",
		"delete", deleteOrphans, "grace", grace, "interval", BranchReconcileInterval, "targets", len(targets))
	return func(ctx context.Context) {
		s.runBranchReconcilePass(ctx, targets, grace, deleteOrphans)
	}, true
}

// branchReconcileTargets returns the PlanetScale database environments this
// process executes against itself, in a stable order. One with no local DSN is
// routed to another deployment, which owns its branches.
func (s *Service) branchReconcileTargets() []branchReconcileTarget {
	var targets []branchReconcileTarget
	for dbName, dbConfig := range s.config.DatabaseConfigs() {
		if dbConfig.Type != storage.DatabaseTypeVitess {
			continue
		}
		for envName, envConfig := range dbConfig.Environments {
			if !envConfig.HasLocalDSN() {
				continue
			}
			targets = append(targets, branchReconcileTarget{database: dbName, environment: envName})
		}
	}
	slices.SortFunc(targets, func(a, b branchReconcileTarget) int {
		return cmp.Or(strings.Compare(a.database, b.database), strings.Compare(a.environment, b.environment))
	})
	return targets
}

// runBranchReconcilePass reconciles every target on the elected instance. The
// targets are independent databases, so one that fails is logged and the pass
// moves on to the next; the failures are joined into the pass's outcome.
func (s *Service) runBranchReconcilePass(ctx context.Context, targets []branchReconcileTarget, grace time.Duration, deleteOrphans bool) {
	deleted := 0
	err := s.storage.Applies().WithBranchReconcileElection(ctx, func(ctx context.Context) error {
		var errs []error
		for _, target := range targets {
			n, err := s.reconcileTargetBranches(ctx, target, grace, deleteOrphans)
			deleted += n
			if err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	})
	s.recordSweepOutcome(ctx, branchReconcileSweep, deleted, err)
}

// reconcileTargetBranches runs the reconcile on one target and returns how
// many branches it deleted.
func (s *Service) reconcileTargetBranches(ctx context.Context, target branchReconcileTarget, grace time.Duration, deleteOrphans bool) (int, error) {
	logger := s.logger.With("database", target.database, "environment", target.environment, "database_type", storage.DatabaseTypeVitess)
	client, err := s.TernClient(target.database, target.environment)
	if err != nil {
		logger.Error("branch reconcile skipped this database because its client could not be created; it is retried on the next pass", "error", err)
		return 0, fmt.Errorf("reconcile branches of %s/%s: %w", target.database, target.environment, err)
	}
	reconciler, ok := client.(branchReconciler)
	if !ok {
		logger.Error("branch reconcile skipped this database because its client cannot reconcile branches", "client_type", fmt.Sprintf("%T", client))
		return 0, fmt.Errorf("reconcile branches of %s/%s: client %T cannot reconcile branches", target.database, target.environment, client)
	}
	result, err := reconciler.ReconcileOrphanedBranches(ctx, tern.BranchReconcileOptions{
		Grace:  grace,
		Delete: deleteOrphans,
		Now:    s.clock.Now(),
	})
	if err != nil {
		logger.Error("branch reconcile on this database was incomplete; branches it could not judge or delete are kept and retried on the next pass",
			"planetscale_database", result.Database, "deleted", result.Deleted, "error", err)
		return result.Deleted, fmt.Errorf("reconcile branches of %s/%s: %w", target.database, target.environment, err)
	}
	return result.Deleted, nil
}
