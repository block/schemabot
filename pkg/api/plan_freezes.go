package api

import (
	"context"
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
)

// PlanFreezes are the freezes that cover a planned change, read in one
// snapshot labelled with the database clock.
type PlanFreezes struct {
	// Now is the database clock the freezes were read at.
	Now time.Time
	// InEffect are the freezes that would refuse the change's apply, start
	// and cutover now.
	InEffect []*storage.EnvironmentFreeze
	// Scheduled are the freezes that have not started yet and will refuse it
	// once they do, oldest start first.
	Scheduled []*storage.EnvironmentFreeze
}

// FreezesCoveringPlan lists the freezes that cover a change to database in
// environment, so a plan can say what its apply will meet. It scopes them by
// the deployments the change gate checks (changeGateDeployments), so the plan
// and its apply agree on which freezes cover the change. A plan narrowed to
// one target passes narrowed, as its apply does.
//
// It only informs: the apply's change gate reads the freezes again and is the
// one that refuses (AZ-10). A server without freeze storage has no freezes.
func (s *Service) FreezesCoveringPlan(ctx context.Context, environment, database, deployment string, narrowed bool) (PlanFreezes, error) {
	if s.storage == nil || s.storage.Freezes() == nil {
		return PlanFreezes{}, nil
	}
	// List needs a positive count of recent freezes, which this ignores.
	listing, err := s.storage.Freezes().List(ctx, storage.CanonicalKey(environment), 1)
	if err != nil {
		return PlanFreezes{}, fmt.Errorf("list freezes in %s: %w", environment, err)
	}
	deployments := s.changeGateDeployments(database, environment, deployment, narrowed)
	result := PlanFreezes{Now: listing.Now}
	for _, freeze := range listing.Current {
		if !freezeCoversChange(freeze, deployments, database) {
			continue
		}
		if freeze.InEffectAt(listing.Now) {
			result.InEffect = append(result.InEffect, freeze)
		} else {
			result.Scheduled = append(result.Scheduled, freeze)
		}
	}
	return result, nil
}

// planResponseFreezes lists the freezes that cover a plan's change for its
// response, those in effect first, so a CLI or API caller learns before
// applying what the apply will meet, as the PR plan comment says. database is
// the request's: the response may leave it empty. A lookup that fails leaves
// the freezes off and never fails the plan, because the apply checks them
// again.
func (s *Service) planResponseFreezes(ctx context.Context, database, environment, repository string, resp *apitypes.PlanResponse) []apitypes.Freeze {
	freezes, err := s.FreezesCoveringPlan(ctx, environment, database, resp.Deployment, resp.NarrowedTo != "")
	if err != nil {
		s.logger.Warn("could not read the freezes for the plan response; it names no freeze, and the apply checks the freezes again",
			"repository", repository, "database", database, "environment", environment, "error", err)
		return nil
	}
	var out []apitypes.Freeze
	for _, freeze := range freezes.InEffect {
		out = append(out, freezeResponse(freeze, freezes.Now))
	}
	for _, freeze := range freezes.Scheduled {
		out = append(out, freezeResponse(freeze, freezes.Now))
	}
	return out
}
