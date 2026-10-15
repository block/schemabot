package webhook

import (
	"context"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// missingApplyIDData resolves the applies on the PR that a command named
// without an apply ID could act on, so the reply can name each one's exact
// command. actsOn selects the applies the command can act on by state. A
// failed lookup is logged and leaves the data incomplete, so the reply points
// at the comment that carries the apply ID instead of claiming there is none.
func (h *Handler) missingApplyIDData(ctx context.Context, repo string, pr int, result CommandResult, command string, actsOn func(applyState string) bool) templates.MissingApplyIDData {
	data := templates.MissingApplyIDData{
		Command:     command,
		Environment: result.Environment,
		Tenant:      h.deploymentTenant(),
	}
	applies, ok := h.prAppliesForMissingApplyID(ctx, repo, pr, result, command)
	if !ok {
		return data
	}
	seen := make(map[string]bool)
	for _, apply := range applies {
		if result.Environment != "" && apply.Environment != result.Environment {
			continue
		}
		if !actsOn(apply.State) {
			continue
		}
		// GetByPR returns the newest apply first, so the first apply seen for
		// a database and environment is the one the command means.
		target := apply.Database + "\x00" + apply.Environment
		if seen[target] {
			continue
		}
		seen[target] = true
		data.Candidates = append(data.Candidates, templates.ApplyIDCandidate{
			ApplyID:     apply.ApplyIdentifier,
			Database:    apply.Database,
			Environment: apply.Environment,
		})
	}
	data.Complete = h.storageHoldsEveryApplyForCommand(repo, result.Tenant)
	if !data.Complete {
		h.logger.Info("missing apply ID reply lists only this deployment's applies; sibling deployments hold the rest",
			"command", command,
			"repo", repo,
			"pr", pr,
			"environment", result.Environment,
			"candidate_count", len(data.Candidates))
	}
	return data
}

func (h *Handler) prAppliesForMissingApplyID(ctx context.Context, repo string, pr int, result CommandResult, command string) ([]*storage.Apply, bool) {
	if h.service == nil || h.service.Storage() == nil || h.service.Storage().Applies() == nil {
		h.logger.Warn("missing apply ID reply will point at the progress comment: apply storage is not configured",
			"command", command,
			"repo", repo,
			"pr", pr,
			"environment", result.Environment)
		return nil, false
	}
	applies, err := h.service.Storage().Applies().GetByPR(ctx, repo, pr)
	if err != nil {
		h.logger.Warn("missing apply ID reply will point at the progress comment: failed to load the PR's applies",
			"command", command,
			"repo", repo,
			"pr", pr,
			"environment", result.Environment,
			"error", err)
		return nil, false
	}
	return applies, true
}

// storageHoldsEveryApplyForCommand reports whether this deployment's storage
// holds every apply an unaddressed command on the repo could mean. On an
// aggregate repo each deployment stores only its own applies, so only a
// command scoped to this deployment's tenant is fully answered here.
func (h *Handler) storageHoldsEveryApplyForCommand(repo, tenant string) bool {
	if tenant != "" {
		return true
	}
	config, ok := h.serverConfig()
	if !ok {
		return true
	}
	return config.AggregateRoleForRepo(repo) == ""
}

// controlCommandActsOn reports whether an apply in applyState is one an
// apply-scoped control command can act on: one that has not settled. A
// stopped apply is terminal but not settled, and start and cancel act on it.
func controlCommandActsOn(applyState string) bool {
	return !state.IsTerminalApplyState(applyState) || state.IsState(applyState, state.Apply.Stopped)
}

// rollbackActsOn reports whether an apply in applyState is one a rollback can
// revert: a completed apply.
func rollbackActsOn(applyState string) bool {
	return state.IsState(applyState, state.Apply.Completed)
}
