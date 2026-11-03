package webhook

import (
	"context"
	"net/http"

	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// controlCommandNamesApply reports whether a command that omits -e names an
// apply whose stored environment settles which deployment answers it: every
// command that takes an apply ID except rollback, whose missing-argument
// replies are its own. A -t-scoped command names its deployment already.
func controlCommandNamesApply(result CommandResult) bool {
	spec, ok := specByName[result.Action]
	if !ok || !spec.HasApplyID || result.Action == action.Rollback {
		return false
	}
	return result.ApplyID != "" && result.Tenant == ""
}

// controlApplyMayLiveElsewhere reports whether an apply this deployment does
// not store may be stored by another deployment on the repo: one that serves
// only some environments, or one of an aggregate repo's deployments.
func (h *Handler) controlApplyMayLiveElsewhere(repo string) bool {
	config, ok := h.serverConfig()
	if !ok {
		return false
	}
	return len(config.AllowedEnvironments) > 0 || config.AggregateRoleForRepo(repo) != ""
}

// answerMissingEnvForNamedApply answers a control command that names an apply
// but omits -e, when the apply settles who answers. Each deployment keeps its
// own storage, so the one that stores the apply is the one deployment that
// knows its environment: it answers with the exact command, whatever its
// respond_to_unscoped setting, and the others stay silent. It reports whether
// it handled the command. When the lookup fails, or no other deployment can
// hold the apply, it returns false and the usual missing-environment reply
// applies.
func (h *Handler) answerMissingEnvForNamedApply(ctx context.Context, w http.ResponseWriter, repo string, pr int, installationID int64, deliveryID, requestedBy string, result CommandResult) bool {
	if h.service == nil || h.service.Storage() == nil {
		h.logger.Warn("storage not configured to find the apply a control command names; the missing-environment reply follows the unscoped policy",
			"repo", repo, "pr", pr, "action", result.Action, "apply_id", result.ApplyID)
		return false
	}
	apply, err := h.service.Storage().Applies().GetByApplyIdentifier(ctx, result.ApplyID)
	if err != nil {
		h.logger.Warn("could not look up the apply a control command names; the missing-environment reply follows the unscoped policy",
			"repo", repo, "pr", pr, "action", result.Action, "apply_id", result.ApplyID, "error", err)
		return false
	}
	if apply == nil || !h.service.Config().IsEnvironmentAllowed(apply.Environment) {
		if !h.controlApplyMayLiveElsewhere(repo) {
			h.logger.Info("control command without -e names an apply this deployment does not store; posting the missing-environment reply",
				"repo", repo, "pr", pr, "action", result.Action, "apply_id", result.ApplyID)
			return false
		}
		h.logger.Info("control command without -e names an apply another deployment stores; staying silent so that deployment answers",
			"repo", repo, "pr", pr, "action", result.Action, "apply_id", result.ApplyID)
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "missing environment deferred to the apply's deployment"})
		return true
	}
	h.acknowledgeCommand(repo, pr, installationID, deliveryID, result.CommentID)
	if apply.Repository != repo || apply.PullRequest != pr {
		h.logger.Warn("control command without -e names an apply of another PR",
			append(apply.LogAttrs(), "command_repo", repo, "command_pr", pr, "action", result.Action)...)
		h.postCommandError(repo, pr, installationID, result.Action, apply.Environment, requestedBy, "Apply "+result.ApplyID+" is not associated with this PR")
		h.writeJSON(w, http.StatusOK, map[string]string{"message": "apply belongs to another PR"})
		return true
	}
	h.logger.Info("control command without -e names an apply this deployment stores; replying with its environment",
		append(apply.LogAttrs(), "action", result.Action)...)
	h.postComment(repo, pr, installationID, templates.RenderControlMissingEnv(result.Action, result.ApplyID, apply.Environment))
	h.writeJSON(w, http.StatusOK, map[string]string{"message": "missing environment flag"})
	return true
}
