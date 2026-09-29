package webhook

import (
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// answersCommandsOnUnregisteredRepos reports whether this deployment replies
// to commands on a repository missing from its repos allowlist. A deployment's
// GitHub App can be installed on repositories it has not registered, for
// example one served only by a sibling deployment for another environment.
// The deployment that answers unscoped commands (respond_to_unscoped) is the
// one that replies there, since its silenced siblings never will. A tenant
// deployment is excluded: it serves the repositories configured for its
// tenant and nothing else, and its siblings answer everything outside them.
// So is a -t command, which names a deployment that owns its reply.
func (h *Handler) answersCommandsOnUnregisteredRepos(result CommandResult) bool {
	if h.service == nil {
		return false
	}
	config := h.service.Config()
	if config.Tenant != "" {
		return false
	}
	if !config.ShouldRespondToUnscoped() {
		return false
	}
	return result.Tenant == "" && !result.TenantError
}

// replyOnUnregisteredRepo answers a command on a repository this deployment
// has not registered, when the answer is one no sibling deployment would
// give: help, an unrecognized command, a missing, malformed, or unknown -e,
// and any command for an environment this deployment serves. It never plans,
// applies, or reads the repository. A command for an environment another
// deployment serves, and a work command with no environment, stay with the
// deployments that registered the repository. Returns the handler response
// message and whether a reply was posted.
func (h *Handler) replyOnUnregisteredRepo(repo string, pr int, installationID int64, deliveryID string, result CommandResult) (string, bool) {
	if !h.answersCommandsOnUnregisteredRepos(result) {
		h.logger.Debug("not replying on unregistered repository; this deployment does not answer unscoped commands",
			"repo", repo, "pr", pr, "tenant", result.Tenant, "action", result.Action)
		return "", false
	}
	if installationID == 0 {
		h.logger.Warn("not replying on unregistered repository; webhook payload has no installation ID",
			"repo", repo, "pr", pr, "action", result.Action)
		return "", false
	}
	config := h.service.Config()

	switch {
	case result.IsHelp:
		h.logUnregisteredRepoReply(repo, pr, result, "help")
		h.postComment(repo, pr, installationID, templates.RenderHelpComment())
		return "help posted", true

	case result.EnvironmentError:
		h.logUnregisteredRepoReply(repo, pr, result, "invalid environment value")
		h.acknowledgeCommand(repo, pr, installationID, deliveryID, result.CommentID)
		h.postComment(repo, pr, installationID, templates.RenderInvalidEnv(result.Action, h.knownEnvironments()))
		return "invalid environment value", true

	case result.MissingEnv && result.Action == action.Plan:
		h.logger.Info("not replying on unregistered repository; plan without -e runs on the deployments that registered it",
			"repo", repo, "pr", pr, "action", result.Action)
		return "", false

	case result.MissingEnv:
		comment, message := missingEnvironmentReply(result)
		h.logUnregisteredRepoReply(repo, pr, result, message)
		h.postComment(repo, pr, installationID, comment)
		return message, true

	case !result.Found:
		h.logUnregisteredRepoReply(repo, pr, result, "invalid command")
		h.postComment(repo, pr, installationID, templates.RenderInvalidCommand())
		return "invalid command", true

	case result.Environment == "":
		h.logger.Info("not replying on unregistered repository; the command names no environment, so the deployments that registered it answer",
			"repo", repo, "pr", pr, "action", result.Action)
		return "", false

	case !config.IsEnvironmentAllowed(result.Environment) && !config.IsEnvironmentKnown(result.Environment):
		h.logUnregisteredRepoReply(repo, pr, result, "unknown environment")
		h.acknowledgeCommand(repo, pr, installationID, deliveryID, result.CommentID)
		h.postComment(repo, pr, installationID, templates.RenderInvalidEnv(result.Action, h.knownEnvironments()))
		return "unknown environment", true

	case !config.IsEnvironmentAllowed(result.Environment):
		h.logger.Info("not replying on unregistered repository; another deployment serves the environment",
			"repo", repo, "pr", pr, "environment", result.Environment, "action", result.Action)
		return "", false

	default:
		h.logUnregisteredRepoReply(repo, pr, result, "repository not registered for environment")
		h.acknowledgeCommand(repo, pr, installationID, deliveryID, result.CommentID)
		h.postComment(repo, pr, installationID, templates.RenderRepositoryNotRegistered(result.Environment))
		return "repository not registered for environment", true
	}
}

func (h *Handler) logUnregisteredRepoReply(repo string, pr int, result CommandResult, reply string) {
	h.logger.Info("replying on unregistered repository; no registered deployment answers this command",
		"repo", repo, "pr", pr, "environment", result.Environment, "action", result.Action, "reply", reply)
}

// missingEnvironmentReply returns the usage comment for a command other than
// plan that omitted -e, and the handler response message that goes with it.
func missingEnvironmentReply(result CommandResult) (comment, message string) {
	switch result.Action {
	case action.Rollback:
		if result.ApplyID == "" {
			return templates.RenderRollbackMissingArguments(), "missing rollback arguments"
		}
		return templates.RenderRollbackMissingEnv(), "missing environment flag"
	case action.RollbackConfirm:
		return templates.RenderRollbackMissingEnv(), "missing environment flag"
	default:
		return templates.RenderMissingEnv(result.Action), "missing environment flag"
	}
}
