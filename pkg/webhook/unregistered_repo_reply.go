package webhook

import (
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// answersCommandsOnUnregisteredRepos reports whether this deployment replies
// to commands on a repository missing from its repos allowlist. A deployment's
// GitHub App can be installed on repositories it has not registered, for
// example one served only by a sibling deployment for another environment.
// Only a deployment an operator named as the fleet's unscoped responder, by
// setting respond_to_unscoped to true, replies there: leaving the flag unset
// keeps a deployment within its own repositories, so a fleet whose
// deployments split repositories by their allowlists gets no reply from a
// deployment that does not own the repository. A tenant deployment is
// excluded: it serves the repositories configured for its tenant and nothing
// else. So is a -t command, which names a deployment that owns its reply.
func (h *Handler) answersCommandsOnUnregisteredRepos(result CommandResult) bool {
	if h.service == nil {
		return false
	}
	config := h.service.Config()
	if config.Tenant != "" {
		return false
	}
	if !config.IsExplicitUnscopedResponder() {
		return false
	}
	return result.Tenant == "" && !result.TenantError
}

// replyOnUnregisteredRepo answers a command on a repository this deployment
// has not registered, when the answer is a fact about the comment that holds
// whichever deployment registered the repository: help, an unrecognized
// command, and a missing, malformed, or unknown -e. It never plans, applies,
// or reads the repository, and it never claims the repository is unserved,
// because this deployment cannot see which repositories its siblings
// registered. Every well-formed command naming a real environment is left to
// the deployments that registered the repository.
//
// The aggregate fan-out deferrals the registered path applies to usage errors
// (silentUsageErrorOnUnscopedFanOut, silentUnknownEnvOnAggregateFanOut) do not
// apply here: they read the repository's aggregate role from this
// deployment's configuration, and an unregistered repository has none.
//
// Returns the handler response message and whether a reply was posted.
func (h *Handler) replyOnUnregisteredRepo(repo string, pr int, installationID int64, deliveryID string, result CommandResult) (string, bool) {
	if !h.answersCommandsOnUnregisteredRepos(result) {
		h.logger.Debug("not replying on unregistered repository; this deployment is not the explicit unscoped responder",
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

	case !config.IsEnvironmentKnown(result.Environment):
		// Known, not allowed: an empty allowed_environments allows every
		// value, typos included, and this deployment serves no repository
		// here for that to mean anything.
		h.logUnregisteredRepoReply(repo, pr, result, "unknown environment")
		h.acknowledgeCommand(repo, pr, installationID, deliveryID, result.CommentID)
		h.postComment(repo, pr, installationID, templates.RenderInvalidEnv(result.Action, h.knownEnvironments()))
		return "unknown environment", true

	default:
		h.logger.Info("not replying on unregistered repository; the command names a known environment, so the deployments that registered the repository answer, and none will if no deployment registered it",
			"repo", repo, "pr", pr, "environment", result.Environment, "action", result.Action)
		return "", false
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
