package webhook

import (
	"strings"

	"github.com/block/schemabot/pkg/storage"
)

// isCommandAppComment reports whether a bot comment comes from an App listed
// in command_apps for a database that accepts repo, so it is parsed as a
// command instead of being ignored. It never admits this deployment's own
// App: when the own App slug is not known yet it cannot rule that out, so it
// admits nothing and the comment is ignored like any other bot's.
func (h *Handler) isCommandAppComment(repo string, installationID int64, login string) bool {
	config, ok := h.serverConfig()
	if !ok {
		return false
	}
	slug, ok := config.CommandAppSlug(repo, login)
	if !ok {
		return false
	}
	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		h.logger.Warn("command App comment ignored because the GitHub client for the repo is unavailable to rule out a self-comment",
			"repo", repo, "author", login, "command_app", slug, "error", err)
		return false
	}
	own := client.AppSlug()
	if own == "" {
		h.logger.Warn("command App comment ignored because this deployment's own App slug is not known yet",
			"repo", repo, "author", login, "command_app", slug)
		return false
	}
	if strings.EqualFold(own, slug) {
		h.logger.Warn("command App comment ignored because command_apps lists this deployment's own App",
			"repo", repo, "author", login, "command_app", slug)
		return false
	}
	h.logger.Info("parsing comment from a command App", "repo", repo, "author", login, "command_app", slug)
	return true
}

// isDurableCommandAppComment is isCommandAppComment for a stored delivery. A
// delivery whose installation cannot be resolved is not admitted, so a bot
// comment is still ignored rather than retried.
func (h *Handler) isDurableCommandAppComment(event *storage.WebhookEvent, payload *webhookPayload) bool {
	installationID, err := durableInstallationID(event)
	if err != nil {
		h.logger.Warn("bot comment not checked against command_apps because its installation is unknown",
			"delivery_id", event.DeliveryID, "repo", event.Repository, "pr", event.PullRequest,
			"author", payload.Comment.User.Login, "error", err)
		return false
	}
	return h.isCommandAppComment(payload.Repository.FullName, installationID, payload.Comment.User.Login)
}
