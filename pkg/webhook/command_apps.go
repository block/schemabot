package webhook

import (
	"errors"
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/storage"
)

// errCommandAppIdentityUnknown means a comment's author is a listed command
// App, but this deployment cannot yet rule out that the comment is its own:
// the GitHub client for the repo, or this deployment's App slug, is not
// available. The comment is not admitted. A stored delivery is retried, since
// the identity usually resolves within seconds of startup.
var errCommandAppIdentityUnknown = errors.New("this deployment's own GitHub App identity is not known yet")

// commandAppAdmission decides whether a bot comment comes from an App listed
// in command_apps for a database that accepts repo, so it is parsed as a
// command instead of being ignored. It never admits this deployment's own
// App: when it cannot rule that out it admits nothing and returns an error
// wrapping errCommandAppIdentityUnknown, so the caller can tell a comment it
// must ignore from one it may retry.
func (h *Handler) commandAppAdmission(repo string, installationID int64, login string) (bool, error) {
	config, ok := h.serverConfig()
	if !ok {
		return false, nil
	}
	slug, ok := config.CommandAppSlug(repo, login)
	if !ok {
		return false, nil
	}
	client, err := h.clientForRepo(repo, installationID)
	if err != nil {
		return false, fmt.Errorf("command App %s on %s: GitHub client unavailable to rule out a self-comment: %w: %w",
			slug, repo, errCommandAppIdentityUnknown, err)
	}
	own := client.AppSlug()
	if own == "" {
		return false, fmt.Errorf("command App %s on %s: %w", slug, repo, errCommandAppIdentityUnknown)
	}
	if strings.EqualFold(own, slug) {
		h.logger.Warn("command App comment ignored because command_apps lists this deployment's own App",
			"repo", repo, "author", login, "command_app", slug)
		return false, nil
	}
	h.logger.Info("parsing comment from a command App", "repo", repo, "author", login, "command_app", slug)
	return true, nil
}

// isCommandAppComment is commandAppAdmission for a live webhook. A comment
// whose admission cannot be decided yet is ignored: it fails closed.
func (h *Handler) isCommandAppComment(repo string, installationID int64, login string) bool {
	admitted, err := h.commandAppAdmission(repo, installationID, login)
	if err != nil {
		h.logger.Warn("command App comment ignored because this deployment cannot yet rule out a self-comment",
			"repo", repo, "installation_id", installationID, "author", login, "error", err)
		return false
	}
	return admitted
}

// durableCommandAppAdmission is commandAppAdmission for a stored delivery. A
// delivery whose installation cannot be resolved is not admitted, so a bot
// comment is ignored rather than retried. A listed App whose comment cannot be
// decided yet returns the error, so the driver keeps the delivery pending and
// retries it.
func (h *Handler) durableCommandAppAdmission(event *storage.WebhookEvent, payload *webhookPayload) (bool, error) {
	installationID, err := durableInstallationID(event)
	if err != nil {
		h.logger.Warn("bot comment not checked against command_apps because its installation is unknown",
			"delivery_id", event.DeliveryID, "repo", event.Repository, "pr", event.PullRequest,
			"author", payload.Comment.User.Login, "error", err)
		return false, nil
	}
	return h.commandAppAdmission(payload.Repository.FullName, installationID, payload.Comment.User.Login)
}
