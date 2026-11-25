package webhook

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// handleFreezeApproveCommand handles "schemabot freeze approve <ovr-id>": a
// freeze approver approves, on the pull request, the override request that was
// asked for there. The decision is api.Service.ApprovePullRequestOverride;
// this supplies the one fact only the webhook can read, whether the commenter
// is in a team of freeze.github_teams.
//
// Dispatch needs no more authorization than the delivery's signature: the
// approvers are their own list, not the PR command admins and operators, and
// the command touches no schema, plan or check.
func (h *Handler) handleFreezeApproveCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(context.Background(), commandTimeout)
	defer cancel()

	if h.service == nil {
		h.logger.Error("service not configured for the freeze approve PR command", "repo", repo, "pr", pr, "requested_by", requestedBy)
		h.postComment(repo, pr, installationID, templates.RenderFreezeOverrideApprovalFailed("SchemaBot is not configured to approve overrides."))
		return
	}
	teams := h.service.Config().Freeze.GitHubTeams
	approved, err := h.service.ApprovePullRequestOverride(ctx, api.PullRequestApproval{
		Login: requestedBy, Repository: repo, PullRequest: pr, ID: result.OverrideID,
		IsApprover: func(ctx context.Context) (bool, error) {
			// An App never approves, and with no team nobody does.
			if isAppLogin(requestedBy) || len(teams) == 0 {
				return false, nil
			}
			client, err := h.clientForRepo(repo, installationID)
			if err != nil {
				return false, err
			}
			member, _, err := actorInAnyTeam(ctx, client, teams, requestedBy)
			return member, err
		},
	})
	if err != nil {
		h.answerFreezeApproveFailure(repo, pr, installationID, requestedBy, result, teams, err)
		return
	}
	h.acknowledgeCommandActPoint(repo, pr, installationID, result)
	h.postComment(repo, pr, installationID, templates.RenderFreezeOverrideApproved(templates.FreezeOverrideApprovedData{
		ID:          approved.Identifier,
		Approver:    requestedBy,
		Action:      approved.Action,
		Database:    approved.Database,
		Environment: approved.Environment,
		RequestedBy: strings.TrimPrefix(approved.RequestedBy, storage.GitHubIdentityPrefix),
		Reason:      approved.Reason,
		ExpiresAt:   approved.ExpiresAt,
	}))
}

// answerFreezeApproveFailure replies to an approval that approved nothing. On
// an aggregate repo an unscoped command reaches every deployment, and the
// request is in one deployment's storage, so the others stay silent.
func (h *Handler) answerFreezeApproveFailure(repo string, pr int, installationID int64, requestedBy string, result CommandResult, teams []string, err error) {
	switch {
	case errors.Is(err, storage.ErrFreezeOverrideNotFound) && h.silentOnUnscopedFanOut(repo, result.Tenant):
		h.logger.Info("unscoped fan-out freeze approve names a request not stored on this deployment; staying silent so the owning deployment responds",
			"repo", repo, "pr", pr, "override_request", result.OverrideID, "requested_by", requestedBy)
		return
	case errors.Is(err, api.ErrInvalidOverrideRequestID) && h.silentUsageErrorOnUnscopedFanOut(repo, result.Tenant):
		h.logger.Info("skipping malformed override request ID reply for unscoped fan-out; the leader posts it once",
			"repo", repo, "pr", pr, "requested_by", requestedBy)
		return
	}
	h.acknowledgeCommandActPoint(repo, pr, installationID, result)

	var body string
	switch refused, isRefusal := errors.AsType[*api.PullRequestApprovalRefusedError](err); {
	case errors.Is(err, api.ErrNotPullRequestApprover):
		h.logger.Warn("freeze approve PR command refused: not an approver",
			"repo", repo, "pr", pr, "override_request", result.OverrideID, "requested_by", requestedBy)
		body = templates.RenderFreezeOverrideApprovalRefused(notApproverReason(requestedBy, teams))
	case isRefusal:
		h.logger.Warn("freeze approve PR command refused",
			"repo", repo, "pr", pr, "override_request", result.OverrideID, "requested_by", requestedBy, "reason", refused.Message)
		body = templates.RenderFreezeOverrideApprovalRefused(refused.Message)
	case errors.Is(err, api.ErrPullRequestApproverUnchecked):
		h.logger.Warn("freeze approve PR command stopped: approver teams could not be read",
			"repo", repo, "pr", pr, "override_request", result.OverrideID, "requested_by", requestedBy, "error", err)
		body = templates.RenderFreezeOverrideApprovalFailed(fmt.Sprintf("SchemaBot could not check whether @%s is a freeze approver.", requestedBy))
	default:
		h.logger.Error("freeze approve PR command failed",
			"repo", repo, "pr", pr, "override_request", result.OverrideID, "requested_by", requestedBy, "error", err)
		body = templates.RenderFreezeOverrideApprovalFailed("SchemaBot could not approve the override request.")
	}
	h.postComment(repo, pr, installationID, body)
}

// notApproverReason says why the commenter cannot approve, and whom to ask.
func notApproverReason(login string, teams []string) string {
	switch {
	case isAppLogin(login):
		return "An App cannot approve an override."
	case len(teams) == 0:
		return "Approval from a pull request is not configured: no GitHub team is listed under `freeze.github_teams`."
	default:
		return fmt.Sprintf("@%s is not a member of a team that may approve an override from a pull request (%s).", login, "`"+strings.Join(teams, "`, `")+"`")
	}
}
