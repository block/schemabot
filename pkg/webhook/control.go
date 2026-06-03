package webhook

import (
	"fmt"

	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// handleStopCommand handles the "schemabot stop <apply-id> -e <env>" PR
// comment command by recording durable stop intent for the scheduler owner.
func (h *Handler) handleStopCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(commandTimeout)
	defer cancel()

	if result.ApplyID == "" {
		h.postComment(repo, pr, installationID, templates.RenderControlMissingApplyID(action.Stop))
		return
	}
	if h.service == nil {
		h.logger.Error("service not configured for stop PR command",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, action.Stop, result.Environment, requestedBy, "SchemaBot service is not configured for stop commands")
		return
	}
	store := h.service.Storage()
	if store == nil {
		h.logger.Error("storage not configured for stop PR command",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, action.Stop, result.Environment, requestedBy, "SchemaBot storage is not configured for stop commands")
		return
	}
	applyStore := store.Applies()
	if applyStore == nil {
		h.logger.Error("apply store not configured for stop PR command",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, action.Stop, result.Environment, requestedBy, "SchemaBot apply storage is not configured for stop commands")
		return
	}
	apply, err := applyStore.GetByApplyIdentifier(ctx, result.ApplyID)
	if err != nil {
		h.logger.Error("failed to load apply for stop PR command",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy,
			"error", err)
		h.postCommandError(repo, pr, installationID, action.Stop, result.Environment, requestedBy, "Failed to look up apply: "+err.Error())
		return
	}
	if apply == nil {
		h.logger.Warn("stop PR command rejected because apply was not found",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, action.Stop, result.Environment, requestedBy, "Apply not found: "+result.ApplyID)
		return
	}
	if apply.Repository != repo || apply.PullRequest != pr {
		h.logger.Warn("stop PR command rejected because apply belongs to another PR",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"apply_repo", apply.Repository,
			"apply_pr", apply.PullRequest,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, action.Stop, result.Environment, requestedBy, "Apply "+result.ApplyID+" is not associated with this PR")
		return
	}
	if apply.Environment != result.Environment {
		h.logger.Warn("stop PR command rejected because environment does not match apply",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"apply_environment", apply.Environment,
			"requested_environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, action.Stop, result.Environment, requestedBy,
			fmt.Sprintf("Apply %s belongs to environment %q, not %q", result.ApplyID, apply.Environment, result.Environment))
		return
	}
	var client *ghclient.InstallationClient
	if h.service.Config().PRCommandAuthorizationEnabled() && h.ghClients.Len() > 0 {
		var clientErr error
		client, clientErr = h.clientForRepo(repo, installationID)
		if clientErr != nil {
			h.logger.Warn("stop PR command blocked because actor authorization client could not be created",
				"repo", repo,
				"pr", pr,
				"apply_id", result.ApplyID,
				"database", apply.Database,
				"database_type", apply.DatabaseType,
				"environment", result.Environment,
				"requested_by", requestedBy,
				"error", clientErr)
			h.postComment(repo, pr, installationID, templates.RenderPRCommandAuthorizationUnavailable(templates.ActorAuthorizationCommentData{
				RequestedBy: requestedBy,
				CommandName: action.Stop,
				Database:    apply.Database,
				Environment: result.Environment,
			}))
			return
		}
	}
	if blocked := h.enforcePRCommandActorAuthorization(ctx, client, repo, pr, installationID, requestedBy, apply.Database, apply.DatabaseType, result.Environment, action.Stop); blocked {
		return
	}

	caller := fmt.Sprintf("github:%s@%s#%d", requestedBy, repo, pr)
	resp, err := h.service.ExecuteStop(ctx, apitypes.ControlRequest{
		ApplyID:     result.ApplyID,
		Environment: result.Environment,
		Caller:      caller,
	})
	if err != nil {
		h.logger.Warn("stop PR command rejected",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy,
			"error", err)
		h.postCommandError(repo, pr, installationID, action.Stop, result.Environment, requestedBy, err.Error())
		return
	}
	if resp == nil {
		h.logger.Error("stop PR command returned no response",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, action.Stop, result.Environment, requestedBy, "SchemaBot accepted the stop command but returned no response")
		return
	}
	if !resp.Accepted {
		detail := resp.ErrorMessage
		if detail == "" {
			detail = "Stop was not accepted"
		}
		h.logger.Warn("stop PR command was not accepted",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy,
			"error_message", resp.ErrorMessage)
		h.postCommandError(repo, pr, installationID, action.Stop, result.Environment, requestedBy, detail)
		return
	}

	h.logger.Info("stop PR command accepted",
		"repo", repo,
		"pr", pr,
		"apply_id", result.ApplyID,
		"environment", result.Environment,
		"requested_by", requestedBy,
		"status", resp.Status,
		"stopped_count", resp.StoppedCount,
		"skipped_count", resp.SkippedCount)
	h.postComment(repo, pr, installationID, templates.RenderStopCommandAccepted(templates.StopCommandAcceptedData{
		ApplyID:      result.ApplyID,
		Environment:  result.Environment,
		RequestedBy:  requestedBy,
		Status:       resp.Status,
		StoppedCount: resp.StoppedCount,
		SkippedCount: resp.SkippedCount,
	}))
}

// handleCutoverCommand handles the "schemabot cutover <apply-id> -e <env>" PR
// comment command by recording durable cutover intent for the scheduler owner.
func (h *Handler) handleCutoverCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(commandTimeout)
	defer cancel()

	if result.ApplyID == "" {
		h.postComment(repo, pr, installationID, templates.RenderControlMissingApplyID(action.Cutover))
		return
	}
	if h.service == nil {
		h.logger.Error("service not configured for cutover PR command",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, action.Cutover, result.Environment, requestedBy, "SchemaBot service is not configured for cutover commands")
		return
	}
	store := h.service.Storage()
	if store == nil {
		h.logger.Error("storage not configured for cutover PR command",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, action.Cutover, result.Environment, requestedBy, "SchemaBot storage is not configured for cutover commands")
		return
	}
	applyStore := store.Applies()
	if applyStore == nil {
		h.logger.Error("apply store not configured for cutover PR command",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, action.Cutover, result.Environment, requestedBy, "SchemaBot apply storage is not configured for cutover commands")
		return
	}
	apply, err := applyStore.GetByApplyIdentifier(ctx, result.ApplyID)
	if err != nil {
		h.logger.Error("failed to load apply for cutover PR command",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy,
			"error", err)
		h.postCommandError(repo, pr, installationID, action.Cutover, result.Environment, requestedBy, "Failed to look up apply: "+err.Error())
		return
	}
	if apply == nil {
		h.logger.Warn("cutover PR command rejected because apply was not found",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, action.Cutover, result.Environment, requestedBy, "Apply not found: "+result.ApplyID)
		return
	}
	if apply.Repository != repo || apply.PullRequest != pr {
		h.logger.Warn("cutover PR command rejected because apply belongs to another PR",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"apply_repo", apply.Repository,
			"apply_pr", apply.PullRequest,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, action.Cutover, result.Environment, requestedBy, "Apply "+result.ApplyID+" is not associated with this PR")
		return
	}
	if apply.Environment != result.Environment {
		h.logger.Warn("cutover PR command rejected because environment does not match apply",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"apply_environment", apply.Environment,
			"requested_environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, action.Cutover, result.Environment, requestedBy,
			fmt.Sprintf("Apply %s belongs to environment %q, not %q", result.ApplyID, apply.Environment, result.Environment))
		return
	}
	var client *ghclient.InstallationClient
	if h.service.Config().PRCommandAuthorizationEnabled() && h.ghClients.Len() > 0 {
		var clientErr error
		client, clientErr = h.clientForRepo(repo, installationID)
		if clientErr != nil {
			h.logger.Warn("cutover PR command blocked because actor authorization client could not be created",
				"repo", repo,
				"pr", pr,
				"apply_id", result.ApplyID,
				"database", apply.Database,
				"database_type", apply.DatabaseType,
				"environment", result.Environment,
				"requested_by", requestedBy,
				"error", clientErr)
			h.postComment(repo, pr, installationID, templates.RenderPRCommandAuthorizationUnavailable(templates.ActorAuthorizationCommentData{
				RequestedBy: requestedBy,
				CommandName: action.Cutover,
				Database:    apply.Database,
				Environment: result.Environment,
			}))
			return
		}
	}
	if blocked := h.enforcePRCommandActorAuthorization(ctx, client, repo, pr, installationID, requestedBy, apply.Database, apply.DatabaseType, result.Environment, action.Cutover); blocked {
		return
	}

	caller := fmt.Sprintf("github:%s@%s#%d", requestedBy, repo, pr)
	resp, err := h.service.ExecuteCutover(ctx, apitypes.ControlRequest{
		ApplyID:     result.ApplyID,
		Environment: result.Environment,
		Caller:      caller,
	})
	if err != nil {
		h.logger.Warn("cutover PR command rejected",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy,
			"error", err)
		h.postCommandError(repo, pr, installationID, action.Cutover, result.Environment, requestedBy, err.Error())
		return
	}
	if resp == nil {
		h.logger.Error("cutover PR command returned no response",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, action.Cutover, result.Environment, requestedBy, "SchemaBot accepted the cutover command but returned no response")
		return
	}
	if !resp.Accepted {
		detail := resp.ErrorMessage
		if detail == "" {
			detail = "Cutover was not accepted"
		}
		h.logger.Warn("cutover PR command was not accepted",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy,
			"error_message", resp.ErrorMessage)
		h.postCommandError(repo, pr, installationID, action.Cutover, result.Environment, requestedBy, detail)
		return
	}

	h.logger.Info("cutover PR command accepted",
		"repo", repo,
		"pr", pr,
		"apply_id", result.ApplyID,
		"environment", result.Environment,
		"requested_by", requestedBy,
		"status", resp.Status)
	h.postComment(repo, pr, installationID, templates.RenderCutoverCommandAccepted(templates.CutoverCommandAcceptedData{
		ApplyID:     result.ApplyID,
		Environment: result.Environment,
		RequestedBy: requestedBy,
		Status:      resp.Status,
	}))
}
