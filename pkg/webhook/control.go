package webhook

import (
	"context"
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// logControlCommandError logs a failed PR control command at the severity
// matching the error class: internal failures (storage, control request store,
// Tern, or other unexpected errors) log at Error so operators investigate
// them, while operator-actionable rejections such as state conflicts log at
// Warn.
func (h *Handler) logControlCommandError(command, repo string, pr int, applyID, environment, requestedBy string, err error) {
	attrs := []any{
		"repo", repo,
		"pr", pr,
		"apply_id", applyID,
		"environment", environment,
		"requested_by", requestedBy,
		"error", err,
	}
	if api.IsInternalControlError(err) {
		h.logger.Error(command+" PR command failed", attrs...)
	} else {
		h.logger.Warn(command+" PR command rejected", attrs...)
	}
}

func (h *Handler) loadApplyForPRControl(ctx context.Context, repo string, pr int, installationID int64, requestedBy string, result CommandResult, command string) (*storage.Apply, bool) {
	if result.ApplyID == "" {
		if h.silentUsageErrorOnUnscopedFanOut(repo, result.Tenant) {
			h.logger.Info("skipping missing-apply-id reply for unscoped fan-out control command; the leader posts it once",
				"command", command,
				"repo", repo,
				"pr", pr,
				"environment", result.Environment,
				"requested_by", requestedBy)
			return nil, false
		}
		h.postComment(repo, pr, installationID, templates.RenderControlMissingApplyID(h.cliName(), command, result.Environment))
		return nil, false
	}
	if h.service == nil {
		h.logger.Error("service not configured for PR control command",
			"command", command,
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, command, result.Environment, requestedBy, "SchemaBot service is not configured for "+command+" commands")
		return nil, false
	}
	store := h.service.Storage()
	if store == nil {
		h.logger.Error("storage not configured for PR control command",
			"command", command,
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, command, result.Environment, requestedBy, "SchemaBot storage is not configured for "+command+" commands")
		return nil, false
	}
	applyStore := store.Applies()
	if applyStore == nil {
		h.logger.Error("apply store not configured for PR control command",
			"command", command,
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, command, result.Environment, requestedBy, "SchemaBot apply storage is not configured for "+command+" commands")
		return nil, false
	}
	apply, err := applyStore.GetByApplyIdentifier(ctx, result.ApplyID)
	if err != nil {
		h.logger.Error("failed to load apply for PR control command",
			"command", command,
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy,
			"error", err)
		h.postCommandError(repo, pr, installationID, command, result.Environment, requestedBy, "Failed to look up apply: "+err.Error())
		return nil, false
	}
	if apply == nil {
		// On an aggregate repo an unscoped control command fans out to every
		// deployment, but the apply lives in exactly one tenant's storage. A
		// deployment that doesn't have it is not the owner and stays silent so
		// only the owning deployment answers.
		if h.silentOnUnscopedFanOut(repo, result.Tenant) {
			h.logger.Info("unscoped fan-out control command targets an apply not stored on this deployment; staying silent so the owning deployment responds",
				"command", command,
				"repo", repo,
				"pr", pr,
				"apply_id", result.ApplyID,
				"environment", result.Environment,
				"requested_by", requestedBy)
			return nil, false
		}
		h.logger.Warn("PR control command rejected because apply was not found",
			"command", command,
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, command, result.Environment, requestedBy, "Apply not found: "+result.ApplyID)
		return nil, false
	}
	if apply.Repository != repo || apply.PullRequest != pr {
		h.logger.Warn("PR control command rejected because apply belongs to another PR",
			"command", command,
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"apply_repo", apply.Repository,
			"apply_pr", apply.PullRequest,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, command, result.Environment, requestedBy, "Apply "+result.ApplyID+" is not associated with this PR")
		return nil, false
	}
	if apply.Environment != result.Environment {
		h.logger.Warn("PR control command rejected because environment does not match apply",
			"command", command,
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"apply_environment", apply.Environment,
			"requested_environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, command, result.Environment, requestedBy,
			fmt.Sprintf("Apply %s belongs to environment %q, not %q", result.ApplyID, apply.Environment, result.Environment))
		return nil, false
	}
	h.acknowledgeCommandActPoint(repo, pr, installationID, result)
	return apply, true
}

// runControlCommand runs the shared lifecycle of a PR control command and
// returns the typed response only when the operation was accepted. It loads and
// authorizes the apply, executes the operation, and converts any failure
// (error, missing response, or rejection) into a PR comment. On a non-nil
// return the caller renders the operation-specific acceptance comment.
func runControlCommand[R any](
	h *Handler,
	ctx context.Context,
	repo string, pr int, installationID int64, requestedBy string,
	result CommandResult,
	actionName string,
	execute func(ctx context.Context, req apitypes.ControlRequest) (*R, error),
	accepted func(*R) bool,
	errorMessage func(*R) string,
) *R {
	apply, ok := h.loadApplyForPRControl(ctx, repo, pr, installationID, requestedBy, result, actionName)
	if !ok {
		return nil
	}
	client, err := h.actorAuthorizationClient(repo, pr, installationID, requestedBy, apply.Database, result.Environment, actionName)
	if err != nil {
		// Control commands are not durable cores, so there is no driver to
		// classify the cause; the gate has already logged the failure and
		// posted the authorization-unavailable comment, and the command
		// stops here (fail closed).
		return nil
	}
	// Control commands are not durable cores, so an authorization evaluation
	// failure and a merit denial both stop the command here; the gate has
	// already logged and posted the distinction. Pass a literal false: no
	// driver retries or posts a terminal answer for a control command, so the
	// gate's comment is the user's only answer and must never be suppressed.
	blocked, authErr := h.enforcePRCommandActorAuthorization(ctx, client, repo, pr, installationID, requestedBy, apply.Database, apply.DatabaseType, result.Environment, actionName, false)
	if authErr != nil || blocked {
		return nil
	}

	caller := formatGitHubCaller(requestedBy, repo, pr)
	resp, err := execute(ctx, apitypes.ControlRequest{
		ApplyID:     result.ApplyID,
		Environment: result.Environment,
		Caller:      caller,
	})
	if err != nil {
		h.logControlCommandError(actionName, repo, pr, result.ApplyID, result.Environment, requestedBy, err)
		h.postCommandError(repo, pr, installationID, actionName, result.Environment, requestedBy, err.Error())
		return nil
	}
	if resp == nil {
		h.logger.Error(actionName+" PR command returned no response",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy)
		h.postCommandError(repo, pr, installationID, actionName, result.Environment, requestedBy,
			fmt.Sprintf("SchemaBot accepted the %s command but returned no response", actionName))
		return nil
	}
	if !accepted(resp) {
		detail := errorMessage(resp)
		if detail == "" {
			detail = strings.ToUpper(actionName[:1]) + actionName[1:] + " was not accepted"
		}
		h.logger.Warn(actionName+" PR command was not accepted",
			"repo", repo,
			"pr", pr,
			"apply_id", result.ApplyID,
			"environment", result.Environment,
			"requested_by", requestedBy,
			"error_message", errorMessage(resp))
		h.postCommandError(repo, pr, installationID, actionName, result.Environment, requestedBy, detail)
		return nil
	}
	return resp
}

// handleStopCommand handles the "schemabot stop <apply-id> -e <env>" PR
// comment command by recording durable stop intent for the operator owner.
func (h *Handler) handleStopCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(context.Background(), commandTimeout)
	defer cancel()

	resp := runControlCommand(h, ctx, repo, pr, installationID, requestedBy, result, action.Stop,
		h.service.ExecuteStop,
		func(r *apitypes.StopResponse) bool { return r.Accepted },
		func(r *apitypes.StopResponse) string { return r.ErrorMessage })
	if resp == nil {
		return
	}

	h.logger.Info("stop PR command accepted",
		"repo", repo,
		"pr", pr,
		"apply_id", result.ApplyID,
		"environment", result.Environment,
		"requested_by", requestedBy,
		"apply_status", resp.Status,
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

// handleCancelCommand handles the "schemabot cancel <apply-id> -e <env>" PR
// comment command by recording durable cancel intent for the operator owner.
func (h *Handler) handleCancelCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(context.Background(), commandTimeout)
	defer cancel()

	resp := runControlCommand(h, ctx, repo, pr, installationID, requestedBy, result, action.Cancel,
		h.service.ExecuteCancel,
		func(r *apitypes.CancelResponse) bool { return r.Accepted },
		func(r *apitypes.CancelResponse) string { return r.ErrorMessage })
	if resp == nil {
		return
	}

	h.logger.Info("cancel PR command accepted",
		"repo", repo,
		"pr", pr,
		"apply_id", result.ApplyID,
		"environment", result.Environment,
		"requested_by", requestedBy,
		"apply_status", resp.Status,
		"cancelled_count", resp.CancelledCount,
		"skipped_count", resp.SkippedCount)
	h.postComment(repo, pr, installationID, templates.RenderCancelCommandAccepted(templates.CancelCommandAcceptedData{
		ApplyID:        result.ApplyID,
		Environment:    result.Environment,
		RequestedBy:    requestedBy,
		Status:         resp.Status,
		CancelledCount: resp.CancelledCount,
		SkippedCount:   resp.SkippedCount,
	}))
}

// handleStartCommand handles the "schemabot start <apply-id> -e <env>" PR
// comment command by recording durable start intent for the operator owner.
func (h *Handler) handleStartCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(context.Background(), commandTimeout)
	defer cancel()

	resp := runControlCommand(h, ctx, repo, pr, installationID, requestedBy, result, action.Start,
		h.service.ExecuteStart,
		func(r *apitypes.StartResponse) bool { return r.Accepted },
		func(r *apitypes.StartResponse) string { return r.ErrorMessage })
	if resp == nil {
		return
	}

	h.logger.Info("start PR command accepted",
		"repo", repo,
		"pr", pr,
		"apply_id", result.ApplyID,
		"environment", result.Environment,
		"requested_by", requestedBy,
		"apply_status", resp.Status,
		"started_count", resp.StartedCount,
		"skipped_count", resp.SkippedCount)
	h.postComment(repo, pr, installationID, templates.RenderStartCommandAccepted(templates.StartCommandAcceptedData{
		ApplyID:      result.ApplyID,
		Environment:  result.Environment,
		RequestedBy:  requestedBy,
		Status:       resp.Status,
		StartedCount: resp.StartedCount,
		SkippedCount: resp.SkippedCount,
	}))
	h.recordStartAcknowledged(ctx, repo, pr, installationID, result.ApplyID, result.Environment, formatGitHubCaller(requestedBy, repo, pr))
}

// recordStartAcknowledged records on the start request that its
// acknowledgement has landed on the PR, and posts the withdrawal notice if a
// stop from outside the PR withdrew the start first. That stop holds its notice
// back until the acknowledgement is recorded, so the PR ends on the withdrawal
// rather than on an acknowledgement promising a resume. Both sides decide under
// the start request's row lock, so the notice is posted once, by whichever
// side finds both facts recorded.
func (h *Handler) recordStartAcknowledged(ctx context.Context, repo string, pr int, installationID int64, applyID, environment, caller string) {
	attrs := []any{"repo", repo, "pr", pr, "apply_id", applyID, "environment", environment}
	stor := h.service.Storage()
	apply, err := stor.Applies().GetByApplyIdentifier(ctx, applyID)
	if err != nil {
		h.logger.Warn("failed to load apply to record its start acknowledgement; a stop that withdrew the start before the acknowledgement landed is not reported on the PR",
			append(attrs, "error", err)...)
		return
	}
	if apply == nil {
		h.logger.Warn("apply not found when recording its start acknowledgement; a stop that withdrew the start before the acknowledgement landed is not reported on the PR", attrs...)
		return
	}
	update, err := updateStartNotice(ctx, stor.ControlRequests(), apply.ID, func(start *storage.ApplyControlRequest, notice *startNotice) bool {
		if start.RequestedBy != caller {
			return false
		}
		notice.Acknowledged = true
		return true
	})
	switch {
	case err != nil:
		h.logger.Warn("failed to record start acknowledgement; a stop that withdrew the start before the acknowledgement landed is not reported on the PR",
			append(attrs, "error", err)...)
	case !update.found:
		h.logger.Warn("start request not found when recording its acknowledgement; a stop that withdrew the start before the acknowledgement landed is not reported on the PR", attrs...)
	case !update.applied:
		h.logger.Info("start request was re-armed by a start from another caller before its acknowledgement was recorded; that start's own acknowledgement owns the PR notice", attrs...)
	case update.claimed:
		h.postStartWithdrawnAfterAcknowledgement(ctx, repo, pr, installationID, apply, attrs)
	case update.notice.Notified:
		h.logger.Debug("start acknowledged; the withdrawal notice was already posted", attrs...)
	default:
		h.logger.Debug("start acknowledged; no stop has withdrawn it, and a stop that does posts its own notice", attrs...)
	}
}

// postStartWithdrawnAfterAcknowledgement posts the withdrawal notice a stop
// held back until the start's acknowledgement landed, naming the stopper when
// the stop request records a GitHub user.
func (h *Handler) postStartWithdrawnAfterAcknowledgement(ctx context.Context, repo string, pr int, installationID int64, apply *storage.Apply, attrs []any) {
	stoppedBy := ""
	stop, err := h.service.Storage().ControlRequests().GetByOperation(ctx, apply.ID, storage.ControlOperationStop)
	switch {
	case err != nil:
		h.logger.Warn("failed to read the stop that withdrew an acknowledged start; telling the PR without naming who stopped it",
			append(attrs, "error", err)...)
	case stop == nil:
		h.logger.Warn("no stop request found for a withdrawn start; telling the PR without naming who stopped it", attrs...)
	default:
		stoppedBy = actorFromCaller(stop.RequestedBy)
	}
	h.logger.Info("start was withdrawn by a stop before its acknowledgement landed; telling the PR after the acknowledgement", attrs...)
	h.postComment(repo, pr, installationID, templates.RenderStartWithdrawn(templates.StartWithdrawnData{
		ApplyID:     apply.ApplyIdentifier,
		Environment: apply.Environment,
		StoppedBy:   stoppedBy,
	}))
}

// handleReleaseCommand handles the "schemabot release <apply-id> -e <env>" PR
// comment command by recording a durable release latch so the operator lets a
// rollout paused after an on_failure=pause failure proceed.
func (h *Handler) handleReleaseCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(context.Background(), commandTimeout)
	defer cancel()

	resp := runControlCommand(h, ctx, repo, pr, installationID, requestedBy, result, action.Release,
		h.service.ExecuteRelease,
		func(r *apitypes.ReleaseResponse) bool { return r.Accepted },
		func(r *apitypes.ReleaseResponse) string { return r.ErrorMessage })
	if resp == nil {
		return
	}

	h.logger.Info("release PR command accepted",
		"repo", repo,
		"pr", pr,
		"apply_id", result.ApplyID,
		"environment", result.Environment,
		"requested_by", requestedBy,
		"apply_status", resp.Status)
	h.postComment(repo, pr, installationID, templates.RenderReleaseCommandAccepted(templates.ReleaseCommandAcceptedData{
		ApplyID:     result.ApplyID,
		Environment: result.Environment,
		RequestedBy: requestedBy,
		Status:      resp.Status,
	}))
}

// handleCutoverCommand handles the "schemabot cutover <apply-id> -e <env>" PR
// comment command by recording durable cutover intent for the operator owner.
func (h *Handler) handleCutoverCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(context.Background(), commandTimeout)
	defer cancel()

	resp := runControlCommand(h, ctx, repo, pr, installationID, requestedBy, result, action.Cutover,
		h.service.ExecuteCutover,
		func(r *apitypes.ControlResponse) bool { return r.Accepted },
		func(r *apitypes.ControlResponse) string { return r.ErrorMessage })
	if resp == nil {
		return
	}

	h.logger.Info("cutover PR command accepted",
		"repo", repo,
		"pr", pr,
		"apply_id", result.ApplyID,
		"environment", result.Environment,
		"requested_by", requestedBy,
		"apply_status", resp.Status)
	h.postComment(repo, pr, installationID, templates.RenderCutoverCommandAccepted(templates.CutoverCommandAcceptedData{
		ApplyID:     result.ApplyID,
		Environment: result.Environment,
		RequestedBy: requestedBy,
		Status:      resp.Status,
	}))
}

func (h *Handler) handleSkipRevertCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(context.Background(), commandTimeout)
	defer cancel()

	resp := runControlCommand(h, ctx, repo, pr, installationID, requestedBy, result, action.SkipRevert,
		h.service.ExecuteSkipRevert,
		func(r *apitypes.ControlResponse) bool { return r.Accepted },
		func(r *apitypes.ControlResponse) string { return r.ErrorMessage })
	if resp == nil {
		return
	}

	h.logger.Info("skip-revert PR command accepted",
		"repo", repo,
		"pr", pr,
		"apply_id", result.ApplyID,
		"environment", result.Environment,
		"requested_by", requestedBy)
	h.postComment(repo, pr, installationID, templates.RenderSkipRevertCommandAccepted(templates.SkipRevertCommandAcceptedData{
		ApplyID:     result.ApplyID,
		Environment: result.Environment,
		RequestedBy: requestedBy,
	}))
}

func (h *Handler) handleRevertCommand(repo string, pr int, installationID int64, requestedBy string, result CommandResult) {
	ctx, cancel := h.commandContext(context.Background(), commandTimeout)
	defer cancel()

	resp := runControlCommand(h, ctx, repo, pr, installationID, requestedBy, result, action.Revert,
		h.service.ExecuteRevert,
		func(r *apitypes.ControlResponse) bool { return r.Accepted },
		func(r *apitypes.ControlResponse) string { return r.ErrorMessage })
	if resp == nil {
		return
	}

	h.logger.Info("revert PR command accepted",
		"repo", repo,
		"pr", pr,
		"apply_id", result.ApplyID,
		"environment", result.Environment,
		"requested_by", requestedBy)
	h.postComment(repo, pr, installationID, templates.RenderRevertCommandAccepted(templates.RevertCommandAcceptedData{
		ApplyID:     result.ApplyID,
		Environment: result.Environment,
		RequestedBy: requestedBy,
	}))
}

// reportQueuedStartWithdrawn tells an apply's PR that a stop withdrew the start
// queued behind it. The apply never left stopped, so no drive runs to update
// the PR, which would otherwise end on the start's acknowledgement promising a
// resume. A stop that was itself a command on this PR is skipped: its reply
// already reports the withdrawal. When the start was a command on this PR whose
// acknowledgement has not landed yet, the notice is left to the start command,
// which posts it after the acknowledgement (see recordStartAcknowledged). The
// work runs off the caller's goroutine, since the stop's response waits on this.
func (h *Handler) reportQueuedStartWithdrawn(apply *storage.Apply, caller string) {
	if apply.Repository == "" || apply.PullRequest == 0 || apply.InstallationID == 0 {
		h.logger.Debug("queued start withdrawn; no PR to tell: apply has no GitHub destination",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment)
		return
	}
	if callerIsPRCommand(caller, apply.Repository, apply.PullRequest) {
		h.logger.Debug("queued start withdrawn by a stop command on the PR; its reply reports the withdrawal",
			"apply_id", apply.ApplyIdentifier,
			"repo", apply.Repository,
			"pr", apply.PullRequest)
		return
	}
	repo, pr, installationID := apply.Repository, apply.PullRequest, apply.InstallationID
	attrs := []any{"apply_id", apply.ApplyIdentifier, "repo", repo, "pr", pr, "environment", apply.Environment}
	body := templates.RenderStartWithdrawn(templates.StartWithdrawnData{
		ApplyID:     apply.ApplyIdentifier,
		Environment: apply.Environment,
		StoppedBy:   actorFromCaller(caller),
	})
	h.goSafe(repo, pr, installationID, "", func() {
		ctx, cancel := h.commandContext(context.Background(), commandTimeout)
		defer cancel()
		update, err := updateStartNotice(ctx, h.service.Storage().ControlRequests(), apply.ID, func(start *storage.ApplyControlRequest, notice *startNotice) bool {
			if start.Status != storage.ControlRequestCompleted {
				return false
			}
			notice.Withdrawn = true
			if !callerIsPRCommand(start.RequestedBy, repo, pr) {
				notice.Acknowledged = true
			}
			return true
		})
		switch {
		case err != nil:
			h.logger.Warn("failed to record the withdrawal of a queued start; the PR is not told the start was withdrawn",
				append(attrs, "error", err)...)
		case !update.found:
			h.logger.Warn("start request not found when recording its withdrawal; the PR is not told the start was withdrawn", attrs...)
		case !update.applied:
			h.logger.Info("a later start re-armed the withdrawn start request; that start's acknowledgement updates the PR", attrs...)
		case update.claimed:
			h.logger.Info("queued start withdrawn by a stop from outside the PR; telling the PR", attrs...)
			h.postComment(repo, pr, installationID, body)
		case !update.notice.Acknowledged:
			h.logger.Info("queued start withdrawn before its acknowledgement landed on the PR; the start command posts the notice after it", attrs...)
		default:
			h.logger.Debug("queued start withdrawn; the withdrawal notice was already posted", attrs...)
		}
	})
}
