package webhook

import (
	"context"
	"time"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// handleCutoverCommand handles the "schemabot cutover <apply-id> -e <env>" PR comment command.
func (h *Handler) handleCutoverCommand(repo string, pr int, applyID, environment string, installationID int64, requestedBy string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if h.service == nil {
		h.logger.Error("service not configured for cutover")
		return
	}

	// Look up the apply
	apply, err := h.service.Storage().Applies().GetByApplyIdentifier(ctx, applyID)
	if err != nil {
		h.logger.Error("failed to look up apply for cutover", "apply_id", applyID, "error", err)
		h.postComment(repo, pr, installationID, templates.RenderCutoverLookupError(applyID, err))
		return
	}
	if apply == nil {
		h.postComment(repo, pr, installationID, templates.RenderCutoverApplyNotFound(applyID))
		return
	}

	if !state.IsState(apply.State, state.Apply.WaitingForCutover) {
		h.postComment(repo, pr, installationID,
			templates.RenderCutoverNotAvailable(apply.Database, apply.Environment, applyID, apply.State))
		return
	}

	// Get tern client using the same deployment resolution as the recovery worker.
	// This ensures the cutover uses the same LocalClient (and Spirit engine) instance
	// that holds the runningMigration from the original apply or recovery resume.
	deployment := h.service.ResolveDeployment(apply.Database, apply.Deployment)
	client, err := h.service.TernClient(deployment, environment)
	if err != nil {
		h.logger.Error("failed to get tern client for cutover", "error", err,
			"database", apply.Database, "deployment", deployment, "environment", environment)
		h.postComment(repo, pr, installationID,
			templates.RenderCutoverTernError(apply.Database, environment, err))
		return
	}

	resp, err := client.Cutover(ctx, &ternv1.CutoverRequest{
		ApplyId:     applyID,
		Database:    apply.Database,
		Environment: environment,
	})
	if err != nil {
		h.logger.Error("cutover failed", "apply_id", applyID, "error", err)
		h.postComment(repo, pr, installationID,
			templates.RenderCutoverFailed(apply.Database, environment, err))
		return
	}

	if !resp.Accepted {
		h.postComment(repo, pr, installationID,
			templates.RenderCutoverNotAccepted(apply.Database, environment, resp.ErrorMessage))
		return
	}

	h.logger.Info("cutover triggered via PR comment",
		"repo", repo, "pr", pr, "apply_id", applyID,
		"database", apply.Database, "environment", environment,
		"requested_by", requestedBy)

	h.postAndTrackComment(ctx, repo, pr, installationID, apply.ID, state.Comment.Cutover,
		templates.RenderCutoverTriggered(apply.Database, environment, applyID))
}
