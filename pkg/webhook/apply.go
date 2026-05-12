package webhook

import (
	"context"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// watchApplyProgress polls an apply's state and updates PR comments according to the
// comment lifecycle state machine. It creates a progress comment on start, edits it
// during execution, optionally creates a cutover comment for defer_cutover mode,
// and posts a summary comment on terminal state.
//
// This function runs as a background goroutine spawned after an apply is submitted.
// It exits when the apply reaches a terminal state or the context is cancelled.
func (h *Handler) watchApplyProgress(ctx context.Context, repo string, pr int, installationID int64, apply *storage.Apply, skipInitialPost ...bool) {
	applyID := apply.ID
	opts := apply.GetOptions()

	// Post initial progress comment (unless caller already posted one)
	if len(skipInitialPost) == 0 || !skipInitialPost[0] {
		body := formatProgressComment(apply, nil)
		h.postAndTrackComment(ctx, repo, pr, installationID, applyID, state.Comment.Progress, body)
	}

	// Adaptive polling: 5s when progress is moving, 30s when stagnant.
	// Reduces GitHub API calls during Spirit's throttled tail where row
	// count barely changes for minutes.
	const (
		activePollInterval   = 5 * time.Second
		stagnantPollInterval = 30 * time.Second
		stagnantThreshold    = 3 // consecutive unchanged ticks before slowing down
	)

	ticker := time.NewTicker(activePollInterval)
	defer ticker.Stop()

	hasCutoverComment := false
	var lastRowsCopied int64
	stagnantTicks := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// Refresh apply state from DB
		current, err := h.service.Storage().Applies().Get(ctx, applyID)
		if err != nil {
			h.logger.Error("failed to get apply for progress", "applyID", applyID, "error", err)
			continue
		}
		if current == nil {
			h.logger.Warn("apply not found during progress watch", "applyID", applyID)
			return
		}

		// Get task progress
		tasks, err := h.service.Storage().Tasks().GetByApplyID(ctx, applyID)
		if err != nil {
			h.logger.Error("failed to get tasks for progress", "applyID", applyID, "error", err)
			continue
		}

		// Adapt poll interval based on whether progress is moving
		var totalRows int64
		for _, t := range tasks {
			totalRows += t.RowsCopied
		}
		if totalRows == lastRowsCopied && !state.IsTerminalApplyState(current.State) {
			stagnantTicks++
			if stagnantTicks == stagnantThreshold {
				ticker.Reset(stagnantPollInterval)
			}
			if stagnantTicks >= stagnantThreshold {
				continue // skip comment edit — nothing changed
			}
		} else {
			if stagnantTicks >= stagnantThreshold {
				ticker.Reset(activePollInterval)
			}
			stagnantTicks = 0
			lastRowsCopied = totalRows
		}

		if state.IsTerminalApplyState(current.State) {
			// Edit active comment to final state
			activeCommentState := state.Comment.Progress
			if hasCutoverComment {
				activeCommentState = state.Comment.Cutover
			}
			finalBody := formatProgressComment(current, tasks)
			h.editTrackedComment(ctx, repo, installationID, applyID, activeCommentState, finalBody)

			// Post summary comment
			summaryBody := formatSummaryComment(current, tasks)
			h.postAndTrackComment(ctx, repo, pr, installationID, applyID, state.Comment.Summary, summaryBody)

			// Update the GitHub check run to reflect the terminal state
			h.updateCheckRecordForApplyResult(ctx, repo, pr, current)
			if aggClient, err := h.ghClient.ForInstallation(installationID); err == nil {
				// Look up the head SHA from the check record
				if checkRecord, err := h.service.Storage().Checks().Get(ctx, repo, pr, current.Environment, current.DatabaseType, current.Database); err == nil && checkRecord != nil {
					h.updateAggregateCheck(ctx, aggClient, repo, pr, checkRecord.HeadSHA)
				}
			}
			return
		}

		// Check if we need to create a cutover comment (defer_cutover mode entering cutting_over)
		if current.State == state.Apply.CuttingOver && opts.DeferCutover && !hasCutoverComment {
			cutoverBody := formatCutoverComment(current, tasks)
			h.postAndTrackComment(ctx, repo, pr, installationID, applyID, state.Comment.Cutover, cutoverBody)
			hasCutoverComment = true
			continue
		}

		// Edit the active comment
		if hasCutoverComment {
			body := formatCutoverComment(current, tasks)
			h.editTrackedComment(ctx, repo, installationID, applyID, state.Comment.Cutover, body)
		} else {
			body := formatProgressComment(current, tasks)
			h.editTrackedComment(ctx, repo, installationID, applyID, state.Comment.Progress, body)
		}
	}
}

// buildApplyCommentData maps storage types to template data.
func buildApplyCommentData(apply *storage.Apply, tasks []*storage.Task) templates.ApplyStatusCommentData {
	data := templates.ApplyStatusCommentData{
		ApplyID:      apply.ApplyIdentifier,
		Database:     apply.Database,
		Environment:  apply.Environment,
		State:        apply.State,
		Engine:       apply.Engine,
		ErrorMessage: apply.ErrorMessage,
	}
	if apply.StartedAt != nil {
		data.StartedAt = apply.StartedAt.Format(time.RFC3339)
	}
	if apply.CompletedAt != nil {
		data.CompletedAt = apply.CompletedAt.Format(time.RFC3339)
	}
	for _, t := range tasks {
		ns := t.Namespace
		if ns == "" {
			ns = apply.Database
		}
		data.Tables = append(data.Tables, templates.TableProgressData{
			Namespace:       ns,
			TableName:       t.TableName,
			DDL:             t.DDL,
			Status:          string(t.State),
			RowsCopied:      t.RowsCopied,
			RowsTotal:       t.RowsTotal,
			PercentComplete: t.ProgressPercent,
			ETASeconds:      int64(t.ETASeconds),
			IsInstant:       t.IsInstant,
			ReadyToComplete: t.ReadyToComplete,
		})
	}
	return data
}

// formatProgressComment renders the progress comment using the template system.
func formatProgressComment(apply *storage.Apply, tasks []*storage.Task) string {
	return templates.RenderApplyStatusComment(buildApplyCommentData(apply, tasks))
}

// formatCutoverComment renders the cutover confirmation comment.
// Uses the same template — the cutover state produces the appropriate header and footer.
func formatCutoverComment(apply *storage.Apply, tasks []*storage.Task) string {
	return templates.RenderApplyStatusComment(buildApplyCommentData(apply, tasks))
}

// formatSummaryComment renders the final summary comment for a terminal apply state.
func formatSummaryComment(apply *storage.Apply, tasks []*storage.Task) string {
	return templates.RenderApplySummaryComment(buildApplyCommentData(apply, tasks))
}
