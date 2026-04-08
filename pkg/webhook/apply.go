package webhook

import (
	"context"
	"fmt"
	"strings"
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
func (h *Handler) watchApplyProgress(ctx context.Context, apply *storage.Apply, skipInitialPost ...bool) {
	repo := apply.Repository
	pr := apply.PullRequest
	installationID := apply.InstallationID
	applyID := apply.ID
	opts := apply.GetOptions()

	// Post initial progress comment (unless caller already posted one)
	if len(skipInitialPost) == 0 || !skipInitialPost[0] {
		body := formatProgressComment(apply, nil)
		h.postAndTrackComment(ctx, repo, pr, installationID, applyID, state.Comment.Progress, body)
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastState string
	var lastPercent int
	hasCutoverComment := false

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

		currentPercent := aggregatePercent(tasks)
		stateChanged := current.State != lastState
		// Only update on state change or 10% progress increments
		significantProgress := currentPercent/10 != lastPercent/10

		if !stateChanged && !significantProgress {
			continue
		}

		lastState = current.State
		lastPercent = currentPercent

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
			h.updateCheckRunForApplyResult(ctx, repo, pr, installationID, current)
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

// aggregatePercent computes a weighted average progress percentage across tasks.
func aggregatePercent(tasks []*storage.Task) int {
	if len(tasks) == 0 {
		return 0
	}
	total := 0
	for _, t := range tasks {
		total += t.ProgressPercent
	}
	return total / len(tasks)
}

// formatProgressComment renders the progress comment body for an apply.
func formatProgressComment(apply *storage.Apply, tasks []*storage.Task) string {
	var sb strings.Builder

	emoji := stateEmoji(apply.State)
	fmt.Fprintf(&sb, "## %s Schema Change: `%s`\n\n", emoji, apply.Database)
	fmt.Fprintf(&sb, "**Environment:** %s\n", apply.Environment)
	fmt.Fprintf(&sb, "**Engine:** %s\n", apply.Engine)
	fmt.Fprintf(&sb, "**State:** %s\n\n", formatState(apply.State))

	if len(tasks) > 0 {
		sb.WriteString("| Table | DDL | Status | Progress |\n")
		sb.WriteString("|-------|-----|--------|----------|\n")
		for _, t := range tasks {
			progress := formatTaskProgress(t)
			fmt.Fprintf(&sb, "| `%s` | `%s` | %s | %s |\n",
				t.TableName, truncateDDL(t.DDL, 60), formatState(string(t.State)), progress)
		}
		sb.WriteString("\n")
	}

	if apply.ErrorMessage != "" {
		fmt.Fprintf(&sb, "**Error:** %s\n", apply.ErrorMessage)
	}

	return sb.String()
}

// formatCutoverComment renders the cutover confirmation comment.
func formatCutoverComment(apply *storage.Apply, tasks []*storage.Task) string {
	var sb strings.Builder

	fmt.Fprintf(&sb, "## Cutover Ready: `%s`\n\n", apply.Database)
	fmt.Fprintf(&sb, "**Environment:** %s\n", apply.Environment)
	fmt.Fprintf(&sb, "**State:** %s\n\n", formatState(apply.State))

	if len(tasks) > 0 {
		readyCount := 0
		for _, t := range tasks {
			if t.ReadyToComplete {
				readyCount++
			}
		}
		fmt.Fprintf(&sb, "%d/%d table(s) ready for cutover.\n\n", readyCount, len(tasks))
	}

	sb.WriteString("To proceed with cutover, comment:\n")
	sb.WriteString("```\nschemabot cutover -e " + apply.Environment + "\n```\n")

	return sb.String()
}

// formatSummaryComment renders the final summary comment using the dedicated template.
func formatSummaryComment(apply *storage.Apply, tasks []*storage.Task) string {
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
		})
	}
	return templates.RenderApplySummaryComment(data)
}

// stateEmoji returns an emoji for the apply state.
func stateEmoji(s string) string {
	switch s {
	case state.Apply.Completed:
		return "✅"
	case state.Apply.Failed:
		return "❌"
	case state.Apply.Stopped:
		return "⏹️"
	case state.Apply.Reverted:
		return "↩️"
	case state.Apply.Running:
		return "⏳"
	default:
		return "🛠️"
	}
}

// formatState formats a state string for display.
func formatState(s string) string {
	return "`" + s + "`"
}

// formatTaskProgress returns a human-readable progress string for a task.
func formatTaskProgress(t *storage.Task) string {
	if t.IsInstant {
		return "instant"
	}
	if t.State == state.Task.Completed {
		return "done"
	}
	if t.ProgressPercent > 0 {
		s := fmt.Sprintf("%d%%", t.ProgressPercent)
		if t.ETASeconds > 0 {
			s += fmt.Sprintf(" (ETA %s)", formatETA(t.ETASeconds))
		}
		return s
	}
	return "-"
}

// formatETA formats seconds into a human-readable duration.
func formatETA(seconds int) string {
	d := time.Duration(seconds) * time.Second
	if d < time.Minute {
		return fmt.Sprintf("%ds", seconds)
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
}

// truncateDDL truncates a DDL string for table display.
func truncateDDL(ddl string, maxLen int) string {
	// Take first line only
	if idx := strings.IndexByte(ddl, '\n'); idx >= 0 {
		ddl = ddl[:idx]
	}
	if len(ddl) > maxLen {
		return ddl[:maxLen-3] + "..."
	}
	return ddl
}
