package webhook

import (
	"time"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

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
	data.Tables = tableProgressFromTasks(apply.Database, tasks)
	return data
}

// tableProgressFromTasks maps storage tasks to per-table template rows. The
// databaseFallback is used as a task's namespace when the task has none, so the
// single-deployment and per-deployment builders render table identities the same
// way.
func tableProgressFromTasks(databaseFallback string, tasks []*storage.Task) []templates.TableProgressData {
	if len(tasks) == 0 {
		return nil
	}
	out := make([]templates.TableProgressData, 0, len(tasks))
	for _, t := range tasks {
		ns := t.Namespace
		if ns == "" {
			ns = databaseFallback
		}
		out = append(out, templates.TableProgressData{
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
			ErrorMessage:    t.ErrorMessage,
		})
	}
	return out
}

// formatProgressComment renders the progress comment using the template system.
func formatProgressComment(apply *storage.Apply, tasks []*storage.Task) string {
	return templates.RenderApplyStatusComment(buildApplyCommentData(apply, tasks))
}

// formatSummaryComment renders the final summary comment for a terminal apply state.
func formatSummaryComment(apply *storage.Apply, tasks []*storage.Task) string {
	return templates.RenderApplySummaryComment(buildApplyCommentData(apply, tasks))
}
