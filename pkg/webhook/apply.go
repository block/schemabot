package webhook

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// buildApplyCommentData maps storage types to template data. vschemaChanges
// carries the apply's per-keyspace VSchema application state, projected from
// engine display metadata by resolveVSchemaByOperation (nil when there is none).
func buildApplyCommentData(apply *storage.Apply, tasks []*storage.Task, vschemaChanges []apitypes.VSchemaChange) templates.ApplyStatusCommentData {
	data := templates.ApplyStatusCommentData{
		ApplyID:        apply.ApplyIdentifier,
		Database:       apply.Database,
		Environment:    apply.Environment,
		State:          apply.State,
		Engine:         apply.Engine,
		ErrorMessage:   apply.ErrorMessage,
		VSchemaChanges: vschemaChanges,
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

// resolveVSchemaByOperation projects each operation's VSchema display state from
// the engine resume metadata persisted on the apply's operations — the same
// storage-backed projection the progress API uses (overlayStoredDisplayMetadata).
// The comment path builds from storage and never reads the engine progress
// response, so VSchema must be projected here too. Best-effort: a non-PlanetScale
// apply, an operation without resume state, or a decode error contributes
// nothing rather than blocking the comment.
func resolveVSchemaByOperation(ctx context.Context, stor storage.Storage, apply *storage.Apply, ops []*storage.ApplyOperation) map[int64][]apitypes.VSchemaChange {
	if apply == nil || apply.Engine != storage.EnginePlanetScale || len(ops) == 0 {
		return nil
	}
	var byOp map[int64][]apitypes.VSchemaChange
	for _, op := range ops {
		rs, err := stor.ApplyOperations().GetEngineResumeState(ctx, op.ID)
		if errors.Is(err, storage.ErrEngineResumeStateNotFound) {
			continue
		}
		if err != nil {
			slog.Warn("comment will omit VSchema status: failed to load engine resume state",
				"apply_id", apply.ApplyIdentifier, "apply_operation_id", op.ID, "error", err)
			continue
		}
		display, err := tern.PSDisplayMetadata(rs.Metadata)
		if err != nil {
			slog.Warn("comment will omit VSchema status: failed to decode engine resume state",
				"apply_id", apply.ApplyIdentifier, "apply_operation_id", op.ID, "error", err)
			continue
		}
		changes, err := apitypes.ParseVSchemaChanges(display)
		if err != nil {
			slog.Warn("comment will omit VSchema status: failed to parse VSchema changes",
				"apply_id", apply.ApplyIdentifier, "apply_operation_id", op.ID, "error", err)
			continue
		}
		if len(changes) == 0 {
			continue
		}
		if byOp == nil {
			byOp = make(map[int64][]apitypes.VSchemaChange, len(ops))
		}
		byOp[op.ID] = changes
	}
	return byOp
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
// It is the no-operations fallback (load error, or the initial rollback comment),
// so it carries no VSchema — the observer refreshes VSchema once operations load.
func formatProgressComment(apply *storage.Apply, tasks []*storage.Task) string {
	return templates.RenderApplyStatusComment(buildApplyCommentData(apply, tasks, nil))
}

// formatSummaryComment renders the final summary comment for a terminal apply
// state. Like formatProgressComment it is the no-operations fallback and carries
// no VSchema.
func formatSummaryComment(apply *storage.Apply, tasks []*storage.Task) string {
	return templates.RenderApplySummaryComment(buildApplyCommentData(apply, tasks, nil))
}
