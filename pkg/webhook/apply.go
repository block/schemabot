package webhook

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"strconv"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// buildApplyCommentData maps storage types to template data. display carries the
// apply's per-operation engine display projection (VSchema application state and
// the PlanetScale deploy-request URL), resolved from engine resume metadata by
// resolveDisplayByOperation (zero value when there is none). shardsByTable holds
// the per-shard detail rows grouped by table (nil for unsharded engines); it is
// attached to each table's progress for the compact per-shard summary.
func buildApplyCommentData(apply *storage.Apply, tasks []*storage.Task, display operationDisplay, shardsByTable map[string][]*storage.Task) templates.ApplyStatusCommentData {
	data := templates.ApplyStatusCommentData{
		ApplyID:          apply.ApplyIdentifier,
		Database:         apply.Database,
		Environment:      apply.Environment,
		State:            apply.State,
		Engine:           apply.Engine,
		ErrorMessage:     apply.ErrorMessage,
		VSchemaChanges:   display.VSchema,
		DeployRequestURL: display.DeployRequestURL,
	}
	if apply.StartedAt != nil {
		data.StartedAt = apply.StartedAt.Format(time.RFC3339)
	}
	if apply.CompletedAt != nil {
		data.CompletedAt = apply.CompletedAt.Format(time.RFC3339)
	}
	data.Tables = tableProgressFromTasks(apply.Database, tasks, shardsByTable)
	return data
}

// operationDisplay is the per-operation engine display projection surfaced in the
// PR comment: VSchema application state and the PlanetScale deploy-request URL.
// Both come from the same engine resume metadata, so they are resolved together.
type operationDisplay struct {
	VSchema          []apitypes.VSchemaChange
	DeployRequestURL string
}

// resolveDisplayByOperation projects each operation's engine display state
// (VSchema status + deploy-request URL) from the engine resume metadata persisted
// on the apply's operations — the same storage-backed projection the progress API
// uses (overlayStoredDisplayMetadata). The comment path builds from storage and
// never reads the engine progress response, so it is projected here too.
// Best-effort: a non-PlanetScale apply, an operation without resume state, or a
// decode error contributes nothing rather than blocking the comment.
func resolveDisplayByOperation(ctx context.Context, stor storage.Storage, apply *storage.Apply, ops []*storage.ApplyOperation) map[int64]operationDisplay {
	if apply == nil || apply.Engine != storage.EnginePlanetScale || len(ops) == 0 {
		return nil
	}
	var byOp map[int64]operationDisplay
	for _, op := range ops {
		rs, err := stor.ApplyOperations().GetEngineResumeState(ctx, op.ID)
		if errors.Is(err, storage.ErrEngineResumeStateNotFound) {
			continue
		}
		if err != nil {
			slog.Warn("comment will omit engine display metadata: failed to load engine resume state",
				"apply_id", apply.ApplyIdentifier, "apply_operation_id", op.ID, "error", err)
			continue
		}
		display, err := tern.PSDisplayMetadata(rs.Metadata)
		if err != nil {
			slog.Warn("comment will omit engine display metadata: failed to decode engine resume state",
				"apply_id", apply.ApplyIdentifier, "apply_operation_id", op.ID, "error", err)
			continue
		}
		changes, err := apitypes.ParseVSchemaChanges(display)
		if err != nil {
			// A malformed VSchema blob should not also drop the deploy-request URL,
			// so log and continue with no VSchema rather than skipping the operation.
			slog.Warn("comment will omit VSchema status: failed to parse VSchema changes",
				"apply_id", apply.ApplyIdentifier, "apply_operation_id", op.ID, "error", err)
		}
		od := operationDisplay{VSchema: changes, DeployRequestURL: display["deploy_request_url"]}
		if len(od.VSchema) == 0 && od.DeployRequestURL == "" {
			continue
		}
		if byOp == nil {
			byOp = make(map[int64]operationDisplay, len(ops))
		}
		byOp[op.ID] = od
	}
	return byOp
}

// tableProgressFromTasks maps storage tasks to per-table template rows. The
// databaseFallback is used as a task's namespace when the task has none, so the
// single-deployment and per-deployment builders render table identities the same
// way. shardsByTable (keyed by shardCommentTableKey on the raw namespace) supplies
// each table's per-shard breakdown when present.
func tableProgressFromTasks(databaseFallback string, tasks []*storage.Task, shardsByTable map[string][]*storage.Task) []templates.TableProgressData {
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
			Namespace:           ns,
			TableName:           t.TableName,
			DDL:                 t.DDL,
			Status:              string(t.State),
			RowsCopied:          t.RowsCopied,
			RowsTotal:           t.RowsTotal,
			PercentComplete:     t.ProgressPercent,
			ETASeconds:          int64(t.ETASeconds),
			ChecksumRowsChecked: t.ChecksumRowsChecked,
			ChecksumRowsTotal:   t.ChecksumRowsTotal,
			IsInstant:           t.IsInstant,
			ReadyToComplete:     t.ReadyToComplete,
			ErrorMessage:        t.ErrorMessage,
			Shards:              shardProgressForTable(shardsByTable, t.ApplyOperationID, t.Namespace, t.TableName),
		})
	}
	return out
}

// shardProgressForTable returns the per-shard summary rows for a table, sorted by
// shard name for stable rendering. The map is keyed by the table's owning
// apply operation plus its raw namespace (the same values the shard rows carry),
// so a multi-deployment apply that shares a namespace/table name across
// deployments keeps each deployment's shards in its own section.
func shardProgressForTable(shardsByTable map[string][]*storage.Task, applyOperationID *int64, namespace, table string) []templates.ShardProgressData {
	rows := shardsByTable[shardCommentTableKey(applyOperationID, namespace, table)]
	if len(rows) == 0 {
		return nil
	}
	out := make([]templates.ShardProgressData, 0, len(rows))
	for _, r := range rows {
		out = append(out, templates.ShardProgressData{
			Shard:           r.Shard,
			Status:          string(r.State),
			PercentComplete: r.ProgressPercent,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Shard < out[j].Shard })
	return out
}

// shardCommentTableKey keys the per-table shard map for the PR comment. It scopes
// the key by the owning apply operation so a multi-deployment apply does not merge
// shards across deployments, then by the raw namespace (before any database
// fallback) so table rows and shard rows — which both carry the same raw
// namespace — line up. A nil operation (legacy single-deployment task) keys to 0.
func shardCommentTableKey(applyOperationID *int64, namespace, table string) string {
	var opID int64
	if applyOperationID != nil {
		opID = *applyOperationID
	}
	return strconv.FormatInt(opID, 10) + "\x00" + namespace + "\x00" + table
}

// formatProgressComment renders the progress comment using the template system.
// It is the no-operations fallback (load error, or the initial rollback comment),
// so it carries no VSchema — the observer refreshes VSchema once operations load.
func formatProgressComment(apply *storage.Apply, tasks []*storage.Task, shardsByTable map[string][]*storage.Task) string {
	return templates.RenderApplyStatusComment(buildApplyCommentData(apply, tasks, operationDisplay{}, shardsByTable))
}

// formatSummaryComment renders the final summary comment for a terminal apply
// state. Like formatProgressComment it is the no-operations fallback and carries
// no VSchema.
func formatSummaryComment(apply *storage.Apply, tasks []*storage.Task, shardsByTable map[string][]*storage.Task) string {
	return templates.RenderApplySummaryComment(buildApplyCommentData(apply, tasks, operationDisplay{}, shardsByTable))
}
