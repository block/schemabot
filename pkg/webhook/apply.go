package webhook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
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
// attached to each table's progress for the compact per-shard summary. tenant is
// the deployment's tenant identity, carried into every pasteable command hint.
func buildApplyCommentData(apply *storage.Apply, tasks []*storage.Task, display operationDisplay, shardsByTable map[string][]*storage.Task, tenant string) templates.ApplyStatusCommentData {
	data := templates.ApplyStatusCommentData{
		ApplyID:          apply.ApplyIdentifier,
		Database:         apply.Database,
		Environment:      apply.Environment,
		State:            apply.State,
		Engine:           apply.Engine,
		ErrorMessage:     apply.ErrorMessage,
		Attempt:          apply.Attempt,
		VSchemaChanges:   display.VSchema,
		DeployRequestURL: display.DeployRequestURL,
		RevertExpiresAt:  display.RevertExpiresAt,
		Step:             display.Step.Step,
		StepsTotal:       display.Step.StepsTotal,
		Statement:        display.Step.Statement,
		Tenant:           tenant,
		Rollback:         apply.IsRollback(),
		DeferCutover:     apply.GetOptions().DeferCutover,
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
// PR comment: the statement position from the operation's durable progress
// metadata, and — for PlanetScale — VSchema application state and the
// deploy-request URL from the engine resume metadata.
type operationDisplay struct {
	VSchema          []apitypes.VSchemaChange
	DeployRequestURL string
	// RevertExpiresAt is the RFC3339 deadline when the revert window closes
	// (PlanetScale only), surfaced so the comment can show time remaining. Empty
	// outside the revert window.
	RevertExpiresAt string
	// Step is the position of a running apply inside its statement sequence, as
	// last persisted by the driver. Zero when the engine reports none.
	Step apitypes.ProgressStep
}

// isZero reports whether the projection carries nothing worth rendering.
func (d operationDisplay) isZero() bool {
	return len(d.VSchema) == 0 && d.DeployRequestURL == "" && d.RevertExpiresAt == "" && d.Step == (apitypes.ProgressStep{})
}

// resolveDisplayByOperation projects each operation's engine display state from
// what is persisted on the apply's operations — the same storage-backed
// projection the progress API uses (loadStoredProgressMetadata). The comment
// path builds from storage and never reads the engine progress response, so it
// is projected here too. Every engine contributes the statement position from
// the operation's stored progress metadata; PlanetScale operations additionally
// contribute VSchema status and the deploy-request URL from engine resume state.
// Best-effort: an operation without stored state, or a decode error, contributes
// nothing rather than blocking the comment.
func resolveDisplayByOperation(ctx context.Context, stor storage.Storage, apply *storage.Apply, ops []*storage.ApplyOperation) map[int64]operationDisplay {
	if apply == nil || len(ops) == 0 {
		return nil
	}
	var byOp map[int64]operationDisplay
	for _, op := range ops {
		step, err := storedProgressStep(op)
		if err != nil {
			slog.Warn("comment will omit the statement position: progress metadata is malformed",
				append(apply.LogAttrs(), "apply_operation_id", op.ID, "operation_deployment", op.Deployment, "error", err)...)
		}
		od := operationDisplay{Step: step}
		if apply.Engine == storage.EnginePlanetScale {
			od.VSchema, od.DeployRequestURL, od.RevertExpiresAt = planetScaleDisplay(ctx, stor, apply, op)
		}
		if od.isZero() {
			continue
		}
		if byOp == nil {
			byOp = make(map[int64]operationDisplay, len(ops))
		}
		byOp[op.ID] = od
	}
	return byOp
}

// storedProgressStep decodes the statement position from the operation's
// durable progress metadata. An operation that has persisted no metadata, or
// whose engine publishes no position, yields the zero step with a nil error;
// a record that cannot be decoded, or that carries an impossible position,
// yields the zero step with the error so the caller can log it and render no
// position rather than a wrong one.
func storedProgressStep(op *storage.ApplyOperation) (apitypes.ProgressStep, error) {
	metadata, err := op.ParseProgressMetadata()
	if err != nil {
		return apitypes.ProgressStep{}, err
	}
	return apitypes.ParseProgressStep(metadata)
}

// progressStepFingerprint summarises the statement position of every operation
// so the comment observer can tell that a step advanced when no other progress
// figure moved — an engine that executes a statement sequence reports no row
// counts, so the position is the only figure that changes between polls.
// Operations with no position, or with metadata that cannot be decoded,
// contribute nothing: the render path logs the malformed record, and
// repeating that warning on every poll would drown the log.
func progressStepFingerprint(ops []*storage.ApplyOperation) string {
	var sb strings.Builder
	for _, op := range ops {
		step, err := storedProgressStep(op)
		if err != nil || step == (apitypes.ProgressStep{}) {
			continue
		}
		fmt.Fprintf(&sb, "%d:%d/%d;", op.ID, step.Step, step.StepsTotal)
	}
	return sb.String()
}

// planetScaleDisplay loads the operation's engine resume state and projects the
// VSchema application state, deploy-request URL, and revert deadline from it.
// An operation without resume state or with an undecodable record contributes
// nothing.
func planetScaleDisplay(ctx context.Context, stor storage.Storage, apply *storage.Apply, op *storage.ApplyOperation) (vschema []apitypes.VSchemaChange, deployRequestURL, revertExpiresAt string) {
	rs, err := stor.ApplyOperations().GetEngineResumeState(ctx, op.ID)
	if errors.Is(err, storage.ErrEngineResumeStateNotFound) {
		return nil, "", ""
	}
	if err != nil {
		slog.Warn("comment will omit engine display metadata: failed to load engine resume state",
			"apply_id", apply.ApplyIdentifier, "apply_operation_id", op.ID, "error", err)
		return nil, "", ""
	}
	display, err := tern.PSDisplayMetadata(rs.Metadata)
	if err != nil {
		slog.Warn("comment will omit engine display metadata: failed to decode engine resume state",
			"apply_id", apply.ApplyIdentifier, "apply_operation_id", op.ID, "error", err)
		return nil, "", ""
	}
	changes, err := apitypes.ParseVSchemaChanges(display)
	if err != nil {
		// A malformed VSchema blob should not also drop the deploy-request URL,
		// so log and continue with no VSchema rather than skipping the operation.
		slog.Warn("comment will omit VSchema status: failed to parse VSchema changes",
			"apply_id", apply.ApplyIdentifier, "apply_operation_id", op.ID, "error", err)
	}
	return changes, display["deploy_request_url"], display["revert_expires_at"]
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
			Throttled:           t.Throttled,
			ThrottleReason:      t.ThrottleReason,
			IsInstant:           t.IsInstant,
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
			RowsCopied:      r.RowsCopied,
			RowsTotal:       r.RowsTotal,
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
func formatProgressComment(apply *storage.Apply, tasks []*storage.Task, shardsByTable map[string][]*storage.Task, tenant string) string {
	return templates.RenderApplyStatusComment(buildApplyCommentData(apply, tasks, operationDisplay{}, shardsByTable, tenant))
}

// formatSummaryComment renders the final summary comment for a terminal apply
// state. Like formatProgressComment it is the no-operations fallback and carries
// no VSchema.
func formatSummaryComment(apply *storage.Apply, tasks []*storage.Task, shardsByTable map[string][]*storage.Task, tenant string) string {
	return templates.RenderApplySummaryComment(buildApplyCommentData(apply, tasks, operationDisplay{}, shardsByTable, tenant))
}
