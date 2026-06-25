package webhook

import (
	"time"

	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// formatApplyStatusComment renders the progress/status PR comment for an apply,
// choosing the layout from how many deployments the apply owns. The threshold is
// the apply's own operation-row count: zero or one operation renders today's
// single-deployment UX byte-for-byte (legacy applies that predate
// apply_operations fall here too), and two or more renders the aggregated
// multi-deployment comment derived from pkg/presentation.
//
// ops must be in resolved deployment order (as returned by
// ApplyOperations().ListByApply); tasks are the apply's tasks across all
// deployments, regrouped per operation for the multi-deployment layout.
func formatApplyStatusComment(apply *storage.Apply, ops []*storage.ApplyOperation, tasks []*storage.Task, displayByOp map[int64]operationDisplay, shardsByTable map[string][]*storage.Task) string {
	if len(ops) <= 1 {
		return templates.RenderApplyStatusComment(buildApplyCommentData(apply, tasks, singleOpDisplay(ops, displayByOp), shardsByTable))
	}
	return templates.RenderMultiDeploymentApplyComment(buildMultiApplyData(apply, ops, tasks, displayByOp, shardsByTable))
}

// formatApplySummaryComment renders the terminal summary PR comment for an apply,
// choosing the layout from how many deployments the apply owns, identically to
// formatApplyStatusComment: zero or one operation renders today's
// single-deployment summary byte-for-byte (legacy applies that predate
// apply_operations fall here too), and two or more renders the aggregated
// multi-deployment summary derived from pkg/presentation.
//
// ops must be in resolved deployment order (as returned by
// ApplyOperations().ListByApply); tasks are the apply's tasks across all
// deployments, regrouped per operation for the multi-deployment layout.
func formatApplySummaryComment(apply *storage.Apply, ops []*storage.ApplyOperation, tasks []*storage.Task, displayByOp map[int64]operationDisplay, shardsByTable map[string][]*storage.Task) string {
	if len(ops) <= 1 {
		return templates.RenderApplySummaryComment(buildApplyCommentData(apply, tasks, singleOpDisplay(ops, displayByOp), shardsByTable))
	}
	return templates.RenderMultiDeploymentApplySummaryComment(buildMultiApplyData(apply, ops, tasks, displayByOp, shardsByTable))
}

// singleOpDisplay returns the engine display projection for a zero/one-operation
// apply rendered with the single-deployment layout: the lone operation's display,
// or the zero value when the apply has no operation (legacy) or no display data.
func singleOpDisplay(ops []*storage.ApplyOperation, displayByOp map[int64]operationDisplay) operationDisplay {
	if len(ops) != 1 {
		return operationDisplay{}
	}
	return displayByOp[ops[0].ID]
}

// buildMultiApplyData assembles the multi-deployment comment input: the derived
// rollup plus each deployment's own single-deployment comment data, so each
// deployment's section reuses the existing per-table renderer.
func buildMultiApplyData(apply *storage.Apply, ops []*storage.ApplyOperation, tasks []*storage.Task, displayByOp map[int64]operationDisplay, shardsByTable map[string][]*storage.Task) templates.MultiDeploymentApplyData {
	tasksByOp := groupTasksByOperation(tasks)

	model := deriveApplyPresentation(ops)
	details := make(map[string]templates.ApplyStatusCommentData, len(ops))
	for _, op := range ops {
		details[op.Deployment] = buildDeploymentDetail(apply, op, tasksByOp[op.ID], displayByOp[op.ID], shardsByTable)
	}

	data := templates.MultiDeploymentApplyData{
		Model:       model,
		ApplyID:     apply.ApplyIdentifier,
		Environment: apply.Environment,
		Details:     details,
	}
	if apply.StartedAt != nil {
		data.StartedAt = apply.StartedAt.Format(time.RFC3339)
	}
	if apply.CompletedAt != nil {
		data.CompletedAt = apply.CompletedAt.Format(time.RFC3339)
	}
	return data
}

// deriveApplyPresentation maps the apply's operation rows to the surface-neutral
// presentation inputs and derives the rollup. Rows are already in resolved order.
func deriveApplyPresentation(ops []*storage.ApplyOperation) presentation.Apply {
	inputs := make([]presentation.Operation, 0, len(ops))
	for _, op := range ops {
		inputs = append(inputs, applyOperationToPresentation(op))
	}
	return presentation.Derive(inputs)
}

// applyOperationToPresentation maps one storage operation row to the neutral
// presentation input, resolving the rollout-policy values at the boundary:
// cutover_policy "barrier" becomes the Barrier flag, and on_failure becomes both
// the HaltOnFailure flag — true unless on_failure is "continue" — and the
// ContinueOnFailure flag — true only when on_failure is exactly "continue". Any
// other value fails closed to halting, the safe default the claim predicate and
// the aggregate projection also assume.
func applyOperationToPresentation(op *storage.ApplyOperation) presentation.Operation {
	return presentation.Operation{
		Deployment:        op.Deployment,
		State:             op.State,
		Barrier:           op.CutoverPolicy == storage.CutoverPolicyBarrier,
		HaltOnFailure:     op.OnFailure != storage.OnFailureContinue,
		ContinueOnFailure: op.OnFailure == storage.OnFailureContinue,
		Error:             op.ErrorMessage,
	}
}

// buildDeploymentDetail builds the single-deployment comment data for one
// deployment's <details> body: its operation state and error, the parent apply's
// identity and timing, and the deployment's own tasks. The deployment's database
// target is shown via the section's deployment name; the per-table rows fall back
// to the apply database for namespace, matching the single-deployment renderer.
func buildDeploymentDetail(apply *storage.Apply, op *storage.ApplyOperation, tasks []*storage.Task, display operationDisplay, shardsByTable map[string][]*storage.Task) templates.ApplyStatusCommentData {
	data := templates.ApplyStatusCommentData{
		ApplyID:          apply.ApplyIdentifier,
		Database:         apply.Database,
		Environment:      apply.Environment,
		State:            op.State,
		Engine:           apply.Engine,
		ErrorMessage:     op.ErrorMessage,
		Tables:           tableProgressFromTasks(apply.Database, tasks, shardsByTable),
		VSchemaChanges:   display.VSchema,
		DeployRequestURL: display.DeployRequestURL,
	}
	if apply.StartedAt != nil {
		data.StartedAt = apply.StartedAt.Format(time.RFC3339)
	}
	if apply.CompletedAt != nil {
		data.CompletedAt = apply.CompletedAt.Format(time.RFC3339)
	}
	return data
}

// groupTasksByOperation buckets tasks by their owning apply_operation. Tasks with
// no apply_operation_id (legacy rows predating the per-deployment back-fill) are
// not attributable to a deployment and are omitted from the per-deployment
// sections; in a genuine multi-deployment apply every task carries the id.
func groupTasksByOperation(tasks []*storage.Task) map[int64][]*storage.Task {
	byOp := make(map[int64][]*storage.Task)
	for _, t := range tasks {
		if t.ApplyOperationID == nil {
			continue
		}
		byOp[*t.ApplyOperationID] = append(byOp[*t.ApplyOperationID], t)
	}
	return byOp
}
