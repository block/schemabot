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
func formatApplyStatusComment(apply *storage.Apply, ops []*storage.ApplyOperation, tasks []*storage.Task) string {
	if len(ops) <= 1 {
		return templates.RenderApplyStatusComment(buildApplyCommentData(apply, tasks))
	}
	return templates.RenderMultiDeploymentApplyComment(buildMultiApplyData(apply, ops, tasks))
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
func formatApplySummaryComment(apply *storage.Apply, ops []*storage.ApplyOperation, tasks []*storage.Task) string {
	if len(ops) <= 1 {
		return templates.RenderApplySummaryComment(buildApplyCommentData(apply, tasks))
	}
	return templates.RenderMultiDeploymentApplySummaryComment(buildMultiApplyData(apply, ops, tasks))
}

// buildMultiApplyData assembles the multi-deployment comment input: the derived
// rollup plus each deployment's own single-deployment comment data, so each
// deployment's section reuses the existing per-table renderer.
func buildMultiApplyData(apply *storage.Apply, ops []*storage.ApplyOperation, tasks []*storage.Task) templates.MultiDeploymentApplyData {
	tasksByOp := groupTasksByOperation(tasks)

	model := deriveApplyPresentation(ops)
	details := make(map[string]templates.ApplyStatusCommentData, len(ops))
	for _, op := range ops {
		details[op.Deployment] = buildDeploymentDetail(apply, op, tasksByOp[op.ID])
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
func buildDeploymentDetail(apply *storage.Apply, op *storage.ApplyOperation, tasks []*storage.Task) templates.ApplyStatusCommentData {
	data := templates.ApplyStatusCommentData{
		ApplyID:      apply.ApplyIdentifier,
		Database:     apply.Database,
		Environment:  apply.Environment,
		State:        op.State,
		Engine:       apply.Engine,
		ErrorMessage: op.ErrorMessage,
		Tables:       tableProgressFromTasks(apply.Database, tasks),
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
