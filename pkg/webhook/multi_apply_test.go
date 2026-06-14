package webhook

import (
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runningApply() *storage.Apply {
	return &storage.Apply{
		ApplyIdentifier: "apply-1",
		Database:        "payments",
		Environment:     "production",
		State:           state.Apply.Running,
		Engine:          "spirit",
	}
}

// An apply with no operation rows (legacy, predating apply_operations) renders
// the single-deployment comment unchanged — no aggregate header.
func TestFormatApplyStatusComment_NoOperationsRendersSingle(t *testing.T) {
	out := formatApplyStatusComment(runningApply(), nil, nil)
	assert.Contains(t, out, "## Schema Change In Progress")
	assert.NotContains(t, out, "**Deployments**:")
}

// A single-deployment apply (one operation) also renders the single-deployment
// comment — the multi-deployment hierarchy only appears with more than one.
func TestFormatApplyStatusComment_OneOperationRendersSingle(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "eu", State: state.ApplyOperation.Running},
	}
	out := formatApplyStatusComment(runningApply(), ops, nil)
	assert.NotContains(t, out, "**Deployments**:")
	assert.NotContains(t, out, "- 🔄 eu")
}

// An apply that fans out across two deployments renders the aggregate header,
// the per-status count line, and a per-deployment summary in resolved order.
func TestFormatApplyStatusComment_MultipleOperationsRendersMulti(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "eu", State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyBarrier},
		{ID: 2, Deployment: "us", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyBarrier},
	}
	out := formatApplyStatusComment(runningApply(), ops, nil)
	assert.Contains(t, out, "## Schema Change In Progress")
	assert.Contains(t, out, "**Deployments**: 1 completed, 1 running")
	assert.Contains(t, out, "- ✅ eu — completed")
	assert.Contains(t, out, "- 🔄 us — running table copy")
}

// Each deployment's tasks are routed into that deployment's section only, by
// apply_operation_id — a task for `us` must not appear under `eu`.
func TestBuildMultiApplyData_RoutesTasksByOperation(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "eu", State: state.ApplyOperation.Running},
		{ID: 2, Deployment: "us", State: state.ApplyOperation.Running},
	}
	tasks := []*storage.Task{
		{ApplyOperationID: new(int64(2)), TableName: "orders", State: state.Task.Running},
		{ApplyOperationID: new(int64(1)), TableName: "customers", State: state.Task.Running},
	}
	data := buildMultiApplyData(runningApply(), ops, tasks)

	require.Len(t, data.Details["eu"].Tables, 1)
	assert.Equal(t, "customers", data.Details["eu"].Tables[0].TableName)
	require.Len(t, data.Details["us"].Tables, 1)
	assert.Equal(t, "orders", data.Details["us"].Tables[0].TableName)
}

// The per-deployment section reflects the operation's own state and error, not
// the parent apply's aggregate state.
func TestBuildDeploymentDetail_UsesOperationStateAndError(t *testing.T) {
	op := &storage.ApplyOperation{ID: 1, Deployment: "us", State: state.ApplyOperation.Failed, ErrorMessage: "lock wait timeout"}
	detail := buildDeploymentDetail(runningApply(), op, nil)
	assert.Equal(t, state.Apply.Failed, detail.State)
	assert.Equal(t, "lock wait timeout", detail.ErrorMessage)
	assert.Equal(t, "payments", detail.Database)
}

// The storage→presentation boundary resolves the rollout policies: barrier flips
// the Barrier flag, and on_failure becomes HaltOnFailure — true unless on_failure
// is "continue", so an unset value resolves to halting (the safe default).
func TestApplyOperationToPresentation_ResolvesPolicies(t *testing.T) {
	barrier := applyOperationToPresentation(&storage.ApplyOperation{
		Deployment: "eu", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyBarrier,
	})
	assert.True(t, barrier.Barrier)
	assert.True(t, barrier.HaltOnFailure, "unset on_failure resolves to halting")

	rolling := applyOperationToPresentation(&storage.ApplyOperation{
		Deployment: "us", State: state.ApplyOperation.Running,
		CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureContinue,
	})
	assert.False(t, rolling.Barrier)
	assert.False(t, rolling.HaltOnFailure)
}

// Tasks without an apply_operation_id (legacy rows) are not attributable to a
// deployment and are dropped from the per-operation grouping.
func TestGroupTasksByOperation_SkipsUnlinkedTasks(t *testing.T) {
	grouped := groupTasksByOperation([]*storage.Task{
		{ApplyOperationID: new(int64(1)), TableName: "a"},
		{ApplyOperationID: nil, TableName: "orphan"},
		{ApplyOperationID: new(int64(1)), TableName: "b"},
	})
	require.Len(t, grouped[1], 2)
	assert.Len(t, grouped, 1)
}
