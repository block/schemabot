package webhook

import (
	"testing"
	"time"

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
	out := formatApplyStatusComment(runningApply(), nil, false, nil, nil, nil, nil, "", "")
	assert.Contains(t, out, "## Schema Change Status")
	assert.NotContains(t, out, "**Deployments**:")
}

// A single-deployment apply (one operation) also renders the single-deployment
// comment — the multi-deployment hierarchy only appears with more than one.
func TestFormatApplyStatusComment_OneOperationRendersSingle(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "eu", State: state.ApplyOperation.Running},
	}
	out := formatApplyStatusComment(runningApply(), ops, false, nil, nil, nil, nil, "", "")
	assert.NotContains(t, out, "**Deployments**:")
	assert.NotContains(t, out, "- 🔄 `eu`")
}

// An apply that fans out across two deployments renders the aggregate header,
// the per-status count line, and a per-deployment summary in resolved order.
func TestFormatApplyStatusComment_MultipleOperationsRendersMulti(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "eu", State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyBarrier},
		{ID: 2, Deployment: "us", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyBarrier},
	}
	out := formatApplyStatusComment(runningApply(), ops, false, nil, nil, nil, nil, "", "")
	assert.Contains(t, out, "## Schema Change Status")
	assert.Contains(t, out, "**Deployments**: 1 completed, 1 running")
	assert.Contains(t, out, "- ✅ `eu` — completed")
	assert.Contains(t, out, "- 🔄 `us` — running table copy")
}

// A barrier rollout with a deployment ready for cutover offers the cutover
// command only when the apply was started with --defer-cutover. Otherwise
// SchemaBot cuts that deployment over itself, and the comment says so instead
// of handing the operator a command.
func TestFormatApplyStatusComment_CutoverNextActionFollowsDeferCutover(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "eu", State: state.ApplyOperation.WaitingForCutover, CutoverPolicy: storage.CutoverPolicyBarrier},
		{ID: 2, Deployment: "us", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyBarrier},
	}

	automatic := formatApplyStatusComment(runningApply(), ops, false, nil, nil, nil, nil, "", "")
	assert.Contains(t, automatic, "SchemaBot will cut over `eu` next — no action needed.")
	assert.NotContains(t, automatic, "schemabot cutover")

	deferred := runningApply()
	deferred.Options = storage.MarshalApplyOptions(storage.ApplyOptions{DeferCutover: true})
	manual := formatApplyStatusComment(deferred, ops, false, nil, nil, nil, nil, "", "")
	assert.Contains(t, manual, "To cut over `eu`:")
	assert.Contains(t, manual, "schemabot cutover apply-1 -e production")
	assert.NotContains(t, manual, "SchemaBot will cut over")
}

// An apply that failed under on_failure=pause with a held sibling renders the
// paused "release or stop" guidance; once the operator releases it, the apply
// renders running degraded instead — the boundary applies the apply-level
// release latch so the comment matches what the operator will claim next.
func TestFormatApplyStatusComment_ReleasedPauseRendersDegradedNotPaused(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "eu", State: state.ApplyOperation.Failed, OnFailure: storage.OnFailurePause, ErrorMessage: "boom"},
		{ID: 2, Deployment: "us", State: state.ApplyOperation.Pending, OnFailure: storage.OnFailurePause},
	}

	paused := formatApplyStatusComment(runningApply(), ops, false, nil, nil, nil, nil, "", "")
	assert.Contains(t, paused, "paused — eu failed; release or stop")

	released := formatApplyStatusComment(runningApply(), ops, true, nil, nil, nil, nil, "", "")
	assert.NotContains(t, released, "paused — eu failed; release or stop")
}

func completedApply() *storage.Apply {
	a := runningApply()
	a.State = state.Apply.Completed
	return a
}

// An apply with no operation rows (legacy) renders the single-deployment summary
// unchanged — no aggregate header.
func TestFormatApplySummaryComment_NoOperationsRendersSingle(t *testing.T) {
	out := formatApplySummaryComment(completedApply(), nil, false, nil, nil, nil, nil, "", "")
	assert.Contains(t, out, "## ✅ Schema Change Applied")
	assert.NotContains(t, out, "**Deployments**:")
}

// A single-deployment apply (one operation) also renders the single-deployment
// summary — the multi-deployment hierarchy only appears with more than one.
func TestFormatApplySummaryComment_OneOperationRendersSingle(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "eu", State: state.ApplyOperation.Completed},
	}
	out := formatApplySummaryComment(completedApply(), ops, false, nil, nil, nil, nil, "", "")
	assert.NotContains(t, out, "**Deployments**:")
	assert.NotContains(t, out, "- ✅ `eu`")
}

// An apply that fans out across two deployments renders the aggregate terminal
// header, the per-status count line, and a per-deployment summary in resolved
// order.
func TestFormatApplySummaryComment_MultipleOperationsRendersMulti(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "eu", State: state.ApplyOperation.Completed},
		{ID: 2, Deployment: "us", State: state.ApplyOperation.Completed},
	}
	out := formatApplySummaryComment(completedApply(), ops, false, nil, nil, nil, nil, "", "")
	assert.Contains(t, out, "## ✅ Schema Change Applied")
	assert.Contains(t, out, "**Deployments**: 2 completed")
	assert.Contains(t, out, "- ✅ `eu` — completed")
	assert.Contains(t, out, "- ✅ `us` — completed")
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
	data := buildMultiApplyData(runningApply(), ops, false, tasks, nil, nil, "", "")

	require.Len(t, data.Details[0].Tables, 1)
	assert.Equal(t, "customers", data.Details[0].Tables[0].TableName)
	require.Len(t, data.Details[1].Tables, 1)
	assert.Equal(t, "orders", data.Details[1].Tables[0].TableName)
}

// A rollout run table by table has one row per target and table. Each target's
// section covers every one of its rows: its tables in row order, under the
// state the target reads as, so orders-001, done with `bikes` and queued for
// `docks`, shows both tables, and the model counts two targets, not four rows.
func TestBuildMultiApplyData_FoldsATargetsTableRows(t *testing.T) {
	row := func(id int64, target, key, st string, step int) *storage.ApplyOperation {
		return &storage.ApplyOperation{ID: id, Deployment: "eu", Target: target, OperationKey: key, OperationKind: storage.ApplyOperationKindWork, State: st, RolloutStep: step}
	}
	ops := []*storage.ApplyOperation{
		row(1, "orders-001", "orders-001/bikes", state.ApplyOperation.Completed, 1),
		row(2, "orders-002", "orders-002/bikes", state.ApplyOperation.Running, 1),
		row(3, "orders-001", "orders-001/docks", state.ApplyOperation.Pending, 2),
		row(4, "orders-002", "orders-002/docks", state.ApplyOperation.Pending, 2),
	}
	tasks := []*storage.Task{
		{ApplyOperationID: new(int64(1)), TableName: "bikes", State: state.Task.Completed},
		{ApplyOperationID: new(int64(2)), TableName: "bikes", State: state.Task.Running},
		{ApplyOperationID: new(int64(3)), TableName: "docks", State: state.Task.Pending},
		{ApplyOperationID: new(int64(4)), TableName: "docks", State: state.Task.Pending},
	}
	data := buildMultiApplyData(runningApply(), ops, false, tasks, nil, nil, "", "")

	require.Len(t, data.Model.Deployments, 2)
	require.Len(t, data.Details, 2)
	for i, want := range []struct {
		target string
		state  string
		tables []string
	}{
		{"orders-001", state.ApplyOperation.Pending, []string{"bikes", "docks"}},
		{"orders-002", state.ApplyOperation.Running, []string{"bikes", "docks"}},
	} {
		assert.Equal(t, want.target, data.Model.Deployments[i].Target)
		assert.Equal(t, want.state, data.Details[i].State, want.target)
		var tables []string
		for _, table := range data.Details[i].Tables {
			tables = append(tables, table.TableName)
		}
		assert.Equal(t, want.tables, tables, want.target)
	}
	require.Len(t, data.Model.Groups(), 1, "the deployment's two targets are one section")
}

// Per-shard rows are scoped to their owning deployment: when two deployments
// share a namespace and table name, each section shows only its own shards
// rather than a merged list across deployments.
func TestBuildMultiApplyData_ScopesShardsByOperation(t *testing.T) {
	euID, usID := int64(1), int64(2)
	ops := []*storage.ApplyOperation{
		{ID: euID, Deployment: "eu", State: state.ApplyOperation.Running},
		{ID: usID, Deployment: "us", State: state.ApplyOperation.Running},
	}
	tasks := []*storage.Task{
		{ApplyOperationID: &euID, Namespace: "commerce", TableName: "users", State: state.Task.Running},
		{ApplyOperationID: &usID, Namespace: "commerce", TableName: "users", State: state.Task.Running},
	}
	shardsByTable := map[string][]*storage.Task{
		shardCommentTableKey(&euID, "commerce", "users"): {
			{Shard: "-80", State: state.Task.Completed, ProgressPercent: 100},
		},
		shardCommentTableKey(&usID, "commerce", "users"): {
			{Shard: "80-", State: state.Task.Running, ProgressPercent: 40},
		},
	}

	data := buildMultiApplyData(runningApply(), ops, false, tasks, nil, shardsByTable, "", "")

	require.Len(t, data.Details[0].Tables, 1)
	require.Len(t, data.Details[0].Tables[0].Shards, 1)
	assert.Equal(t, "-80", data.Details[0].Tables[0].Shards[0].Shard)
	require.Len(t, data.Details[1].Tables, 1)
	require.Len(t, data.Details[1].Tables[0].Shards, 1)
	assert.Equal(t, "80-", data.Details[1].Tables[0].Shards[0].Shard)
}

// The per-deployment section reflects the operation's own state and error, not
// the parent apply's aggregate state. Redispatch is apply-level, so the parent
// apply's attempt count carries into each deployment's retry counter.
func TestBuildDeploymentDetail_UsesOperationStateAndError(t *testing.T) {
	op := &storage.ApplyOperation{ID: 1, Deployment: "us", State: state.ApplyOperation.Failed, ErrorMessage: "lock wait timeout"}
	apply := runningApply()
	apply.Attempt = 3
	detail := buildDeploymentDetail(apply, op, nil, operationDisplay{}, nil, "", "")
	assert.Equal(t, state.Apply.Failed, detail.State)
	assert.Equal(t, "lock wait timeout", detail.ErrorMessage)
	assert.Equal(t, "payments", detail.Database)
	assert.Equal(t, 3, detail.Attempt)
}

// The storage→presentation boundary resolves the rollout policies: barrier flips
// the Barrier flag, and on_failure becomes the ContinueOnFailure / PauseOnFailure
// pair — each true only when on_failure is exactly that value, so an unset value
// leaves both false (halt, the safe default).
func TestApplyOperationToPresentation_ResolvesPolicies(t *testing.T) {
	halting := applyOperationToPresentation(&storage.ApplyOperation{
		Deployment: "eu", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyBarrier,
	}, false)
	assert.True(t, halting.Barrier)
	assert.False(t, halting.ContinueOnFailure, "unset on_failure does not continue")
	assert.False(t, halting.PauseOnFailure, "unset on_failure does not pause")

	rolling := applyOperationToPresentation(&storage.ApplyOperation{
		Deployment: "us", State: state.ApplyOperation.Running,
		CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureContinue,
	}, false)
	assert.False(t, rolling.Barrier)
	assert.True(t, rolling.ContinueOnFailure)
	assert.False(t, rolling.PauseOnFailure)

	paused := applyOperationToPresentation(&storage.ApplyOperation{
		Deployment: "au", State: state.ApplyOperation.Running,
		CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailurePause,
	}, false)
	assert.True(t, paused.PauseOnFailure)
	assert.False(t, paused.ContinueOnFailure)
	assert.False(t, paused.Released, "unreleased pause stays held")

	// A released pause carries the apply-level release latch, so it behaves like
	// continue: held siblings proceed and the aggregate runs degraded, not paused.
	released := applyOperationToPresentation(&storage.ApplyOperation{
		Deployment: "au", State: state.ApplyOperation.Running,
		CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailurePause,
	}, true)
	assert.True(t, released.PauseOnFailure)
	assert.True(t, released.Released)
}

// TestDeriveApplyPresentation_MatchesStoredVerdict verifies that the PR
// comment header and applies.state read the same rows the same way. In a
// targets-list rollout of payments-a under continue, shard -80 of
// payments-001 fails and payments-002 completes, leaving payments-001's
// finalizer pending with nothing that will start it; under an unreleased
// pause, the same rows. Both surfaces settle failed.
func TestDeriveApplyPresentation_MatchesStoredVerdict(t *testing.T) {
	started := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	for _, onFailure := range []string{storage.OnFailureContinue, storage.OnFailurePause} {
		t.Run(onFailure, func(t *testing.T) {
			var ops []*storage.ApplyOperation
			add := func(target, key, kind, opState string, didStart bool) {
				op := &storage.ApplyOperation{
					ID: int64(len(ops) + 1), Deployment: "payments-a", Target: target,
					OperationKey: storage.TargetOperationKey(target, key), OperationKind: kind,
					State: opState, OnFailure: onFailure,
				}
				if didStart {
					op.StartedAt = &started
				}
				ops = append(ops, op)
			}
			add("payments-001", storage.ShardOperationKey("orders", "-80", "orders"), storage.ApplyOperationKindWork, state.ApplyOperation.Failed, true)
			add("payments-001", storage.ShardOperationKey("orders", "80-", "orders"), storage.ApplyOperationKindWork, state.ApplyOperation.Completed, true)
			add("payments-001", "orders/group_finalizer", storage.ApplyOperationKindGroupFinalizer, state.ApplyOperation.Pending, false)
			add("payments-002", storage.ShardOperationKey("orders", "-80", "orders"), storage.ApplyOperationKindWork, state.ApplyOperation.Completed, true)
			add("payments-002", storage.ShardOperationKey("orders", "80-", "orders"), storage.ApplyOperationKindWork, state.ApplyOperation.Completed, true)
			add("payments-002", "orders/group_finalizer", storage.ApplyOperationKindGroupFinalizer, state.ApplyOperation.Completed, true)

			rolloutOps := make([]state.RolloutOperation, len(ops))
			for i, op := range ops {
				rolloutOps[i] = op.RolloutOperation(false)
			}
			stored := state.DeriveRolloutApplyState(state.RolloutChildren(rolloutOps))
			require.Equal(t, state.Apply.Failed, stored)
			assert.Equal(t, stored, deriveApplyPresentation(ops, false).State)
		})
	}
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
