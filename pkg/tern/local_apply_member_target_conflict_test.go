package tern

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// memberCopyTask is payments-001's copy of orders, running under a driven
// apply: a task on the shared database name whose operation row records the
// target it runs on.
func memberCopyTask() *storage.Task {
	operationID := int64(21)
	return &storage.Task{
		ID:               1,
		TaskIdentifier:   "task-payments-001-orders",
		ApplyID:          1,
		ApplyOperationID: &operationID,
		Database:         "payments",
		DatabaseType:     storage.DatabaseTypeMySQL,
		TableName:        "orders",
		State:            state.Task.Running,
	}
}

// memberTargetConflictClient builds a data-plane client whose storage holds
// the given task under a live drive of payments-001, with operation rows keyed
// by id. The engine reports a running copy, so only target scoping can let a
// dispatch past the task.
func memberTargetConflictClient(task *storage.Task, ops map[int64]*storage.ApplyOperation) *LocalClient {
	return &LocalClient{
		config: LocalConfig{Database: "payments", Type: storage.DatabaseTypeMySQL},
		storage: &exactProgressStorage{
			applies: &mockApplyStore{apply: &storage.Apply{
				ID:              1,
				ApplyIdentifier: "apply-payments-001",
				Database:        "payments",
				DatabaseType:    storage.DatabaseTypeMySQL,
				Deployment:      "payments-001",
				State:           state.Apply.Running,
				LeaseOwner:      "driver-a",
				UpdatedAt:       time.Now(),
			}},
			tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
			logs:            &mockApplyLogStore{},
			applyOperations: &mockApplyOperationStore{ops: ops},
		},
		spiritEngine: &fakeControlEngine{
			progressResult: &engine.ProgressResult{State: engine.StateRunning, Message: "Copying rows"},
		},
		logger: slog.Default(),
	}
}

// memberDispatchScope derives the scope of a whole-target dispatch naming
// memberTarget against a plan for planTarget, the way the data plane derives
// it from the wire request.
func memberDispatchScope(t *testing.T, planTarget, memberTarget string) (*storage.Plan, dispatchScope) {
	t.Helper()
	plan := &storage.Plan{
		PlanIdentifier: "plan-" + planTarget,
		Database:       "payments",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Target:         planTarget,
	}
	req := &ternv1.ApplyRequest{}
	if memberTarget != "" {
		req.Options = map[string]string{dispatchMemberTargetOption: memberTarget}
	}
	scope, err := deriveDispatchScope(plan, req)
	require.NoError(t, err)
	return plan, scope
}

// A deployment addresses targets payments-001 and payments-002 under a
// parallel cutover policy capped at two drivers. While payments-001 copies
// orders, payments-002's dispatch shares the database name but not the
// physical database, so it proceeds; a second dispatch for payments-001 is
// still refused, and so is a dispatch that names no member target, which is
// how every single-target and deployments-map dispatch arrives.
func TestConflictCheck_MemberTargetScopesToItsOwnTarget(t *testing.T) {
	task := memberCopyTask()
	client := memberTargetConflictClient(task, map[int64]*storage.ApplyOperation{
		21: {ID: 21, ApplyID: 1, Deployment: "payments-001", OperationKey: "payments-001", Target: "payments-001"},
	})

	plan, sibling := memberDispatchScope(t, "payments-002", "payments-002")
	assert.Equal(t, "payments-002", sibling.memberTarget)
	_, _, err := client.checkActiveTaskConflict(t.Context(), plan, "production", sibling, 0)
	require.NoError(t, err, "a sibling target's copy must not hold this target's dispatch")

	plan, same := memberDispatchScope(t, "payments-001", "payments-001")
	blocking, _, err := client.checkActiveTaskConflict(t.Context(), plan, "production", same, 0)
	require.Error(t, err, "two applies on one target must still conflict")
	assert.Contains(t, err.Error(), "schema change already in progress")
	assert.Equal(t, "task-payments-001-orders", blocking.taskIdentifier)

	plan, whole := memberDispatchScope(t, "payments-002", "")
	assert.Empty(t, whole.memberTarget)
	_, _, err = client.checkActiveTaskConflict(t.Context(), plan, "production", whole, 0)
	require.Error(t, err, "a dispatch reserving no target conflicts with every target's work")
	assert.Contains(t, err.Error(), "schema change already in progress")
}

// A member dispatch is scoped to its target only when it reached the client
// serving that target, which is the plan's target. One naming a target the
// plan does not has reached a sibling's database or carries a sibling's plan,
// and no reservation stops it from running a schema its target was never
// planned for, so it is refused before any conflict check or row exists.
func TestDeriveDispatchScope_RefusesMemberTargetDisagreeingWithPlan(t *testing.T) {
	plan := &storage.Plan{
		PlanIdentifier: "plan-payments-001",
		Database:       "payments",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Target:         "payments-001",
	}
	_, err := deriveDispatchScope(plan, &ternv1.ApplyRequest{Options: map[string]string{dispatchMemberTargetOption: "payments-002"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `dispatch for rollout member target "payments-002" runs plan plan-payments-001, which was produced for target "payments-001"`)
}

// A task's target is known only through its operation row. When that row is
// missing, unreadable, or records no target, the task cannot be proven to run
// elsewhere and keeps blocking the member dispatch (OW-7).
func TestConflictCheck_MemberTargetBlocksOnUnattributableTask(t *testing.T) {
	cases := map[string]struct {
		task func() *storage.Task
		ops  map[int64]*storage.ApplyOperation
	}{
		"operation row missing": {
			task: memberCopyTask,
			ops:  map[int64]*storage.ApplyOperation{},
		},
		"operation records no target": {
			task: memberCopyTask,
			ops: map[int64]*storage.ApplyOperation{
				21: {ID: 21, ApplyID: 1, Deployment: "payments"},
			},
		},
		"task records no operation": {
			task: func() *storage.Task {
				task := memberCopyTask()
				task.ApplyOperationID = nil
				return task
			},
			ops: map[int64]*storage.ApplyOperation{},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			task := tc.task()
			client := memberTargetConflictClient(task, tc.ops)
			plan, scope := memberDispatchScope(t, "payments-002", "payments-002")

			blocking, _ := client.findBlockingTask(t.Context(), []*storage.Task{task}, plan, "production", scope, 0, newConflictScanMemo())
			assert.True(t, blocking.blocks(), "an unattributable task must keep blocking")
			assert.Equal(t, "task-payments-001-orders", blocking.taskIdentifier)
		})
	}
}

// The one-active-apply guard reserves the deployment an apply records. A
// member dispatch served by its own target records that target, so the
// sibling targets of one deployment hold separate reservations; every other
// dispatch records the database this client is bound to, as it always has.
func TestDispatchDeployment_ReservesTheMemberTarget(t *testing.T) {
	client := &LocalClient{config: LocalConfig{Database: "payments"}}

	_, member := memberDispatchScope(t, "payments-002", "payments-002")
	assert.Equal(t, "payments-002", client.dispatchDeployment(member))

	_, whole := memberDispatchScope(t, "payments", "")
	assert.Equal(t, "payments", client.dispatchDeployment(whole))
}
