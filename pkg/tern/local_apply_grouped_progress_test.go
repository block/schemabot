package tern

import (
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A pending grouped progress report only signals lost work when at least one
// stored task is genuinely in flight. Resting states — stopped,
// failed_retryable — have no active engine work by design, terminal states are
// never re-verified, and an apply with no tasks has nothing to settle.
func TestEngineReportsLostApplyWork(t *testing.T) {
	cases := []struct {
		name        string
		engineState string
		taskStates  []string
		want        bool
	}{
		{"running task with pending engine report is lost work", state.Task.Pending, []string{state.Task.Running, state.Task.Completed}, true},
		{"cutting-over task with pending engine report is lost work", state.Task.Pending, []string{state.Task.CuttingOver}, true},
		{"all tasks at rest is not divergence", state.Task.Pending, []string{state.Task.FailedRetryable, state.Task.Stopped}, false},
		{"all tasks terminal is never re-verified", state.Task.Pending, []string{state.Task.Completed, state.Task.Cancelled}, false},
		{"running engine report is not lost work", state.Task.Running, []string{state.Task.Running}, false},
		{"no tasks means nothing to settle", state.Task.Pending, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tasks := make([]*storage.Task, len(tc.taskStates))
			for i, s := range tc.taskStates {
				tasks[i] = &storage.Task{State: s}
			}
			assert.Equal(t, tc.want, engineReportsLostApplyWork(tc.engineState, tasks))
		})
	}
}

// lostWorkAtomicPollFixture builds a running grouped (Vitess) apply with two
// running tasks and a LocalClient polling the given engine with a fast poll
// cadence, so lost-work scenarios can drive many polls without waiting out the
// real cadence. trustBudget sets how long the drive keeps trusting an engine
// reporting no active schema change: a tiny budget reaches target
// verification, and a large one proves a short pending run is tolerated.
func lostWorkAtomicPollFixture(eng engine.Engine, trustBudget time.Duration) (*LocalClient, *storage.Apply, []*storage.Task, *stateRecordingTaskStore) {
	return lostWorkAtomicPollFixtureInState(eng, trustBudget, state.Task.Running)
}

// lostWorkAtomicPollFixtureInState is lostWorkAtomicPollFixture with the
// tasks' stored state chosen by the caller, for the states whose settlement
// rules differ.
func lostWorkAtomicPollFixtureInState(eng engine.Engine, trustBudget time.Duration, taskState string) (*LocalClient, *storage.Apply, []*storage.Task, *stateRecordingTaskStore) {
	apply := &storage.Apply{
		ID: 1, PlanID: 7, ApplyIdentifier: "apply-1", Database: "appdb",
		DatabaseType: storage.DatabaseTypeVitess, Environment: "staging",
		State: state.Apply.Running,
	}
	tasks := []*storage.Task{
		{
			ID: 1, ApplyID: 1, PlanID: 7, TaskIdentifier: "task-orders",
			Database: "appdb", DatabaseType: storage.DatabaseTypeVitess,
			Namespace: "appdb", TableName: "orders",
			Environment: "staging", State: taskState,
		},
		{
			ID: 2, ApplyID: 1, PlanID: 7, TaskIdentifier: "task-payments",
			Database: "appdb", DatabaseType: storage.DatabaseTypeVitess,
			Namespace: "appdb", TableName: "payments",
			Environment: "staging", State: taskState,
		},
	}
	taskStore := &stateRecordingTaskStore{
		exactProgressTaskStore: &exactProgressTaskStore{tasks: tasks},
	}
	client := &LocalClient{
		config:            LocalConfig{Database: "appdb", Type: storage.DatabaseTypeVitess},
		planetscaleEngine: eng,
		storage: &exactProgressStorage{
			applies:         &snapshotApplyStore{stored: *apply},
			tasks:           taskStore,
			controlRequests: &testControlRequestStore{},
			logs:            &mockApplyLogStore{},
			plans:           &scriptedPlanStore{plan: &storage.Plan{ID: 7, SchemaFiles: schema.SchemaFiles{}}},
		},
		logger:                              slog.Default(),
		taskPollIntervalOverride:            time.Millisecond,
		lostEngineWorkPendingBudgetOverride: trustBudget,
	}
	return client, apply, tasks, taskStore
}

// An engine can complete a grouped apply's work and then lose all record of it
// — after that, every progress poll reports no active schema change while
// durable storage still says the tasks are running. Once the tolerated
// staleness window is exhausted the drive must verify the target directly: a
// target that already has the desired schema means the change landed and only
// its outcome was lost, so every task completes and the apply terminalizes
// instead of polling forever and holding the database's active-apply slot.
func TestPollForCompletionAtomic_LostEngineWorkTargetConverged(t *testing.T) {
	eng := &lostWorkEngine{
		phaseSequenceEngine: phaseSequenceEngine{results: []*engine.ProgressResult{
			{State: engine.StateRunning},
			{State: engine.StatePending},
		}},
		// The re-plan reports no remaining change for any table: the target
		// already has the desired schema.
		planResult: &engine.PlanResult{NoChanges: true},
	}
	client, apply, tasks, _ := lostWorkAtomicPollFixture(eng, lostWorkTrustBudgetReached)

	client.pollForCompletionAtomic(t.Context(), apply, tasks, nil, nil, map[string]string{}, false)

	assert.Equal(t, state.Apply.Completed, apply.State, "a converged target settles the apply through the normal completed flow")
	require.NotNil(t, apply.CompletedAt)
	for _, task := range tasks {
		assert.Equal(t, state.Task.Completed, task.State, "table %s", task.TableName)
		assert.Equal(t, 100, task.ProgressPercent, "table %s", task.TableName)
		require.NotNil(t, task.CompletedAt, "table %s", task.TableName)
	}
	assert.Equal(t, 1, eng.planCalls, "one shared re-plan verifies the target for every task of the grouped apply")
	assert.GreaterOrEqual(t, eng.calls, 3, "a single pending report never settles the apply; the engine is polled again first")
}

// An engine that never had (or irrecoverably lost) grouped work reports no
// active schema change forever while the target still needs some of it. The
// drive settles each task on its own verification verdict: a table whose
// change already landed completes, a table the target still needs rests
// retryable — never permanently failed, because nothing about the target is
// broken — and the apply pauses for a fresh claim to re-drive the rest.
func TestPollForCompletionAtomic_LostEngineWorkTargetNotConverged(t *testing.T) {
	eng := &lostWorkEngine{
		phaseSequenceEngine: phaseSequenceEngine{results: []*engine.ProgressResult{
			{State: engine.StatePending},
		}},
		// The re-plan still contains orders but not payments: the target has
		// payments' change and still needs orders'.
		planResult: &engine.PlanResult{Changes: []engine.SchemaChange{{
			Namespace: "appdb",
			TableChanges: []engine.TableChange{{
				Table: "orders",
				DDL:   "ALTER TABLE `orders` ADD COLUMN `note` VARCHAR(255)",
			}},
		}}},
	}
	client, apply, tasks, _ := lostWorkAtomicPollFixture(eng, lostWorkTrustBudgetReached)

	client.pollForCompletionAtomic(t.Context(), apply, tasks, nil, nil, map[string]string{}, false)

	assert.Equal(t, state.Apply.FailedRetryable, apply.State, "a lost change the target still needs pauses the apply for retry, never fails it permanently")
	assert.Nil(t, apply.CompletedAt, "a retryable apply carries no completion timestamp")
	orders, payments := tasks[0], tasks[1]
	assert.Equal(t, state.Task.FailedRetryable, orders.State, "the table the target still needs is retryable")
	assert.Contains(t, orders.ErrorMessage, "orders")
	assert.Contains(t, orders.ErrorMessage, "still needs the change")
	assert.Nil(t, orders.CompletedAt, "a retryable task carries no completion timestamp")
	assert.Equal(t, state.Task.Completed, payments.State, "the table whose change already landed completes")
	require.NotNil(t, payments.CompletedAt)
}

// When the engine reports no active schema change and the target plan cannot
// be read either, neither side can answer what happened to the work. The drive
// must not spin between the two forever: each failed verification counts
// against the same bounded error budget as a failed poll, and exhausting it
// pauses the apply retryable for a fresh claim to re-drive — never permanently
// failed, because nothing proved the target is broken.
func TestPollForCompletionAtomic_LostEngineWorkVerificationErrorsAreBounded(t *testing.T) {
	eng := &lostWorkEngine{
		phaseSequenceEngine: phaseSequenceEngine{results: []*engine.ProgressResult{
			{State: engine.StatePending},
		}},
		planResult: &engine.PlanResult{NoChanges: true},
	}
	client, apply, tasks, _ := lostWorkAtomicPollFixture(eng, lostWorkTrustBudgetReached)
	client.storage.(*exactProgressStorage).plans = &scriptedPlanStore{err: fmt.Errorf("storage read failed")}

	client.pollForCompletionAtomic(t.Context(), apply, tasks, nil, nil, map[string]string{}, false)

	assert.Equal(t, state.Apply.FailedRetryable, apply.State, "an unverifiable target pauses the apply retryable, never permanently failed")
	assert.Contains(t, apply.ErrorMessage, "could not be verified")
	assert.Contains(t, apply.ErrorMessage, "consecutive errors")
	for _, task := range tasks {
		assert.Equal(t, state.Task.FailedRetryable, task.State, "table %s", task.TableName)
		assert.Nil(t, task.CompletedAt, "a retryable task carries no completion timestamp (table %s)", task.TableName)
	}
	assert.Equal(t, 0, eng.planCalls, "a failed plan read settles nothing; the engine re-plan is never reached")
}

// A freshly restarted engine can serve a stale snapshot that omits in-flight
// grouped work for a few polls before it catches up. A short run of pending
// reports inside the tolerated window must self-heal: the drive keeps polling,
// never distrusts the engine, and the apply completes through the normal flow.
func TestPollForCompletionAtomic_StaleEngineSnapshotSelfHeals(t *testing.T) {
	eng := &lostWorkEngine{
		phaseSequenceEngine: phaseSequenceEngine{results: []*engine.ProgressResult{
			{State: engine.StatePending},
			{State: engine.StatePending},
			{State: engine.StatePending},
			{State: engine.StateRunning},
			{State: engine.StateCompleted},
		}},
		planResult: &engine.PlanResult{NoChanges: true},
	}
	client, apply, tasks, taskStore := lostWorkAtomicPollFixture(eng, lostWorkTrustBudgetAmple)

	client.pollForCompletionAtomic(t.Context(), apply, tasks, nil, nil, map[string]string{}, false)

	assert.Equal(t, state.Apply.Completed, apply.State)
	for _, task := range tasks {
		assert.Equal(t, state.Task.Completed, task.State, "table %s", task.TableName)
	}
	assert.Equal(t, 0, eng.planCalls, "a self-healing stale snapshot never triggers target verification")
	assert.NotContains(t, taskStore.states, state.Task.FailedRetryable)
	assert.NotContains(t, taskStore.states, state.Task.Failed)
}

// Settlement touches only the tasks whose stored state says work is in
// flight. A task that already reached its terminal state — here a table that
// cut over and completed before the engine lost the rest of the apply — is a
// durable final answer: the target read must not re-settle it or rewrite its
// completion record.
func TestPollForCompletionAtomic_LostEngineWorkLeavesSettledTasksUntouched(t *testing.T) {
	eng := &lostWorkEngine{
		phaseSequenceEngine: phaseSequenceEngine{results: []*engine.ProgressResult{
			{State: engine.StatePending},
		}},
		// The re-plan still contains orders: only the in-flight task is settled
		// from it; the already-completed payments is not consulted at all.
		planResult: &engine.PlanResult{Changes: []engine.SchemaChange{{
			Namespace: "appdb",
			TableChanges: []engine.TableChange{{
				Table: "orders",
				DDL:   "ALTER TABLE `orders` ADD COLUMN `note` VARCHAR(255)",
			}},
		}}},
	}
	client, apply, tasks, _ := lostWorkAtomicPollFixture(eng, lostWorkTrustBudgetReached)
	orders, payments := tasks[0], tasks[1]
	completedEarlier := time.Now().Add(-time.Hour)
	payments.State = state.Task.Completed
	payments.CompletedAt = &completedEarlier

	client.pollForCompletionAtomic(t.Context(), apply, tasks, nil, nil, map[string]string{}, false)

	assert.Equal(t, state.Task.FailedRetryable, orders.State, "the in-flight task still settles from the target read")
	assert.Equal(t, state.Task.Completed, payments.State, "a terminal task is never re-settled")
	require.NotNil(t, payments.CompletedAt)
	assert.Equal(t, completedEarlier, *payments.CompletedAt, "a terminal task's completion record is never rewritten by settlement")
	assert.Equal(t, state.Apply.FailedRetryable, apply.State)
}

// Once a grouped schema change has cut over, the live schema matches the
// reviewed target whether or not the revert that was undoing it ever ran — so
// tasks in their revert phase can never be settled by reading the target. An
// engine that loses a revert must leave them retryable for a fresh claim to
// re-drive; completing them would report the apply as a successful schema
// change while the change it was reverting is still in place.
func TestPollForCompletionAtomic_LostEngineWorkNeverCompletesRevertPhaseTasks(t *testing.T) {
	eng := &lostWorkEngine{
		phaseSequenceEngine: phaseSequenceEngine{results: []*engine.ProgressResult{
			{State: engine.StatePending},
		}},
		// A converged target is exactly what a post-cutover re-plan reports, and
		// it must not be read as the revert having finished.
		planResult: &engine.PlanResult{NoChanges: true},
	}
	client, apply, tasks, _ := lostWorkAtomicPollFixtureInState(eng, lostWorkTrustBudgetReached, state.Task.Reverting)

	client.pollForCompletionAtomic(t.Context(), apply, tasks, nil, nil, map[string]string{}, false)

	assert.Equal(t, state.Apply.FailedRetryable, apply.State, "a lost revert pauses the apply for retry, never completes it")
	for _, task := range tasks {
		assert.Equal(t, state.Task.FailedRetryable, task.State, "a lost revert is retryable, never a completed schema change (table %s)", task.TableName)
		assert.Nil(t, task.CompletedAt, "a retryable task carries no completion timestamp (table %s)", task.TableName)
		assert.Contains(t, task.ErrorMessage, "revert phase")
	}
	assert.Equal(t, 0, eng.planCalls, "the target schema is never consulted for revert-phase tasks")
}
