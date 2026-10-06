package tern

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// heldTargetEngine refuses the target while another run holds it: each
// accepted Apply is followed by a progress report, the first heldAttempts of
// them held and the rest completed. Plan returns what the target looks like
// once the holder lets go.
type heldTargetEngine struct {
	engine.Engine
	heldAttempts int
	plan         *engine.PlanResult
	planErr      error

	applies   int
	planCalls int
}

func (e *heldTargetEngine) Name() string { return "held-target" }

func (e *heldTargetEngine) Apply(context.Context, *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.applies++
	return &engine.ApplyResult{Accepted: true}, nil
}

func (e *heldTargetEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	if e.applies <= e.heldAttempts {
		return &engine.ProgressResult{
			State: engine.StateFailed, TargetHeld: true,
			ErrorMessage: "could not acquire advisory lock: lock is held by another connection",
		}, nil
	}
	return &engine.ProgressResult{State: engine.StateCompleted}, nil
}

func (e *heldTargetEngine) Plan(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) {
	e.planCalls++
	return e.plan, e.planErr
}

const heldTaskDDL = "ALTER TABLE `orders` ADD COLUMN `note` VARCHAR(255)"

// heldTaskReplan is a re-plan that still asks for ddl on the task's table.
func heldTaskReplan(ddl string) *engine.PlanResult {
	return &engine.PlanResult{Changes: []engine.SchemaChange{{
		Namespace:    "appdb_sharded",
		TableChanges: []engine.TableChange{{Table: "orders", DDL: ddl}},
	}}}
}

// runHeldTask drives one whole-namespace task through runEngineTask against
// eng.
func runHeldTask(t *testing.T, eng *heldTargetEngine) (taskAction, *storage.Task) {
	t.Helper()
	client, apply, task, _ := lostWorkPollFixture(eng, lostWorkTrustBudgetAmple)
	task.Shard = ""
	task.DDL = heldTaskDDL
	plan := &storage.Plan{ID: 7}
	action := client.runEngineTask(t.Context(), apply, task, plan, []*storage.Task{task}, nil)
	return action, task
}

// A drive waiting for another run to release the table can find, once it
// does, that the holder ran this same change to its cutover — a driver that
// lost the apply leaves exactly that behind. The task is settled from the
// re-plan instead of running its statement a second time against a table
// that already has it.
func TestRunEngineTask_HeldTargetThatLandedTheChangeSettlesTheTask(t *testing.T) {
	eng := &heldTargetEngine{heldAttempts: 1, plan: &engine.PlanResult{NoChanges: true}}

	action, task := runHeldTask(t, eng)

	assert.Equal(t, taskContinue, action)
	assert.Equal(t, 1, eng.applies, "the statement is not started again once the table has it")
	assert.Equal(t, 1, eng.planCalls, "the target is re-planned before the task could start again")
	assert.Equal(t, state.Task.Completed, task.State)
	assert.Equal(t, 100, task.ProgressPercent)
	require.NotNil(t, task.CompletedAt)
}

// A table still needing the task's reviewed statement once the holder lets go
// is the ordinary case: the task starts again and runs to completion.
func TestRunEngineTask_HeldTargetStillNeedingTheChangeStartsItAgain(t *testing.T) {
	eng := &heldTargetEngine{heldAttempts: 1, plan: heldTaskReplan(heldTaskDDL)}

	action, task := runHeldTask(t, eng)

	assert.Equal(t, taskContinue, action)
	assert.Equal(t, 2, eng.applies, "the task starts again after the re-plan confirms it is still needed")
	assert.Equal(t, 1, eng.planCalls)
	assert.Equal(t, state.Task.Completed, task.State)
}

// While the drive waited, another schema change may have reshaped the table,
// so the reviewed statement is no longer what the target needs. The task
// fails rather than run a statement planned against the table's old shape,
// and the failure reads in operator terms without echoing either statement.
func TestRunEngineTask_HeldTargetWhoseReplanDroppedTheStatementFails(t *testing.T) {
	eng := &heldTargetEngine{heldAttempts: 1, plan: heldTaskReplan("ALTER TABLE `orders` ADD COLUMN `memo` TEXT")}

	action, task := runHeldTask(t, eng)

	assert.Equal(t, taskFailed, action)
	assert.Equal(t, 1, eng.applies, "the stale statement is never started again")
	assert.Equal(t, state.Task.Failed, task.State)
	assert.Contains(t, task.ErrorMessage, "a fresh plan no longer includes this task's statement")
	assert.NotContains(t, task.ErrorMessage, "memo")
	assert.NotContains(t, task.ErrorMessage, "ALTER TABLE")
}

// A target that cannot be re-planned is unverified, so the task never starts
// again on it. Once the failures use up the poll's error budget the drive
// exits with the apply still active and no failure recorded on the task.
func TestRunEngineTask_HeldTargetThatCannotBeReplannedIsNotStarted(t *testing.T) {
	eng := &heldTargetEngine{heldAttempts: 1, planErr: errors.New("dial tcp: connection refused")}

	action, task := runHeldTask(t, eng)

	assert.Equal(t, taskAbort, action)
	assert.Equal(t, 1, eng.applies, "an unverified target is never started again")
	assert.Equal(t, maxConsecutiveProgressPollErrors, eng.planCalls)
	assert.Equal(t, state.Task.Running, task.State, "no failure verdict is written for an unverified target")
}

// A table that stays held is waited on for a bounded time. Past the cap the
// drive exits with the apply still active and nothing spent from its retry
// budget, and the next claim re-plans the target the way any reclaim does.
func TestRunEngineTask_HeldTargetPastTheWaitCapHandsTheApplyBack(t *testing.T) {
	eng := &heldTargetEngine{heldAttempts: 1 << 30, plan: heldTaskReplan(heldTaskDDL)}
	client, apply, task, recording := lostWorkPollFixture(eng, lostWorkTrustBudgetAmple)
	task.Shard = ""
	task.DDL = heldTaskDDL

	action := client.runEngineTask(t.Context(), apply, task, &storage.Plan{ID: 7}, []*storage.Task{task}, nil)

	assert.Equal(t, taskHandover, action)
	assert.Equal(t, client.targetHeldMaxWaits()+1, eng.applies, "one attempt per wait, then the drive stops waiting")
	assert.Equal(t, state.Task.Running, task.State)
	assert.NotContains(t, recording.states, state.Task.FailedRetryable, "a held table never spends the retry budget")
	assert.NotContains(t, recording.states, state.Task.Failed)
}
