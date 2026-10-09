package tern

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// replanTargetEngine accepts every Apply and reports it completed. Plan
// returns what the target looks like when the drive re-plans it.
type replanTargetEngine struct {
	engine.Engine
	plan    *engine.PlanResult
	planErr error

	applies   int
	planCalls int
}

func (e *replanTargetEngine) Name() string { return "replan-target" }

func (e *replanTargetEngine) Apply(context.Context, *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.applies++
	return &engine.ApplyResult{Accepted: true}, nil
}

func (e *replanTargetEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	return &engine.ProgressResult{State: engine.StateCompleted}, nil
}

func (e *replanTargetEngine) Plan(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) {
	e.planCalls++
	return e.plan, e.planErr
}

const resumeTaskDDL = "ALTER TABLE `orders` ADD COLUMN `note` VARCHAR(255)"

// resumeTaskReplan is a re-plan that still asks for ddl on the task's table.
func resumeTaskReplan(ddl string) *engine.PlanResult {
	return &engine.PlanResult{Changes: []engine.SchemaChange{{
		Namespace:    "appdb_sharded",
		TableChanges: []engine.TableChange{{Table: "orders", DDL: ddl}},
	}}}
}

// A shard-scoped dispatch tags its tasks with their shard, while a re-plan of
// the reviewed schema set describes each namespace as a unit. The verdict
// reads the unit's statements as the shard's, and never reads the unit's
// silence as the shard having the change.
func TestReplanVerdictForTask_ShardTaskUnderANamespaceUnitReplan(t *testing.T) {
	task := &storage.Task{Namespace: "appdb_sharded", Shard: "-40", TableName: "orders"}
	unitKey := shardTableKey{namespace: "appdb_sharded", table: "orders"}
	shardKey := shardTableKey{namespace: "appdb_sharded", shard: "-40", table: "orders"}

	verdict, key := replanVerdictForTask(map[shardTableKey][]string{unitKey: {resumeTaskDDL}}, false, task)
	assert.Equal(t, replanNeedsChange, verdict, "the unit still owing the table is reason to run the reviewed statement")
	assert.Equal(t, unitKey, key)

	verdict, _ = replanVerdictForTask(map[shardTableKey][]string{}, false, task)
	assert.Equal(t, replanCannotAttribute, verdict, "the unit's silence is not evidence about this shard")

	verdict, key = replanVerdictForTask(map[shardTableKey][]string{shardKey: {resumeTaskDDL}}, false, task)
	assert.Equal(t, replanNeedsChange, verdict)
	assert.Equal(t, shardKey, key)

	otherShard := shardTableKey{namespace: "appdb_sharded", shard: "40-", table: "orders"}
	verdict, _ = replanVerdictForTask(map[shardTableKey][]string{otherShard: {resumeTaskDDL}}, false, task)
	assert.Equal(t, replanChangeLanded, verdict, "a re-plan that keys by shard and omits this one speaks for it")
}

// shardTaskFixture is a resume of one shard-tagged task whose reviewed
// statement the engine runs, against a re-plan that describes the namespace as
// a unit.
func shardTaskFixture(eng engine.Engine) (*LocalClient, *storage.Apply, *storage.Task) {
	client, apply, task, _ := lostWorkPollFixtureInState(eng, lostWorkTrustBudgetAmple, state.Task.Pending)
	client.heartbeatInterval = 10 * time.Second
	client.storage.(*exactProgressStorage).applies = &mockApplyStore{apply: apply}
	task.DDL = resumeTaskDDL
	return client, apply, task
}

// unattributableStartLog is the timeline line a resume writes when it runs a
// task's reviewed statement without evidence of whether the change landed.
const unattributableStartLog = "could not tell whether table orders on shard"

// captureApplyLogs records the timeline lines the client writes.
func captureApplyLogs(client *LocalClient) *capturingApplyLogStore {
	logs := &capturingApplyLogStore{}
	client.storage.(*exactProgressStorage).logs = logs
	return logs
}

// assertTimelineMentions asserts a timeline line contains text.
func assertTimelineMentions(t *testing.T, logs *capturingApplyLogStore, text string) {
	t.Helper()
	for _, entry := range logs.entries {
		if strings.Contains(entry.Message, text) {
			assert.Equal(t, storage.LogLevelWarn, entry.Level)
			return
		}
	}
	assert.Failf(t, "timeline line missing", "no apply log contains %q", text)
}

// A resume never completes a shard's task because a namespace-unit re-plan no
// longer mentions the table: that silence says nothing about the shard. The
// task stays active with its reviewed statement, so the engine decides its
// outcome rather than the resume reporting a change it never saw made.
func TestReplanAndFilterTasks_ShardTaskIsNotCompletedByTheUnitsSilence(t *testing.T) {
	eng := &replanTargetEngine{plan: &engine.PlanResult{NoChanges: true}}
	client, apply, task := shardTaskFixture(eng)
	logs := captureApplyLogs(client)

	rp, err := client.replanAndFilterTasks(t.Context(), apply, []*storage.Task{task}, &storage.Plan{ID: 7})

	require.NoError(t, err)
	assert.Zero(t, rp.CompletedCount)
	require.Len(t, rp.ActiveTasks, 1)
	assert.Equal(t, resumeTaskDDL, rp.ActiveTasks[0].DDL, "the reviewed statement is kept")
	assert.Equal(t, state.Task.Pending, task.State)
	assertTimelineMentions(t, logs, unattributableStartLog)
}

// A namespace-unit re-plan that still lists the shard task's table is checked
// against the reviewed statement like any other, so drift on the unit fails
// the resume closed instead of being run on the shard.
func TestReplanAndFilterTasks_ShardTaskIsVerifiedAgainstTheUnitsStatements(t *testing.T) {
	eng := &replanTargetEngine{plan: resumeTaskReplan(resumeTaskDDL)}
	client, apply, task := shardTaskFixture(eng)
	rp, err := client.replanAndFilterTasks(t.Context(), apply, []*storage.Task{task}, &storage.Plan{ID: 7})
	require.NoError(t, err)
	require.Len(t, rp.ActiveTasks, 1)
	assert.Equal(t, resumeTaskDDL, rp.ActiveTasks[0].DDL)

	eng = &replanTargetEngine{plan: resumeTaskReplan("ALTER TABLE `orders` ADD COLUMN `memo` TEXT")}
	client, apply, task = shardTaskFixture(eng)
	_, err = client.replanAndFilterTasks(t.Context(), apply, []*storage.Task{task}, &storage.Plan{ID: 7})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "drifted from the reviewed plan")
}

// A namespace-unit re-plan that refuses the statement it still lists is the
// target's verdict on the shard task that will run it, so the resumed drive's
// blocked-row gate refuses the task rather than sending it to the engine.
func TestReplanAndFilterTasks_ShardTaskTakesTheUnitsRefusal(t *testing.T) {
	plan := resumeTaskReplan(resumeTaskDDL)
	plan.Changes[0].TableChanges[0].ExecutionMode = engine.ExecutionModeBlocked
	plan.Changes[0].TableChanges[0].ModeReason = "table exceeds the direct-execution bound"
	eng := &replanTargetEngine{plan: plan}
	client, apply, task := shardTaskFixture(eng)

	rp, err := client.replanAndFilterTasks(t.Context(), apply, []*storage.Task{task}, &storage.Plan{ID: 7})

	require.NoError(t, err)
	require.Len(t, rp.ActiveTasks, 1)
	assert.Equal(t, engine.ExecutionModeBlocked, task.ExecutionMode)
	assert.Equal(t, "table exceeds the direct-execution bound", task.ModeReason)
}

// siblingStatementDDL is the reviewed statement of a second task on the same
// shard and table.
const siblingStatementDDL = "ALTER TABLE `orders` ADD INDEX `idx_created` (`created_at`)"

// withPendingSibling adds a second pending task for the same shard and table,
// reviewed with siblingStatementDDL.
func withPendingSibling(task *storage.Task) []*storage.Task {
	sibling := *task
	sibling.ID = task.ID + 1
	sibling.TaskIdentifier = task.TaskIdentifier + "-sibling"
	sibling.DDL = siblingStatementDDL
	sibling.State = state.Task.Pending
	return []*storage.Task{task, &sibling}
}

// A namespace-unit re-plan that lists only a sibling's statement for the
// table says the namespace still owes that statement. It says nothing about
// whether this task's statement reached the shard, so the task stays active
// with its reviewed statement rather than being settled as landed.
func TestReplanAndFilterTasks_ShardTaskIsNotSettledByTheUnitsSiblingStatement(t *testing.T) {
	eng := &replanTargetEngine{plan: resumeTaskReplan(siblingStatementDDL)}
	client, apply, task := shardTaskFixture(eng)
	logs := captureApplyLogs(client)
	tasks := withPendingSibling(task)

	rp, err := client.replanAndFilterTasks(t.Context(), apply, tasks, &storage.Plan{ID: 7})

	require.NoError(t, err)
	assert.Zero(t, rp.CompletedCount, "no task is settled on the unit's statements")
	require.Len(t, rp.ActiveTasks, 2)
	assert.Equal(t, resumeTaskDDL, rp.ActiveTasks[0].DDL, "the task keeps its reviewed statement")
	assert.Equal(t, siblingStatementDDL, rp.ActiveTasks[1].DDL)
	assert.Equal(t, state.Task.Pending, task.State)
	assertTimelineMentions(t, logs, unattributableStartLog)
}

// The sequential resume runs a shard task with its reviewed statement when a
// namespace-unit re-plan lists only a sibling's statement for the table.
func TestResumeApplySequential_ShardTaskRunsWhenTheUnitListsOnlyASibling(t *testing.T) {
	eng := &replanTargetEngine{plan: resumeTaskReplan(siblingStatementDDL)}
	client, apply, task := shardTaskFixture(eng)
	logs := captureApplyLogs(client)
	tasks := withPendingSibling(task)
	client.storage.(*exactProgressStorage).tasks.(*stateRecordingTaskStore).tasks = tasks

	err := client.resumeApplySequential(t.Context(), apply, tasks, &storage.Plan{ID: 7}, nil)

	require.NoError(t, err)
	assert.Equal(t, 2, eng.applies, "the engine runs both tasks and decides their outcomes")
	assert.Equal(t, state.Task.Completed, task.State)
	assert.Equal(t, state.Task.Completed, tasks[1].State)
	assertTimelineMentions(t, logs, unattributableStartLog)
}

// The sequential resume re-plans each table right before it runs. A target
// that cannot be re-planned is unverified, so the resume returns the failure
// without starting the task, and the apply stays active for a later drive.
func TestResumeApplySequential_UnverifiedTargetIsNotStarted(t *testing.T) {
	eng := &replanTargetEngine{planErr: errors.New("dial tcp: connection refused")}
	client, apply, task := shardTaskFixture(eng)
	task.Shard = ""

	err := client.resumeApplySequential(t.Context(), apply, []*storage.Task{task}, &storage.Plan{ID: 7}, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
	assert.Zero(t, eng.applies, "the task is never started on an unverified target")
	assert.Equal(t, state.Task.Pending, task.State)
}

// A shard task the namespace-unit re-plan no longer mentions is run with its
// reviewed statement on resume rather than settled from that silence.
func TestResumeApplySequential_ShardTaskRunsWhenTheUnitIsSilent(t *testing.T) {
	eng := &replanTargetEngine{plan: &engine.PlanResult{NoChanges: true}}
	client, apply, task := shardTaskFixture(eng)
	logs := captureApplyLogs(client)

	err := client.resumeApplySequential(t.Context(), apply, []*storage.Task{task}, &storage.Plan{ID: 7}, nil)

	require.NoError(t, err)
	assert.Equal(t, 1, eng.applies, "the engine runs the task and decides its outcome")
	assert.Equal(t, state.Task.Completed, task.State)
	assertTimelineMentions(t, logs, unattributableStartLog)
}

// shardKeyedReplanEngine is a replanTargetEngine that declares its Plan lists
// every shard that still needs a change.
type shardKeyedReplanEngine struct {
	*replanTargetEngine
}

func (shardKeyedReplanEngine) PlansEachShard() bool { return true }

// An engine that plans each shard on its own leaves out every shard that
// already has the change, and leaves out a namespace whose every shard has it.
// Its re-plan therefore speaks for a shard it does not mention, even when it
// mentions no shard of the namespace at all.
func TestReplanVerdictForTask_ShardTaskUnderAShardKeyedReplan(t *testing.T) {
	task := &storage.Task{Namespace: "appdb_sharded", Shard: "-40", TableName: "orders"}
	shardKey := shardTableKey{namespace: "appdb_sharded", shard: "-40", table: "orders"}

	verdict, _ := replanVerdictForTask(map[shardTableKey][]string{}, true, task)
	assert.Equal(t, replanChangeLanded, verdict, "a namespace the plan does not mention has the change on every shard")

	verdict, key := replanVerdictForTask(map[shardTableKey][]string{shardKey: {resumeTaskDDL}}, true, task)
	assert.Equal(t, replanNeedsChange, verdict)
	assert.Equal(t, shardKey, key)

	otherNamespace := shardTableKey{namespace: "appdb_other", shard: "-40", table: "orders"}
	verdict, _ = replanVerdictForTask(map[shardTableKey][]string{otherNamespace: {resumeTaskDDL}}, true, task)
	assert.Equal(t, replanChangeLanded, verdict, "another namespace still owing the table says nothing against this one")
}

// A resume of a shard task whose change already landed, on an engine that
// plans each shard on its own, settles the task as completed and never runs
// its statement again. Every shard of the namespace has the change, so the
// re-plan does not mention the namespace at all. Running the statement again
// is not safe to rely on failing: adding an unnamed index or foreign key a
// second time succeeds and leaves the shard with two.
func TestResumeApplySequential_ShardTaskSettlesWhenTheEnginePlansEachShard(t *testing.T) {
	eng := &replanTargetEngine{plan: &engine.PlanResult{NoChanges: true}}
	client, apply, task := shardTaskFixture(shardKeyedReplanEngine{eng})
	logs := captureApplyLogs(client)

	err := client.resumeApplySequential(t.Context(), apply, []*storage.Task{task}, &storage.Plan{ID: 7}, nil)

	require.NoError(t, err)
	assert.Zero(t, eng.applies, "the landed statement is not run again")
	assert.Equal(t, state.Task.Completed, task.State)
	assert.NotContains(t, timelineMessages(logs), unattributableStartLog)
}

// The resume-entry re-plan settles the same task the same way, so a grouped
// resume does not keep it active either.
func TestReplanAndFilterTasks_ShardTaskSettlesWhenTheEnginePlansEachShard(t *testing.T) {
	eng := &replanTargetEngine{plan: &engine.PlanResult{NoChanges: true}}
	client, apply, task := shardTaskFixture(shardKeyedReplanEngine{eng})

	rp, err := client.replanAndFilterTasks(t.Context(), apply, []*storage.Task{task}, &storage.Plan{ID: 7})

	require.NoError(t, err)
	assert.Equal(t, int64(1), rp.CompletedCount)
	assert.Empty(t, rp.ActiveTasks)
	assert.Equal(t, state.Task.Completed, task.State)
}

// timelineMessages joins every timeline line the client wrote.
func timelineMessages(logs *capturingApplyLogStore) string {
	messages := make([]string, 0, len(logs.entries))
	for _, entry := range logs.entries {
		messages = append(messages, entry.Message)
	}
	return strings.Join(messages, "\n")
}

// blockedDrainEngine is an engine whose in-process work from an earlier drive
// never exits, so draining it waits until the caller gives up.
type blockedDrainEngine struct {
	*replanTargetEngine
	draining chan struct{}
}

func (e *blockedDrainEngine) DrainContext(ctx context.Context) error {
	close(e.draining)
	<-ctx.Done()
	return ctx.Err()
}

// A resume waits for in-process engine work to exit before it reads the
// target, and that work can be a run an earlier drive left behind that never
// ends. The wait lasts only as long as the drive's claim: a drive cancelled
// while it waits returns promptly and hands the apply back without
// re-planning the target or starting the task.
func TestResumeApplySequential_CancelledDriveStopsWaitingOnEngineWork(t *testing.T) {
	target := &replanTargetEngine{plan: resumeTaskReplan(resumeTaskDDL)}
	eng := &blockedDrainEngine{replanTargetEngine: target, draining: make(chan struct{})}
	client, apply, task, _ := lostWorkPollFixtureInState(eng, lostWorkTrustBudgetAmple, state.Task.Pending)
	client.heartbeatInterval = 10 * time.Second
	task.Shard = ""
	task.DDL = resumeTaskDDL
	driveCtx, cancelDrive := context.WithCancel(t.Context())
	defer cancelDrive()

	returned := make(chan error, 1)
	go func() {
		returned <- client.resumeApplySequential(driveCtx, apply, []*storage.Task{task}, &storage.Plan{ID: 7}, nil)
	}()
	select {
	case <-eng.draining:
	case <-time.After(resumeTestDeadline):
		require.FailNow(t, "the resume never waited on the engine work")
	}
	cancelDrive()

	select {
	case err := <-returned:
		require.NoError(t, err, "a cancelled drive hands the apply back")
	case <-time.After(resumeTestDeadline):
		require.FailNow(t, "the cancelled drive kept waiting on the engine work")
	}
	assert.Zero(t, target.planCalls, "the target is not re-planned once the drive is cancelled")
	assert.Zero(t, target.applies, "the task is not started once the drive is cancelled")
	assert.Equal(t, state.Task.Pending, task.State)
}

// cancellingPlanEngine fails Plan because the drive was cancelled while the
// target was being re-planned.
type cancellingPlanEngine struct {
	*replanTargetEngine
	cancelDrive context.CancelFunc
}

func (e *cancellingPlanEngine) Plan(ctx context.Context, _ *engine.PlanRequest) (*engine.PlanResult, error) {
	e.planCalls++
	e.cancelDrive()
	return nil, fmt.Errorf("read target schema: %w", ctx.Err())
}

// A drive cancelled while it re-plans the target hands the apply back: the
// re-plan's error describes the cancellation, not the target, so the resume
// returns no failure and starts nothing.
func TestResumeApplySequential_CancelledDuringReplanHandsTheApplyBack(t *testing.T) {
	driveCtx, cancelDrive := context.WithCancel(t.Context())
	defer cancelDrive()
	eng := &cancellingPlanEngine{replanTargetEngine: &replanTargetEngine{}, cancelDrive: cancelDrive}
	client, apply, task, _ := lostWorkPollFixtureInState(eng, lostWorkTrustBudgetAmple, state.Task.Pending)
	client.heartbeatInterval = 10 * time.Second
	task.Shard = ""
	task.DDL = resumeTaskDDL

	err := client.resumeApplySequential(driveCtx, apply, []*storage.Task{task}, &storage.Plan{ID: 7}, nil)

	require.NoError(t, err, "a cancelled drive hands the apply back")
	assert.Equal(t, 1, eng.planCalls)
	assert.Zero(t, eng.applies, "the task is not started once the drive is cancelled")
	assert.Equal(t, state.Task.Pending, task.State)
}

// resumeTestDeadline bounds a wait on the drive under test.
const resumeTestDeadline = 5 * time.Second

// cancellingApplyEngine fails Apply because the drive was cancelled while the
// engine was starting the task.
type cancellingApplyEngine struct {
	*replanTargetEngine
	cancelDrive context.CancelFunc
}

func (e *cancellingApplyEngine) Apply(ctx context.Context, _ *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.applies++
	e.cancelDrive()
	return nil, fmt.Errorf("wait for the previous schema change to exit: %w", ctx.Err())
}

// An engine error from a drive that was cancelled while the engine started
// the task describes the cancellation, not the task. The drive hands the
// apply back with no verdict recorded, so the retry budget is not spent.
func TestRunEngineTask_CancelledDriveRecordsNoVerdictForTheEnginesError(t *testing.T) {
	driveCtx, cancelDrive := context.WithCancel(t.Context())
	defer cancelDrive()
	eng := &cancellingApplyEngine{replanTargetEngine: &replanTargetEngine{}, cancelDrive: cancelDrive}
	client, apply, task, recording := lostWorkPollFixture(eng, lostWorkTrustBudgetAmple)
	task.Shard = ""
	task.DDL = resumeTaskDDL

	action := client.runEngineTask(driveCtx, apply, task, &storage.Plan{ID: 7}, []*storage.Task{task}, nil)

	assert.Equal(t, taskHandover, action)
	assert.Equal(t, 1, eng.applies)
	assert.Empty(t, recording.states, "no failure verdict is written for a cancelled drive")
	assert.Empty(t, task.ErrorMessage)
}
