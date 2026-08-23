package tern

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	spiritstatus "github.com/block/spirit/pkg/status"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// resumeRequiringEngine models a sharded engine (Strata): Progress identifies the
// operation to report on from ResumeState.Metadata and errors without it. With it,
// the operation reports completed.
type resumeRequiringEngine struct {
	engine.Engine
	gotResumeState *engine.ResumeState
}

func (e *resumeRequiringEngine) Name() string { return "resume-requiring" }

func (e *resumeRequiringEngine) Progress(_ context.Context, req *engine.ProgressRequest) (*engine.ProgressResult, error) {
	e.gotResumeState = req.ResumeState
	if req.ResumeState == nil || req.ResumeState.Metadata == "" {
		return nil, fmt.Errorf("strata progress: missing resume state")
	}
	return &engine.ProgressResult{State: engine.StateCompleted}, nil
}

// The sequential poll must thread the engine's returned resume state into
// Progress. A sharded engine (Strata) errors without ResumeState.Metadata, so a
// dropped resume state means Progress fails every tick, the apply never reaches a
// terminal state, and it hangs running — holding the database's active-apply slot
// and blocking every later apply. Drive the poll against such an engine and assert
// the task completes and the engine received the resume state.
func TestPollTaskToCompletion_ThreadsResumeState(t *testing.T) {
	task := &storage.Task{
		ID: 1, ApplyID: 1, TaskIdentifier: "task-1",
		Database: "cdb_resolute", DatabaseType: storage.DatabaseTypeStrata,
		Namespace: "cdb_resolute_sharded", TableName: "mutes", Shard: "-40",
		State: state.Task.Running,
	}
	apply := &storage.Apply{
		ID: 1, ApplyIdentifier: "apply-1", Database: "cdb_resolute",
		DatabaseType: storage.DatabaseTypeStrata, Environment: "staging",
	}
	eng := &resumeRequiringEngine{}
	client := &LocalClient{
		config:       LocalConfig{Database: "cdb_resolute", Type: storage.DatabaseTypeStrata},
		customEngine: eng,
		storage: &exactProgressStorage{
			tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
			controlRequests: &testControlRequestStore{},
			logs:            &mockApplyLogStore{},
		},
		logger: slog.Default(),
	}
	resume := &engine.ResumeState{Metadata: "shard-meta"}

	action := client.pollTaskToCompletion(t.Context(), apply, task, nil, resume)

	assert.Equal(t, taskContinue, action, "the task completes once Progress can report on it")
	assert.Equal(t, state.Task.Completed, task.State)
	require.NotNil(t, eng.gotResumeState, "Progress must receive the resume state")
	assert.Equal(t, "shard-meta", eng.gotResumeState.Metadata, "the engine's resume-state metadata is threaded into Progress")
}

// phaseSequenceEngine scripts one ProgressResult per poll, modelling a Spirit
// runner advancing through its post-copy phases. The last result repeats once
// the script is exhausted.
type phaseSequenceEngine struct {
	engine.Engine
	results []*engine.ProgressResult
	calls   int
}

func (e *phaseSequenceEngine) Name() string { return "phase-sequence" }

func (e *phaseSequenceEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	i := e.calls
	if i >= len(e.results) {
		i = len(e.results) - 1
	}
	e.calls++
	return e.results[i], nil
}

func spiritRunningResult(tableState string) *engine.ProgressResult {
	return &engine.ProgressResult{
		State:  engine.StateRunning,
		Tables: []engine.TableProgress{{Table: "mutes", State: tableState}},
	}
}

// stateRecordingTaskStore records every persisted task state so a test can
// assert the exact sequence of stored states across polls.
type stateRecordingTaskStore struct {
	*exactProgressTaskStore
	states []string
}

func (s *stateRecordingTaskStore) Update(_ context.Context, t *storage.Task) error {
	s.states = append(s.states, t.State)
	return nil
}

// A sequential (non-atomic) drive runs one Spirit runner per DDL, and the
// stored task is the single render surface for the CLI and the PR comment. The
// poll must refine a running task into the engine-reported post-copy phase —
// catching up, checksumming, post-checksum — and hold the displayed endgame
// monotonic through engine phases that map back to plain running (index
// restore, table analyze), so an operator watching a default apply sees the
// same phase names an atomic apply shows.
func TestPollTaskToCompletion_RefinesPostCopyPhases(t *testing.T) {
	task := &storage.Task{
		ID: 1, ApplyID: 1, TaskIdentifier: "task-1",
		Database: "appdb", DatabaseType: storage.DatabaseTypeMySQL,
		TableName: "mutes", State: state.Task.Running,
	}
	apply := &storage.Apply{
		ID: 1, ApplyIdentifier: "apply-1", Database: "appdb",
		DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging",
	}
	taskStore := &stateRecordingTaskStore{
		exactProgressTaskStore: &exactProgressTaskStore{tasks: []*storage.Task{task}},
	}
	eng := &phaseSequenceEngine{results: []*engine.ProgressResult{
		spiritRunningResult(spiritstatus.CopyRows.String()),
		spiritRunningResult(spiritstatus.ApplyChangeset.String()),
		spiritRunningResult(spiritstatus.RestoreSecondaryIndexes.String()),
		spiritRunningResult(spiritstatus.Checksum.String()),
		spiritRunningResult(spiritstatus.PostChecksum.String()),
		{State: engine.StateCompleted},
	}}
	client := &LocalClient{
		config:       LocalConfig{Database: "appdb", Type: storage.DatabaseTypeMySQL},
		spiritEngine: eng,
		storage: &exactProgressStorage{
			tasks:           taskStore,
			controlRequests: &testControlRequestStore{},
			logs:            &mockApplyLogStore{},
		},
		logger: slog.Default(),
	}

	action := client.pollTaskToCompletion(t.Context(), apply, task, nil, nil)

	assert.Equal(t, taskContinue, action)
	assert.Equal(t, state.Task.Completed, task.State)
	assert.Equal(t, []string{
		state.Task.Running,      // copyRows
		state.Task.CatchingUp,   // applyChangeset refines the running task
		state.Task.CatchingUp,   // restoreSecondaryIndexes maps to running; monotonic guard holds the phase
		state.Task.Checksumming, // checksum
		state.Task.PostChecksum, // postChecksum
		state.Task.Completed,
	}, taskStore.states)
}

// A drive claim that reattaches to an engine's durable checkpoint must surface
// that in the apply timeline exactly once, even though the engine reports the
// resume flag on every subsequent poll — so an operator reading the timeline
// can tell a resumed copy from a fresh start without the event repeating on
// every tick. Drive the sequential poll against an engine that reports a
// resumed copy across several polls and assert a single timeline event.
func TestPollTaskToCompletion_LogsResumeOnce(t *testing.T) {
	task := &storage.Task{
		ID: 1, ApplyID: 1, TaskIdentifier: "task-1",
		Database: "appdb", DatabaseType: storage.DatabaseTypeMySQL,
		TableName: "mutes", State: state.Task.Running,
	}
	apply := &storage.Apply{
		ID: 1, ApplyIdentifier: "apply-1", Database: "appdb",
		DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging",
	}
	resumedResult := func(engineState engine.State) *engine.ProgressResult {
		return &engine.ProgressResult{
			State:                 engineState,
			ResumedFromCheckpoint: true,
			Tables:                []engine.TableProgress{{Table: "mutes", State: spiritstatus.CopyRows.String()}},
		}
	}
	logs := &mockApplyLogStore{}
	eng := &phaseSequenceEngine{results: []*engine.ProgressResult{
		resumedResult(engine.StateRunning),
		resumedResult(engine.StateRunning),
		resumedResult(engine.StateCompleted),
	}}
	client := &LocalClient{
		config:       LocalConfig{Database: "appdb", Type: storage.DatabaseTypeMySQL},
		spiritEngine: eng,
		storage: &exactProgressStorage{
			tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
			controlRequests: &testControlRequestStore{},
			logs:            logs,
		},
		logger: slog.Default(),
	}

	action := client.pollTaskToCompletion(t.Context(), apply, task, nil, nil)

	assert.Equal(t, taskContinue, action)
	assert.Equal(t, state.Task.Completed, task.State)
	resumeEvents := 0
	for _, entry := range logs.logs {
		if strings.Contains(entry.Message, "resumed from checkpoint") {
			resumeEvents++
		}
	}
	assert.Equal(t, 1, resumeEvents, "the resume flag on every poll records one timeline event per drive claim")
}

// lostWorkEngine scripts progress results like phaseSequenceEngine and answers
// re-plans with a fixed result, modelling an engine whose in-flight work
// vanished: progress reports no active schema change while the target's true
// state is whatever the re-plan reports.
type lostWorkEngine struct {
	phaseSequenceEngine
	planResult *engine.PlanResult
	planCalls  int
}

func (e *lostWorkEngine) Plan(context.Context, *engine.PlanRequest) (*engine.PlanResult, error) {
	e.planCalls++
	return e.planResult, nil
}

// A pending engine report only signals lost work when the stored task is
// genuinely in flight. Resting states — stopped, failed_retryable — have no
// active engine work by design, and terminal states are never re-verified, so
// a pending report for them must not trigger settlement.
func TestEngineReportsLostWork(t *testing.T) {
	cases := []struct {
		name        string
		storedState string
		engineState string
		want        bool
	}{
		{"running task with pending engine report is lost work", state.Task.Running, state.Task.Pending, true},
		{"cutting-over task with pending engine report is lost work", state.Task.CuttingOver, state.Task.Pending, true},
		{"failed_retryable is a resting state, not divergence", state.Task.FailedRetryable, state.Task.Pending, false},
		{"stopped is a resting state, not divergence", state.Task.Stopped, state.Task.Pending, false},
		{"pending stored task has not dispatched work yet", state.Task.Pending, state.Task.Pending, false},
		{"completed task is terminal, never re-verified", state.Task.Completed, state.Task.Pending, false},
		{"running engine report is not lost work", state.Task.Running, state.Task.Running, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, engineReportsLostWork(tc.storedState, tc.engineState))
		})
	}
}

// lostWorkPollFixture builds a running sequential task, its apply, and a
// LocalClient polling the given engine with a fast poll interval, so lost-work
// scenarios can drive many polls without waiting out the real poll cadence.
// The re-plan's target plan row is served by a scripted plan store.
func lostWorkPollFixture(eng engine.Engine) (*LocalClient, *storage.Apply, *storage.Task, *stateRecordingTaskStore) {
	task := &storage.Task{
		ID: 1, ApplyID: 1, PlanID: 7, TaskIdentifier: "task-1",
		Database: "appdb", DatabaseType: storage.DatabaseTypeStrata,
		Namespace: "appdb_sharded", TableName: "orders", Shard: "-40",
		Environment: "staging", State: state.Task.Running,
	}
	apply := &storage.Apply{
		ID: 1, PlanID: 7, ApplyIdentifier: "apply-1", Database: "appdb",
		DatabaseType: storage.DatabaseTypeStrata, Environment: "staging",
	}
	taskStore := &stateRecordingTaskStore{
		exactProgressTaskStore: &exactProgressTaskStore{tasks: []*storage.Task{task}},
	}
	client := &LocalClient{
		config:       LocalConfig{Database: "appdb", Type: storage.DatabaseTypeStrata},
		customEngine: eng,
		storage: &exactProgressStorage{
			tasks:           taskStore,
			controlRequests: &testControlRequestStore{},
			logs:            &mockApplyLogStore{},
			plans:           &scriptedPlanStore{plan: &storage.Plan{ID: 7, SchemaFiles: schema.SchemaFiles{}}},
		},
		logger:                   slog.Default(),
		taskPollIntervalOverride: time.Millisecond,
	}
	return client, apply, task, taskStore
}

// An engine can complete a schema change and then lose all record of it — after
// that, every progress poll reports no active schema change while durable
// storage still says the task is running. Once the tolerated staleness window
// is exhausted the drive must verify the target directly: a target that
// already has the desired schema means the change landed and only its outcome
// was lost, so the task completes instead of polling forever and holding the
// database's active-apply slot.
func TestPollTaskToCompletion_LostEngineWorkTargetConverged(t *testing.T) {
	eng := &lostWorkEngine{
		phaseSequenceEngine: phaseSequenceEngine{results: []*engine.ProgressResult{
			{State: engine.StateRunning},
			{State: engine.StatePending},
		}},
		// The re-plan reports no remaining change for the table: the target
		// already has the desired schema.
		planResult: &engine.PlanResult{NoChanges: true},
	}
	client, apply, task, _ := lostWorkPollFixture(eng)

	action := client.pollTaskToCompletion(t.Context(), apply, task, nil, nil)

	assert.Equal(t, taskContinue, action, "a converged target settles the task through the normal completed flow")
	assert.Equal(t, state.Task.Completed, task.State)
	assert.Equal(t, 100, task.ProgressPercent)
	require.NotNil(t, task.CompletedAt)
	assert.GreaterOrEqual(t, eng.planCalls, 1, "the drive verifies the target schema before completing")
	assert.GreaterOrEqual(t, eng.calls, lostEngineWorkPendingPolls, "the drive tolerates the full staleness window before verifying")
}

// An engine that never had (or irrecoverably lost) the work reports no active
// schema change forever while the target still needs the change. The drive
// must not fail the task permanently — nothing about the target is broken —
// and must not poll forever: it marks the task retryable so a fresh claim
// re-drives the schema change.
func TestPollTaskToCompletion_LostEngineWorkTargetNotConverged(t *testing.T) {
	eng := &lostWorkEngine{
		phaseSequenceEngine: phaseSequenceEngine{results: []*engine.ProgressResult{
			{State: engine.StatePending},
		}},
		// The re-plan still contains this task's (namespace, shard, table):
		// the target does not have the desired schema yet.
		planResult: &engine.PlanResult{Changes: []engine.SchemaChange{{
			Namespace: "appdb_sharded",
			Shard:     engine.Shard{Name: "-40"},
			TableChanges: []engine.TableChange{{
				Table: "orders",
				DDL:   "ALTER TABLE `orders` ADD COLUMN `note` VARCHAR(255)",
			}},
		}}},
	}
	client, apply, task, _ := lostWorkPollFixture(eng)

	action := client.pollTaskToCompletion(t.Context(), apply, task, nil, nil)

	assert.Equal(t, taskFailed, action)
	assert.Equal(t, state.Task.FailedRetryable, task.State, "a lost change the target still needs is retryable, never permanently failed")
	assert.Contains(t, task.ErrorMessage, "orders")
	assert.Contains(t, task.ErrorMessage, "still needs the change")
	assert.Nil(t, task.CompletedAt, "a retryable task carries no completion timestamp")
}

// A freshly restarted engine can serve a stale snapshot that omits in-flight
// work for a few polls before it catches up. A short run of pending reports
// inside the tolerated window must self-heal: the drive keeps polling, never
// distrusts the engine, and the task completes through the normal flow.
func TestPollTaskToCompletion_StaleEngineSnapshotSelfHeals(t *testing.T) {
	results := []*engine.ProgressResult{
		{State: engine.StatePending},
		{State: engine.StatePending},
		{State: engine.StatePending},
		{State: engine.StateRunning},
		{State: engine.StateCompleted},
	}
	eng := &lostWorkEngine{
		phaseSequenceEngine: phaseSequenceEngine{results: results},
		planResult:          &engine.PlanResult{NoChanges: true},
	}
	client, apply, task, taskStore := lostWorkPollFixture(eng)

	action := client.pollTaskToCompletion(t.Context(), apply, task, nil, nil)

	assert.Equal(t, taskContinue, action)
	assert.Equal(t, state.Task.Completed, task.State)
	assert.Equal(t, 0, eng.planCalls, "a self-healing stale snapshot never triggers target verification")
	assert.NotContains(t, taskStore.states, state.Task.FailedRetryable)
	assert.NotContains(t, taskStore.states, state.Task.Failed)
}

// recordingLogHandler collects slog records so a test can assert on the
// warnings a drive emits.
type recordingLogHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingLogHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *recordingLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingLogHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingLogHandler) messages(level slog.Level) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, r := range h.records {
		if r.Level == level {
			out = append(out, r.Message)
		}
	}
	return out
}

// A task can legitimately sit in one state for a long time (a slow copy, a
// throttled engine), but an operator reading the logs must be able to tell a
// slow task from a wedged one. When the stored state and progress fields show
// no movement for a full stall-warning interval the drive emits a warning —
// purely observational: it never changes the task's state, and the task still
// completes normally once the engine finishes.
func TestPollTaskToCompletion_StallWatchdogWarnsWithoutChangingState(t *testing.T) {
	var results []*engine.ProgressResult
	for range 60 {
		results = append(results, spiritRunningResult(spiritstatus.CopyRows.String()))
	}
	results = append(results, &engine.ProgressResult{State: engine.StateCompleted})
	eng := &phaseSequenceEngine{results: results}

	task := &storage.Task{
		ID: 1, ApplyID: 1, TaskIdentifier: "task-1",
		Database: "appdb", DatabaseType: storage.DatabaseTypeMySQL,
		TableName: "mutes", State: state.Task.Running,
	}
	apply := &storage.Apply{
		ID: 1, ApplyIdentifier: "apply-1", Database: "appdb",
		DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging",
	}
	taskStore := &stateRecordingTaskStore{
		exactProgressTaskStore: &exactProgressTaskStore{tasks: []*storage.Task{task}},
	}
	handler := &recordingLogHandler{}
	client := &LocalClient{
		config:       LocalConfig{Database: "appdb", Type: storage.DatabaseTypeMySQL},
		spiritEngine: eng,
		storage: &exactProgressStorage{
			tasks:           taskStore,
			controlRequests: &testControlRequestStore{},
			logs:            &mockApplyLogStore{},
		},
		logger:                        slog.New(handler),
		taskPollIntervalOverride:      time.Millisecond,
		taskStallWarnIntervalOverride: 20 * time.Millisecond,
	}

	action := client.pollTaskToCompletion(t.Context(), apply, task, nil, nil)

	assert.Equal(t, taskContinue, action)
	assert.Equal(t, state.Task.Completed, task.State, "the watchdog is observational; the task still completes normally")
	warns := handler.messages(slog.LevelWarn)
	stallWarns := 0
	for _, msg := range warns {
		if strings.Contains(msg, "stall-warning interval") {
			stallWarns++
		}
	}
	assert.GreaterOrEqual(t, stallWarns, 1, "a motionless task past the interval is warned about")
	assert.NotContains(t, taskStore.states, state.Task.FailedRetryable, "the watchdog never changes task state")
	assert.NotContains(t, taskStore.states, state.Task.Failed, "the watchdog never changes task state")
}

// The stall watchdog warns once per interval, not once per poll, and any
// movement in the observed state or progress fields resets both the stall
// clock and the warning latch.
func TestTaskStallWatchdogWarnsOncePerInterval(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	w := taskStallWatchdog{interval: time.Minute}
	running := taskProgressSnapshot{state: state.Task.Running, rowsCopied: 10}

	_, warn := w.observe(base, running)
	assert.False(t, warn, "the first observation only arms the watchdog")

	_, warn = w.observe(base.Add(30*time.Second), running)
	assert.False(t, warn, "no warning inside the interval")

	stalledFor, warn := w.observe(base.Add(61*time.Second), running)
	assert.True(t, warn, "a full motionless interval warns")
	assert.GreaterOrEqual(t, stalledFor, time.Minute)

	_, warn = w.observe(base.Add(90*time.Second), running)
	assert.False(t, warn, "the warning does not repeat until another full interval passes")

	stalledFor, warn = w.observe(base.Add(122*time.Second), running)
	assert.True(t, warn, "a second full interval warns again")
	assert.GreaterOrEqual(t, stalledFor, 2*time.Minute)

	advanced := running
	advanced.rowsCopied = 500
	_, warn = w.observe(base.Add(123*time.Second), advanced)
	assert.False(t, warn, "progress movement resets the stall clock")

	_, warn = w.observe(base.Add(150*time.Second), advanced)
	assert.False(t, warn, "the reset clock starts a fresh interval")
}

// permanentProgressErrorEngine always fails Progress with a permanent error.
type permanentProgressErrorEngine struct{ engine.Engine }

func (permanentProgressErrorEngine) Name() string { return "permanent-error" }
func (permanentProgressErrorEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	return nil, engine.NewPermanentError("deploy request not found")
}

// A permanent progress error fails the task immediately rather than waiting out
// the consecutive-error budget, matching the grouped poll.
func TestPollTaskToCompletion_PermanentErrorFailsFast(t *testing.T) {
	task := &storage.Task{
		ID: 1, ApplyID: 1, TaskIdentifier: "task-1",
		Database: "cdb_resolute", DatabaseType: storage.DatabaseTypeStrata,
		Namespace: "cdb_resolute_sharded", TableName: "mutes", Shard: "-40",
		State: state.Task.Running,
	}
	apply := &storage.Apply{
		ID: 1, ApplyIdentifier: "apply-1", Database: "cdb_resolute",
		DatabaseType: storage.DatabaseTypeStrata, Environment: "staging",
	}
	client := &LocalClient{
		config:       LocalConfig{Database: "cdb_resolute", Type: storage.DatabaseTypeStrata},
		customEngine: permanentProgressErrorEngine{},
		storage: &exactProgressStorage{
			tasks:           &exactProgressTaskStore{tasks: []*storage.Task{task}},
			controlRequests: &testControlRequestStore{},
			logs:            &mockApplyLogStore{},
		},
		logger: slog.Default(),
	}

	action := client.pollTaskToCompletion(t.Context(), apply, task, nil, &engine.ResumeState{Metadata: "shard-meta"})

	assert.Equal(t, taskFailed, action)
	assert.Equal(t, state.Task.Failed, task.State, "a permanent progress error fails the task without exhausting the retry budget")
}
