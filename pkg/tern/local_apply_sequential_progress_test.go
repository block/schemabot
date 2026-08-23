package tern

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	spiritstatus "github.com/block/spirit/pkg/status"

	"github.com/block/schemabot/pkg/engine"
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
