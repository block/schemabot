package tern

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
