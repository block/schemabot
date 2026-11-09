package tern

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
)

// finalizerPollStep is one scripted answer to a progress poll.
type finalizerPollStep struct {
	result *engine.ProgressResult
	err    error
}

// finalizerPollEngine answers progress polls from a script, one step per call,
// repeating the last step once the script runs out.
type finalizerPollEngine struct {
	engine.Engine
	steps []finalizerPollStep
	calls int
}

func (e *finalizerPollEngine) Name() string { return "finalizer-poll" }

func (e *finalizerPollEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	step := e.steps[min(e.calls, len(e.steps)-1)]
	e.calls++
	return step.result, step.err
}

func driveFinalizerPoll(t *testing.T, eng engine.Engine, persist func(*engine.ResumeState)) (engine.State, error) {
	t.Helper()
	client := &LocalClient{
		config:                   LocalConfig{Database: "appdb", Type: storage.DatabaseTypeVitess},
		logger:                   slog.Default(),
		taskPollIntervalOverride: time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	apply := &storage.Apply{ApplyIdentifier: "apply-finalizer-poll", Database: "appdb"}
	return client.driveFinalizerToTerminal(ctx, eng, apply, nil, &engine.ResumeState{MigrationContext: "deploy-1"}, persist)
}

// A finalizer polling a deploy whose progress never answers must stop after the
// same run of failed polls every other drive tolerates, and hand back a
// retryable error so the finalizer pauses for a later claim instead of holding
// the apply's drive forever.
func TestDriveFinalizerToTerminal_TransientErrorsSpendABudgetAndReturnRetryable(t *testing.T) {
	pollErr := errors.New("get deploy request: rate limited")
	eng := &finalizerPollEngine{steps: []finalizerPollStep{{err: pollErr}}}

	finalState, err := driveFinalizerPoll(t, eng, func(*engine.ResumeState) {})

	require.Error(t, err)
	assert.Empty(t, finalState)
	assert.Equal(t, maxConsecutiveProgressPollErrors, eng.calls, "the drive polls exactly its budget of failing polls")
	require.ErrorIs(t, err, pollErr)
	assert.ErrorContains(t, err, fmt.Sprintf("failed after %d consecutive errors", maxConsecutiveProgressPollErrors))
	assert.True(t, engine.IsRetryable(err), "a spent budget must not read as a permanent failure")
	assert.NotErrorIs(t, err, context.DeadlineExceeded)
}

// A poll that answers resets the run of failures, so a deploy whose progress
// API is flaky but alive is still followed to its outcome.
func TestDriveFinalizerToTerminal_AnsweredPollRestartsTheBudget(t *testing.T) {
	pollErr := errors.New("get deploy request: connection reset")
	var steps []finalizerPollStep
	addErrors := func(n int) {
		for range n {
			steps = append(steps, finalizerPollStep{err: pollErr})
		}
	}
	addErrors(maxConsecutiveProgressPollErrors - 1)
	steps = append(steps, finalizerPollStep{result: &engine.ProgressResult{State: engine.StateRunning}})
	addErrors(maxConsecutiveProgressPollErrors - 1)
	steps = append(steps, finalizerPollStep{result: &engine.ProgressResult{State: engine.StateCompleted}})
	eng := &finalizerPollEngine{steps: steps}

	finalState, err := driveFinalizerPoll(t, eng, func(*engine.ResumeState) {})

	require.NoError(t, err)
	assert.Equal(t, engine.StateCompleted, finalState)
	assert.Equal(t, len(steps), eng.calls)
}

// A permanent progress error fails the drive on the poll that reports it,
// without waiting out the budget.
func TestDriveFinalizerToTerminal_PermanentErrorEndsTheDriveAtOnce(t *testing.T) {
	eng := &finalizerPollEngine{steps: []finalizerPollStep{{err: engine.NewPermanentError("deploy request deploy-1 not found")}}}

	_, err := driveFinalizerPoll(t, eng, func(*engine.ResumeState) {})

	require.Error(t, err)
	assert.Equal(t, 1, eng.calls)
	assert.ErrorContains(t, err, "progress poll failed permanently")
	assert.False(t, engine.IsRetryable(err))
}

// Each answered poll saves the resume state the engine reports, so a reclaimed
// finalizer reattaches to the deploy from where the engine last said it was.
func TestDriveFinalizerToTerminal_SavesReportedResumeState(t *testing.T) {
	eng := &finalizerPollEngine{steps: []finalizerPollStep{
		{result: &engine.ProgressResult{State: engine.StateRunning, ResumeState: &engine.ResumeState{MigrationContext: "deploy-2"}}},
		{result: &engine.ProgressResult{State: engine.StateRevertWindow}},
	}}
	var saved []string
	persist := func(rs *engine.ResumeState) { saved = append(saved, rs.MigrationContext) }

	finalState, err := driveFinalizerPoll(t, eng, persist)

	require.NoError(t, err)
	assert.Equal(t, engine.StateRevertWindow, finalState)
	assert.Equal(t, []string{"deploy-2"}, saved)
}
