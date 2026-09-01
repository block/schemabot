package tern

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// countingProgressEngine reports how many times the drive polled it.
type countingProgressEngine struct {
	engine.Engine
	progressCalls int
}

func (e *countingProgressEngine) Name() string { return "counting" }

func (e *countingProgressEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	e.progressCalls++
	return &engine.ProgressResult{State: engine.StateRevertWindow}, nil
}

// cancelledDriveContext returns a context that is already done, standing in for
// a drive whose process is winding down or whose claim has been released.
func cancelledDriveContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	return ctx
}

// revertWindowDrive returns an apply and task parked in the revert window: the
// change has already cut over on the provider and the drive is only waiting out
// the window before finalizing.
func revertWindowDrive() (*storage.Apply, *storage.Task) {
	apply := failureLogTestApply(state.Apply.RevertWindow, 0)
	task := &storage.Task{
		ID: 1, TaskIdentifier: "task-1", ApplyID: apply.ID,
		TableName: "orders", State: state.Task.RevertWindow,
	}
	return apply, task
}

// A process winding down cancels the drive's context, and every storage write
// the drive attempts from that moment fails with the same cancellation. Pausing
// the apply on that evidence would park a schema change that has already cut
// over and is healthy on the provider, so the failure is left unrecorded and the
// apply stays exactly as it stands for the driver that reclaims it.
func TestCancelledDriveDoesNotPauseAHealthyApply(t *testing.T) {
	apply, task := revertWindowDrive()
	client, logs := newFailureLogTestClient(apply, []*storage.Task{task})

	client.markApplyRetryableWithTasks(cancelledDriveContext(t), apply, []*storage.Task{task},
		"failed to save engine resume state from progress polling: context canceled")

	assert.Equal(t, state.Apply.RevertWindow, apply.State, "a cancelled drive must not pause the apply")
	assert.Equal(t, state.Task.RevertWindow, task.State, "a cancelled drive must not pause the apply's tasks")
	assert.Empty(t, task.ErrorMessage, "a cancelled drive must not stamp an error on a healthy task")
	assert.Empty(t, logs.entries, "a cancelled drive must not tell the operator the apply paused")
}

// The same reasoning applies to a permanent failure: a cancelled drive knows
// nothing about the change, and terminalizing on it would report a schema change
// failed while it was still running on the provider.
func TestCancelledDriveDoesNotFailAHealthyApply(t *testing.T) {
	apply, task := revertWindowDrive()
	client, logs := newFailureLogTestClient(apply, []*storage.Task{task})

	client.failApplyWithTasks(cancelledDriveContext(t), apply, []*storage.Task{task}, "context canceled")

	assert.Equal(t, state.Apply.RevertWindow, apply.State, "a cancelled drive must not fail the apply")
	assert.Equal(t, state.Task.RevertWindow, task.State, "a cancelled drive must not fail the apply's tasks")
	assert.Nil(t, apply.CompletedAt, "a cancelled drive must not stamp the apply finished")
	assert.Empty(t, logs.entries, "a cancelled drive must not tell the operator the apply failed")
}

// The poll loop's select can pick a ready ticker over an equally ready
// ctx.Done(), so a tick can begin after the drive has been cancelled. Every
// engine call and storage write it made would fail on that context, so the tick
// stops the drive instead of starting work it cannot finish.
func TestAtomicProgressTickHandsOverWhenTheDriveIsCancelled(t *testing.T) {
	apply, task := revertWindowDrive()
	client, logs := newFailureLogTestClient(apply, []*storage.Task{task})
	client.config.Type = storage.DatabaseTypeVitess
	eng := &countingProgressEngine{}

	done := client.handleAtomicProgressTick(cancelledDriveContext(t), eng, apply, []*storage.Task{task},
		nil, &engine.ResumeState{}, &atomicPollState{}, nil, false)

	assert.True(t, done, "a cancelled drive stops polling")
	assert.Zero(t, eng.progressCalls, "a tick that starts after cancellation must not call the engine")
	assert.Equal(t, state.Apply.RevertWindow, apply.State, "a cancelled tick must leave the apply as it stands")
	assert.Empty(t, logs.entries, "a cancelled tick must record nothing against the apply")
}
