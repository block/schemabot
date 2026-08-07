package api

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// terminalSummaryApplyStore serves a single apply from Get and records how many
// times it was reloaded, so a test can assert the publisher reloads the parent
// at its terminal state exactly once.
type terminalSummaryApplyStore struct {
	storage.ApplyStore
	apply    *storage.Apply
	getErr   error
	getCalls int
}

func (s *terminalSummaryApplyStore) Get(context.Context, int64) (*storage.Apply, error) {
	s.getCalls++
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.apply, nil
}

// terminalSummaryTaskStore serves every task of an apply from GetByApplyID and
// fails GetByApplyOperationID, so a test can prove the publisher reloads the
// whole-apply task set rather than a single operation's tasks.
type terminalSummaryTaskStore struct {
	storage.TaskStore
	tasks  []*storage.Task
	err    error
	called bool
}

func (s *terminalSummaryTaskStore) GetByApplyID(context.Context, int64) ([]*storage.Task, error) {
	s.called = true
	if s.err != nil {
		return nil, s.err
	}
	return s.tasks, nil
}

func (s *terminalSummaryTaskStore) GetByApplyOperationID(context.Context, int64) ([]*storage.Task, error) {
	return nil, errors.New("aggregate terminal summary must load whole-apply tasks, not operation-scoped tasks")
}

type terminalSummaryStorage struct {
	mockStorage
	applies storage.ApplyStore
	tasks   storage.TaskStore
}

func (m *terminalSummaryStorage) Applies() storage.ApplyStore { return m.applies }
func (m *terminalSummaryStorage) Tasks() storage.TaskStore    { return m.tasks }

func newTerminalSummaryTestService(applies storage.ApplyStore, tasks storage.TaskStore) *Service {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&terminalSummaryStorage{applies: applies, tasks: tasks}, testServerConfig(), nil, logger)
}

type recordedTerminalSummary struct {
	apply *storage.Apply
	tasks []*storage.Task
}

// TestPublishTerminalSummaryIfWon_Gating verifies the publisher only fires when
// this drive won the non-terminal→terminal projection CAS: a result that did not
// terminalize the parent is an inert no-op, whatever the operation count.
func TestPublishTerminalSummaryIfWon_Gating(t *testing.T) {
	cases := []struct {
		name   string
		result applyProjectionResult
	}{
		{"not terminal multi operation", applyProjectionResult{BecameTerminal: false, OperationCount: 2}},
		{"not terminal single operation", applyProjectionResult{BecameTerminal: false, OperationCount: 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			applyStore := &terminalSummaryApplyStore{}
			taskStore := &terminalSummaryTaskStore{}
			svc := newTerminalSummaryTestService(applyStore, taskStore)
			called := false
			svc.OnApplyTerminalSummary = func(context.Context, *storage.Apply, []*storage.Task) error {
				called = true
				return nil
			}

			svc.publishTerminalSummaryIfWon(t.Context(), 1,
				&storage.Apply{ID: 7, ApplyIdentifier: "apply-x", State: state.Apply.Running}, tc.result)

			assert.False(t, called, "callback must not fire for gated case")
			assert.Zero(t, applyStore.getCalls, "must not reload the apply for a gated case")
			assert.False(t, taskStore.called, "must not reload tasks for a gated case")
		})
	}
}

// TestPublishTerminalSummaryIfWon_SingleOperationStopped verifies the CAS winner
// publishes for a single-operation apply that settled stopped — the stop
// reconciliation path, where no live per-driver observer remains to post the
// stopped summary. The publisher receives the reloaded stopped apply.
func TestPublishTerminalSummaryIfWon_SingleOperationStopped(t *testing.T) {
	stoppedApply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-x", Database: "db", Environment: "staging", State: state.Apply.Stopped}
	tasks := []*storage.Task{{ID: 1, ApplyID: 7, TableName: "users", State: state.Task.Stopped}}
	applyStore := &terminalSummaryApplyStore{apply: stoppedApply}
	taskStore := &terminalSummaryTaskStore{tasks: tasks}
	svc := newTerminalSummaryTestService(applyStore, taskStore)

	var recorded []recordedTerminalSummary
	svc.OnApplyTerminalSummary = func(_ context.Context, a *storage.Apply, ts []*storage.Task) error {
		recorded = append(recorded, recordedTerminalSummary{apply: a, tasks: ts})
		return nil
	}

	preCAS := &storage.Apply{ID: 7, ApplyIdentifier: "apply-x", State: state.Apply.Pending}
	svc.publishTerminalSummaryIfWon(t.Context(), 1, preCAS,
		applyProjectionResult{BecameTerminal: true, OperationCount: 1, DerivedState: state.Apply.Stopped})

	require.Len(t, recorded, 1, "publisher must fire exactly once for a stopped single-operation apply")
	assert.Same(t, stoppedApply, recorded[0].apply, "publisher receives the reloaded stopped apply")
	assert.Equal(t, state.Apply.Stopped, recorded[0].apply.State)
	assert.Len(t, recorded[0].tasks, 1)
}

// TestPublishTerminalSummaryIfWon_NilCallback verifies a missing publisher is a
// safe no-op: the apply is not reloaded and nothing panics.
func TestPublishTerminalSummaryIfWon_NilCallback(t *testing.T) {
	applyStore := &terminalSummaryApplyStore{}
	taskStore := &terminalSummaryTaskStore{}
	svc := newTerminalSummaryTestService(applyStore, taskStore)
	svc.OnApplyTerminalSummary = nil

	svc.publishTerminalSummaryIfWon(t.Context(), 1,
		&storage.Apply{ID: 7, ApplyIdentifier: "apply-x", State: state.Apply.Running},
		applyProjectionResult{BecameTerminal: true, OperationCount: 2})

	assert.Zero(t, applyStore.getCalls, "must not reload the apply when no publisher is configured")
}

// TestPublishTerminalSummaryIfWon_PublishesOnce verifies the CAS winner reloads
// the parent at its terminal state, reloads the whole-apply task set, and invokes
// the publisher exactly once with that reloaded apply and tasks.
func TestPublishTerminalSummaryIfWon_PublishesOnce(t *testing.T) {
	terminalApply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-x", Database: "db", Environment: "staging", State: state.Apply.Failed, ErrorMessage: "boom"}
	tasks := []*storage.Task{
		{ID: 1, ApplyID: 7, TableName: "users", State: state.Task.Completed},
		{ID: 2, ApplyID: 7, TableName: "orders", State: state.Task.Failed},
	}
	applyStore := &terminalSummaryApplyStore{apply: terminalApply}
	taskStore := &terminalSummaryTaskStore{tasks: tasks}
	svc := newTerminalSummaryTestService(applyStore, taskStore)

	var recorded []recordedTerminalSummary
	svc.OnApplyTerminalSummary = func(_ context.Context, a *storage.Apply, ts []*storage.Task) error {
		recorded = append(recorded, recordedTerminalSummary{apply: a, tasks: ts})
		return nil
	}

	// The input apply carries the pre-CAS running state; the publisher must use
	// the reloaded terminal apply, not this stale copy.
	preCAS := &storage.Apply{ID: 7, ApplyIdentifier: "apply-x", State: state.Apply.Running}
	svc.publishTerminalSummaryIfWon(t.Context(), 1, preCAS,
		applyProjectionResult{BecameTerminal: true, OperationCount: 2, DerivedState: state.Apply.Failed})

	require.Len(t, recorded, 1, "publisher must fire exactly once")
	assert.Equal(t, 1, applyStore.getCalls, "parent apply reloaded once")
	assert.True(t, taskStore.called, "whole-apply tasks reloaded")
	assert.Same(t, terminalApply, recorded[0].apply, "publisher receives the reloaded terminal apply")
	assert.Equal(t, state.Apply.Failed, recorded[0].apply.State)
	assert.Len(t, recorded[0].tasks, 2)
}

// TestPublishTerminalSummaryIfWon_NotTerminalAfterReload verifies the publisher
// fails closed if the reloaded apply is no longer terminal (a concurrent change
// after the CAS): it skips the publish rather than rendering a stale summary.
func TestPublishTerminalSummaryIfWon_NotTerminalAfterReload(t *testing.T) {
	applyStore := &terminalSummaryApplyStore{apply: &storage.Apply{ID: 7, ApplyIdentifier: "apply-x", State: state.Apply.Running}}
	taskStore := &terminalSummaryTaskStore{}
	svc := newTerminalSummaryTestService(applyStore, taskStore)
	called := false
	svc.OnApplyTerminalSummary = func(context.Context, *storage.Apply, []*storage.Task) error {
		called = true
		return nil
	}

	svc.publishTerminalSummaryIfWon(t.Context(), 1,
		&storage.Apply{ID: 7, ApplyIdentifier: "apply-x", State: state.Apply.Running},
		applyProjectionResult{BecameTerminal: true, OperationCount: 2, DerivedState: state.Apply.Failed})

	assert.False(t, called, "must not publish when the reloaded apply is not terminal")
	assert.False(t, taskStore.called, "must not reload tasks when the apply is not terminal")
}

// TestPublishTerminalSummaryIfWon_ReloadError verifies a reload failure after the
// CAS is best effort: no publish, and no panic. The parent stays terminal.
func TestPublishTerminalSummaryIfWon_ReloadError(t *testing.T) {
	applyStore := &terminalSummaryApplyStore{getErr: errors.New("storage down")}
	taskStore := &terminalSummaryTaskStore{}
	svc := newTerminalSummaryTestService(applyStore, taskStore)
	called := false
	svc.OnApplyTerminalSummary = func(context.Context, *storage.Apply, []*storage.Task) error {
		called = true
		return nil
	}

	svc.publishTerminalSummaryIfWon(t.Context(), 1,
		&storage.Apply{ID: 7, ApplyIdentifier: "apply-x", State: state.Apply.Running},
		applyProjectionResult{BecameTerminal: true, OperationCount: 2, DerivedState: state.Apply.Failed})

	assert.False(t, called, "must not publish when the apply reload fails")
	assert.False(t, taskStore.called, "must not reload tasks when the apply reload fails")
}
