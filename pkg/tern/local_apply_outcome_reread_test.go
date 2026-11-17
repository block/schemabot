package tern

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// reReadFailingApplyStore fails every apply read while serving writes from the
// wrapped store, modelling a storage blip at the moment a drive re-reads the
// apply before recording its outcome.
type reReadFailingApplyStore struct {
	*mockApplyStore
	getErr error
}

func (s *reReadFailingApplyStore) Get(context.Context, int64) (*storage.Apply, error) {
	return nil, s.getErr
}

// outcomeRecorder is one of the two drive paths that re-read the apply before
// recording an outcome for it.
type outcomeRecorder struct {
	name   string
	record func(c *LocalClient, ctx context.Context, apply *storage.Apply) error
}

var outcomeRecorders = []outcomeRecorder{
	{name: "failed", record: func(c *LocalClient, ctx context.Context, apply *storage.Apply) error {
		return c.failApplyWithTasks(ctx, apply, nil, "engine error")
	}},
	{name: "failed_retryable", record: func(c *LocalClient, ctx context.Context, apply *storage.Apply) error {
		return c.markApplyRetryableWithTasks(ctx, apply, nil, "engine error")
	}},
}

func outcomeRereadClient(applies storage.ApplyStore) *LocalClient {
	return &LocalClient{
		config: LocalConfig{Database: "orders", Type: storage.DatabaseTypeMySQL},
		storage: &exactProgressStorage{
			applies: applies,
			tasks:   &exactProgressTaskStore{},
			logs:    &mockApplyLogStore{},
		},
		logger: slog.Default(),
	}
}

// An operator stops an apply while its drive is handling an engine error, and
// the drive's re-read of the apply then fails. Storage would let a failed
// outcome land on the stopped row, so writing blind could turn an apply the
// operator can resume into one they must re-apply. The drive records nothing
// and returns the read error naming the apply, leaving the stopped row as the
// operator left it for the next claim to judge.
func TestOutcomeWrite_FailedReReadWritesNothing(t *testing.T) {
	for _, recorder := range outcomeRecorders {
		t.Run(recorder.name, func(t *testing.T) {
			getErr := errors.New("connection reset by peer")
			inner := &mockApplyStore{apply: failureLogTestApply(state.Apply.Stopped, 0)}
			client := outcomeRereadClient(&reReadFailingApplyStore{mockApplyStore: inner, getErr: getErr})

			drive := *failureLogTestApply(state.Apply.Running, 0)
			err := recorder.record(client, t.Context(), &drive)

			require.ErrorIs(t, err, getErr)
			assert.Contains(t, err.Error(), "apply-7", "the error names the apply whose outcome was not recorded")
			assert.Empty(t, inner.updates, "no outcome is written without knowing the stored state")
			assert.Equal(t, state.Apply.Stopped, inner.apply.State)
			assert.Equal(t, state.Apply.Running, drive.State, "the in-memory apply is not marked with an outcome that was never stored")
		})
	}
}

// The apply row is gone when the drive re-reads it before recording an
// outcome. The drive returns an error that says the apply was not found rather
// than writing an outcome for a row it cannot see.
func TestOutcomeWrite_MissingRowIsAnError(t *testing.T) {
	for _, recorder := range outcomeRecorders {
		t.Run(recorder.name, func(t *testing.T) {
			applies := &mockApplyStore{}
			client := outcomeRereadClient(applies)

			drive := *failureLogTestApply(state.Apply.Running, 0)
			err := recorder.record(client, t.Context(), &drive)

			require.ErrorIs(t, err, storage.ErrApplyNotFound)
			assert.Empty(t, applies.updates)
		})
	}
}

// A drive whose context is cancelled while it re-reads the apply is a process
// shutting down or a claim being released, not a failed schema change. The
// drive hands the apply back with no error and writes nothing, the same as a
// cancellation observed anywhere else on the outcome path.
func TestOutcomeWrite_ReReadCancelledHandsTheApplyBack(t *testing.T) {
	for _, recorder := range outcomeRecorders {
		t.Run(recorder.name, func(t *testing.T) {
			inner := &mockApplyStore{apply: failureLogTestApply(state.Apply.Running, 0)}
			ctx, cancel := context.WithCancel(t.Context())
			client := outcomeRereadClient(&cancellingReReadApplyStore{mockApplyStore: inner, cancel: cancel})

			drive := *failureLogTestApply(state.Apply.Running, 0)
			err := recorder.record(client, ctx, &drive)

			require.NoError(t, err)
			assert.Empty(t, inner.updates)
			assert.Equal(t, state.Apply.Running, inner.apply.State)
		})
	}
}

// cancellingReReadApplyStore cancels the drive's context during the apply
// re-read and fails the read with the cancellation, as a storage client does
// when its caller goes away mid-query.
type cancellingReReadApplyStore struct {
	*mockApplyStore
	cancel context.CancelFunc
}

func (s *cancellingReReadApplyStore) Get(ctx context.Context, _ int64) (*storage.Apply, error) {
	s.cancel()
	return nil, ctx.Err()
}

// A dispatch arrives while another apply's task still holds the database. The
// conflict check re-reads the database's tasks a bounded number of times to
// ride out an engine that has not settled yet. When the dispatch's caller goes
// away during that wait, the check stops waiting and returns the context's
// error with no blocking task, so the dispatch is abandoned as a read that did
// not finish rather than refused as a conflict it never confirmed.
func TestConflictCheckStopsWaitingWhenTheDispatchIsCancelled(t *testing.T) {
	stopped := &storage.Task{
		ID:             1,
		TaskIdentifier: "task-stopped",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		TableName:      "users",
		State:          state.Task.Stopped,
	}
	client := newNoActiveChangeClient("testdb", []*storage.Task{stopped})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	plan := &storage.Plan{Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL}
	blocking, released, err := client.checkActiveTaskConflict(ctx, plan, "", "", 0)

	require.ErrorIs(t, err, context.Canceled)
	assert.NotContains(t, err.Error(), "schema change already in progress", "a cancelled wait is not a confirmed conflict")
	assert.Contains(t, err.Error(), "task-stopped", "the error names the task the check was waiting on")
	assert.False(t, blocking.blocks(), "no blocking task is reported for a check that did not finish")
	assert.Empty(t, released)
}
