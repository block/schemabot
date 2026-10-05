package spirit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

func TestCancelMarksRunningSchemaChangeCancelled(t *testing.T) {
	eng := New(Config{})
	cancelCalled := false
	eng.runningSchemaChange = &runningSchemaChange{
		database: "testdb",
		tables:   []string{"users"},
		state:    engine.StateRunning,
		cancelFunc: func() {
			cancelCalled = true
		},
	}

	_, err := eng.Cancel(t.Context(), &engine.ControlRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cleanup missing connection details")
	assert.True(t, cancelCalled)
	assert.Equal(t, engine.StateCancelled, eng.runningSchemaChange.state)
}

// registerRunningSchemaChange installs a simulated running schema change on the
// engine the same way Apply initializes one. The caller drives the change's
// lifecycle through rm.wg.
func registerRunningSchemaChange(eng *Engine) *runningSchemaChange {
	rm := &runningSchemaChange{
		database:       "testdb",
		tableNamespace: map[string]string{},
		state:          engine.StateRunning,
		host:           "127.0.0.1:1",
		username:       "root",
	}
	eng.installRunningSchemaChange(rm)
	return rm
}

// assertSettledOutcomeRefusal checks that err is the typed refusal for a
// control operation that found the schema change already settled: the durable
// request resolves on it, it never reads as a gap in what the engine supports or
// as a completion, and its reason tells the operator how to recover.
func assertSettledOutcomeRefusal(t *testing.T, err error, wantReason string) {
	t.Helper()
	require.Error(t, err)
	assert.True(t, engine.IsSettledOutcome(err), "the durable request must resolve terminally on the settled outcome")
	assert.False(t, engine.IsUnsupportedOperation(err), "a settled outcome is not a control operation the engine lacks")
	assert.False(t, engine.IsAlreadyCompleted(err), "a change that did not complete must never reconcile as completed")
	assert.Contains(t, err.Error(), wantReason)
	assert.Contains(t, err.Error(), "plan and apply again to retry")
}

// A stop that reaches a schema change which has already failed leaves the
// failure in place. A failed change cannot be paused or resumed, so recording it
// as stopped would tell the operator to resume a change that is dead; the stop
// is refused permanently instead, the change is not cancelled again, and it
// keeps its failed state and the reason it failed.
func TestStopLeavesFailedSchemaChangeFailed(t *testing.T) {
	eng := New(Config{})
	rm := registerRunningSchemaChange(eng)
	cancelCalled := false
	rm.cancelFunc = func() { cancelCalled = true }
	eng.setSchemaChangeFailed(engine.OperatorErrorf(nil, "copy of users hit a duplicate key"))

	result, err := eng.Stop(t.Context(), &engine.ControlRequest{})
	assert.Nil(t, result)
	assertSettledOutcomeRefusal(t, err, "failed before the stop arrived; the failure stands")
	assert.False(t, cancelCalled)
	assert.Equal(t, engine.StateFailed, rm.state)
	assert.Equal(t, "copy of users hit a duplicate key", rm.errorMessage)
}

func TestStopLeavesCancelledSchemaChangeCancelled(t *testing.T) {
	eng := New(Config{})
	rm := registerRunningSchemaChange(eng)
	rm.state = engine.StateCancelled

	result, err := eng.Stop(t.Context(), &engine.ControlRequest{})
	assert.Nil(t, result)
	assertSettledOutcomeRefusal(t, err, "was already cancelled before the stop arrived")
	assert.Equal(t, engine.StateCancelled, rm.state)
}

// A cancel that reaches a schema change which has already failed leaves the
// failure in place, the same way a stop does: recording it as cancelled would
// replace the failure and its reason with an outcome the operator chose. The
// change is not cancelled again and its artifacts are not touched.
func TestCancelLeavesFailedSchemaChangeFailed(t *testing.T) {
	eng := New(Config{})
	rm := registerRunningSchemaChange(eng)
	cancelCalled := false
	rm.cancelFunc = func() { cancelCalled = true }
	eng.setSchemaChangeFailed(engine.OperatorErrorf(nil, "copy of users hit a duplicate key"))

	result, err := eng.Cancel(t.Context(), &engine.ControlRequest{})
	assert.Nil(t, result)
	assertSettledOutcomeRefusal(t, err, "failed before the cancel arrived; the failure stands")
	assert.False(t, cancelCalled)
	assert.Equal(t, engine.StateFailed, rm.state)
	assert.Equal(t, "copy of users hit a duplicate key", rm.errorMessage)
	assert.Same(t, rm, eng.runningSchemaChange, "a refused cancel must leave the failed change tracked so progress keeps reporting it")
}

// A stop checkpoints the copy before it cancels, and the schema change can
// settle on its own while that checkpoint is being written. Whatever it settles
// on wins over the stop: a failure that lands during the checkpoint stays a
// failure rather than being relabelled as a resumable stop, and a completion
// that lands there stays completed, with the stop answered by the typed
// already-completed rejection its caller reconciles from.
func TestStopKeepsOutcomeThatLandsDuringCheckpoint(t *testing.T) {
	tests := []struct {
		name      string
		settle    func(eng *Engine)
		wantState engine.State
		checkErr  func(t *testing.T, err error)
	}{
		{
			name: "failure",
			settle: func(eng *Engine) {
				eng.setSchemaChangeFailed(engine.OperatorErrorf(nil, "copy of users hit a duplicate key"))
			},
			wantState: engine.StateFailed,
			checkErr: func(t *testing.T, err error) {
				assertSettledOutcomeRefusal(t, err, "failed before the stop arrived; the failure stands")
			},
		},
		{
			name:      "completion",
			settle:    func(eng *Engine) { eng.setSchemaChangeCompleted() },
			wantState: engine.StateCompleted,
			checkErr: func(t *testing.T, err error) {
				assert.True(t, engine.IsAlreadyCompleted(err))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			eng := New(Config{})
			rm := registerRunningSchemaChange(eng)
			cancelCalled := false
			rm.cancelFunc = func() { cancelCalled = true }
			eng.stopCheckpointWindow = func() { tc.settle(eng) }

			result, err := eng.Stop(t.Context(), &engine.ControlRequest{})
			require.Error(t, err)
			assert.Nil(t, result)
			tc.checkErr(t, err)
			assert.False(t, cancelCalled, "a change that already settled has nothing left to cancel")
			assert.Equal(t, tc.wantState, rm.state)
		})
	}
}

// Stateless control operations (cutover, deferred cutover sentinel lookup)
// must address the schema the DSN connects to: under per-deployment schema
// overrides the DSN carries the physical schema name while the request carries
// the logical (canonical) database name. The request database is only a
// fallback for DSNs without a schema.
func TestStatelessControlDatabase(t *testing.T) {
	t.Run("DSN database wins over request database", func(t *testing.T) {
		got, err := statelessControlDatabase("root@tcp(localhost:3306)/bikeshare_eu_qa", "bikeshare")
		require.NoError(t, err)
		assert.Equal(t, "bikeshare_eu_qa", got)
	})

	t.Run("request database is the fallback for a namespace-free DSN", func(t *testing.T) {
		got, err := statelessControlDatabase("root@tcp(localhost:3306)/", "bikeshare")
		require.NoError(t, err)
		assert.Equal(t, "bikeshare", got)
	})

	t.Run("empty when neither names a schema", func(t *testing.T) {
		got, err := statelessControlDatabase("root@tcp(localhost:3306)/", "")
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("invalid DSN is an error", func(t *testing.T) {
		_, err := statelessControlDatabase("not a dsn", "bikeshare")
		require.Error(t, err)
	})
}

// The revert-window controls decline with a typed unsupported-operation error.
// Spirit copies into a shadow table and swaps it in, so once a change cuts over
// there is no engine phase left to revert from and no window to close. The typed
// decline is what lets a durable control request resolve terminally instead of
// retrying a rejection that can never succeed while the schema change keeps
// executing.
func TestRevertWindowControlsDeclineAsUnsupported(t *testing.T) {
	eng := New(Config{})

	tests := []struct {
		name   string
		call   func(t *testing.T) error
		reason string
	}{
		{"revert", func(t *testing.T) error {
			result, err := eng.Revert(t.Context(), &engine.ControlRequest{})
			assert.Nil(t, result)
			return err
		}, "no revert window to undo it from"},
		{"skip-revert", func(t *testing.T) error {
			result, err := eng.SkipRevert(t.Context(), &engine.ControlRequest{})
			assert.Nil(t, result)
			return err
		}, "no revert window to close"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call(t)
			require.Error(t, err)
			assert.True(t, engine.IsUnsupportedOperation(err),
				"the decline must be typed so durable control consumers resolve it terminally")
			assert.Contains(t, err.Error(), tc.reason,
				"the decline reason reaches operator-facing surfaces and must say why")
		})
	}
}
