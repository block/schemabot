package spirit

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
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

// An operator stops or cancels a schema change whose Spirit cutover commits
// just before the cancel reaches the runner. The runner then returns having
// finished the change, and the run records it completed. Once the runner has
// returned, the stop or cancel reads that outcome and answers that the change
// completed, so the caller reconciles to the completed outcome rather than
// recording a stop or cancel of a change the target already holds. A runner
// that returns without finishing leaves the stop or cancel in effect.
func TestControlKeepsACompletionThatLandsAfterTheCancel(t *testing.T) {
	controls := map[string]struct {
		run       func(eng *Engine, ctx context.Context) (*engine.ControlResult, error)
		wantState engine.State
	}{
		"stop": {run: func(eng *Engine, ctx context.Context) (*engine.ControlResult, error) {
			return eng.Stop(ctx, &engine.ControlRequest{})
		}, wantState: engine.StateStopped},
		"cancel": {run: func(eng *Engine, ctx context.Context) (*engine.ControlResult, error) {
			return eng.Cancel(ctx, &engine.ControlRequest{})
		}, wantState: engine.StateCancelled},
	}
	for name, control := range controls {
		t.Run(name+"/the change completes after the cancel", func(t *testing.T) {
			eng := New(Config{})
			rm := registerRunningSchemaChange(eng)
			runCtx, cancelRun := context.WithCancel(t.Context())
			rm.cancelFunc = cancelRun
			rm.wg.Go(func() {
				<-runCtx.Done()
				eng.completeSchemaChange(runCtx, eng.changeLogger(), rm.database)
			})

			result, err := control.run(eng, t.Context())

			require.Error(t, err)
			assert.Nil(t, result)
			assert.True(t, engine.IsAlreadyCompleted(err), "the caller reconciles to the completed outcome, got %v", err)
			assert.Contains(t, err.Error(), "completed before the "+name+" took effect")
			assert.Equal(t, engine.StateCompleted, rm.state)
		})

		t.Run(name+"/the runner returns without finishing", func(t *testing.T) {
			eng := New(Config{})
			rm := registerRunningSchemaChange(eng)
			runCtx, cancelRun := context.WithCancel(t.Context())
			rm.cancelFunc = cancelRun
			rm.wg.Go(func() { <-runCtx.Done() })

			_, err := control.run(eng, t.Context())

			if name == "cancel" {
				// The cancel goes on to release artifacts against a target this
				// fixture does not run, so only the state it settled matters here.
				assert.False(t, engine.IsAlreadyCompleted(err))
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, control.wantState, rm.state)
		})
	}
}

// A stop that arrives while an earlier stop is still waiting for the runner
// finds the change marked stopped, but that mark is provisional until the
// runner returns: the runner can still finish the change on its way out. The
// second stop waits the runner out and answers from its outcome, so a change
// that lands is rejected as completed by both stops rather than reported
// stopped by the second.
func TestSecondStopDuringFirstStopWaitDoesNotReportAlreadyStopped(t *testing.T) {
	eng := New(Config{})
	rm := registerRunningSchemaChange(eng)
	runCtx, cancelRun := context.WithCancel(t.Context())
	rm.cancelFunc = cancelRun
	release := make(chan struct{})
	rm.wg.Go(func() {
		<-runCtx.Done()
		<-release
		eng.completeSchemaChange(runCtx, eng.changeLogger(), rm.database)
	})

	first := make(chan error, 1)
	go func() {
		_, err := eng.Stop(t.Context(), &engine.ControlRequest{})
		first <- err
	}()
	require.Eventually(t, func() bool {
		eng.mu.Lock()
		defer eng.mu.Unlock()
		return rm.state == engine.StateStopped
	}, 5*time.Second, 5*time.Millisecond, "the first stop marks the change stopped before waiting for the runner")

	second := make(chan error, 1)
	go func() {
		_, err := eng.Stop(t.Context(), &engine.ControlRequest{})
		second <- err
	}()
	// The second stop must not answer from the provisional mark: it is still
	// waiting when the runner has not returned.
	select {
	case err := <-second:
		t.Fatalf("the second stop answered %v before the runner returned", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)

	assert.True(t, engine.IsAlreadyCompleted(<-first), "the first stop reconciles to the completed outcome")
	assert.True(t, engine.IsAlreadyCompleted(<-second), "the second stop reconciles to the completed outcome too")
	assert.Equal(t, engine.StateCompleted, rm.state)
}

// A stop that finds the change stopped with no runner alive answers at once:
// there is nothing to wait out, and the change really is paused.
func TestStopOfAStoppedChangeWithNoRunnerAnswersAlreadyStopped(t *testing.T) {
	eng := New(Config{})
	rm := registerRunningSchemaChange(eng)
	rm.state = engine.StateStopped

	result, err := eng.Stop(t.Context(), &engine.ControlRequest{})
	require.NoError(t, err)
	assert.True(t, result.Accepted)
	assert.Equal(t, "Already stopped", result.Message)
	assert.Equal(t, engine.StateStopped, rm.state)
}

// silentTarget returns the address of a TCP listener that accepts connections
// and never sends the MySQL handshake, so any connection attempt against it
// blocks until its context is cancelled. The returned channel closes once the
// first connection has been accepted.
func silentTarget(t *testing.T) (string, <-chan struct{}) {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	accepted := make(chan struct{})
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			if len(conns) == 0 {
				close(accepted)
			}
			conns = append(conns, conn)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		utils.CloseAndLog(ln)
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			utils.CloseAndLog(conn)
		}
	})
	return ln.Addr().String(), accepted
}

// An operator stops or cancels a schema change while the engine is still
// connecting to the target, either to read the current table definitions it
// routes each ALTER by or to open the pool a direct-routed ALTER runs on. The
// connection attempt returns because the stop or cancel cancelled its context,
// not because the target failed, so the change keeps the stopped or cancelled
// state the operator asked for: progress never reports it as a retryable
// failure.
func TestControlDuringTargetConnectionKeepsTheControlOutcome(t *testing.T) {
	const alter = "ALTER TABLE `users` ADD COLUMN `email` varchar(255) NULL"
	phases := map[string]func(eng *Engine, ctx context.Context, host string) bool{
		"ALTER routing": func(eng *Engine, ctx context.Context, host string) bool {
			return eng.executeAlterPhase(ctx, host, "root", "", "testdb", []string{alter}, false, directPolicy{})
		},
		"direct execution connect": func(eng *Engine, ctx context.Context, host string) bool {
			target := &lazyTargetDB{dsn: targetDSN(host, "root", "", "testdb")}
			defer target.close()
			stmts := []directRouted{{stmt: alter, table: "users", reason: "refused by the schema change engine"}}
			return eng.executeDirectStatements(ctx, target, "testdb", stmts, directPolicy{Enabled: true})
		},
	}
	controls := map[string]struct {
		run       func(eng *Engine, ctx context.Context) (*engine.ControlResult, error)
		wantState engine.State
	}{
		"stop": {run: func(eng *Engine, ctx context.Context) (*engine.ControlResult, error) {
			return eng.Stop(ctx, &engine.ControlRequest{})
		}, wantState: engine.StateStopped},
		"cancel": {run: func(eng *Engine, ctx context.Context) (*engine.ControlResult, error) {
			return eng.Cancel(ctx, &engine.ControlRequest{})
		}, wantState: engine.StateCancelled},
	}
	for phaseName, runPhase := range phases {
		for controlName, control := range controls {
			t.Run(controlName+" during "+phaseName, func(t *testing.T) {
				host, accepted := silentTarget(t)
				eng := New(Config{})
				rm := registerRunningSchemaChange(eng)
				runCtx, cancelRun := context.WithCancel(t.Context())
				rm.cancelFunc = cancelRun
				finished := make(chan bool, 1)
				rm.wg.Go(func() {
					finished <- runPhase(eng, runCtx, host)
				})
				select {
				case <-accepted:
				case <-time.After(10 * time.Second):
					t.Fatal("the phase never connected to the target")
				}

				result, err := control.run(eng, t.Context())

				if controlName == "cancel" {
					// The cancel goes on to release artifacts against a target this
					// fixture does not run, so only the state it settled matters here.
					assert.False(t, engine.IsAlreadyCompleted(err))
				} else {
					require.NoError(t, err)
					assert.True(t, result.Accepted)
				}
				assert.False(t, <-finished, "a phase interrupted by a "+controlName+" must not let later phases run")
				progress := pollProgress(t, eng)
				assert.Equal(t, control.wantState, progress.State)
				assert.Empty(t, progress.ErrorMessage)
				assert.False(t, progress.Retryable)
			})
		}
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
