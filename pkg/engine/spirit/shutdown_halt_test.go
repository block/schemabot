package spirit

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// An instance with no schema change in flight holds nothing on any target, so
// shutdown has nothing to wait for and must not stall the process.
func TestHaltForShutdownWithNoSchemaChangeIsANoOp(t *testing.T) {
	eng := New(Config{})

	require.NoError(t, eng.HaltForShutdown(t.Context()))
}

// Spirit copies in a goroutine of this process and holds the target's advisory
// lock for as long as that goroutine lives. Halting must cancel the copy and
// wait for the goroutine to exit, so the lock is gone before the process stops
// renewing the apply's lease and a peer driver reclaims the work.
func TestHaltForShutdownCancelsTheCopyAndWaitsForItToExit(t *testing.T) {
	eng := New(Config{})
	runCtx, cancelRun := context.WithCancel(t.Context())
	rm := &runningSchemaChange{
		database:   "orders",
		tables:     []string{"line_items"},
		state:      engine.StateRunning,
		cancelFunc: cancelRun,
	}

	var lockReleased sync.WaitGroup
	lockReleased.Add(1)
	startTestRun(eng, rm, func() {
		<-runCtx.Done()
		lockReleased.Done()
	})
	eng.runningSchemaChange = rm

	require.NoError(t, eng.HaltForShutdown(t.Context()))

	lockReleased.Wait()
	assert.Equal(t, engine.StateRunning, rm.state,
		"halting for shutdown records no operator intent, so the apply stays active for another driver")
}

// A copy that will not come down must not hold shutdown open indefinitely: the
// halt fails on its deadline so the caller can report that the target may still
// be locked, rather than blocking the process from exiting.
func TestHaltForShutdownFailsWhenTheCopyWillNotComeDown(t *testing.T) {
	eng := New(Config{})
	stuck := make(chan struct{})
	t.Cleanup(func() { close(stuck) })
	rm := &runningSchemaChange{
		database: "orders",
		tables:   []string{"line_items"},
		state:    engine.StateRunning,
	}
	startTestRun(eng, rm, func() { <-stuck })
	eng.runningSchemaChange = rm

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	err := eng.HaltForShutdown(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "orders", "the failure names the target that may still be locked")
	assert.Contains(t, err.Error(), "line_items")
}

// A drive halts the engine work it started every time it returns, including
// after the change has run to its outcome. Work that has already ended holds
// nothing, so the halt neither cancels nor waits.
func TestHaltForShutdownAfterTheChangeEndedIsANoOp(t *testing.T) {
	eng := New(Config{})
	cancelled := false
	rm := &runningSchemaChange{
		database:   "orders",
		tables:     []string{"line_items"},
		state:      engine.StateCompleted,
		cancelFunc: func() { cancelled = true },
	}
	startTestRun(eng, rm, func() {})
	waitForTestRun(t, eng, rm)
	eng.runningSchemaChange = rm

	require.NoError(t, eng.HaltForShutdown(t.Context()))

	assert.False(t, cancelled, "a change that has ended is not cancelled")
	assert.Equal(t, engine.StateCompleted, rm.state)
}

// A change that has reached its outcome may still be tearing down, and it
// holds the target's lock until it returns. The halt waits for that teardown
// without cancelling it, so the next driver finds the target released and the
// outcome intact.
func TestHaltForShutdownWaitsOutATeardownWithoutCancellingIt(t *testing.T) {
	eng := New(Config{})
	cancelled := false
	rm := &runningSchemaChange{
		database:   "orders",
		tables:     []string{"line_items"},
		state:      engine.StateCompleted,
		cancelFunc: func() { cancelled = true },
	}
	teardown := make(chan struct{})
	startTestRun(eng, rm, func() { <-teardown })
	eng.runningSchemaChange = rm

	halted := make(chan error, 1)
	go func() { halted <- eng.HaltForShutdown(t.Context()) }()

	select {
	case err := <-halted:
		require.Failf(t, "halt returned before the teardown ended", "error: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(teardown)
	select {
	case err := <-halted:
		require.NoError(t, err)
	case <-time.After(shutdownHaltTestDeadline):
		require.FailNow(t, "halt did not return once the teardown ended")
	}
	assert.False(t, cancelled, "a teardown is waited out, not cancelled")
}

// shutdownHaltTestDeadline bounds a wait for the halt to return.
const shutdownHaltTestDeadline = 5 * time.Second

// A drive halts the engine work it started the moment it returns, which can
// be right after the engine accepted the work. The run's cancel is in place by
// the time Apply returns, so that halt always reaches the run instead of
// finding it active with nothing to cancel.
func TestApplyPublishesTheRunsCancelBeforeReturning(t *testing.T) {
	eng := New(Config{})
	t.Cleanup(eng.Drain)

	result, err := eng.Apply(t.Context(), &engine.ApplyRequest{
		Database:    "testdb",
		Credentials: &engine.Credentials{DSN: "root:pass@tcp(127.0.0.1:1)/testdb"},
		Changes: []engine.SchemaChange{{
			Namespace:    "testdb",
			TableChanges: []engine.TableChange{{Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"}},
		}},
	})
	require.NoError(t, err)
	require.True(t, result.Accepted)

	eng.mu.Lock()
	rm := eng.runningSchemaChange
	var cancelRun context.CancelFunc
	if rm != nil {
		cancelRun = rm.cancelFunc
	}
	eng.mu.Unlock()
	require.NotNil(t, rm)
	assert.NotNil(t, cancelRun, "the accepted run can be cancelled as soon as Apply returns")
}

// One engine serves every drive of a target in this process. A drive that
// hands its apply back halts the run it started, and never the run a later
// drive has already started in its place.
func TestHaltWorkOwnedByLeavesAnotherDrivesRunRunning(t *testing.T) {
	eng := New(Config{})
	runCtx, cancelRun := context.WithCancel(t.Context())
	t.Cleanup(cancelRun)
	cancelled := false
	rm := &runningSchemaChange{
		database:   "orders",
		tables:     []string{"line_items"},
		state:      engine.StateRunning,
		owner:      "drive-b",
		cancelFunc: func() { cancelled = true; cancelRun() },
	}
	startTestRun(eng, rm, func() { <-runCtx.Done() })
	eng.runningSchemaChange = rm

	require.NoError(t, eng.HaltWorkOwnedBy(t.Context(), "drive-a"))
	assert.False(t, cancelled, "the run belongs to drive-b, so drive-a's halt leaves it running")
	assert.True(t, testRunAlive(eng, rm), "the run is still alive")

	require.NoError(t, eng.HaltWorkOwnedBy(t.Context(), "drive-b"))
	assert.True(t, cancelled, "drive-b's halt reaches the run drive-b started")
	assert.False(t, testRunAlive(eng, rm), "the halted run has returned")
}

// The run records the owner of the context it was started under, so the halt
// can tell whose run it is.
func TestApplyRecordsTheRunsOwner(t *testing.T) {
	eng := New(Config{})
	t.Cleanup(eng.Drain)

	result, err := eng.Apply(engine.WithWorkOwner(t.Context(), "drive-a"), &engine.ApplyRequest{
		Database:    "testdb",
		Credentials: &engine.Credentials{DSN: "root:pass@tcp(127.0.0.1:1)/testdb"},
		Changes: []engine.SchemaChange{{
			Namespace:    "testdb",
			TableChanges: []engine.TableChange{{Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"}},
		}},
	})
	require.NoError(t, err)
	require.True(t, result.Accepted)

	eng.mu.Lock()
	rm := eng.runningSchemaChange
	var owner string
	if rm != nil {
		owner = rm.owner
	}
	eng.mu.Unlock()
	require.NotNil(t, rm)
	assert.Equal(t, "drive-a", owner)
}

// A drive waits for an earlier schema change to exit only while it holds the
// apply. When its context ends first, DrainContext returns and the change
// stays tracked, so the engine still reports the target as held; once the run
// exits, a drain releases it.
func TestDrainContextEndsWithTheCallersContext(t *testing.T) {
	eng := New(Config{})
	exit := make(chan struct{})
	rm := &runningSchemaChange{
		database: "orders",
		tables:   []string{"line_items"},
		state:    engine.StateRunning,
	}
	startTestRun(eng, rm, func() { <-exit })
	eng.runningSchemaChange = rm

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err := eng.DrainContext(ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Contains(t, err.Error(), "line_items")
	assert.Same(t, rm, eng.runningSchemaChange, "a change still running stays tracked")

	close(exit)
	require.NoError(t, eng.DrainContext(t.Context()))
	assert.Nil(t, eng.runningSchemaChange)
}

// A drive whose context has already ended can still drain a schema change
// whose run has returned: with no runner alive there is nothing to wait for,
// so the drain releases the change and returns nil instead of the context's
// error. The run has returned and the context has ended before the drain
// starts, so the drain must prefer the return every time; the attempts repeat
// it so a drain that picked between the two at random would fail. A run that
// returns only after the wait has begun races the context, which no test
// can schedule; runExit.wait checks for the return again when the context
// ends for that case.
func TestDrainContextWithAnEndedCallerAndNoRunnerAliveReleasesTheChange(t *testing.T) {
	ended, cancel := context.WithCancel(t.Context())
	cancel()
	for attempt := range 100 {
		eng := New(Config{})
		rm := &runningSchemaChange{
			database: "orders",
			tables:   []string{"line_items"},
			state:    engine.StateCompleted,
		}
		startTestRun(eng, rm, func() {})
		eng.runningSchemaChange = rm
		waitForTestRun(t, eng, rm)

		require.NoError(t, eng.DrainContext(ended), "attempt %d", attempt)
		assert.Nil(t, eng.runningSchemaChange, "attempt %d: the drained change is released", attempt)
	}
}

// A run publishes its tables under the engine lock after it is tracked, so a
// drain that gives up on a run still starting reads them under the same lock
// when it names the run in its error.
func TestDrainContextNamesARunThatIsStillPublishingItsTables(t *testing.T) {
	eng := New(Config{})
	exit := make(chan struct{})
	rm := &runningSchemaChange{
		database: "orders",
		state:    engine.StateRunning,
	}
	startTestRun(eng, rm, func() {
		for {
			select {
			case <-exit:
				return
			default:
			}
			eng.mu.Lock()
			rm.tables = []string{"line_items"}
			eng.mu.Unlock()
			runtime.Gosched()
		}
	})
	eng.runningSchemaChange = rm

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err := eng.DrainContext(ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Contains(t, err.Error(), "database orders")
	close(exit)
	require.NoError(t, eng.DrainContext(t.Context()))
}

// Spirit drains the previous schema change before it starts the next. That
// wait is bounded by the caller's context, so a drive cancelled behind a run
// that will not exit gets an error back instead of blocking.
func TestApplyStopsWaitingForThePreviousRunWithTheCallersContext(t *testing.T) {
	eng := New(Config{})
	exit := make(chan struct{})
	t.Cleanup(func() { close(exit) })
	rm := &runningSchemaChange{
		database: "testdb",
		tables:   []string{"users"},
		state:    engine.StateRunning,
	}
	startTestRun(eng, rm, func() { <-exit })
	eng.runningSchemaChange = rm

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err := eng.Apply(ctx, &engine.ApplyRequest{
		Database:    "testdb",
		Credentials: &engine.Credentials{DSN: "root:pass@tcp(127.0.0.1:1)/testdb"},
		Changes: []engine.SchemaChange{{
			Namespace:    "testdb",
			TableChanges: []engine.TableChange{{Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"}},
		}},
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Contains(t, err.Error(), "previous schema change")
	assert.Same(t, rm, eng.runningSchemaChange, "the earlier run is not replaced")
}
