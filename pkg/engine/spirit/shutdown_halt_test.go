package spirit

import (
	"context"
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
	rm.wg.Go(func() {
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
	rm.wg.Go(func() { <-stuck })
	eng.runningSchemaChange = rm

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	err := eng.HaltForShutdown(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "orders", "the failure names the target that may still be locked")
	assert.Contains(t, err.Error(), "line_items")
}
