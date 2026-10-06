package postgres

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// A drive waits for in-flight apply goroutines only while it holds the apply.
// When its context ends first, DrainContext returns and the tracked progress
// stays, so the engine still reports the work; once the goroutines finish, a
// drain clears it.
func TestDrainContextEndsWithTheCallersContext(t *testing.T) {
	eng := New()
	exit := make(chan struct{})
	eng.wg.Go(func() { <-exit })
	eng.progress = map[string]*trackedApply{"apply-1": {}}

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err := eng.DrainContext(ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Len(t, eng.progress, 1, "work still running stays tracked")

	close(exit)
	require.NoError(t, eng.DrainContext(t.Context()))
	assert.Empty(t, eng.progress)
}

// A drain clears only the work it waited out. An apply accepted while the
// drain waits belongs to the drive that started it, and that drive's poller
// must still find its progress, whether or not the apply has finished.
func TestDrainKeepsAnApplyAcceptedWhileItWaits(t *testing.T) {
	eng := New()
	earlierDone := make(chan struct{})
	close(earlierDone)
	stillRunning := make(chan struct{})
	eng.progress = map[string]*trackedApply{
		"earlier":       {result: &engine.ProgressResult{State: engine.StateCompleted}, done: earlierDone},
		"still-running": {result: &engine.ProgressResult{State: engine.StateRunning}, done: stillRunning},
	}

	drained := eng.trackedAtDrainStart()
	acceptedDone := eng.claimProgress("accepted", &engine.ProgressResult{State: engine.StateRunning}, nil, slog.Default(), false, func() {})
	close(acceptedDone)
	eng.clearDrainedProgress(drained)

	assert.NotContains(t, eng.progress, "earlier", "the drained apply has exited, so it is cleared")
	assert.Contains(t, eng.progress, "still-running", "an apply whose goroutine has not returned stays tracked")
	assert.Contains(t, eng.progress, "accepted", "an apply accepted during the drain is not the drain's to clear")
}
