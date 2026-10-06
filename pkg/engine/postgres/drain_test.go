package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	assert.Nil(t, eng.progress)
}
