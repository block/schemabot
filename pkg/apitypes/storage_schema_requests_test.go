package apitypes

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A request that names no budget runs under whatever the resolving side hands
// in as the unnamed budget — the boot budget on a server, the operator default
// in a client — rather than under a value this function chooses. The two ends
// of a request agree on the ceiling because the client names the one it waits
// for; a server left to pick its own would answer "too slow" differently from
// the caller holding the terminal.
func TestResolveStorageApplyTimeout_ZeroRunsUnderTheUnnamedBudget(t *testing.T) {
	t.Parallel()

	for name, unnamed := range map[string]time.Duration{
		"a server's boot budget":      90 * time.Second,
		"a client's operator default": DefaultStorageApplyTimeout,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			budget, err := ResolveStorageApplyTimeout(0, unnamed)
			require.NoError(t, err)
			assert.Equal(t, unnamed, budget)
		})
	}
}

// A budget the caller names is honored exactly, never replaced by the unnamed
// budget, so a command can report the ceiling it is running under and be right.
func TestResolveStorageApplyTimeout_HonorsARequestedBudget(t *testing.T) {
	t.Parallel()

	budget, err := ResolveStorageApplyTimeout(90, DefaultStorageApplyTimeout)
	require.NoError(t, err)
	assert.Equal(t, 90*time.Second, budget)
}

// Out of range is refused, never clamped. An operator quietly given a smaller
// budget than they asked for watches the convergence fail at a ceiling they
// did not choose, with nothing in the output to explain it.
func TestResolveStorageApplyTimeout_RefusesOutOfRange(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		seconds int64
		message string
	}{
		"negative": {seconds: -1, message: "must be positive"},
		"above the maximum": {
			seconds: int64(MaxStorageApplyTimeout/time.Second) + 1,
			message: "exceeds the maximum",
		},
		// Bounding after multiplying by time.Second would wrap this into a
		// small or negative duration and hand it back as a budget the caller
		// never named, which is the one way past a bound stated in seconds.
		"beyond what a duration can hold": {
			seconds: math.MaxInt64,
			message: "exceeds the maximum",
		},
		"one second short of the overflow": {
			seconds: math.MaxInt64/int64(time.Second) + 1,
			message: "exceeds the maximum",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := ResolveStorageApplyTimeout(tc.seconds, DefaultStorageApplyTimeout)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.message)
		})
	}
}

// The maximum does not exceed the default while a running convergence cannot
// be stopped. The context bounding a convergence is built so that a caller
// hanging up cannot abandon a table copy, which leaves the budget as the only
// thing that ends one early — so the longest budget a request can name is also
// the longest a mistake can hold the storage bootstrap lock and keep pods from
// booting. Raising the maximum belongs with the ability to cancel.
func TestStorageApplyMaximumDoesNotExceedTheDefault(t *testing.T) {
	t.Parallel()

	assert.LessOrEqual(t, MaxStorageApplyTimeout, DefaultStorageApplyTimeout,
		"a budget no operator can take back must not be raisable past the default")
}
