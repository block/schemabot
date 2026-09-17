package apitypes

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A request that names no budget takes the operator default, not the budget a
// booting pod runs under. This is the whole point of the field existing: the
// deliberate path and the boot path answer "too slow" differently, because a
// pod converging is a pod not yet serving and an operator at a terminal is
// nobody's outage.
func TestResolveStorageApplyTimeout_ZeroTakesTheOperatorDefault(t *testing.T) {
	t.Parallel()

	budget, err := ResolveStorageApplyTimeout(0)
	require.NoError(t, err)
	assert.Equal(t, DefaultStorageApplyTimeout, budget)
}

// A budget the caller names is honored exactly, so a command can report the
// ceiling it is running under and be right.
func TestResolveStorageApplyTimeout_HonorsARequestedBudget(t *testing.T) {
	t.Parallel()

	budget, err := ResolveStorageApplyTimeout(90)
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
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := ResolveStorageApplyTimeout(tc.seconds)
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
