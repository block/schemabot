package webhook

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// The self-scheduled re-fold budget caps consecutive arms per PR so a
// participant that never reports cannot keep a timer chain alive forever, and
// a fold that resolves every participant resets the budget so a later commit
// on the same PR converges again.
func TestParticipantRefoldBudget(t *testing.T) {
	const (
		repo = "octocat/shared-repo"
		pr   = 7
	)
	h := &Handler{logger: testLogger()}
	// Keep armed timers from firing inside the test process; pending timers at
	// process exit are inert.
	h.participantRefoldDelayOverride = time.Hour

	attempts := func() (int, bool) {
		h.participantRefoldMu.Lock()
		defer h.participantRefoldMu.Unlock()
		n, ok := h.participantRefoldAttempts[participantRefoldKey(repo, pr)]
		return n, ok
	}

	for i := range maxParticipantRefoldAttempts {
		h.scheduleParticipantRefold(t.Context(), repo, pr, 42)
		n, ok := attempts()
		assert.True(t, ok)
		assert.Equal(t, i+1, n)
	}

	// The budget is spent: further schedules arm nothing and do not grow the
	// counter.
	h.scheduleParticipantRefold(t.Context(), repo, pr, 42)
	n, _ := attempts()
	assert.Equal(t, maxParticipantRefoldAttempts, n)

	// A fold that resolves every participant clears the budget, so a later
	// commit gets fresh attempts.
	h.clearParticipantRefoldBudget(repo, pr)
	_, ok := attempts()
	assert.False(t, ok)

	h.scheduleParticipantRefold(t.Context(), repo, pr, 42)
	n, _ = attempts()
	assert.Equal(t, 1, n)
}
