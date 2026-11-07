package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The first redispatch after a failure is immediate, each later one waits
// twice as long as the one before, and the wait never exceeds the cap.
func TestRecoveryBackoff(t *testing.T) {
	assert.Zero(t, RecoveryBackoff(0))
	assert.Equal(t, RecoveryBackoffBase, RecoveryBackoff(1))
	assert.Equal(t, 2*RecoveryBackoffBase, RecoveryBackoff(2))
	assert.Equal(t, 4*RecoveryBackoffBase, RecoveryBackoff(3))

	previous := RecoveryBackoff(0)
	for attempt := 1; attempt <= MaxRecoveryAttempts; attempt++ {
		backoff := RecoveryBackoff(attempt)
		assert.GreaterOrEqual(t, backoff, previous, "attempt %d", attempt)
		assert.LessOrEqual(t, backoff, MaxRecoveryBackoff, "attempt %d", attempt)
		previous = backoff
	}
	assert.Equal(t, MaxRecoveryBackoff, RecoveryBackoff(MaxRecoveryAttempts))
}
