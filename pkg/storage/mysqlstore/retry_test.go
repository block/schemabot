package mysqlstore

import (
	"context"
	"errors"
	"fmt"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsRetryableLockError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"deadlock 1213", &gomysql.MySQLError{Number: mysqlErrDeadlock, Message: "Deadlock found"}, true},
		{"lock wait timeout 1205", &gomysql.MySQLError{Number: mysqlErrLockWaitTimeout, Message: "Lock wait timeout exceeded"}, true},
		{"wrapped deadlock", fmt.Errorf("insert lock: %w", &gomysql.MySQLError{Number: mysqlErrDeadlock}), true},
		{"duplicate key 1062", &gomysql.MySQLError{Number: 1062, Message: "Duplicate entry"}, false},
		{"non-mysql error", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isRetryableLockError(tt.err))
		})
	}
}

func TestWithLockRetry_SucceedsFirstAttempt(t *testing.T) {
	calls := 0
	err := withLockRetry(t.Context(), "op", func() error {
		calls++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
}

func TestWithLockRetry_RetriesThenSucceeds(t *testing.T) {
	calls := 0
	err := withLockRetry(t.Context(), "op", func() error {
		calls++
		if calls < 3 {
			return &gomysql.MySQLError{Number: mysqlErrDeadlock, Message: "Deadlock found"}
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, calls)
}

func TestWithLockRetry_ExhaustsAttempts(t *testing.T) {
	calls := 0
	err := withLockRetry(t.Context(), "acquire something", func() error {
		calls++
		return &gomysql.MySQLError{Number: mysqlErrLockWaitTimeout, Message: "Lock wait timeout exceeded"}
	})
	require.Error(t, err)
	assert.Equal(t, lockRetryMaxAttempts, calls)
	assert.Contains(t, err.Error(), "acquire something")
	assert.Contains(t, err.Error(), fmt.Sprintf("after %d attempts", lockRetryMaxAttempts))
	// The terminal lock error is preserved in the chain.
	assert.True(t, isRetryableLockError(err))
}

func TestWithLockRetry_NonRetryableReturnsImmediately(t *testing.T) {
	sentinel := errors.New("genuine conflict")
	calls := 0
	err := withLockRetry(t.Context(), "op", func() error {
		calls++
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)
	assert.Equal(t, 1, calls, "non-retryable error must not be retried")
}

func TestWithLockRetry_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	calls := 0
	err := withLockRetry(ctx, "op", func() error {
		calls++
		return &gomysql.MySQLError{Number: mysqlErrDeadlock, Message: "Deadlock found"}
	})
	require.ErrorIs(t, err, context.Canceled)
	// First attempt runs (no pre-attempt wait); the cancelled context stops the
	// backoff before a second attempt.
	assert.Equal(t, 1, calls)
}
