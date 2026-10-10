package tern

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/storage"
)

// A statement killed by the release's deadline can come back as the SQL
// driver's own error rather than the context's. The release's clock, not the
// error text, decides whether the release ran out of time.
func TestReleaseOutranItsHold(t *testing.T) {
	expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()

	assert.True(t, releaseOutranItsHold(expired, fmt.Errorf("drop checkpoint: %w", driver.ErrBadConn)),
		"a driver error under an expired hold is a release that ran out of time")
	assert.False(t, releaseOutranItsHold(expired, fmt.Errorf("drop checkpoint: %w", context.DeadlineExceeded)),
		"an error that already says it ran out of time needs nothing added")
	assert.False(t, releaseOutranItsHold(t.Context(), fmt.Errorf("drop checkpoint: %w", driver.ErrBadConn)),
		"a failure inside the hold is not a timeout")
}

// The operator-facing reason for a skipped release follows the cause, and a
// timeout reported as a driver error reads as a timeout once the release has
// said so.
func TestSkippedArtifactReleaseReason(t *testing.T) {
	expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()

	driverTimeout := fmt.Errorf("drop checkpoint: %w", driver.ErrBadConn)
	if releaseOutranItsHold(expired, driverTimeout) {
		driverTimeout = fmt.Errorf("release ran past its hold: %w", errors.Join(context.DeadlineExceeded, driverTimeout))
	}

	tests := []struct {
		name string
		err  error
		want string
	}{
		{"another apply owns the target", fmt.Errorf("hold target: %w", storage.ErrActiveApplyExists),
			"another schema change is running against the same target and may own the copy"},
		{"timeout surfaced as a driver error", driverTimeout,
			"the release ran past the time it is allowed to hold the target, so it gave the target back"},
		{"stopped part-way", &artifactReleaseError{namespace: "testdb", reclaimedAny: true, err: driver.ErrBadConn},
			"the release failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, skippedArtifactReleaseReason(tt.err))
		})
	}
}
