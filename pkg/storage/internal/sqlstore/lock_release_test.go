package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scriptedLocker is a namedlock.Locker whose Acquire and Release answers are
// fixed by the test. It records the context Release was handed so tests can
// assert how the release was bounded.
type scriptedLocker struct {
	acquired    bool
	released    bool
	releaseErr  error
	releaseCall int

	releaseDeadline    time.Time
	releaseHasDeadline bool
	releaseCtxErr      error
}

func (l *scriptedLocker) Acquire(context.Context, *sql.Conn, string, time.Duration) (bool, error) {
	return l.acquired, nil
}

func (l *scriptedLocker) Release(ctx context.Context, _ *sql.Conn, _ string) (bool, error) {
	l.releaseCall++
	l.releaseDeadline, l.releaseHasDeadline = ctx.Deadline()
	l.releaseCtxErr = ctx.Err()
	return l.released, l.releaseErr
}

func (l *scriptedLocker) HeldByAnySession(context.Context, *sql.Conn, string) (bool, error) {
	return false, nil
}

var testReaperSweep = strandedSweep{
	lockName: "schemabot_test_reaper",
	busy:     errors.New("another instance holds the reaper lock"),
	subject:  "test sweeps",
}

func noopReap(context.Context) ([]int, error) { return nil, nil }

// A reaper pass that finishes with a healthy release hands its session back to
// the pool, so a clean pass costs no new connection.
func TestReapUnderElection_HealthyReleaseKeepsSession(t *testing.T) {
	rdb, connector := newRecordingRebindDB(t, &countingBinder{})
	locker := &scriptedLocker{acquired: true, released: true}

	_, err := reapUnderElection(t.Context(), rdb, locker, testReaperSweep, noopReap)
	require.NoError(t, err)

	assert.Equal(t, 1, locker.releaseCall)
	assert.Equal(t, 0, connector.closes, "a cleanly released session returns to the pool")
}

// A session whose release errored may still hold the election lock, so the pool
// must destroy it rather than hand it to the next caller while every other
// instance's reaper stays parked behind it.
func TestReapUnderElection_ReleaseFailureRetiresSession(t *testing.T) {
	rdb, connector := newRecordingRebindDB(t, &countingBinder{})
	locker := &scriptedLocker{acquired: true, releaseErr: errors.New("connection reset by peer")}

	_, err := reapUnderElection(t.Context(), rdb, locker, testReaperSweep, noopReap)
	require.NoError(t, err)

	assert.Equal(t, 1, locker.releaseCall)
	assert.Equal(t, 1, connector.closes, "a session whose release failed is destroyed, not pooled")
}

// A release that reports the lock was not held leaves the session in an unknown
// state, so it is retired just like a failed release.
func TestReapUnderElection_ReleaseNotHeldRetiresSession(t *testing.T) {
	rdb, connector := newRecordingRebindDB(t, &countingBinder{})
	locker := &scriptedLocker{acquired: true, released: false}

	_, err := reapUnderElection(t.Context(), rdb, locker, testReaperSweep, noopReap)
	require.NoError(t, err)

	assert.Equal(t, 1, locker.releaseCall)
	assert.Equal(t, 1, connector.closes, "a session that was not holding the lock is destroyed, not pooled")
}

// The release runs after the pass's context may already be cancelled, and it
// must still be bounded: a release stuck on a half-open connection would
// otherwise never return, and the pool cannot retire a connection in use.
func TestReapUnderElection_ReleaseIsBoundedAndSurvivesCancellation(t *testing.T) {
	rdb, _ := newRecordingRebindDB(t, &countingBinder{})
	locker := &scriptedLocker{acquired: true, released: true}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	_, err := reapUnderElection(ctx, rdb, locker, testReaperSweep, func(context.Context) ([]int, error) {
		cancel()
		return nil, nil
	})
	require.NoError(t, err)

	require.True(t, locker.releaseHasDeadline, "the release must carry a deadline")
	assert.LessOrEqual(t, time.Until(locker.releaseDeadline), lockReleaseTimeout)
	assert.NoError(t, locker.releaseCtxErr, "the release must not inherit the pass's cancellation")
}

// An instance that loses the election never held the lock, so it has nothing to
// release and its session goes straight back to the pool.
func TestReapUnderElection_NotAcquiredSkipsRelease(t *testing.T) {
	rdb, connector := newRecordingRebindDB(t, &countingBinder{})
	locker := &scriptedLocker{acquired: false}

	_, err := reapUnderElection(t.Context(), rdb, locker, testReaperSweep, noopReap)
	require.ErrorIs(t, err, testReaperSweep.busy)

	assert.Equal(t, 0, locker.releaseCall)
	assert.Equal(t, 0, connector.closes)
}

// The apply target lock path and the reaper share one release routine, so a
// failed or unheld release retires the session on both.
func TestReleaseApplyTargetLockConn_RetiresSessionOnFailedRelease(t *testing.T) {
	tests := []struct {
		name       string
		locker     *scriptedLocker
		wantCloses int
	}{
		{"healthy release keeps the session", &scriptedLocker{released: true}, 0},
		{"release error retires the session", &scriptedLocker{releaseErr: errors.New("connection reset by peer")}, 1},
		{"lock not held retires the session", &scriptedLocker{released: false}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdb, connector := newRecordingRebindDB(t, &countingBinder{})
			conn, err := rdb.Conn(t.Context())
			require.NoError(t, err)

			releaseApplyTargetLockConn(t.Context(), tt.locker, conn, "schemabot_apply_test", "test operation")

			require.True(t, tt.locker.releaseHasDeadline, "the release must carry a deadline")
			assert.LessOrEqual(t, time.Until(tt.locker.releaseDeadline), lockReleaseTimeout)
			assert.Equal(t, tt.wantCloses, connector.closes)
		})
	}
}
