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
// assert how the release was bounded. With hangRelease set, Release blocks
// until its context ends, as a release on a half-open connection does.
type scriptedLocker struct {
	acquired    bool
	acquireErr  error
	released    bool
	releaseErr  error
	hangRelease bool
	releaseCall int

	releaseDeadline    time.Time
	releaseHasDeadline bool
	releaseCtxErr      error
}

func (l *scriptedLocker) Acquire(context.Context, *sql.Conn, string, time.Duration) (bool, error) {
	return l.acquired, l.acquireErr
}

func (l *scriptedLocker) Release(ctx context.Context, _ *sql.Conn, _ string) (bool, error) {
	l.releaseCall++
	l.releaseDeadline, l.releaseHasDeadline = ctx.Deadline()
	l.releaseCtxErr = ctx.Err()
	if l.hangRelease {
		<-ctx.Done()
		return false, ctx.Err()
	}
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

// Every way a reaper pass ends hands its pinned session back, either to the
// pool or destroyed: a session left checked out would pin one pool connection
// per pass for good.
func TestReapUnderElection_ReturnsSessionToPool(t *testing.T) {
	tests := []struct {
		name   string
		locker *scriptedLocker
		want   error
	}{
		{"clean pass", &scriptedLocker{acquired: true, released: true}, nil},
		{"lock busy", &scriptedLocker{acquired: false}, testReaperSweep.busy},
		{"acquire fails", &scriptedLocker{acquireErr: errAcquireFailed}, errAcquireFailed},
		{"release fails", &scriptedLocker{acquired: true, releaseErr: errors.New("connection reset by peer")}, nil},
		{"lock not held at release", &scriptedLocker{acquired: true, released: false}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdb, _ := newRecordingRebindDB(t, &countingBinder{})

			_, err := reapUnderElection(t.Context(), rdb, tt.locker, testReaperSweep, noopReap)
			if tt.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.want)
			}

			assert.Equal(t, 0, rdb.pool.Stats().InUse, "no pinned session outlives the pass")
		})
	}
}

var errAcquireFailed = errors.New("lock service unavailable")

// A release that blocks until its context ends, as one on a half-open
// connection does, gives up at the bound: the pass returns, and the session is
// destroyed rather than pooled or left pinned.
func TestReapUnderElection_HungReleaseGivesUpAtTheBound(t *testing.T) {
	saved := lockReleaseTimeout
	lockReleaseTimeout = 50 * time.Millisecond
	t.Cleanup(func() { lockReleaseTimeout = saved })
	rdb, connector := newRecordingRebindDB(t, &countingBinder{})
	locker := &scriptedLocker{acquired: true, hangRelease: true}

	done := make(chan error, 1)
	go func() {
		_, err := reapUnderElection(t.Context(), rdb, locker, testReaperSweep, noopReap)
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the pass did not return after the release bound passed")
	}

	assert.Equal(t, 1, connector.closes, "the session of a hung release is destroyed, not pooled")
	assert.Equal(t, 0, rdb.pool.Stats().InUse, "no pinned session outlives the pass")
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
