package webhook

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// renderIfDue runs one progress tick against the cadence and, when the tick is
// due, records a shown edit, as OnProgress does after GitHub accepts it.
func renderIfDue(o *CommentObserver, now time.Time, snapshot progressSnapshot) bool {
	cadence, due := o.progressCommentDue(now, snapshot)
	if due {
		o.recordProgressEdit(now, snapshot, cadence, trackedCommentEdit{shown: true})
	}
	return due
}

// An engine that executes a statement sequence reports no row counts, so the
// statement position is the only progress figure that moves. Advancing to the
// next statement must count as movement: the comment is refreshed at the active
// cadence, on the first due tick after the position changed, rather than being
// treated as stagnant and refreshed only at the slow interval.
func TestProgressCommentDueRendersStepAdvanceAtActiveCadence(t *testing.T) {
	const tick = 500 * time.Millisecond
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	running := func(steps string) progressSnapshot {
		return progressSnapshot{state: state.Apply.Running, rowsCopied: 0, steps: steps}
	}
	o := &CommentObserver{}

	require.True(t, renderIfDue(o, t0, running("1:1/3;")), "first tick renders")

	// The step advances one second after the render. Every tick inside the
	// active interval keeps reading as movement, so the first tick at the
	// interval renders the new position.
	for now := t0.Add(time.Second); now.Before(t0.Add(activeInterval)); now = now.Add(tick) {
		assert.False(t, renderIfDue(o, now, running("1:2/3;")), "inside the active interval at %s", now.Sub(t0))
	}
	require.True(t, renderIfDue(o, t0.Add(activeInterval), running("1:2/3;")), "step advance renders at the active interval")
	assert.Equal(t, running("1:2/3;"), o.lastRendered)

	// Held still at step 2 the comment goes stagnant and slows to the long
	// interval; another advance is then rendered on its first tick, because the
	// active interval has long since elapsed.
	stagnantFrom := t0.Add(activeInterval)
	for i := 1; i <= stagnantThresh+2; i++ {
		assert.False(t, renderIfDue(o, stagnantFrom.Add(time.Duration(i)*tick), running("1:2/3;")))
	}
	assert.False(t, renderIfDue(o, stagnantFrom.Add(activeInterval+time.Second), running("1:2/3;")), "stagnant position waits for the slow interval")
	assert.True(t, renderIfDue(o, stagnantFrom.Add(activeInterval+2*time.Second), running("1:3/3;")), "a further advance renders at once")
}

// A state change is rendered on the tick it is observed, even inside the active
// interval, and a snapshot that is byte-for-byte what the comment last showed
// is stagnant regardless of how many ticks report it.
func TestProgressCommentDueStateChangeRendersImmediately(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	o := &CommentObserver{}
	copying := progressSnapshot{state: state.Apply.Running, rowsCopied: 100}

	require.True(t, renderIfDue(o, t0, copying))
	assert.False(t, renderIfDue(o, t0.Add(time.Second), progressSnapshot{state: state.Apply.Running, rowsCopied: 150}), "row growth inside the interval waits")
	assert.True(t, renderIfDue(o, t0.Add(2*time.Second), progressSnapshot{state: state.Apply.Checksumming, rowsCopied: 150}), "state change renders at once")

	for i := 1; i <= stagnantThresh+1; i++ {
		assert.False(t, renderIfDue(o, t0.Add(2*time.Second+time.Duration(i)*500*time.Millisecond), progressSnapshot{state: state.Apply.Checksumming, rowsCopied: 150}))
	}
	assert.Equal(t, stagnantThresh+1, o.stagnantTicks)
}

// The fingerprint changes exactly when an operation's statement position does,
// is keyed per operation so sibling deployments cannot mask each other, and
// ignores operations whose stored metadata carries no readable position.
func TestProgressFingerprintTracksPositionPerOperation(t *testing.T) {
	ops := func(euStep, usStep string) []*storage.ApplyOperation {
		return []*storage.ApplyOperation{
			{ID: 1, Deployment: "eu", ProgressMetadata: `{"step":"` + euStep + `","steps_total":"3"}`},
			{ID: 2, Deployment: "us", ProgressMetadata: `{"step":"` + usStep + `","steps_total":"3"}`},
		}
	}

	assert.Equal(t, "1:1/3;2:1/3;", progressFingerprint(ops("1", "1")))
	assert.NotEqual(t, progressFingerprint(ops("1", "1")), progressFingerprint(ops("2", "1")), "eu advancing is movement")
	assert.NotEqual(t, progressFingerprint(ops("2", "1")), progressFingerprint(ops("1", "2")), "the same steps on swapped deployments are different positions")

	unreadable := []*storage.ApplyOperation{
		{ID: 1, ProgressMetadata: `{"step":`},
		{ID: 2, ProgressMetadata: `{"step":"4","steps_total":"3"}`},
		{ID: 3, ProgressMetadata: `{"phase":"preflight"}`},
		{ID: 4},
	}
	assert.Empty(t, progressFingerprint(unreadable))
	assert.Empty(t, progressFingerprint(nil))
}

// A concurrent index build holds its statement position for the whole build
// while the server's block, tuple, and locker counters, its phase, and its
// attempt move, so each of those is movement in the fingerprint; a position
// with no build work fingerprints as before.
func TestProgressFingerprintTracksBuildWork(t *testing.T) {
	building := func(extra string) []*storage.ApplyOperation {
		return []*storage.ApplyOperation{{ID: 1, Deployment: "eu",
			ProgressMetadata: `{"step":"2","steps_total":"3","executor_operation":"concurrent-index-build","server_phase":"building index: scanning table",` + extra + `}`}}
	}
	scanning := progressFingerprint(building(`"blocks_done":"2500","blocks_total":"10000"`))
	assert.Equal(t, "1:2/3 concurrent-index-build@building index: scanning table#0 b2500/10000 t0/0 l0/0;", scanning)
	assert.NotEqual(t, scanning, progressFingerprint(building(`"blocks_done":"2600","blocks_total":"10000"`)), "blocks advancing is movement")
	assert.NotEqual(t, scanning, progressFingerprint(building(`"blocks_done":"2500","blocks_total":"10000","attempt":"2"`)), "a retry is movement")

	waiting := func(lockersDone string) []*storage.ApplyOperation {
		return []*storage.ApplyOperation{{ID: 1, Deployment: "eu",
			ProgressMetadata: `{"step":"2","steps_total":"3","executor_operation":"concurrent-index-build","server_phase":"waiting for writers before build","lockers_done":"` + lockersDone + `","lockers_total":"3"}`}}
	}
	assert.NotEqual(t, scanning, progressFingerprint(waiting("0")), "a phase change is movement")
	assert.NotEqual(t, progressFingerprint(waiting("0")), progressFingerprint(waiting("1")), "a locker releasing is movement")

	assert.Equal(t, "1:2/3;", progressFingerprint([]*storage.ApplyOperation{{ID: 1, ProgressMetadata: `{"step":"2","steps_total":"3"}`}}))
	assert.Equal(t, "1:0/0 concurrent-index-build@waiting for old snapshots#0 b0/0 t0/0 l1/3;",
		progressFingerprint([]*storage.ApplyOperation{{ID: 1, ProgressMetadata: `{"executor_operation":"concurrent-index-build","server_phase":"waiting for old snapshots","lockers_done":"1","lockers_total":"3"}`}}),
		"build work without a position is still movement")
	assert.Empty(t, progressFingerprint(building(`"blocks_done":"many"`)), "a malformed counter contributes nothing")
}

// The progress comment's edit spacing grows with the comment's age, never
// shrinks, and tops out at the unchanged-comment heartbeat, so an old comment
// still refreshes at least as often as its liveness footer promises.
func TestProgressEditIntervalDecaysWithAge(t *testing.T) {
	cases := []struct {
		age  time.Duration
		want time.Duration
	}{
		{0, 5 * time.Second},
		{9 * time.Minute, 5 * time.Second},
		{10 * time.Minute, 30 * time.Second},
		{59 * time.Minute, 30 * time.Second},
		{time.Hour, time.Minute},
		{2 * time.Hour, 2 * time.Minute},
		{4 * time.Hour, 5 * time.Minute},
		{6 * time.Hour, 10 * time.Minute},
		{72 * time.Hour, 10 * time.Minute},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, progressEditInterval(c.age), "age %s", c.age)
	}
	assert.Equal(t, unchangedCommentHeartbeat, progressEditInterval(72*time.Hour))
	assert.Equal(t, progressEditInterval(0), freshProgressCommentCadence())

	prev := time.Duration(0)
	for age := time.Duration(0); age <= 8*time.Hour; age += time.Minute {
		got := progressEditInterval(age)
		require.GreaterOrEqual(t, got, prev, "spacing must not shrink as the comment ages (age %s)", age)
		prev = got
	}
}

// A rollout member or table changing state is rendered on the tick it is
// observed, even when the apply's own state has not moved and the comment is
// old enough that row progress waits minutes.
func TestProgressCommentDueMemberStateChangeRendersImmediately(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	o := &CommentObserver{progressCommentCreatedAt: t0.Add(-3 * time.Hour)}
	copying := progressSnapshot{state: state.Apply.Running, rowsCopied: 100, memberStates: "o1=running;o2=pending;"}

	require.True(t, renderIfDue(o, t0, copying))
	assert.False(t, renderIfDue(o, t0.Add(time.Second), progressSnapshot{state: state.Apply.Running, rowsCopied: 150, memberStates: "o1=running;o2=pending;"}),
		"row growth on an old comment waits for its spacing")
	assert.True(t, renderIfDue(o, t0.Add(2*time.Second), progressSnapshot{state: state.Apply.Running, rowsCopied: 150, memberStates: "o1=completed;o2=running;"}),
		"a member state change renders at once")
}

// An edit that does not reach GitHub leaves the snapshot it carried pending:
// the comment has not rendered it, so the state change is still urgent on the
// next tick. The retry waits out the fastest spacing so an outage does not
// turn every tick into a request.
func TestRecordProgressEditFailedEditKeepsChangePending(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	o := &CommentObserver{progressCommentCreatedAt: t0.Add(-3 * time.Hour)}
	running := progressSnapshot{state: state.Apply.Running, rowsCopied: 100}
	cutover := progressSnapshot{state: state.Apply.WaitingForCutover, rowsCopied: 100}

	require.True(t, renderIfDue(o, t0, running))

	failedAt := t0.Add(time.Second)
	_, due := o.progressCommentDue(failedAt, cutover)
	require.True(t, due, "the state change is due at once")
	o.recordProgressEdit(failedAt, cutover, freshProgressCommentCadence(), trackedCommentEdit{shown: false})
	assert.Equal(t, running, o.lastRendered, "a failed edit does not count as rendered")
	assert.Equal(t, t0, o.lastProgressPost, "a failed edit does not reset the spacing")

	_, due = o.progressCommentDue(failedAt.Add(freshProgressCommentCadence()-time.Second), cutover)
	assert.False(t, due, "the retry waits out the hold-back")
	_, due = o.progressCommentDue(failedAt.Add(freshProgressCommentCadence()), cutover)
	assert.True(t, due, "the pending state change is retried after the hold-back")
}

// A shown edit carries the comment's creation time from GitHub, which replaces
// the observer's own estimate, so a drive that took over an old comment
// resumes at that comment's spacing.
func TestRecordProgressEditLearnsCommentAge(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	o := &CommentObserver{}
	snapshot := progressSnapshot{state: state.Apply.Running, rowsCopied: 100}

	cadence, due := o.progressCommentDue(t0, snapshot)
	require.True(t, due)
	assert.Equal(t, freshProgressCommentCadence(), cadence, "an unknown age starts at the fastest spacing")

	o.recordProgressEdit(t0, snapshot, cadence, trackedCommentEdit{shown: true, commentCreatedAt: t0.Add(-3 * time.Hour)})
	cadence, _ = o.progressCommentDue(t0.Add(time.Second), snapshot)
	assert.Equal(t, 2*time.Minute, cadence, "the spacing follows the age GitHub reported")

	o.restartProgressCommentAge(t0.Add(2 * time.Second))
	cadence, _ = o.progressCommentDue(t0.Add(3*time.Second), snapshot)
	assert.Equal(t, freshProgressCommentCadence(), cadence, "a rotation starts the new comment at the fastest spacing")
}

// The footer promises how long a change can take to show, so a longer spacing
// is published before it applies. When the comment's age crosses into a slower
// step, the next tick edits even though nothing moved, and the spacing that
// tick reports is the one the new footer shows.
func TestProgressCommentDuePublishesLongerSpacingBeforeApplyingIt(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	o := &CommentObserver{progressCommentCreatedAt: t0.Add(-10*time.Minute + time.Second)}
	snapshot := progressSnapshot{state: state.Apply.Running, rowsCopied: 100}

	require.True(t, renderIfDue(o, t0, snapshot))
	assert.Equal(t, 5*time.Second, o.progressShownCadence)

	cadence, due := o.progressCommentDue(t0.Add(2*time.Second), snapshot)
	assert.Equal(t, 30*time.Second, cadence)
	assert.True(t, due, "crossing into a slower step publishes the new footer at once")
	o.recordProgressEdit(t0.Add(2*time.Second), snapshot, cadence, trackedCommentEdit{shown: true})
	assert.Equal(t, 30*time.Second, o.progressShownCadence)

	_, due = o.progressCommentDue(t0.Add(4*time.Second), snapshot)
	assert.False(t, due, "once the footer shows the longer spacing, it applies")

	// A rotation resets the promise to the fastest spacing with the fresh
	// comment, so a shorter spacing never forces an edit.
	o.restartProgressCommentAge(t0.Add(5 * time.Second))
	assert.Equal(t, freshProgressCommentCadence(), o.progressShownCadence)
	_, due = o.progressCommentDue(t0.Add(6*time.Second), snapshot)
	assert.False(t, due, "a faster spacing than the footer promises needs no edit")
}
