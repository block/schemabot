package webhook

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

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

	require.True(t, o.progressCommentDue(t0, running("1:1/3;")), "first tick renders")

	// The step advances one second after the render. Every tick inside the
	// active interval keeps reading as movement, so the first tick at the
	// interval renders the new position.
	for now := t0.Add(time.Second); now.Before(t0.Add(activeInterval)); now = now.Add(tick) {
		assert.False(t, o.progressCommentDue(now, running("1:2/3;")), "inside the active interval at %s", now.Sub(t0))
	}
	require.True(t, o.progressCommentDue(t0.Add(activeInterval), running("1:2/3;")), "step advance renders at the active interval")
	assert.Equal(t, running("1:2/3;"), o.lastRendered)

	// Held still at step 2 the comment goes stagnant and slows to the long
	// interval; another advance is then rendered on its first tick, because the
	// active interval has long since elapsed.
	stagnantFrom := t0.Add(activeInterval)
	for i := 1; i <= stagnantThresh+2; i++ {
		assert.False(t, o.progressCommentDue(stagnantFrom.Add(time.Duration(i)*tick), running("1:2/3;")))
	}
	assert.False(t, o.progressCommentDue(stagnantFrom.Add(activeInterval+time.Second), running("1:2/3;")), "stagnant position waits for the slow interval")
	assert.True(t, o.progressCommentDue(stagnantFrom.Add(activeInterval+2*time.Second), running("1:3/3;")), "a further advance renders at once")
}

// A state change is rendered on the tick it is observed, even inside the active
// interval, and a snapshot that is byte-for-byte what the comment last showed
// is stagnant regardless of how many ticks report it.
func TestProgressCommentDueStateChangeRendersImmediately(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	o := &CommentObserver{}
	copying := progressSnapshot{state: state.Apply.Running, rowsCopied: 100}

	require.True(t, o.progressCommentDue(t0, copying))
	assert.False(t, o.progressCommentDue(t0.Add(time.Second), progressSnapshot{state: state.Apply.Running, rowsCopied: 150}), "row growth inside the interval waits")
	assert.True(t, o.progressCommentDue(t0.Add(2*time.Second), progressSnapshot{state: state.Apply.Checksumming, rowsCopied: 150}), "state change renders at once")

	for i := 1; i <= stagnantThresh+1; i++ {
		assert.False(t, o.progressCommentDue(t0.Add(2*time.Second+time.Duration(i)*500*time.Millisecond), progressSnapshot{state: state.Apply.Checksumming, rowsCopied: 150}))
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
	assert.Empty(t, progressFingerprint(building(`"blocks_done":"many"`)), "a malformed counter contributes nothing")
}
