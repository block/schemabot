package commands

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/kong"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// followTestDeadline bounds every wait in the follow-loop tests so a stuck
// loop fails fast instead of hanging the suite.
const followTestDeadline = 10 * time.Second

func TestLogsCommandFlagValidation(t *testing.T) {
	err := (&LogsCmd{Deployment: "data-plane", Database: "orders", Environment: "production"}).Run(t.Context(), &Globals{})
	require.EqualError(t, err, "--deployment requires an explicit apply_id")

	err = (&LogsCmd{ApplyID: "apply-a", Follow: true, JSON: true}).Run(t.Context(), &Globals{})
	require.ErrorContains(t, err, "--json is incompatible with --follow")

	err = (&LogsCmd{Deployment: "data-plane", ApplyID: "apply-a", Follow: true, JSON: true}).Run(t.Context(), &Globals{})
	require.ErrorContains(t, err, "--json is incompatible with --follow")
}

func followLogEntry(id int64) *client.LogEntry {
	return &client.LogEntry{ID: id, Message: fmt.Sprintf("entry %d", id)}
}

// TestFollowStateAdvance verifies the follow dedupe: overlapping poll windows
// only surface entries whose id is newer than the last printed one, in the
// order delivered, so an operator tailing an apply never sees a line twice.
func TestFollowStateAdvance(t *testing.T) {
	state := &followState{}

	first := state.advance([]*client.LogEntry{followLogEntry(1), followLogEntry(2), followLogEntry(3)})
	require.Len(t, first, 3)
	assert.Equal(t, int64(1), first[0].ID)
	assert.Equal(t, int64(3), first[2].ID)

	// A window that fully overlaps what was printed surfaces nothing.
	assert.Empty(t, state.advance([]*client.LogEntry{followLogEntry(2), followLogEntry(3)}))

	// A window with overlap plus new entries surfaces only the new ones.
	fresh := state.advance([]*client.LogEntry{followLogEntry(3), followLogEntry(4), followLogEntry(5)})
	require.Len(t, fresh, 2)
	assert.Equal(t, "entry 4", fresh[0].Message)
	assert.Equal(t, "entry 5", fresh[1].Message)

	assert.Empty(t, state.advance(nil))
}

// TestFollowStateMissedEntries verifies gap detection on tail polls: a window
// that hit its bound and no longer overlaps what was already printed cannot
// prove the entries between polls were all shown, while an overlapping
// window, a window under its bound, or a first render can.
func TestFollowStateMissedEntries(t *testing.T) {
	state := &followState{}

	full := []*client.LogEntry{followLogEntry(5), followLogEntry(6)}
	assert.False(t, state.missedEntries(full, true), "nothing printed yet, so nothing can have been missed")

	state.advance([]*client.LogEntry{followLogEntry(1), followLogEntry(2)})
	assert.True(t, state.missedEntries(full, true), "a full window past the last printed entry cannot prove continuity")
	assert.False(t, state.missedEntries(full, false), "a window under its bound returned everything that exists")
	assert.False(t, state.missedEntries(nil, true))

	overlapping := []*client.LogEntry{followLogEntry(2), followLogEntry(3)}
	assert.False(t, state.missedEntries(overlapping, true), "an overlapping window proves the tail kept up")
}

type followFetchResult struct {
	logs []*client.LogEntry
	err  error
}

// scriptedFetch returns each result in script in turn, repeating the final
// result once the script is exhausted, and records the limit of every call.
func scriptedFetch(script []followFetchResult) (fetch func(limit int) ([]*client.LogEntry, error), limits func() []int) {
	var mu sync.Mutex
	var seen []int
	calls := 0
	fetch = func(limit int) ([]*client.LogEntry, error) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, limit)
		result := script[min(calls, len(script)-1)]
		calls++
		return result.logs, result.err
	}
	limits = func() []int {
		mu.Lock()
		defer mu.Unlock()
		return append([]int(nil), seen...)
	}
	return fetch, limits
}

// TestRunFollowLoop verifies the follow loop end to end: the initial poll
// prints the newest-N window, later polls print only entries newer than the
// last printed id, a transient fetch failure is retried after its backoff
// without killing the tail, and cancellation exits cleanly.
func TestRunFollowLoop(t *testing.T) {
	script := []followFetchResult{
		{logs: []*client.LogEntry{followLogEntry(1), followLogEntry(2), followLogEntry(3)}},
		{logs: []*client.LogEntry{followLogEntry(2), followLogEntry(3)}},
		{err: transientFollowError()},
		{logs: []*client.LogEntry{followLogEntry(3), followLogEntry(4), followLogEntry(5)}},
	}
	fetch, limits := scriptedFetch(script)

	printed := make(chan *client.LogEntry, 64)
	// The loop polls until cancellation, so the render record is unbounded and
	// must never be what blocks it.
	var rendersMu sync.Mutex
	var renders []bool
	state := &followState{}
	emit := func(logs []*client.LogEntry, initial bool) {
		rendersMu.Lock()
		renders = append(renders, initial)
		rendersMu.Unlock()
		for _, log := range state.advance(logs) {
			printed <- log
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runFollowLoop(ctx, fetch, emit, 3, fastFollowPacing())
	}()

	var got []*client.LogEntry
	for len(got) < 5 {
		select {
		case entry := <-printed:
			got = append(got, entry)
		case <-time.After(followTestDeadline):
			t.Fatalf("timed out waiting for followed entries, got %d of 5", len(got))
		}
	}
	for i, entry := range got {
		assert.Equal(t, int64(i+1), entry.ID)
		assert.Equal(t, fmt.Sprintf("entry %d", i+1), entry.Message)
	}

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(followTestDeadline):
		t.Fatal("follow loop did not exit after cancellation")
	}

	// The steady-state script repeats a fully-printed window, so nothing
	// beyond the five expected entries is ever re-printed.
	select {
	case entry := <-printed:
		t.Fatalf("unexpected duplicate entry printed: id=%d", entry.ID)
	default:
	}

	seen := limits()
	require.NotEmpty(t, seen)
	assert.Equal(t, 3, seen[0], "initial poll uses the -n window")
	for _, limit := range seen[1:] {
		assert.Equal(t, followFetchLimit, limit, "follow polls use the bounded fetch window")
	}

	// Only the first render is the operator's own window, so only it may report
	// that history precedes the tail.
	rendersMu.Lock()
	defer rendersMu.Unlock()
	require.NotEmpty(t, renders)
	assert.True(t, renders[0], "the first render is the initial window")
	for _, initial := range renders[1:] {
		assert.False(t, initial, "later renders are polls, not the initial window")
	}
}

// TestRunFollowLoopInitialFetchFails verifies that a broken endpoint fails the
// command immediately rather than tailing nothing.
func TestRunFollowLoopInitialFetchFails(t *testing.T) {
	fetch, _ := scriptedFetch([]followFetchResult{{err: fmt.Errorf("endpoint unreachable")}})
	err := runFollowLoop(t.Context(), fetch, func([]*client.LogEntry, bool) {}, 3, fastFollowPacing())
	require.ErrorContains(t, err, "fetch initial log window")
	require.ErrorContains(t, err, "endpoint unreachable")
}

// fastFollowPacing polls and retries without real waiting, so a test drives the
// loop through many polls in moments.
func fastFollowPacing() followPacing {
	return followPacing{
		interval:  time.Millisecond,
		retryWait: func(error, int) time.Duration { return time.Millisecond },
		sleep:     sleepUnlessDone,
	}
}

// A tail waits the poll interval after every poll that answered and the
// backoff for its place in the run after every poll that failed. Once a poll
// answers again the tail is back on the interval, so a server that recovered
// is not polled at the pace of the last backoff, and a server that is failing
// is not polled at the pace of a healthy one.
func TestRunFollowLoopWaitsTheBackoffOnlyAfterAFailedPoll(t *testing.T) {
	const interval = 5 * time.Second
	fetch, limits := scriptedFetch([]followFetchResult{
		{logs: []*client.LogEntry{followLogEntry(1)}},
		{err: transientFollowError()},
		{err: transientFollowError()},
		{logs: []*client.LogEntry{followLogEntry(2)}},
		{logs: []*client.LogEntry{followLogEntry(3)}},
		{err: followAPIError(http.StatusNotFound, "apply not found")},
	})
	var waits []time.Duration
	pacing := followPacing{
		interval:  interval,
		retryWait: func(_ error, failures int) time.Duration { return time.Duration(failures) * time.Minute },
		sleep:     func(_ context.Context, d time.Duration) { waits = append(waits, d) },
	}

	err := runFollowLoop(t.Context(), fetch, func([]*client.LogEntry, bool) {}, 3, pacing)

	require.ErrorContains(t, err, "apply not found")
	assert.Len(t, limits(), 6, "every scripted poll ran")
	assert.Equal(t, []time.Duration{interval, time.Minute, 2 * time.Minute, interval, interval}, waits,
		"one wait before each poll after the first: the interval, the backoff after each of two failures, then the interval again")
}

// A tail polls every five seconds and, after a failed poll, backs off on the
// schedule the progress watches use, including a longer delay the server asked
// for, so a tail and a watch ride out the same outage for the same time.
func TestDefaultFollowPacingMatchesTheWatches(t *testing.T) {
	assert.Equal(t, 5*time.Second, defaultFollowPacing.interval)

	unreachable := transientFollowError()
	for failures := 1; failures < maxConsecutiveProgressFailures; failures++ {
		assert.Equal(t, progressRetryWait(unreachable, failures), defaultFollowPacing.retryWait(unreachable, failures), "failure %d", failures)
	}
	assert.Equal(t, 4*time.Second, defaultFollowPacing.retryWait(unreachable, 1), "the first backoff")
	assert.Equal(t, 30*time.Second, defaultFollowPacing.retryWait(unreachable, maxConsecutiveProgressFailures-1), "the backoff is capped")

	// The default wait is a real one that an interrupt cuts short.
	started := time.Now()
	defaultFollowPacing.sleep(t.Context(), 50*time.Millisecond)
	assert.GreaterOrEqual(t, time.Since(started), 50*time.Millisecond, "the wait lasts its full duration")

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	started = time.Now()
	defaultFollowPacing.sleep(cancelled, followTestDeadline)
	assert.Less(t, time.Since(started), followTestDeadline, "a cancelled tail does not wait out the interval")
}

// transientFollowError is a failure the watches judge worth retrying: the
// server could not be reached.
func transientFollowError() error {
	return &client.ConnectionError{Endpoint: "http://schemabot.example", Err: errors.New("connection refused")}
}

func followAPIError(status int, message string) error {
	return &client.APIError{Status: status, Message: message}
}

// A tail that the server answers with a permanent refusal ends with an error
// and a non-zero exit instead of polling forever: the apply having disappeared
// or the credentials being refused will not change on the next poll, and a tail
// that prints nothing reads as an apply with nothing to say.
func TestRunFollowLoopStopsOnPermanentFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "apply not found", err: followAPIError(http.StatusNotFound, "apply not found"), want: "apply not found"},
		{name: "credentials refused", err: followAPIError(http.StatusUnauthorized, "invalid token"), want: "invalid token"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fetch, limits := scriptedFetch([]followFetchResult{
				{logs: []*client.LogEntry{followLogEntry(1)}},
				{err: tc.err},
			})
			ctx, cancel := context.WithTimeout(t.Context(), followTestDeadline)
			defer cancel()

			err := runFollowLoop(ctx, fetch, func([]*client.LogEntry, bool) {}, 3, fastFollowPacing())

			require.Error(t, err)
			require.NoError(t, ctx.Err(), "the loop must end on the failure, not on the test deadline")
			assert.ErrorContains(t, err, "stopped following logs")
			assert.ErrorContains(t, err, tc.want)
			var apiErr *client.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Len(t, limits(), 2, "one initial poll and the one that failed, with no retry of a permanent failure")
		})
	}
}

// A tail that cannot reach the server retries with backoff and gives up with an
// error after the same number of polls in a row that the progress watches
// tolerate, so a script following an apply's logs does not hang on a server
// that never comes back.
func TestRunFollowLoopGivesUpAfterConsecutiveTransientFailures(t *testing.T) {
	fetch, limits := scriptedFetch([]followFetchResult{
		{logs: []*client.LogEntry{followLogEntry(1)}},
		{err: transientFollowError()},
	})
	ctx, cancel := context.WithTimeout(t.Context(), followTestDeadline)
	defer cancel()

	err := runFollowLoop(ctx, fetch, func([]*client.LogEntry, bool) {}, 3, fastFollowPacing())

	require.Error(t, err)
	require.NoError(t, ctx.Err(), "the loop must give up on its own, not on the test deadline")
	assert.ErrorContains(t, err, "stopped following logs after 10 consecutive failed polls")
	assert.ErrorContains(t, err, "does not affect the apply")
	var connErr *client.ConnectionError
	require.ErrorAs(t, err, &connErr)
	assert.Len(t, limits(), 1+maxConsecutiveProgressFailures, "the initial poll, then every failed poll up to the bound")
}

// A poll that succeeds clears the run of failures, and each failed poll waits
// the backoff for its place in the run, so intermittent trouble never ends a
// tail that keeps recovering.
func TestRunFollowLoopSuccessResetsTheFailureRun(t *testing.T) {
	script := []followFetchResult{{logs: []*client.LogEntry{followLogEntry(1)}}}
	for range maxConsecutiveProgressFailures - 1 {
		script = append(script, followFetchResult{err: followAPIError(http.StatusServiceUnavailable, "unavailable")})
	}
	script = append(script, followFetchResult{logs: []*client.LogEntry{followLogEntry(2)}})
	for range maxConsecutiveProgressFailures - 1 {
		script = append(script, followFetchResult{err: transientFollowError()})
	}
	script = append(script, followFetchResult{err: followAPIError(http.StatusNotFound, "apply not found")})
	fetch, limits := scriptedFetch(script)

	var waits []int
	pacing := fastFollowPacing()
	pacing.retryWait = func(_ error, failures int) time.Duration {
		waits = append(waits, failures)
		return time.Millisecond
	}
	ctx, cancel := context.WithTimeout(t.Context(), followTestDeadline)
	defer cancel()

	err := runFollowLoop(ctx, fetch, func([]*client.LogEntry, bool) {}, 3, pacing)

	require.ErrorContains(t, err, "apply not found")
	require.NoError(t, ctx.Err())
	assert.Len(t, limits(), len(script), "every scripted poll ran, so no failure run reached the bound")
	var want []int
	for range 2 {
		for failures := 1; failures < maxConsecutiveProgressFailures; failures++ {
			want = append(want, failures)
		}
	}
	assert.Equal(t, want, waits, "the failure count restarts after a successful poll")
}

// An operator who presses Ctrl-C while a poll is in flight ends the tail
// normally, even if that poll comes back with an error.
func TestRunFollowLoopInterruptDuringFailedPollIsNotAFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	polls := 0
	fetch := func(int) ([]*client.LogEntry, error) {
		polls++
		if polls == 1 {
			return []*client.LogEntry{followLogEntry(1)}, nil
		}
		cancel()
		return nil, followAPIError(http.StatusNotFound, "apply not found")
	}

	err := runFollowLoop(ctx, fetch, func([]*client.LogEntry, bool) {}, 3, fastFollowPacing())

	require.NoError(t, err)
	assert.Equal(t, 2, polls)
}

func deploymentFollowSource(target, externalID string, ids ...int64) *apitypes.DeploymentLogSource {
	source := &apitypes.DeploymentLogSource{
		ExternalID: externalID,
		Operations: []*apitypes.LogOperationProvenance{{OperationKey: target + "-op", Target: target, OperationKind: "spirit"}},
	}
	for _, id := range ids {
		source.Logs = append(source.Logs, followLogEntry(id))
	}
	return source
}

func deploymentFollowError(target, externalID, message string) *apitypes.DeploymentLogError {
	return &apitypes.DeploymentLogError{
		ExternalID: externalID,
		Target:     target,
		Operations: []*apitypes.LogOperationProvenance{{OperationKey: target + "-op", Target: target, OperationKind: "spirit"}},
		Code:       "RemoteLogReadFailed",
		Reason:     "remote_log_read_failed",
		Message:    message,
	}
}

// TestDeploymentFollowStateAdvance verifies the per-source dedupe of a
// data-plane tail: each source's log ids are only comparable within that
// source, so overlapping ids across sources still all print, while an
// overlapping window from the same source prints only the new entries.
// Batches carry a source label only once the apply fans out to more than one
// source, so a single-source tail reads like the plain apply-log tail.
func TestDeploymentFollowStateAdvance(t *testing.T) {
	state := newDeploymentFollowState()

	batches, warnings := state.advance(&apitypes.DeploymentLogsResponse{
		Sources: []*apitypes.DeploymentLogSource{deploymentFollowSource("target-a", "remote-1", 1, 2)},
	})
	require.Len(t, batches, 1)
	assert.Empty(t, batches[0].label, "a single-source tail is unadorned")
	require.Len(t, batches[0].logs, 2)
	assert.Equal(t, int64(1), batches[0].logs[0].ID)
	assert.Equal(t, int64(2), batches[0].logs[1].ID)
	assert.Empty(t, warnings)

	// A second source appears with ids overlapping the first source's: its
	// entries are all new (independent id sequence), and batches become
	// labeled. The first source's overlapping window dedupes as usual.
	batches, warnings = state.advance(&apitypes.DeploymentLogsResponse{
		Sources: []*apitypes.DeploymentLogSource{
			deploymentFollowSource("target-a", "remote-1", 2, 3),
			deploymentFollowSource("target-b", "remote-2", 1, 2),
		},
	})
	require.Len(t, batches, 2)
	assert.Equal(t, "target-a / spirit (remote-1)", batches[0].label)
	require.Len(t, batches[0].logs, 1)
	assert.Equal(t, int64(3), batches[0].logs[0].ID)
	assert.Equal(t, "target-b / spirit (remote-2)", batches[1].label)
	require.Len(t, batches[1].logs, 2)
	assert.Equal(t, int64(1), batches[1].logs[0].ID)
	assert.Equal(t, int64(2), batches[1].logs[1].ID)
	assert.Empty(t, warnings)

	// A fully-overlapping poll surfaces nothing for either source.
	batches, warnings = state.advance(&apitypes.DeploymentLogsResponse{
		Sources: []*apitypes.DeploymentLogSource{
			deploymentFollowSource("target-a", "remote-1", 3),
			deploymentFollowSource("target-b", "remote-2", 2),
		},
	})
	assert.Empty(t, batches)
	assert.Empty(t, warnings)
}

// TestDeploymentFollowStateCarriesPerSourceTruncation verifies that each
// source's window signal rides its own batch: a fan-out where one target has
// more history than the window holds must not make the other target's complete
// tail look partial, or vice versa.
func TestDeploymentFollowStateCarriesPerSourceTruncation(t *testing.T) {
	state := newDeploymentFollowState()

	truncated := deploymentFollowSource("target-a", "remote-1", 1, 2)
	truncated.Truncated = true
	batches, _ := state.advance(&apitypes.DeploymentLogsResponse{
		Sources: []*apitypes.DeploymentLogSource{
			truncated,
			deploymentFollowSource("target-b", "remote-2", 1),
		},
	})
	require.Len(t, batches, 2)
	assert.Equal(t, "target-a / spirit (remote-1)", batches[0].label)
	assert.True(t, batches[0].truncated, "the source with older entries reports its window is partial")
	assert.Equal(t, "target-b / spirit (remote-2)", batches[1].label)
	assert.False(t, batches[1].truncated, "a complete source is not marked partial by a sibling")
}

// TestDeploymentFollowStateFirstRenderPerSource verifies that the truncation
// notice belongs to each source's own first successful read: a source that
// fails the initial fan-out and recovers on a later poll still gets exactly
// one first render carrying its window signal, and a later poll that outruns
// the window reports a possible gap instead of repeating the notice.
func TestDeploymentFollowStateFirstRenderPerSource(t *testing.T) {
	state := newDeploymentFollowState()

	// target-b fails the initial fan-out read; target-a renders with history
	// beyond its window.
	initial := deploymentFollowSource("target-a", "remote-1", 1, 2)
	initial.Truncated = true
	batches, warnings := state.advance(&apitypes.DeploymentLogsResponse{
		Sources: []*apitypes.DeploymentLogSource{initial},
		Errors:  []*apitypes.DeploymentLogError{deploymentFollowError("target-b", "remote-2", "Data-plane logs could not be read; check server logs and retry.")},
	})
	require.Len(t, batches, 1)
	require.Len(t, warnings, 1)
	assert.True(t, batches[0].firstRender)
	assert.True(t, batches[0].truncated)
	assert.False(t, batches[0].possibleGap)

	// target-b recovers with more accumulated history than the poll window
	// holds: its first render arrives late but still carries the signal.
	recovered := deploymentFollowSource("target-b", "remote-2", 7, 8)
	recovered.Truncated = true
	batches, _ = state.advance(&apitypes.DeploymentLogsResponse{
		Sources: []*apitypes.DeploymentLogSource{
			deploymentFollowSource("target-a", "remote-1", 2, 3),
			recovered,
		},
	})
	require.Len(t, batches, 2)
	assert.Equal(t, "target-a / spirit (remote-1)", batches[0].label)
	assert.False(t, batches[0].firstRender, "an already-rendered source does not repeat its window notice")
	assert.Equal(t, "target-b / spirit (remote-2)", batches[1].label)
	assert.True(t, batches[1].firstRender, "a source that only became readable later still gets its window notice")
	assert.True(t, batches[1].truncated)
	assert.False(t, batches[1].possibleGap, "a first render has printed nothing to fall behind")

	// target-a's next poll outruns the window: every entry returned is newer
	// than the last printed one and the read hit its bound.
	outrun := deploymentFollowSource("target-a", "remote-1", 9, 10)
	outrun.Truncated = true
	batches, _ = state.advance(&apitypes.DeploymentLogsResponse{
		Sources: []*apitypes.DeploymentLogSource{outrun},
	})
	require.Len(t, batches, 1)
	assert.False(t, batches[0].firstRender)
	assert.True(t, batches[0].possibleGap, "a full window past the last printed entry reports the possible gap")

	// An overlapping later window proves continuity even at its bound.
	overlap := deploymentFollowSource("target-a", "remote-1", 10, 11)
	overlap.Truncated = true
	batches, _ = state.advance(&apitypes.DeploymentLogsResponse{
		Sources: []*apitypes.DeploymentLogSource{overlap},
	})
	require.Len(t, batches, 1)
	assert.False(t, batches[0].possibleGap, "an overlapping window shows the tail kept up")
}

// TestDeploymentFollowStateWarnings verifies that a persistently failing
// source is reported once rather than on every poll, and that a source which
// recovers and then fails again is reported again — the operator needs to see
// the relapse, not assume the old warning still applies.
func TestDeploymentFollowStateWarnings(t *testing.T) {
	state := newDeploymentFollowState()

	failing := &apitypes.DeploymentLogsResponse{
		Sources: []*apitypes.DeploymentLogSource{deploymentFollowSource("target-a", "remote-1", 1)},
		Errors:  []*apitypes.DeploymentLogError{deploymentFollowError("target-b", "remote-2", "Data-plane logs could not be read; check server logs and retry.")},
	}

	batches, warnings := state.advance(failing)
	require.Len(t, batches, 1)
	assert.Equal(t, "target-a / spirit (remote-1)", batches[0].label, "a failing source still counts toward the fan-out, so the readable one is labeled")
	require.Len(t, warnings, 1)
	assert.Equal(t, "target-b / spirit (remote-2): Data-plane logs could not be read; check server logs and retry.", warnings[0])

	// The same failure on the next poll is not repeated.
	_, warnings = state.advance(failing)
	assert.Empty(t, warnings)

	// The source recovers, then fails again: the relapse is reported.
	_, warnings = state.advance(&apitypes.DeploymentLogsResponse{
		Sources: []*apitypes.DeploymentLogSource{
			deploymentFollowSource("target-a", "remote-1"),
			deploymentFollowSource("target-b", "remote-2", 5),
		},
	})
	assert.Empty(t, warnings)

	_, warnings = state.advance(failing)
	require.Len(t, warnings, 1)
	assert.Equal(t, "target-b / spirit (remote-2): Data-plane logs could not be read; check server logs and retry.", warnings[0])
}

// A followed read opens with the scope it is narrowed to, so an operator
// tailing one shard of one target does not mistake its lines for the whole
// apply's.
func TestScopeHeader(t *testing.T) {
	assert.Empty(t, scopeHeader(client.LogScope{}))
	assert.Equal(t, ", target shop-002", scopeHeader(client.LogScope{Target: "shop-002"}))
	assert.Equal(t, ", shard -80", scopeHeader(client.LogScope{Shard: "-80"}))
	assert.Equal(t, ", target shop-002, shard -80", scopeHeader(client.LogScope{Target: "shop-002", Shard: "-80"}))
}

// Every keyspace's first shard starts with "-", which reads as a flag when it
// is given as its own argument. The documented form joins it to the flag, and
// the parser's refusal of the separated form names that form.
func TestLogsCmdParsesShardStartingWithHyphen(t *testing.T) {
	var cli struct {
		Logs LogsCmd `cmd:"" name:"logs"`
	}
	parser, err := kong.New(&cli)
	require.NoError(t, err)

	_, err = parser.Parse([]string{"logs", "apply-a", "--target", "shop-002", "--shard=-80", "--follow"})
	require.NoError(t, err)
	assert.Equal(t, "apply-a", cli.Logs.ApplyIDArg)
	assert.Equal(t, "shop-002", cli.Logs.Target)
	assert.Equal(t, "-80", cli.Logs.Shard)
	assert.True(t, cli.Logs.Follow)

	cli.Logs = LogsCmd{}
	_, err = parser.Parse([]string{"logs", "apply-a", "--shard", "80-"})
	require.NoError(t, err)
	assert.Equal(t, "80-", cli.Logs.Shard)

	cli.Logs = LogsCmd{}
	_, err = parser.Parse([]string{"logs", "apply-a", "--shard", "-80"})
	require.ErrorContains(t, err, `--shard="-80"`)
}
