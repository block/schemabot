package commands

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

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
// last printed id, a transient fetch failure is retried on the next tick
// without killing the tail, and cancellation exits cleanly.
func TestRunFollowLoop(t *testing.T) {
	script := []followFetchResult{
		{logs: []*client.LogEntry{followLogEntry(1), followLogEntry(2), followLogEntry(3)}},
		{logs: []*client.LogEntry{followLogEntry(2), followLogEntry(3)}},
		{err: fmt.Errorf("transient poll failure")},
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
		done <- runFollowLoop(ctx, fetch, emit, 3, time.Millisecond)
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
	err := runFollowLoop(t.Context(), fetch, func([]*client.LogEntry, bool) {}, 3, time.Millisecond)
	require.ErrorContains(t, err, "fetch initial log window")
	require.ErrorContains(t, err, "endpoint unreachable")
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
