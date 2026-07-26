// Package enginetest is a contract-test kit for engine.Engine implementations.
//
// Every engine wired into SchemaBot must run this suite. It pins down the
// error-typing contract the driver layer depends on to keep durable control
// requests from livelocking: a stop or cancel that arrives after the backend
// already completed the change must be rejected with a typed
// AlreadyCompletedError (so the drive reconciles stored state to the completed
// outcome instead of retrying a rejection that can never succeed), a control
// operation addressing a change the backend has no record of must be permanent
// (retrying can never make the change appear), progress must report terminal
// outcomes truthfully in engine-agnostic states, and a backend that is merely
// not ready yet must be distinguishable from a failure.
//
// An engine that genuinely cannot exhibit one of the cases documents that with
// an explicit skip reason in Harness.Skips — the suite fails on a missing
// fixture without one, so a new engine cannot silently opt out of the
// contract.
//
// The postgres engine is not wired into this suite: it is a stub whose
// operations are all unimplemented, so it has no control or progress behavior
// to hold to the contract yet. Wire it here as part of implementing its
// control operations.
package enginetest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

// Case names one behavior of the engine contract the suite verifies. Cases are
// the keys of Harness.Skips.
type Case string

const (
	// CaseCancelAlreadyCompleted: a cancel of a change the backend already
	// completed is rejected with a typed AlreadyCompletedError.
	CaseCancelAlreadyCompleted Case = "cancel-already-completed"
	// CaseStopAlreadyCompleted: a stop of a change the backend already
	// completed is rejected with a typed AlreadyCompletedError.
	CaseStopAlreadyCompleted Case = "stop-already-completed"
	// CaseCancelNonexistent: a cancel of a change the backend has no record of
	// is a permanent error, not a retryable one.
	CaseCancelNonexistent Case = "cancel-nonexistent"
	// CaseStopNonexistent: a stop of a change the backend has no record of is
	// a permanent error, not a retryable one.
	CaseStopNonexistent Case = "stop-nonexistent"
	// CaseProgressTerminalTruth: progress reports the backend's terminal
	// outcomes truthfully as terminal engine-agnostic states.
	CaseProgressTerminalTruth Case = "progress-terminal-truth"
	// CaseNotReadyDistinguishable: a backend that is not yet ready to accept
	// an operation surfaces a typed NotReadyError, distinguishable from a
	// permanent failure and from an already-completed rejection.
	CaseNotReadyDistinguishable Case = "not-ready-distinguishable"
)

// ControlFixture is an engine primed so that the returned control request
// addresses the scenario under test (a completed change, a nonexistent change).
type ControlFixture struct {
	Engine engine.Engine
	Req    *engine.ControlRequest
}

// ProgressFixture is an engine primed so that a progress read of Req reports
// the terminal state Want.
type ProgressFixture struct {
	// Name labels the subtest (typically the backend state being reported).
	Name   string
	Engine engine.Engine
	Req    *engine.ProgressRequest
	Want   engine.State
}

// NotReadyFixture invokes an operation against a backend that is not yet ready
// to accept it.
type NotReadyFixture struct {
	// Invoke performs the operation whose rejection must be a typed
	// NotReadyError.
	Invoke func(ctx context.Context) error
}

// Harness supplies per-case fixtures for one engine. Each fixture builds a
// fresh engine primed for its scenario; the suite owns the assertions. A case
// with no fixture must carry a documented skip in Skips — the suite fails
// otherwise.
type Harness struct {
	CancelAlreadyCompleted func(t *testing.T) ControlFixture
	StopAlreadyCompleted   func(t *testing.T) ControlFixture
	CancelNonexistent      func(t *testing.T) ControlFixture
	StopNonexistent        func(t *testing.T) ControlFixture
	TerminalProgress       func(t *testing.T) []ProgressFixture
	NotReady               func(t *testing.T) NotReadyFixture

	// Skips documents the cases this engine genuinely cannot exhibit, keyed by
	// case with a human-readable reason. A skipped case must not also provide
	// a fixture, and the reason must be non-empty.
	Skips map[Case]string
}

// Run executes the engine contract suite against the harness. Every case runs
// as a subtest; a case is either exercised by its fixture or skipped with the
// harness's documented reason — a case with neither fails.
func Run(t *testing.T, h Harness) {
	t.Run(string(CaseCancelAlreadyCompleted), func(t *testing.T) {
		fixture, ok := controlFixture(t, h, CaseCancelAlreadyCompleted, h.CancelAlreadyCompleted)
		if !ok {
			return
		}
		_, err := fixture.Engine.Cancel(t.Context(), fixture.Req)
		requireAlreadyCompleted(t, "cancel", err)
	})

	t.Run(string(CaseStopAlreadyCompleted), func(t *testing.T) {
		fixture, ok := controlFixture(t, h, CaseStopAlreadyCompleted, h.StopAlreadyCompleted)
		if !ok {
			return
		}
		_, err := fixture.Engine.Stop(t.Context(), fixture.Req)
		requireAlreadyCompleted(t, "stop", err)
	})

	t.Run(string(CaseCancelNonexistent), func(t *testing.T) {
		fixture, ok := controlFixture(t, h, CaseCancelNonexistent, h.CancelNonexistent)
		if !ok {
			return
		}
		_, err := fixture.Engine.Cancel(t.Context(), fixture.Req)
		requirePermanent(t, "cancel", err)
	})

	t.Run(string(CaseStopNonexistent), func(t *testing.T) {
		fixture, ok := controlFixture(t, h, CaseStopNonexistent, h.StopNonexistent)
		if !ok {
			return
		}
		_, err := fixture.Engine.Stop(t.Context(), fixture.Req)
		requirePermanent(t, "stop", err)
	})

	t.Run(string(CaseProgressTerminalTruth), func(t *testing.T) {
		if handleSkip(t, h, CaseProgressTerminalTruth, h.TerminalProgress == nil) {
			return
		}
		fixtures := h.TerminalProgress(t)
		require.NotEmpty(t, fixtures, "the terminal-progress fixture must cover at least one terminal backend state")
		for _, fixture := range fixtures {
			t.Run(fixture.Name, func(t *testing.T) {
				require.True(t, fixture.Want.IsTerminal(),
					"fixture %s wants state %q, which is not terminal — this case only covers terminal truth", fixture.Name, fixture.Want)
				result, err := fixture.Engine.Progress(t.Context(), fixture.Req)
				require.NoError(t, err, "progress on a terminal change must report the outcome, not fail")
				require.NotNil(t, result)
				assert.Equal(t, fixture.Want, result.State,
					"progress must report the backend's terminal outcome truthfully")
			})
		}
	})

	t.Run(string(CaseNotReadyDistinguishable), func(t *testing.T) {
		if handleSkip(t, h, CaseNotReadyDistinguishable, h.NotReady == nil) {
			return
		}
		fixture := h.NotReady(t)
		require.NotNil(t, fixture.Invoke, "the not-ready fixture must supply the operation to invoke")
		err := fixture.Invoke(t.Context())
		require.Error(t, err, "an operation against a not-ready backend must be rejected")
		assert.True(t, engine.IsNotReady(err),
			"a not-ready rejection must be a typed NotReadyError so polling drives reattempt instead of reporting a failure, got: %v", err)
		assert.True(t, engine.IsRetryable(err),
			"a not-ready rejection must stay retryable — the backend is expected to accept the operation once it catches up, got: %v", err)
		assert.False(t, engine.IsAlreadyCompleted(err),
			"a not-ready rejection must not read as already-completed, got: %v", err)
	})
}

// controlFixture resolves the fixture for a control case, honoring a
// documented skip. ok is false when the subtest should not proceed (skipped;
// a missing fixture without a skip fails the subtest).
func controlFixture(t *testing.T, h Harness, c Case, build func(t *testing.T) ControlFixture) (ControlFixture, bool) {
	if handleSkip(t, h, c, build == nil) {
		return ControlFixture{}, false
	}
	fixture := build(t)
	require.NotNil(t, fixture.Engine, "case %s fixture must supply an engine", c)
	require.NotNil(t, fixture.Req, "case %s fixture must supply a control request", c)
	return fixture, true
}

// handleSkip applies the harness's documented skip for a case, if any, and
// fails the subtest when a case has neither a fixture nor a skip. Returns true
// when the subtest must not proceed to the fixture.
func handleSkip(t *testing.T, h Harness, c Case, fixtureMissing bool) bool {
	reason, skipped := h.Skips[c]
	if skipped {
		require.NotEmpty(t, reason, "case %s is skipped without a reason — document why this engine cannot exhibit it", c)
		require.True(t, fixtureMissing, "case %s has both a fixture and a skip — remove one so the suite's coverage stays honest", c)
		t.Skip(reason)
		return true
	}
	if fixtureMissing {
		t.Fatalf("case %s has no fixture — wire the case or document a skip in Harness.Skips", c)
		return true
	}
	return false
}

func requireAlreadyCompleted(t *testing.T, operation string, err error) {
	t.Helper()
	require.Error(t, err, "a %s of an already-completed change must be rejected", operation)
	assert.True(t, engine.IsAlreadyCompleted(err),
		"a %s rejected because the change already completed must be a typed AlreadyCompletedError so the caller reconciles to the completed outcome instead of retrying, got: %v", operation, err)
}

func requirePermanent(t *testing.T, operation string, err error) {
	t.Helper()
	require.Error(t, err, "a %s of a nonexistent change must be rejected", operation)
	assert.False(t, engine.IsRetryable(err),
		"a %s of a change the backend has no record of must be permanent — retrying can never make the change appear, got: %v", operation, err)
	assert.False(t, engine.IsAlreadyCompleted(err),
		"a %s of a nonexistent change must not read as already-completed — nothing landed, got: %v", operation, err)
}
