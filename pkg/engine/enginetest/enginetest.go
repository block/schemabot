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
// The postgres engine is not wired into this suite: it deliberately declines
// every control operation with a typed UnsupportedOperationError (its
// statements commit or fail on their own, so there is no engine phase for a
// command to act on), which its own unit tests pin. The suite's cases target
// engines whose backends carry per-change control state; wire postgres here
// if it ever grows backend-driven control behavior.
package enginetest

import (
	"context"
	"reflect"
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

// contractCase binds one Case to the Harness fixture it consumes, the
// engine.Engine methods whose contract it pins, and the subtest body that
// exercises it. The completeness test in this package holds the registry,
// the Harness fixture fields, and the engine.Engine method set in lockstep,
// so a new fixture, case, or engine capability cannot land unclassified.
type contractCase struct {
	name Case
	// harnessField names the Harness fixture field this case consumes.
	harnessField string
	// fixtureType is the exact function signature required from harnessField.
	fixtureType reflect.Type
	// engineMethods names the engine.Engine methods whose cross-engine
	// contract this case pins. A method no case names must carry a
	// documented reason in engineMethodExclusions.
	engineMethods []string
	run           func(t *testing.T, h Harness, c contractCase)
}

var contractCases = []contractCase{
	{name: CaseCancelAlreadyCompleted, harnessField: "CancelAlreadyCompleted", fixtureType: reflect.TypeFor[func(*testing.T) ControlFixture](), engineMethods: []string{"Cancel"}, run: runCancelAlreadyCompleted},
	{name: CaseStopAlreadyCompleted, harnessField: "StopAlreadyCompleted", fixtureType: reflect.TypeFor[func(*testing.T) ControlFixture](), engineMethods: []string{"Stop"}, run: runStopAlreadyCompleted},
	{name: CaseCancelNonexistent, harnessField: "CancelNonexistent", fixtureType: reflect.TypeFor[func(*testing.T) ControlFixture](), engineMethods: []string{"Cancel"}, run: runCancelNonexistent},
	{name: CaseStopNonexistent, harnessField: "StopNonexistent", fixtureType: reflect.TypeFor[func(*testing.T) ControlFixture](), engineMethods: []string{"Stop"}, run: runStopNonexistent},
	{name: CaseProgressTerminalTruth, harnessField: "TerminalProgress", fixtureType: reflect.TypeFor[func(*testing.T) []ProgressFixture](), engineMethods: []string{"Progress"}, run: runProgressTerminalTruth},
	// The not-ready fixture invokes an operation of the engine's choosing,
	// so the case pins the rejection's error typing, not one method.
	{name: CaseNotReadyDistinguishable, harnessField: "NotReady", fixtureType: reflect.TypeFor[func(*testing.T) NotReadyFixture](), run: runNotReadyDistinguishable},
}

// engineMethodExclusions documents the engine.Engine methods this suite
// deliberately does not pin, keyed by method name with the reason. The suite
// exists for the cross-engine error-typing contract the driver layer depends
// on; behavior that is inherently engine-specific belongs to each engine's
// own tests. A new engine.Engine method must either be pinned by a contract
// case or carry a reason here — the completeness test fails it otherwise.
var engineMethodExclusions = map[string]string{
	"Name":       "identifier accessor with no backend behavior to pin",
	"Plan":       "plan output is engine-specific; each engine's own tests pin it",
	"Apply":      "apply orchestration is engine-specific; each engine's own tests pin it",
	"Start":      "resume semantics are engine-specific; each engine's own tests pin them",
	"Cutover":    "the suite exercises cutover only indirectly through engine-specific fixtures, so it cannot pin the method directly",
	"Revert":     "revert semantics are engine-specific; each engine's own tests pin them",
	"SkipRevert": "revert-window semantics are engine-specific; each engine's own tests pin them",
	"Volume":     "throttle semantics are engine-specific; each engine's own tests pin them",
}

// Run executes the engine contract suite against the harness. Every
// registered case runs as a subtest; a case is either exercised by its
// fixture or skipped with the harness's documented reason — a case with
// neither fails.
func Run(t *testing.T, h Harness) {
	registered := make(map[Case]bool, len(contractCases))
	for _, c := range contractCases {
		registered[c.name] = true
	}
	for key := range h.Skips {
		require.True(t, registered[key], "unknown skip key %q — not a registered contract case", key)
	}
	for _, c := range contractCases {
		t.Run(string(c.name), func(t *testing.T) { c.run(t, h, c) })
	}
}

func runCancelAlreadyCompleted(t *testing.T, h Harness, c contractCase) {
	fixture, ok := controlFixture(t, h, c.name, fixtureField[func(*testing.T) ControlFixture](t, h, c))
	if !ok {
		return
	}
	_, err := fixture.Engine.Cancel(t.Context(), fixture.Req)
	requireAlreadyCompleted(t, "cancel", err)
}

func runStopAlreadyCompleted(t *testing.T, h Harness, c contractCase) {
	fixture, ok := controlFixture(t, h, c.name, fixtureField[func(*testing.T) ControlFixture](t, h, c))
	if !ok {
		return
	}
	_, err := fixture.Engine.Stop(t.Context(), fixture.Req)
	requireAlreadyCompleted(t, "stop", err)
}

func runCancelNonexistent(t *testing.T, h Harness, c contractCase) {
	fixture, ok := controlFixture(t, h, c.name, fixtureField[func(*testing.T) ControlFixture](t, h, c))
	if !ok {
		return
	}
	_, err := fixture.Engine.Cancel(t.Context(), fixture.Req)
	requirePermanent(t, "cancel", err)
}

func runStopNonexistent(t *testing.T, h Harness, c contractCase) {
	fixture, ok := controlFixture(t, h, c.name, fixtureField[func(*testing.T) ControlFixture](t, h, c))
	if !ok {
		return
	}
	_, err := fixture.Engine.Stop(t.Context(), fixture.Req)
	requirePermanent(t, "stop", err)
}

func runProgressTerminalTruth(t *testing.T, h Harness, c contractCase) {
	build := fixtureField[func(*testing.T) []ProgressFixture](t, h, c)
	if handleSkip(t, h, c.name, build == nil) {
		return
	}
	fixtures := build(t)
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
}

func runNotReadyDistinguishable(t *testing.T, h Harness, c contractCase) {
	build := fixtureField[func(*testing.T) NotReadyFixture](t, h, c)
	if handleSkip(t, h, c.name, build == nil) {
		return
	}
	fixture := build(t)
	require.NotNil(t, fixture.Invoke, "the not-ready fixture must supply the operation to invoke")
	err := fixture.Invoke(t.Context())
	require.Error(t, err, "an operation against a not-ready backend must be rejected")
	assert.True(t, engine.IsNotReady(err),
		"a not-ready rejection must be a typed NotReadyError so polling drives reattempt instead of reporting a failure, got: %v", err)
	assert.True(t, engine.IsRetryable(err),
		"a not-ready rejection must stay retryable — the backend is expected to accept the operation once it catches up, got: %v", err)
	assert.False(t, engine.IsAlreadyCompleted(err),
		"a not-ready rejection must not read as already-completed, got: %v", err)
}

func fixtureField[T any](t *testing.T, h Harness, c contractCase) T {
	t.Helper()
	field := reflect.ValueOf(h).FieldByName(c.harnessField)
	require.True(t, field.IsValid() && field.CanInterface(),
		"case %q must name an exported Harness field, %q is missing or unexported", c.name, c.harnessField)
	fixture, ok := field.Interface().(T)
	require.True(t, ok, "case %q Harness field %q has type %s, not the fixture type its runner requires", c.name, c.harnessField, field.Type())
	return fixture
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
