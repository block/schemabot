package localscale_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/localscale"
	"github.com/block/schemabot/pkg/state"
)

var drState = state.DeployRequest

// clearAction is what cleanup between tests does with a deploy request it finds
// in a given state.
type clearAction int

const (
	// actionNone: the request does not occupy the database's one active
	// deploy, so the next test can deploy over it.
	actionNone clearAction = iota
	// actionSkipRevert: the request is holding its revert window open and
	// has to be asked to close it.
	actionSkipRevert
	// actionCancel: the request is active and has to be asked to stop.
	actionCancel
	// actionWaitOut: the request is already on its way to a terminal state
	// under its own steam. It still blocks, so cleanup waits for it, but
	// asking it to stop again is refused — and a refusal that cleanup reads
	// as failure is what makes it walk away while the slot is still taken.
	actionWaitOut
)

func (a clearAction) String() string {
	switch a {
	case actionNone:
		return "none"
	case actionSkipRevert:
		return "skip-revert"
	case actionCancel:
		return "cancel"
	case actionWaitOut:
		return "wait-out"
	default:
		return "unknown"
	}
}

// clearActionFor decides how cleanup frees the active deploy slot from a
// request in this state.
func clearActionFor(deployState string) clearAction {
	switch {
	case deployState == drState.CompletePendingRevert:
		return actionSkipRevert
	case alreadyClearing(deployState):
		return actionWaitOut
	case blocksNewDeploy(deployState):
		return actionCancel
	default:
		return actionNone
	}
}

// Cancel is the only lever cleanup has on a running deploy, and the only one
// that frees the slot. A state cleanup answers with a cancel that the server
// refuses leaves the request holding the slot with nothing left to try: every
// test that deploys after it fails, each naming the held slot rather than the
// refusal that caused it.
func TestServerAcceptsCancelInEveryStateCleanupCancels(t *testing.T) {
	cancelled := 0
	for deployState, want := range wantClearAction {
		if want != actionCancel {
			continue
		}
		cancelled++
		t.Run(deployState, func(t *testing.T) {
			assert.True(t, localscale.CanCancelDeployRequest(deployState),
				"cleanup cancels a deploy request in %q, but the server refuses a cancel in that state",
				deployState)
		})
	}
	require.Positive(t, cancelled, "no state expects a cancel, so this proves nothing")
}

// The converse: a state the server will still act on is one cleanup can free,
// so cleanup must ask for something. Leaving it alone is the wedge, and it is
// the answer a state reaches by default when nobody decided about it.
func TestCleanupActsOnEveryStateTheServerWillStillCancel(t *testing.T) {
	for deployState, want := range wantClearAction {
		if !localscale.CanCancelDeployRequest(deployState) {
			continue
		}
		t.Run(deployState, func(t *testing.T) {
			assert.NotEqual(t, actionNone.String(), want.String(),
				"the server would still act on a deploy request in %q, so cleanup can free the slot and should",
				deployState)
		})
	}
}

// blocksNewDeploy reports whether a deploy request in this state occupies the
// one active deploy a database is allowed, so that a later test's deploy is
// refused until it clears.
//
// It asks the same question the deploy gate asks — whether the state is
// terminal — rather than listing the blocking states over again. A list here
// would be a second copy to find and update, and a state missing from it
// reads as cleared while the gate still refuses on it.
//
// Pending and Ready are the exception: they precede the deploy, so the gate
// never sees them and the processor never advances them.
func blocksNewDeploy(deployState string) bool {
	switch deployState {
	case drState.Pending, drState.Ready:
		return false
	default:
		return !localscale.IsTerminalDeployState(deployState)
	}
}

// alreadyClearing reports whether a deploy request in this state is on its way
// to a terminal one under its own steam, because a cancel or a revert is
// already running.
func alreadyClearing(deployState string) bool {
	switch deployState {
	case drState.InProgressCancel, drState.InProgressRevert, drState.InProgressRevertVSchema:
		return true
	default:
		return false
	}
}

// The states cleanup can meet, and what it owes each one. Written out rather
// than derived, so that a change to the predicates has to be restated here as
// a change in what cleanup does — which is the part a reviewer can check.
var wantClearAction = map[string]clearAction{
	// Before the deploy: the gate never sees these.
	"pending":    actionNone,
	"ready":      actionNone,
	"no_changes": actionNone,

	// Running: the slot is taken and nothing is retiring it on its own.
	"submitting":          actionCancel,
	"queued":              actionCancel,
	"in_progress":         actionCancel,
	"pending_cutover":     actionCancel,
	"in_progress_cutover": actionCancel,
	"in_progress_vschema": actionCancel,

	// Already retiring: still blocking, but a second ask is refused.
	"in_progress_cancel":         actionWaitOut,
	"in_progress_revert":         actionWaitOut,
	"in_progress_revert_vschema": actionWaitOut,

	// Holding the revert window open.
	"complete_pending_revert": actionSkipRevert,

	// Terminal: the slot is free.
	"complete":              actionNone,
	"complete_error":        actionNone,
	"complete_cancel":       actionNone,
	"complete_revert":       actionNone,
	"complete_revert_error": actionNone,
	"error":                 actionNone,

	// Neither the processor nor the gate writes these, but a request read
	// back in one still has to be classified rather than silently blocking.
	"cancelled": actionCancel,
	"failed":    actionCancel,
}

// Cleanup between tests has one job: leave the database's single active deploy
// slot free. Getting a state wrong costs the next test that deploys, which
// fails for a reason naming neither the request that is still holding the slot
// nor the cleanup that walked past it — so every state a deploy request can be
// read in is spelled out here.
func TestClearActionForEveryDeployState(t *testing.T) {
	for deployState, want := range wantClearAction {
		t.Run(deployState, func(t *testing.T) {
			assert.Equal(t, want.String(), clearActionFor(deployState).String())
		})
	}
}

// A state added to pkg/state without a decision here would be met by cleanup's
// default branch, which leaves it alone — silently reintroducing the wedge this
// table exists to prevent.
func TestEveryDeployStateHasAClearAction(t *testing.T) {
	v := reflect.ValueOf(state.DeployRequest)
	require.Positive(t, v.NumField(), "state.DeployRequest has no fields")
	for i := range v.NumField() {
		deployState := v.Field(i).String()
		_, ok := wantClearAction[deployState]
		assert.True(t, ok, "state.DeployRequest.%s (%q) has no entry in wantClearAction, "+
			"so cleanup's behavior in it is untested", v.Type().Field(i).Name, deployState)
	}
}
