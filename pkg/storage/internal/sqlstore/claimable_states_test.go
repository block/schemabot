package sqlstore

import (
	"reflect"
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClaimableApplyStates_CoverEveryRegisteredState enforces the recovery
// invariant documented on claimableApplyStates: every canonical Apply state is
// either terminal, stale-heartbeat claimable, or deliberately routed through
// its own recovery path. The stale-heartbeat re-claim is the only way a dead
// driver's apply gets picked back up, so a dwellable state missing from all
// three buckets is orphaned by any pod restart — no operator action can ever
// terminalize it. A new state added to pkg/state must fail here until its
// recovery story is decided explicitly.
func TestClaimableApplyStates_CoverEveryRegisteredState(t *testing.T) {
	claimable := make(map[string]bool)
	for _, s := range claimableApplyStates() {
		claimable[s] = true
	}

	// Non-terminal states deliberately absent from the stale-heartbeat claim
	// because a dedicated path owns their recovery. Each entry names that path;
	// adding a state here requires such a path to exist.
	recoveredElsewhere := map[string]string{
		// The claim predicate's pending arm (ClaimApplyByID) claims pending
		// applies with no staleness requirement; persistApplyClaim transitions
		// pending to running.
		state.Apply.Pending: "ClaimApplyByID pending arm",
		// Paused needs an explicit human decision: release resumes it, and the
		// stop-reconciliation claim (FindNextApplyForStopReconciliation)
		// terminalizes it when an operator stops the rollout instead.
		state.Apply.Paused: "release / stop-reconciliation path",
		// The retryable recovery claim re-drives it under its own attempt
		// budget (maxRecoveryAttempts) and freshness window.
		state.Apply.FailedRetryable: "retryable recovery claim path",
	}

	v := reflect.ValueOf(state.Apply)
	for i := 0; i < v.NumField(); i++ {
		fieldName := v.Type().Field(i).Name
		stateValue := v.Field(i).String()
		require.NotEmptyf(t, stateValue, "state.Apply.%s is empty", fieldName)

		if state.IsTerminalApplyState(stateValue) {
			assert.Falsef(t, claimable[stateValue],
				"state.Apply.%s (%q) is terminal and must not be stale-claimable: re-claiming a finished apply would re-drive completed work",
				fieldName, stateValue)
			continue
		}
		if path, ok := recoveredElsewhere[stateValue]; ok {
			assert.Falsef(t, claimable[stateValue],
				"state.Apply.%s (%q) is owned by the %s and must not also be stale-claimable: two claim paths racing over one apply",
				fieldName, stateValue, path)
			continue
		}
		assert.Truef(t, claimable[stateValue],
			"state.Apply.%s (%q) is a non-terminal dwell state with no recovery path: add it to claimableApplyStates() or give it a dedicated claim arm and allowlist it here, otherwise a driver death permanently orphans the apply",
			fieldName, stateValue)
	}
}

// TestClaimableApplyStates_AreRegisteredActiveStates checks the claim list
// from the other direction: every entry must normalize to a canonical,
// non-terminal Apply state. The claim query matches raw stored strings, so
// legacy aliases (e.g. recovering_cutover) are allowed as long as
// NormalizeState maps them onto a registered active state — an entry that
// normalizes to nothing registered would silently never match a row, and a
// terminal entry would let recovery re-claim finished applies.
func TestClaimableApplyStates_AreRegisteredActiveStates(t *testing.T) {
	registered := make(map[string]bool)
	v := reflect.ValueOf(state.Apply)
	for _, field := range v.Fields() {
		registered[field.String()] = true
	}

	for _, s := range claimableApplyStates() {
		canonical := state.NormalizeState(s)
		assert.Truef(t, registered[canonical],
			"claimable entry %q normalizes to %q, which is not a registered state.Apply value",
			s, canonical)
		assert.Falsef(t, state.IsTerminalApplyState(canonical),
			"claimable entry %q normalizes to terminal state %q: recovery must never re-claim a finished apply",
			s, canonical)
	}
}
