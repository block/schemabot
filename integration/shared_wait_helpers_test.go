//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
)

// progressTimeout is the per-request timeout for progress API polling.
const progressTimeout = 5 * time.Second

// fetchProgressState calls the progress API by database/environment and returns the normalized state.
func fetchProgressState(endpoint, dbName, env string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), progressTimeout)
	defer cancel()
	result, err := client.GetProgressCtx(ctx, endpoint, dbName, env)
	if err != nil {
		return "", err
	}
	return state.NormalizeState(result.State), nil
}

// waitForState polls the progress API until the overall state matches expectedState
// or the timeout expires. Uses database/environment lookup (no apply_id needed).
func waitForState(t *testing.T, endpoint, dbName, env, expectedState string, timeout time.Duration) {
	t.Helper()
	expected := state.NormalizeState(expectedState)
	deadline := time.Now().Add(timeout)
	var lastState string
	for time.Now().Before(deadline) {
		state, err := fetchProgressState(endpoint, dbName, env)
		if err == nil {
			lastState = state
			if state == expected {
				return
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	require.Failf(t, "timeout", "timeout waiting for state %q, last API state: %q", expectedState, lastState)
}

// waitForStateByApplyID polls progress by apply_id until the expected state is reached.
// This is preferred over waitForState when multiple applies exist for the same database.
func waitForStateByApplyID(t *testing.T, endpoint, applyID, expectedState string, timeout time.Duration) {
	t.Helper()
	expected := state.NormalizeState(expectedState)
	deadline := time.Now().Add(timeout)
	var lastState string
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), progressTimeout)
		result, err := client.GetProgressByApplyIDCtx(ctx, endpoint, applyID)
		cancel()
		if err == nil {
			lastState = state.NormalizeState(result.State)
			if lastState == expected {
				return
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	require.Failf(t, "timeout", "timeout waiting for apply %s state %q, last state: %q", applyID, expectedState, lastState)
}

// waitForAnyState polls the progress API until any of the expected states matches
// or timeout expires. Returns the matched state string.
func waitForAnyState(t *testing.T, endpoint, dbName, env string, expectedStates []string, timeout time.Duration) string {
	t.Helper()
	expected := make([]string, len(expectedStates))
	for i, s := range expectedStates {
		expected[i] = state.NormalizeState(s)
	}
	deadline := time.Now().Add(timeout)
	var lastState string
	for time.Now().Before(deadline) {
		state, err := fetchProgressState(endpoint, dbName, env)
		if err == nil {
			lastState = state
			for i, exp := range expected {
				if state == exp {
					return expectedStates[i]
				}
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	require.Failf(t, "timeout", "timeout waiting for any of states %v, last API state: %q", expectedStates, lastState)
	return ""
}
