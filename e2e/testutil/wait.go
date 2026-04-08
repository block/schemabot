//go:build e2e || integration

// Package testutil provides shared test helpers for e2e tests that depend on
// internal SchemaBot packages (client, state). For helpers with zero internal
// dependencies, use pkg/e2eutil instead.
package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
)

// ProgressTimeout is the per-request timeout for progress API polling.
const ProgressTimeout = 5 * time.Second

// FetchProgress calls the progress API by database/environment and returns the
// full ProgressResponse with its State normalized.
func FetchProgress(endpoint, dbName, env string) (*apitypes.ProgressResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ProgressTimeout)
	defer cancel()
	result, err := client.GetProgressCtx(ctx, endpoint, dbName, env)
	if err != nil {
		return nil, err
	}
	result.State = state.NormalizeState(result.State)
	return result, nil
}

// WaitForState polls the progress API until the overall state matches expectedState
// or the timeout expires.
func WaitForState(t *testing.T, endpoint, dbName, env, expectedState string, timeout time.Duration) {
	t.Helper()
	expected := state.NormalizeState(expectedState)
	deadline := time.Now().Add(timeout)
	var lastState string
	for time.Now().Before(deadline) {
		result, err := FetchProgress(endpoint, dbName, env)
		if err == nil {
			lastState = result.State
			if result.State == expected {
				return
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	require.Failf(t, "timeout", "timeout waiting for state %q, last API state: %q", expectedState, lastState)
}

// WaitForStateByApplyID polls progress by apply_id until the expected state is reached.
// Prefer this over WaitForState when multiple applies exist for the same database.
func WaitForStateByApplyID(t *testing.T, endpoint, applyID, expectedState string, timeout time.Duration) {
	t.Helper()
	expected := state.NormalizeState(expectedState)
	deadline := time.Now().Add(timeout)
	var lastState string
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), ProgressTimeout)
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

// WaitForAnyStateByApplyID polls progress by apply_id until any of the expected
// states matches or timeout expires. Returns the matched state string.
// Useful for stop/start flows where Spirit may complete before stop takes effect.
func WaitForAnyStateByApplyID(t *testing.T, endpoint, applyID string, expectedStates []string, timeout time.Duration) string {
	t.Helper()
	expected := make([]string, len(expectedStates))
	for i, s := range expectedStates {
		expected[i] = state.NormalizeState(s)
	}
	deadline := time.Now().Add(timeout)
	var lastState string
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), ProgressTimeout)
		result, err := client.GetProgressByApplyIDCtx(ctx, endpoint, applyID)
		cancel()
		if err == nil {
			lastState = state.NormalizeState(result.State)
			for i, exp := range expected {
				if lastState == exp {
					return expectedStates[i]
				}
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	require.Failf(t, "timeout", "timeout waiting for apply %s any of states %v, last state: %q", applyID, expectedStates, lastState)
	return ""
}

// WaitForAnyState polls the progress API until any of the expected states matches
// or timeout expires. Returns the matched state string.
func WaitForAnyState(t *testing.T, endpoint, dbName, env string, expectedStates []string, timeout time.Duration) string {
	t.Helper()
	expected := make([]string, len(expectedStates))
	for i, s := range expectedStates {
		expected[i] = state.NormalizeState(s)
	}
	deadline := time.Now().Add(timeout)
	var lastState string
	for time.Now().Before(deadline) {
		result, err := FetchProgress(endpoint, dbName, env)
		if err == nil {
			lastState = result.State
			for i, exp := range expected {
				if result.State == exp {
					return expectedStates[i]
				}
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	require.Failf(t, "timeout", "timeout waiting for any of states %v, last API state: %q", expectedStates, lastState)
	return ""
}
