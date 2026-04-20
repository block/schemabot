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

// FetchProgress calls the progress API by apply ID and returns the
// full ProgressResponse with its State normalized.
func FetchProgress(endpoint, applyID string) (*apitypes.ProgressResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ProgressTimeout)
	defer cancel()
	result, err := client.GetProgressCtx(ctx, endpoint, applyID)
	if err != nil {
		return nil, err
	}
	result.State = state.NormalizeState(result.State)
	return result, nil
}

// WaitForState polls the progress API by apply ID until the overall state
// matches expectedState or the timeout expires.
func WaitForState(t *testing.T, endpoint, applyID, expectedState string, timeout time.Duration) {
	t.Helper()
	expected := state.NormalizeState(expectedState)
	deadline := time.Now().Add(timeout)
	var lastState string
	for time.Now().Before(deadline) {
		result, err := FetchProgress(endpoint, applyID)
		if err == nil {
			lastState = result.State
			if result.State == expected {
				return
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	require.Failf(t, "timeout", "timeout waiting for apply %s state %q, last state: %q", applyID, expectedState, lastState)
}

// WaitForAnyState polls the progress API by apply ID until any of the expected
// states matches or timeout expires. Returns the matched state string.
func WaitForAnyState(t *testing.T, endpoint, applyID string, expectedStates []string, timeout time.Duration) string {
	t.Helper()
	expected := make([]string, len(expectedStates))
	for i, s := range expectedStates {
		expected[i] = state.NormalizeState(s)
	}
	deadline := time.Now().Add(timeout)
	var lastState string
	for time.Now().Before(deadline) {
		result, err := FetchProgress(endpoint, applyID)
		if err == nil {
			lastState = result.State
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
