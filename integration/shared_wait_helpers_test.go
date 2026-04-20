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

// fetchProgressState calls the progress API by apply ID and returns the normalized state.
func fetchProgressState(endpoint, applyID string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), progressTimeout)
	defer cancel()
	result, err := client.GetProgressCtx(ctx, endpoint, applyID)
	if err != nil {
		return "", err
	}
	return state.NormalizeState(result.State), nil
}

// waitForState polls the progress API by apply ID until the overall state
// matches expectedState or the timeout expires.
func waitForState(t *testing.T, endpoint, applyID, expectedState string, timeout time.Duration) {
	t.Helper()
	expected := state.NormalizeState(expectedState)
	deadline := time.Now().Add(timeout)
	var lastState string
	for time.Now().Before(deadline) {
		s, err := fetchProgressState(endpoint, applyID)
		if err == nil {
			lastState = s
			if s == expected {
				return
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	require.Failf(t, "timeout", "timeout waiting for apply %s state %q, last state: %q", applyID, expectedState, lastState)
}
