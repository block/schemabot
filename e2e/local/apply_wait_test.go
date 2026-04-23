//go:build e2e

package local

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/e2e/testutil"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
)

func extractApplyID(t *testing.T, output string) string {
	t.Helper()
	for line := range strings.SplitSeq(strings.TrimSpace(output), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var result struct {
			ApplyID string `json:"apply_id"`
		}
		if err := json.Unmarshal([]byte(line), &result); err == nil && result.ApplyID != "" {
			return result.ApplyID
		}
	}
	require.Failf(t, "apply_id not found", "could not find apply_id in JSON output: %s", output)
	return ""
}

func fetchApplyState(endpoint, applyID string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), testutil.ProgressTimeout)
	defer cancel()
	result, err := client.GetProgressCtx(ctx, endpoint, applyID)
	if err != nil {
		return "", err
	}
	return state.NormalizeState(result.State), nil
}

func waitForApplyState(t *testing.T, endpoint, applyID, expectedState string, timeout time.Duration) {
	t.Helper()
	expected := strings.ToLower(expectedState)
	deadline := time.Now().Add(timeout)
	var lastState, lastError string
	start := time.Now()
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), testutil.ProgressTimeout)
		result, err := client.GetProgressCtx(ctx, endpoint, applyID)
		cancel()
		if err == nil {
			newState := state.NormalizeState(result.State)
			if newState != lastState {
				t.Logf("waitForApplyState: %s state=%s (elapsed=%s)", applyID, newState, time.Since(start))
			}
			lastState = newState
			lastError = result.ErrorMessage
			if lastState == expected {
				return
			}
		} else {
			t.Logf("waitForApplyState: %s poll error: %v (elapsed=%s)", applyID, err, time.Since(start))
		}
		time.Sleep(300 * time.Millisecond)
	}
	require.Failf(t, "timeout", "timeout waiting for apply %s state %q after %s, last API state: %q, error: %q", applyID, expectedState, time.Since(start), lastState, lastError)
}

func waitForApplyAnyState(t *testing.T, endpoint, applyID string, expectedStates []string, timeout time.Duration) string {
	t.Helper()
	expected := make([]string, len(expectedStates))
	for i, s := range expectedStates {
		expected[i] = strings.ToLower(s)
	}
	deadline := time.Now().Add(timeout)
	var lastState string
	for time.Now().Before(deadline) {
		s, err := fetchApplyState(endpoint, applyID)
		if err == nil {
			lastState = s
			for i, exp := range expected {
				if s == exp {
					return expectedStates[i]
				}
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	require.Failf(t, "timeout", "timeout waiting for apply %s any of states %v, last API state: %q", applyID, expectedStates, lastState)
	return ""
}
