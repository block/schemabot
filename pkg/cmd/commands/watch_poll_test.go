package commands

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/state"
)

const scriptedApplyID = "apply-a1b2c3d4e5f6"

// pollStep is one scripted answer to a progress fetch: an apply state, or an
// error in its place.
type pollStep struct {
	state string
	err   error
}

// scriptedPoller answers each progress fetch with the next scripted step and
// records every wait instead of sleeping. A watch that polls past the end of
// its script fails the test, which is how a watch that never exits shows up.
func scriptedPoller(t *testing.T, steps ...pollStep) (*progressPoller, *[]time.Duration) {
	t.Helper()
	var waits []time.Duration
	calls := 0
	p := &progressPoller{
		applyID: scriptedApplyID,
		fetch: func() (*apitypes.ProgressResponse, error) {
			require.Less(t, calls, len(steps), "watch kept polling after its scripted progress ran out")
			step := steps[calls]
			calls++
			if step.err != nil {
				return nil, step.err
			}
			return &apitypes.ProgressResponse{
				ApplyID: scriptedApplyID,
				State:   step.state,
				Tables: []*apitypes.TableProgressResponse{
					{TableName: "orders", Status: step.state},
				},
			}, nil
		},
		sleep: func(d time.Duration) { waits = append(waits, d) },
	}
	return p, &waits
}

func transientPollError() error {
	return &client.ConnectionError{Endpoint: "http://schemabot.test", Err: errors.New("dial tcp: connection refused")}
}

func transientSteps(n int) []pollStep {
	steps := make([]pollStep, n)
	for i := range steps {
		steps[i] = pollStep{err: transientPollError()}
	}
	return steps
}

// terminalWatchCases pairs every terminal apply state with the exit a
// non-interactive watch reports for it: completed and stopped exit cleanly,
// and every settled state in which the schema change did not land fails.
func terminalWatchCases(t *testing.T) map[string]error {
	t.Helper()
	cases := map[string]error{state.Apply.Stopped: nil}
	for _, s := range state.SettledApplyStates {
		cases[s] = ErrSilent
	}
	cases[state.Apply.Completed] = nil
	for s := range cases {
		require.True(t, state.IsTerminalApplyState(s), "%s must be terminal", s)
	}
	require.Contains(t, cases, state.Apply.Cancelled)
	require.Contains(t, cases, state.Apply.Reverted)
	return cases
}

// A CI job watching an apply in log mode ends as soon as the apply reaches any
// terminal state, including one an operator caused from elsewhere by
// cancelling or reverting it. It prints the outcome summary and exits non-zero
// unless the change completed or was deliberately stopped.
func TestWatchApplyProgressLog_ExitsOnEveryTerminalState(t *testing.T) {
	for terminal, wantErr := range terminalWatchCases(t) {
		t.Run(terminal, func(t *testing.T) {
			poller, _ := scriptedPoller(t,
				pollStep{state: state.Apply.Running},
				pollStep{state: terminal},
			)

			var err error
			out := stripANSI(captureOutput(t, func() {
				err = watchApplyProgressLog(poller, time.Hour)
			}))

			if wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, wantErr)
			}
			assert.Contains(t, out, "Apply "+terminal+" ")
			assert.Contains(t, out, `tables="`)
		})
	}
}

// Cancelled and reverted tables count toward the summary's table total and
// are named, so the summary of a cancelled apply does not read as zero tables.
func TestLogEmitter_EmitApplySummaryCountsCancelledAndReverted(t *testing.T) {
	e := &logEmitter{applyID: scriptedApplyID}
	tableStates := map[string]*tableLogState{
		"users":    {status: state.Apply.Cancelled},
		"orders":   {status: state.Apply.Cancelled},
		"products": {status: state.Apply.Reverted},
	}

	plain := stripANSI(captureOutput(t, func() {
		e.emitApplySummary(state.Apply.Cancelled, tableStates, time.Now(), "")
	}))

	assert.Contains(t, plain, "Apply cancelled")
	assert.Contains(t, plain, `tables="0/3 succeeded"`)
	assert.Contains(t, plain, "cancelled=2")
	assert.Contains(t, plain, "reverted=1")
	assert.NotContains(t, plain, "failed=")
}

// A script consuming the JSON progress stream sees the terminal state as the
// last line and the process exits, with a failing status for an apply that
// was cancelled or reverted.
func TestWatchApplyProgressJSON_ExitsOnEveryTerminalState(t *testing.T) {
	for terminal, wantErr := range terminalWatchCases(t) {
		t.Run(terminal, func(t *testing.T) {
			poller, waits := scriptedPoller(t,
				pollStep{state: state.Apply.Running},
				pollStep{state: terminal},
			)

			var err error
			out := captureOutput(t, func() {
				err = watchApplyProgressJSON(poller)
			})

			if wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, wantErr)
			}
			lines := strings.Split(strings.TrimSpace(out), "\n")
			require.Len(t, lines, 2)
			var last map[string]string
			require.NoError(t, json.Unmarshal([]byte(lines[1]), &last))
			assert.Equal(t, terminal, last["state"])
			assert.Equal(t, []time.Duration{pollInterval}, *waits)
		})
	}
}

// An operator watching after triggering cutover is told when someone else
// cancels or reverts the apply, with the next step, instead of the watch
// polling on. Only a completed apply exits cleanly.
func TestWatchAfterCutover_ExitsOnEveryTerminalState(t *testing.T) {
	cases := []struct {
		state   string
		wantErr string
	}{
		{state: state.Apply.Completed},
		{state: state.Apply.Failed, wantErr: "cutover failed"},
		{state: state.Apply.Stopped, wantErr: "schema change was stopped during cutover"},
		{state: state.Apply.Cancelled, wantErr: "schema change was cancelled during cutover; start a new apply to retry"},
		{state: state.Apply.Reverted, wantErr: "schema change was reverted after cutover; start a new apply to make it again"},
	}
	for _, tc := range cases {
		t.Run(tc.state, func(t *testing.T) {
			poller, _ := scriptedPoller(t,
				pollStep{state: state.Apply.CuttingOver},
				pollStep{state: state.Apply.RevertWindow},
				pollStep{state: tc.state},
			)

			var err error
			out := captureOutput(t, func() {
				err = watchAfterCutover(poller)
			})

			if tc.wantErr == "" {
				require.NoError(t, err)
				assert.Contains(t, out, "✓ Complete")
				return
			}
			require.EqualError(t, err, tc.wantErr)
		})
	}
}

// A watch rides out a run of transient failures one short of the limit,
// backing off on the shared schedule and reporting each retry, then carries
// on watching once progress is readable again.
func TestProgressPoller_ToleratesTransientFailuresBelowLimit(t *testing.T) {
	steps := append(transientSteps(maxConsecutiveProgressFailures-1),
		pollStep{state: state.Apply.Running},
		pollStep{state: state.Apply.Completed},
	)
	poller, waits := scriptedPoller(t, steps...)

	var err error
	stderr := captureStderr(t, func() {
		captureOutput(t, func() {
			err = watchApplyProgressJSON(poller)
		})
	})

	require.NoError(t, err)
	wantWaits := make([]time.Duration, 0, maxConsecutiveProgressFailures)
	for attempt := 1; attempt < maxConsecutiveProgressFailures; attempt++ {
		wantWaits = append(wantWaits, fetchErrorBackoff(attempt))
	}
	wantWaits = append(wantWaits, pollInterval)
	assert.Equal(t, wantWaits, *waits)
	assert.Equal(t, maxConsecutiveProgressFailures-1, strings.Count(stderr, "Progress unavailable"))
	assert.Contains(t, stderr, "Progress unavailable (attempt 1/10), retrying in 4s: cannot connect to http://schemabot.test (is the server running?)")
}

// The failure count is consecutive: a readable poll in the middle of a flaky
// stretch starts the count again, so a long watch over an unreliable network
// is not ended by failures spread across hours.
func TestProgressPoller_SuccessResetsFailureCount(t *testing.T) {
	steps := transientSteps(maxConsecutiveProgressFailures - 1)
	steps = append(steps, pollStep{state: state.Apply.Running})
	steps = append(steps, transientSteps(maxConsecutiveProgressFailures-1)...)
	steps = append(steps, pollStep{state: state.Apply.Completed})
	poller, _ := scriptedPoller(t, steps...)

	var err error
	captureStderr(t, func() {
		captureOutput(t, func() {
			err = watchApplyProgressJSON(poller)
		})
	})

	require.NoError(t, err)
}

// Once the limit of consecutive transient failures is reached the watch ends
// with an error that keeps the cause and tells the operator the apply is
// still running and how to resume watching it.
func TestProgressPoller_FailsAfterConsecutiveTransientFailures(t *testing.T) {
	poller, waits := scriptedPoller(t, transientSteps(maxConsecutiveProgressFailures)...)

	var err error
	out := stripANSI(captureOutput(t, func() {
		err = watchApplyProgressLog(poller, time.Hour)
	}))

	require.Error(t, err)
	var connErr *client.ConnectionError
	require.ErrorAs(t, err, &connErr)
	assert.Contains(t, err.Error(), "fetch progress for apply "+scriptedApplyID+": 10 consecutive attempts failed")
	assert.Contains(t, err.Error(), "the schema change continues on the server, resume watching with '"+cliname.Name()+" progress "+scriptedApplyID+"'")
	assert.Len(t, *waits, maxConsecutiveProgressFailures-1)
	assert.Equal(t, maxConsecutiveProgressFailures-1, strings.Count(out, "Progress unavailable, retrying"))
	assert.Contains(t, out, `attempt=1/10 retry_in=4s error="cannot connect to http://schemabot.test (is the server running?)"`)
}

// A permanent error, such as an apply ID the server does not know, ends the
// watch on the first poll: retrying cannot change the answer.
func TestProgressPoller_FailsImmediatelyOnPermanentError(t *testing.T) {
	notFound := &client.APIError{Status: http.StatusNotFound, ErrorCode: apitypes.ErrCodeNotFound, Message: "apply not found"}
	poller, waits := scriptedPoller(t, pollStep{err: notFound})

	_, err := poller.next(func(progressRetry) { t.Fatal("a permanent error must not be retried") })

	require.ErrorIs(t, err, notFound)
	assert.EqualError(t, err, "fetch progress for apply "+scriptedApplyID+": apply not found")
	assert.Empty(t, *waits)
}

// A rate-limited poll waits at least as long as the server asked, even when
// that is longer than the backoff would have been.
func TestProgressPoller_HonorsServerRetryAfter(t *testing.T) {
	limited := &client.APIError{Status: http.StatusTooManyRequests, ErrorCode: apitypes.ErrCodeRateLimited, Message: "rate limited", RetryAfterSeconds: 45}
	poller, waits := scriptedPoller(t, pollStep{err: limited}, pollStep{state: state.Apply.Running})

	result, err := poller.next(func(progressRetry) {})

	require.NoError(t, err)
	assert.Equal(t, state.Apply.Running, result.State)
	assert.Equal(t, []time.Duration{45 * time.Second}, *waits)
}
