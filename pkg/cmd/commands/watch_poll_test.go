package commands

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
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
				ApplyID:     scriptedApplyID,
				Environment: "staging",
				State:       step.state,
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

// terminalWatchCases pairs every terminal apply state with whether a
// non-interactive watch exits cleanly for it: only completed does, and every
// other terminal state, in which the schema change is not on the target, fails.
func terminalWatchCases(t *testing.T) map[string]bool {
	t.Helper()
	cases := map[string]bool{state.Apply.Stopped: false}
	for _, s := range state.SettledApplyStates {
		cases[s] = false
	}
	cases[state.Apply.Completed] = true
	for s := range cases {
		require.True(t, state.IsTerminalApplyState(s), "%s must be terminal", s)
	}
	require.Contains(t, cases, state.Apply.Cancelled)
	require.Contains(t, cases, state.Apply.Reverted)
	return cases
}

// requireTerminalWatchExit checks a watch's exit against terminalWatchCases:
// a clean exit, or a failure that the CLI turns into a non-zero status.
func requireTerminalWatchExit(t *testing.T, err error, wantSuccess bool) {
	t.Helper()
	if wantSuccess {
		require.NoError(t, err)
		return
	}
	require.Error(t, err)
	assert.Equal(t, 1, ExitCodeFor(err))
}

// A CI job watching an apply in log mode ends as soon as the apply reaches any
// terminal state, including one an operator caused from elsewhere by
// stopping, cancelling, or reverting it. It prints the outcome summary and
// exits non-zero unless the change completed.
func TestWatchApplyProgressLog_ExitsOnEveryTerminalState(t *testing.T) {
	for terminal, wantSuccess := range terminalWatchCases(t) {
		t.Run(terminal, func(t *testing.T) {
			poller, _ := scriptedPoller(t,
				pollStep{state: state.Apply.Running},
				pollStep{state: terminal},
			)

			var err error
			out := stripANSI(captureOutput(t, func() {
				err = watchApplyProgressLog(poller, time.Hour)
			}))

			requireTerminalWatchExit(t, err, wantSuccess)
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
// was stopped, cancelled, or reverted.
func TestWatchApplyProgressJSON_ExitsOnEveryTerminalState(t *testing.T) {
	for terminal, wantSuccess := range terminalWatchCases(t) {
		t.Run(terminal, func(t *testing.T) {
			poller, waits := scriptedPoller(t,
				pollStep{state: state.Apply.Running},
				pollStep{state: terminal},
			)

			var err error
			out := captureOutput(t, func() {
				err = watchApplyProgressJSON(poller)
			})

			requireTerminalWatchExit(t, err, wantSuccess)
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
// with an error that keeps the cause, claims nothing about an apply it can no
// longer see, and tells the operator how to see it again.
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
	assert.Contains(t, err.Error(), "this watch does not affect the apply; rerun the original watch command, or 'schemabot progress "+scriptedApplyID+"', to see its current state")
	assert.Len(t, *waits, maxConsecutiveProgressFailures-1)
	assert.Equal(t, maxConsecutiveProgressFailures-1, strings.Count(out, "Progress unavailable, retrying"))
	assert.Contains(t, out, `attempt=1/10 retry_in=4s error="cannot connect to http://schemabot.test (is the server running?)"`)
}

func TestIsRetryableFetchError_TransportAndUnstructuredServerErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "server error without API code", err: &client.APIError{Status: http.StatusBadGateway, Message: "bad gateway"}},
		{name: "rate limited by a proxy without API code", err: &client.APIError{Status: http.StatusTooManyRequests, Message: "too many requests"}},
		{name: "response body interrupted", err: fmt.Errorf("read response: %w", io.ErrUnexpectedEOF)},
		{name: "response body read timed out", err: fmt.Errorf("read response: %w", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded})},
		{name: "response body connection reset", err: fmt.Errorf("read response: %w", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET})},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.True(t, isRetryableFetchError(tc.err))
		})
	}
}

// Failures the CLI produces on its own side end the watch on the first poll,
// even when they wrap a network error: an operator who cancelled the watch
// means it, and a refused token transport refuses again on every retry.
func TestIsRetryableFetchError_ClientSideFailuresArePermanent(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "operator cancelled", err: context.Canceled},
		{name: "operator cancelled while reading the body", err: fmt.Errorf("read response: %w", &net.OpError{Op: "read", Net: "tcp", Err: context.Canceled})},
		{name: "token refused over plaintext", err: &url.Error{Op: "Get", URL: "http://schemabot.example.test/api/progress/apply/x", Err: client.ErrInsecureTokenTransport}},
		{name: "client error without API code", err: &client.APIError{Status: http.StatusForbidden, Message: "forbidden"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.False(t, isRetryableFetchError(tc.err))
		})
	}
}

// A bearer token that would cross a plaintext connection to a remote host is
// refused before any dial. Retrying cannot change that answer, so the watch
// must end on the first poll with the refusal rather than retrying it.
func TestIsRetryableFetchError_InsecureTokenRefusalIsPermanent(t *testing.T) {
	client.SetAuthToken("probe-token")
	t.Cleanup(func() { client.SetAuthToken("") })

	_, err := client.GetProgress("http://schemabot.example.test", "apply-a1b2c3d4e5f6")

	require.ErrorIs(t, err, client.ErrInsecureTokenTransport)
	assert.False(t, isRetryableFetchError(err), "a token-transport refusal is permanent")
}

// A stopped apply fails the watch, so a script gating on its exit status does
// not carry on as if the schema change had landed, and the error tells the
// operator the command that resumes it.
func TestTerminalWatchExit_StoppedNamesTheResumeCommand(t *testing.T) {
	err := terminalWatchExit(&apitypes.ProgressResponse{ApplyID: scriptedApplyID, Environment: "staging", State: state.Apply.Stopped})

	require.EqualError(t, err, "apply "+scriptedApplyID+" was stopped, so the schema change is not on the target; use 'schemabot start -e staging "+scriptedApplyID+"' to resume it")
	assert.Equal(t, 1, ExitCodeFor(err))
}

// A by-apply-ID watch that is told there is no active schema change never saw
// how the apply ended. Both non-interactive watches fail on it, naming the
// apply and where to look, rather than one exiting cleanly and the other
// polling forever.
func TestWatchApplyProgress_NoActiveChangeFailsTheWatch(t *testing.T) {
	wantErr := "progress for apply " + scriptedApplyID + " reported no active schema change, so this watch cannot tell how the apply ended; check 'schemabot progress " + scriptedApplyID + "'"
	watches := map[string]func(*progressPoller) error{
		"log":  func(p *progressPoller) error { return watchApplyProgressLog(p, time.Hour) },
		"json": watchApplyProgressJSON,
	}
	for name, watch := range watches {
		t.Run(name, func(t *testing.T) {
			poller, waits := scriptedPoller(t,
				pollStep{state: state.Apply.Running},
				pollStep{state: state.NoActiveChange},
			)

			var err error
			captureOutput(t, func() {
				err = watch(poller)
			})

			require.EqualError(t, err, wantErr)
			assert.Len(t, *waits, 1, "the watch ends on the first no_active_change response")
		})
	}
}

func TestIsActiveStatus_CancelledIsInactive(t *testing.T) {
	assert.False(t, isActiveStatus(state.Apply.Cancelled))
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

// proxyResponse is one scripted answer from whatever sits in front of the
// server: a status, the Retry-After header it sends (empty for none), and its
// body.
type proxyResponse struct {
	status     int
	retryAfter string
	body       string
}

// proxiedProgressPoller is a real progress poller whose fetches go through the
// CLI's HTTP client to a server that answers with each scripted response in
// turn. Waits are recorded instead of slept.
func proxiedProgressPoller(t *testing.T, responses ...proxyResponse) (*progressPoller, *[]time.Duration) {
	t.Helper()
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !assert.Less(t, calls, len(responses), "watch kept polling after its scripted responses ran out") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		resp := responses[calls]
		calls++
		if resp.retryAfter != "" {
			w.Header().Set("Retry-After", resp.retryAfter)
		}
		w.WriteHeader(resp.status)
		_, _ = w.Write([]byte(resp.body))
	}))
	t.Cleanup(srv.Close)

	var waits []time.Duration
	p := newProgressPoller(srv.URL, scriptedApplyID)
	p.sleep = func(d time.Duration) { waits = append(waits, d) }
	return p, &waits
}

const (
	proxyRateLimitPage = `<html><body><h1>429 Too Many Requests</h1></body></html>`
	proxyUnavailable   = `<html><body><h1>503 Service Unavailable</h1></body></html>`
)

func runningProgressBody() string {
	return `{"apply_id": "` + scriptedApplyID + `", "state": "` + state.Apply.Running + `"}`
}

// A rate limiter in front of the server that refuses a poll with a
// Retry-After header and an HTML page is waited out for as long as it asked,
// not on the shorter fetch-error backoff.
func TestProgressPoller_HonorsProxyRetryAfterHeader(t *testing.T) {
	poller, waits := proxiedProgressPoller(t,
		proxyResponse{status: http.StatusTooManyRequests, retryAfter: "45", body: proxyRateLimitPage},
		proxyResponse{status: http.StatusOK, body: runningProgressBody()},
	)

	result, err := poller.next(func(progressRetry) {})

	require.NoError(t, err)
	assert.Equal(t, state.Apply.Running, result.State)
	assert.Equal(t, []time.Duration{45 * time.Second}, *waits)
}

// A code-less 503 that names no delay is retried on the plain fetch-error
// backoff.
func TestProgressPoller_ProxyUnavailableWithoutRetryAfterUsesBackoff(t *testing.T) {
	poller, waits := proxiedProgressPoller(t,
		proxyResponse{status: http.StatusServiceUnavailable, body: proxyUnavailable},
		proxyResponse{status: http.StatusOK, body: runningProgressBody()},
	)

	result, err := poller.next(func(progressRetry) {})

	require.NoError(t, err)
	assert.Equal(t, state.Apply.Running, result.State)
	assert.Equal(t, []time.Duration{fetchErrorBackoff(1)}, *waits)
}

// A Retry-After header asking for longer than the CLI's bound, whether in
// seconds or as a far-future date, is waited out only up to that bound, so a
// misconfigured proxy cannot park a watch for a day.
func TestProgressPoller_BoundsProxyRetryAfterHeader(t *testing.T) {
	for _, header := range []string{"86400", "Fri, 31 Dec 9999 23:59:59 GMT"} {
		t.Run(header, func(t *testing.T) {
			poller, waits := proxiedProgressPoller(t,
				proxyResponse{status: http.StatusServiceUnavailable, retryAfter: header, body: proxyUnavailable},
				proxyResponse{status: http.StatusOK, body: runningProgressBody()},
			)

			result, err := poller.next(func(progressRetry) {})

			require.NoError(t, err)
			assert.Equal(t, state.Apply.Running, result.State)
			assert.Equal(t, []time.Duration{5 * time.Minute}, *waits)
		})
	}
}

// When the body and the Retry-After header both name a delay, the watch waits
// for the larger, so it never polls sooner than either asked.
func TestProgressRetryWait_LargerOfBodyAndHeaderDelayWins(t *testing.T) {
	tests := []struct {
		name        string
		bodySeconds int
		header      time.Duration
		want        time.Duration
	}{
		{name: "body longer", bodySeconds: 45, header: 10 * time.Second, want: 45 * time.Second},
		{name: "header longer", bodySeconds: 10, header: 45 * time.Second, want: 45 * time.Second},
		{name: "both shorter than backoff", bodySeconds: 1, header: time.Second, want: fetchErrorBackoff(1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			limited := &client.APIError{
				Status:            http.StatusTooManyRequests,
				ErrorCode:         apitypes.ErrCodeRateLimited,
				Message:           "rate limited",
				RetryAfterSeconds: tc.bodySeconds,
				RetryAfterHeader:  tc.header,
			}
			assert.Equal(t, tc.want, progressRetryWait(limited, 1))
		})
	}
}
