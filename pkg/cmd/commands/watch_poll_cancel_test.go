package commands

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

// watchStopDeadline bounds how long a cancelled watch may take to end. It is
// far shorter than the waits the tests cancel, so a watch that sits one out
// fails instead of passing late.
const watchStopDeadline = 5 * time.Second

// awaitSignal waits for ch to fire, failing the test if it does not within the
// shared deadline.
func awaitSignal[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(watchStopDeadline):
		require.FailNow(t, "timed out waiting for "+what)
		var zero T
		return zero
	}
}

// requireWatchStopped checks that a watch ended because the operator stopped
// it: a silent error carrying the cancellation, so the CLI exits non-zero
// without a raw "context canceled" line under the stopped-watching notice.
func requireWatchStopped(t *testing.T, err error) {
	t.Helper()
	require.ErrorIs(t, err, ErrSilent)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, ExitCodeFor(err))
}

// blockingProgressServer answers every progress fetch by holding the request
// open until the client gives up on it, and reports each fetch that arrives on
// the returned channel. Requests are released when the test ends, so a client
// that never cancels cannot hang the server's shutdown.
func blockingProgressServer(t *testing.T, next http.Handler) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	inFlight := make(chan struct{}, 1)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if next != nil && r.URL.Path != "/api/progress/apply/"+cancelApplyID {
			next.ServeHTTP(w, r)
			return
		}
		select {
		case inFlight <- struct{}{}:
		default:
		}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	return srv, inFlight
}

const cancelApplyID = "apply-7c2e"

// An operator presses Ctrl-C while the watch is waiting out a server that
// asked for 30 seconds before the next poll. The watch ends at once with the
// notice that the apply continues and how to watch it again, rather than
// sitting out the wait and needing a second Ctrl-C.
func TestProgressPoller_CancelDuringRetryWaitStopsPromptly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, err := w.Write([]byte(proxyUnavailable))
		assert.NoError(t, err)
	}))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	poller := newProgressPoller(ctx, srv.URL, cancelApplyID)

	retrying := make(chan progressRetry, 1)
	done := make(chan error, 1)
	stderr := captureStderr(t, func() {
		go func() {
			_, err := poller.next(func(r progressRetry) { retrying <- r })
			done <- err
		}()
		retry := awaitSignal(t, retrying, "the watch to start waiting out the 503")
		assert.Equal(t, 30*time.Second, retry.wait)
		cancel()
		requireWatchStopped(t, awaitSignal(t, done, "the cancelled watch to end"))
	})

	assert.Contains(t, stderr, progressStoppedMessage(cancelApplyID))
}

// An operator presses Ctrl-C while a progress fetch is still waiting on the
// server. The fetch is abandoned at once, and the watch reports that the
// operator stopped it instead of retrying the cut-off fetch as a transient
// failure.
func TestProgressPoller_CancelDuringFetchStopsWithoutRetry(t *testing.T) {
	srv, inFlight := blockingProgressServer(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	poller := newProgressPoller(ctx, srv.URL, cancelApplyID)

	var retries atomic.Int32
	done := make(chan error, 1)
	stderr := captureStderr(t, func() {
		go func() {
			_, err := poller.next(func(progressRetry) { retries.Add(1) })
			done <- err
		}()
		awaitSignal(t, inFlight, "the progress fetch to reach the server")
		cancel()
		requireWatchStopped(t, awaitSignal(t, done, "the cancelled fetch to end the watch"))
	})

	assert.Contains(t, stderr, progressStoppedMessage(cancelApplyID))
	assert.Zero(t, retries.Load(), "a cancelled fetch must not be retried")
}

// An operator runs apply -o log, and presses Ctrl-C once the watch is polling
// progress. The single Ctrl-C ends the command with the notice that the apply
// continues and how to watch it again, and the command exits non-zero.
func TestApplyCmd_LogWatchStopsOnFirstCancel(t *testing.T) {
	plan := planWithTablesAndEngine("mysql", createUsers())
	plan.PlanID = "plan-7c2e"
	planBody, err := json.Marshal(plan)
	require.NoError(t, err)
	api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/status":
			assert.NoError(t, json.NewEncoder(w).Encode(apitypes.StatusResponse{}))
		case "/api/plan":
			writeTestJSON(t, w, planBody)
		case "/api/apply":
			writeTestJSON(t, w, []byte(`{"accepted":true,"apply_id":"`+cancelApplyID+`"}`))
		default:
			assert.Fail(t, "unexpected request", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	srv, inFlight := blockingProgressServer(t, api)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cmd := ApplyCmd{
		SchemaDir:   writeTestSchemaDir(t),
		Environment: "staging",
		AutoApprove: true,
		NoLock:      true,
		Watch:       true,
		Output:      OutputFormatLog,
	}

	done := make(chan error, 1)
	var stdout string
	stderr := captureStderr(t, func() {
		stdout = captureStdout(func() {
			go func() { done <- cmd.Run(ctx, &Globals{Endpoint: srv.URL}) }()
			awaitSignal(t, inFlight, "the log watch to poll progress")
			cancel()
			requireWatchStopped(t, awaitSignal(t, done, "the cancelled apply watch to end"))
		})
	})

	assert.Contains(t, stdout, "Apply started: "+cancelApplyID)
	assert.Contains(t, stderr, "Stopped watching apply apply-7c2e; stopping the watch does not affect the apply; "+
		"rerun the original watch command, or 'schemabot progress apply-7c2e', to see its current state\n")
}
