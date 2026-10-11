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
// it: a silent error carrying the cancellation, so the CLI exits with the
// interrupt status without a raw "context canceled" line under the
// stopped-watching notice.
func requireWatchStopped(t *testing.T, err error) {
	t.Helper()
	require.ErrorIs(t, err, ErrSilent)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, ExitInterrupted, ExitCodeFor(err))
}

// holdingHandler answers every request by holding it open until the client
// gives up on it, and reports each arrival on inFlight. Requests are released
// when the test ends, so a client that never cancels cannot hang the server's
// shutdown.
type holdingHandler struct {
	inFlight chan struct{}
	release  chan struct{}
}

func newHoldingHandler(t *testing.T) *holdingHandler {
	t.Helper()
	h := &holdingHandler{inFlight: make(chan struct{}, 1), release: make(chan struct{})}
	t.Cleanup(func() { close(h.release) })
	return h
}

func (h *holdingHandler) ServeHTTP(_ http.ResponseWriter, r *http.Request) {
	select {
	case h.inFlight <- struct{}{}:
	default:
	}
	select {
	case <-r.Context().Done():
	case <-h.release:
	}
}

// blockingProgressServer holds every progress fetch for cancelApplyID open
// until the client gives up on it, hands every other request to next, and
// reports each held fetch on the returned channel.
func blockingProgressServer(t *testing.T, next http.Handler) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	hold := newHoldingHandler(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if next != nil && r.URL.Path != "/api/progress/apply/"+cancelApplyID {
			next.ServeHTTP(w, r)
			return
		}
		hold.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, hold.inFlight
}

const cancelApplyID = "apply-7c2e"

// applyAPI serves the requests an apply makes up to and including submission:
// status, the plan, and the apply itself, which answers with cancelApplyID.
// onSubmit is called for each apply request before it is answered.
func applyAPI(t *testing.T, onSubmit func()) http.Handler {
	t.Helper()
	plan := planWithTablesAndEngine("mysql", createUsers())
	plan.PlanID = "plan-7c2e"
	planBody, err := json.Marshal(plan)
	require.NoError(t, err)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/status":
			assert.NoError(t, json.NewEncoder(w).Encode(apitypes.StatusResponse{}))
		case "/api/plan":
			writeTestJSON(t, w, planBody)
		case "/api/apply":
			onSubmit()
			writeTestJSON(t, w, []byte(`{"accepted":true,"apply_id":"`+cancelApplyID+`"}`))
		default:
			assert.Fail(t, "unexpected request", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})
}

// watchingApplyCmd is an apply that submits without prompting or locking and
// then watches in the given format, the shape a script runs it in.
func watchingApplyCmd(t *testing.T, format OutputFormat) ApplyCmd {
	t.Helper()
	return ApplyCmd{
		SchemaDir:   writeTestSchemaDir(t),
		Environment: "staging",
		AutoApprove: true,
		NoLock:      true,
		Watch:       true,
		Output:      format,
	}
}

// runApplyUntilWatchCancelled runs cmd against srv, cancels it once its watch
// is polling progress, and returns what the command wrote to each stream
// once it has ended as a stopped watch.
func runApplyUntilWatchCancelled(t *testing.T, cmd *ApplyCmd, srv *httptest.Server, inFlight <-chan struct{}) (stdout, stderr string) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	stderr = captureStderr(t, func() {
		stdout = captureStdout(func() {
			go func() { done <- cmd.Run(ctx, &Globals{Endpoint: srv.URL}) }()
			awaitSignal(t, inFlight, "the watch to poll progress")
			cancel()
			requireWatchStopped(t, awaitSignal(t, done, "the cancelled apply watch to end"))
		})
	})
	return stdout, stderr
}

const stoppedWatchingNotice = "Stopped watching apply apply-7c2e; stopping the watch does not affect the apply; " +
	"watch it again with 'schemabot progress apply-7c2e'\n"

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

// A progress fetch answers in the same instant the operator presses Ctrl-C.
// The frame is returned rather than dropped: it may be the apply's final
// state, and the watch reports the stop on its next wait instead.
func TestProgressPoller_FetchAnsweredBeforeCancelIsReturned(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	poller := &progressPoller{
		ctx:     ctx,
		applyID: cancelApplyID,
		fetch: func() (*apitypes.ProgressResponse, error) {
			cancel()
			return &apitypes.ProgressResponse{ApplyID: cancelApplyID, State: "completed"}, nil
		},
		sleep: func(time.Duration) {},
	}

	var result *apitypes.ProgressResponse
	var err error
	stderr := captureStderr(t, func() {
		result, err = poller.next(func(progressRetry) { assert.Fail(t, "an answered fetch must not be retried") })
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "completed", result.State)
	assert.Empty(t, stderr, "the stop is reported by the next wait, not by an answered fetch")
	requireWatchStopped(t, poller.pause(0))
}

// An operator runs apply -o log, and presses Ctrl-C once the watch is polling
// progress. The single Ctrl-C ends the command with the notice that the apply
// continues and how to watch it again, and the command exits with the
// interrupt status.
func TestApplyCmd_LogWatchStopsOnFirstCancel(t *testing.T) {
	srv, inFlight := blockingProgressServer(t, applyAPI(t, func() {}))
	cmd := watchingApplyCmd(t, OutputFormatLog)

	stdout, stderr := runApplyUntilWatchCancelled(t, &cmd, srv, inFlight)

	assert.Contains(t, stdout, "Apply started: "+cancelApplyID)
	assert.Contains(t, stderr, stoppedWatchingNotice)
}

// An operator runs apply -o json, and presses Ctrl-C once the watch is
// polling progress. The watch ends the same way as the log watch, and the
// notice goes to stderr so the JSON stream on stdout stays machine-readable.
func TestApplyCmd_JSONWatchStopsOnFirstCancel(t *testing.T) {
	srv, inFlight := blockingProgressServer(t, applyAPI(t, func() {}))
	cmd := watchingApplyCmd(t, OutputFormatJSON)

	stdout, stderr := runApplyUntilWatchCancelled(t, &cmd, srv, inFlight)

	assert.Contains(t, stdout, "Apply started: "+cancelApplyID)
	assert.NotContains(t, stdout, "Stopped watching")
	assert.Contains(t, stderr, stoppedWatchingNotice)
}

// An operator runs cutover, which watches the apply once the cutover is
// requested, and presses Ctrl-C while that watch is polling. The one Ctrl-C
// ends the watch with the same notice and interrupt status as an apply watch;
// the cutover the server accepted carries on.
func TestCutoverCmd_WatchStopsOnFirstCancel(t *testing.T) {
	hold := newHoldingHandler(t)
	var progressFetches atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/cutover":
			writeTestJSON(t, w, []byte(`{"accepted":true}`))
		case "/api/progress/apply/" + cancelApplyID:
			// The command reads the apply's state once before requesting the
			// cutover; the watch's polls after that are held.
			if progressFetches.Add(1) == 1 {
				writeTestJSON(t, w, []byte(`{"apply_id":"`+cancelApplyID+`","state":"running"}`))
				return
			}
			hold.ServeHTTP(w, r)
		default:
			assert.Fail(t, "unexpected request", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cmd := CutoverCmd{ControlFlags: ControlFlags{ApplyID: cancelApplyID, Environment: "staging"}, Watch: true}

	done := make(chan error, 1)
	var stdout string
	stderr := captureStderr(t, func() {
		stdout = captureStdout(func() {
			go func() { done <- cmd.Run(ctx, &Globals{Endpoint: srv.URL}) }()
			awaitSignal(t, hold.inFlight, "the cutover watch to poll progress")
			cancel()
			requireWatchStopped(t, awaitSignal(t, done, "the cancelled cutover watch to end"))
		})
	})

	assert.Contains(t, stdout, "Cutover requested successfully.")
	assert.Contains(t, stderr, stoppedWatchingNotice)
}

// An operator presses Ctrl-C while apply is still planning, before anything
// has been submitted. The command ends there and says that nothing was
// started, instead of submitting the apply and reporting its watch as
// stopped, and it exits with the interrupt status.
func TestApplyCmd_CancelBeforeSubmitStartsNothing(t *testing.T) {
	srv := httptest.NewServer(applyAPI(t, func() {
		assert.Fail(t, "a cancelled apply must not be submitted")
	}))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	cmd := watchingApplyCmd(t, OutputFormatLog)

	var err error
	var stdout string
	stderr := captureStderr(t, func() {
		stdout = captureStdout(func() {
			err = cmd.Run(ctx, &Globals{Endpoint: srv.URL})
		})
	})

	requireWatchStopped(t, err)
	assert.Equal(t, "Stopped before the apply was submitted; nothing was started.\n", stderr)
	assert.NotContains(t, stdout, "Apply started")
}
