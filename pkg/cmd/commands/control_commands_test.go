package commands

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
)

const controlTestApplyID = "apply-0f1e2d3c4b5a6978"

// controlTestStub describes the server a control command talks to: the
// schema change's state, and how the control request on controlPath is
// answered. A zero status answers 200 OK.
type controlTestStub struct {
	applyState  string
	controlPath string
	controlBody any
	// controlStatus is the HTTP status of the answer to the control request.
	controlStatus int
	// progressStatus, when set, fails the progress lookup with that HTTP
	// status, the way the server answers an apply it does not know.
	progressStatus int
	// stateAfterControl, when set, is the state the progress lookup reports
	// once the control request has arrived, the way a driver settles it.
	stateAfterControl string
	// progressStatusAfterControl, when set, fails every progress lookup made
	// after the control request has arrived with that HTTP status.
	progressStatusAfterControl int
	// controlRequests are the control request outcomes the progress lookup
	// reports, and controlRequestsAfterControl the ones it reports once the
	// control request has arrived.
	controlRequests             []apitypes.ControlRequestOutcome
	controlRequestsAfterControl []apitypes.ControlRequestOutcome
	// progressDelayAfterControl, when set, holds every progress lookup made
	// after the control request has arrived for that long before answering.
	progressDelayAfterControl time.Duration
}

// controlTestServer serves a schema change in the given state and answers the
// control request on controlPath with the given body, recording every request
// it receives on that path. A cancel it accepts ends the schema change as
// cancelled, the way a driver settles one.
func controlTestServer(t *testing.T, applyState, controlPath string, controlBody any) (*httptest.Server, *[]apitypes.ControlRequest) {
	t.Helper()
	stub := controlTestStub{applyState: applyState, controlPath: controlPath, controlBody: controlBody}
	if cancel, ok := controlBody.(apitypes.CancelResponse); ok && cancel.Accepted {
		stub.stateAfterControl = state.Apply.Cancelled
	}
	return serveControlTestStub(t, stub)
}

// serveControlTestStub serves stub, recording every control request it
// receives on stub.controlPath.
func serveControlTestStub(t *testing.T, stub controlTestStub) (*httptest.Server, *[]apitypes.ControlRequest) {
	t.Helper()
	var requests []apitypes.ControlRequest
	var mu sync.Mutex
	currentState := stub.applyState
	progressStatus := stub.progressStatus
	controlRequests := stub.controlRequests
	var progressDelay time.Duration
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		delay := progressDelay
		mu.Unlock()
		if delay > 0 && r.Method == http.MethodGet {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/progress/apply/"+controlTestApplyID:
			if status := progressStatus; status != 0 {
				w.WriteHeader(status)
				assert.NoError(t, json.NewEncoder(w).Encode(apitypes.ErrorResponse{Error: "apply not found: " + controlTestApplyID}))
				return
			}
			assert.NoError(t, json.NewEncoder(w).Encode(apitypes.ProgressResponse{
				State:           currentState,
				Database:        "orders",
				ApplyID:         controlTestApplyID,
				ControlRequests: controlRequests,
			}))
		case r.Method == http.MethodPost && r.URL.Path == stub.controlPath:
			var req apitypes.ControlRequest
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			requests = append(requests, req)
			if stub.stateAfterControl != "" {
				currentState = stub.stateAfterControl
			}
			if stub.progressStatusAfterControl != 0 {
				progressStatus = stub.progressStatusAfterControl
			}
			if stub.controlRequestsAfterControl != nil {
				controlRequests = stub.controlRequestsAfterControl
			}
			progressDelay = stub.progressDelayAfterControl
			if stub.controlStatus != 0 {
				w.WriteHeader(stub.controlStatus)
			}
			assert.NoError(t, json.NewEncoder(w).Encode(stub.controlBody))
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	return server, &requests
}

func controlTestFlags() ControlFlags {
	return ControlFlags{ApplyID: controlTestApplyID, Environment: "staging"}
}

// runControlCmd runs a control command against endpoint and returns its
// output with the terminal colors removed.
func runControlCmd(endpoint string, run func(*Globals) error) (string, error) {
	var runErr error
	output := captureStdout(func() {
		runErr = run(&Globals{Endpoint: endpoint})
	})
	return stripAnsi(output), runErr
}

// assertSentOnce checks that the command sent exactly one control request,
// that it named the schema change and environment the operator asked for, and
// that it carried the CLI identity the server records as the requester.
func assertSentOnce(t *testing.T, requests []apitypes.ControlRequest) {
	t.Helper()
	require.Len(t, requests, 1)
	assert.Equal(t, controlTestApplyID, requests[0].ApplyID)
	assert.Equal(t, "staging", requests[0].Environment)
	assert.Equal(t, client.GenerateCLIOwner(), requests[0].Caller)
}

// controlCmdCase is one control command, a state it acts on, and the first
// line of the success output it prints.
type controlCmdCase struct {
	applyState string
	path       string
	run        func(*Globals) error
	success    string
}

// controlCmdCases returns each control command run against a schema change in
// a state it acts on.
func controlCmdCases() map[string]controlCmdCase {
	return map[string]controlCmdCase{
		"cancel":  {state.Apply.Running, "/api/cancel", (&CancelCmd{ControlFlags: controlTestFlags(), AutoApprove: true}).Run, "Schema change cancelled"},
		"start":   {state.Apply.Stopped, "/api/start", (&StartCmd{ControlFlags: controlTestFlags()}).Run, "Schema change resumed"},
		"release": {state.Apply.Paused, "/api/release", (&ReleaseCmd{ControlFlags: controlTestFlags()}).Run, "Paused rollout released"},
	}
}

// An operator who cancels a schema change that is no longer running is told
// why there is nothing to cancel, and the CLI sends no cancel request.
func TestCancelCmdSendsNothingWhenThereIsNothingToCancel(t *testing.T) {
	cases := map[string]struct {
		applyState string
		want       string
	}{
		"no active schema change": {
			applyState: state.NoActiveChange,
			want:       "No active schema change for orders/staging (apply " + controlTestApplyID + ")\n",
		},
		"completed": {
			applyState: state.Apply.Completed,
			want:       "Schema change already terminal (state: completed) - nothing to cancel\n",
		},
		"failed": {
			applyState: state.Apply.Failed,
			want:       "Schema change already terminal (state: failed) - nothing to cancel\n",
		},
		"already cancelled": {
			applyState: state.Apply.Cancelled,
			want:       "Schema change already terminal (state: cancelled) - nothing to cancel\n",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			server, requests := controlTestServer(t, tc.applyState, "/api/cancel", apitypes.CancelResponse{Accepted: true})

			cmd := CancelCmd{ControlFlags: controlTestFlags(), AutoApprove: true}
			out, err := runControlCmd(server.URL, cmd.Run)
			require.NoError(t, err)
			assert.Equal(t, tc.want, out)
			assert.Empty(t, *requests, "a schema change with nothing to cancel must not be sent a cancel")
		})
	}
}

// Cancelling is permanent, so the CLI asks first. An operator who answers
// anything but "yes" leaves the schema change untouched; "yes" sends the cancel.
func TestCancelCmdAsksBeforeCancelling(t *testing.T) {
	t.Run("declined", func(t *testing.T) {
		server, requests := controlTestServer(t, state.Apply.Running, "/api/cancel", apitypes.CancelResponse{Accepted: true})
		answerPrompt(t, "no")

		cmd := CancelCmd{ControlFlags: controlTestFlags()}
		out, err := runControlCmd(server.URL, cmd.Run)
		require.NoError(t, err)
		assert.Contains(t, out, "About to permanently cancel the schema change for orders/staging (apply "+controlTestApplyID+") (state: running).")
		assert.Contains(t, out, "This cannot be resumed. Only 'yes' will be accepted: ")
		assert.Contains(t, out, "Cancel aborted.")
		assert.NotContains(t, out, "Schema change cancelled")
		assert.Empty(t, *requests, "a declined prompt must not send the cancel")
	})

	t.Run("confirmed", func(t *testing.T) {
		server, requests := controlTestServer(t, state.Apply.Running, "/api/cancel", apitypes.CancelResponse{Accepted: true, CancelledCount: 2})
		answerPrompt(t, "yes")

		cmd := CancelCmd{ControlFlags: controlTestFlags()}
		out, err := runControlCmd(server.URL, cmd.Run)
		require.NoError(t, err)
		assertSentOnce(t, *requests)
		assert.Contains(t, out, "✖ Schema change cancelled")
		assert.Contains(t, out, "Cancelled:   2 table(s)")
	})
}

// A stopped schema change can still be cancelled, and the success output
// reports how many tables were cancelled and how many had already finished.
func TestCancelCmdCancelsAStoppedSchemaChange(t *testing.T) {
	server, requests := controlTestServer(t, state.Apply.Stopped, "/api/cancel", apitypes.CancelResponse{
		Accepted:       true,
		CancelledCount: 2,
		SkippedCount:   1,
	})

	cmd := CancelCmd{ControlFlags: controlTestFlags(), AutoApprove: true}
	out, err := runControlCmd(server.URL, cmd.Run)
	require.NoError(t, err)
	assertSentOnce(t, *requests)
	assert.Equal(t, "✖ Schema change cancelled\n\n"+
		"Database:    orders\n"+
		"Environment: staging\n"+
		"Cancelled:   2 table(s)\n"+
		"Skipped:     1 table(s) (already terminal)\n\n"+
		"This schema change cannot be resumed.\n", out)
}

// The server records a cancel before a driver acts on it, and the schema
// change keeps running until then. A cancel that has not ended the schema
// change by the deadline says it is requested, not done, and names the
// command that watches it end, so an operator does not start a new apply
// that would be refused.
func TestCancelCmdReportsACancelThatHasNotTakenEffect(t *testing.T) {
	server, requests := serveControlTestStub(t, controlTestStub{
		applyState:  state.Apply.Running,
		controlPath: "/api/cancel",
		controlBody: apitypes.CancelResponse{Accepted: true, CancelledCount: 1},
	})

	cmd := CancelCmd{ControlFlags: controlTestFlags(), AutoApprove: true, settleTimeout: 30 * time.Millisecond, settlePollInterval: 10 * time.Millisecond}
	out, err := runControlCmd(server.URL, cmd.Run)
	require.NoError(t, err)
	assertSentOnce(t, *requests)
	assert.Equal(t, "⏳ Cancel requested\n\n"+
		"Database:    orders\n"+
		"Environment: staging\n"+
		"Cancelling:  1 table(s)\n\n"+
		"The cancel is recorded. The schema change is still running and ends when SchemaBot next picks it up.\n"+
		"A new apply is refused until it ends.\n\n"+
		"To watch it end:\n"+
		"  schemabot progress "+controlTestApplyID+" -e staging\n", out)
}

// A cancel whose wait cannot read the schema change's state still reports
// the cancel as requested, with the reason the state is unknown.
func TestCancelCmdReportsACancelWhoseStateCannotBeRead(t *testing.T) {
	server, requests := serveControlTestStub(t, controlTestStub{
		applyState:                 state.Apply.Running,
		controlPath:                "/api/cancel",
		controlBody:                apitypes.CancelResponse{Accepted: true, CancelledCount: 1},
		progressStatusAfterControl: http.StatusServiceUnavailable,
	})

	cmd := CancelCmd{ControlFlags: controlTestFlags(), AutoApprove: true, settleTimeout: 30 * time.Millisecond, settlePollInterval: 10 * time.Millisecond}
	out, err := runControlCmd(server.URL, cmd.Run)
	require.NoError(t, err)
	assertSentOnce(t, *requests)
	assert.Contains(t, out, "⏳ Cancel requested\n")
	assert.Contains(t, out, "The cancel is recorded, but its state could not be read: get progress for apply "+controlTestApplyID)
	assert.Contains(t, out, "schemabot progress "+controlTestApplyID+" -e staging")
	assert.NotContains(t, out, "Schema change cancelled")
}

// A schema change that finishes before the driver consumes the cancel ends in
// that state, not cancelled, and the output says so instead of claiming the
// cancel worked.
func TestCancelCmdReportsASchemaChangeThatEndedFirst(t *testing.T) {
	server, requests := serveControlTestStub(t, controlTestStub{
		applyState:        state.Apply.Running,
		controlPath:       "/api/cancel",
		controlBody:       apitypes.CancelResponse{Accepted: true, CancelledCount: 1},
		stateAfterControl: state.Apply.Completed,
	})

	cmd := CancelCmd{ControlFlags: controlTestFlags(), AutoApprove: true, settleTimeout: 30 * time.Millisecond, settlePollInterval: 10 * time.Millisecond}
	out, err := runControlCmd(server.URL, cmd.Run)
	require.NoError(t, err)
	assertSentOnce(t, *requests)
	assert.Equal(t, "Schema change ended as "+state.Apply.Completed+" before the cancel took effect\n\n"+
		"Database:    orders\n"+
		"Environment: staging\n\n"+
		"To see how it ended:\n"+
		"  schemabot progress "+controlTestApplyID+" -e staging\n", out)
}

// An engine can decline a cancel it cannot carry out, such as a PostgreSQL
// step that commits or fails on its own. The schema change runs on, so the
// output says the cancel did not take effect, gives the engine's reason, and
// names the command that watches the change finish, rather than claiming a
// cancel or leaving the operator waiting for one.
func TestCancelCmdReportsADeclinedCancel(t *testing.T) {
	const reason = "cancel is supported for PostgreSQL index builds only: this schema change runs as plain DDL that commits or fails on its own"
	server, requests := serveControlTestStub(t, controlTestStub{
		applyState:  state.Apply.Running,
		controlPath: "/api/cancel",
		controlBody: apitypes.CancelResponse{Accepted: true, CancelledCount: 1},
		controlRequestsAfterControl: []apitypes.ControlRequestOutcome{
			{Operation: "cancel", Status: "failed", ErrorMessage: reason},
		},
	})

	cmd := CancelCmd{ControlFlags: controlTestFlags(), AutoApprove: true, settleTimeout: time.Second, settlePollInterval: 10 * time.Millisecond}
	out, err := runControlCmd(server.URL, cmd.Run)
	require.NoError(t, err)
	assertSentOnce(t, *requests)
	assert.Equal(t, "⛔ Cancel did not take effect\n\n"+
		"Database:    orders\n"+
		"Environment: staging\n\n"+
		reason+"\n"+
		"The schema change keeps running and finishes on its own.\n\n"+
		"To watch it finish:\n"+
		"  schemabot progress "+controlTestApplyID+" -e staging\n", out)
}

// A cancel that failed before this one was sent is not this cancel's answer.
// Until the new request settles, the cancel reads as requested.
func TestCancelCmdIgnoresAnEarlierDeclinedCancel(t *testing.T) {
	earlier := apitypes.ControlRequestOutcome{Operation: "cancel", Status: "failed", Revision: 0, ErrorMessage: "an earlier decline"}
	server, requests := serveControlTestStub(t, controlTestStub{
		applyState:                  state.Apply.Running,
		controlPath:                 "/api/cancel",
		controlBody:                 apitypes.CancelResponse{Accepted: true, CancelledCount: 1},
		controlRequests:             []apitypes.ControlRequestOutcome{earlier},
		controlRequestsAfterControl: []apitypes.ControlRequestOutcome{earlier},
	})

	cmd := CancelCmd{ControlFlags: controlTestFlags(), AutoApprove: true, settleTimeout: 30 * time.Millisecond, settlePollInterval: 10 * time.Millisecond}
	out, err := runControlCmd(server.URL, cmd.Run)
	require.NoError(t, err)
	assertSentOnce(t, *requests)
	assert.Contains(t, out, "⏳ Cancel requested\n")
	assert.NotContains(t, out, "an earlier decline")
}

// An operator cancels a sharded apply after one shard has already failed. The
// cancel ends the shards still running and the apply ends failed, since
// failed outranks cancelled. The cancel did what was asked, so the output says
// it took effect and why the apply reads failed, rather than saying the
// schema change ended before the cancel could act.
func TestCancelCmdReportsACancelThatEndedTheRestOfAFailedRollout(t *testing.T) {
	accepted := int64(3)
	server, requests := serveControlTestStub(t, controlTestStub{
		applyState:        state.Apply.Running,
		controlPath:       "/api/cancel",
		controlBody:       apitypes.CancelResponse{Accepted: true, CancelledCount: 2, ControlRequestRevision: &accepted},
		stateAfterControl: state.Apply.Failed,
		controlRequestsAfterControl: []apitypes.ControlRequestOutcome{
			{Operation: "cancel", Status: "completed", Revision: accepted},
		},
	})

	cmd := CancelCmd{ControlFlags: controlTestFlags(), AutoApprove: true, settleTimeout: time.Second, settlePollInterval: 10 * time.Millisecond}
	out, err := runControlCmd(server.URL, cmd.Run)
	require.NoError(t, err)
	assertSentOnce(t, *requests)
	assert.Equal(t, "✖ Cancel took effect\n\n"+
		"Database:    orders\n"+
		"Environment: staging\n\n"+
		"The parts still running were cancelled. The schema change ended "+state.Apply.Failed+" because another part had already failed.\n"+
		"This schema change cannot be resumed.\n\n"+
		"To see what failed:\n"+
		"  schemabot progress "+controlTestApplyID+" -e staging\n", out)
}

// The apply's cancel request is re-armed for every later cancel. When another
// operator's cancel re-arms it after this one settled and the engine declines
// that one, the decline is not this cancel's answer, so this cancel reads as
// requested rather than declined.
func TestCancelCmdIgnoresALaterCancelsDecline(t *testing.T) {
	accepted := int64(1)
	server, requests := serveControlTestStub(t, controlTestStub{
		applyState:  state.Apply.Running,
		controlPath: "/api/cancel",
		controlBody: apitypes.CancelResponse{Accepted: true, CancelledCount: 1, ControlRequestRevision: &accepted},
		controlRequestsAfterControl: []apitypes.ControlRequestOutcome{
			{Operation: "cancel", Status: "failed", Revision: 2, ErrorMessage: "a later cancel's decline"},
		},
	})

	cmd := CancelCmd{ControlFlags: controlTestFlags(), AutoApprove: true, settleTimeout: 30 * time.Millisecond, settlePollInterval: 10 * time.Millisecond}
	out, err := runControlCmd(server.URL, cmd.Run)
	require.NoError(t, err)
	assertSentOnce(t, *requests)
	assert.Contains(t, out, "⏳ Cancel requested\n")
	assert.NotContains(t, out, "a later cancel's decline")
}

// A progress lookup that hangs does not hold the cancel past its settle
// timeout: one deadline bounds the whole wait, including a lookup in flight,
// and the cancel reads as requested when it passes.
func TestCancelCmdWaitIsBoundedBySettleTimeout(t *testing.T) {
	server, requests := serveControlTestStub(t, controlTestStub{
		applyState:                state.Apply.Running,
		controlPath:               "/api/cancel",
		controlBody:               apitypes.CancelResponse{Accepted: true, CancelledCount: 1},
		progressDelayAfterControl: 10 * time.Second,
	})

	cmd := CancelCmd{ControlFlags: controlTestFlags(), AutoApprove: true, settleTimeout: 50 * time.Millisecond, settlePollInterval: 10 * time.Millisecond}
	started := time.Now()
	out, err := runControlCmd(server.URL, cmd.Run)
	require.NoError(t, err)
	assert.Less(t, time.Since(started), 5*time.Second, "the settle timeout bounds a hanging progress lookup")
	assertSentOnce(t, *requests)
	assert.Contains(t, out, "⏳ Cancel requested\n")
	assert.Contains(t, out, "The cancel is recorded. The schema change is still running")
}

// When the server refuses a control request, the CLI returns the server's
// reason as an error instead of printing the success output.
func TestControlCmdsReturnARefusalAsAnError(t *testing.T) {
	const reason = "schema change is owned by another environment"
	cases := map[string]struct {
		applyState string
		path       string
		body       any
		run        func(*Globals) error
		wantErr    string
		success    string
	}{
		"cancel": {
			applyState: state.Apply.Running,
			path:       "/api/cancel",
			body:       apitypes.CancelResponse{ErrorMessage: reason},
			run:        (&CancelCmd{ControlFlags: controlTestFlags(), AutoApprove: true}).Run,
			wantErr:    "cancel not accepted: " + reason,
			success:    "Schema change cancelled",
		},
		"start": {
			applyState: state.Apply.Stopped,
			path:       "/api/start",
			body:       apitypes.StartResponse{ErrorMessage: reason},
			run:        (&StartCmd{ControlFlags: controlTestFlags()}).Run,
			wantErr:    "start not accepted: " + reason,
			success:    "Schema change resumed",
		},
		"release": {
			applyState: state.Apply.Paused,
			path:       "/api/release",
			body:       apitypes.ReleaseResponse{ErrorMessage: reason},
			run:        (&ReleaseCmd{ControlFlags: controlTestFlags()}).Run,
			wantErr:    "release not accepted: " + reason,
			success:    "Paused rollout released",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			server, requests := controlTestServer(t, tc.applyState, tc.path, tc.body)

			out, err := runControlCmd(server.URL, tc.run)
			require.Error(t, err)
			assert.Equal(t, tc.wantErr, err.Error())
			assert.Equal(t, 1, ExitCodeFor(err))
			assertSentOnce(t, *requests)
			assert.NotContains(t, out, tc.success)
		})
	}
}

// The server refuses most control requests an operator gets wrong (an unknown
// apply, a caller who may not act on it, one that is already finishing) with
// an HTTP error rather than an unaccepted answer. The CLI returns the server's
// reason as an error and exits non-zero, so a runbook never reads a refusal as
// done.
func TestControlCmdsReturnAnHTTPRejectionAsAnError(t *testing.T) {
	const reason = "schema change is already terminal"
	for name, tc := range controlCmdCases() {
		for _, status := range []int{http.StatusForbidden, http.StatusNotFound, http.StatusConflict} {
			t.Run(name+"/"+http.StatusText(status), func(t *testing.T) {
				server, requests := serveControlTestStub(t, controlTestStub{
					applyState:    tc.applyState,
					controlPath:   tc.path,
					controlBody:   apitypes.ErrorResponse{Error: reason},
					controlStatus: status,
				})

				out, err := runControlCmd(server.URL, tc.run)
				require.Error(t, err)
				assert.Contains(t, err.Error(), reason)
				assert.Equal(t, 1, ExitCodeFor(err))
				assertSentOnce(t, *requests)
				assert.NotContains(t, out, tc.success)
			})
		}
	}
}

// When the CLI cannot read the schema change it was asked to act on, it stops
// with the server's reason and sends no control request.
func TestControlCmdsStopWhenProgressCannotBeRead(t *testing.T) {
	for name, tc := range controlCmdCases() {
		t.Run(name, func(t *testing.T) {
			server, requests := serveControlTestStub(t, controlTestStub{
				controlPath:    tc.path,
				controlBody:    map[string]any{"accepted": true},
				progressStatus: http.StatusNotFound,
			})

			out, err := runControlCmd(server.URL, tc.run)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "get progress")
			assert.Contains(t, err.Error(), "apply not found: "+controlTestApplyID)
			assert.Equal(t, 1, ExitCodeFor(err))
			assert.Empty(t, *requests, "a schema change the CLI cannot read must not be sent a control request")
			assert.NotContains(t, out, tc.success)
		})
	}
}

// A start on a schema change that is already complete or already running has
// no effect, so the CLI says so and sends no start request.
func TestStartCmdSendsNothingWhenThereIsNothingToStart(t *testing.T) {
	cases := map[string]struct {
		applyState string
		want       string
	}{
		"completed":    {applyState: state.Apply.Completed, want: "Schema change already complete - nothing to start\n"},
		"running":      {applyState: state.Apply.Running, want: "Schema change already running\n"},
		"catching up":  {applyState: state.Apply.CatchingUp, want: "Schema change already running\n"},
		"cutting over": {applyState: state.Apply.CuttingOver, want: "Schema change already running\n"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			server, requests := controlTestServer(t, tc.applyState, "/api/start", apitypes.StartResponse{Accepted: true})

			cmd := StartCmd{ControlFlags: controlTestFlags()}
			out, err := runControlCmd(server.URL, cmd.Run)
			require.NoError(t, err)
			assert.Equal(t, tc.want, out)
			assert.Empty(t, *requests, "a schema change with nothing to start must not be sent a start")
		})
	}
}

// Only a stopped schema change, or one waiting for its deploy, can be started.
// Any other state is an error, and no start request is sent.
func TestStartCmdRefusesAStateItCannotStart(t *testing.T) {
	for _, applyState := range []string{state.Apply.Cancelled, state.Apply.Failed, state.Apply.Paused} {
		t.Run(applyState, func(t *testing.T) {
			server, requests := controlTestServer(t, applyState, "/api/start", apitypes.StartResponse{Accepted: true})

			cmd := StartCmd{ControlFlags: controlTestFlags()}
			_, err := runControlCmd(server.URL, cmd.Run)
			require.Error(t, err)
			assert.Equal(t, "cannot start schema change in state: "+applyState, err.Error())
			assert.Empty(t, *requests)
		})
	}
}

// Starting a stopped schema change, or one waiting for its deploy, sends one
// start request and, without --watch, tells the operator how to follow it.
func TestStartCmdStartsAStartableSchemaChange(t *testing.T) {
	for _, applyState := range []string{state.Apply.Stopped, state.Apply.WaitingForDeploy} {
		t.Run(applyState, func(t *testing.T) {
			server, requests := controlTestServer(t, applyState, "/api/start", apitypes.StartResponse{Accepted: true, StartedCount: 1})

			cmd := StartCmd{ControlFlags: controlTestFlags()}
			out, err := runControlCmd(server.URL, cmd.Run)
			require.NoError(t, err)
			assertSentOnce(t, *requests)
			assert.Equal(t, "▶️  Schema change resumed\n\n"+
				"To watch and manage: schemabot progress "+controlTestApplyID+" -e staging\n", out)
		})
	}
}

// Release applies only to a rollout paused after a deployment failed. Any
// other state is an error, and no release request is sent.
func TestReleaseCmdRefusesARolloutThatIsNotPaused(t *testing.T) {
	for _, applyState := range []string{state.Apply.Running, state.Apply.Stopped, state.Apply.Completed} {
		t.Run(applyState, func(t *testing.T) {
			server, requests := controlTestServer(t, applyState, "/api/release", apitypes.ReleaseResponse{Accepted: true})

			cmd := ReleaseCmd{ControlFlags: controlTestFlags()}
			_, err := runControlCmd(server.URL, cmd.Run)
			require.Error(t, err)
			assert.Equal(t, "cannot release schema change in state: "+applyState+
				" (release only applies to a paused rollout)", err.Error())
			assert.Empty(t, *requests)
		})
	}
}

// Releasing a paused rollout sends one release request and tells the operator
// how to follow the deployments it was holding back.
func TestReleaseCmdReleasesAPausedRollout(t *testing.T) {
	server, requests := controlTestServer(t, state.Apply.Paused, "/api/release", apitypes.ReleaseResponse{Accepted: true})

	cmd := ReleaseCmd{ControlFlags: controlTestFlags()}
	out, err := runControlCmd(server.URL, cmd.Run)
	require.NoError(t, err)
	assertSentOnce(t, *requests)
	assert.Equal(t, "▶️  Paused rollout released\n\n"+
		"Database:    orders\n"+
		"Environment: staging\n\n"+
		"Held deployments will resume. Use 'schemabot progress "+controlTestApplyID+" -e staging' to follow them.\n", out)
}

// controlWatch is one watch a control command entered.
type controlWatch struct {
	endpoint            string
	applyID             string
	environment         string
	allowControlActions bool
}

// controlWatchMarker is the line the stand-in watch prints, so a test can see
// where in the command's output the watch began.
const controlWatchMarker = "[watching]\n"

// stubControlWatch stands in for the terminal watch that start and release
// follow by default. It records every watch entered and prints
// controlWatchMarker in place of the progress display.
func stubControlWatch(t *testing.T) *[]controlWatch {
	t.Helper()
	var watches []controlWatch
	previous := watchControlledApply
	watchControlledApply = func(endpoint, applyID, environment string, allowControlActions bool) error {
		watches = append(watches, controlWatch{endpoint, applyID, environment, allowControlActions})
		fmt.Print(controlWatchMarker)
		return nil
	}
	t.Cleanup(func() { watchControlledApply = previous })
	return &watches
}

// assertWatchedOnce checks that the command entered exactly one watch, on the
// schema change and environment the operator named, with control keys on.
func assertWatchedOnce(t *testing.T, endpoint string, watches []controlWatch) {
	t.Helper()
	assert.Equal(t, []controlWatch{{endpoint, controlTestApplyID, "staging", true}}, watches)
}

// Starting a stopped schema change with --watch, the default, sends one start
// request, says how many tables resumed and how many were already complete,
// and then follows the schema change.
func TestStartCmdWatchesTheSchemaChangeItStarts(t *testing.T) {
	watches := stubControlWatch(t)
	server, requests := controlTestServer(t, state.Apply.Stopped, "/api/start",
		apitypes.StartResponse{Accepted: true, StartedCount: 3, SkippedCount: 1})

	cmd := StartCmd{ControlFlags: controlTestFlags(), Watch: true}
	out, err := runControlCmd(server.URL, cmd.Run)
	require.NoError(t, err)
	assertSentOnce(t, *requests)
	assert.Equal(t, "▶️  Schema change resumed\n\n"+
		"Database:    orders\n"+
		"Environment: staging\n"+
		"Resumed:     3 table(s)\n"+
		"Skipped:     1 table(s) (already complete)\n\n"+
		"Resuming from checkpoint...\n\n"+
		controlWatchMarker, out)
	assertWatchedOnce(t, server.URL, *watches)
}

// A start with --watch on a schema change that is already running sends no
// start request and follows the schema change that is running.
func TestStartCmdWatchesASchemaChangeThatIsAlreadyRunning(t *testing.T) {
	watches := stubControlWatch(t)
	server, requests := controlTestServer(t, state.Apply.Running, "/api/start", apitypes.StartResponse{Accepted: true})

	cmd := StartCmd{ControlFlags: controlTestFlags(), Watch: true}
	out, err := runControlCmd(server.URL, cmd.Run)
	require.NoError(t, err)
	assert.Empty(t, *requests, "a schema change that is already running must not be sent a start")
	assert.Equal(t, "Schema change already running\n"+controlWatchMarker, out)
	assertWatchedOnce(t, server.URL, *watches)
}

// Releasing a paused rollout with --watch, the default, sends one release
// request and then follows the deployments it was holding back.
func TestReleaseCmdWatchesTheRolloutItReleases(t *testing.T) {
	watches := stubControlWatch(t)
	server, requests := controlTestServer(t, state.Apply.Paused, "/api/release", apitypes.ReleaseResponse{Accepted: true})

	cmd := ReleaseCmd{ControlFlags: controlTestFlags(), Watch: true}
	out, err := runControlCmd(server.URL, cmd.Run)
	require.NoError(t, err)
	assertSentOnce(t, *requests)
	assert.Equal(t, "▶️  Paused rollout released\n\n"+
		"Database:    orders\n"+
		"Environment: staging\n\n"+
		"Held deployments will resume. Use 'schemabot progress "+controlTestApplyID+" -e staging' to follow them.\n\n"+
		controlWatchMarker, out)
	assertWatchedOnce(t, server.URL, *watches)
}
