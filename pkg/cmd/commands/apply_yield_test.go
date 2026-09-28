package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
)

const yieldTestApplyID = "apply-yield"

// yieldTestServer serves one auto-approved apply of writeTestSchemaDir's
// schema: the lock is free and granted, the apply is accepted, and progress
// reports progressState for every read (or fails when progressState is empty).
// It counts the lock releases the CLI sends.
func yieldTestServer(t *testing.T, progressState string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	plan := planWithTablesAndEngine("mysql", createUsers())
	plan.PlanID = "plan-yield"
	planBody, err := json.Marshal(plan)
	require.NoError(t, err)

	var releases atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var body []byte
		switch r.Method + " " + r.URL.Path {
		case "POST /api/plan":
			body = planBody
		case "POST /api/locks/acquire":
			body = []byte(`{"lock":{"database":"testdb","database_type":"mysql"}}`)
		case "POST /api/apply":
			body = []byte(`{"accepted":true,"apply_id":"` + yieldTestApplyID + `"}`)
		case "GET /api/progress/apply/" + yieldTestApplyID:
			if progressState == "" {
				w.WriteHeader(http.StatusInternalServerError)
				body = []byte(`{"error":"storage unavailable"}`)
				break
			}
			body = []byte(`{"state":"` + progressState + `","apply_id":"` + yieldTestApplyID + `"}`)
		case "DELETE /api/locks":
			releases.Add(1)
			body = []byte(`{}`)
		default:
			// The active-apply preflight and the free-lock lookup both read a
			// miss as "nothing there".
			w.WriteHeader(http.StatusNotFound)
			body = []byte(`{"error":"not found"}`)
		}
		_, writeErr := w.Write(body)
		assert.NoError(t, writeErr)
	}))
	t.Cleanup(server.Close)
	return server, &releases
}

// runYieldApply runs `apply --yield -y` against server and returns its output.
func runYieldApply(t *testing.T, server *httptest.Server, watch bool) string {
	t.Helper()
	cmd := ApplyCmd{
		SchemaDir:   writeTestSchemaDir(t),
		Environment: "staging",
		AutoApprove: true,
		Yield:       true,
		Watch:       watch,
		Output:      OutputFormatLog,
	}
	var runErr error
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: server.URL}) }))
	require.NoError(t, runErr, out)
	return out
}

// An operator who runs `apply --yield --no-watch` gets their prompt back while
// the apply is still pending on the server. The lock stays held, so no other
// operator can take the database mid-apply, and the output says the lock was
// kept and how to release it once the apply has finished.
func TestApplyYield_NoWatchKeepsLockWhileApplyRuns(t *testing.T) {
	server, releases := yieldTestServer(t, state.Apply.Pending)

	out := runYieldApply(t, server, false)

	assert.Zero(t, releases.Load(), "an unfinished apply must keep its lock:\n%s", out)
	assert.Contains(t, out, "Lock kept for testdb (mysql) despite --yield: apply apply-yield has not finished (state: pending).")
	assert.Contains(t, out, "Release it once the apply has finished: schemabot unlock -d testdb -t mysql")
	assert.NotContains(t, out, "Lock released")
}

// An apply watched with --yield that ends stopped can still be resumed with
// `start`, so the lock that keeps other operators off the database stays held
// and the output says why.
func TestApplyYield_StoppedApplyKeepsLock(t *testing.T) {
	server, releases := yieldTestServer(t, state.Apply.Stopped)

	out := runYieldApply(t, server, true)

	assert.Contains(t, out, "Apply stopped")
	assert.Zero(t, releases.Load(), "a resumable apply must keep its lock:\n%s", out)
	assert.Contains(t, out, "Lock kept for testdb (mysql) despite --yield: apply apply-yield is stopped and can still be resumed.")
	assert.Contains(t, out, "schemabot unlock -d testdb -t mysql")
	assert.NotContains(t, out, "Lock released")
}

// An apply watched with --yield to completion releases the lock exactly once.
func TestApplyYield_CompletedApplyReleasesLock(t *testing.T) {
	server, releases := yieldTestServer(t, state.Apply.Completed)

	out := runYieldApply(t, server, true)

	assert.Contains(t, out, "Apply completed")
	assert.Equal(t, int32(1), releases.Load(), "a completed apply releases its lock:\n%s", out)
	assert.Contains(t, out, "Lock released for testdb (mysql)")
	assert.NotContains(t, out, "Lock kept")
}

// When the apply's state cannot be read back, --yield cannot tell whether the
// apply has finished, so it keeps the lock rather than guess.
func TestApplyYield_UnreadableStateKeepsLock(t *testing.T) {
	server, releases := yieldTestServer(t, "")
	cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "staging", AutoApprove: true, Yield: true, Output: OutputFormatLog}
	var runErr error
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: server.URL}) }))

	require.Error(t, runErr)
	assert.Zero(t, releases.Load(), "an unknown apply state must keep the lock:\n%s", out)
	assert.Contains(t, out, "Lock kept for testdb (mysql) despite --yield: the state of apply apply-yield could not be read")
	assert.Contains(t, out, "schemabot unlock -d testdb -t mysql")
}

func TestApplyYield_FailedWatchKeepsLockWithoutQuiescenceProof(t *testing.T) {
	server, releases := yieldTestServer(t, state.Apply.Failed)
	cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "staging", AutoApprove: true, Yield: true, Watch: true, Output: OutputFormatLog}
	var runErr error
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: server.URL}) }))

	require.ErrorIs(t, runErr, ErrSilent)
	assert.Zero(t, releases.Load(), "a failed parent without child operation states must keep its lock:\n%s", out)
	assert.Contains(t, out, "Lock kept for testdb (mysql) despite --yield")
}

func TestYieldLock_EmptyApplyIDReturnsError(t *testing.T) {
	var yieldErr error
	out := stripAnsi(captureStdout(func() { yieldErr = yieldLock("unused", "testdb", "mysql", "owner", "") }))

	require.EqualError(t, yieldErr, "release lock for testdb: the server returned no apply ID")
	assert.Contains(t, out, "the server returned no apply ID to check")
}

func TestLockKeptReason_NoActiveChange(t *testing.T) {
	assert.Equal(t, "the server reported no state for apply "+yieldTestApplyID,
		lockKeptReason(yieldTestApplyID, state.NoActiveChange))
}

// Only settled applies give the lock up. Every other state, including a
// stopped apply that is terminal but resumable, keeps it.
func TestShouldYieldLock(t *testing.T) {
	for _, s := range []string{state.Apply.Completed, state.Apply.Cancelled, state.Apply.Reverted} {
		assert.True(t, shouldYieldLock(&apitypes.ProgressResponse{State: s}), "settled state %s releases the lock", s)
		assert.True(t, shouldYieldLock(&apitypes.ProgressResponse{State: "STATE_" + s}), "proto spelling of %s releases the lock", s)
	}
	for _, s := range []string{
		state.Apply.Pending,
		state.Apply.Running,
		state.Apply.WaitingForCutover,
		state.Apply.CuttingOver,
		state.Apply.RevertWindow,
		state.Apply.FailedRetryable,
		state.Apply.Stopped,
		state.NoActiveChange,
		"",
	} {
		assert.False(t, shouldYieldLock(&apitypes.ProgressResponse{State: s}), "state %q keeps the lock", s)
	}
	assert.False(t, shouldYieldLock(&apitypes.ProgressResponse{State: state.Apply.Failed}), "a failed parent alone does not prove child operations are quiet")
	assert.False(t, shouldYieldLock(&apitypes.ProgressResponse{State: state.Apply.Failed, Operations: []*apitypes.ProgressOperationResponse{{State: state.Apply.Running}}}))
	assert.True(t, shouldYieldLock(&apitypes.ProgressResponse{State: state.Apply.Failed, Operations: []*apitypes.ProgressOperationResponse{{State: state.Apply.Failed}}}))
}
