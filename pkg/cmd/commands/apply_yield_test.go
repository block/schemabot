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
	if progressState == "" {
		return yieldTestServerWithProgress(t, "")
	}
	return yieldTestServerWithProgress(t, `{"state":"`+progressState+`","apply_id":"`+yieldTestApplyID+`"}`)
}

// yieldTestServerWithProgress is yieldTestServer with the progress body given
// verbatim, so a test can shape the operation and table rows under the apply.
// An empty body makes every progress read fail.
func yieldTestServerWithProgress(t *testing.T, progressJSON string) (*httptest.Server, *atomic.Int32) {
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
			body = []byte(acquireFoundLock)
		case "POST /api/apply":
			body = []byte(`{"accepted":true,"apply_id":"` + yieldTestApplyID + `"}`)
		case "GET /api/progress/apply/" + yieldTestApplyID:
			if progressJSON == "" {
				w.WriteHeader(http.StatusInternalServerError)
				body = []byte(`{"error":"storage unavailable"}`)
				break
			}
			body = []byte(progressJSON)
		case "DELETE /api/locks":
			releases.Add(1)
			var sent map[string]any
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&sent), "decode the lock release request")
			assert.Equal(t, float64(acquiredLockRowID), sent["id"], "the release names the lock row the acquire reported")
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
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL}) }))
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
	assert.Contains(t, out, "Release it once nothing is left to run: schemabot unlock -d testdb -t mysql")
	assert.NotContains(t, out, "Lock released")
}

// An apply watched with --yield that ends stopped can still be resumed with
// `start`, so the lock that keeps other operators off the database stays held
// and the output says why. The schema change is not on the target, so the
// command still fails and names the command that resumes it.
func TestApplyYield_StoppedApplyKeepsLock(t *testing.T) {
	server, releases := yieldTestServer(t, state.Apply.Stopped)
	cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "staging", AutoApprove: true, Yield: true, Watch: true, Output: OutputFormatLog}
	var runErr error
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL}) }))

	require.ErrorContains(t, runErr, "apply apply-yield was stopped, so the schema change is not on the target")
	assert.ErrorContains(t, runErr, "start apply-yield -e")
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
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL}) }))

	require.Error(t, runErr)
	assert.Zero(t, releases.Load(), "an unknown apply state must keep the lock:\n%s", out)
	assert.Contains(t, out, "Lock kept for testdb (mysql) despite --yield: the state of apply apply-yield could not be read")
	assert.Contains(t, out, "schemabot unlock -d testdb -t mysql")
}

func TestApplyYield_FailedWatchKeepsLockWithoutQuiescenceProof(t *testing.T) {
	server, releases := yieldTestServer(t, state.Apply.Failed)
	cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "staging", AutoApprove: true, Yield: true, Watch: true, Output: OutputFormatLog}
	var runErr error
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL}) }))

	require.ErrorIs(t, runErr, ErrSilent)
	assert.Zero(t, releases.Load(), "a failed parent without child operation states must keep its lock:\n%s", out)
	assert.Contains(t, out, "Lock kept for testdb (mysql) despite --yield: apply apply-yield is failed and the server reported no deployment rows to check (see schemabot progress apply-yield -e staging).")
}

// A rollout projects cancelled as soon as one deployment's operation is
// cancelled, while a sibling deployment's operation may still be copying. The
// unwatched --yield reads that back and keeps the lock, naming what to check.
func TestApplyYield_CancelledRolloutWithRunningSiblingKeepsLock(t *testing.T) {
	server, releases := yieldTestServerWithProgress(t, `{"state":"cancelled","apply_id":"`+yieldTestApplyID+`",`+
		`"operations":[{"deployment":"eu","state":"cancelled"},{"deployment":"us","state":"running"}]}`)

	out := runYieldApply(t, server, false)

	assert.Zero(t, releases.Load(), "a settled parent with a running operation must keep its lock:\n%s", out)
	assert.Contains(t, out, "Lock kept for testdb (mysql) despite --yield: apply apply-yield is cancelled, but not every deployment and table under it has stopped yet (see schemabot progress apply-yield -e staging).")
	assert.Contains(t, out, "schemabot unlock -d testdb -t mysql")
	assert.NotContains(t, out, "Lock released")
}

// A settled apply whose lock release the server refuses returns an error, does
// not claim the lock was released, and still names the unlock command: this is
// the one kept-lock case where the operator has nothing left to wait for.
func TestYieldLock_ReleaseFailureKeepsLockAndReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var body []byte
		switch r.Method + " " + r.URL.Path {
		case "GET /api/progress/apply/" + yieldTestApplyID:
			body = []byte(`{"state":"completed","apply_id":"` + yieldTestApplyID + `"}`)
		default:
			w.WriteHeader(http.StatusInternalServerError)
			body = []byte(`{"error":"storage unavailable"}`)
		}
		_, err := w.Write(body)
		assert.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	var yieldErr error
	out := stripAnsi(captureStdout(func() {
		yieldErr = yieldLock(server.URL, "testdb", "mysql", "owner", "staging", yieldTestApplyID, acquiredLockRowID)
	}))

	require.ErrorContains(t, yieldErr, "release lock for testdb")
	assert.Contains(t, out, "Lock kept for testdb (mysql) despite --yield: the server did not release it (")
	assert.Contains(t, out, "schemabot unlock -d testdb -t mysql")
	assert.NotContains(t, out, "Lock released")
}

func TestYieldLock_EmptyApplyIDReturnsError(t *testing.T) {
	var yieldErr error
	out := stripAnsi(captureStdout(func() { yieldErr = yieldLock("unused", "testdb", "mysql", "owner", "staging", "", acquiredLockRowID) }))

	require.EqualError(t, yieldErr, "release lock for testdb: the server returned no apply ID")
	assert.Contains(t, out, "the server returned no apply ID to check")
}

// settledYieldServer answers a completed apply's progress and a lock release
// with releaseStatus and releaseBody, and counts the releases it is sent.
func settledYieldServer(t *testing.T, releaseStatus int, releaseBody string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var releases atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "GET /api/progress/apply/" + yieldTestApplyID:
			_, err := w.Write([]byte(`{"state":"completed","apply_id":"` + yieldTestApplyID + `"}`))
			assert.NoError(t, err)
		case "DELETE /api/locks":
			releases.Add(1)
			w.WriteHeader(releaseStatus)
			_, err := w.Write([]byte(releaseBody))
			assert.NoError(t, err)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server, &releases
}

// runSettledYield yields lockID after a completed apply against server and
// returns the output and error.
func runSettledYield(server *httptest.Server, lockID int64) (string, error) {
	var yieldErr error
	out := stripAnsi(captureStdout(func() {
		yieldErr = yieldLock(server.URL, "testdb", "mysql", "owner", "staging", yieldTestApplyID, lockID)
	}))
	return out, yieldErr
}

// This apply has completed, but another command of the same owner found the
// same lock and its apply still runs under it. --yield keeps the lock for that
// apply and names it, rather than taking the lock out from under it.
func TestYieldLock_KeepsALockAnotherCommandsApplyRunsUnder(t *testing.T) {
	server, releases := settledYieldServer(t, http.StatusConflict, lockInUseAnswer)

	out, err := runSettledYield(server, acquiredLockRowID)

	require.NoError(t, err)
	assert.Equal(t, int32(1), releases.Load(), out)
	assert.Contains(t, out, "🔒 Lock kept for testdb (mysql): apply apply-b2c3d4e5 (running) runs under it.\n"+
		"   Watch that apply: schemabot progress apply-b2c3d4e5 -e staging\n")
	assert.NotContains(t, out, "Lock released")
}

// The lock this apply took was released and taken again since. --yield leaves
// the lock held now in place and says this command released nothing.
func TestYieldLock_LeavesALockTakenAgainSinceInPlace(t *testing.T) {
	server, releases := settledYieldServer(t, http.StatusConflict, lockReplacedAnswer)

	out, err := runSettledYield(server, acquiredLockRowID)

	require.NoError(t, err)
	assert.Equal(t, int32(1), releases.Load(), out)
	assert.Contains(t, out, "🔒 Lock for testdb (mysql) was released and taken again since this command took it, so it was left in place.\n"+
		"   This command released nothing. See who holds it: schemabot locks\n")
	assert.NotContains(t, out, "Lock released")
}

// The apply completed, but its lock is already gone or now held by another
// owner. --yield has nothing to release, says so, and does not fail the
// command or offer an unlock.
func TestYieldLock_NothingToReleaseIsNotAFailure(t *testing.T) {
	cases := map[string]struct {
		status int
		body   string
		want   string
	}{
		"lock already released": {
			status: http.StatusNotFound,
			body:   `{"error":"lock not found","error_code":"not_found"}`,
			want:   "🔓 Lock for testdb (mysql) was already released, so nothing is left to unlock.\n",
		},
		"lock held by another owner": {
			status: http.StatusForbidden,
			body:   `{"error":"lock is not owned by you","error_code":"lock_not_owned"}`,
			want: "🔒 Lock for testdb (mysql) is now held by another owner, so it was left in place.\n" +
				"   This command released nothing. See who holds it: schemabot locks\n",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			server, releases := settledYieldServer(t, tc.status, tc.body)

			out, err := runSettledYield(server, acquiredLockRowID)

			require.NoError(t, err, out)
			assert.Equal(t, int32(1), releases.Load(), out)
			assert.Contains(t, out, tc.want)
			assert.NotContains(t, out, "schemabot unlock")
		})
	}
}

// A server too old to report the lock row leaves --yield no release that
// spares another command's apply, so it keeps the lock, sends no release, and
// shows how to check and release it.
func TestYieldLock_KeepsTheLockWhenTheServerReportsNoLockID(t *testing.T) {
	server, releases := settledYieldServer(t, http.StatusOK, `{"status":"released"}`)

	out, err := runSettledYield(server, 0)

	require.NoError(t, err)
	assert.Zero(t, releases.Load(), out)
	assert.Contains(t, out, "🔒 Lock kept for testdb (mysql): the server did not say which lock this command took, so releasing it could remove a lock another command uses.\n"+
		"   See who holds it: schemabot locks\n"+
		"   If no apply of yours runs under it, release it: schemabot unlock -d testdb -t mysql\n")
}

func TestLockKeptReason_NoActiveChange(t *testing.T) {
	assert.Equal(t, "the server reported no state for apply "+yieldTestApplyID,
		lockKeptReason(yieldTestApplyID, "staging", &apitypes.ProgressResponse{State: state.NoActiveChange}))
}

// A settled parent that shouldYieldLock still rejects is held back by a row
// under it, and the reason says so rather than asking the operator to wait for
// an apply that has already finished. The proto spelling normalizes in the
// message too.
func TestLockKeptReason_SettledParentHeldByChildren(t *testing.T) {
	progress := &apitypes.ProgressResponse{
		State:      "STATE_FAILED",
		Operations: []*apitypes.ProgressOperationResponse{{State: state.Apply.Failed}},
		Tables:     []*apitypes.TableProgressResponse{{TableName: "users", Status: state.Task.Running}},
	}
	assert.Equal(t,
		"apply apply-yield is failed, but not every deployment and table under it has stopped yet (see schemabot progress apply-yield -e production)",
		lockKeptReason(yieldTestApplyID, "production", progress))
	assert.Equal(t, "apply apply-yield has not finished (state: running)",
		lockKeptReason(yieldTestApplyID, "production", &apitypes.ProgressResponse{State: state.Apply.Running}))
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

// A settled parent is not proof that its work stopped: a rollout projects
// cancelled as soon as one deployment's operation is cancelled, one failed
// task fails its operation while a sibling task under it keeps copying, and a
// stopped operation can be started again. --yield keeps the lock in each case
// and releases once every operation and table row has settled too.
func TestShouldYieldLock_ChildRowsDecideForASettledParent(t *testing.T) {
	keeps := map[string]*apitypes.ProgressResponse{
		"cancelled parent, sibling operation still running": {
			State: state.Apply.Cancelled,
			Operations: []*apitypes.ProgressOperationResponse{
				{State: state.Apply.Cancelled}, {State: state.Apply.Running},
			},
		},
		"reverted parent, table still reverting": {
			State:  state.Apply.Reverted,
			Tables: []*apitypes.TableProgressResponse{{TableName: "orders", Status: state.Task.Reverting}},
		},
		"completed parent, table still cutting over": {
			State:  state.Apply.Completed,
			Tables: []*apitypes.TableProgressResponse{{TableName: "orders", Status: state.Task.CuttingOver}},
		},
		"failed operation, sibling table still copying": {
			State:      state.Apply.Failed,
			Operations: []*apitypes.ProgressOperationResponse{{State: state.Apply.Failed}},
			Tables: []*apitypes.TableProgressResponse{
				{TableName: "orders", Status: state.Task.Failed},
				{TableName: "users", Status: state.Task.Running},
			},
		},
		"failed parent, sibling operation stopped": {
			State: state.Apply.Failed,
			Operations: []*apitypes.ProgressOperationResponse{
				{State: state.Apply.Failed}, {State: state.Apply.Stopped},
			},
		},
		"failed parent, table stopped": {
			State:      state.Apply.Failed,
			Operations: []*apitypes.ProgressOperationResponse{{State: state.Apply.Failed}},
			Tables:     []*apitypes.TableProgressResponse{{TableName: "orders", Status: state.Task.Stopped}},
		},
	}
	for name, progress := range keeps {
		assert.False(t, shouldYieldLock(progress), name)
	}

	releases := map[string]*apitypes.ProgressResponse{
		"completed parent, every row completed": {
			State:      state.Apply.Completed,
			Operations: []*apitypes.ProgressOperationResponse{{State: "STATE_COMPLETED"}},
			Tables: []*apitypes.TableProgressResponse{
				{TableName: "orders", Status: state.Task.Completed}, {TableName: "users", Status: state.Task.Completed},
			},
		},
		"failed parent, failed operation, one table failed and one completed": {
			State:      state.Apply.Failed,
			Operations: []*apitypes.ProgressOperationResponse{{State: state.Apply.Failed}},
			Tables: []*apitypes.TableProgressResponse{
				{TableName: "orders", Status: state.Task.Failed}, {TableName: "users", Status: state.Task.Completed},
			},
		},
		"cancelled parent, every operation cancelled": {
			State: state.Apply.Cancelled,
			Operations: []*apitypes.ProgressOperationResponse{
				{State: state.Apply.Cancelled}, {State: state.Apply.Cancelled},
			},
		},
	}
	for name, progress := range releases {
		assert.True(t, shouldYieldLock(progress), name)
	}
}
