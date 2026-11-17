package commands

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

// unsubmittedLockServer serves one auto-approved apply or rollback of the
// "testdb" mysql database up to its submit. The lock is free; the acquire
// answers acquireBody, after calling onAcquire when it is set; the submit
// answers applyStatus with applyBody; and a lock release answers
// releaseStatus. It counts the releases the CLI sends.
type unsubmittedLockServer struct {
	acquireBody   string
	onAcquire     func()
	applyStatus   int
	applyBody     string
	releaseStatus int
}

func (s unsubmittedLockServer) start(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	plan := planWithTablesAndEngine("mysql", createUsers())
	plan.PlanID = "plan-unsubmitted"
	planBody, err := json.Marshal(plan)
	require.NoError(t, err)
	rollbackPlan := apitypes.PlanResponse{PlanID: "plan-rollback-unsubmitted", Database: "testdb", DatabaseType: "mysql", Environment: "staging", Changes: []*apitypes.SchemaChangeResponse{{Namespace: "testdb", TableChanges: []*apitypes.TableChangeResponse{{TableName: "users", ChangeType: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `nickname` varchar(64)"}}}}}
	rollbackBody, err := json.Marshal(rollbackPlan)
	require.NoError(t, err)

	var releases atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		status := http.StatusOK
		var body []byte
		switch r.Method + " " + r.URL.Path {
		case "POST /api/plan":
			body = planBody
		case "POST /api/rollback/plan":
			body = rollbackBody
		case "GET /api/status":
			body = []byte(`{}`)
		case "POST /api/locks/acquire":
			if s.onAcquire != nil {
				s.onAcquire()
			}
			body = []byte(s.acquireBody)
		case "POST /api/apply":
			status = s.applyStatus
			body = []byte(s.applyBody)
		case "DELETE /api/locks":
			releases.Add(1)
			status = s.releaseStatus
			body = []byte(`{}`)
			if status != http.StatusOK {
				body = []byte(`{"error":"storage unavailable"}`)
			}
		default:
			// The active-apply preflight and the free-lock lookup both read a
			// miss as "nothing there".
			status = http.StatusNotFound
			body = []byte(`{"error":"not found"}`)
		}
		w.WriteHeader(status)
		_, writeErr := w.Write(body)
		assert.NoError(t, writeErr)
	}))
	t.Cleanup(server.Close)
	return server, &releases
}

const (
	acquireCreatedLock = `{"lock":{"database":"testdb","database_type":"mysql"},"created":true}`
	acquireFoundLock   = `{"lock":{"database":"testdb","database_type":"mysql"}}`
	activeApplyRefusal = `{"error":"apply blocked: apply apply-other is still running on testdb","error_code":"active_apply_exists"}`
)

// runUnsubmittedApply runs `apply -y` against server under ctx and returns its
// output and error.
func runUnsubmittedApply(ctx context.Context, schemaDir string, server *httptest.Server) (string, error) {
	cmd := ApplyCmd{SchemaDir: schemaDir, Environment: "staging", AutoApprove: true, Output: OutputFormatLog}
	var runErr error
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(ctx, &Globals{Endpoint: server.URL}) }))
	return out, runErr
}

// The server refuses the apply because another apply is still running on the
// database. Nothing was queued, so the lock this apply took is released rather
// than left to refuse every later apply until someone unlocks it by hand.
func TestApplyReleasesItsLockWhenTheServerRefusesTheApply(t *testing.T) {
	t.Setenv("SCHEMABOT_TOKEN", "fictional-unsubmitted-lock-test")
	server, releases := unsubmittedLockServer{acquireBody: acquireCreatedLock, applyStatus: http.StatusConflict, applyBody: activeApplyRefusal, releaseStatus: http.StatusOK}.start(t)

	out, err := runUnsubmittedApply(t.Context(), writeTestSchemaDir(t), server)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "apply apply-other is still running")
	assert.Equal(t, int32(1), releases.Load(), out)
	assert.Contains(t, out, "🔓 Lock released for testdb (mysql)")
}

// A server error leaves it unknown whether the apply was stored before the
// failure, so the lock stays held: releasing it could let a second apply start
// on a database the first one is changing.
func TestApplyKeepsItsLockWhenTheSubmitOutcomeIsUnknown(t *testing.T) {
	t.Setenv("SCHEMABOT_TOKEN", "fictional-unsubmitted-lock-test")
	server, releases := unsubmittedLockServer{acquireBody: acquireCreatedLock, applyStatus: http.StatusInternalServerError, applyBody: `{"error":"apply failed"}`, releaseStatus: http.StatusOK}.start(t)

	out, err := runUnsubmittedApply(t.Context(), writeTestSchemaDir(t), server)

	require.Error(t, err)
	assert.Equal(t, int32(0), releases.Load(), out)
	assert.NotContains(t, out, "Lock released")
}

// The operator already held the lock before this apply, for example from an
// earlier apply that is still running. A refused submit releases only a lock
// this command took, so the lock that guards the earlier apply stays held.
func TestApplyKeepsALockItFoundAlreadyHeld(t *testing.T) {
	t.Setenv("SCHEMABOT_TOKEN", "fictional-unsubmitted-lock-test")
	server, releases := unsubmittedLockServer{acquireBody: acquireFoundLock, applyStatus: http.StatusConflict, applyBody: activeApplyRefusal, releaseStatus: http.StatusOK}.start(t)

	out, err := runUnsubmittedApply(t.Context(), writeTestSchemaDir(t), server)

	require.Error(t, err)
	assert.Equal(t, int32(0), releases.Load(), out)
}

// The operator presses Ctrl-C while the lock is being taken. The apply is
// never sent, so the lock is released and the command still exits as
// interrupted.
func TestApplyReleasesItsLockWhenStoppedBeforeSubmit(t *testing.T) {
	t.Setenv("SCHEMABOT_TOKEN", "fictional-unsubmitted-lock-test")
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	server, releases := unsubmittedLockServer{acquireBody: acquireCreatedLock, onAcquire: cancel, applyStatus: http.StatusOK, applyBody: `{"accepted":true,"apply_id":"apply-never"}`, releaseStatus: http.StatusOK}.start(t)

	out, err := runUnsubmittedApply(ctx, writeTestSchemaDir(t), server)

	exitErr, ok := errors.AsType[*ExitCodeError](err)
	require.True(t, ok, "the command exits as interrupted: %v", err)
	assert.Equal(t, ExitInterrupted, exitErr.Code)
	assert.Equal(t, int32(1), releases.Load(), out)
	assert.Contains(t, out, "🔓 Lock released for testdb (mysql)")
}

// When the release itself fails, the operator is told the lock is still held
// and given the command that releases it, and the command fails with both the
// refusal and the release failure.
func TestApplyNamesTheUnlockCommandWhenTheReleaseFails(t *testing.T) {
	t.Setenv("SCHEMABOT_TOKEN", "fictional-unsubmitted-lock-test")
	server, releases := unsubmittedLockServer{acquireBody: acquireCreatedLock, applyStatus: http.StatusConflict, applyBody: activeApplyRefusal, releaseStatus: http.StatusInternalServerError}.start(t)

	out, err := runUnsubmittedApply(t.Context(), writeTestSchemaDir(t), server)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "apply apply-other is still running")
	assert.Contains(t, err.Error(), "release lock for testdb")
	assert.Equal(t, int32(1), releases.Load(), out)
	assert.Contains(t, out, "🔒 Lock kept for testdb (mysql): the server did not release it")
	assert.Contains(t, out, "Nothing was submitted, so release it now: schemabot unlock -d testdb -t mysql")
}

// A rollback the server refuses releases the lock it took, the same as an
// apply.
func TestRollbackReleasesItsLockWhenTheServerRefusesTheRollback(t *testing.T) {
	t.Setenv("SCHEMABOT_TOKEN", "fictional-unsubmitted-lock-test")
	server, releases := unsubmittedLockServer{acquireBody: acquireCreatedLock, applyStatus: http.StatusConflict, applyBody: activeApplyRefusal, releaseStatus: http.StatusOK}.start(t)

	var runErr error
	out := stripAnsi(captureStdout(func() {
		cmd := RollbackCmd{ApplyID: "apply-example-85", Environment: "staging", AutoApprove: true}
		runErr = cmd.Run(t.Context(), &Globals{Endpoint: server.URL})
	}))

	require.Error(t, runErr)
	assert.Contains(t, runErr.Error(), "apply apply-other is still running")
	assert.Equal(t, int32(1), releases.Load(), out)
	assert.Contains(t, out, "🔓 Lock released for testdb (mysql)")
}
