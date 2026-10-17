package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
)

const stopTestApplyID = "apply-a1b2c3d4e5f67890"

// stopTestServer serves a stopped schema change's progress and answers the
// stop request with the given status and body, counting the stop requests.
func stopTestServer(t *testing.T, stopStatus int, stopBody any) (*httptest.Server, *int) {
	t.Helper()
	stopCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/progress/apply/"+stopTestApplyID:
			assert.NoError(t, json.NewEncoder(w).Encode(apitypes.ProgressResponse{
				State:    state.Apply.Stopped,
				Database: "orders",
				ApplyID:  stopTestApplyID,
			}))
		case r.Method == http.MethodPost && r.URL.Path == "/api/stop":
			stopCalls++
			w.WriteHeader(stopStatus)
			assert.NoError(t, json.NewEncoder(w).Encode(stopBody))
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	return server, &stopCalls
}

func runStopCmd(t *testing.T, endpoint string) (string, error) {
	t.Helper()
	var runErr error
	output := captureStdout(func() {
		cmd := StopCmd{ControlFlags: ControlFlags{ApplyID: stopTestApplyID, Environment: "staging"}}
		runErr = cmd.Run(&Globals{Endpoint: endpoint})
	})
	return stripAnsi(output), runErr
}

// An operator who stops a schema change moments after starting it can find it
// still stopped, with the start queued behind it. The CLI sends the stop
// anyway, and when the server withdraws the start it says so: the schema
// change stays stopped and the operator is shown how to resume it.
func TestStopCmdWithdrawsAQueuedStart(t *testing.T) {
	server, stopCalls := stopTestServer(t, http.StatusOK, apitypes.StopResponse{
		Accepted: true,
		Status:   apitypes.ControlStatusStartWithdrawn,
	})

	out, err := runStopCmd(t, server.URL)
	require.NoError(t, err)
	assert.Equal(t, 1, *stopCalls, "a stopped schema change still sends the stop, so a queued start can be withdrawn")
	assert.Contains(t, out, "Schema change stopped")
	assert.Contains(t, out, "The start requested earlier had not taken effect yet, so this stop withdrew it.")
	assert.Contains(t, out, "start -e staging "+stopTestApplyID)
	assert.NotContains(t, out, "Stopped:")
}

// A stopped schema change with no start queued has nothing to stop: the server
// refuses with the already-stopped code, and the CLI reports it as already
// stopped rather than as a failure.
func TestStopCmdReportsAlreadyStopped(t *testing.T) {
	server, stopCalls := stopTestServer(t, http.StatusConflict, apitypes.ErrorResponse{
		Error:     "stop rejected: schema change is already stopped",
		ErrorCode: apitypes.ErrCodeAlreadyStopped,
	})

	out, err := runStopCmd(t, server.URL)
	require.NoError(t, err)
	assert.Equal(t, 1, *stopCalls)
	assert.Equal(t, "Schema change already stopped\n", out)
}

// A server that predates the already-stopped code refuses a stop on a stopped
// schema change with a bare conflict, including when a start is queued behind
// it that will still resume the rollout. The CLI surfaces that refusal as an
// error rather than reporting the schema change as stopped.
func TestStopCmdSurfacesABareConflictFromAnOlderServer(t *testing.T) {
	server, stopCalls := stopTestServer(t, http.StatusConflict, apitypes.ErrorResponse{
		Error: "stop rejected: schema change is already terminal (current state: stopped)",
	})

	out, err := runStopCmd(t, server.URL)
	require.Error(t, err)
	assert.Equal(t, 1, *stopCalls)
	assert.Contains(t, err.Error(), "stop rejected: schema change is already terminal (current state: stopped)")
	assert.NotContains(t, out, "Schema change already stopped")
}
