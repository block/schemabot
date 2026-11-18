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

const revertControlTestApplyID = "apply-b2c3d4e5f6a78901"

// revertControlTestServer serves a schema change in its revert window and
// answers the given revert control with the given body.
func revertControlTestServer(t *testing.T, operation string, body apitypes.ControlResponse) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/progress/apply/"+revertControlTestApplyID:
			assert.NoError(t, json.NewEncoder(w).Encode(apitypes.ProgressResponse{
				State:    state.Apply.RevertWindow,
				Database: "orders",
				ApplyID:  revertControlTestApplyID,
			}))
		case r.Method == http.MethodPost && r.URL.Path == "/api/"+operation:
			assert.NoError(t, json.NewEncoder(w).Encode(body))
		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func runRevertControl(t *testing.T, operation, endpoint string) (string, error) {
	t.Helper()
	flags := ControlFlags{ApplyID: revertControlTestApplyID, Environment: "staging"}
	g := &Globals{Endpoint: endpoint}
	var runErr error
	output := captureStdout(func() {
		switch operation {
		case "revert":
			cmd := RevertCmd{ControlFlags: flags}
			runErr = cmd.Run(g)
		case "skip-revert":
			cmd := SkipRevertCmd{ControlFlags: flags}
			runErr = cmd.Run(g)
		default:
			t.Fatalf("unknown revert control %q", operation)
		}
	})
	return stripAnsi(output), runErr
}

// The server can refuse a revert or skip-revert after the CLI has seen the
// schema change in its revert window — the window closed in between, or
// another operator's control got there first. The CLI returns the refusal as
// an error, which the top-level command prints to stderr and exits 1 on, so a
// script never mistakes the refusal for a request that went through.
func TestRevertControlsReturnARefusalAsAnError(t *testing.T) {
	for _, operation := range []string{"revert", "skip-revert"} {
		t.Run(operation, func(t *testing.T) {
			server := revertControlTestServer(t, operation, apitypes.ControlResponse{
				ErrorMessage: "the revert window has closed",
			})

			out, err := runRevertControl(t, operation, server.URL)

			require.EqualError(t, err, operation+" not accepted: the revert window has closed")
			assert.Empty(t, out)
			assert.NotErrorIs(t, err, ErrSilent, "the refusal is printed as an error, not suppressed")
			assert.Equal(t, 1, ExitCodeFor(err), "the command exits 1 on the refusal")
		})
	}
}

// An accepted revert or skip-revert has only recorded a request the driver
// consumes later, so the CLI says the operation was requested, names the
// command that follows its effect, and exits zero. A request the server already
// held pending is reported as already requested.
func TestRevertControlsReportARequestNotAnEffect(t *testing.T) {
	watch := "To watch and manage: schemabot progress " + revertControlTestApplyID + " -e staging\n"
	target := "orders/staging (apply " + revertControlTestApplyID + ")"
	for _, tc := range []struct {
		name      string
		operation string
		status    string
		want      string
	}{
		{"revert", "revert", "", "Revert requested for " + target + " — SchemaBot will undo this schema change\n" + watch},
		{"revert already requested", "revert", apitypes.ControlStatusAlreadyRequested, "Revert already requested for " + target + " — SchemaBot will finish the existing request\n" + watch},
		{"skip-revert", "skip-revert", "", "Skip-revert requested for " + target + " — SchemaBot will close the revert window and make this schema change permanent\n" + watch},
		{"skip-revert already requested", "skip-revert", apitypes.ControlStatusAlreadyRequested, "Skip-revert already requested for " + target + " — SchemaBot will finish the existing request\n" + watch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := revertControlTestServer(t, tc.operation, apitypes.ControlResponse{Accepted: true, Status: tc.status})

			out, err := runRevertControl(t, tc.operation, server.URL)

			require.NoError(t, err)
			assert.Equal(t, tc.want, out)
		})
	}
}
