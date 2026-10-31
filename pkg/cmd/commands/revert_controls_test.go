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
// an error, so the command exits non-zero and a script never mistakes the
// refusal for a request that went through.
func TestRevertControlsReturnARefusalAsAnError(t *testing.T) {
	for _, operation := range []string{"revert", "skip-revert"} {
		t.Run(operation, func(t *testing.T) {
			server := revertControlTestServer(t, operation, apitypes.ControlResponse{
				ErrorMessage: "the revert window has closed",
			})

			out, err := runRevertControl(t, operation, server.URL)

			require.EqualError(t, err, operation+" not accepted: the revert window has closed")
			assert.Empty(t, out)
		})
	}
}

// An accepted revert or skip-revert reports success and exits zero.
func TestRevertControlsReportAnAcceptedRequest(t *testing.T) {
	for _, tc := range []struct {
		operation string
		want      string
	}{
		{"revert", "Revert initiated for orders/staging (apply " + revertControlTestApplyID + ")\n"},
		{"skip-revert", "Revert window skipped for orders/staging (apply " + revertControlTestApplyID + ") — schema change finalized\n"},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			server := revertControlTestServer(t, tc.operation, apitypes.ControlResponse{Accepted: true})

			out, err := runRevertControl(t, tc.operation, server.URL)

			require.NoError(t, err)
			assert.Equal(t, tc.want, out)
		})
	}
}
