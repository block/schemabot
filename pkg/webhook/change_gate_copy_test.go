package webhook

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

// The comment names the freezes the refusal carries and the request it
// recorded, with the identities written as GitHub logins.
func TestChangeGateRefusalData(t *testing.T) {
	ends := time.Date(2026, 11, 22, 18, 0, 0, 0, time.UTC)
	refused := &api.ChangeGateRefusedError{
		Environment: "production",
		Blockers: []apitypes.ChangeGateBlocker{
			{Gate: "freeze", ID: "frz-1", Reason: "SEV-1234"},
			{Gate: "freeze", ID: "frz-2", Reason: "deploy window", EndsAt: &ends},
		},
		OverrideRequest: &storage.FreezeOverrideRequest{
			Identifier: "ovr-1", RequestedBy: "github:carol", PRAuthor: "github:alice", Reason: "hotfix",
		},
		OverrideReasonKept: true,
	}

	data := changeGateRefusalData(refused, action.Start, "acme", "carol", []string{"acme/freeze-approvers"})

	assert.Equal(t, action.Start, data.CommandName)
	assert.Equal(t, "production", data.Environment)
	assert.Equal(t, "acme", data.Tenant)
	assert.Equal(t, "carol", data.RequestedBy)
	assert.Equal(t, []templates.RefusingFreeze{{ID: "frz-1", Reason: "SEV-1234"}, {ID: "frz-2", Reason: "deploy window", EndsAt: &ends}}, data.Freezes)
	assert.Equal(t, &templates.RefusalOverrideRequest{ID: "ovr-1", Requester: "carol", Author: "alice", Reason: "hotfix", ReasonKept: true}, data.Override)
	assert.False(t, data.OverrideUnrecorded)
	assert.Equal(t, []string{"acme/freeze-approvers"}, data.Approvers)
	assert.True(t, data.TakesOverride, "start takes --override")

	// Whether a command takes --override comes from its spec, so the
	// refusal offers one exactly where the parser accepts the flag.
	for _, spec := range commandSpecs {
		assert.Equal(t, spec.SupportsOverride, changeGateRefusalData(refused, spec.Name, "", "carol", nil).TakesOverride, spec.Name)
	}
}

// A freeze refusal posts as its own comment, and a gate that could not
// decide posts its fixed message as a command error. A reason that reads
// like a gRPC error is quoted as it is, not explained as an outage.
func TestPostChangeGateRefusal(t *testing.T) {
	client, mux := setupGitHubServer(t)
	bodies := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		bodies <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	h := &Handler{
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
	}
	posted := func() string {
		select {
		case body := <-bodies:
			return body
		case <-time.After(2 * time.Second):
			require.FailNow(t, "timed out waiting for posted comment")
			return ""
		}
	}

	refused := &api.ChangeGateRefusedError{
		Environment: "production",
		Blockers:    []apitypes.ChangeGateBlocker{{Gate: "freeze", ID: "frz-1", Reason: "rpc error: code = Unavailable during the outage"}},
	}
	h.postChangeGateRefusal("octocat/hello-world", 1, 12345, action.Apply, "production", "alice", refused)
	body := posted()
	assert.Contains(t, body, "## ❄️ Apply Blocked by a Freeze\n")
	assert.Contains(t, body, "*Requested by @alice")
	assert.Contains(t, body, "| `frz-1` | until lifted | rpc error: code = Unavailable during the outage |")
	assert.NotContains(t, body, "could not reach")

	unavailable := &api.ChangeGateUnavailableError{Gate: "freeze", Err: errors.New("storage down")}
	h.postChangeGateRefusal("octocat/hello-world", 1, 12345, action.Apply, "production", "alice", unavailable)
	body = posted()
	assert.Contains(t, body, "Apply Failed")
	assert.Contains(t, body, "> "+unavailable.UserMessage())
	assert.NotContains(t, body, "storage down")
}
