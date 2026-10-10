package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A finished barrier rollout whose eu and us deployments each address two
// targets: `schemabot progress` renders each deployment as one rollup
// section, counted in targets, with the shared change shown once per
// deployment. The finished table counts its targets in the deployment line and
// lists none of them, as the PR comment does.
func TestProgressCmd_MultiTargetDeploymentsRenderAsRollups(t *testing.T) {
	const applyID = "apply-5e21"
	alters := map[string]string{
		"eu": "ALTER TABLE `orders` ADD COLUMN `email` varchar(255) DEFAULT NULL",
		"us": "ALTER TABLE `orders` ADD COLUMN `name` varchar(255) NOT NULL, ADD COLUMN `email` varchar(255) DEFAULT NULL",
	}
	resp := apitypes.ProgressResponse{
		State:        state.Apply.Completed,
		Engine:       "Spirit",
		ApplyID:      applyID,
		Database:     "orders",
		DatabaseType: "mysql",
		Environment:  "production",
	}
	for _, member := range []struct{ deployment, target string }{
		{"eu", "payments-001"}, {"eu", "payments-002"},
		{"us", "payments-003"}, {"us", "payments-004"},
	} {
		resp.Operations = append(resp.Operations, &apitypes.ProgressOperationResponse{
			Deployment:    member.deployment,
			Target:        member.target,
			State:         state.ApplyOperation.Completed,
			CutoverPolicy: storage.CutoverPolicyBarrier,
			StartedAt:     "2026-09-30T12:00:00Z",
			CompletedAt:   "2026-09-30T12:05:00Z",
		})
		resp.Tables = append(resp.Tables, &apitypes.TableProgressResponse{
			TableName:       "orders",
			DDL:             alters[member.deployment],
			Deployment:      member.deployment,
			Target:          member.target,
			Keyspace:        "orders",
			ChangeType:      "alter",
			Status:          state.Task.Completed,
			RowsCopied:      1000,
			RowsTotal:       1000,
			PercentComplete: 100,
		})
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/progress/apply/"+applyID {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	t.Cleanup(server.Close)

	var runErr error
	output := captureStdout(func() {
		cmd := ProgressCmd{ControlFlags: ControlFlags{ApplyID: applyID}, Watch: false}
		runErr = cmd.Run(&Globals{Endpoint: server.URL})
	})
	require.NoError(t, runErr)
	out := stripAnsi(output)

	assert.Contains(t, out, "│  Targets:      4 completed  │", "the header counts targets, not deployments:\n%s", out)
	assert.NotContains(t, out, "Deployments:")
	assertContainsInOrder(t, out,
		"✅ eu — 2 completed (2 targets)",
		"ALTER TABLE `orders` ADD COLUMN `email` varchar(255) DEFAULT NULL;",
		"✅ us — 2 completed (2 targets)",
		"ADD COLUMN `name` varchar(255) NOT NULL,",
	)
	for _, target := range []string{"payments-001", "payments-002", "payments-003", "payments-004"} {
		assert.NotContainsf(t, out, target, "%s is counted in its deployment's rollup, not listed or given a section of its own:\n%s", target, out)
	}
	assert.NotContains(t, out, "• Targets:", "a finished table lists no targets:\n%s", out)
	assert.Equal(t, 1, strings.Count(out, "ALTER TABLE `orders` ADD COLUMN `email` varchar(255) DEFAULT NULL;"), "eu's change is shown once for both targets:\n%s", out)
}
