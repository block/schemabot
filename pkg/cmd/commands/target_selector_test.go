package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

func TestPlanAndApplyCmdsParseTarget(t *testing.T) {
	var cli struct {
		Plan  PlanCmd  `cmd:""`
		Apply ApplyCmd `cmd:""`
	}
	parser, err := kong.New(&cli, kong.Vars{"cli_name": "schemabot"})
	require.NoError(t, err)

	_, err = parser.Parse([]string{"plan", "-e", "production", "--target", "payments-002"})
	require.NoError(t, err)
	assert.Equal(t, "payments-002", cli.Plan.Target)

	_, err = parser.Parse([]string{"apply", "-s", ".", "-e", "production", "--target", "prod/payments-002"})
	require.NoError(t, err)
	assert.Equal(t, "prod/payments-002", cli.Apply.Target)
}

// A target names a member of one environment, so planning every environment
// with one is refused before anything is sent.
func TestPlanCmd_TargetRequiresEnvironment(t *testing.T) {
	cmd := PlanCmd{SchemaDir: writeTestSchemaDir(t), Target: "payments-002"}
	err := cmd.Run(&Globals{Endpoint: "http://unreachable.invalid"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pass -e")
}

// apply --target sends the selector with the plan, then applies the plan to the
// member the server narrowed it to, so the apply cannot run rollout-wide.
func TestApplyCmd_TargetNarrowsPlanAndApply(t *testing.T) {
	plan := planWithTablesAndEngine("mysql", createUsers())
	plan.PlanID = "plan-narrowed"
	plan.Target = "payments-002"
	plan.NarrowedTo = "prod/payments-002"
	planBody, err := json.Marshal(plan)
	require.NoError(t, err)

	var mu sync.Mutex
	var planReq apitypes.PlanRequest
	var applyReq apitypes.ApplyRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/api/plan":
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&planReq))
			_, writeErr := w.Write(planBody)
			assert.NoError(t, writeErr)
		case "/api/apply":
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&applyReq))
			_, writeErr := w.Write([]byte(`{"accepted":true,"apply_id":"apply-narrowed"}`))
			assert.NoError(t, writeErr)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	cmd := ApplyCmd{
		SchemaDir:   writeTestSchemaDir(t),
		Environment: "production",
		Target:      "payments-002",
		AutoApprove: true,
		NoLock:      true,
		Output:      OutputFormatJSON,
	}
	out := stripAnsi(captureStdout(func() {
		require.NoError(t, cmd.Run(&Globals{Endpoint: server.URL}))
	}))

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, "payments-002", planReq.Target)
	assert.Equal(t, "plan-narrowed", applyReq.PlanID)
	assert.Equal(t, "prod/payments-002", applyReq.Target)
	assert.Contains(t, out, "Target: prod/payments-002 (this plan covers only this rollout member)")
}
