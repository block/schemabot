package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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

// narrowedPlanServer serves plan from /api/plan and accepts every apply, and
// records the requests the CLI sent and the paths it called.
type narrowedPlanServer struct {
	mu       sync.Mutex
	planReq  apitypes.PlanRequest
	applyReq apitypes.ApplyRequest
	paths    []string
}

func newNarrowedPlanServer(t *testing.T, plan *apitypes.PlanResponse) (*narrowedPlanServer, string) {
	t.Helper()
	planBody, err := json.Marshal(plan)
	require.NoError(t, err)
	recorded := &narrowedPlanServer{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		recorded.mu.Lock()
		defer recorded.mu.Unlock()
		recorded.paths = append(recorded.paths, r.URL.Path)
		switch r.URL.Path {
		case "/api/plan":
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&recorded.planReq))
			_, writeErr := w.Write(planBody)
			assert.NoError(t, writeErr)
		case "/api/apply":
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&recorded.applyReq))
			_, writeErr := w.Write([]byte(`{"accepted":true,"apply_id":"apply-narrowed"}`))
			assert.NoError(t, writeErr)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return recorded, server.URL
}

// narrowedPlan is a plan the server narrowed to prod/payments-002.
func narrowedPlan(tables ...*apitypes.TableChangeResponse) *apitypes.PlanResponse {
	plan := planWithTablesAndEngine("mysql", tables...)
	plan.PlanID = "plan-narrowed"
	plan.Target = "payments-002"
	plan.NarrowedTo = "prod/payments-002"
	return plan
}

func targetedApplyCmd(t *testing.T) ApplyCmd {
	return ApplyCmd{
		SchemaDir:   writeTestSchemaDir(t),
		Environment: "production",
		Target:      "payments-002",
		AutoApprove: true,
		NoLock:      true,
		Output:      OutputFormatJSON,
	}
}

// apply --target sends the selector with the plan, then applies the plan to the
// member the server narrowed it to, so the apply cannot run rollout-wide. The
// active schema change preflight asks about the whole environment, so a
// targeted apply leaves conflicts to the server's deployment reservation
// instead of being blocked by an apply on another deployment.
func TestApplyCmd_TargetNarrowsPlanAndApply(t *testing.T) {
	recorded, endpoint := newNarrowedPlanServer(t, narrowedPlan(createUsers()))

	cmd := targetedApplyCmd(t)
	out := stripAnsi(captureStdout(func() {
		require.NoError(t, cmd.Run(&Globals{Endpoint: endpoint}))
	}))

	recorded.mu.Lock()
	defer recorded.mu.Unlock()
	assert.Equal(t, "payments-002", recorded.planReq.Target)
	assert.Equal(t, "plan-narrowed", recorded.applyReq.PlanID)
	assert.Equal(t, "prod/payments-002", recorded.applyReq.Target)
	assert.Equal(t, []string{"/api/plan", "/api/apply"}, recorded.paths, "a targeted apply skips the environment-wide status preflight")
	assert.Equal(t, 1, strings.Count(out, "Target: prod/payments-002 (this plan covers only this rollout member)"), "the narrowing is disclosed once:\n%s", out)
}

// A member with nothing to change still says it was the only member planned,
// so "up-to-date" is not read as covering the whole environment.
func TestApplyCmd_TargetDisclosedWhenMemberIsUpToDate(t *testing.T) {
	plan := narrowedPlan()
	plan.Changes = nil
	recorded, endpoint := newNarrowedPlanServer(t, plan)

	cmd := targetedApplyCmd(t)
	out := stripAnsi(captureStdout(func() {
		require.NoError(t, cmd.Run(&Globals{Endpoint: endpoint}))
	}))

	assert.Contains(t, out, "No changes. Your schema is up-to-date.\nTarget: prod/payments-002 (this plan covers only this rollout member)")
	recorded.mu.Lock()
	defer recorded.mu.Unlock()
	assert.Equal(t, []string{"/api/plan"}, recorded.paths, "nothing is applied when the member is up to date")
}

// An unsafe change planned for one member is blocked with a retry command that
// keeps --target, so following it re-runs the change on that member and not
// across the whole rollout.
func TestApplyCmd_UnsafeRetryKeepsTarget(t *testing.T) {
	recorded, endpoint := newNarrowedPlanServer(t, narrowedPlan(&apitypes.TableChangeResponse{
		DDL: "DROP TABLE users", ChangeType: "DROP", TableName: "users", IsUnsafe: true,
	}))

	cmd := targetedApplyCmd(t)
	cmd.Output = OutputFormatInteractive
	var runErr error
	out := stripAnsi(captureStdout(func() {
		runErr = cmd.Run(&Globals{Endpoint: endpoint})
	}))

	require.ErrorIs(t, runErr, ErrSilent)
	assert.Contains(t, out, "Target: prod/payments-002 (this plan covers only this rollout member)")
	assert.Contains(t, out, "-e production --target payments-002 --allow-unsafe")
	recorded.mu.Lock()
	defer recorded.mu.Unlock()
	assert.NotContains(t, recorded.paths, "/api/apply", "a blocked unsafe change is never applied")
}
