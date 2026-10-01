package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

// narrowedPlanWithRollout is a plan narrowed to prod/payments-002 that also
// carries a rollout block naming the other targets, one of which could not be
// planned. The server never pairs the two; the CLI reads the narrowing as the
// fact that decides what the plan covers.
func narrowedPlanWithRollout() *apitypes.PlanResponse {
	return &apitypes.PlanResponse{
		PlanID:     "plan-narrowed",
		Database:   "orders",
		Engine:     "mysql",
		Target:     "payments-002",
		NarrowedTo: "prod/payments-002",
		Changes:    addColumnTo("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     3,
			Independent: true,
			Groups: []*apitypes.PlanMemberGroupResponse{
				{Members: paymentsTargets(1, 1), Primary: true, Changes: addColumnTo("region")},
				{Members: paymentsTargets(2, 2), Changes: addColumnTo("tier")},
			},
			Attention: []*apitypes.PlanMemberAttentionResponse{
				{Member: "prod/payments-003", Reason: apitypes.PlanMemberUnplanned, Detail: "could not be planned; see server logs for the cause, then plan again"},
			},
		},
	}
}

// A narrowed plan is rendered as its one member's plan: no divergence heading
// and no group naming the other targets, so it never reads as what applies
// across the rollout.
func TestWritePlanBody_NarrowedPlanIsNotPresentedAsTheRollout(t *testing.T) {
	plan := narrowedPlanWithRollout()

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))
	assert.Contains(t, out, "ADD COLUMN `region`", "%s", out)
	assert.NotContains(t, out, "ADD COLUMN `tier`", "another member's plan is not shown:\n%s", out)
	assert.NotContains(t, out, "Targets diverge", "%s", out)
	assert.NotContains(t, out, "▸", "no rollout group heading:\n%s", out)
	assert.NotContains(t, out, "prod/payments-003", "%s", out)

	assert.Nil(t, plan.WholeRollout())
	require.Len(t, plan.MemberPlans(), 1)
	assert.Same(t, plan, plan.MemberPlans()[0])
}

// applyRecordingServer answers /api/plan with plan and records the body of the
// /api/apply request, answering it with a server error so the command stops
// once the request is made.
func applyRecordingServer(t *testing.T, plan *apitypes.PlanResponse) (*httptest.Server, func() (apitypes.ApplyRequest, bool)) {
	t.Helper()
	body, err := json.Marshal(plan)
	require.NoError(t, err)
	var mu sync.Mutex
	var applyReq apitypes.ApplyRequest
	applied := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/api/plan":
			w.Header().Set("Content-Type", "application/json")
			_, writeErr := w.Write(body)
			assert.NoError(t, writeErr)
		case "/api/apply":
			applied = true
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&applyReq))
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server, func() (apitypes.ApplyRequest, bool) {
		mu.Lock()
		defer mu.Unlock()
		return applyReq, applied
	}
}

// Another member needing attention does not block an apply narrowed to
// prod/payments-002: the apply runs on that member only, and is sent narrowed
// to it.
func TestApplyCmd_NarrowedApplyIsNotGatedOnOtherMembers(t *testing.T) {
	server, applyRequest := applyRecordingServer(t, narrowedPlanWithRollout())

	cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "production", Target: "payments-002", NoLock: true, AutoApprove: true}
	var runErr error
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: server.URL}) }))

	require.Error(t, runErr, "the recording server fails the apply once it is requested")
	assert.NotContains(t, runErr.Error(), "rollout members cannot be applied as planned")
	req, applied := applyRequest()
	require.True(t, applied, "the narrowed apply is requested:\n%s", out)
	assert.Equal(t, "plan-narrowed", req.PlanID)
	assert.Equal(t, "prod/payments-002", req.Target)
}

// An apply of the whole rollout is sent with no target, so the server holds it
// to the rollout primary's plan, and says the CLI rendered every target's plan,
// which the server requires before it applies a rollout from an API caller.
func TestApplyCmd_RolloutWideApplySendsNoTarget(t *testing.T) {
	server, applyRequest := applyRecordingServer(t, &apitypes.PlanResponse{
		PlanID:  "plan-orders-1",
		Engine:  "mysql",
		Target:  "payments-001",
		Changes: addColumnTo("region"),
		Rollout: &apitypes.PlanRolloutResponse{
			Members:     3,
			Independent: true,
			Groups:      []*apitypes.PlanMemberGroupResponse{{Members: paymentsTargets(1, 3), Primary: true, Changes: addColumnTo("region")}},
		},
	})

	cmd := ApplyCmd{SchemaDir: writeTestSchemaDir(t), Environment: "production", NoLock: true, AutoApprove: true}
	var runErr error
	out := stripAnsi(captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: server.URL}) }))

	require.Error(t, runErr, "the recording server fails the apply once it is requested")
	req, applied := applyRequest()
	require.True(t, applied, "the rollout apply is requested:\n%s", out)
	assert.Equal(t, "plan-orders-1", req.PlanID)
	assert.Empty(t, req.Target)
	assert.True(t, req.RendersRollout, "the apply says the CLI rendered the rollout")
}
