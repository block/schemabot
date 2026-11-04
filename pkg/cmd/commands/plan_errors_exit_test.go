package commands

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine"
)

// A plan that apply would refuse whatever its flags is a failed plan: the
// command prints the full plan, in text or JSON, and then exits non-zero so a
// script running it as a gate stops. The error is silent, so stdout stays the
// plan alone and a JSON consumer still receives a single document.
func TestPlanCmd_ExitsNonZeroWhenApplyWouldRefuseThePlan(t *testing.T) {
	// The server's errors field carries whatever stopped the plan from being
	// produced in full; the wording here stands in for any such message.
	const planError = "schema file users.sql: statement could not be planned"
	for _, tc := range []struct {
		name    string
		json    bool
		errors  []string
		wantErr bool
	}{
		{name: "text with errors", errors: []string{planError}, wantErr: true},
		{name: "json with errors", json: true, errors: []string{planError}, wantErr: true},
		{name: "text without errors"},
		{name: "json without errors", json: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := planWithTablesAndEngine("mysql", createUsers())
			plan.Errors = tc.errors
			_, endpoint := newNarrowedPlanServer(t, plan)

			cmd := PlanCmd{SchemaDir: writeTestSchemaDir(t), Environment: "staging", JSON: tc.json}
			var runErr error
			out := captureStdout(func() { runErr = cmd.Run(&Globals{Endpoint: endpoint}) })

			if !tc.wantErr {
				require.NoError(t, runErr)
			} else {
				require.ErrorIs(t, runErr, ErrSilent, "the plan output already reports the errors")
				assert.Equal(t, 1, ExitCodeFor(runErr))
				assert.Contains(t, runErr.Error(), "staging")
			}

			if tc.json {
				var printed map[string]*apitypes.PlanResponse
				require.NoError(t, json.Unmarshal([]byte(out), &printed), "stdout is one JSON document:\n%s", out)
				require.Contains(t, printed, "staging")
				assert.Equal(t, tc.errors, printed["staging"].Errors)
				return
			}
			if tc.wantErr {
				assert.Contains(t, stripAnsi(out), planError, "the text plan still prints the errors")
			}
		})
	}
}

// The exit status follows apply's own unconditional refusals: errors, rollout
// members that need attention, and a blocked change each fail the command,
// and the error names which. Refusals a flag or a narrowed apply lifts do
// not, so an unsafe change, a rollout member apply runs under its own plan,
// and a rollout whose shape refuses a whole-rollout apply all exit 0.
func TestPlanRefusalExit_MirrorsApplyUnconditionalRefusals(t *testing.T) {
	blocked := createUsers()
	blocked.ExecutionMode = engine.ExecutionModeBlocked
	blocked.ModeReason = "direct execution is disabled"
	attention := []*apitypes.PlanMemberAttentionResponse{{Member: "prod/payments-002", Reason: apitypes.PlanMemberUnplanned, Detail: "plan failed; see server logs"}}
	refused := []*apitypes.PlanMemberRefusalResponse{{Member: "prod/payments-002", Target: "payments-002", Reason: apitypes.PlanMemberNeedsTarget}}
	unsafe := createUsers()
	unsafe.IsUnsafe = true
	unsafe.UnsafeReason = "drops a column"

	for _, tc := range []struct {
		name       string
		plan       *apitypes.PlanResponse
		wantReason string
	}{
		{name: "errors", plan: &apitypes.PlanResponse{Engine: "mysql", Errors: []string{"plan failed"}}, wantReason: "has errors"},
		{
			name:       "rollout member needs attention",
			plan:       &apitypes.PlanResponse{Engine: "mysql", Rollout: &apitypes.PlanRolloutResponse{Members: 2, Attention: attention}},
			wantReason: "has rollout members that need attention",
		},
		{name: "blocked change", plan: planWithTablesAndEngine("mysql", blocked), wantReason: "has a blocked change"},
		{
			name: "errors are named before attention and blocked changes",
			plan: &apitypes.PlanResponse{
				Engine: "mysql", Errors: []string{"plan failed"},
				Changes: []*apitypes.SchemaChangeResponse{{Namespace: "default", TableChanges: []*apitypes.TableChangeResponse{blocked}}},
				Rollout: &apitypes.PlanRolloutResponse{Members: 2, Attention: attention},
			},
			wantReason: "has errors",
		},
		{name: "changes to apply", plan: planWithTablesAndEngine("mysql", createUsers())},
		{name: "no changes", plan: planWithTablesAndEngine("mysql")},
		{name: "unsafe change needs an opt-in, not a different plan", plan: planWithTablesAndEngine("mysql", unsafe)},
		{
			name: "refused member runs under a narrowed apply",
			plan: &apitypes.PlanResponse{Engine: "mysql", Rollout: &apitypes.PlanRolloutResponse{Members: 2, Refused: refused}},
		},
		{
			name: "shape refusal names the apply to run instead",
			plan: &apitypes.PlanResponse{Engine: "mysql", Rollout: &apitypes.PlanRolloutResponse{Members: 2, ShapeRefusal: "apply one deployment at a time"}},
		},
		{
			name: "attention on a narrowed plan is not the rollout's",
			plan: &apitypes.PlanResponse{Engine: "mysql", NarrowedTo: "prod/payments-001", Rollout: &apitypes.PlanRolloutResponse{Members: 2, Attention: attention}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := planRefusalExit(map[string]*apitypes.PlanResponse{"staging": tc.plan})
			if tc.wantReason == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrSilent)
			assert.Equal(t, "plan for environment staging "+tc.wantReason+": "+ErrSilent.Error(), err.Error())
		})
	}
}

// Every environment's plan is held to the same rule, so one environment's
// refusal fails the command even when the others planned cleanly, and the
// error names the environment that failed.
func TestPlanRefusalExit_NamesTheEnvironmentWithErrors(t *testing.T) {
	results := map[string]*apitypes.PlanResponse{
		"staging":    planWithTablesAndEngine("mysql", createUsers()),
		"production": {Engine: "mysql", Errors: []string{"plan failed"}},
		"canary":     nil,
	}

	err := planRefusalExit(results)

	require.ErrorIs(t, err, ErrSilent)
	assert.Contains(t, err.Error(), "production")
	assert.NotContains(t, err.Error(), "staging")

	results["production"].Errors = nil
	require.NoError(t, planRefusalExit(results))
}

// When several environments' plans are refused, the error names the same one
// on every run: the first in name order, so a script that reads the message
// sees a stable environment rather than whichever the map yielded first.
func TestPlanRefusalExit_NamesTheFirstEnvironmentWithErrors(t *testing.T) {
	results := map[string]*apitypes.PlanResponse{
		"staging":    {Engine: "mysql", Errors: []string{"plan failed"}},
		"production": {Engine: "mysql", Errors: []string{"plan failed"}},
		"canary":     planWithTablesAndEngine("mysql", createUsers()),
	}

	for range 20 {
		err := planRefusalExit(results)
		require.ErrorIs(t, err, ErrSilent)
		assert.Contains(t, err.Error(), "production")
		assert.NotContains(t, err.Error(), "staging")
	}
}
