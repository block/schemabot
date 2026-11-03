package commands

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

// A plan that reports errors is a failed plan: the command prints the full
// plan, in text or JSON, and then exits non-zero so a script running it as a
// gate stops. The error is silent, so stdout stays the plan alone and a JSON
// consumer still receives a single document.
func TestPlanCmd_ExitsNonZeroWhenThePlanHasErrors(t *testing.T) {
	const planError = "lint error in `users`: PRIMARY KEY is required"
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

// Every environment's plan is held to the same rule, so one environment's
// errors fail the command even when the others planned cleanly, and the error
// names the environment that failed.
func TestPlanErrorsExit_NamesTheEnvironmentWithErrors(t *testing.T) {
	results := map[string]*apitypes.PlanResponse{
		"staging":    planWithTablesAndEngine("mysql", createUsers()),
		"production": {Engine: "mysql", Errors: []string{"table `users` has no primary key"}},
		"canary":     nil,
	}

	err := planErrorsExit(results)

	require.ErrorIs(t, err, ErrSilent)
	assert.Contains(t, err.Error(), "production")
	assert.NotContains(t, err.Error(), "staging")

	results["production"].Errors = nil
	require.NoError(t, planErrorsExit(results))
}
