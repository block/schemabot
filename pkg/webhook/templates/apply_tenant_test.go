package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// Every pasteable lifecycle hint in the apply status comment addresses a
// specific deployment, so on a tenant deployment each carries the --tenant
// flag; single-tenant deployments render the same command unchanged.
func TestApplyStatusCommentHintsCarryTenant(t *testing.T) {
	cases := []struct {
		name     string
		state    string
		engine   string
		commands []string
	}{
		{name: "waiting_for_deploy cutover", state: state.Apply.WaitingForDeploy, commands: []string{"schemabot cutover apply-x -e production"}},
		{name: "waiting_for_cutover cutover", state: state.Apply.WaitingForCutover, commands: []string{"schemabot cutover apply-x -e production"}},
		{name: "running stop", state: state.Apply.Running, commands: []string{"schemabot stop apply-x -e production"}},
		{name: "running planetscale cancel", state: state.Apply.Running, engine: storage.EnginePlanetScale, commands: []string{"schemabot cancel apply-x -e production"}},
		{name: "failed_retryable stop", state: state.Apply.FailedRetryable, commands: []string{"schemabot stop apply-x -e production"}},
		{name: "stopped start", state: state.Apply.Stopped, commands: []string{"schemabot start apply-x -e production"}},
		{name: "failed retry", state: state.Apply.Failed, commands: []string{"schemabot apply -e production"}},
		{name: "revert window skip-revert and revert", state: state.Apply.RevertWindow, engine: storage.EnginePlanetScale, commands: []string{
			"schemabot skip-revert apply-x -e production",
			"schemabot revert apply-x -e production",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := ApplyStatusCommentData{
				ApplyID:     "apply-x",
				Database:    "orders",
				Environment: "production",
				State:       tc.state,
				Engine:      tc.engine,
			}

			untenanted := RenderApplyStatusComment(data)
			assert.NotContains(t, untenanted, "--tenant")
			for _, cmd := range tc.commands {
				assert.Contains(t, untenanted, cmd+"\n", "single-tenant hint must be the bare command")
			}

			data.Tenant = "acme"
			tenanted := RenderApplyStatusComment(data)
			for _, cmd := range tc.commands {
				assert.Contains(t, tenanted, cmd+" --tenant acme", "tenant hint must carry the deployment's tenant")
			}
		})
	}
}

// The terminal summary comment's retry and resume hints carry the tenant flag
// on tenant deployments and stay unchanged on single-tenant ones.
func TestApplySummaryCommentHintsCarryTenant(t *testing.T) {
	cases := []struct {
		name    string
		state   string
		command string
	}{
		{name: "failed retry", state: state.Apply.Failed, command: "schemabot apply -e production"},
		{name: "stopped start", state: state.Apply.Stopped, command: "schemabot start apply-x -e production"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := ApplyStatusCommentData{
				ApplyID:     "apply-x",
				Database:    "orders",
				Environment: "production",
				State:       tc.state,
			}

			untenanted := RenderApplySummaryComment(data)
			assert.NotContains(t, untenanted, "--tenant")
			assert.Contains(t, untenanted, tc.command+"\n", "single-tenant hint must be the bare command")

			data.Tenant = "acme"
			tenanted := RenderApplySummaryComment(data)
			assert.Contains(t, tenanted, tc.command+" --tenant acme", "tenant hint must carry the deployment's tenant")
		})
	}
}
