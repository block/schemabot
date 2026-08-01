package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
)

// The multi-deployment comment's aggregate next-action hint addresses a
// specific deployment, so on a tenant deployment it carries the --tenant flag;
// single-tenant deployments render the same command unchanged.
func TestMultiDeploymentApplyHintsCarryTenant(t *testing.T) {
	cases := []struct {
		name    string
		ops     []presentation.Operation
		command string
	}{
		{
			name:    "cutover next action",
			ops:     []presentation.Operation{barrierOp("eu", so.WaitingForCutover), barrierOp("us", so.Running)},
			command: "schemabot cutover apply-123 -e production",
		},
		{
			name:    "resume next action",
			ops:     []presentation.Operation{rollingOp("eu", so.Stopped), rollingOp("us", so.Completed)},
			command: "schemabot start apply-123 -e production",
		},
		{
			name:    "retry next action",
			ops:     []presentation.Operation{rollingOp("eu", so.Completed), rollingOp("us", so.Failed)},
			command: "schemabot apply -e production",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := MultiDeploymentApplyData{
				Model:       presentation.Derive(tc.ops),
				ApplyID:     "apply-123",
				Environment: "production",
			}

			untenanted := RenderMultiDeploymentApplyComment(data)
			assert.NotContains(t, untenanted, "--tenant")
			assert.Contains(t, untenanted, tc.command+"\n", "single-tenant hint must be the bare command")

			data.Tenant = "acme"
			tenanted := RenderMultiDeploymentApplyComment(data)
			assert.Contains(t, tenanted, tc.command+" --tenant acme", "tenant hint must carry the deployment's tenant")
		})
	}
}

// Each deployment's <details> body is rendered by the single-deployment
// renderer including its lifecycle footer, so its hints carry the tenant flag
// from the deployment's own detail data.
func TestMultiDeploymentApplyDetailHintsCarryTenant(t *testing.T) {
	data := MultiDeploymentApplyData{
		Model:       presentation.Derive([]presentation.Operation{rollingOp("eu", so.Stopped), rollingOp("us", so.Completed)}),
		ApplyID:     "apply-123",
		Environment: "production",
		Tenant:      "acme",
		Details: map[string]ApplyStatusCommentData{
			"eu": {ApplyID: "apply-123", Environment: "production", State: state.Apply.Stopped, Tenant: "acme"},
		},
	}

	out := RenderMultiDeploymentApplyComment(data)
	assert.Contains(t, out, "schemabot start apply-123 -e production --tenant acme")
}
