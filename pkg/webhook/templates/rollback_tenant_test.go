package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Every pasteable command hint in a confirm-time rejection addresses a
// specific deployment, so on a tenant deployment each carries the --tenant
// flag; single-tenant deployments render the same command unchanged.
func TestConfirmRejectionHintsCarryTenant(t *testing.T) {
	cases := []struct {
		name     string
		render   func(environment, tenant string) string
		commands []string
	}{
		{
			name:     "rollback-confirm blocked plan",
			render:   RenderRollbackConfirmBlockedPlan,
			commands: []string{"schemabot rollback <apply-id> -e production"},
		},
		{
			name:   "rollback-confirm blocked plan, lock held",
			render: RenderRollbackConfirmBlockedPlanLockHeld,
			commands: []string{
				"schemabot unlock",
				"schemabot rollback <apply-id> -e production",
			},
		},
		{
			name:     "defer-cutover on all-direct apply-confirm",
			render:   RenderDeferCutoverAllDirectConfirm,
			commands: []string{"schemabot apply-confirm -e production"},
		},
		{
			name:     "defer-cutover on all-direct rollback-confirm",
			render:   RenderDeferCutoverAllDirectRollbackConfirm,
			commands: []string{"schemabot rollback-confirm -e production"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			untenanted := tc.render("production", "")
			assert.NotContains(t, untenanted, "--tenant")
			for _, cmd := range tc.commands {
				assert.Contains(t, untenanted, cmd+"`", "single-tenant hint must be the bare command")
			}

			tenanted := tc.render("production", "acme")
			for _, cmd := range tc.commands {
				assert.Contains(t, tenanted, cmd+" --tenant acme`", "tenant hint must carry the deployment's tenant")
			}
		})
	}
}
