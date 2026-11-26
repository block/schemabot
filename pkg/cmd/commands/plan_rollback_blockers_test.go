package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
)

// changeWithRollbackBlocker is a namespace that adds a column while a table
// in it holds a foreign key added NOT VALID, so the plan cannot save the
// namespace's current schema.
func changeWithRollbackBlocker() []*apitypes.SchemaChangeResponse {
	return []*apitypes.SchemaChangeResponse{{
		Namespace:    "public",
		TableChanges: []*apitypes.TableChangeResponse{alterUsersIn("public", "email")},
		RollbackBlockers: []apitypes.SkippedTable{{
			Table: "orders", Reason: "holds a foreign key added NOT VALID, which a schema file cannot declare",
		}},
	}}
}

// A plan for an environment with one target says, before anyone applies, that
// the apply cannot be rolled back and which table is why.
func TestWritePlanBody_SaysWhyASingleTargetApplyCannotBeRolledBack(t *testing.T) {
	plan := &apitypes.PlanResponse{Database: "orders", Engine: "postgres", Changes: changeWithRollbackBlocker()}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))

	assert.Contains(t, out, "No rollback for namespace public", "%s", out)
	assert.Contains(t, out, "- orders: holds a foreign key added NOT VALID", "%s", out)
}

// A rollout of two targets is refused rollback whatever either target saved,
// so its plan does not name a table as the reason an apply cannot be rolled
// back, even when a member's plan carries one.
func TestWritePlanBody_RolloutNamesNoRollbackBlockers(t *testing.T) {
	changes := changeWithRollbackBlocker()
	plan := &apitypes.PlanResponse{
		Database: "orders", Engine: "postgres", Changes: changes,
		Rollout: &apitypes.PlanRolloutResponse{
			Members: 2,
			Groups:  []*apitypes.PlanMemberGroupResponse{{Members: []string{"orders-a", "orders-b"}, Primary: true, Changes: changes}},
		},
	}

	out := stripAnsi(captureStdout(func() { writePlanBody(plan, false) }))

	assert.Contains(t, out, "ALTER TABLE users ADD COLUMN email INT", "%s", out)
	assert.NotContains(t, out, "No rollback", "%s", out)
}
