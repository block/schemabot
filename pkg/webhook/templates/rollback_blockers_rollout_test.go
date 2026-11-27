package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A rollout of two targets is refused rollback whatever either target saved,
// so its plan comment does not name a table as the reason an apply cannot be
// rolled back, even when the primary target's plan carries one.
func TestPlanComment_RolloutNamesNoRollbackBlockers(t *testing.T) {
	changes := []KeyspaceChangeData{{Keyspace: "public", Statements: []string{"ALTER TABLE users ADD COLUMN email text"}}}
	data := PlanCommentData{
		Database: "orders", Environment: "staging", DatabaseType: "postgres",
		Changes: changes,
		RollbackBlockers: []RollbackBlockersData{{Namespace: "public", Tables: []RollbackBlockerData{{
			Table: "orders", Reason: "holds a foreign key added NOT VALID, which a schema file cannot declare",
		}}}},
		DeploymentDrift: &DeploymentDriftData{
			Computed: true, Clean: true, Independent: true,
			Deployments: []DeploymentDriftEntry{
				{Deployment: "shop", Target: "orders_a", Primary: true, Class: "planned"},
				{Deployment: "shop", Target: "orders_b", Class: "match"},
			},
			Plans: []DeploymentPlanGroup{{Members: []string{"shop/orders_a", "shop/orders_b"}, Primary: true, Changes: changes}},
		},
	}

	comment := RenderPlanComment(data)

	assert.Contains(t, comment, "ALTER TABLE users ADD COLUMN email text")
	assert.NotContains(t, comment, "No rollback")
}
