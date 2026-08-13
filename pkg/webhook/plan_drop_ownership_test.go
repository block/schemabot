package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/webhook/templates"
)

func TestPlannedDropTables_CollectsDropsFromBothViews(t *testing.T) {
	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace: "keyspace",
			TableChanges: []*apitypes.TableChangeResponse{
				{TableName: "users", ChangeType: "alter"},
				{TableName: "orders", ChangeType: "drop"},
			},
		}},
		Shards: []*apitypes.ShardPlanResponse{{
			Namespace: "keyspace",
			Shard:     "-80",
			Changes: []*apitypes.TableChangeResponse{
				{TableName: "audit_log", ChangeType: "drop"},
				{TableName: "orders", ChangeType: "drop"},
			},
		}},
	}

	assert.Equal(t, []string{"audit_log", "orders"}, plannedDropTables(planResp))
}

func TestPlannedDropTables_NoDrops(t *testing.T) {
	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace:    "keyspace",
			TableChanges: []*apitypes.TableChangeResponse{{TableName: "users", ChangeType: "create"}},
		}},
	}

	assert.Empty(t, plannedDropTables(planResp))
	assert.Empty(t, plannedDropTables(nil))
}

func TestRenderPlanComment_AttributedDropNamesOwnerAndStillOffersApply(t *testing.T) {
	data := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "staging",
		IsMySQL:     true,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"DROP TABLE `reconcile_state`"},
		}},
		OwnedDrops: []templates.OwnedDropData{{
			Table:       "reconcile_state",
			Repository:  "block/schemabot",
			PullRequest: 42,
		}},
	}

	rendered := templates.RenderPlanComment(data)

	assert.Contains(t, rendered, "🛑 **Check before applying**: **1** drop attributed to another open PR")
	assert.Contains(t, rendered, "[block/schemabot#42](https://github.com/block/schemabot/pull/42)")
	// Reconciling the live database to the declared schema stays the operator's
	// call, so the attribution informs the decision without removing it.
	assert.Contains(t, rendered, "▶️ **To apply**")
	assert.Contains(t, rendered, "schemabot apply -e staging")
}

func TestRenderPlanComment_UnresolvedDropOwnershipReadsAsUnresolved(t *testing.T) {
	data := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "staging",
		IsMySQL:     true,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"DROP TABLE `reconcile_state`"},
		}},
		OwnedDrops: []templates.OwnedDropData{{Table: "reconcile_state", Unresolved: true}},
	}

	rendered := templates.RenderPlanComment(data)

	assert.Contains(t, rendered, "ownership could not be established; see server logs")
	assert.Contains(t, rendered, "▶️ **To apply**")
}

func TestRenderMultiEnvPlanComment_AttributedDropAnnotatesItsOwnEnvironmentOnly(t *testing.T) {
	staging := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "staging",
		IsMySQL:     true,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"DROP TABLE `reconcile_state`"},
		}},
		OwnedDrops: []templates.OwnedDropData{{
			Table:       "reconcile_state",
			Repository:  "block/schemabot",
			PullRequest: 42,
		}},
	}
	production := templates.PlanCommentData{
		Database:    "testdb",
		Environment: "production",
		IsMySQL:     true,
		Changes: []templates.KeyspaceChangeData{{
			Keyspace:   "testdb",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
	}

	rendered := templates.RenderMultiEnvPlanComment(templates.MultiEnvPlanCommentData{
		Database:     "testdb",
		DatabaseType: "mysql",
		IsMySQL:      true,
		Environments: []string{"staging", "production"},
		Plans: map[string]*templates.PlanCommentData{
			"staging":    &staging,
			"production": &production,
		},
		Errors: map[string]string{},
	})

	assert.Contains(t, rendered, "🛑 **Check before applying**")
	// The sequence walks the environments in order and is offered in full: the
	// attribution belongs to staging's drop, not to production's unrelated add.
	assert.Contains(t, rendered, "▶️ **To apply**")
	assert.Contains(t, rendered, "schemabot apply -e staging")
}
