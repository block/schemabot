package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func TestRenderPlanComment_HeldBackDropNamesOwnerAndWithholdsApply(t *testing.T) {
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

	assert.Contains(t, rendered, "🛑 **Held back**: **1** drop that may not be this PR's to make")
	assert.Contains(t, rendered, "[block/schemabot#42](https://github.com/block/schemabot/pull/42)")
	assert.NotContains(t, rendered, "▶️ **To apply**")
	assert.Contains(t, rendered, "No apply command is offered")
	require.True(t, data.ApplyPromptWithheld())
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
	assert.NotContains(t, rendered, "▶️ **To apply**")
}

func TestRenderMultiEnvPlanComment_HeldBackDropInOneEnvironmentWithholdsEverySequenceStep(t *testing.T) {
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

	assert.Contains(t, rendered, "🛑 **Held back**")
	assert.NotContains(t, rendered, "▶️ **To apply**")
	assert.Contains(t, rendered, "No apply command is offered")
}
