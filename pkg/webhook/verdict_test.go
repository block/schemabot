package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
)

// A change the planner's execution-mode verdict marks blocked is surfaced in
// the plan comment's blocked section, naming the table and the engine's
// reason, so the operator learns at plan time that the apply will fail.
func TestBuildPlanCommentData_BlockedChanges(t *testing.T) {
	schema := &ghclient.SchemaRequestResult{Database: "testapp", Type: "mysql"}
	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace: "testapp",
			TableChanges: []*apitypes.TableChangeResponse{
				{TableName: "users", DDL: "ALTER TABLE `users` DROP PRIMARY KEY", ChangeType: "alter",
					ExecutionMode: "blocked", ModeReason: "dropping primary key is not supported"},
				{TableName: "orders", DDL: "ALTER TABLE `orders` ADD COLUMN `notes` TEXT", ChangeType: "alter"},
			},
		}},
	}

	data := buildPlanCommentData(schema, planResp, "staging", "", "testuser", "")

	require.Len(t, data.BlockedChanges, 1)
	assert.Equal(t, "users", data.BlockedChanges[0].Table)
	assert.Equal(t, "dropping primary key is not supported", data.BlockedChanges[0].Reason)
	assert.Empty(t, data.BlockedChanges[0].Shards)
}

// A blocked change on a sharded plan is derived per shard and grouped by
// (table, reason), so a refused statement fanned out to several shards lists
// them together instead of repeating.
func TestBuildPlanCommentData_PerShardBlocked(t *testing.T) {
	schema := &ghclient.SchemaRequestResult{Database: "cdb_resolute", Type: "strata"}
	const dropPK = "ALTER TABLE `mutes` DROP PRIMARY KEY"
	blockedChange := func() *apitypes.TableChangeResponse {
		return &apitypes.TableChangeResponse{
			TableName: "mutes", DDL: dropPK, ChangeType: "alter",
			ExecutionMode: "blocked", ModeReason: "dropping primary key is not supported",
		}
	}
	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace:    "cdb_resolute_sharded",
			TableChanges: []*apitypes.TableChangeResponse{blockedChange()},
		}},
		Shards: []*apitypes.ShardPlanResponse{
			{Namespace: "cdb_resolute_sharded", Shard: "-40", Changes: []*apitypes.TableChangeResponse{blockedChange()}},
			{Namespace: "cdb_resolute_sharded", Shard: "40-80", Changes: []*apitypes.TableChangeResponse{blockedChange()}},
		},
	}

	data := buildPlanCommentData(schema, planResp, "staging", "", "testuser", "")

	require.Len(t, data.BlockedChanges, 1, "the same refused statement on two shards is grouped, not repeated")
	assert.Equal(t, "mutes", data.BlockedChanges[0].Table)
	assert.Equal(t, []string{"-40", "40-80"}, data.BlockedChanges[0].Shards)
}

// A plan with no blocked verdicts renders no blocked section.
func TestBuildPlanCommentData_NoBlockedChanges(t *testing.T) {
	schema := &ghclient.SchemaRequestResult{Database: "testapp", Type: "mysql"}
	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace: "testapp",
			TableChanges: []*apitypes.TableChangeResponse{
				{TableName: "orders", DDL: "ALTER TABLE `orders` ADD COLUMN `notes` TEXT", ChangeType: "alter"},
			},
		}},
	}

	data := buildPlanCommentData(schema, planResp, "staging", "", "testuser", "")

	assert.Empty(t, data.BlockedChanges)
}

// A change the planner's execution-mode verdict routes to direct execution is
// surfaced in the plan comment's direct section, naming the table and the
// planner's reason, so the operator sees the disclosure before confirming.
func TestBuildPlanCommentData_DirectChanges(t *testing.T) {
	schema := &ghclient.SchemaRequestResult{Database: "testapp", Type: "mysql"}
	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace: "testapp",
			TableChanges: []*apitypes.TableChangeResponse{
				{TableName: "users", DDL: "ALTER TABLE `users` DROP PRIMARY KEY", ChangeType: "alter",
					ExecutionMode: "direct", ModeReason: "dropping primary key is not supported; runs as native MySQL DDL on a table with ~40 rows"},
				{TableName: "orders", DDL: "ALTER TABLE `orders` ADD COLUMN `notes` TEXT", ChangeType: "alter"},
			},
		}},
	}

	data := buildPlanCommentData(schema, planResp, "staging", "", "testuser", "")

	require.Len(t, data.DirectChanges, 1)
	assert.Equal(t, "users", data.DirectChanges[0].Table)
	assert.Equal(t, "dropping primary key is not supported; runs as native MySQL DDL on a table with ~40 rows", data.DirectChanges[0].Reason)
	assert.Empty(t, data.DirectChanges[0].Shards)
	assert.Empty(t, data.BlockedChanges, "a direct verdict is not a block")
}

// A direct change on a sharded plan is derived per shard and grouped by
// (table, reason), so a statement fanned out to several shards lists them
// together instead of repeating.
func TestBuildPlanCommentData_PerShardDirect(t *testing.T) {
	schema := &ghclient.SchemaRequestResult{Database: "testapp", Type: "strata"}
	const dropPK = "ALTER TABLE `mutes` DROP PRIMARY KEY"
	directChange := func() *apitypes.TableChangeResponse {
		return &apitypes.TableChangeResponse{
			TableName: "mutes", DDL: dropPK, ChangeType: "alter",
			ExecutionMode: "direct", ModeReason: "dropping primary key is not supported; runs as native MySQL DDL on a table with ~40 rows",
		}
	}
	planResp := &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{{
			Namespace:    "testapp_sharded",
			TableChanges: []*apitypes.TableChangeResponse{directChange()},
		}},
		Shards: []*apitypes.ShardPlanResponse{
			{Namespace: "testapp_sharded", Shard: "-40", Changes: []*apitypes.TableChangeResponse{directChange()}},
			{Namespace: "testapp_sharded", Shard: "40-80", Changes: []*apitypes.TableChangeResponse{directChange()}},
		},
	}

	data := buildPlanCommentData(schema, planResp, "staging", "", "testuser", "")

	require.Len(t, data.DirectChanges, 1, "the same direct statement on two shards is grouped, not repeated")
	assert.Equal(t, "mutes", data.DirectChanges[0].Table)
	assert.Equal(t, []string{"-40", "40-80"}, data.DirectChanges[0].Shards)
}
