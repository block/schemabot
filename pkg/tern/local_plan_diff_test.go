package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/statement"
)

// PlanDiff is the read-only producer for review-time drift detection: it must
// return the deployment's desired-vs-live changes without persisting a plan and
// without a plan_id, so its result can never be mistaken for an applyable plan.
func TestPlanDiff_ReturnsChangesWithoutPersisting(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailPlan())

	resp, err := c.PlanDiff(t.Context(), &ternv1.PlanRequest{Database: "testapp"})
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Nil(t, store.created, "PlanDiff must not persist a plan")
	require.Len(t, resp.Changes, 1)
	assert.Equal(t, "testapp", resp.Changes[0].Namespace)
	require.Len(t, resp.Changes[0].TableChanges, 1)
	tc := resp.Changes[0].TableChanges[0]
	assert.Equal(t, "users", tc.TableName)
	assert.Equal(t, ternv1.ChangeType_CHANGE_TYPE_ALTER, tc.ChangeType)
	assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", tc.Ddl)
}

// A sharded engine emits one change per shard; PlanDiff must surface per-shard
// membership so the rollup can compare shard-scoped drift.
func TestPlanDiff_SurfacesPerShardMembership(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, shardedAlterPlan())

	resp, err := c.PlanDiff(t.Context(), &ternv1.PlanRequest{Database: "testapp"})
	require.NoError(t, err)

	assert.Nil(t, store.created)
	require.Len(t, resp.Shards, 2)
	byShard := map[string]*ternv1.ShardPlan{}
	for _, sp := range resp.Shards {
		byShard[sp.Shard] = sp
	}
	require.Contains(t, byShard, "-80")
	require.Contains(t, byShard, "80-")
	require.Len(t, byShard["-80"].Changes, 1)
	assert.Equal(t, "ALTER TABLE `users` ADD INDEX (`created_at`)", byShard["-80"].Changes[0].Ddl)
	assert.Equal(t, "ALTER TABLE `users` ADD INDEX (`updated_at`)", byShard["80-"].Changes[0].Ddl)
}

// The rollup's primary member comes from Plan and its non-primary members from
// PlanDiff, so both must convert an identical engine result into identical
// change sets — otherwise a deployment that matches the reviewed plan could read
// as drifted purely from a conversion difference.
func TestPlanDiff_MatchesPlanConversion(t *testing.T) {
	for name, result := range map[string]*engine.PlanResult{
		"non-sharded": alterUsersEmailPlan(),
		"sharded":     shardedAlterPlan(),
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakePlanStore{
				getFn:    func(string) (*storage.Plan, error) { return nil, nil },
				createID: 1,
			}
			c := newPlanMaterializeClientWithPlan(store, result)

			planResp, err := c.Plan(t.Context(), &ternv1.PlanRequest{Database: "testapp"})
			require.NoError(t, err)
			diffResp, err := c.PlanDiff(t.Context(), &ternv1.PlanRequest{Database: "testapp"})
			require.NoError(t, err)

			assert.Equal(t, planResp.Engine, diffResp.Engine)
			require.Len(t, diffResp.Changes, len(planResp.Changes))
			for i := range planResp.Changes {
				assert.True(t, proto.Equal(planResp.Changes[i], diffResp.Changes[i]),
					"change %d differs between Plan and PlanDiff", i)
			}
			require.Len(t, diffResp.Shards, len(planResp.Shards))
			for i := range planResp.Shards {
				assert.True(t, proto.Equal(planResp.Shards[i], diffResp.Shards[i]),
					"shard %d differs between Plan and PlanDiff", i)
			}
		})
	}
}

// shardedAlterPlan is a two-shard re-plan where each shard carries its own DDL,
// exercising per-shard membership conversion.
func shardedAlterPlan() *engine.PlanResult {
	return &engine.PlanResult{
		Changes: []engine.SchemaChange{
			{
				Namespace: "testapp",
				Shard:     engine.Shard{Name: "-80"},
				TableChanges: []engine.TableChange{{
					Table:     "users",
					Operation: statement.StatementAlterTable,
					DDL:       "ALTER TABLE `users` ADD INDEX (`created_at`)",
				}},
			},
			{
				Namespace: "testapp",
				Shard:     engine.Shard{Name: "80-"},
				TableChanges: []engine.TableChange{{
					Table:     "users",
					Operation: statement.StatementAlterTable,
					DDL:       "ALTER TABLE `users` ADD INDEX (`updated_at`)",
				}},
			},
		},
	}
}
