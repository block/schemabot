package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// A sharded plan whose shards diverge renders "what applies where": one DDL
// block per distinct change set, each labelled with the shards it applies to.
func TestRenderPlanComment_ShardedDivergent(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "cdb_resolute", Environment: "staging", DatabaseType: "strata",
		Changes: []KeyspaceChangeData{{
			Keyspace:   "cdb_resolute_sharded",
			Statements: []string{"ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"},
			Shards: []KeyspaceShardChange{
				{Shard: "-40", Statements: []string{"ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"}},
				{Shard: "80-", Statements: []string{"ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"}},
				{Shard: "40-80", Statements: []string{"ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`), ADD COLUMN `reason` varchar(255)"}},
			},
		}},
	})

	assert.Contains(t, out, "Shards diverge — what applies where:")
	assert.Contains(t, out, "**shards `-40`, `80-`**", "shards sharing a change are grouped")
	assert.Contains(t, out, "**shard `40-80`**", "the drifted shard is its own group")
	assert.Contains(t, out, "ADD COLUMN `reason`", "the divergent statement is shown")
	assert.Equal(t, 2, strings.Count(out, "```sql"), "one DDL block per group")
}

// A uniform sharded plan (every shard the same change) shows the DDL once with
// no divergence header.
func TestRenderPlanComment_ShardedUniform(t *testing.T) {
	stmt := "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"
	out := RenderPlanComment(PlanCommentData{
		Database: "cdb_resolute", Environment: "staging", DatabaseType: "strata",
		Changes: []KeyspaceChangeData{{
			Keyspace:   "cdb_resolute_sharded",
			Statements: []string{stmt},
			Shards: []KeyspaceShardChange{
				{Shard: "-40", Statements: []string{stmt}},
				{Shard: "80-", Statements: []string{stmt}},
			},
		}},
	})

	assert.NotContains(t, out, "diverge", "a uniform plan is not grouped")
	assert.Equal(t, 1, strings.Count(out, "```sql"), "the shared DDL is shown once")
}
