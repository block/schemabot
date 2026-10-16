//go:build e2e

package k8s

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/e2e/testutil"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
)

// mixedTable is one table of a schema that one shard already holds an older
// version of and the newer shards do not hold at all.
type mixedTable struct {
	name string
	v1   string
	v2   string
}

// timestampTables carries TIMESTAMP columns, which the linter flags as unsafe,
// and each v2 adds a column under a unique index, which a shard holding v1 rows
// has to copy to build.
var timestampTables = []mixedTable{
	{
		name: "stations",
		v1:   "CREATE TABLE `stations` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		v2:   "CREATE TABLE `stations` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n  `station_token` varchar(64) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `idx_station_token` (`station_token`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	},
	{
		name: "bikes",
		v1:   "CREATE TABLE `bikes` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `bike_number` varchar(20) NOT NULL,\n  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `idx_bike_number` (`bike_number`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		v2:   "CREATE TABLE `bikes` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `bike_number` varchar(20) NOT NULL,\n  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n  `bike_token` varchar(64) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `idx_bike_number` (`bike_number`),\n  UNIQUE KEY `idx_bike_token` (`bike_token`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	},
	{
		name: "members",
		v1:   "CREATE TABLE `members` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `idx_email` (`email`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		v2:   "CREATE TABLE `members` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `email` varchar(255) NOT NULL,\n  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n  `member_token` varchar(64) DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `idx_email` (`email`),\n  UNIQUE KEY `idx_member_token` (`member_token`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	},
}

// mixedTables is timestampTables with DATETIME columns, which the linter
// does not flag, so only the unique index additions on the altered shard are
// unsafe.
var mixedTables = withDatetimeColumns(timestampTables)

func withDatetimeColumns(tables []mixedTable) []mixedTable {
	out := make([]mixedTable, len(tables))
	for i, tbl := range tables {
		out[i] = mixedTable{
			name: tbl.name,
			v1:   strings.ReplaceAll(tbl.v1, "` timestamp ", "` datetime "),
			v2:   strings.ReplaceAll(tbl.v2, "` timestamp ", "` datetime "),
		}
	}
	return out
}

// mixedFiles returns the desired schema: every table at v2.
func mixedFiles(tables []mixedTable) map[string]string {
	files := make(map[string]string, len(tables))
	for _, tbl := range tables {
		files[tbl.name] = tbl.v2
	}
	return files
}

// seedMixedShards puts every table at v1 with rows on shard 1 and leaves the
// other shards empty.
func seedMixedShards(t *testing.T, dsns []string, tables []mixedTable) {
	t.Helper()
	for _, tbl := range tables {
		execOn(t, dsns[:1], tbl.v1)
	}
	for i := range 50 {
		execOn(t, dsns[:1],
			fmt.Sprintf("INSERT INTO stations (name) VALUES ('station-%d')", i),
			fmt.Sprintf("INSERT INTO bikes (bike_number) VALUES ('bike-%d')", i),
			fmt.Sprintf("INSERT INTO members (email) VALUES ('member-%d@example.com')", i))
	}
}

// A database is being split across shards: shard 1 is the original and holds
// every table at an older version with rows in it, while shards 2 and 3 are
// new and hold no tables yet. One apply runs both kinds of plan in the same
// rollout, table by table: shard 1 alters each table to add a column under a
// unique index, and the new shards create it. Every shard ends on the same
// schema, and a re-plan finds nothing left to change anywhere.
func TestK8sMultiTarget_MixedCreateAndAlterRolloutCompletes(t *testing.T) {
	ep := testutil.Endpoint(t)
	dsns := shardDSNs(t, mtShardApp)
	resetSchema(t, dsns)
	cleanupRolloutState(t)
	seedMixedShards(t, dsns, mixedTables)

	files := shardFiles(mtShardApp, mixedFiles(mixedTables))
	plan := planRollout(t, ep, mtShardApp, files)
	require.NotNil(t, plan.Rollout, "a rollout whose shards hold different schemas renders each one's plan")
	applyID := applyRollout(t, ep, plan.PlanID, map[string]string{"allow_unsafe": "true"})
	p := requireCompleted(t, ep, applyID)
	t.Log(describeProgress(p))

	steps := operationsByStep(p)
	assert.Len(t, steps, len(mixedTables), "one rollout step per table")
	for step, ops := range steps {
		assert.Len(t, ops, len(mtTargets), "step %d runs on every shard", step)
		for _, op := range ops {
			assert.True(t, state.IsState(op.State, state.ApplyOperation.Completed), "step %d on %s completed, got %s", step, op.Target, op.State)
		}
	}
	tokens := map[string]string{"stations": "station_token", "bikes": "bike_token", "members": "member_token"}
	for i, dsn := range dsns {
		for _, tbl := range mixedTables {
			assert.True(t, testutil.ColumnExists(t, dsn, tbl.name, tokens[tbl.name]), "%s.%s on %s", tbl.name, tokens[tbl.name], mtTargets[i])
			assert.True(t, indexOn(t, dsn, tbl.name, "idx_"+tokens[tbl.name]), "%s.idx_%s on %s", tbl.name, tokens[tbl.name], mtTargets[i])
			assert.Empty(t, shadowTables(t, dsn, tbl.name), "no shadow copy of %s left on %s", tbl.name, mtTargets[i])
		}
	}
	var stationRows int
	require.NoError(t, testutil.OpenMySQL(t, dsns[0]).QueryRowContext(t.Context(), "SELECT COUNT(*) FROM stations").Scan(&stationRows))
	assert.Equal(t, 50, stationRows, "shard 1 keeps its rows through the copy")

	again := planRollout(t, ep, mtShardApp, files)
	assert.Empty(t, again.Changes, "primary has nothing left to change")
	if again.Rollout != nil {
		for _, g := range again.Rollout.Groups {
			assert.Empty(t, g.Changes, "group %v has nothing left to change", g.Members)
		}
	}
}

// The same split, but the new shards' CREATE statements carry TIMESTAMP
// columns the linter flags as unsafe, which the original shard's ALTERs do not
// carry. A rollout-wide apply through the API was shown only the original
// shard's plan, so its unsafe opt-in does not reach the new shards' changes:
// the plan lists each new shard as needing its own apply, and the apply is
// refused naming the shard and the table before anything runs.
func TestK8sMultiTarget_UndisclosedMemberUnsafeIsRefused(t *testing.T) {
	ep := testutil.Endpoint(t)
	dsns := shardDSNs(t, mtShardApp)
	resetSchema(t, dsns)
	cleanupRolloutState(t)
	seedMixedShards(t, dsns, timestampTables)

	plan := planRollout(t, ep, mtShardApp, shardFiles(mtShardApp, mixedFiles(timestampTables)))
	require.NotNil(t, plan.Rollout, "a rollout whose shards hold different schemas renders each one's plan")
	refused := map[string]string{}
	for _, r := range plan.Rollout.Refused {
		assert.Equal(t, apitypes.PlanMemberNeedsTarget, r.Reason, "%s is refused as needing its own apply", r.Member)
		refused[r.Target] = r.Detail
	}
	for _, target := range mtTargets[1:] {
		assert.Contains(t, refused[target], "carries an unsafe change for table", "the plan names %s as needing its own apply", target)
	}
	assert.NotContains(t, refused, mtTargets[0], "the primary target's own plan is the one the caller was shown")

	_, err := client.CallApplyAPIForTarget(ep, plan.PlanID, mtEnvironment, "", "", true, map[string]string{"allow_unsafe": "true"})
	require.Error(t, err, "a rollout-wide apply of member changes its caller was never shown is refused")
	assert.Contains(t, err.Error(), "carries an unsafe change for table")
	assert.Contains(t, err.Error(), "never named it and no opt-in covers it")
	for i, dsn := range dsns[1:] {
		for _, tbl := range timestampTables {
			assert.False(t, tableOn(t, dsn, tbl.name), "nothing ran on %s: %s was not created", mtTargets[i+1], tbl.name)
		}
	}
}
