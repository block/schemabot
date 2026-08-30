package templates

import (
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
)

// previewShardStatuses derives per-shard statuses from sample operations using
// the shard name as the operation identity — the same projection the webhook
// uses — so the preview's emoji/labels match production rendering.
func previewShardStatuses(ops []presentation.Operation) []ShardStatus {
	derived := presentation.Derive(ops).Deployments
	out := make([]ShardStatus, 0, len(derived))
	for _, d := range derived {
		out = append(out, ShardStatus{Shard: d.Deployment, Emoji: d.Emoji, Label: d.Label, State: d.State, Error: d.Error})
	}
	return out
}

const (
	previewMutesIndex      = "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`);"
	previewMutesIndexDrift = "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`), ADD COLUMN `reason` varchar(255);"
)

func previewMutesCell(shard string) ShardCell {
	return ShardCell{Shard: shard, Table: "mutes", DDL: previewMutesIndex}
}

// PreviewCommentShardedApplyInProgress renders a sharded apply mid-rollout: the
// first shard copying, the rest gated behind it.
func PreviewCommentShardedApplyInProgress() string {
	return RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "production", Database: "cdb_resolute",
		ApplyID: "apply-a1b2c3d4e5f6", RequestedBy: previewRequestedBy,
		Keyspaces: []ShardedKeyspace{{
			Keyspace: "cdb_resolute_sharded",
			Shards: previewShardStatuses([]presentation.Operation{
				{Deployment: "-40", State: state.ApplyOperation.Running},
				{Deployment: "40-80", State: state.ApplyOperation.Pending},
				{Deployment: "80-c0", State: state.ApplyOperation.Pending},
				{Deployment: "c0-", State: state.ApplyOperation.Pending},
			}),
			Cells: []ShardCell{previewMutesCell("-40"), previewMutesCell("40-80"), previewMutesCell("80-c0"), previewMutesCell("c0-")},
		}},
	})
}

// PreviewCommentShardedApplyFailed renders a sharded apply where one shard failed
// and the rest halted behind it, with the failed shard's error surfaced.
func PreviewCommentShardedApplyFailed() string {
	return RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "production", Database: "cdb_resolute",
		ApplyID: "apply-a1b2c3d4e5f6", RequestedBy: previewRequestedBy,
		Keyspaces: []ShardedKeyspace{{
			Keyspace: "cdb_resolute_sharded",
			Shards: previewShardStatuses([]presentation.Operation{
				{Deployment: "-40", State: state.ApplyOperation.Failed, Error: "resolve shard primary for `-40`: context deadline exceeded"},
				{Deployment: "40-80", State: state.ApplyOperation.Pending},
				{Deployment: "80-c0", State: state.ApplyOperation.Pending},
				{Deployment: "c0-", State: state.ApplyOperation.Pending},
			}),
			Cells: []ShardCell{previewMutesCell("-40"), previewMutesCell("40-80"), previewMutesCell("80-c0"), previewMutesCell("c0-")},
		}},
	})
}

// PreviewCommentShardedSummaryCompleted renders the terminal summary for a
// sharded apply whose shards all completed alongside a keyspace VSchema
// update: the applied verdict header and outcome line over the final
// per-shard results, with the applied VSchema change in its own section.
func PreviewCommentShardedSummaryCompleted() string {
	return RenderShardedApplySummaryComment(ShardedApplyData{
		State: state.Apply.Completed, Environment: "production", Database: "cdb_resolute",
		ApplyID:     "apply-a1b2c3d4e5f6",
		RequestedBy: previewRequestedBy,
		StartedAt:   sampleTime().Add(-30 * time.Minute).UTC().Format(time.RFC3339),
		CompletedAt: sampleTime().Add(-2 * time.Minute).UTC().Format(time.RFC3339),
		Keyspaces: []ShardedKeyspace{{
			Keyspace: "cdb_resolute_sharded",
			Shards: previewShardStatuses([]presentation.Operation{
				{Deployment: "-40", State: state.ApplyOperation.Completed},
				{Deployment: "40-80", State: state.ApplyOperation.Completed},
				{Deployment: "80-c0", State: state.ApplyOperation.Completed},
				{Deployment: "c0-", State: state.ApplyOperation.Completed},
			}),
			Cells: []ShardCell{previewMutesCell("-40"), previewMutesCell("40-80"), previewMutesCell("80-c0"), previewMutesCell("c0-")},
		}},
		VSchemaChanges: []apitypes.VSchemaChange{{
			Namespace: "cdb_resolute_sharded",
			Status:    "applied",
			Diff: `--- current
+++ new
@@ -3,6 +3,11 @@
   "tables": {
     "mutes": {
+      "column_vindexes": [
+        {"column": "target_id", "name": "hash"}
+      ]
     }
   }
 }`,
		}},
	})
}

// PreviewCommentShardedSummaryFailed renders the terminal summary for a sharded
// apply where one shard failed and the rest halted behind it: the failed
// verdict header, the surfaced error, the final per-shard results, the
// keyspace's VSchema change that will now never run (Cancelled, not Pending),
// and the retry action.
func PreviewCommentShardedSummaryFailed() string {
	return RenderShardedApplySummaryComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "production", Database: "cdb_resolute",
		ApplyID:     "apply-a1b2c3d4e5f6",
		RequestedBy: previewRequestedBy,
		StartedAt:   sampleTime().Add(-30 * time.Minute).UTC().Format(time.RFC3339),
		CompletedAt: sampleTime().Add(-2 * time.Minute).UTC().Format(time.RFC3339),
		Keyspaces: []ShardedKeyspace{{
			Keyspace: "cdb_resolute_sharded",
			Shards: previewShardStatuses([]presentation.Operation{
				{Deployment: "-40", State: state.ApplyOperation.Failed, Error: "resolve shard primary for `-40`: context deadline exceeded"},
				{Deployment: "40-80", State: state.ApplyOperation.Pending},
				{Deployment: "80-c0", State: state.ApplyOperation.Pending},
				{Deployment: "c0-", State: state.ApplyOperation.Pending},
			}),
			Cells: []ShardCell{previewMutesCell("-40"), previewMutesCell("40-80"), previewMutesCell("80-c0"), previewMutesCell("c0-")},
		}},
		VSchemaChanges: []apitypes.VSchemaChange{{Namespace: "cdb_resolute_sharded", Status: "cancelled"}},
	})
}

// PreviewCommentShardedApplyDivergent renders a sharded apply whose shards
// diverged (one shard's combined ALTER also adds a column), grouped by change.
func PreviewCommentShardedApplyDivergent() string {
	return RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "production", Database: "cdb_resolute",
		ApplyID: "apply-a1b2c3d4e5f6", RequestedBy: previewRequestedBy,
		Keyspaces: []ShardedKeyspace{{
			Keyspace: "cdb_resolute_sharded",
			Shards: previewShardStatuses([]presentation.Operation{
				{Deployment: "-40", State: state.ApplyOperation.Running},
				{Deployment: "40-80", State: state.ApplyOperation.Pending},
				{Deployment: "80-c0", State: state.ApplyOperation.Pending},
			}),
			Cells: []ShardCell{
				previewMutesCell("-40"),
				{Shard: "40-80", Table: "mutes", DDL: previewMutesIndexDrift},
				previewMutesCell("80-c0"),
			},
		}},
	})
}

// PreviewCommentShardedApplyMultiKeyspace renders a sharded apply spanning
// several keyspaces in one deployment — the shape a database with unsharded
// sibling keyspaces produces, where every unsharded keyspace contributes a
// single "-" shard. Each keyspace renders its own section while the histogram
// and rollout ordering span all of them, and the sharded keyspace's VSchema
// change renders in the shared VSchema section.
func PreviewCommentShardedApplyMultiKeyspace() string {
	shards := previewShardStatuses([]presentation.Operation{
		{Deployment: "cdb_resolute/-", State: state.ApplyOperation.Completed},
		{Deployment: "cdb_resolute_lookup/-", State: state.ApplyOperation.Running},
		{Deployment: "cdb_resolute_sharded/-40", State: state.ApplyOperation.Pending},
		{Deployment: "cdb_resolute_sharded/40-80", State: state.ApplyOperation.Pending},
		{Deployment: "cdb_resolute_sharded/80-c0", State: state.ApplyOperation.Pending},
		{Deployment: "cdb_resolute_sharded/c0-", State: state.ApplyOperation.Pending},
	})
	unshard := func(s ShardStatus, shard string) ShardStatus {
		s.Shard = shard
		return s
	}
	return RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "production", Database: "cdb_resolute",
		ApplyID: "apply-a1b2c3d4e5f6", RequestedBy: previewRequestedBy,
		Keyspaces: []ShardedKeyspace{
			{
				Keyspace: "cdb_resolute",
				Shards:   []ShardStatus{unshard(shards[0], "-")},
				Cells:    []ShardCell{{Shard: "-", Table: "outcomes", DDL: "ALTER TABLE `outcomes` ADD COLUMN `verdict` varchar(32);"}},
			},
			{
				Keyspace: "cdb_resolute_lookup",
				Shards:   []ShardStatus{unshard(shards[1], "-")},
				Cells:    []ShardCell{{Shard: "-", Table: "outcomes_lookup", DDL: "ALTER TABLE `outcomes_lookup` ADD COLUMN `verdict` varchar(32);"}},
			},
			{
				Keyspace: "cdb_resolute_sharded",
				Shards: []ShardStatus{
					unshard(shards[2], "-40"),
					unshard(shards[3], "40-80"),
					unshard(shards[4], "80-c0"),
					unshard(shards[5], "c0-"),
				},
				Cells: []ShardCell{previewMutesCell("-40"), previewMutesCell("40-80"), previewMutesCell("80-c0"), previewMutesCell("c0-")},
			},
		},
		VSchemaChanges: []apitypes.VSchemaChange{{Namespace: "cdb_resolute_sharded", Status: ""}},
	})
}

// PreviewCommentShardedPlanDivergent renders a sharded plan whose shards diverge,
// showing "what applies where".
func PreviewCommentShardedPlanDivergent() string {
	idx := "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"
	drift := "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`), ADD COLUMN `reason` varchar(255)"
	return RenderPlanComment(PlanCommentData{
		Database: "cdb_resolute", Environment: "production", DatabaseType: "strata",
		HeadSHA: previewHeadSHA, Repository: previewRepository, RequestedBy: previewRequestedBy,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "cdb_resolute_sharded",
			Statements: []string{idx},
			Shards: []KeyspaceShardChange{
				{Shard: "-40", Statements: []string{idx}},
				{Shard: "80-c0", Statements: []string{idx}},
				{Shard: "c0-", Statements: []string{idx}},
				{Shard: "40-80", Statements: []string{drift}},
			},
		}},
	})
}

// PreviewCommentShardedPlanManyShards renders a uniform sharded plan across a
// wide keyspace: the DDL shows once under an "all N shards" heading, with the
// shard names behind a collapsed block instead of walling the comment.
func PreviewCommentShardedPlanManyShards() string {
	idx := "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"
	shards := make([]KeyspaceShardChange, 0, 32)
	for i := range 32 {
		shards = append(shards, KeyspaceShardChange{Shard: previewShardRange(i, 32), Statements: []string{idx}})
	}
	return RenderPlanComment(PlanCommentData{
		Database: "cdb_resolute", Environment: "production", DatabaseType: "strata",
		HeadSHA: previewHeadSHA, Repository: previewRepository, RequestedBy: previewRequestedBy,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "cdb_resolute_sharded",
			Statements: []string{idx},
			Shards:     shards,
		}},
	})
}

// previewShardRange returns shard i's keyrange name in an evenly-split
// keyspace of n shards, in Vitess notation: "-08", "08-10", …, "f8-".
func previewShardRange(i, n int) string {
	width := 256 / n
	lower := fmt.Sprintf("%02x", i*width)
	upper := fmt.Sprintf("%02x", (i+1)*width)
	switch i {
	case 0:
		return "-" + upper
	case n - 1:
		return lower + "-"
	default:
		return lower + "-" + upper
	}
}

// PreviewCommentShardedPlanPartiallyApplied renders a sharded plan where one
// shard already has the change (e.g. an interrupted earlier rollout) and the
// rest still need it. The satisfied shard renders as an "already applied" group
// so the partially-applied keyspace shows its divergent state.
func PreviewCommentShardedPlanPartiallyApplied() string {
	idx := "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"
	return RenderPlanComment(PlanCommentData{
		Database: "cdb_resolute", Environment: "production", DatabaseType: "strata",
		HeadSHA: previewHeadSHA, Repository: previewRepository, RequestedBy: previewRequestedBy,
		Changes: []KeyspaceChangeData{{
			Keyspace: "cdb_resolute_sharded",
			Shards: []KeyspaceShardChange{
				{Shard: "-40", Satisfied: true},
				{Shard: "40-80", Statements: []string{idx}},
				{Shard: "80-c0", Statements: []string{idx}},
				{Shard: "c0-", Statements: []string{idx}},
			},
		}},
	})
}

// PreviewCommentShardedPlanUnsafe renders a sharded plan where one shard's
// combined ALTER drops a column (unsafe), flagged with the shard.
func PreviewCommentShardedPlanUnsafe() string {
	idx := "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"
	drop := "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`), DROP COLUMN `legacy_reason`"
	return RenderPlanComment(PlanCommentData{
		Database: "cdb_resolute", Environment: "production", DatabaseType: "strata",
		HeadSHA: previewHeadSHA, Repository: previewRepository, RequestedBy: previewRequestedBy,
		HasUnsafeChanges: true,
		UnsafeChanges:    []UnsafeChangeData{{Table: "mutes", Reason: "DROP COLUMN removes data and is irreversible", Shards: []string{"40-80"}, TotalShards: 4}},
		Changes: []KeyspaceChangeData{{
			Keyspace: "cdb_resolute_sharded",
			Shards: []KeyspaceShardChange{
				{Shard: "-40", Statements: []string{idx}},
				{Shard: "80-c0", Statements: []string{idx}},
				{Shard: "c0-", Statements: []string{idx}},
				{Shard: "40-80", Statements: []string{drop}},
			},
		}},
	})
}
