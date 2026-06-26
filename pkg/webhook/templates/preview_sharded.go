package templates

import (
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
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-a1b2c3d4e5f6", RequestedBy: previewRequestedBy,
		Shards: previewShardStatuses([]presentation.Operation{
			{Deployment: "-40", State: state.ApplyOperation.Running, HaltOnFailure: true},
			{Deployment: "40-80", State: state.ApplyOperation.Pending, HaltOnFailure: true},
			{Deployment: "80-c0", State: state.ApplyOperation.Pending, HaltOnFailure: true},
			{Deployment: "c0-", State: state.ApplyOperation.Pending, HaltOnFailure: true},
		}),
		Cells: []ShardCell{previewMutesCell("-40"), previewMutesCell("40-80"), previewMutesCell("80-c0"), previewMutesCell("c0-")},
	})
}

// PreviewCommentShardedApplyFailed renders a sharded apply where one shard failed
// and the rest halted behind it, with the failed shard's error surfaced.
func PreviewCommentShardedApplyFailed() string {
	return RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "production", Database: "cdb_resolute",
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-a1b2c3d4e5f6", RequestedBy: previewRequestedBy,
		Shards: previewShardStatuses([]presentation.Operation{
			{Deployment: "-40", State: state.ApplyOperation.Failed, HaltOnFailure: true, Error: "resolve shard primary for `-40`: context deadline exceeded"},
			{Deployment: "40-80", State: state.ApplyOperation.Pending, HaltOnFailure: true},
			{Deployment: "80-c0", State: state.ApplyOperation.Pending, HaltOnFailure: true},
			{Deployment: "c0-", State: state.ApplyOperation.Pending, HaltOnFailure: true},
		}),
		Cells: []ShardCell{previewMutesCell("-40"), previewMutesCell("40-80"), previewMutesCell("80-c0"), previewMutesCell("c0-")},
	})
}

// PreviewCommentShardedApplyDivergent renders a sharded apply whose shards
// diverged (one shard's combined ALTER also adds a column), grouped by change.
func PreviewCommentShardedApplyDivergent() string {
	return RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "production", Database: "cdb_resolute",
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-a1b2c3d4e5f6", RequestedBy: previewRequestedBy,
		Shards: previewShardStatuses([]presentation.Operation{
			{Deployment: "-40", State: state.ApplyOperation.Running, HaltOnFailure: true},
			{Deployment: "40-80", State: state.ApplyOperation.Pending, HaltOnFailure: true},
			{Deployment: "80-c0", State: state.ApplyOperation.Pending, HaltOnFailure: true},
		}),
		Cells: []ShardCell{
			previewMutesCell("-40"),
			{Shard: "40-80", Table: "mutes", DDL: previewMutesIndexDrift},
			previewMutesCell("80-c0"),
		},
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
		UnsafeChanges:    []UnsafeChangeData{{Table: "mutes", Reason: "DROP COLUMN removes data and is irreversible", Shards: []string{"40-80"}}},
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
