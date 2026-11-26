package templates

import (
	"fmt"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
)

// previewShardStatuses derives per-shard statuses from sample operations
// through presentation.DeriveShards, the projection the webhook uses, so the
// preview's emoji and labels match production rendering. Each sample names its
// shard in Deployment, qualified as "keyspace/shard" when the preview spans
// keyspaces, and an EndedBy names the shard that ended it the same way. Every
// sample runs on one target, as a sharded apply's shards do.
func previewShardStatuses(applyState string, ops []presentation.Operation) []ShardStatus {
	const target = "default"
	work := make([]presentation.ShardWork, 0, len(ops))
	for _, op := range ops {
		keyspace, shard, ok := strings.Cut(op.Deployment, "/")
		if !ok {
			keyspace, shard = "preview", op.Deployment
		}
		op.OperationKey, op.Deployment = op.Deployment, target
		if op.EndedBy != nil {
			op.EndedBy = &presentation.OperationRef{Deployment: target, OperationKey: op.EndedBy.Deployment}
		}
		work = append(work, presentation.ShardWork{Keyspace: keyspace, Shard: shard, Operations: []presentation.Operation{op}})
	}
	derived := presentation.DeriveShards(applyState, work)
	out := make([]ShardStatus, 0, len(derived))
	for _, d := range derived {
		out = append(out, ShardStatus{
			Shard: d.Shard, Emoji: d.Emoji, Label: d.Label, State: d.State, Error: d.Error,
			StoppedAtCutover: d.Presentation == presentation.StateStoppedAtCutover, EndedBy: d.EndedBy,
			Landed: d.Landed,
		})
	}
	return out
}

const (
	previewMutesIndex      = "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`);"
	previewMutesIndexDrift = "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`), ADD COLUMN `reason` varchar(255);"

	// Copy figures for the mutes fixture's reporting shard. The shard percent
	// is derived from the row figures so the summary line and the rows line
	// cannot drift apart when either is edited.
	previewMutesRowsCopied  = int64(914707)
	previewMutesRowsTotal   = int64(1466232)
	previewMutesCopyPercent = int(previewMutesRowsCopied * 100 / previewMutesRowsTotal)
)

func previewMutesCell(shard string) ShardCell {
	return ShardCell{Shard: shard, Table: "mutes", Statements: []string{previewMutesIndex}}
}

// PreviewCommentShardedApplyInProgress renders a sharded apply mid-rollout: the
// first shard copying, the rest queued until a driver is free.
func PreviewCommentShardedApplyInProgress() string {
	return RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "production", Database: "cdb_resolute",
		ApplyID: "apply-a1b2c3d4e5f6", RequestedBy: previewRequestedBy,
		Keyspaces: []ShardedKeyspace{{
			Keyspace: "cdb_resolute_sharded",
			Tables: []ShardedTableStatus{{
				Table: "mutes", Status: state.Task.Running,
				RowsCopied: previewMutesRowsCopied, RowsTotal: previewMutesRowsTotal, ETASeconds: 195,
				ShardsReporting: 1,
				EstimatedBytes:  new(int64(23_400_000_000)),
				PlannedShards:   4,
				Shards: []ShardProgressData{
					{Shard: "-40", Status: state.Task.Running, PercentComplete: previewMutesCopyPercent},
					{Shard: "40-80", Status: state.Task.Pending},
					{Shard: "80-c0", Status: state.Task.Pending},
					{Shard: "c0-", Status: state.Task.Pending},
				},
			}},
			Shards: previewShardStatuses(state.Apply.Running, []presentation.Operation{
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
// and the rest never started, with the failed shard's error surfaced.
func PreviewCommentShardedApplyFailed() string {
	return RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "production", Database: "cdb_resolute",
		ApplyID: "apply-a1b2c3d4e5f6", RequestedBy: previewRequestedBy,
		Keyspaces: []ShardedKeyspace{{
			Keyspace: "cdb_resolute_sharded",
			Tables: []ShardedTableStatus{{
				Table: "mutes", Status: state.Task.Failed,
				Shards: []ShardProgressData{
					{Shard: "-40", Status: state.Task.Failed},
					{Shard: "40-80", Status: state.Task.Pending},
					{Shard: "80-c0", Status: state.Task.Pending},
					{Shard: "c0-", Status: state.Task.Pending},
				},
			}},
			Shards: previewShardStatuses(state.Apply.Failed, []presentation.Operation{
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
			Tables: []ShardedTableStatus{{
				Table: "mutes", Status: state.Task.Completed,
				Shards: []ShardProgressData{
					{Shard: "-40", Status: state.Task.Completed},
					{Shard: "40-80", Status: state.Task.Completed},
					{Shard: "80-c0", Status: state.Task.Completed},
					{Shard: "c0-", Status: state.Task.Completed},
				},
			}},
			Shards: previewShardStatuses(state.Apply.Completed, []presentation.Operation{
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
			Summary: []string{"table `mutes`: adds vindex `hash` on `target_id`"},
		}},
	})
}

// PreviewCommentShardedSummaryFailed renders the terminal summary for a sharded
// apply where one shard failed and the rest never started: the failed
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
			Tables: []ShardedTableStatus{{
				Table: "mutes", Status: state.Task.Failed,
				Shards: []ShardProgressData{
					{Shard: "-40", Status: state.Task.Failed},
					{Shard: "40-80", Status: state.Task.Pending},
					{Shard: "80-c0", Status: state.Task.Pending},
					{Shard: "c0-", Status: state.Task.Pending},
				},
			}},
			Shards: previewShardStatuses(state.Apply.Failed, []presentation.Operation{
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

// PreviewCommentShardedSummaryFinalizeFailed renders the terminal summary for
// a sharded apply whose shards all completed and whose keyspace finalize then
// failed. The failure callout names the keyspace, since the finalize error
// need not, and the comment lists no other step for the finalize.
func PreviewCommentShardedSummaryFinalizeFailed() string {
	const finalizeErr = "publish routing rules: target is read-only"
	shards := []string{"-40", "40-80", "80-c0", "c0-"}
	progress := make([]ShardProgressData, 0, len(shards))
	ops := make([]presentation.Operation, 0, len(shards))
	cells := make([]ShardCell, 0, len(shards))
	for _, shard := range shards {
		progress = append(progress, ShardProgressData{Shard: shard, Status: state.Task.Completed})
		ops = append(ops, presentation.Operation{Deployment: shard, State: state.ApplyOperation.Completed})
		cells = append(cells, previewMutesCell(shard))
	}
	return RenderShardedApplySummaryComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "production", Database: "shop",
		ApplyID:      "apply-a1b2c3d4e5f6",
		RequestedBy:  previewRequestedBy,
		StartedAt:    sampleTime().Add(-30 * time.Minute).UTC().Format(time.RFC3339),
		CompletedAt:  sampleTime().Add(-2 * time.Minute).UTC().Format(time.RFC3339),
		ErrorMessage: finalizeErr,
		FailedFinalizer: &FinalizerFailure{
			Keyspace: "commerce", State: state.ApplyOperation.Failed, Error: finalizeErr,
		},
		Keyspaces: []ShardedKeyspace{{
			Keyspace: "commerce",
			Tables:   []ShardedTableStatus{{Table: "mutes", Status: state.Task.Completed, Shards: progress}},
			Shards:   previewShardStatuses(state.Apply.Failed, ops),
			Cells:    cells,
		}},
	})
}

// PreviewCommentShardedSummaryStoppedAtCutover renders the terminal summary
// for a sharded apply that deferred its cutover. Shard -40 failed while the
// other shards had copied and waited at the cutover, which could then never
// run for every shard, so SchemaBot stopped them, saved their copies, and
// failed the apply. The first failure names -40 alone, and a note says the
// stopped shards resume on a new apply.
func PreviewCommentShardedSummaryStoppedAtCutover() string {
	endedBy := &presentation.OperationRef{Deployment: "-40"}
	return RenderShardedApplySummaryComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "production", Database: "shop",
		ApplyID:     "apply-a1b2c3d4e5f6",
		RequestedBy: previewRequestedBy,
		StartedAt:   sampleTime().Add(-30 * time.Minute).UTC().Format(time.RFC3339),
		CompletedAt: sampleTime().Add(-2 * time.Minute).UTC().Format(time.RFC3339),
		Keyspaces: []ShardedKeyspace{{
			Keyspace: "shop_sharded",
			Tables: []ShardedTableStatus{{
				Table: "mutes", Status: state.Task.Failed,
				Shards: []ShardProgressData{
					{Shard: "-40", Status: state.Task.Failed},
					{Shard: "40-80", Status: state.Task.Stopped},
					{Shard: "80-c0", Status: state.Task.Stopped},
					{Shard: "c0-", Status: state.Task.Stopped},
				},
			}},
			Shards: previewShardStatuses(state.Apply.Failed, []presentation.Operation{
				{Deployment: "-40", State: state.ApplyOperation.Failed, Barrier: true, Error: "resolve shard primary for `-40`: context deadline exceeded"},
				{Deployment: "40-80", State: state.ApplyOperation.Failed, Barrier: true, EndedBy: endedBy},
				{Deployment: "80-c0", State: state.ApplyOperation.Failed, Barrier: true, EndedBy: endedBy},
				{Deployment: "c0-", State: state.ApplyOperation.Failed, Barrier: true, EndedBy: endedBy},
			}),
			Cells: []ShardCell{previewMutesCell("-40"), previewMutesCell("40-80"), previewMutesCell("80-c0"), previewMutesCell("c0-")},
		}},
	})
}

// PreviewCommentShardedSummaryCancelledPartial renders the terminal summary
// for a sharded apply cancelled after part of the fleet landed: the table line
// states the landed coverage and the divergent outcome promotes the per-shard
// status table, so the outcome record names which shards carry the change and
// which do not.
func PreviewCommentShardedSummaryCancelledPartial() string {
	return RenderShardedApplySummaryComment(ShardedApplyData{
		State: state.Apply.Cancelled, Environment: "production", Database: "cdb_resolute",
		ApplyID:     "apply-a1b2c3d4e5f6",
		RequestedBy: previewRequestedBy,
		StartedAt:   sampleTime().Add(-30 * time.Minute).UTC().Format(time.RFC3339),
		CompletedAt: sampleTime().Add(-2 * time.Minute).UTC().Format(time.RFC3339),
		Keyspaces: []ShardedKeyspace{{
			Keyspace: "cdb_resolute_sharded",
			Tables: []ShardedTableStatus{{
				Table: "mutes", Status: state.Task.Cancelled,
				Shards: []ShardProgressData{
					{Shard: "-40", Status: state.Task.Completed},
					{Shard: "40-80", Status: state.Task.Completed},
					{Shard: "80-c0", Status: state.Task.Cancelled},
					{Shard: "c0-", Status: state.Task.Cancelled},
				},
			}},
			Shards: previewShardStatuses(state.Apply.Cancelled, []presentation.Operation{
				{Deployment: "-40", State: state.ApplyOperation.Completed},
				{Deployment: "40-80", State: state.ApplyOperation.Completed},
				{Deployment: "80-c0", State: state.ApplyOperation.Cancelled},
				{Deployment: "c0-", State: state.ApplyOperation.Cancelled},
			}),
			Cells: []ShardCell{previewMutesCell("-40"), previewMutesCell("40-80"), previewMutesCell("80-c0"), previewMutesCell("c0-")},
		}},
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
			Tables: []ShardedTableStatus{{
				Table: "mutes", Status: state.Task.Running,
				Shards: []ShardProgressData{
					{Shard: "-40", Status: state.Task.Running, PercentComplete: previewMutesCopyPercent},
					{Shard: "40-80", Status: state.Task.Pending},
					{Shard: "80-c0", Status: state.Task.Pending},
				},
			}},
			Shards: previewShardStatuses(state.Apply.Running, []presentation.Operation{
				{Deployment: "-40", State: state.ApplyOperation.Running},
				{Deployment: "40-80", State: state.ApplyOperation.Pending},
				{Deployment: "80-c0", State: state.ApplyOperation.Pending},
			}),
			Cells: []ShardCell{
				previewMutesCell("-40"),
				{Shard: "40-80", Table: "mutes", Statements: []string{previewMutesIndexDrift}},
				previewMutesCell("80-c0"),
			},
		}},
	})
}

// PreviewCommentShardedApplyMultiKeyspace renders a sharded apply spanning
// several keyspaces in one deployment — the shape a database with unsharded
// sibling keyspaces produces, where every unsharded keyspace contributes a
// single "-" shard. Each keyspace renders its own section while the histogram
// spans all of them, and the sharded keyspace's VSchema change renders in the
// shared VSchema section. Every keyspace belongs to one rollout member, so no
// keyspace's shards wait behind another's.
func PreviewCommentShardedApplyMultiKeyspace() string {
	shards := previewShardStatuses(state.Apply.Running, []presentation.Operation{
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
				Tables: []ShardedTableStatus{{
					Table: "outcomes", Status: state.Task.Completed,
					Shards: []ShardProgressData{{Shard: "-", Status: state.Task.Completed}},
				}},
				Shards: []ShardStatus{unshard(shards[0], "-")},
				Cells:  []ShardCell{{Shard: "-", Table: "outcomes", Statements: []string{"ALTER TABLE `outcomes` ADD COLUMN `verdict` varchar(32);"}}},
			},
			{
				Keyspace: "cdb_resolute_lookup",
				Tables: []ShardedTableStatus{{
					Table: "outcomes_lookup", Status: state.Task.Running,
					RowsCopied: 540211, RowsTotal: 2000780, ETASeconds: 480,
					ShardsReporting: 1,
					EstimatedBytes:  new(int64(612_000_000)),
					PlannedShards:   1,
					Shards:          []ShardProgressData{{Shard: "-", Status: state.Task.Running, PercentComplete: 27}},
				}},
				Shards: []ShardStatus{unshard(shards[1], "-")},
				Cells:  []ShardCell{{Shard: "-", Table: "outcomes_lookup", Statements: []string{"ALTER TABLE `outcomes_lookup` ADD COLUMN `verdict` varchar(32);"}}},
			},
			{
				Keyspace: "cdb_resolute_sharded",
				Tables: []ShardedTableStatus{{
					Table: "mutes", Status: state.Task.Pending,
					Shards: []ShardProgressData{
						{Shard: "-40", Status: state.Task.Pending},
						{Shard: "40-80", Status: state.Task.Pending},
						{Shard: "80-c0", Status: state.Task.Pending},
						{Shard: "c0-", Status: state.Task.Pending},
					},
				}},
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
			TableSizes: []TableSizeData{
				{Table: "mutes", EstimatedBytes: previewBytes(22_800_000_000), ShardCount: 4},
			},
			Shards: []KeyspaceShardChange{
				{Shard: "-40", Statements: []string{idx}},
				{Shard: "80-c0", Statements: []string{idx}},
				{Shard: "c0-", Statements: []string{idx}},
				{Shard: "40-80", Statements: []string{drift}},
			},
		}},
	})
}

// PreviewCommentShardedPlanMultiEnvShardOnly renders a multi-environment plan
// whose keyspace carries its DDL only per shard, with no keyspace-wide
// statements. Each environment still counts as having work, so each gets its
// own section and apply command.
func PreviewCommentShardedPlanMultiEnvShardOnly() string {
	idx := "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"
	plan := func(env string, shards ...string) *PlanCommentData {
		changes := make([]KeyspaceShardChange, 0, len(shards))
		for _, shard := range shards {
			changes = append(changes, KeyspaceShardChange{Shard: shard, Statements: []string{idx}})
		}
		return &PlanCommentData{
			Database: "cdb_resolute", Environment: env, DatabaseType: "strata", RequestedBy: previewRequestedBy,
			Changes: []KeyspaceChangeData{{Keyspace: "cdb_resolute_sharded", Shards: changes}},
		}
	}
	return RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "cdb_resolute", DatabaseType: "strata",
		HeadSHA: previewHeadSHA, Repository: previewRepository, RequestedBy: previewRequestedBy,
		Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{
			"staging":    plan("staging", "-80", "80-"),
			"production": plan("production", "-40", "40-80", "80-c0", "c0-"),
		},
		Errors: map[string]string{},
	})
}

// PreviewCommentShardedPlanManyShards renders a uniform sharded plan across a
// wide keyspace: the DDL shows once under an "all N shards" heading, with the
// shard names in a collapsed grid instead of walling the comment.
func PreviewCommentShardedPlanManyShards() string {
	return previewShardedPlanUniform(32)
}

// PreviewCommentShardedPlanFewShards renders a uniform sharded plan across a
// keyspace narrow enough that its shard names fit in the heading.
func PreviewCommentShardedPlanFewShards() string {
	return previewShardedPlanUniform(8)
}

// PreviewCommentShardedPlanHugeShards renders a uniform sharded plan across
// the widest keyspace an even two-hex-digit split allows, where the collapsed
// grid packs the short key ranges many to a row.
func PreviewCommentShardedPlanHugeShards() string {
	return previewShardedPlanUniform(256)
}

// previewShardedPlanUniform renders a plan that runs the same DDL on every
// shard of an evenly split keyspace of n shards.
func previewShardedPlanUniform(n int) string {
	idx := "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`)"
	shards := make([]KeyspaceShardChange, 0, n)
	for i := range n {
		shards = append(shards, KeyspaceShardChange{Shard: previewShardRange(i, n), Statements: []string{idx}})
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
	outcomesIdx := "ALTER TABLE `outcomes` ADD INDEX `status`(`status`)"
	return RenderPlanComment(PlanCommentData{
		Database: "cdb_resolute", Environment: "production", DatabaseType: "strata",
		HeadSHA: previewHeadSHA, Repository: previewRepository, RequestedBy: previewRequestedBy,
		Changes: []KeyspaceChangeData{{
			Keyspace: "cdb_resolute_sharded",
			// A table without a byte estimate beside one that has it renders
			// as explicitly unavailable, so a failed size probe never reads as
			// a small table. The satisfied shard needs no change, so the
			// change spans three shards.
			TableSizes: []TableSizeData{
				{Table: "mutes", ShardCount: 3},
				{Table: "outcomes", EstimatedBytes: previewBytes(4_210_000_000), ShardCount: 3},
			},
			Shards: []KeyspaceShardChange{
				{Shard: "-40", Satisfied: true},
				{Shard: "40-80", Statements: []string{idx, outcomesIdx}},
				{Shard: "80-c0", Statements: []string{idx, outcomesIdx}},
				{Shard: "c0-", Statements: []string{idx, outcomesIdx}},
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
