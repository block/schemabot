package templates

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
)

const mutesDDL = "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`);"

func mutesCell(shard string) ShardCell { return ShardCell{Shard: shard, Table: "mutes", DDL: mutesDDL} }

// oneKeyspace wraps a single keyspace's shards and cells the way the webhook
// builder shapes a single-keyspace apply.
func oneKeyspace(shards []ShardStatus, cells []ShardCell) []ShardedKeyspace {
	return []ShardedKeyspace{{Keyspace: "cdb_resolute_sharded", Shards: shards, Cells: cells}}
}

// A uniform sharded apply (every shard the same single change) renders one
// status table and no per-shard grouping. The DDL is not repeated in the applied
// comment — it is shown in the plan and apply-gate comments.
func TestRenderShardedApplyComment_UniformSingleTable(t *testing.T) {
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x", RequestedBy: "morgo",
		Keyspaces: oneKeyspace([]ShardStatus{
			{Shard: "-40", Emoji: "🔄", Label: "running table copy", State: state.ApplyOperation.Running},
			{Shard: "80-", Emoji: "⏳", Label: "queued — next in order", State: state.ApplyOperation.Pending},
		}, []ShardCell{mutesCell("-40"), mutesCell("80-")}),
	})

	assert.Contains(t, out, "**Shards**: 1 running table copy, 1 queued")
	assert.Contains(t, out, "| Shard | Status |")
	assert.NotContains(t, out, "grouped by change", "a uniform apply is not grouped")
	assert.NotContains(t, out, "```sql", "the applied comment shows status only, not DDL")
}

// A failed shard's error is lifted to the top and shown in its status row.
func TestRenderShardedApplyComment_FailedSurfacesError(t *testing.T) {
	const failErr = "resolve shard primary for `-40`: context deadline exceeded"
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x",
		Keyspaces: oneKeyspace([]ShardStatus{
			{Shard: "-40", Emoji: "❌", Label: "failed", State: state.ApplyOperation.Failed, Error: failErr},
			{Shard: "80-", Emoji: "⏸", Label: "halted — -40 failed", State: state.ApplyOperation.Pending},
		}, []ShardCell{mutesCell("-40"), mutesCell("80-")}),
	})

	assert.Contains(t, out, "## Schema Change Status")
	assert.Contains(t, out, "> ❌ **First failure:** shard <code>-40</code> — "+failErr)
	assert.Contains(t, out, failErr, "the error also appears in the failed shard's row")
	assert.Contains(t, out, "To retry:")
}

// An auto-retrying (failed_retryable) shard surfaces its error and the apply
// offers the stop-retrying action, matching the single-deployment footer.
func TestRenderShardedApplyComment_FailedRetryableSurfacesErrorAndStop(t *testing.T) {
	const retryErr = "lost connection to shard primary; retrying"
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.FailedRetryable, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x",
		Keyspaces: oneKeyspace([]ShardStatus{
			{Shard: "-40", Emoji: "🔁", Label: "retrying", State: state.ApplyOperation.FailedRetryable, Error: retryErr},
			{Shard: "80-", Emoji: "⏳", Label: "queued — next in order", State: state.ApplyOperation.Pending},
		}, []ShardCell{mutesCell("-40"), mutesCell("80-")}),
	})

	assert.Contains(t, out, "> ⚠️ **First failure:** shard <code>-40</code> — "+retryErr,
		"a retrying shard's error is still lifted, with the attention glyph — SchemaBot is retrying, nothing has stopped")
	assert.NotContains(t, out, "❌", "no failure glyph while SchemaBot retries on its own")
	assert.Contains(t, out, "To stop retrying:")
	assert.Contains(t, out, "schemabot stop apply-x")
}

// When shards diverge, they are still grouped by change signature so the applied
// comment shows which shards moved together — but the DDL itself is not repeated
// here (it lives in the plan and apply-gate comments).
func TestRenderShardedApplyComment_DivergentGroupsByVariant(t *testing.T) {
	const driftDDL = "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`), ADD COLUMN `reason` varchar(255);"
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x",
		Keyspaces: oneKeyspace([]ShardStatus{
			{Shard: "-40", Emoji: "🔄", Label: "running table copy", State: state.ApplyOperation.Running},
			{Shard: "40-80", Emoji: "⏳", Label: "queued — next in order", State: state.ApplyOperation.Pending},
			{Shard: "80-c0", Emoji: "⏳", Label: "waiting for -40", State: state.ApplyOperation.Pending},
		}, []ShardCell{
			mutesCell("-40"),
			{Shard: "40-80", Table: "mutes", DDL: driftDDL}, // different signature → its own group
			mutesCell("80-c0"),
		}),
	})

	assert.Contains(t, out, "Shards diverge — grouped by change:")
	assert.Contains(t, out, "**shards `-40`, `80-c0`**", "shards sharing the standard change are one group")
	assert.Contains(t, out, "**shard `40-80`**", "the drifted shard is its own group")
	assert.NotContains(t, out, "```sql", "the applied comment shows status only, not DDL")
	assert.NotContains(t, out, driftDDL, "the DDL is not repeated in the applied comment")
}

// A wide divergence group states its coverage against the keyspace's shard
// count instead of enumerating names — the group's status table right below
// still lists every shard. The fraction uses the keyspace total, so a subset
// can never read as covering all shards.
func TestRenderShardedApplyComment_DivergentWideGroupStatesCoverage(t *testing.T) {
	const driftDDL = "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`), ADD COLUMN `reason` varchar(255);"
	shards := make([]ShardStatus, 0, 16)
	cells := make([]ShardCell, 0, 16)
	for i := range 15 {
		shard := fmt.Sprintf("s%02d", i)
		shards = append(shards, ShardStatus{Shard: shard, Emoji: "⏳", Label: "queued — next in order", State: state.ApplyOperation.Pending})
		cells = append(cells, mutesCell(shard))
	}
	shards = append(shards, ShardStatus{Shard: "s15", Emoji: "⏳", Label: "queued — next in order", State: state.ApplyOperation.Pending})
	cells = append(cells, ShardCell{Shard: "s15", Table: "mutes", DDL: driftDDL})

	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "cdb_resolute",
		ApplyID:   "apply-x",
		Keyspaces: oneKeyspace(shards, cells),
	})

	assert.Contains(t, out, "**15 of 16 shards**", "the wide group states coverage, not names")
	assert.NotContains(t, out, "all 16 shards", "a divergent subset never reads as full coverage")
	assert.Contains(t, out, "**shard `s15`**", "the small group still names its shard inline")
	assert.Contains(t, out, "| `s00` |", "the group's status table still names every shard")
}

// A uniform multi-table change set is one group (no spurious "grouped by change"
// header), and the applied comment shows status only — no DDL.
func TestRenderShardedApplyComment_UniformMultiTableIsOneGroup(t *testing.T) {
	blocks := func(shard string) ShardCell {
		return ShardCell{Shard: shard, Table: "blocks", DDL: "ALTER TABLE `blocks` ADD INDEX `created_at`(`created_at`);"}
	}
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x",
		Keyspaces: oneKeyspace([]ShardStatus{
			{Shard: "-40", Emoji: "🔄", Label: "running table copy", State: state.ApplyOperation.Running},
			{Shard: "80-", Emoji: "⏳", Label: "queued — next in order", State: state.ApplyOperation.Pending},
		}, []ShardCell{mutesCell("-40"), blocks("-40"), mutesCell("80-"), blocks("80-")}),
	})

	assert.NotContains(t, out, "grouped by change", "identical multi-table change sets are one group")
	assert.Contains(t, out, "| Shard | Status |")
	assert.NotContains(t, out, "```sql", "the applied comment shows status only, not DDL")
}

// An apply spanning several keyspaces renders one section per keyspace in
// resolved order, and the shard histogram spans every keyspace's shards, so
// one comment tells the operator where the whole rollout stands.
func TestRenderShardedApplyComment_MultiKeyspaceSections(t *testing.T) {
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x",
		Keyspaces: []ShardedKeyspace{
			{
				Keyspace: "cdb_resolute",
				Shards:   []ShardStatus{{Shard: "-", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed}},
				Cells:    []ShardCell{{Shard: "-", Table: "outcomes", DDL: "ALTER TABLE `outcomes` ADD COLUMN `verdict` varchar(32);"}},
			},
			{
				Keyspace: "cdb_resolute_sharded",
				Shards: []ShardStatus{
					{Shard: "-40", Emoji: "🔄", Label: "running table copy", State: state.ApplyOperation.Running},
					{Shard: "80-", Emoji: "⏳", Label: "queued — next in order", State: state.ApplyOperation.Pending},
				},
				Cells: []ShardCell{mutesCell("-40"), mutesCell("80-")},
			},
		},
	})

	assert.Contains(t, out, "#### Keyspace `cdb_resolute`")
	assert.Contains(t, out, "#### Keyspace `cdb_resolute_sharded`")
	assert.Less(t, strings.Index(out, "#### Keyspace `cdb_resolute`"), strings.Index(out, "#### Keyspace `cdb_resolute_sharded`"),
		"keyspaces render in resolved order")
	assert.Contains(t, out, "**Shards**: 1 completed, 1 running table copy, 1 queued",
		"the histogram spans every keyspace's shards")
	assert.Contains(t, out, "| `-` | ✅ completed |", "an unsharded keyspace's lone shard renders under its own heading")
	assert.NotContains(t, out, "grouped by change", "uniform keyspaces are not grouped")
}

// A failure in one keyspace's shard is lifted to the top even when other
// keyspaces' shards are healthy, so the cross-keyspace comment still leads with
// the cause.
func TestRenderShardedApplyComment_MultiKeyspaceFailureLifted(t *testing.T) {
	const failErr = "resolve shard primary for `-40`: context deadline exceeded"
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x",
		Keyspaces: []ShardedKeyspace{
			{
				Keyspace: "cdb_resolute",
				Shards:   []ShardStatus{{Shard: "-", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed}},
				Cells:    []ShardCell{{Shard: "-", Table: "outcomes", DDL: "ALTER TABLE `outcomes` ADD COLUMN `verdict` varchar(32);"}},
			},
			{
				Keyspace: "cdb_resolute_sharded",
				Shards:   []ShardStatus{{Shard: "-40", Emoji: "❌", Label: "failed", State: state.ApplyOperation.Failed, Error: failErr}},
				Cells:    []ShardCell{mutesCell("-40")},
			},
		},
	})

	assert.Contains(t, out, "> ❌ **First failure:** shard <code>-40</code> — "+failErr)
	assert.Contains(t, out, "**Shards**: 1 completed, 1 failed")
}

// A sharded rollback apply carries rollback vocabulary on the stable headline,
// matching the single-deployment status comment.
func TestRenderShardedApplyComment_Rollback(t *testing.T) {
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x", RequestedBy: "morgo",
		Rollback: true,
		Keyspaces: oneKeyspace([]ShardStatus{
			{Shard: "-40", Emoji: "🔄", Label: "running table copy", State: state.ApplyOperation.Running},
		}, []ShardCell{mutesCell("-40")}),
	})

	assert.Contains(t, out, "## Rollback Status")
	assert.NotContains(t, out, "Schema Change Status")
}

// A failed shard's raw engine error can carry internal endpoints, newlines, and
// table-cell separators. Both places it renders — the lifted first-failure line
// and the shard's status cell — must redact endpoints and keep the message on
// one Markdown line so it cannot break the comment layout.
func TestRenderShardedApplyComment_FailedErrorSanitized(t *testing.T) {
	const failErr = "dial tcp db-primary.internal:3306: connect refused\nretry | later"
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x",
		Keyspaces: oneKeyspace([]ShardStatus{
			{Shard: "-40", Emoji: "❌", Label: "failed", State: state.ApplyOperation.Failed, Error: failErr},
		}, []ShardCell{mutesCell("-40")}),
	})

	assert.NotContains(t, out, "db-primary.internal", "internal endpoints are redacted")
	assert.Contains(t, out, "> ❌ **First failure:** shard <code>-40</code> — dial tcp [endpoint redacted]: connect refused retry | later\n",
		"the first-failure line stays on one line")
	assert.Contains(t, out, "| `-40` | ❌ failed — dial tcp [endpoint redacted]: connect refused retry / later |",
		"the status cell neutralizes the cell separator")
}

// A completed sharded apply's terminal summary reads as a verdict: the applied
// header and outcome line every other apply shape's summary leads with, over
// the final per-shard results — not another status-titled snapshot.
func TestRenderShardedApplySummaryComment_Completed(t *testing.T) {
	out := RenderShardedApplySummaryComment(ShardedApplyData{
		State: state.Apply.Completed, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x", RequestedBy: "morgo",
		StartedAt: "2026-01-01T00:00:00Z", CompletedAt: "2026-01-01T00:28:00Z",
		Keyspaces: oneKeyspace([]ShardStatus{
			{Shard: "-40", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
			{Shard: "80-", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
		}, []ShardCell{mutesCell("-40"), mutesCell("80-")}),
	})

	assert.Contains(t, out, "## ✅ Schema Change Applied — Staging")
	assert.NotContains(t, out, "Schema Change Status", "the summary is a verdict, not a status snapshot")
	assert.Contains(t, out, "Applied successfully — your schema change is live!",
		"one table's change across shards reads as a single schema change")
	assert.Contains(t, out, "**Database**: `cdb_resolute` | **Type**: `Strata` | **Apply ID**: `apply-x` | **Duration**: 28m")
	assert.Contains(t, out, "**Shards**: 2 completed")
	assert.Contains(t, out, "| `-40` | ✅ completed |")
	assert.NotContains(t, out, "Last updated", "a terminal summary carries no last-updated line")
}

// A failed sharded apply's terminal summary carries the failed verdict header,
// the surfaced first failure, the final per-shard results, and the retry action.
func TestRenderShardedApplySummaryComment_FailedSurfacesErrorAndRetry(t *testing.T) {
	const failErr = "resolve shard primary for `-40`: context deadline exceeded"
	out := RenderShardedApplySummaryComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x",
		Keyspaces: oneKeyspace([]ShardStatus{
			{Shard: "-40", Emoji: "❌", Label: "failed", State: state.ApplyOperation.Failed, Error: failErr},
			{Shard: "80-", Emoji: "⏸", Label: "halted — -40 failed", State: state.ApplyOperation.Pending},
		}, []ShardCell{mutesCell("-40"), mutesCell("80-")}),
	})

	assert.Contains(t, out, "## ❌ Schema Change Failed — Staging")
	assert.NotContains(t, out, "Applied successfully", "a failed apply writes no success line")
	assert.Contains(t, out, "> ❌ **First failure:** shard <code>-40</code> — "+failErr)
	assert.Contains(t, out, "| `80-` | ⏸ halted — -40 failed |", "halted siblings keep their final state in the results")
	assert.Contains(t, out, "To retry:")
}

// A completed rollback's terminal summary keeps the rollback vocabulary in both
// the header and the outcome line.
func TestRenderShardedApplySummaryComment_Rollback(t *testing.T) {
	out := RenderShardedApplySummaryComment(ShardedApplyData{
		State: state.Apply.Completed, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x", RequestedBy: "morgo",
		Rollback: true,
		Keyspaces: oneKeyspace([]ShardStatus{
			{Shard: "-40", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
		}, []ShardCell{mutesCell("-40")}),
	})

	assert.Contains(t, out, "Rolled back successfully — the schema change has been reverted.")
	assert.NotContains(t, out, "Schema Change Status")
}

// A cancelled sharded apply's terminal summary is permanent: it offers no
// resume command and directs the operator to open a new schema change.
func TestRenderShardedApplySummaryComment_CancelledOffersNoResume(t *testing.T) {
	out := RenderShardedApplySummaryComment(ShardedApplyData{
		State: state.Apply.Cancelled, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x",
		Keyspaces: oneKeyspace([]ShardStatus{
			{Shard: "-40", Emoji: "🚫", Label: "cancelled", State: state.ApplyOperation.Cancelled},
		}, []ShardCell{mutesCell("-40")}),
	})

	assert.Contains(t, out, "## 🚫 Schema Change Cancelled — Staging")
	assert.Contains(t, out, "This schema change was cancelled and cannot be resumed. Open a new schema change to apply it again.")
	assert.NotContains(t, out, "schemabot start", "a cancelled apply offers no resume command")
}

// A sharded apply can fail outside shard work (e.g. a finalizer operation),
// leaving no shard in a failure state. The failed verdict must still name the
// cause, so the apply-level error is lifted to the top in that case — sanitized
// like every other PR-facing error.
func TestRenderShardedApplySummaryComment_FailureOutsideShardWorkSurfacesApplyError(t *testing.T) {
	out := RenderShardedApplySummaryComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "staging", Database: "cdb_resolute",
		ApplyID:      "apply-x",
		ErrorMessage: "finalize vschema: dial tcp db-primary.internal:3306: connect refused retry | later\nsecond line",
		Keyspaces: oneKeyspace([]ShardStatus{
			{Shard: "-40", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
		}, []ShardCell{mutesCell("-40")}),
	})

	assert.Contains(t, out, "## ❌ Schema Change Failed — Staging")
	assert.Contains(t, out, "> ❌ **Failure:** finalize vschema: dial tcp [endpoint redacted]: connect refused retry | later second line",
		"the apply-level error is surfaced when no shard failed, sanitized to one line")
	assert.NotContains(t, out, "db-primary.internal", "internal endpoints never render in PR comments")
	assert.NotContains(t, out, "First failure:", "no shard failed, so there is no shard failure callout")
}

// When a shard did fail, the shard's error owns the failure callout and the
// apply-level error is not repeated below it.
func TestRenderShardedApplyComment_ShardFailureOwnsCallout(t *testing.T) {
	const failErr = "resolve shard primary for `-40`: context deadline exceeded"
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "staging", Database: "cdb_resolute",
		ApplyID:      "apply-x",
		ErrorMessage: "apply failed",
		Keyspaces: oneKeyspace([]ShardStatus{
			{Shard: "-40", Emoji: "❌", Label: "failed", State: state.ApplyOperation.Failed, Error: failErr},
		}, []ShardCell{mutesCell("-40")}),
	})

	assert.Contains(t, out, "> ❌ **First failure:** shard <code>-40</code> — "+failErr)
	assert.NotContains(t, out, "**Failure:** apply failed")
}

// The duration is decoration: a completed timestamp earlier than the started
// timestamp (bad data, clock skew) drops it rather than rendering a negative
// duration.
func TestRenderShardedApplySummaryComment_NegativeDurationOmitted(t *testing.T) {
	out := RenderShardedApplySummaryComment(ShardedApplyData{
		State: state.Apply.Completed, Environment: "staging", Database: "cdb_resolute",
		ApplyID:   "apply-x",
		StartedAt: "2026-01-01T01:00:00Z", CompletedAt: "2026-01-01T00:00:00Z",
		Keyspaces: oneKeyspace([]ShardStatus{
			{Shard: "-40", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
		}, []ShardCell{mutesCell("-40")}),
	})

	assert.NotContains(t, out, "**Duration**:")
}

// A sharded apply that carries a keyspace VSchema update renders it in its own
// VSchema section — the same section the single-deployment comment uses — with
// the keyspace and its display status, so the operator sees the finalizer's
// progress alongside the shard rollout.
func TestRenderShardedApplyComment_VSchemaSection(t *testing.T) {
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x",
		Keyspaces: oneKeyspace([]ShardStatus{
			{Shard: "-40", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
			{Shard: "80-", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
		}, []ShardCell{mutesCell("-40"), mutesCell("80-")}),
		VSchemaChanges: []apitypes.VSchemaChange{{Namespace: "cdb_resolute_sharded", Status: "applying", Diff: "--- current\n+++ new\n+ vindex hash"}},
	})

	assert.Contains(t, out, "### VSchema")
	assert.Contains(t, out, "**`cdb_resolute_sharded`**: Applying...")
	assert.Contains(t, out, "```diff\n--- current\n+++ new\n+ vindex hash\n```",
		"the stored plan's diff renders as a diff block under the keyspace entry")
}

// A completed sharded apply that landed both a table change and a VSchema
// update shows the applied VSchema section in the terminal summary and counts
// the VSchema update in the outcome line's grammar: two changes read as plural.
func TestRenderShardedApplySummaryComment_VSchemaSectionAndPluralGrammar(t *testing.T) {
	out := RenderShardedApplySummaryComment(ShardedApplyData{
		State: state.Apply.Completed, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x", RequestedBy: "morgo",
		Keyspaces: oneKeyspace([]ShardStatus{
			{Shard: "-40", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
			{Shard: "80-", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
		}, []ShardCell{mutesCell("-40"), mutesCell("80-")}),
		VSchemaChanges: []apitypes.VSchemaChange{{Namespace: "cdb_resolute_sharded", Status: "applied"}},
	})

	assert.Contains(t, out, "Applied successfully — your schema changes are live!",
		"a table change plus a VSchema update reads as plural")
	assert.Contains(t, out, "### VSchema")
	assert.Contains(t, out, "**`cdb_resolute_sharded`**: Applied")
}

// The outcome line's grammar counts distinct (keyspace, table) pairs and
// VSchema updates together: exactly one change in total reads as singular,
// anything else as plural.
func TestShardedChangeIsSingular_CountsVSchemaChanges(t *testing.T) {
	oneTable := oneKeyspace(nil, []ShardCell{mutesCell("-40"), mutesCell("80-")})
	oneVSchema := []apitypes.VSchemaChange{{Namespace: "ks", Status: "applied"}}

	assert.True(t, shardedChangeIsSingular(ShardedApplyData{Keyspaces: oneTable}),
		"one table across shards is one change")
	assert.False(t, shardedChangeIsSingular(ShardedApplyData{Keyspaces: oneTable, VSchemaChanges: oneVSchema}),
		"a table change plus a VSchema update is two changes")
	assert.True(t, shardedChangeIsSingular(ShardedApplyData{VSchemaChanges: oneVSchema}),
		"a lone VSchema update is one change")
	assert.False(t, shardedChangeIsSingular(ShardedApplyData{}),
		"no cells and no VSchema changes does not prove a single change")
	assert.False(t, shardedChangeIsSingular(ShardedApplyData{Keyspaces: []ShardedKeyspace{
		{Keyspace: "cdb_resolute", Cells: []ShardCell{mutesCell("-")}},
		{Keyspace: "cdb_resolute_sharded", Cells: []ShardCell{mutesCell("-40")}},
	}}), "the same table name in two keyspaces is two changes")
}

// The status comment is frozen at terminal with the same failure callout the
// summary uses, so a failure outside shard work must surface the apply-level
// error on the status path too — not just in the summary.
func TestRenderShardedApplyComment_FailureOutsideShardWorkSurfacesApplyError(t *testing.T) {
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "staging", Database: "cdb_resolute",
		ApplyID:      "apply-x",
		ErrorMessage: "finalize vschema: apply vschema to keyspace: context deadline exceeded",
		Keyspaces: oneKeyspace([]ShardStatus{
			{Shard: "-40", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
		}, []ShardCell{mutesCell("-40")}),
	})

	assert.Contains(t, out, "## Schema Change Status — Staging")
	assert.Contains(t, out, "> ❌ **Failure:** finalize vschema: apply vschema to keyspace: context deadline exceeded")
	assert.NotContains(t, out, "First failure:", "no shard failed, so there is no shard failure callout")
}

// An apply-level error on a still-retrying apply is surfaced with the
// attention glyph, not the failure glyph — SchemaBot is retrying on its own,
// so nothing has stopped and no triage is due yet.
func TestRenderShardedApplyComment_RetryingApplyErrorCarriesAttentionGlyph(t *testing.T) {
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.FailedRetryable, Environment: "staging", Database: "cdb_resolute",
		ApplyID:      "apply-x",
		ErrorMessage: "finalize vschema: apply vschema to keyspace: context deadline exceeded",
		Keyspaces: oneKeyspace([]ShardStatus{
			{Shard: "-40", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
		}, []ShardCell{mutesCell("-40")}),
	})

	assert.Contains(t, out, "> ⚠️ **Failure:** finalize vschema: apply vschema to keyspace: context deadline exceeded")
	assert.NotContains(t, out, "❌", "no failure glyph while SchemaBot retries on its own")
}

// A keyspace carrying a table rollup renders per-table lines as its unit — the
// table's aggregate phrase plus the compact shard summary while it copies —
// and suppresses the per-shard status table while the keyspace is healthy and
// uniform, keeping the wide-fan-out comment at the table altitude.
func TestRenderShardedApplyComment_TableLinesReplaceShardTable(t *testing.T) {
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x", RequestedBy: "morgo",
		Keyspaces: []ShardedKeyspace{{
			Keyspace: "cdb_resolute_sharded",
			Tables: []ShardedTableStatus{{
				Table: "mutes", Status: state.Task.Running,
				Shards: []ShardProgressData{
					{Shard: "-40", Status: state.Task.Running, PercentComplete: 45},
					{Shard: "80-", Status: state.Task.Pending},
				},
			}},
			Shards: []ShardStatus{
				{Shard: "-40", Emoji: "🔄", Label: "running table copy", State: state.ApplyOperation.Running},
				{Shard: "80-", Emoji: "⏳", Label: "queued — next in order", State: state.ApplyOperation.Pending},
			},
			Cells: []ShardCell{mutesCell("-40"), mutesCell("80-")},
		}},
	})

	assert.Contains(t, out, "**`mutes`**: 🔄 Row copy in progress")
	assert.Contains(t, out, "└ shards: ◐ -40 45% · ⏳ 80-", "the in-flight table carries the compact shard summary")
	assert.NotContains(t, out, "| Shard | Status |", "a healthy uniform keyspace renders no per-shard table")
}

// A shard in a failure state promotes the keyspace's per-shard status table
// back into the section — the failed shard and its halted siblings need naming
// — below the table rollup lines.
func TestRenderShardedApplyComment_ShardFailurePromotesShardTable(t *testing.T) {
	const failErr = "resolve shard primary for `-40`: context deadline exceeded"
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x",
		Keyspaces: []ShardedKeyspace{{
			Keyspace: "cdb_resolute_sharded",
			Tables: []ShardedTableStatus{{
				Table: "mutes", Status: state.Task.Failed,
				Shards: []ShardProgressData{
					{Shard: "-40", Status: state.Task.Failed},
					{Shard: "80-", Status: state.Task.Pending},
				},
			}},
			Shards: []ShardStatus{
				{Shard: "-40", Emoji: "❌", Label: "failed", State: state.ApplyOperation.Failed, Error: failErr},
				{Shard: "80-", Emoji: "⏸", Label: "halted — -40 failed", State: state.ApplyOperation.Pending},
			},
			Cells: []ShardCell{mutesCell("-40"), mutesCell("80-")},
		}},
	})

	assert.Contains(t, out, "**`mutes`**: ❌ Failed")
	assert.Contains(t, out, "| Shard | Status |", "a failure promotes the per-shard table")
	assert.Contains(t, out, "| `-40` | ❌ failed — "+failErr+" |")
	assert.Contains(t, out, "| `80-` | ⏸ halted — -40 failed |")
}

// Divergent shards keep their grouped per-shard tables below the table rollup
// lines: which shards moved together is invisible at the table altitude.
func TestRenderShardedApplyComment_DivergenceKeepsGroupedShardTables(t *testing.T) {
	const driftDDL = "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`), ADD COLUMN `reason` varchar(255);"
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x",
		Keyspaces: []ShardedKeyspace{{
			Keyspace: "cdb_resolute_sharded",
			Tables: []ShardedTableStatus{{
				Table: "mutes", Status: state.Task.Running,
				Shards: []ShardProgressData{
					{Shard: "-40", Status: state.Task.Running, PercentComplete: 45},
					{Shard: "80-", Status: state.Task.Pending},
				},
			}},
			Shards: []ShardStatus{
				{Shard: "-40", Emoji: "🔄", Label: "running table copy", State: state.ApplyOperation.Running},
				{Shard: "80-", Emoji: "⏳", Label: "queued — next in order", State: state.ApplyOperation.Pending},
			},
			Cells: []ShardCell{
				mutesCell("-40"),
				{Shard: "80-", Table: "mutes", DDL: driftDDL},
			},
		}},
	})

	assert.Contains(t, out, "**`mutes`**: 🔄 Row copy in progress")
	assert.Contains(t, out, "Shards diverge — grouped by change:")
	assert.Contains(t, out, "| Shard | Status |", "divergence promotes the grouped per-shard tables")
}

// The status comment's at-a-glance line counts one change per (keyspace,
// table) unit plus one per VSchema update, matching the plan's arithmetic. A
// table in its revert window has landed and counts as applied; a lone change
// reads in the singular.
func TestRenderShardedApplyComment_StatusLineChangeFraction(t *testing.T) {
	tbl := func(table, status string) ShardedTableStatus {
		return ShardedTableStatus{Table: table, Status: status, Shards: []ShardProgressData{{Shard: "-", Status: status}}}
	}
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x",
		Keyspaces: []ShardedKeyspace{
			{Keyspace: "cdb_resolute", Tables: []ShardedTableStatus{tbl("outcomes", state.Task.Completed), tbl("verdicts", state.Task.RevertWindow)},
				Shards: []ShardStatus{{Shard: "-", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed}},
				Cells:  []ShardCell{{Shard: "-", Table: "outcomes", DDL: "ALTER ..."}}},
			{Keyspace: "cdb_resolute_sharded", Tables: []ShardedTableStatus{tbl("mutes", state.Task.Running)},
				Shards: []ShardStatus{{Shard: "-40", Emoji: "🔄", Label: "running table copy", State: state.ApplyOperation.Running}},
				Cells:  []ShardCell{mutesCell("-40")}},
		},
		VSchemaChanges: []apitypes.VSchemaChange{{Namespace: "cdb_resolute_sharded", Status: ""}},
	})

	assert.Contains(t, out, "**Status**: In Progress — 2 of 4 changes applied",
		"completed and revert-window tables count as applied; the running table and pending VSchema update do not")

	single := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x",
		Keyspaces: []ShardedKeyspace{{
			Keyspace: "cdb_resolute_sharded",
			Tables:   []ShardedTableStatus{tbl("mutes", state.Task.Running)},
			Shards:   []ShardStatus{{Shard: "-40", Emoji: "🔄", Label: "running table copy", State: state.ApplyOperation.Running}},
			Cells:    []ShardCell{mutesCell("-40")},
		}},
	})
	assert.Contains(t, single, "**Status**: In Progress — 0 of 1 change applied", "a lone change reads in the singular")
}

// Without a table rollup the status line is omitted — a fraction computed from
// no tables would read zero regardless of real progress — and the terminal
// summary never carries the line at all; its verdict header is the outcome.
func TestShardedStatusLine_OmittedWithoutTablesAndInSummary(t *testing.T) {
	noTables := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x",
		Keyspaces: oneKeyspace([]ShardStatus{
			{Shard: "-40", Emoji: "🔄", Label: "running table copy", State: state.ApplyOperation.Running},
		}, []ShardCell{mutesCell("-40")}),
	})
	assert.NotContains(t, noTables, "**Status**:", "no table rollup, no status line")

	summary := RenderShardedApplySummaryComment(ShardedApplyData{
		State: state.Apply.Completed, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x", RequestedBy: "morgo",
		Keyspaces: []ShardedKeyspace{{
			Keyspace: "cdb_resolute_sharded",
			Tables: []ShardedTableStatus{{
				Table: "mutes", Status: state.Task.Completed,
				Shards: []ShardProgressData{{Shard: "-40", Status: state.Task.Completed}, {Shard: "80-", Status: state.Task.Completed}},
			}},
			Shards: []ShardStatus{
				{Shard: "-40", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
				{Shard: "80-", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
			},
			Cells: []ShardCell{mutesCell("-40"), mutesCell("80-")},
		}},
	})
	assert.NotContains(t, summary, "**Status**:", "the verdict header is the summary's outcome")
	assert.Contains(t, summary, "**`mutes`**: ✅ Complete (2 shards)",
		"a completed sharded table names its shard count")
	assert.NotContains(t, summary, "| Shard | Status |", "a fully completed keyspace renders no per-shard table")
}

// A completed rollback's frozen status comment reads "Rolled Back", not
// "Applied" — the change was removed, not landed.
func TestRenderShardedApplyComment_RollbackStatusWord(t *testing.T) {
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Completed, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x", Rollback: true,
		Keyspaces: []ShardedKeyspace{{
			Keyspace: "cdb_resolute_sharded",
			Tables: []ShardedTableStatus{{
				Table: "mutes", Status: state.Task.Completed,
				Shards: []ShardProgressData{{Shard: "-40", Status: state.Task.Completed}},
			}},
			Shards: []ShardStatus{{Shard: "-40", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed}},
			Cells:  []ShardCell{mutesCell("-40")},
		}},
	})

	assert.Contains(t, out, "**Status**: Rolled Back — 1 of 1 change applied")
}

// The revert-window phrase deliberately carries no checkmark: the change is
// applied but not final while the window is open, and a checkmark reads as
// "done, walk away".
func TestShardedTableStatusPhrase_RevertWindowWithoutCheckmark(t *testing.T) {
	phrase := shardedTableStatusPhrase(state.Task.RevertWindow)
	assert.Equal(t, "Complete (revert window open)", phrase)
	assert.NotContains(t, phrase, "✅")
}
