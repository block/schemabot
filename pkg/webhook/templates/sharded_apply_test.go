package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
)

const mutesDDL = "ALTER TABLE `mutes` ADD INDEX `created_at`(`created_at`);"

func mutesCell(shard string) ShardCell { return ShardCell{Shard: shard, Table: "mutes", DDL: mutesDDL} }

// A uniform sharded apply (every shard the same single change) renders one
// status table and no per-shard grouping. The DDL is not repeated in the applied
// comment — it is shown in the plan and apply-gate comments.
func TestRenderShardedApplyComment_UniformSingleTable(t *testing.T) {
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "cdb_resolute",
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x", RequestedBy: "morgo",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "🔄", Label: "running table copy", State: state.ApplyOperation.Running},
			{Shard: "80-", Emoji: "⏳", Label: "queued — next in order", State: state.ApplyOperation.Pending},
		},
		Cells: []ShardCell{mutesCell("-40"), mutesCell("80-")},
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
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "❌", Label: "failed", State: state.ApplyOperation.Failed, Error: failErr},
			{Shard: "80-", Emoji: "⏸", Label: "halted — -40 failed", State: state.ApplyOperation.Pending},
		},
		Cells: []ShardCell{mutesCell("-40"), mutesCell("80-")},
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
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "🔁", Label: "retrying", State: state.ApplyOperation.FailedRetryable, Error: retryErr},
			{Shard: "80-", Emoji: "⏳", Label: "queued — next in order", State: state.ApplyOperation.Pending},
		},
		Cells: []ShardCell{mutesCell("-40"), mutesCell("80-")},
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
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "🔄", Label: "running table copy", State: state.ApplyOperation.Running},
			{Shard: "40-80", Emoji: "⏳", Label: "queued — next in order", State: state.ApplyOperation.Pending},
			{Shard: "80-c0", Emoji: "⏳", Label: "waiting for -40", State: state.ApplyOperation.Pending},
		},
		Cells: []ShardCell{
			mutesCell("-40"),
			{Shard: "40-80", Table: "mutes", DDL: driftDDL}, // different signature → its own group
			mutesCell("80-c0"),
		},
	})

	assert.Contains(t, out, "Shards diverge — grouped by change:")
	assert.Contains(t, out, "**shards `-40`, `80-c0`**", "shards sharing the standard change are one group")
	assert.Contains(t, out, "**shard `40-80`**", "the drifted shard is its own group")
	assert.NotContains(t, out, "```sql", "the applied comment shows status only, not DDL")
	assert.NotContains(t, out, driftDDL, "the DDL is not repeated in the applied comment")
}

// A uniform multi-table change set is one group (no spurious "grouped by change"
// header), and the applied comment shows status only — no DDL.
func TestRenderShardedApplyComment_UniformMultiTableIsOneGroup(t *testing.T) {
	blocks := func(shard string) ShardCell {
		return ShardCell{Shard: shard, Table: "blocks", DDL: "ALTER TABLE `blocks` ADD INDEX `created_at`(`created_at`);"}
	}
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "cdb_resolute",
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "🔄", Label: "running table copy", State: state.ApplyOperation.Running},
			{Shard: "80-", Emoji: "⏳", Label: "queued — next in order", State: state.ApplyOperation.Pending},
		},
		Cells: []ShardCell{mutesCell("-40"), blocks("-40"), mutesCell("80-"), blocks("80-")},
	})

	assert.NotContains(t, out, "grouped by change", "identical multi-table change sets are one group")
	assert.Contains(t, out, "| Shard | Status |")
	assert.NotContains(t, out, "```sql", "the applied comment shows status only, not DDL")
}

// A sharded rollback apply carries rollback vocabulary on the stable headline,
// matching the single-deployment status comment.
func TestRenderShardedApplyComment_Rollback(t *testing.T) {
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "cdb_resolute",
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x", RequestedBy: "morgo",
		Rollback: true,
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "🔄", Label: "running table copy", State: state.ApplyOperation.Running},
		},
		Cells: []ShardCell{mutesCell("-40")},
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
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "❌", Label: "failed", State: state.ApplyOperation.Failed, Error: failErr},
		},
		Cells: []ShardCell{mutesCell("-40")},
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
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x", RequestedBy: "morgo",
		StartedAt: "2026-01-01T00:00:00Z", CompletedAt: "2026-01-01T00:28:00Z",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
			{Shard: "80-", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
		},
		Cells: []ShardCell{mutesCell("-40"), mutesCell("80-")},
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
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "❌", Label: "failed", State: state.ApplyOperation.Failed, Error: failErr},
			{Shard: "80-", Emoji: "⏸", Label: "halted — -40 failed", State: state.ApplyOperation.Pending},
		},
		Cells: []ShardCell{mutesCell("-40"), mutesCell("80-")},
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
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x", RequestedBy: "morgo",
		Rollback: true,
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
		},
		Cells: []ShardCell{mutesCell("-40")},
	})

	assert.Contains(t, out, "Rolled back successfully — the schema change has been reverted.")
	assert.NotContains(t, out, "Schema Change Status")
}

// A cancelled sharded apply's terminal summary is permanent: it offers no
// resume command and directs the operator to open a new schema change.
func TestRenderShardedApplySummaryComment_CancelledOffersNoResume(t *testing.T) {
	out := RenderShardedApplySummaryComment(ShardedApplyData{
		State: state.Apply.Cancelled, Environment: "staging", Database: "cdb_resolute",
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "🚫", Label: "cancelled", State: state.ApplyOperation.Cancelled},
		},
		Cells: []ShardCell{mutesCell("-40")},
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
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x",
		ErrorMessage: "finalize vschema: dial tcp db-primary.internal:3306: connect refused retry | later\nsecond line",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
		},
		Cells: []ShardCell{mutesCell("-40")},
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
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x",
		ErrorMessage: "apply failed",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "❌", Label: "failed", State: state.ApplyOperation.Failed, Error: failErr},
		},
		Cells: []ShardCell{mutesCell("-40")},
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
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x",
		StartedAt: "2026-01-01T01:00:00Z", CompletedAt: "2026-01-01T00:00:00Z",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
		},
		Cells: []ShardCell{mutesCell("-40")},
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
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
			{Shard: "80-", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
		},
		Cells:          []ShardCell{mutesCell("-40"), mutesCell("80-")},
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
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x", RequestedBy: "morgo",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
			{Shard: "80-", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
		},
		Cells:          []ShardCell{mutesCell("-40"), mutesCell("80-")},
		VSchemaChanges: []apitypes.VSchemaChange{{Namespace: "cdb_resolute_sharded", Status: "applied"}},
	})

	assert.Contains(t, out, "Applied successfully — your schema changes are live!",
		"a table change plus a VSchema update reads as plural")
	assert.Contains(t, out, "### VSchema")
	assert.Contains(t, out, "**`cdb_resolute_sharded`**: Applied")
}

// The outcome line's grammar counts distinct tables and VSchema updates
// together: exactly one change in total reads as singular, anything else as
// plural.
func TestShardedChangeIsSingular_CountsVSchemaChanges(t *testing.T) {
	oneTable := []ShardCell{mutesCell("-40"), mutesCell("80-")}
	oneVSchema := []apitypes.VSchemaChange{{Namespace: "ks", Status: "applied"}}

	assert.True(t, shardedChangeIsSingular(ShardedApplyData{Cells: oneTable}),
		"one table across shards is one change")
	assert.False(t, shardedChangeIsSingular(ShardedApplyData{Cells: oneTable, VSchemaChanges: oneVSchema}),
		"a table change plus a VSchema update is two changes")
	assert.True(t, shardedChangeIsSingular(ShardedApplyData{VSchemaChanges: oneVSchema}),
		"a lone VSchema update is one change")
	assert.False(t, shardedChangeIsSingular(ShardedApplyData{}),
		"no cells and no VSchema changes does not prove a single change")
}

// The status comment is frozen at terminal with the same failure callout the
// summary uses, so a failure outside shard work must surface the apply-level
// error on the status path too — not just in the summary.
func TestRenderShardedApplyComment_FailureOutsideShardWorkSurfacesApplyError(t *testing.T) {
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Failed, Environment: "staging", Database: "cdb_resolute",
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x",
		ErrorMessage: "finalize vschema: apply vschema to keyspace: context deadline exceeded",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
		},
		Cells: []ShardCell{mutesCell("-40")},
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
		Keyspace: "cdb_resolute_sharded", ApplyID: "apply-x",
		ErrorMessage: "finalize vschema: apply vschema to keyspace: context deadline exceeded",
		Shards: []ShardStatus{
			{Shard: "-40", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
		},
		Cells: []ShardCell{mutesCell("-40")},
	})

	assert.Contains(t, out, "> ⚠️ **Failure:** finalize vschema: apply vschema to keyspace: context deadline exceeded")
	assert.NotContains(t, out, "❌", "no failure glyph while SchemaBot retries on its own")
}
