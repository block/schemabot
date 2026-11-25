package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
)

const archiveTableDiff = `   "tables": {
     "orders": {
       "column_vindexes": [{"column": "id", "name": "hash"}]
+    },
+    "orders_archive": {
+      "column_vindexes": [{"column": "id", "name": "hash"}]
     }
   }`

// A keyspace that gains a table reads as that table being added, with the raw
// diff collapsed beneath it. Read on its own, the diff's added lines start
// inside the table above the new one.
func TestVSchemaChangeReadsTableByTableWithTheDiffCollapsed(t *testing.T) {
	var sb strings.Builder
	writeVSchemaStatus(&sb, []apitypes.VSchemaChange{{
		Namespace: "commerce",
		Status:    "applying",
		Diff:      archiveTableDiff,
		Summary:   []string{"adds table `orders_archive`: vindex `hash` on `id`"},
	}})
	out := sb.String()

	assert.Contains(t, out, "**`commerce`**: Applying...\n\n- adds table `orders_archive`: vindex `hash` on `id`\n\n<details><summary>VSchema diff</summary>\n\n```diff\n")
	assert.Contains(t, out, `+    "orders_archive": {`, "the full diff stays available")
	assert.True(t, strings.HasSuffix(strings.TrimSpace(out), "</details>"), "the diff is collapsed")
}

// A change recorded without a description shows its diff open, since the
// diff is then the only account of the change.
func TestVSchemaChangeWithoutADescriptionShowsTheDiffOpen(t *testing.T) {
	var sb strings.Builder
	writeVSchemaStatus(&sb, []apitypes.VSchemaChange{{Namespace: "commerce", Status: "applying", Diff: archiveTableDiff}})
	out := sb.String()

	assert.Contains(t, out, "**`commerce`**: Applying...\n\n```diff\n")
	assert.NotContains(t, out, "<details>")
}

// A sharded apply names each keyspace's VSchema change inside that keyspace's
// section, the way a Vitess plan does. A change for a keyspace with no section
// of its own still renders, after the last keyspace.
func TestShardedApplyPlacesEachVSchemaChangeInItsKeyspace(t *testing.T) {
	out := RenderShardedApplyComment(ShardedApplyData{
		State: state.Apply.Running, Environment: "staging", Database: "cdb_resolute",
		ApplyID: "apply-x",
		Keyspaces: oneKeyspace([]ShardStatus{
			{Shard: "-40", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
			{Shard: "80-", Emoji: "✅", Label: "completed", State: state.ApplyOperation.Completed},
		}, []ShardCell{mutesCell("-40"), mutesCell("80-")}),
		VSchemaChanges: []apitypes.VSchemaChange{
			{Namespace: "cdb_resolute_sharded", Status: "applying", Diff: archiveTableDiff, Summary: []string{"adds table `orders_archive`: vindex `hash` on `id`"}},
			{Namespace: "cdb_resolute_lookup", Summary: []string{"adds table `orders_email_idx`"}},
		},
	})

	keyspace := strings.Index(out, "#### Keyspace `cdb_resolute_sharded`")
	inKeyspace := strings.Index(out, "**VSchema**: Applying...\n\n- adds table `orders_archive`: vindex `hash` on `id`")
	assert.Greater(t, inKeyspace, keyspace, "the VSchema change renders inside its keyspace's section")
	assert.Contains(t, out, "### VSchema\n\n**`cdb_resolute_lookup`**: Pending\n\n- adds table `orders_email_idx`",
		"a keyspace with no section gets its VSchema change after the last keyspace")
	assert.Greater(t, strings.Index(out, "### VSchema"), inKeyspace)
}
