package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
)

// A sharded apply shows each keyspace's VSchema change inside that keyspace's
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
			{Namespace: "cdb_resolute_sharded", Status: "applying", Diff: "+ vindex hash"},
			{Namespace: "cdb_resolute_lookup", Diff: "+ orders_email_idx"},
		},
	})

	keyspace := strings.Index(out, "#### Keyspace `cdb_resolute_sharded`")
	inKeyspace := strings.Index(out, "**VSchema**: Applying...\n\n```diff\n+ vindex hash\n```")
	assert.Greater(t, inKeyspace, keyspace, "the VSchema change renders inside its keyspace's section")
	assert.Contains(t, out, "### VSchema\n\n**`cdb_resolute_lookup`**: Pending\n\n```diff\n+ orders_email_idx\n```",
		"a keyspace with no section gets its VSchema change after the last keyspace")
	assert.Greater(t, strings.Index(out, "### VSchema"), inKeyspace)
}
