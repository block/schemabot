package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
)

// The sharded-apply comment's lifecycle footer addresses a specific deployment,
// so on a tenant deployment its hint carries the --tenant flag; single-tenant
// deployments render the same command unchanged.
func TestShardedApplyHintsCarryTenant(t *testing.T) {
	cases := []struct {
		name    string
		state   string
		command string
	}{
		{name: "failed retry", state: state.Apply.Failed, command: "schemabot apply -e staging"},
		{name: "failed_retryable stop", state: state.Apply.FailedRetryable, command: "schemabot stop apply-x -e staging"},
		{name: "stopped start", state: state.Apply.Stopped, command: "schemabot start apply-x -e staging"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := ShardedApplyData{
				State:       tc.state,
				Environment: "staging",
				Database:    "cdb_resolute",
				Keyspace:    "cdb_resolute_sharded",
				ApplyID:     "apply-x",
				Shards: []ShardStatus{
					{Shard: "-40", Emoji: "⏸", Label: "stopped", State: state.ApplyOperation.Stopped},
					{Shard: "80-", Emoji: "⏸", Label: "stopped", State: state.ApplyOperation.Stopped},
				},
				Cells: []ShardCell{mutesCell("-40"), mutesCell("80-")},
			}

			untenanted := RenderShardedApplyComment(data)
			assert.NotContains(t, untenanted, "--tenant")
			assert.Contains(t, untenanted, tc.command+"\n", "single-tenant hint must be the bare command")

			data.Tenant = "acme"
			tenanted := RenderShardedApplyComment(data)
			assert.Contains(t, tenanted, tc.command+" --tenant acme", "tenant hint must carry the deployment's tenant")
		})
	}
}
