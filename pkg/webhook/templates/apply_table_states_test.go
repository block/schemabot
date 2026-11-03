package templates

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
)

// Every task state other than running renders its own table row. A state that
// fell through to the running row would show a table as copying, or as
// "Running...", when it is reverted, waiting on an operator, or otherwise not
// moving at all.
func TestRenderTableProgress_EveryStateHasItsOwnRow(t *testing.T) {
	table := TableProgressData{TableName: "users"}

	var running strings.Builder
	renderRunningTable(&running, schema.DialectMySQL, table, nil)

	v := reflect.ValueOf(state.Task)
	for i := range v.NumField() {
		status := v.Field(i).String()
		require.NotEmpty(t, status, "state.Task.%s is empty", v.Type().Field(i).Name)
		if status == state.Task.Running {
			continue
		}
		t.Run(status, func(t *testing.T) {
			table.Status = status
			var row strings.Builder
			renderTableProgress(&row, schema.DialectMySQL, table, "", 0, "", nil)
			assert.NotContainsf(t, row.String(), running.String(),
				"state.Task.%s renders as a running table:\n%s", v.Type().Field(i).Name, row.String())
		})
	}
}

// A reverted apply's comment shows each table as reverted, and an apply
// holding for its deploy shows each table as waiting for the deploy, rather
// than as a copy that is still running. Row counters the table carries from
// before it settled do not bring back a partial copy bar, and the table keeps
// its DDL. A table waiting for its deploy is still in flight, so it lists its
// shards; a reverted table has settled, so it does not.
func TestRenderApplyStatusComment_RevertedAndWaitingForDeployTables(t *testing.T) {
	for _, tc := range []struct {
		applyState string
		taskState  string
		want       string
		listShards bool
	}{
		{state.Apply.Reverted, state.Task.Reverted, "**`users`**: 🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧 ↩️ Reverted\n", false},
		{state.Apply.WaitingForDeploy, state.Task.WaitingForDeploy, "**`users`**: ⏸️ Waiting for deploy\n", true},
	} {
		t.Run(tc.taskState, func(t *testing.T) {
			comment := RenderApplyStatusComment(ApplyStatusCommentData{
				Database: "shop", Environment: "staging", State: tc.applyState, Engine: "Vitess",
				ApplyID: "apply-c3d4e5f6a7b89012",
				Tables: []TableProgressData{{
					TableName: "users", Status: tc.taskState,
					DDL:        "ALTER TABLE `users` ADD COLUMN `nickname` varchar(64)",
					RowsCopied: 400, RowsTotal: 1000, PercentComplete: 40,
					Shards: []ShardProgressData{
						{Shard: "-80", Status: tc.taskState},
						{Shard: "80-", Status: tc.taskState},
					},
				}},
			})

			assert.Contains(t, comment, tc.want)
			assert.NotContains(t, comment, "Running...")
			assert.NotContains(t, comment, "🟦", "the stale counters do not render a copy bar")
			assert.NotContains(t, comment, "Rows: 400", "the stale counters do not render copy progress")
			assert.Contains(t, comment, "ADD COLUMN `nickname`")
			if tc.listShards {
				assert.Contains(t, comment, "- Shards: 2 (")
			} else {
				assert.NotContains(t, comment, "- Shards:")
			}
		})
	}
}
