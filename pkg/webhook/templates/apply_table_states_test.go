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
// than as a copy that is still running.
func TestRenderApplyStatusComment_RevertedAndWaitingForDeployTables(t *testing.T) {
	for _, tc := range []struct {
		applyState string
		taskState  string
		want       string
	}{
		{state.Apply.Reverted, state.Task.Reverted, "**`users`**: 🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧🟧 ↩️ Reverted\n"},
		{state.Apply.WaitingForDeploy, state.Task.WaitingForDeploy, "**`users`**: ⏸️ Waiting for deploy\n"},
	} {
		t.Run(tc.taskState, func(t *testing.T) {
			comment := RenderApplyStatusComment(ApplyStatusCommentData{
				Database: "shop", Environment: "staging", State: tc.applyState, Engine: "Vitess",
				ApplyID: "apply-c3d4e5f6a7b89012",
				Tables:  []TableProgressData{{TableName: "users", Status: tc.taskState}},
			})

			assert.Contains(t, comment, tc.want)
			assert.NotContains(t, comment, "Running...")
		})
	}
}
