package templates

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rowLabels(rows []TableProgressData) []string {
	labels := make([]string, len(rows))
	for i, r := range rows {
		labels[i] = r.TableName + ":" + r.Status
	}
	return labels
}

func TestSortProgressRows(t *testing.T) {
	t.Run("one row per table sorts by state and keeps plan order within a state", func(t *testing.T) {
		rows := []TableProgressData{
			{Namespace: "testapp", TableName: "orders", Status: state.Task.Completed},
			{Namespace: "testapp", TableName: "users", Status: state.Task.Pending},
			{Namespace: "testapp", TableName: "carts", Status: state.Task.Running},
			{Namespace: "testapp", TableName: "products", Status: state.Task.Pending},
		}
		got := sortProgressRows(rows)

		assert.Equal(t, []string{
			"carts:" + state.Task.Running,
			"users:" + state.Task.Pending,
			"products:" + state.Task.Pending,
			"orders:" + state.Task.Completed,
		}, rowLabels(got))
		assert.Equal(t, "orders", rows[0].TableName, "input is not reordered")
	})

	t.Run("a table with several statements stays together, ranked by its most urgent row", func(t *testing.T) {
		rows := []TableProgressData{
			{Namespace: "public", TableName: "users", Status: state.Task.Completed},
			{Namespace: "public", TableName: "users", Status: state.Task.Running},
			{Namespace: "public", TableName: "sessions", Status: state.Task.Pending},
		}
		got := sortProgressRows(rows)

		assert.Equal(t, []string{
			"users:" + state.Task.Running,
			"users:" + state.Task.Completed,
			"sessions:" + state.Task.Pending,
		}, rowLabels(got))
	})

	t.Run("same table name in another namespace is a different table", func(t *testing.T) {
		rows := []TableProgressData{
			{Namespace: "billing", TableName: "users", Status: state.Task.Completed},
			{Namespace: "public", TableName: "users", Status: state.Task.Running},
		}
		got := sortProgressRows(rows)

		require.Len(t, got, 2)
		assert.Equal(t, "public", got[0].Namespace)
		assert.Equal(t, "billing", got[1].Namespace)
	})
}

func TestRenderApplyStatusComment_MultiStatementTableRowsStayTogether(t *testing.T) {
	data := multiStatementApplyData(state.Apply.Running, multiStatementTableRows(state.Task.Completed, state.Task.Running, state.Task.Pending))
	out := RenderApplyStatusComment(data)

	running := "**`users`**: Running..."
	complete := "**`users`**: " + ui.ProgressBarComplete() + " ✅ Complete"
	queued := "**`sessions`**: ⏳ Queued"
	require.Contains(t, out, running)
	require.Contains(t, out, complete)
	require.Contains(t, out, queued)
	assert.Less(t, strings.Index(out, running), strings.Index(out, complete), "the table's running row leads its block")
	assert.Less(t, strings.Index(out, complete), strings.Index(out, queued), "the other table follows the whole block")
}
