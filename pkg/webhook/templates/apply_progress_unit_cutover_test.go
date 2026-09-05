package templates

import (
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
)

func TestRenderApplyStatusComment_CutoverSummaryUnit(t *testing.T) {
	t.Run("one statement per table", func(t *testing.T) {
		data := multiStatementApplyData(state.Apply.WaitingForCutover, []TableProgressData{
			{Namespace: "public", TableName: "users", DDL: "ALTER TABLE users ADD COLUMN a int", Status: state.Task.WaitingForCutover},
			{Namespace: "public", TableName: "sessions", DDL: "ALTER TABLE sessions ADD COLUMN b int", Status: state.Task.Running},
		})
		out := RenderApplyStatusComment(data)

		assert.Contains(t, out, "**1/2** table(s) ready for cutover")
	})

	t.Run("several statements on one table", func(t *testing.T) {
		data := multiStatementApplyData(state.Apply.WaitingForCutover, []TableProgressData{
			{Namespace: "public", TableName: "users", DDL: "ALTER TABLE users ADD COLUMN a int", Status: state.Task.WaitingForCutover},
			{Namespace: "public", TableName: "users", DDL: "ALTER TABLE users ADD COLUMN b int", Status: state.Task.Running},
		})
		out := RenderApplyStatusComment(data)

		assert.Contains(t, out, "**1/2** statement(s) ready for cutover")
	})
}

// A multi-statement namespace next to a namespace of distinct tables: both
// collapsed headers count in the comment-wide unit, so the reader never sees
// "3 tables" above "3 statements" for the same apply.
func TestRenderApplySummaryComment_CollapsedGroupCountUsesUnit(t *testing.T) {
	var tables []TableProgressData
	for range 3 {
		tables = append(tables, TableProgressData{Namespace: "public", TableName: "users", DDL: "ALTER TABLE users ADD COLUMN c int", Status: state.Task.Completed})
	}
	for _, table := range []string{"invoices", "payments", "refunds"} {
		tables = append(tables, TableProgressData{Namespace: "billing", TableName: table, DDL: "ALTER TABLE " + table + " ADD COLUMN c int", Status: state.Task.Completed})
	}
	tables[0].Status = state.Task.Failed
	out := RenderApplySummaryComment(multiStatementApplyData(state.Apply.Failed, tables))

	assert.Contains(t, out, "<strong>public</strong> (3 statements)</summary>")
	assert.Contains(t, out, "<strong>billing</strong> (3 statements)</summary>")
	assert.NotContains(t, out, "(3 tables)")
	assert.Contains(t, out, "5 of 6 statements completed before failure.")
}
