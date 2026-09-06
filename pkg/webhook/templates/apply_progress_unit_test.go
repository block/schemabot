package templates

import (
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
)

func TestProgressUnit(t *testing.T) {
	tests := []struct {
		name   string
		tables []TableProgressData
		want   string
	}{
		{"no rows", nil, "table"},
		{"one row per table", []TableProgressData{
			{Namespace: "public", TableName: "users"},
			{Namespace: "public", TableName: "orders"},
		}, "table"},
		{"same table name in two namespaces is two tables", []TableProgressData{
			{Namespace: "billing", TableName: "users"},
			{Namespace: "public", TableName: "users"},
		}, "table"},
		{"names that concatenate alike are still two tables", []TableProgressData{
			{Namespace: "ab", TableName: "c"},
			{Namespace: "a", TableName: "bc"},
		}, "table"},
		{"several rows on one table", []TableProgressData{
			{Namespace: "public", TableName: "users"},
			{Namespace: "public", TableName: "users"},
			{Namespace: "public", TableName: "orders"},
		}, "statement"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, progressUnit(tc.tables))
		})
	}
}

// multiStatementTableRows is a PostgreSQL apply where the plan split one
// table's change into two statements next to a single-statement table: three
// rows, two tables.
func multiStatementTableRows(users1, users2, sessions string) []TableProgressData {
	return []TableProgressData{
		{Namespace: "public", TableName: "users", DDL: "ALTER TABLE users ADD COLUMN last_seen_at timestamptz", Status: users1},
		{Namespace: "public", TableName: "users", DDL: "CREATE INDEX CONCURRENTLY idx_users_last_seen_at ON users (last_seen_at)", Status: users2},
		{Namespace: "public", TableName: "sessions", DDL: "ALTER TABLE sessions ADD COLUMN payload jsonb", Status: sessions},
	}
}

func multiStatementApplyData(applyState string, tables []TableProgressData) ApplyStatusCommentData {
	return ApplyStatusCommentData{
		ApplyID:     "apply-1",
		Database:    "testapp",
		Environment: "staging",
		Engine:      "postgres",
		State:       applyState,
		Tables:      tables,
	}
}

func TestRenderApplySummaryComment_MultiStatementTableCountsStatements(t *testing.T) {
	t.Run("failed", func(t *testing.T) {
		data := multiStatementApplyData(state.Apply.Failed, multiStatementTableRows(state.Task.Completed, state.Task.Failed, state.Task.Cancelled))
		out := RenderApplySummaryComment(data)

		assert.Contains(t, out, "1 of 3 statements completed before failure.")
		assert.NotContains(t, out, "3 tables")
		assert.Contains(t, out, "ALTER TABLE users ADD COLUMN last_seen_at timestamptz")
		assert.Contains(t, out, "CREATE INDEX CONCURRENTLY idx_users_last_seen_at ON users USING btree (last_seen_at);")
		assert.Contains(t, out, "ALTER TABLE sessions ADD COLUMN payload jsonb")
	})

	t.Run("stopped", func(t *testing.T) {
		data := multiStatementApplyData(state.Apply.Stopped, multiStatementTableRows(state.Task.Completed, state.Task.Stopped, state.Task.Pending))
		out := RenderApplySummaryComment(data)

		assert.Contains(t, out, "1 of 3 statements completed before stop.")
		assert.NotContains(t, out, "3 tables")
	})

	t.Run("cancelled", func(t *testing.T) {
		data := multiStatementApplyData(state.Apply.Cancelled, multiStatementTableRows(state.Task.Completed, state.Task.Cancelled, state.Task.Cancelled))
		out := RenderApplySummaryComment(data)

		assert.Contains(t, out, "1 of 3 statements completed before cancellation.")
		assert.NotContains(t, out, "3 tables")
	})

	t.Run("completed", func(t *testing.T) {
		data := multiStatementApplyData(state.Apply.Completed, multiStatementTableRows(state.Task.Completed, state.Task.Completed, state.Task.Completed))
		out := RenderApplySummaryComment(data)

		assert.Contains(t, out, "<details><summary>Apply details (3 statements)</summary>")
		assert.NotContains(t, out, "3 tables")
	})
}

func TestRenderApplySummaryComment_MultiStatementNamespaceSummaryUsesOneUnit(t *testing.T) {
	tables := multiStatementTableRows(state.Task.Completed, state.Task.Completed, state.Task.Completed)
	tables[2].Namespace = "billing"
	out := RenderApplySummaryComment(multiStatementApplyData(state.Apply.Completed, tables))

	assert.Contains(t, out, "- `public`: 2 statements")
	assert.Contains(t, out, "- `billing`: 1 statement")
	assert.NotContains(t, out, " table")
}

func TestRenderApplySummaryComment_OneStatementPerTableCountsTables(t *testing.T) {
	data := ApplyStatusCommentData{
		ApplyID:     "apply-1",
		Database:    "testapp",
		Environment: "staging",
		Engine:      "spirit",
		State:       state.Apply.Failed,
		// The error text is rendered verbatim and may itself say "statement";
		// the assertions below pin the count line, not the whole comment.
		ErrorMessage: "table users failed: statement exceeded lock wait timeout",
		Tables: []TableProgressData{
			{Namespace: "testapp", TableName: "orders", DDL: "ALTER TABLE orders ADD COLUMN note varchar(255)", Status: state.Task.Completed},
			{Namespace: "testapp", TableName: "users", DDL: "ALTER TABLE users ADD COLUMN note varchar(255)", Status: state.Task.Failed},
		},
	}
	out := RenderApplySummaryComment(data)

	assert.Contains(t, out, "1 of 2 tables completed before failure.")
	assert.NotContains(t, out, "2 statements")
}
