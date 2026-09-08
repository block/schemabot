package templates

import (
	"time"

	"github.com/block/schemabot/pkg/state"
)

// samplePostgresMultiStatementTables returns a PostgreSQL apply whose plan
// splits one table's change into several statements — each its own row — next
// to a second table with a single statement.
func samplePostgresMultiStatementTables() []TableProgressData {
	return []TableProgressData{
		{Namespace: "public", TableName: "users", DDL: "ALTER TABLE users ADD COLUMN last_seen_at timestamptz"},
		{Namespace: "public", TableName: "users", DDL: "CREATE INDEX CONCURRENTLY idx_users_last_seen_at ON users (last_seen_at)"},
		{Namespace: "public", TableName: "sessions", DDL: "ALTER TABLE sessions ADD COLUMN payload jsonb"},
	}
}

func samplePostgresApplyData(s string, tables []TableProgressData) ApplyStatusCommentData {
	data := sampleApplyData(s, tables)
	data.Engine = "postgres"
	return data
}

// PreviewCommentApplyPostgresMultiStatement renders a PostgreSQL apply in
// progress where one table has several statements: the rows repeat the table
// name with a different statement under each.
func PreviewCommentApplyPostgresMultiStatement() string {
	tables := samplePostgresMultiStatementTables()
	tables[0].Status = state.Task.Completed
	tables[1].Status = state.Task.Running
	tables[2].Status = state.Task.Pending
	return RenderApplyStatusComment(samplePostgresApplyData(state.Apply.Running, tables))
}

// PreviewCommentSummaryPostgresMultiStatementFailed renders the failed summary
// for the same apply: the totals count statements, since two of the three rows
// belong to one table.
func PreviewCommentSummaryPostgresMultiStatementFailed() string {
	tables := samplePostgresMultiStatementTables()
	tables[0].Status = state.Task.Completed
	tables[1].Status = state.Task.Failed
	tables[2].Status = state.Task.Cancelled
	data := samplePostgresApplyData(state.Apply.Failed, tables)
	data.StartedAt = sampleTime().Add(-3 * time.Minute).UTC().Format(time.RFC3339)
	data.CompletedAt = sampleTime().UTC().Format(time.RFC3339)
	data.ErrorMessage = "table users failed: schema change failed: canceling statement due to lock timeout"
	return RenderApplySummaryComment(data)
}
