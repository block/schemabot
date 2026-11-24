package templates

import (
	"time"

	"github.com/block/schemabot/pkg/apitypes"
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
	tables[1].PercentComplete = 10
	tables[2].Status = state.Task.Pending
	data := samplePostgresApplyData(state.Apply.Running, tables)
	data.Step, data.StepsTotal = 2, 3
	data.Statement = "CREATE INDEX CONCURRENTLY idx_users_last_seen_at ON public.users (last_seen_at)"
	data.BuildWork = apitypes.BuildWork{
		Operation: "concurrent-index-build", ServerPhase: "building index: scanning table",
		BlocksDone: 2500, BlocksTotal: 10000, Percent: 10.42, HasPercent: true,
	}
	return RenderApplyStatusComment(data)
}

// PreviewCommentApplyPostgresIndexBuild renders a PostgreSQL apply whose only
// statement is a concurrent index build: the table's bar is the whole build's
// percent with two decimals, the line under the step names the phase's own
// counters, and the footer offers cancel, the one command that can end the
// build.
func PreviewCommentApplyPostgresIndexBuild() string {
	tables := []TableProgressData{{
		Namespace: "public", TableName: "users", Status: state.Task.Running, PercentComplete: 25,
		DDL: "CREATE INDEX CONCURRENTLY idx_users_email_created ON users (email, created_at)",
	}}
	data := samplePostgresApplyData(state.Apply.Running, tables)
	data.Step, data.StepsTotal = 1, 1
	data.Statement = "CREATE INDEX CONCURRENTLY idx_users_email_created ON public.users (email, created_at)"
	data.BuildWork = apitypes.BuildWork{
		Operation: "concurrent-index-build", ServerPhase: "building index: scanning table",
		BlocksDone: 30000, BlocksTotal: 48000, Percent: 25.37, HasPercent: true,
	}
	return RenderApplyStatusComment(data)
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
