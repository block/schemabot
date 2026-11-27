package templates

import (
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/stretchr/testify/assert"
)

func TestPreviewCommentPostgresMultiStatement(t *testing.T) {
	t.Run("running progress lists every statement under its table", func(t *testing.T) {
		out := PreviewCommentApplyPostgresMultiStatement()

		assert.Contains(t, out, "1/3 complete · 1 running (10%) · 1 queued")
		assert.Contains(t, out, "**Schema `public`**")
		assert.Contains(t, out, "ALTER TABLE users ADD COLUMN last_seen_at timestamptz;")
		assert.Contains(t, out, "CREATE INDEX CONCURRENTLY idx_users_last_seen_at ON users USING btree (last_seen_at);")
		assert.Contains(t, out, "ALTER TABLE sessions ADD COLUMN payload jsonb;")
	})

	t.Run("failed summary counts statements", func(t *testing.T) {
		out := PreviewCommentSummaryPostgresMultiStatementFailed()

		assert.Contains(t, out, "1 of 3 statements completed before failure.")
		assert.NotContains(t, out, "3 tables")
		assert.Contains(t, out, "**`users`** — Failed")
		assert.Contains(t, out, "**`users`** — Completed")
		assert.Contains(t, out, "**`sessions`** — Not run")
	})
}

// The PR comment's footer offers a PostgreSQL apply only the command its
// engine accepts: cancel for a concurrent index build that is the apply's only
// statement, and no command for anything else, since PostgreSQL has no stop.
func TestPostgresFooterOffersOnlyTheCommandItsEngineAccepts(t *testing.T) {
	t.Run("a lone index build offers cancel", func(t *testing.T) {
		out := PreviewCommentApplyPostgresIndexBuild()

		assert.Contains(t, out, "To cancel this schema change:\n```\nschemabot cancel apply-a1b2c3d4e5f6 -e staging\n```")
		assert.NotContains(t, out, "schemabot stop")
	})

	t.Run("an index build among several statements offers nothing", func(t *testing.T) {
		out := PreviewCommentApplyPostgresMultiStatement()

		assert.NotContains(t, out, "schemabot stop")
		assert.NotContains(t, out, "schemabot cancel")
	})

	t.Run("a retrying apply explains the retry without a command", func(t *testing.T) {
		tables := samplePostgresMultiStatementTables()[:1]
		tables[0].Status = state.Task.FailedRetryable
		out := RenderApplyStatusComment(samplePostgresApplyData(state.Apply.FailedRetryable, tables))

		assert.Contains(t, out, "\n---\n\n"+retryInterruptedNote+"\n")
		assert.NotContains(t, out, "schemabot stop")
		assert.NotContains(t, out, "schemabot cancel")
	})
}
