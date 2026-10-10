package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPreviewCommentPostgresMultiStatement(t *testing.T) {
	t.Run("running progress lists every statement under its table", func(t *testing.T) {
		out := PreviewCommentApplyPostgresMultiStatement()

		assert.Contains(t, out, "1/3 complete · 1 running · 1 queued")
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
		assert.Contains(t, out, "**`sessions`** — Cancelled")
	})
}
