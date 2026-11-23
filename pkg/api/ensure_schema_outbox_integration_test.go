//go:build integration

package api

import (
	"database/sql"
	"log/slog"
	"testing"

	"github.com/block/schemabot/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Existing intent survives adding the nullable quarantine marker on startup.
func TestEnsureSchema_AddsOutboxQuarantine(t *testing.T) {
	for _, dialect := range []schema.Dialect{schema.DialectMySQL, schema.DialectPostgres} {
		t.Run(string(dialect), func(t *testing.T) {
			var dsn string
			var db *sql.DB
			if dialect == schema.DialectMySQL {
				fixture, pool := openEnsureSchemaDatabase(t)
				dsn, db = fixture.DSN, pool
			} else {
				dsn, db = startPostgresStorage(t)
			}
			logger := slog.New(slog.DiscardHandler)
			ctx := t.Context()
			require.NoError(t, EnsureSchema(dsn, logger, WithDialect(dialect)))
			_, err := db.ExecContext(ctx, "ALTER TABLE outbox_events DROP COLUMN quarantine_token")
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, `INSERT INTO outbox_events (apply_id, event_type, payload) VALUES (1, 'apply_completed', '{"id":"apply_existing"}')`)
			require.NoError(t, err)
			require.NoError(t, EnsureSchema(dsn, logger, WithDialect(dialect)))
			require.NoError(t, EnsureSchema(dsn, logger, WithDialect(dialect)))
			var payload string
			var token sql.NullString
			var published sql.NullTime
			require.NoError(t, db.QueryRowContext(ctx, "SELECT payload, quarantine_token, published_at FROM outbox_events").Scan(&payload, &token, &published))
			assert.JSONEq(t, `{"id":"apply_existing"}`, payload)
			assert.False(t, token.Valid)
			assert.False(t, published.Valid)
		})
	}
}
