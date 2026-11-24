//go:build integration

package api

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Fresh storage has authoritative outbox status, and repeated bootstrap
// preserves every disposition, including held intent with recorded acceptance.
func TestEnsureSchema_OutboxStatus(t *testing.T) {
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
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			converge := func() error {
				return EnsureSchema(dsn, logger, WithDialect(dialect), WithConvergenceTimeout(30*time.Second))
			}
			require.NoError(t, converge())
			require.NoError(t, converge())

			// Every authoritative disposition survives repeated convergence,
			// including held intent that records a late transport acceptance.
			_, err := db.ExecContext(ctx, `INSERT INTO outbox_events (apply_id, event_type, payload, status, published_at, quarantine_token) VALUES
				(1, 'apply_completed', '{"id":"pending"}', 'pending', NULL, NULL),
				(2, 'apply_completed', '{"id":"published"}', 'published', '2026-01-01 00:00:00', NULL),
				(3, 'apply_completed', '{"id":"quarantined"}', 'quarantined', '2026-01-01 00:00:00', 'incident'),
				(4, 'apply_completed', '{"id":"skipped"}', 'skipped', NULL, NULL)`)
			require.NoError(t, err)
			require.NoError(t, converge())
			require.NoError(t, converge())
			rows, err := db.QueryContext(ctx, "SELECT status FROM outbox_events ORDER BY id")
			require.NoError(t, err)
			var statuses []string
			for rows.Next() {
				var status string
				require.NoError(t, rows.Scan(&status))
				statuses = append(statuses, status)
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			assert.Equal(t, []string{storage.OutboxStatusPending, storage.OutboxStatusPublished, storage.OutboxStatusQuarantined, storage.OutboxStatusSkipped}, statuses)
		})
	}
}
