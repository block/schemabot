//go:build integration

package api

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Fresh and empty pre-status storage converge without changing intent. A
// populated pre-status development table is refused before any DDL on every
// attempt, rather than defaulting accepted or held notifications to pending.
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

			// Re-create only the empty development table, with its pre-status
			// shape. The rest of the freshly bootstrapped storage stays intact.
			var ddl []byte
			var err error
			if dialect == schema.DialectMySQL {
				ddl, err = schema.MySQLFS.ReadFile("mysql/outbox_events.sql")
			} else {
				ddl, err = schema.PostgresFS.ReadFile("postgres/outbox_events.sql")
			}
			require.NoError(t, err)
			preStatus := strings.ReplaceAll(string(ddl), "  `status` varchar(20) NOT NULL DEFAULT 'pending',\n", "")
			preStatus = strings.ReplaceAll(preStatus, "  status varchar(20) NOT NULL DEFAULT 'pending',\n", "")
			preStatus = strings.ReplaceAll(preStatus, "`idx_status` (`status`,`created_at`,`id`)", "`idx_unpublished` (`published_at`,`created_at`,`id`)")
			preStatus = strings.ReplaceAll(preStatus, "idx_outbox_events_status ON outbox_events (status, created_at, id)", "idx_outbox_events_unpublished ON outbox_events (published_at, created_at, id)")
			createPreStatus := func() {
				t.Helper()
				_, err := db.ExecContext(ctx, "DROP TABLE outbox_events")
				require.NoError(t, err)
				_, err = db.ExecContext(ctx, preStatus)
				require.NoError(t, err)
			}
			createPreStatus()
			require.NoError(t, converge(), "empty pre-status table can add status")
			require.NoError(t, converge())
			var count int
			require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbox_events").Scan(&count))
			assert.Zero(t, count)

			// Every authoritative disposition survives repeated convergence,
			// including held intent that records a late transport acceptance.
			_, err = db.ExecContext(ctx, `INSERT INTO outbox_events (apply_id, event_type, payload, status, published_at, quarantine_token) VALUES
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

			createPreStatus()
			_, err = db.ExecContext(ctx, `INSERT INTO outbox_events (apply_id, event_type, payload, published_at, quarantine_token) VALUES
				(1, 'apply_completed', '{"id":"pending"}', NULL, NULL),
				(2, 'apply_completed', '{"id":"published"}', '2026-01-01 00:00:00', NULL),
				(3, 'apply_completed', '{"id":"quarantined"}', NULL, 'incident')`)
			require.NoError(t, err)
			// Snapshot every stored field and the complete column/index catalogs,
			// not just the absent status column or selected fixture details.
			scope := "DATABASE()"
			indexQuery := `SELECT index_name, seq_in_index, column_name, non_unique, index_type
				FROM information_schema.statistics WHERE table_schema = DATABASE()
				AND table_name = 'outbox_events' ORDER BY index_name, seq_in_index`
			if dialect == schema.DialectPostgres {
				scope = "current_schema()"
				indexQuery = `SELECT indexname, indexdef FROM pg_indexes
					WHERE schemaname = current_schema() AND tablename = 'outbox_events' ORDER BY indexname`
			}
			columnQuery := `SELECT column_name, ordinal_position, data_type, is_nullable, column_default,
				character_maximum_length, numeric_precision, numeric_scale, datetime_precision
				FROM information_schema.columns WHERE table_schema = ` + scope + `
				AND table_name = 'outbox_events' ORDER BY ordinal_position`
			rowQuery := `SELECT * FROM outbox_events ORDER BY id`
			beforeRows := outboxBootstrapSnapshot(t, db, rowQuery)
			beforeColumns := outboxBootstrapSnapshot(t, db, columnQuery)
			beforeIndexes := outboxBootstrapSnapshot(t, db, indexQuery)
			for range 2 {
				require.ErrorContains(t, converge(), "populated outbox_events has no status column")
				assert.Equal(t, beforeRows, outboxBootstrapSnapshot(t, db, rowQuery), "all row fields remain unchanged")
				assert.Equal(t, beforeColumns, outboxBootstrapSnapshot(t, db, columnQuery), "no defaulted or partial column")
				assert.Equal(t, beforeIndexes, outboxBootstrapSnapshot(t, db, indexQuery), "no partial index")
			}
		})
	}
}

// Capture the full result without depending on driver-specific JSON/time types.
func outboxBootstrapSnapshot(t *testing.T, db *sql.DB, query string) [][]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, query)
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	columns, err := rows.Columns()
	require.NoError(t, err)
	var result [][]any
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		require.NoError(t, rows.Scan(pointers...))
		for i, value := range values {
			if b, ok := value.([]byte); ok {
				values[i] = string(b)
			}
		}
		result = append(result, values)
	}
	require.NoError(t, rows.Err())
	return result
}
