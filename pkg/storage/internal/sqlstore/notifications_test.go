//go:build integration

package sqlstore

import (
	"database/sql"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/testutil"
)

type notificationQueryCapture struct {
	binder binder
	query  string
}

func (c *notificationQueryCapture) Rebind(query string) string {
	c.query = query
	return c.binder.Rebind(query)
}

func TestNotificationClaimsExpireAndFenceOldSenders(t *testing.T) {
	t.Run("mysql", func(t *testing.T) { clearTables(t); testNotificationExpiry(t, testDB, MySQLDialect{}) })
	t.Run("postgres", func(t *testing.T) {
		_, db := testutil.StartPostgres(t, "notification_claims")
		applyPostgresTestSchema(t, db)
		testNotificationExpiry(t, db, PostgresDialect{})
	})
}

func testNotificationExpiry(t *testing.T, db *sql.DB, dialect interface {
	Dialect
	binder
}) {
	t.Helper()
	capture := &notificationQueryCapture{binder: dialect}
	n := &notificationStore{db: newRebindDB(db, capture), dialect: dialect}
	// Use the dialect's actual public store so this fixture is also eligible for delivery.
	var store *Storage
	switch dialect.(type) {
	case MySQLDialect:
		store = NewMySQL(db)
	case PostgresDialect:
		store = NewPostgres(db)
	}
	apply := &storage.Apply{ApplyIdentifier: "claim-expiry", Database: "notifydb", DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging", State: state.Apply.Completed}
	id, err := store.Applies().Create(t.Context(), apply)
	require.NoError(t, err)
	apply.ID = id
	completed := time.Now().Add(-time.Hour)
	apply.CompletedAt = &completed
	require.NoError(t, store.Applies().Update(t.Context(), apply))
	won, err := n.Claim(t.Context(), id, "crashed-sender", time.Minute)
	require.NoError(t, err)
	require.True(t, won)
	expired := dialect.RelativeTime(TimestampPrecisionMicrosecond, BeforeCurrentTime, LiteralIntervalAmount(1), IntervalSecond)
	_, err = n.db.ExecContext(t.Context(), `UPDATE apply_notifications SET claimed_until = `+expired+` WHERE apply_id = ?`, id)
	require.NoError(t, err)
	missing, err := n.FindUnpublished(t.Context(), 24*time.Hour, 100)
	require.NoError(t, err)
	require.Len(t, missing, 1)
	// Explain the exact delivery statement captured at the binding boundary.
	query := capture.query
	logNotificationPlan(t, n.db, query, int64((24 * time.Hour).Microseconds()), state.Apply.Completed, 100)
	won, err = n.Claim(t.Context(), id, "replacement", time.Minute)
	require.NoError(t, err)
	require.True(t, won)
	changed, err := n.Release(t.Context(), id, "crashed-sender", "old failure")
	require.NoError(t, err)
	assert.False(t, changed)
	changed, err = n.MarkPublished(t.Context(), id, "crashed-sender")
	require.NoError(t, err)
	assert.False(t, changed)
	changed, err = n.Release(t.Context(), id, "replacement", "queue unavailable")
	require.NoError(t, err)
	require.True(t, changed)
	var lastError string
	require.NoError(t, n.db.QueryRowContext(t.Context(), `SELECT last_error FROM apply_notifications WHERE apply_id = ?`, id).Scan(&lastError))
	assert.Equal(t, "queue unavailable", lastError)
	won, err = n.Claim(t.Context(), id, "retry", time.Minute)
	require.NoError(t, err)
	require.True(t, won)
	changed, err = n.MarkPublished(t.Context(), id, "retry")
	require.NoError(t, err)
	require.True(t, changed)
	_, err = n.ObservePage(t.Context(), 0, id, 500)
	require.NoError(t, err)
	query = capture.query
	logNotificationPlan(t, n.db, query, int64(0), id, 500)
}

func logNotificationPlan(t *testing.T, db *rebindDB, query string, args ...any) {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "EXPLAIN "+query, args...)
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	columns, err := rows.Columns()
	require.NoError(t, err)
	for rows.Next() {
		values := make([]any, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		require.NoError(t, rows.Scan(dest...))
		for i, value := range values {
			if text, ok := value.([]byte); ok {
				values[i] = string(text)
			}
		}
		t.Logf("EXPLAIN %v = %v", columns, values)
	}
	require.NoError(t, rows.Err())
}
