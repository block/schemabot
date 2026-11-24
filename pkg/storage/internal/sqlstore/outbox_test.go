//go:build integration

package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/notifications"
	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/storagetest"
	"github.com/block/schemabot/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Committed intent survives storage reconstruction, and concurrent state or
// lease changes prevent a losing projection from recording intent.
func TestMySQLCompletionOutboxWriters(t *testing.T) {
	testCompletionOutboxRestart(t, func(t *testing.T) *Storage {
		t.Helper()
		clearTables(t)
		return NewMySQL(testDB)
	})
	testCompletingProjectionRaces(t, func(t *testing.T) *Storage {
		t.Helper()
		clearTables(t)
		return NewMySQL(testDB)
	}, mysqlLockWaiter(testDB))
}

// PostgreSQL shares the same persistent intent and concurrent-writer fences.
func TestPostgresCompletionOutboxWriters(t *testing.T) {
	dsn, fixtureDB := testutil.StartPostgres(t, "outbox_atomicity")
	db, err := postgresconn.Open(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.PingContext(t.Context()))
	applyPostgresTestSchema(t, fixtureDB)
	testCompletionOutboxRestart(t, func(t *testing.T) *Storage {
		t.Helper()
		clearPostgresTables(t, db)
		return NewPostgres(db)
	})
	testCompletingProjectionRaces(t, func(t *testing.T) *Storage {
		t.Helper()
		clearPostgresTables(t, db)
		return NewPostgres(db)
	}, postgresLockWaiter(db))
}

func testCompletionOutboxRestart(t *testing.T, newStore func(*testing.T) *Storage) {
	t.Helper()
	store := newStore(t)
	lock := storagetest.CreateLock(t, store, "outbox_db", storage.DatabaseTypeMySQL)
	apply := storagetest.CreateApplyWithStateAndEnv(t, store, lock, "apply_persistent_outbox", 0, state.Apply.Running, "staging")
	apply.State = state.Apply.Completed
	require.NoError(t, store.Applies().Update(t.Context(), apply))
	var restarted *Storage
	if _, mysql := store.db.binder.(MySQLDialect); mysql {
		restarted = NewMySQL(store.db.pool)
	} else {
		restarted = NewPostgres(store.db.pool)
	}
	assertRestartedSenderDelivers(t, restarted, apply.ApplyIdentifier)
}

// A completing projection waits for an in-flight state or token change and
// classifies the committed loss without recording notification intent.
func testCompletingProjectionRaces(t *testing.T, newStore func(*testing.T) *Storage, waiting lockWaiter) {
	t.Helper()
	for _, operationLease := range []bool{false, true} {
		name := "StateChanged"
		if operationLease {
			name = "OperationTokenChanged"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), leaseStealRaceDeadline)
			defer cancel()
			store := newStore(t)
			lock := storagetest.CreateLock(t, store, "outbox_race", storage.DatabaseTypeMySQL)
			apply := storagetest.CreateApplyWithStateAndEnv(t, store, lock, "apply_outbox_race", 0, state.Apply.Running, "staging")
			steal := `UPDATE applies SET state = ? WHERE id = ?`
			args := []any{state.Apply.Stopped, apply.ID}
			if operationLease {
				opID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{ApplyID: apply.ID, Deployment: "region-a", OperationKey: "schema"})
				require.NoError(t, err)
				_, err = store.db.ExecContext(ctx, `UPDATE apply_operations SET lease_owner = ?, lease_token = ? WHERE id = ?`, "driver-a", "token-a", opID)
				require.NoError(t, err)
				ctx = storage.WithOperationLease(ctx, storage.OperationLease{ApplyID: apply.ID, OperationID: opID, Owner: "driver-a", Token: "token-a"})
				steal = `UPDATE apply_operations SET lease_owner = ?, lease_token = ? WHERE id = ?`
				args = []any{"driver-b", "token-b", opID}
			}
			var swapped bool
			err := writeDuringUncommittedSteal(t, store, waiting, "UPDATE applies", func() error {
				var err error
				swapped, err = store.Applies().UpdateDerivedState(ctx, apply.ID, state.Apply.Running, state.Apply.Completed, "", nil, nil)
				return err
			}, steal, args...)
			if operationLease {
				require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
			} else {
				require.NoError(t, err)
			}
			assert.False(t, swapped)
			events, err := store.OutboxEvents().ListUnpublished(ctx, 0, 10)
			require.NoError(t, err)
			assert.Empty(t, events)
		})
	}
}

type completionPublisher struct{ accepted chan json.RawMessage }

func (p completionPublisher) Publish(ctx context.Context, payload json.RawMessage) error {
	select {
	case p.accepted <- append(json.RawMessage(nil), payload...):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// No sender exists at commit time. A fresh sender starts delivery from
// the durable row alone, without another completion or an in-memory signal.
func assertRestartedSenderDelivers(t *testing.T, store *Storage, applyIdentifier string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), leaseStealRaceDeadline)
	defer cancel()
	accepted := make(chan json.RawMessage, 1)
	sender := notifications.NewSender(store.OutboxEvents(), completionPublisher{accepted}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	done := make(chan struct{})
	go func() { defer close(done); sender.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(leaseStealRaceDeadline):
			assert.Fail(t, "restarted sender did not stop")
		}
	})
	select {
	case payload := <-accepted:
		var message storage.ApplyFinishedEvent
		require.NoError(t, json.Unmarshal(payload, &message))
		assert.Equal(t, applyIdentifier, message.ID)
	case <-ctx.Done():
		require.FailNow(t, "restarted sender did not publish committed notification")
	}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		events, err := store.OutboxEvents().ListUnpublished(ctx, 0, 10)
		require.NoError(c, err)
		assert.Empty(c, events)
	}, leaseStealRaceDeadline, 10*time.Millisecond)
}

type rejectQuarantineDiagnosticBinder struct{ binder }

func (b rejectQuarantineDiagnosticBinder) Rebind(query string) string {
	if strings.Contains(query, "INSERT INTO apply_logs") {
		return "INSERT INTO nonexistent_notification_diagnostic (message) VALUES ('rejected')"
	}
	return b.binder.Rebind(query)
}

// A diagnostic write failure rolls back quarantine along with its audit entry.
func TestMySQLNotificationQuarantine(t *testing.T) {
	testNotificationQuarantine(t, func(t *testing.T) *Storage {
		t.Helper()
		clearTables(t)
		return NewMySQL(testDB)
	})
}

// PostgreSQL shares the same quarantine transaction semantics, including
// rollback when the durable diagnostic cannot be written.
func TestPostgresNotificationQuarantine(t *testing.T) {
	dsn, fixtureDB := testutil.StartPostgres(t, "outbox_quarantine")
	db, err := postgresconn.Open(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.PingContext(t.Context()))
	applyPostgresTestSchema(t, fixtureDB)
	testNotificationQuarantine(t, func(t *testing.T) *Storage {
		t.Helper()
		clearPostgresTables(t, db)
		return NewPostgres(db)
	})
}

func testNotificationQuarantine(t *testing.T, newStore func(*testing.T) *Storage) {
	t.Helper()
	store := newStore(t)
	ctx, cancel := context.WithTimeout(t.Context(), leaseStealRaceDeadline)
	defer cancel()
	lock := storagetest.CreateLock(t, store, "outbox_quarantine", storage.DatabaseTypeMySQL)
	apply := storagetest.CreateApplyWithStateAndEnv(t, store, lock, "apply_quarantine", 0, state.Apply.Running, "staging")
	apply.State = state.Apply.Completed
	require.NoError(t, store.Applies().Update(ctx, apply))
	events, err := store.OutboxEvents().ListUnpublished(ctx, 0, 10)
	require.NoError(t, err)
	require.Len(t, events, 1)
	event := events[0]
	original := store.db.binder
	store.db.binder = rejectQuarantineDiagnosticBinder{original}
	require.ErrorContains(t, store.OutboxEvents().Quarantine(ctx, event.ID), "record quarantine diagnostic")
	store.db.binder = original
	var status string
	require.NoError(t, store.db.QueryRowContext(ctx, `SELECT status FROM outbox_events WHERE id = ?`, event.ID).Scan(&status))
	assert.Equal(t, storage.OutboxStatusPending, status)
	logs, err := store.ApplyLogs().GetByApply(ctx, apply.ID)
	require.NoError(t, err)
	assert.Empty(t, logs)
	testNotificationStatuses(t, store)
}

// Delivery and backlog follow status, while repeated acceptance marks retain
// the first timestamp without clearing a quarantine or changing the payload.
func testNotificationStatuses(t *testing.T, store *Storage) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), leaseStealRaceDeadline)
	defer cancel()
	_, err := store.db.ExecContext(ctx, `DELETE FROM outbox_events`)
	require.NoError(t, err)
	created := time.Date(2026, 1, 4, 0, 0, 0, 0, time.UTC)
	for i, status := range []string{storage.OutboxStatusPublished, storage.OutboxStatusPending, storage.OutboxStatusQuarantined} {
		_, err := store.db.ExecContext(ctx, `INSERT INTO outbox_events (id, apply_id, event_type, payload, status, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
			i+1, 1, applyFinishedEventType, `{"id":"apply_status"}`, status, created.Add(-time.Duration(i)*time.Hour))
		require.NoError(t, err)
	}
	events, err := store.OutboxEvents().ListUnpublished(ctx, 0, 10)
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.EqualValues(t, 2, events[0].ID)
	oldest, err := store.OutboxEvents().OldestUnpublishedAt(ctx)
	require.NoError(t, err)
	require.NotNil(t, oldest)
	assert.Equal(t, created.Add(-2*time.Hour), oldest.UTC(), "age follows timestamps, not the first pending ID")
	for i, expected := range []string{storage.OutboxStatusPublished, storage.OutboxStatusPublished, storage.OutboxStatusQuarantined} {
		id := int64(i + 1)
		require.NoError(t, store.OutboxEvents().MarkPublished(ctx, id))
		var status, payload string
		var first, second sql.NullTime
		require.NoError(t, store.db.QueryRowContext(ctx, `SELECT status, payload, published_at FROM outbox_events WHERE id = ?`, id).Scan(&status, &payload, &first))
		assert.Equal(t, expected, status)
		assert.JSONEq(t, `{"id":"apply_status"}`, payload)
		require.True(t, first.Valid)
		require.NoError(t, store.OutboxEvents().MarkPublished(ctx, id))
		require.NoError(t, store.db.QueryRowContext(ctx, `SELECT published_at FROM outbox_events WHERE id = ?`, id).Scan(&second))
		assert.Equal(t, first, second)
	}
}
