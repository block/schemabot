//go:build integration

package sqlstore

import (
	"context"
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

type rejectOutboxInsertBinder struct{ binder }

func (b rejectOutboxInsertBinder) Rebind(query string) string {
	if strings.Contains(query, "INSERT INTO outbox_events") {
		return "INSERT INTO nonexistent_notification_table (payload) VALUES ('rejected')"
	}
	return b.binder.Rebind(query)
}

// A notification insert failure rolls back completion, and a fresh storage
// instance sees intent committed by a successful retry without another write.
func TestMySQLCompletionOutboxRollback(t *testing.T) {
	testCompletionOutboxRollback(t, func(t *testing.T) *Storage {
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

// PostgreSQL uses the same atomic completion boundary as MySQL, including when
// the insert aborts its transaction and storage is reconstructed after a later commit.
func TestPostgresCompletionOutboxRollback(t *testing.T) {
	dsn, fixtureDB := testutil.StartPostgres(t, "outbox_atomicity")
	db, err := postgresconn.Open(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.PingContext(t.Context()))
	applyPostgresTestSchema(t, fixtureDB)
	testCompletionOutboxRollback(t, func(t *testing.T) *Storage {
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

func testCompletionOutboxRollback(t *testing.T, newStore func(*testing.T) *Storage) {
	t.Helper()
	for _, derived := range []bool{false, true} {
		name := "Direct"
		if derived {
			name = "Derived"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			store := newStore(t)
			lock := storagetest.CreateLock(t, store, "outbox_db", storage.DatabaseTypeMySQL)
			apply := &storage.Apply{ApplyIdentifier: "apply_atomic_outbox", LockID: lock.ID,
				Database: "outbox_db", DatabaseType: storage.DatabaseTypeMySQL,
				Environment: "staging", State: state.Apply.Running}
			id, err := store.Applies().Create(ctx, apply)
			require.NoError(t, err)
			apply.ID = id
			now := time.Now()
			complete := func() error {
				if derived {
					swapped, err := store.Applies().UpdateDerivedState(ctx, id, state.Apply.Running, state.Apply.Completed, "", nil, &now)
					if err == nil {
						require.True(t, swapped)
					}
					return err
				}
				apply.State = state.Apply.Completed
				apply.CompletedAt = &now
				return store.Applies().Update(ctx, apply)
			}
			original := store.db.binder
			store.db.binder = rejectOutboxInsertBinder{original}
			require.ErrorContains(t, complete(), "enqueue completion notification")
			store.db.binder = original
			stored, err := store.Applies().Get(ctx, id)
			require.NoError(t, err)
			require.Equal(t, state.Apply.Running, stored.State)
			events, err := store.OutboxEvents().ListUnpublished(ctx, 0, 10)
			require.NoError(t, err)
			require.Empty(t, events)
			require.NoError(t, complete())
			// Reconstruct storage over the persistent database, as on restart.
			var restarted *Storage
			if _, mysql := original.(MySQLDialect); mysql {
				restarted = NewMySQL(store.db.pool)
			} else {
				restarted = NewPostgres(store.db.pool)
			}
			events, err = restarted.OutboxEvents().ListUnpublished(ctx, 0, 10)
			require.NoError(t, err)
			require.Len(t, events, 1)
			assert.Equal(t, id, events[0].ApplyID)
			stored, err = restarted.Applies().Get(ctx, id)
			require.NoError(t, err)
			assert.Equal(t, state.Apply.Completed, stored.State)
			assertRestartedSenderDelivers(t, restarted, apply.ApplyIdentifier)
		})
	}
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
		var message storage.ApplyCompletedEvent
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
