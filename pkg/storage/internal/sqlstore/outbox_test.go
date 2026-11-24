//go:build integration

package sqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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

// Event metadata captures the configured application label and completion time.
func TestMySQLApplyEventMetadata(t *testing.T) {
	testApplyEventMetadata(t, func(t *testing.T, opts ...storage.Option) *Storage {
		t.Helper()
		clearTables(t)
		return NewMySQL(testDB, opts...)
	})
}

// PostgreSQL records the same immutable completion payload.
func TestPostgresApplyEventMetadata(t *testing.T) {
	_, db := testutil.StartPostgres(t, "apply_outcome_outbox")
	applyPostgresTestSchema(t, db)
	testApplyEventMetadata(t, func(t *testing.T, opts ...storage.Option) *Storage {
		t.Helper()
		clearPostgresTables(t, db)
		return NewPostgres(db, opts...)
	})
}

func testApplyEventMetadata(t *testing.T, newStore func(*testing.T, ...storage.Option) *Storage) {
	t.Helper()
	store := newStore(t, storage.WithChangeEventApp(func(database string) string {
		assert.Equal(t, "outcome_db", database)
		return "checkout"
	}))
	lock := createTestLock(t, store, "outcome_db", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_example", 1, state.Apply.Running, "production")
	before := time.Now()
	swapped, err := store.Applies().UpdateDerivedState(t.Context(), apply.ID, state.Apply.Running, state.Apply.Completed, "", nil, nil)
	require.NoError(t, err)
	require.True(t, swapped)
	events, err := store.OutboxEvents().ListUnpublished(t.Context(), 0, 10)
	require.NoError(t, err)
	require.Len(t, events, 1)
	var event storage.ApplyFinishedEvent
	require.NoError(t, json.Unmarshal(events[0].Payload, &event))
	assert.Equal(t, "checkout", event.App)
	assert.Equal(t, state.Apply.Completed, event.Outcome)
	assert.GreaterOrEqual(t, event.OccurredAtNanos, before.UnixNano())
	assert.LessOrEqual(t, event.OccurredAtNanos, time.Now().UnixNano())
}

// Both completion writers recover a failed intent insert without hiding errors
// in state, ownership or transaction handling.
func TestMySQLCompletionRecording(t *testing.T) {
	testCompletionRecording(t, func(t *testing.T) *Storage {
		t.Helper()
		clearTables(t)
		return NewMySQL(testDB)
	}, failingCompletionCommitPool(t, testDB.Driver(), testDSN))
}

// PostgreSQL statement errors abort a transaction until rollback to a savepoint.
// Recording must recover that transaction, not just ignore the insert error.
func TestPostgresCompletionRecording(t *testing.T) {
	dsn, fixtureDB := testutil.StartPostgres(t, "outbox_recording")
	db, err := postgresconn.Open(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.PingContext(t.Context()))
	applyPostgresTestSchema(t, fixtureDB)
	testCompletionRecording(t, func(t *testing.T) *Storage {
		t.Helper()
		clearPostgresTables(t, db)
		return NewPostgres(db)
	}, failingCompletionCommitPool(t, db.Driver(), dsn))
}

// Roll back through the real driver at the commit boundary, then return an
// error. The writer must not report a lost notification for an uncommitted apply.
type completionCommitConnector struct {
	driver driver.Driver
	dsn    string
}

func (c completionCommitConnector) Driver() driver.Driver { return c.driver }

func (c completionCommitConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return completionCommitConn{Conn: conn}, nil
}

type completionCommitConn struct{ driver.Conn }

func (c completionCommitConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return completionCommitTx{Tx: tx}, nil
}

type completionCommitTx struct{ driver.Tx }

func (tx completionCommitTx) Commit() error {
	if err := tx.Rollback(); err != nil {
		return err
	}
	return errors.New("injected completion commit failure")
}

func failingCompletionCommitPool(t *testing.T, d driver.Driver, dsn string) *sql.DB {
	t.Helper()
	db := sql.OpenDB(completionCommitConnector{driver: d, dsn: dsn})
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

type recordingFaultBinder struct {
	binder
	fault string
}

func (b recordingFaultBinder) Rebind(query string) string {
	switch {
	case strings.Contains(query, "INSERT INTO outbox_events"):
		query = strings.Replace(query, "INSERT INTO outbox_events", "INSERT INTO nonexistent_notification_table", 1)
	case b.fault == "state" && strings.Contains(query, "UPDATE applies"):
		query = strings.Replace(query, "UPDATE applies", "UPDATE nonexistent_apply_table", 1)
	case b.fault == "read" && strings.Contains(query, "FROM applies WHERE id = ? AND state = ?"):
		query = strings.Replace(query, "FROM applies", "FROM nonexistent_apply_table", 1)
	case b.fault == "savepoint" && query == "SAVEPOINT completion_notification":
		query = "SAVEPOINT"
	case b.fault == "recover" && query == "ROLLBACK TO SAVEPOINT completion_notification":
		query = "ROLLBACK TO SAVEPOINT nonexistent_notification_savepoint"
	case b.fault == "release" && query == "RELEASE SAVEPOINT completion_notification":
		query = "RELEASE SAVEPOINT nonexistent_notification_savepoint"
	}
	return b.binder.Rebind(query)
}

func testCompletionRecording(t *testing.T, newStore func(*testing.T) *Storage, commitPool *sql.DB) {
	t.Helper()
	outcome := state.Apply.Completed
	for _, derived := range []bool{false, true} {
		writer := "Direct"
		if derived {
			writer = "Derived"
		}
		t.Run(writer, func(t *testing.T) {
			for _, tc := range []struct {
				name      string
				fault     string
				completes bool
				wantError string
			}{
				{name: "InsertError", fault: "insert", completes: true},
				{name: "Success", completes: true},
				{name: "CommitError", fault: "commit", wantError: "injected completion commit failure"},
				{name: "StateError", fault: "state", wantError: "nonexistent_apply_table"},
				{name: "StateReadError", fault: "read", wantError: "read finished apply"},
				{name: "SavepointError", fault: "savepoint", wantError: "save notification recording"},
				{name: "RecoveryError", fault: "recover", wantError: "recover notification recording"},
				{name: "ReleaseError", fault: "release", wantError: "release notification recording"},
				{name: "LeaseLost", fault: "lease"},
				{name: "CASMiss", fault: "cas"},
			} {
				if tc.fault == "cas" && !derived {
					continue
				}
				t.Run(tc.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
					defer cancel()
					store := newStore(t)
					lock := storagetest.CreateLock(t, store, "outbox_recording", storage.DatabaseTypeMySQL)
					apply := storagetest.CreateApplyWithStateAndEnv(t, store, lock, "apply_recording", 0, state.Apply.Running, "staging")
					reader, output := captureRecordingFailureSignals(t)
					originalPool := store.db.pool
					if tc.fault == "commit" {
						store.db.pool = commitPool
					}
					original := store.db.binder
					if tc.fault != "" {
						store.db.binder = recordingFaultBinder{binder: original, fault: tc.fault}
					}
					if tc.fault == "lease" {
						ctx = storage.WithApplyLease(ctx, storage.ApplyLease{ApplyID: apply.ID, Owner: "stale-driver", Token: "stale-token"})
					}
					now := time.Now().UTC().Truncate(time.Second)
					var err error
					if derived {
						expectedState := state.Apply.Running
						if tc.fault == "cas" {
							expectedState = state.Apply.Pending
						}
						var swapped bool
						swapped, err = store.Applies().UpdateDerivedState(ctx, apply.ID, expectedState, outcome, "", nil, &now)
						assert.Equal(t, tc.completes, swapped)
					} else {
						apply.State = outcome
						apply.CompletedAt = &now
						err = store.Applies().Update(ctx, apply)
					}
					store.db.binder = original
					store.db.pool = originalPool
					switch {
					case tc.fault == "lease":
						require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
					case tc.wantError != "":
						require.ErrorContains(t, err, tc.wantError)
					default:
						require.NoError(t, err)
					}
					stored, err := store.Applies().Get(ctx, apply.ID)
					require.NoError(t, err)
					expected := state.Apply.Running
					if tc.completes {
						expected = outcome
					}
					assert.Equal(t, expected, stored.State)
					if tc.completes {
						require.NotNil(t, stored.CompletedAt)
						assert.WithinDuration(t, now, *stored.CompletedAt, time.Second)
					}
					events, err := store.OutboxEvents().ListUnpublished(ctx, 0, 10)
					require.NoError(t, err)
					if tc.fault == "" {
						require.Len(t, events, 1)
					} else {
						assert.Empty(t, events)
					}
					assertRecordingFailureSignals(t, reader, output, tc.completes && tc.fault == "insert")
				})
			}
		})
	}
}

func captureRecordingFailureSignals(t *testing.T) (*metric.ManualReader, *bytes.Buffer) {
	t.Helper()
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	previousLogger := slog.Default()
	output := &bytes.Buffer{}
	slog.SetDefault(slog.New(slog.NewJSONHandler(output, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
		otel.SetMeterProvider(previousProvider)
		require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context())))
	})
	return reader, output
}

func assertRecordingFailureSignals(t *testing.T, reader *metric.ManualReader, output *bytes.Buffer, wantLoss bool) {
	t.Helper()
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &data))
	var count int64
	for _, scope := range data.ScopeMetrics {
		for _, instrument := range scope.Metrics {
			if instrument.Name != "schemabot.outbox.recording_failures_total" {
				continue
			}
			sum, ok := instrument.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			require.Len(t, sum.DataPoints, 1)
			assert.Equal(t, attribute.NewSet(attribute.String("environment", "staging")), sum.DataPoints[0].Attributes)
			count += sum.DataPoints[0].Value
		}
	}
	if !wantLoss {
		assert.Zero(t, count)
		assert.NotContains(t, output.String(), "Apply outcome committed without notification intent")
		return
	}
	assert.EqualValues(t, 1, count)
	var log map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(output.Bytes()), &log))
	assert.Equal(t, "ERROR", log["level"])
	assert.Equal(t, "Apply outcome committed without notification intent; recording failed and will not be retried", log["msg"])
	assert.Equal(t, "apply_recording", log["apply_id"])
	assert.Equal(t, "outbox_recording", log["database"])
	assert.Equal(t, "staging", log["environment"])
	assert.Contains(t, log["error"], "nonexistent_notification_table")
	assert.NotContains(t, output.String(), "payload")
}
