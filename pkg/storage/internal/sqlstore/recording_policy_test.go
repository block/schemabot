//go:build integration

package sqlstore

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/storagetest"
	"github.com/block/schemabot/pkg/testutil"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Both completion writers obey the install policy. A real SQL insert error
// rolls back completion by default, but continue recovers and commits it.
func TestMySQLCompletionRecordingPolicy(t *testing.T) {
	testCompletionRecordingPolicy(t, func(t *testing.T, opts ...storage.Option) *Storage {
		t.Helper()
		clearTables(t)
		return NewMySQL(testDB, opts...)
	})
}

// PostgreSQL statement errors abort a transaction until rollback to a savepoint.
// Continue must recover that transaction, not just ignore the insert error.
func TestPostgresCompletionRecordingPolicy(t *testing.T) {
	dsn, fixtureDB := testutil.StartPostgres(t, "outbox_recording_policy")
	db, err := postgresconn.Open(dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.PingContext(t.Context()))
	applyPostgresTestSchema(t, fixtureDB)
	testCompletionRecordingPolicy(t, func(t *testing.T, opts ...storage.Option) *Storage {
		t.Helper()
		clearPostgresTables(t, db)
		return NewPostgres(db, opts...)
	})
}

type recordingPolicyFaultBinder struct {
	binder
	fault string
}

func (b recordingPolicyFaultBinder) Rebind(query string) string {
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

func testCompletionRecordingPolicy(t *testing.T, newStore func(*testing.T, ...storage.Option) *Storage) {
	t.Helper()
	for _, derived := range []bool{false, true} {
		writer := "Direct"
		if derived {
			writer = "Derived"
		}
		t.Run(writer, func(t *testing.T) {
			for _, tc := range []struct {
				name      string
				policy    storage.RecordingErrorPolicy
				fault     string
				completes bool
				wantError string
			}{
				{name: "Omitted", fault: "insert", wantError: "enqueue completion notification"},
				{name: "Block", policy: storage.RecordingErrorBlock, fault: "insert", wantError: "enqueue completion notification"},
				{name: "Continue", policy: storage.RecordingErrorContinue, fault: "insert", completes: true},
				{name: "ContinueSuccess", policy: storage.RecordingErrorContinue, completes: true},
				{name: "StateError", policy: storage.RecordingErrorContinue, fault: "state", wantError: "nonexistent_apply_table"},
				{name: "StateReadError", policy: storage.RecordingErrorContinue, fault: "read", wantError: "read completed apply"},
				{name: "SavepointError", policy: storage.RecordingErrorContinue, fault: "savepoint", wantError: "save notification recording"},
				{name: "RecoveryError", policy: storage.RecordingErrorContinue, fault: "recover", wantError: "recover notification recording"},
				{name: "ReleaseError", policy: storage.RecordingErrorContinue, fault: "release", wantError: "release notification recording"},
				{name: "LeaseLost", policy: storage.RecordingErrorContinue, fault: "lease"},
				{name: "CASMiss", policy: storage.RecordingErrorContinue, fault: "cas"},
			} {
				if tc.fault == "cas" && !derived {
					continue
				}
				t.Run(tc.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
					defer cancel()
					var opts []storage.Option
					if tc.policy != "" {
						opts = append(opts, storage.WithChangeEventsOnRecordingError(tc.policy))
					}
					store := newStore(t, opts...)
					lock := storagetest.CreateLock(t, store, "outbox_policy", storage.DatabaseTypeMySQL)
					apply := storagetest.CreateApplyWithStateAndEnv(t, store, lock, "apply_recording_policy", 0, state.Apply.Running, "staging")
					reader, output := captureRecordingFailureSignals(t)
					original := store.db.binder
					if tc.fault != "" {
						store.db.binder = recordingPolicyFaultBinder{binder: original, fault: tc.fault}
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
						swapped, err = store.Applies().UpdateDerivedState(ctx, apply.ID, expectedState, state.Apply.Completed, "", nil, &now)
						assert.Equal(t, tc.completes, swapped)
					} else {
						apply.State = state.Apply.Completed
						apply.CompletedAt = &now
						err = store.Applies().Update(ctx, apply)
					}
					store.db.binder = original
					switch {
					case tc.fault == "lease":
						require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
					case tc.wantError != "":
						require.ErrorContains(t, err, tc.wantError)
						if tc.fault == "insert" {
							if _, postgres := original.(PostgresDialect); postgres {
								var sqlErr *pgconn.PgError
								require.ErrorAs(t, err, &sqlErr)
								assert.Equal(t, "42P01", sqlErr.Code, "a server-side undefined-table error, not a client argument failure")
							}
						}
					default:
						require.NoError(t, err)
					}
					stored, err := store.Applies().Get(ctx, apply.ID)
					require.NoError(t, err)
					expected := state.Apply.Running
					if tc.completes {
						expected = state.Apply.Completed
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
		assert.NotContains(t, output.String(), "Apply completed without notification intent")
		return
	}
	assert.EqualValues(t, 1, count)
	var log map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(output.Bytes()), &log))
	assert.Equal(t, "ERROR", log["level"])
	assert.Equal(t, "Apply completed without notification intent; recording failed and will not be retried", log["msg"])
	assert.Equal(t, "apply_recording_policy", log["apply_id"])
	assert.Equal(t, "outbox_policy", log["database"])
	assert.Equal(t, "staging", log["environment"])
	assert.Equal(t, "continue", log["recording_error_policy"])
	assert.Contains(t, log["error"], "nonexistent_notification_table")
	assert.NotContains(t, output.String(), "payload")
}
