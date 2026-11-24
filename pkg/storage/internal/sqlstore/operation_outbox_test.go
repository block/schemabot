//go:build integration

package sqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/testutil"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Operation identities and attempts survive recording and rendering independently
// of sibling retries and the parent apply's aggregate attempt count.
func TestMySQLOperationOutcomeOutbox(t *testing.T) {
	testOperationOutcomeOutbox(t, func(t *testing.T, opts ...storage.Option) *Storage {
		t.Helper()
		clearTables(t)
		return NewMySQL(testDB, opts...)
	})
}

// PostgreSQL also proves that continue recovers a real failed SQL insert before
// committing the operation state and its durable attempt marker.
func TestPostgresOperationOutcomeOutbox(t *testing.T) {
	_, db := testutil.StartPostgres(t, "operation_outcome_outbox")
	applyPostgresTestSchema(t, db)
	testOperationOutcomeOutbox(t, func(t *testing.T, opts ...storage.Option) *Storage {
		t.Helper()
		clearPostgresTables(t, db)
		return NewPostgres(db, opts...)
	})
}

func operationOutboxFixture(t *testing.T, store *Storage, outcome string) (*storage.Apply, *storage.ApplyOperation, context.Context) {
	t.Helper()
	lock := createTestLock(t, store, "operation_events", storage.DatabaseTypeMySQL)
	apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_example", 1, state.Apply.Running, "staging")
	op := &storage.ApplyOperation{ApplyID: apply.ID, Deployment: "region-a", State: outcome}
	id, err := store.ApplyOperations().Insert(t.Context(), op)
	require.NoError(t, err)
	_, err = store.db.ExecContext(t.Context(), `UPDATE apply_operations SET id = ? WHERE id = ?`, 17, id)
	require.NoError(t, err)
	id = 17
	_, err = store.db.ExecContext(t.Context(), `UPDATE apply_operations SET lease_owner = ?, lease_token = ? WHERE id = ?`, "driver", "first-token", id)
	require.NoError(t, err)
	op, err = store.ApplyOperations().Get(t.Context(), id)
	require.NoError(t, err)
	return apply, op, storage.WithOperationLease(t.Context(), op.Lease())
}

func testOperationOutcomeOutbox(t *testing.T, newStore func(*testing.T, ...storage.Option) *Storage) {
	t.Helper()
	t.Run("MappingAndRetry", func(t *testing.T) {
		store := newStore(t, storage.WithChangeEventApp(func(string) string { return "checkout" }))
		apply, first, firstCtx := operationOutboxFixture(t, store, state.ApplyOperation.Running)
		_, err := store.db.ExecContext(t.Context(), `UPDATE applies SET attempt = ? WHERE id = ?`, maxRecoveryAttempts-1, apply.ID)
		require.NoError(t, err)
		sibling := &storage.ApplyOperation{ApplyID: apply.ID, Deployment: "region-b", State: state.ApplyOperation.Running}
		_, err = store.ApplyOperations().Insert(t.Context(), sibling)
		require.NoError(t, err)
		_, err = store.db.ExecContext(t.Context(), `UPDATE apply_operations SET lease_owner = ?, lease_token = ? WHERE id = ?`, "driver", "sibling-token", sibling.ID)
		require.NoError(t, err)
		siblingCtx := storage.WithOperationLease(t.Context(), storage.OperationLease{ApplyID: apply.ID, OperationID: sibling.ID, Owner: "driver", Token: "sibling-token"})
		before := time.Now().UnixNano()
		require.NoError(t, store.ApplyOperations().MarkCompleted(firstCtx, first.ID))
		require.NoError(t, store.ApplyOperations().MarkCompleted(firstCtx, first.ID))
		require.NoError(t, store.ApplyOperations().MarkFailedRetryable(siblingCtx, sibling.ID, "retry"))
		require.NoError(t, store.ApplyOperations().MarkFailedRetryable(siblingCtx, sibling.ID, "retry"))
		// A genuine redispatch increments only this operation, despite the parent's
		// higher retry budget. A recovery retains that attempt while rotating its lease.
		_, err = store.db.ExecContext(t.Context(), `UPDATE applies SET state = ? WHERE id = ?`, state.Apply.FailedRetryable, apply.ID)
		require.NoError(t, err)
		released, err := store.ApplyOperations().ReleaseClaim(siblingCtx, storage.OperationLease{ApplyID: apply.ID, OperationID: sibling.ID, Owner: "driver", Token: "sibling-token"})
		require.NoError(t, err)
		require.True(t, released)
		claimed, err := store.ApplyOperations().FindNextApplyOperation(t.Context(), "retry-driver")
		require.NoError(t, err)
		require.NotNil(t, claimed)
		require.Equal(t, sibling.ID, claimed.ID)
		require.Equal(t, 1, claimed.Attempt)
		// Simulate a crashed driver without admitting a new retry.
		_, err = store.db.ExecContext(t.Context(), `UPDATE applies SET state = ?, updated_at = ? WHERE id = ?`, state.Apply.Running, time.Now().Add(-2*storage.ApplyLeaseStaleAfter), apply.ID)
		require.NoError(t, err)
		_, err = store.db.ExecContext(t.Context(), `UPDATE apply_operations SET updated_at = ? WHERE id = ?`, time.Now().Add(-2*storage.ApplyLeaseStaleAfter), sibling.ID)
		require.NoError(t, err)
		recovered, err := store.ApplyOperations().FindNextApplyOperation(t.Context(), "recovery-driver")
		require.NoError(t, err)
		require.NotNil(t, recovered)
		require.Equal(t, claimed.ID, recovered.ID)
		require.Equal(t, claimed.Attempt, recovered.Attempt)
		require.NotEqual(t, claimed.LeaseToken, recovered.LeaseToken)
		retryCtx := storage.WithOperationLease(t.Context(), recovered.Lease())
		require.ErrorIs(t, store.ApplyOperations().MarkCompleted(siblingCtx, sibling.ID), storage.ErrApplyLeaseLost)
		require.NoError(t, store.ApplyOperations().MarkCompleted(retryCtx, sibling.ID))
		require.NoError(t, store.ApplyOperations().MarkCompleted(retryCtx, sibling.ID))
		events, err := store.OutboxEvents().ListUnpublished(t.Context(), 0, 10)
		require.NoError(t, err)
		require.Len(t, events, 3)
		seen := map[string]bool{}
		for i, row := range events {
			var event storage.OperationFinishedEvent
			require.NoError(t, json.Unmarshal(row.Payload, &event))
			_, err := uuid.Parse(event.EventID)
			require.NoError(t, err)
			assert.False(t, seen[event.EventID])
			seen[event.EventID] = true
			assert.Equal(t, "operation.finished", row.EventType)
			assert.Equal(t, "apply_example", event.ID)
			assert.Equal(t, "checkout", event.App)
			assert.GreaterOrEqual(t, event.OccurredAtNanos, before)
			assert.LessOrEqual(t, event.OccurredAtNanos, time.Now().UnixNano())
			wantID, wantDeployment, wantAttempt, wantOutcome := first.ID, "region-a", 1, state.ApplyOperation.Completed
			if i > 0 {
				wantID, wantDeployment = sibling.ID, "region-b"
				wantOutcome = state.ApplyOperation.FailedRetryable
			}
			if i == 2 {
				wantAttempt, wantOutcome = 2, state.ApplyOperation.Completed
			}
			assert.Equal(t, wantID, event.OperationID)
			assert.Equal(t, wantDeployment, event.Deployment)
			assert.Equal(t, wantAttempt, event.Attempt)
			assert.Equal(t, wantOutcome, event.Outcome)
			assertOperationTemplate(t, event, fmt.Sprintf("operation.finished apply=apply_example operation=%d deployment=%s attempt=%d outcome=%s", wantID, wantDeployment, wantAttempt, wantOutcome))
		}
	})
	for _, outcome := range append(append([]string{}, state.SettledApplyStates...), state.ApplyOperation.FailedRetryable) {
		t.Run(outcome, func(t *testing.T) {
			for _, tc := range []struct {
				name   string
				policy storage.RecordingErrorPolicy
				fault  string
			}{
				{name: "Success"}, {name: "DefaultBlock", fault: "insert"},
				{name: "Continue", policy: storage.RecordingErrorContinue, fault: "insert"},
				{name: "RecoveryError", policy: storage.RecordingErrorContinue, fault: "recover"},
				{name: "ReleaseError", policy: storage.RecordingErrorContinue, fault: "release"},
				{name: "StateError", policy: storage.RecordingErrorContinue, fault: "state"},
				{name: "ReadError", policy: storage.RecordingErrorContinue, fault: "read"},
				{name: "MarkerError", policy: storage.RecordingErrorContinue, fault: "marker"},
				{name: "SavepointError", policy: storage.RecordingErrorContinue, fault: "savepoint"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					store := newStore(t, storage.WithChangeEventsOnRecordingError(tc.policy))
					_, op, ctx := operationOutboxFixture(t, store, state.ApplyOperation.Running)
					reader, logs := captureRecordingFailureSignals(t)
					original := store.db.binder
					if tc.fault != "" {
						store.db.binder = operationOutcomeFaultBinder{recordingPolicyFaultBinder{original, tc.fault}, tc.fault}
					}
					err := store.ApplyOperations().UpdateState(ctx, op.ID, outcome)
					store.db.binder = original
					committedLoss := tc.name == "Continue"
					committed := tc.name == "Success" || committedLoss
					if committed {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
					}
					if tc.name == "DefaultBlock" {
						if _, ok := store.applyOperations.dialect.(PostgresDialect); ok {
							var pgErr *pgconn.PgError
							require.ErrorAs(t, err, &pgErr)
							assert.Equal(t, "42P01", pgErr.Code)
						}
					}
					stored, err := store.ApplyOperations().Get(t.Context(), op.ID)
					require.NoError(t, err)
					if committed {
						assert.Equal(t, outcome, stored.State)
					} else {
						assert.Equal(t, state.ApplyOperation.Running, stored.State)
					}
					events, err := store.OutboxEvents().ListUnpublished(t.Context(), 0, 10)
					require.NoError(t, err)
					if tc.name == "Success" {
						require.Len(t, events, 1)
						var event storage.OperationFinishedEvent
						require.NoError(t, json.Unmarshal(events[0].Payload, &event))
						assert.Equal(t, "operation_events", event.App)
					} else {
						assert.Empty(t, events)
					}
					var recorded sql.NullInt64
					require.NoError(t, store.db.QueryRowContext(t.Context(), `SELECT finished_event_attempt FROM apply_operations WHERE id = ?`, op.ID).Scan(&recorded))
					assert.Equal(t, committed, recorded.Valid)
					if committedLoss {
						assert.Contains(t, logs.String(), "Operation outcome committed without notification intent")
						assert.Contains(t, logs.String(), `"apply_id":"apply_example"`)
						assert.Contains(t, logs.String(), `"attempt":1`)
						logs.Reset()
						require.NoError(t, store.ApplyOperations().UpdateState(ctx, op.ID, outcome))
						assert.Empty(t, logs.String())
						events, err = store.OutboxEvents().ListUnpublished(t.Context(), 0, 10)
						require.NoError(t, err)
						assert.Empty(t, events)
						// The shared counter is collected below without depending on log wording.
					}
					var data metricdata.ResourceMetrics
					require.NoError(t, reader.Collect(t.Context(), &data))
					var count int64
					for _, scope := range data.ScopeMetrics {
						for _, instrument := range scope.Metrics {
							if instrument.Name == "schemabot.outbox.recording_failures_total" {
								for _, point := range instrument.Data.(metricdata.Sum[int64]).DataPoints {
									count += point.Value
								}
							}
						}
					}
					if committedLoss {
						assert.EqualValues(t, 1, count)
					} else {
						assert.Zero(t, count)
						assert.NotContains(t, logs.String(), "committed without notification intent")
					}
				})
			}
		})
	}
}

func assertOperationTemplate(t *testing.T, event storage.OperationFinishedEvent, description string) {
	t.Helper()
	tmpl, err := template.New("message").Funcs(template.FuncMap{"json": func(v any) (string, error) { b, err := json.Marshal(v); return string(b), err }}).Parse(`{"app":{{json .App}},"environment":{{json .Environment}},"timestamp":{{.OccurredAtNanos}},"description":{{json .Description}}}`)
	require.NoError(t, err)
	var output bytes.Buffer
	require.NoError(t, tmpl.Execute(&output, struct {
		storage.OperationFinishedEvent
		Description string
	}{event, fmt.Sprintf("%s apply=%s operation=%d deployment=%s attempt=%d outcome=%s", event.Type, event.ID, event.OperationID, event.Deployment, event.Attempt, event.Outcome)}))
	assert.JSONEq(t, fmt.Sprintf(`{"app":%q,"environment":%q,"timestamp":%d,"description":%q}`, event.App, event.Environment, event.OccurredAtNanos, description), output.String())
}

type operationOutcomeFaultBinder struct {
	binder
	fault string
}

func (b operationOutcomeFaultBinder) Rebind(query string) string {
	switch {
	case b.fault == "state" && strings.Contains(query, "UPDATE apply_operations ao SET"):
		query = strings.Replace(query, "UPDATE apply_operations", "UPDATE nonexistent_operation_table", 1)
	case b.fault == "read" && strings.Contains(query, "FROM apply_operations ao JOIN applies"):
		query = strings.Replace(query, "FROM apply_operations", "FROM nonexistent_operation_table", 1)
	case b.fault == "marker" && strings.Contains(query, "SET finished_event_attempt"):
		query = strings.Replace(query, "UPDATE apply_operations", "UPDATE nonexistent_operation_table", 1)
	}
	return b.binder.Rebind(query)
}
