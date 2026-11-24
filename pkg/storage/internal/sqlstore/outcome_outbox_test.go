//go:build integration

package sqlstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"text/template"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/testutil"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every aggregate writer records its winning outcome under the same install
// policy; a failed insert rolls back the state unless continue was selected.
func TestMySQLApplyOutcomeOutbox(t *testing.T) {
	testApplyOutcomeOutbox(t, func(t *testing.T, opts ...storage.Option) *Storage {
		t.Helper()
		clearTables(t)
		return NewMySQL(testDB, opts...)
	})
}

// PostgreSQL exercises real transaction-aborting insert errors, including the
// savepoint recovery used by background expiry and ended-operation settlement.
func TestPostgresApplyOutcomeOutbox(t *testing.T) {
	_, db := testutil.StartPostgres(t, "apply_outcome_outbox")
	applyPostgresTestSchema(t, db)
	testApplyOutcomeOutbox(t, func(t *testing.T, opts ...storage.Option) *Storage {
		t.Helper()
		clearPostgresTables(t, db)
		return NewPostgres(db, opts...)
	})
}

func testApplyOutcomeOutbox(t *testing.T, newStore func(*testing.T, ...storage.Option) *Storage) {
	t.Helper()
	t.Run("MappingAndCorrections", func(t *testing.T) {
		app := "checkout"
		store := newStore(t, storage.WithChangeEventApp(func(database string) string {
			assert.Equal(t, "outcome_db", database)
			return app
		}))
		lock := createTestLock(t, store, "outcome_db", storage.DatabaseTypeMySQL)
		apply := createTestApplyWithStateAndEnv(t, store, lock, "apply_example", 1, state.Apply.Running, "production")
		before := time.Now()
		swapped, err := store.Applies().UpdateDerivedState(t.Context(), apply.ID, state.Apply.Running, state.Apply.Failed, "", nil, nil)
		require.NoError(t, err)
		require.True(t, swapped)
		events, err := store.OutboxEvents().ListUnpublished(t.Context(), 0, 10)
		require.NoError(t, err)
		require.Len(t, events, 1)
		var first storage.ApplyFinishedEvent
		require.NoError(t, json.Unmarshal(events[0].Payload, &first))
		_, err = uuid.Parse(first.EventID)
		require.NoError(t, err)
		assert.Equal(t, "checkout", first.App)
		assert.Equal(t, "failed", first.Outcome)
		assert.GreaterOrEqual(t, first.OccurredAtNanos, before.UnixNano())
		assert.LessOrEqual(t, first.OccurredAtNanos, time.Now().UnixNano())

		// The configurable JSON envelope is derived solely from the durable record.
		tmpl, err := template.New("message").Funcs(template.FuncMap{"json": func(v any) (string, error) {
			b, err := json.Marshal(v)
			return string(b), err
		}}).Parse(`{"app":{{json .App}},"environment":{{json .Environment}},"timestamp":{{.OccurredAtNanos}},"description":{{json .Description}}}`)
		require.NoError(t, err)
		var rendered bytes.Buffer
		require.NoError(t, tmpl.Execute(&rendered, struct {
			storage.ApplyFinishedEvent
			Description string
		}{first, fmt.Sprintf("%s apply=%s outcome=%s", first.Type, first.ID, first.Outcome)}))
		assert.JSONEq(t, fmt.Sprintf(`{"app":"checkout","environment":"production","timestamp":%d,"description":"apply.finished apply=apply_example outcome=failed"}`, first.OccurredAtNanos), rendered.String())

		// A corrected aggregate can settle again without reusing its delivery key.
		swapped, err = store.Applies().UpdateDerivedState(t.Context(), apply.ID, state.Apply.Failed, state.Apply.RunningDegraded, "", nil, nil)
		require.NoError(t, err)
		require.True(t, swapped)
		app = "new-registration"
		swapped, err = store.Applies().UpdateDerivedState(t.Context(), apply.ID, state.Apply.RunningDegraded, state.Apply.Failed, "", nil, nil)
		require.NoError(t, err)
		require.True(t, swapped)
		events, err = store.OutboxEvents().ListUnpublished(t.Context(), 0, 10)
		require.NoError(t, err)
		require.Len(t, events, 2)
		var second storage.ApplyFinishedEvent
		require.NoError(t, json.Unmarshal(events[1].Payload, &second))
		assert.Equal(t, first.ID, second.ID)
		assert.NotEqual(t, first.EventID, second.EventID)
		assert.Equal(t, "new-registration", second.App)
		var retained storage.ApplyFinishedEvent
		require.NoError(t, json.Unmarshal(events[0].Payload, &retained))
		assert.Equal(t, first, retained)
	})
	for _, writer := range []string{"RetryExpiry", "EndedOperationSettlement"} {
		t.Run(writer, func(t *testing.T) {
			for _, tc := range []struct {
				name       string
				policy     storage.RecordingErrorPolicy
				failInsert bool
			}{
				{name: "Success"},
				{name: "Block", policy: storage.RecordingErrorBlock, failInsert: true},
				{name: "Continue", policy: storage.RecordingErrorContinue, failInsert: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					store := newStore(t, storage.WithChangeEventsOnRecordingError(tc.policy))
					var apply *storage.Apply
					var run func() error
					if writer == "RetryExpiry" {
						lock := createTestLock(t, store, "outcome_expiry_db", storage.DatabaseTypeMySQL)
						apply = createTestApplyWithStateAndEnv(t, store, lock, "apply_outcome_expiry", 1, state.Apply.FailedRetryable, "staging")
						setApplyAttempt(t, store, apply.ID, state.Apply.FailedRetryable, maxRecoveryAttempts, 0)
						run = func() error {
							_, err := store.Applies().ExpireRetryable(t.Context(), endedOperationStopTypes, 10)
							return err
						}
					} else {
						var parkedID int64
						apply, _, parkedID = createParkedBehindRetryApply(t, store, "apply_outcome_settlement")
						_, err := store.db.ExecContext(t.Context(), "UPDATE apply_operations SET attempt = ? WHERE apply_id = ? AND state = ?", maxRecoveryAttempts, apply.ID, state.ApplyOperation.FailedRetryable)
						require.NoError(t, err)
						_, err = store.db.ExecContext(t.Context(), "UPDATE apply_operations SET state = ? WHERE id = ?", state.ApplyOperation.Stopped, parkedID)
						require.NoError(t, err)
						_, err = store.db.ExecContext(t.Context(), "UPDATE tasks SET state = ? WHERE apply_operation_id = ?", state.Task.Stopped, parkedID)
						require.NoError(t, err)
						_, _, err = store.ControlRequests().RequestPending(t.Context(), &storage.ApplyControlRequest{
							ApplyID: apply.ID, Operation: storage.ControlOperationStop, Status: storage.ControlRequestPending,
							RequestedBy: storage.EndedOperationStopCaller, Metadata: []byte(`{}`),
						})
						require.NoError(t, err)
						run = func() error {
							_, err := store.Applies().SettleAppliesStoppedForEndedOperation(t.Context(), 10)
							return err
						}
					}
					original := store.db.binder
					if tc.failInsert {
						store.db.binder = rejectOutboxInsertBinder{original}
					}
					err := run()
					store.db.binder = original
					blocked := tc.failInsert && tc.policy == storage.RecordingErrorBlock
					if blocked {
						require.ErrorContains(t, err, "enqueue apply outcome notification")
					} else {
						require.NoError(t, err)
					}
					stored, err := store.Applies().Get(t.Context(), apply.ID)
					require.NoError(t, err)
					if blocked {
						assert.Equal(t, state.Apply.FailedRetryable, stored.State)
					} else {
						assert.Equal(t, state.Apply.Failed, stored.State)
					}
					events, err := store.OutboxEvents().ListUnpublished(t.Context(), 0, 10)
					require.NoError(t, err)
					if tc.failInsert {
						assert.Empty(t, events)
					} else {
						require.Len(t, events, 1)
						var event storage.ApplyFinishedEvent
						require.NoError(t, json.Unmarshal(events[0].Payload, &event))
						assert.Equal(t, apply.ApplyIdentifier, event.ID)
						assert.Equal(t, state.Apply.Failed, event.Outcome)
						require.NoError(t, run())
						again, err := store.OutboxEvents().ListUnpublished(t.Context(), 0, 10)
						require.NoError(t, err)
						assert.Len(t, again, 1, "a losing sweep does not record a second event")
					}
				})
			}
		})
	}
}
