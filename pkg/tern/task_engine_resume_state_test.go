package tern

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
)

// failingListApplyOperationStore fails ListByApply, so the cutover decision a
// drive acts on cannot be read.
type failingListApplyOperationStore struct {
	*controlTestApplyOperationStore
	err error
}

func (s *failingListApplyOperationStore) ListByApply(context.Context, int64) ([]*storage.ApplyOperation, error) {
	return nil, s.err
}

// The resume state a control or progress request carries decides which engine
// work the request addresses. A state the engine persisted always wins. With
// none persisted, a grouped drive (Vitess, a MySQL apply the operator deferred,
// or a MySQL operation that parks at a cutover barrier on its own) sends its own
// context, so the request goes out without one, and a drive of one task at a
// time is addressed by the task identity it sent with Apply. Every storage
// uncertainty is an error rather than a guess, so a durable request is never
// settled against work it did not name.
func TestTaskEngineResumeState(t *testing.T) {
	const applyID = int64(7)
	operationID := int64(70)
	siblingID := int64(71)
	task := &storage.Task{TaskIdentifier: "task-orders", ApplyID: applyID, ApplyOperationID: &operationID, TableName: "orders"}
	storeErr := errors.New("connection reset by peer")

	applyOf := func(databaseType string, opts storage.ApplyOptions) *storage.Apply {
		apply := &storage.Apply{ID: applyID, ApplyIdentifier: "apply-orders", Database: "orders", DatabaseType: databaseType}
		apply.SetOptions(opts)
		return apply
	}
	operation := func(id int64, policy string) *storage.ApplyOperation {
		return &storage.ApplyOperation{ID: id, ApplyID: applyID, CutoverPolicy: policy}
	}

	tests := []struct {
		name       string
		applies    *exactProgressApplyStore
		operations storage.ApplyOperationStore
		task       *storage.Task
		want       *engine.ResumeState
		wantErr    string
		wantErrIs  error
	}{
		{
			name:    "a persisted state wins without reading the apply",
			applies: &exactProgressApplyStore{err: storeErr},
			operations: &controlTestApplyOperationStore{data: &storage.EngineResumeState{
				MigrationContext: "engine-ctx-orders", Metadata: `{"branch":"b1"}`,
			}},
			want: &engine.ResumeState{MigrationContext: "engine-ctx-orders", Metadata: `{"branch":"b1"}`},
		},
		{
			name:       "a PostgreSQL task is addressed by its drive's identity",
			applies:    &exactProgressApplyStore{apply: applyOf(storage.DatabaseTypePostgres, storage.ApplyOptions{})},
			operations: &controlTestApplyOperationStore{operations: []*storage.ApplyOperation{operation(operationID, "")}},
			want:       &engine.ResumeState{MigrationContext: "task-orders"},
		},
		{
			name:       "a Vitess apply groups, so the request carries no state",
			applies:    &exactProgressApplyStore{apply: applyOf(storage.DatabaseTypeVitess, storage.ApplyOptions{})},
			operations: &controlTestApplyOperationStore{operations: []*storage.ApplyOperation{operation(operationID, "")}},
		},
		{
			name:       "a MySQL apply the operator deferred groups",
			applies:    &exactProgressApplyStore{apply: applyOf(storage.DatabaseTypeMySQL, storage.ApplyOptions{DeferCutover: true})},
			operations: &controlTestApplyOperationStore{operations: []*storage.ApplyOperation{operation(operationID, "")}},
		},
		{
			name:    "a MySQL operation that parks at a cutover barrier on its own groups",
			applies: &exactProgressApplyStore{apply: applyOf(storage.DatabaseTypeMySQL, storage.ApplyOptions{})},
			operations: &controlTestApplyOperationStore{operations: []*storage.ApplyOperation{
				operation(operationID, storage.CutoverPolicyBarrier), operation(siblingID, storage.CutoverPolicyBarrier),
			}},
		},
		{
			name:    "a rolling MySQL operation is driven a table at a time",
			applies: &exactProgressApplyStore{apply: applyOf(storage.DatabaseTypeMySQL, storage.ApplyOptions{})},
			operations: &controlTestApplyOperationStore{operations: []*storage.ApplyOperation{
				operation(operationID, storage.CutoverPolicyRolling), operation(siblingID, storage.CutoverPolicyRolling),
			}},
			want: &engine.ResumeState{MigrationContext: "task-orders"},
		},
		{
			name:       "a missing apply row is an error",
			applies:    &exactProgressApplyStore{},
			operations: &controlTestApplyOperationStore{},
			wantErr:    "the apply of task task-orders does not exist; its engine work cannot be addressed for cancel",
		},
		{
			name:       "a failed apply read is an error",
			applies:    &exactProgressApplyStore{err: storeErr},
			operations: &controlTestApplyOperationStore{},
			wantErrIs:  storeErr,
		},
		{
			name:       "a failed persisted state read is an error",
			applies:    &exactProgressApplyStore{apply: applyOf(storage.DatabaseTypePostgres, storage.ApplyOptions{})},
			operations: &controlTestApplyOperationStore{err: storeErr},
			wantErrIs:  storeErr,
		},
		{
			name:       "a failed operation list is an error",
			applies:    &exactProgressApplyStore{apply: applyOf(storage.DatabaseTypeMySQL, storage.ApplyOptions{})},
			operations: &failingListApplyOperationStore{controlTestApplyOperationStore: &controlTestApplyOperationStore{}, err: storeErr},
			wantErrIs:  storeErr,
		},
		{
			name:       "a task naming an operation the apply lacks is an error",
			applies:    &exactProgressApplyStore{apply: applyOf(storage.DatabaseTypeMySQL, storage.ApplyOptions{})},
			operations: &controlTestApplyOperationStore{operations: []*storage.ApplyOperation{operation(siblingID, "")}},
			wantErr:    "task task-orders names apply operation 70, which the apply does not have",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &LocalClient{
				storage: &controlTestStorage{applies: tt.applies, applyOperations: tt.operations},
				logger:  slog.Default(),
			}
			got, err := client.taskEngineResumeState(t.Context(), task, "cancel")
			switch {
			case tt.wantErrIs != nil:
				require.ErrorIs(t, err, tt.wantErrIs)
				assert.Nil(t, got)
			case tt.wantErr != "":
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, got)
			default:
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
