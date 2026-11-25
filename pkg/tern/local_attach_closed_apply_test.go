package tern

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A store refusal that read the apply's state carries it through any wrapping,
// so the refusal names the state the apply actually ended in. A bare sentinel
// names none, and the refusal must not guess one.
func TestStoredStateOfInactiveApply(t *testing.T) {
	notActive := &storage.ApplyNotActiveError{ApplyID: "apply-1", State: state.Apply.Cancelled, Detail: "attach operation payments-002 to apply apply-1 in state cancelled"}
	assert.Equal(t, state.Apply.Cancelled, storedStateOfInactiveApply(fmt.Errorf("attach operation: %w", notActive)))
	assert.True(t, errors.Is(notActive, storage.ErrApplyNotActive), "the typed refusal still matches the sentinel")
	assert.Empty(t, storedStateOfInactiveApply(fmt.Errorf("attach operation: %w", storage.ErrApplyNotActive)))
	assert.Empty(t, storedStateOfInactiveApply(errors.New("connection reset")))
}

// The refusal reports the state the store read, both in its message and as a
// field the control plane settles the refused work against. The copy of the
// apply loaded before the attach can still read running, so the refusal never
// prints it.
func TestRefuseAttachToTerminalApplyReportsTheStoredState(t *testing.T) {
	client := &LocalClient{logger: discardLogger()}
	req := &ternv1.ApplyRequest{Database: "payments", Environment: "staging", IdempotencyKey: "key-1"}
	apply := &storage.Apply{ApplyIdentifier: "apply-1", Database: "payments", Environment: "staging", State: state.Apply.RunningDegraded}

	resp := client.refuseAttachToTerminalApply(t.Context(), req, apply, state.Apply.Cancelled, "payments-002")
	assert.False(t, resp.Accepted)
	assert.Equal(t, state.Apply.Cancelled, resp.ClosedApplyState)
	assert.Equal(t, "apply apply-1 for this idempotency key is terminal (cancelled); operation payments-002 cannot attach", resp.ErrorMessage)
	assert.NotContains(t, resp.ErrorMessage, state.Apply.RunningDegraded)

	unknown := client.refuseAttachToTerminalApply(t.Context(), req, apply, "", "payments-002")
	assert.False(t, unknown.Accepted)
	assert.Empty(t, unknown.ClosedApplyState, "a refusal without a stored state names none")
	assert.Equal(t, "apply apply-1 for this idempotency key is no longer active; operation payments-002 cannot attach", unknown.ErrorMessage)
}

// A concurrent dispatch of the same operation attached it, and the apply
// closed behind it, before this dispatch's insert was refused. The operation
// ran, so the dispatch replays the winner's row rather than telling the
// control plane the work never started. With no winner the refusal names the
// closed state, and a re-read that fails returns an error so the dispatch is
// retried rather than settled on a guess.
func TestResolveAttachToClosedApply(t *testing.T) {
	apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-1", Database: "payments", Environment: "staging", State: state.Apply.Running}
	req := &ternv1.ApplyRequest{Database: "payments", Environment: "staging", IdempotencyKey: "key-1"}
	refused := &storage.ApplyNotActiveError{ApplyID: "apply-1", State: state.Apply.Failed, Detail: "attach operation payments-002 to apply apply-1 in state failed"}
	clientWith := func(operations *mockApplyOperationStore) *LocalClient {
		return &LocalClient{
			config:  LocalConfig{Database: "default"},
			storage: &mockStorage{operations: operations},
			logger:  discardLogger(),
		}
	}

	t.Run("a concurrent attach won", func(t *testing.T) {
		client := clientWith(&mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
			42: {ID: 42, ApplyID: apply.ID, Deployment: "default", OperationKey: "payments-002", State: state.ApplyOperation.Failed},
		}})
		resp, err := client.resolveAttachToClosedApply(t.Context(), req, apply, "payments-002", refused)
		require.NoError(t, err)
		assert.True(t, resp.Accepted, "the dispatch replays the operation that ran")
		assert.Equal(t, "42", resp.ApplyOperationId)
		assert.Equal(t, "apply-1", resp.ApplyId)
		assert.Empty(t, resp.ClosedApplyState, "work that ran is never reported as never started")
	})

	t.Run("no attach won", func(t *testing.T) {
		client := clientWith(&mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{}})
		resp, err := client.resolveAttachToClosedApply(t.Context(), req, apply, "payments-002", refused)
		require.NoError(t, err)
		assert.False(t, resp.Accepted)
		assert.Equal(t, state.Apply.Failed, resp.ClosedApplyState)
	})

	t.Run("the re-read fails", func(t *testing.T) {
		client := clientWith(&mockApplyOperationStore{getByKeyErr: errors.New("storage unavailable")})
		resp, err := client.resolveAttachToClosedApply(t.Context(), req, apply, "payments-002", refused)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "payments-002")
		assert.Contains(t, err.Error(), "storage unavailable")
	})
}
