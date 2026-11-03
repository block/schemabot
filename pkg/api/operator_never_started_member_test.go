package api

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// haltedRolloutOperationStore holds the claimed operation beside a sibling
// whose failure halted the rollout.
type haltedRolloutOperationStore struct {
	recoverOperationStore
	failedSibling storage.ApplyOperation
}

func (s *haltedRolloutOperationStore) ListByApply(context.Context, int64) ([]*storage.ApplyOperation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, sibling := *s.op, s.failedSibling
	return []*storage.ApplyOperation{&sibling, &op}, nil
}

func (s *haltedRolloutOperationStore) UpdateState(_ context.Context, id int64, newState string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id != s.op.ID {
		return fmt.Errorf("unexpected operation %d", id)
	}
	s.op.State = newState
	return nil
}

// A rollout halts on failure. payments-002 failed while payments-003 was
// being dispatched, and the data plane settled the apply before payments-003
// began, so payments-003's drive ends with ErrRemoteOperationNeverStarts. The
// operation goes back to pending, and the rollout derives failed from a failed
// member and one that never started, instead of holding payments-003 running
// while a driver polls it forever.
func TestRecoverMultiApplyOperation_SettlesAMemberItsRemoteApplySettledBefore(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	applyStore := &casApplyStore{
		template: storage.Apply{
			ID:              7,
			ApplyIdentifier: "apply-halted-rollout",
			Database:        "payments",
			DatabaseType:    storage.DatabaseTypeMySQL,
			Environment:     "staging",
		},
		state: state.Apply.Running,
	}
	claimed := &storage.ApplyOperation{ID: 42, ApplyID: 7, Deployment: "west", Target: "payments-003", State: state.ApplyOperation.Running}
	opStore := &haltedRolloutOperationStore{
		recoverOperationStore: recoverOperationStore{op: claimed},
		failedSibling:         storage.ApplyOperation{ID: 41, ApplyID: 7, Deployment: "west", Target: "payments-002", State: state.ApplyOperation.Failed},
	}
	deploymentClient := &mockTernClient{resumeErr: fmt.Errorf("poll remote apply remote-apply-shared for operation 42: %w", tern.ErrRemoteOperationNeverStarts)}

	svc := New(
		&recoverTestStorage{applies: applyStore, ops: opStore, control: &fakeControlRequestStore{}},
		testServerConfig(),
		map[string]tern.Client{"west/staging": deploymentClient},
		logger,
	)

	svc.recoverMultiApplyOperation(t.Context(), 1, &storage.ApplyOperation{
		ID: 42, ApplyID: 7, Deployment: "west", Target: "payments-003", State: state.ApplyOperation.Running,
	}, storage.OperationLease{})

	assert.Equal(t, state.ApplyOperation.Pending, opStore.op.State, "the member never started, so it goes back to pending")
	assert.Equal(t, state.Apply.Failed, applyStore.currentState(), "a failed member and one that never started settle the halted rollout as failed")
}
