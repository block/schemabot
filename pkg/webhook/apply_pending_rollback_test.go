package webhook

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// pendingRollbackTestStorage provides the stores rollbackAwaitsConfirmation
// reads: plans for the pinned rollback plan and applies for the proof that it
// ran.
type pendingRollbackTestStorage struct {
	emptyStorage
	plans   storage.PlanStore
	applies storage.ApplyStore
}

func (s *pendingRollbackTestStorage) Plans() storage.PlanStore    { return s.plans }
func (s *pendingRollbackTestStorage) Applies() storage.ApplyStore { return s.applies }

type pendingRollbackTestApplyStore struct {
	storage.ApplyStore
	byPlan map[int64]*storage.Apply
	err    error
}

func (s *pendingRollbackTestApplyStore) GetByPlan(_ context.Context, planID int64) (*storage.Apply, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.byPlan[planID], nil
}

// TestRollbackAwaitsConfirmation pins the disposition of every same-PR
// rollback pin an apply can find on its lock. Only a plan that loaded and has
// an apply proves the rollback ran; a pin that proves nothing keeps the lock,
// and a storage failure stays an error so the apply is retried rather than
// deciding either way on a read it never completed.
func TestRollbackAwaitsConfirmation(t *testing.T) {
	rollbackPlan := &storage.Plan{ID: 42, PlanIdentifier: "plan_rollback", Environment: "staging"}
	lockPinning := func(pendingPlanID string) *storage.Lock {
		lock := pinnedRollbackLock()
		lock.PendingPlanID = pendingPlanID
		return lock
	}
	storageErr := errors.New("storage unavailable")

	tests := []struct {
		name         string
		lock         *storage.Lock
		plans        *rollbackConfirmTestPlanStore
		applies      *pendingRollbackTestApplyStore
		wantAwaiting bool
		wantPlan     *storage.Plan
		wantErr      error
	}{
		{
			name:         "plan without an apply is awaiting confirmation",
			lock:         lockPinning(rollbackPendingPlanPrefix + rollbackPlan.PlanIdentifier),
			plans:        &rollbackConfirmTestPlanStore{plans: map[string]*storage.Plan{rollbackPlan.PlanIdentifier: rollbackPlan}},
			applies:      &pendingRollbackTestApplyStore{},
			wantAwaiting: true,
			wantPlan:     rollbackPlan,
		},
		{
			name:  "plan with an apply has run",
			lock:  lockPinning(rollbackPendingPlanPrefix + rollbackPlan.PlanIdentifier),
			plans: &rollbackConfirmTestPlanStore{plans: map[string]*storage.Plan{rollbackPlan.PlanIdentifier: rollbackPlan}},
			applies: &pendingRollbackTestApplyStore{byPlan: map[int64]*storage.Apply{
				rollbackPlan.ID: {ApplyIdentifier: "apply_rollback", PlanID: rollbackPlan.ID},
			}},
			wantAwaiting: false,
			wantPlan:     rollbackPlan,
		},
		{
			// The prefix alone names no plan, so nothing can prove the
			// rollback ran and the lock must stay.
			name:         "pin without a plan identifier is awaiting confirmation",
			lock:         lockPinning(rollbackPendingPlanPrefix),
			plans:        &rollbackConfirmTestPlanStore{err: storageErr},
			applies:      &pendingRollbackTestApplyStore{err: storageErr},
			wantAwaiting: true,
		},
		{
			name:         "pin whose plan no longer exists is awaiting confirmation",
			lock:         lockPinning(rollbackPendingPlanPrefix + "plan_gone"),
			plans:        &rollbackConfirmTestPlanStore{plans: map[string]*storage.Plan{}},
			applies:      &pendingRollbackTestApplyStore{err: storageErr},
			wantAwaiting: true,
		},
		{
			name:    "plan load failure is an error",
			lock:    lockPinning(rollbackPendingPlanPrefix + rollbackPlan.PlanIdentifier),
			plans:   &rollbackConfirmTestPlanStore{err: storageErr},
			applies: &pendingRollbackTestApplyStore{},
			wantErr: storageErr,
		},
		{
			name:    "apply load failure is an error",
			lock:    lockPinning(rollbackPendingPlanPrefix + rollbackPlan.PlanIdentifier),
			plans:   &rollbackConfirmTestPlanStore{plans: map[string]*storage.Plan{rollbackPlan.PlanIdentifier: rollbackPlan}},
			applies: &pendingRollbackTestApplyStore{err: storageErr},
			wantErr: storageErr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &pendingRollbackTestStorage{plans: tt.plans, applies: tt.applies}
			h := unlockTestHandler(t, st, nil)

			plan, awaiting, err := h.rollbackAwaitsConfirmation(t.Context(), tt.lock)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, plan)
				assert.False(t, awaiting)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantAwaiting, awaiting)
			assert.Equal(t, tt.wantPlan, plan)
		})
	}
}
