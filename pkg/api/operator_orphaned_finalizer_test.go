package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestUpdateApplyStateFromOperations_SettlesPastRowsNothingWillStart verifies
// the stored rollout verdict over rows no claim will ever start. In a
// targets-list rollout of deployment payments-a under continue, shard -80 of
// payments-001 fails and payments-002 completes, which leaves payments-001's
// orders finalizer pending with nothing that will start it: the apply settles
// failed rather than sitting running_degraded. Under halt, region-a fails and
// a stop caught region-b's finalizer before any driver claimed it: the apply
// settles failed, as it would with the finalizer still pending, while a
// finalizer that had started before the stop holds it running_degraded.
func TestUpdateApplyStateFromOperations_SettlesPastRowsNothingWillStart(t *testing.T) {
	started := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	row := func(id int64, deployment, target, key, kind, opState, onFailure string, didStart bool) *storage.ApplyOperation {
		op := &storage.ApplyOperation{
			ID: id, Deployment: deployment, Target: target,
			OperationKey: key, OperationKind: kind,
			State: opState, OnFailure: onFailure,
		}
		if didStart {
			op.StartedAt = &started
		}
		return op
	}
	shard := func(id int64, target, shardName, opState string) *storage.ApplyOperation {
		return row(id, "payments-a", target,
			storage.TargetOperationKey(target, storage.ShardOperationKey("orders", shardName, "orders")),
			storage.ApplyOperationKindWork, opState, storage.OnFailureContinue, true)
	}
	finalizer := func(id int64, target, opState string, didStart bool) *storage.ApplyOperation {
		return row(id, "payments-a", target,
			storage.TargetOperationKey(target, "orders/group_finalizer"),
			storage.ApplyOperationKindGroupFinalizer, opState, storage.OnFailureContinue, didStart)
	}
	regionFinalizer := func(id int64, deployment, opState string, didStart bool) *storage.ApplyOperation {
		return row(id, deployment, "", "group_finalizer",
			storage.ApplyOperationKindGroupFinalizer, opState, storage.OnFailureHalt, didStart)
	}

	cases := []struct {
		name      string
		ops       []*storage.ApplyOperation
		wantState string
	}{
		{
			name: "continue settles failed past a finalizer its own failed work orphaned",
			ops: []*storage.ApplyOperation{
				shard(1, "payments-001", "-80", state.ApplyOperation.Failed),
				shard(2, "payments-001", "80-", state.ApplyOperation.Completed),
				finalizer(3, "payments-001", state.ApplyOperation.Pending, false),
				shard(4, "payments-002", "-80", state.ApplyOperation.Completed),
				shard(5, "payments-002", "80-", state.ApplyOperation.Completed),
				finalizer(6, "payments-002", state.ApplyOperation.Completed, true),
			},
			wantState: state.Apply.Failed,
		},
		{
			name: "halt settles failed past a finalizer stopped before it started",
			ops: []*storage.ApplyOperation{
				regionFinalizer(1, "region-a", state.ApplyOperation.Failed, true),
				regionFinalizer(2, "region-b", state.ApplyOperation.Stopped, false),
			},
			wantState: state.Apply.Failed,
		},
		{
			name: "halt holds running_degraded over a finalizer stopped after it started",
			ops: []*storage.ApplyOperation{
				regionFinalizer(1, "region-a", state.ApplyOperation.Failed, true),
				regionFinalizer(2, "region-b", state.ApplyOperation.Stopped, true),
			},
			wantState: state.Apply.RunningDegraded,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			applyStore := &recordingApplyStore{swapped: true}
			svc := newOperatorStateTestService(&listingApplyOperationStore{ops: tc.ops}, applyStore)
			apply := &storage.Apply{
				ID:              3,
				ApplyIdentifier: "apply-rollout-settle",
				State:           state.Apply.Running,
				Environment:     "staging",
			}

			_, err := svc.updateApplyStateFromOperations(t.Context(), 1, apply, allowLeaseScopedFailedReopen)
			require.NoError(t, err)
			require.NotNil(t, applyStore.updated, "the derived state differs from running, so the apply is persisted")
			assert.Equal(t, tc.wantState, applyStore.updated.State)
		})
	}
}
