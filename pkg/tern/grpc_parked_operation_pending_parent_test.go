package tern

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A rollout across orders-001..003 under a barrier cutover policy: orders-001
// has completed, orders-002 is parked at the cutover barrier with its own
// data-plane apply, and orders-003 is still queued. The operator stops the
// rollout, and the copy claim hands orders-002 to a drive so the stop reaches
// its parked data-plane apply.
//
// The drive is handed the parent as storage holds it. For each parent state a
// started rollout can be stored in, the drive delivers the stop to orders-002's
// data-plane apply and stands down. It never sends that parked apply a start:
// the operator asked for a stop, and starting work that is waiting for its
// cutover is not something any parent state may ask the data plane to do.
func TestGRPCClient_ParkedOperationStopNeverStartsItsDataPlaneApply(t *testing.T) {
	for _, parentState := range []string{state.Apply.Pending, state.Apply.Running} {
		t.Run(parentState, func(t *testing.T) {
			server := &capturingTernServer{progressStates: []ternv1.State{
				ternv1.State_STATE_WAITING_FOR_CUTOVER,
				ternv1.State_STATE_STOPPED,
				ternv1.State_STATE_STOPPED,
			}}
			client, cleanup := testCapturingGRPCClient(t, server)
			defer cleanup()

			apply := &storage.Apply{
				ID:              5,
				ApplyIdentifier: "apply-parked-stop",
				PlanID:          50,
				Database:        "orders",
				Environment:     "staging",
				State:           parentState,
			}
			completed := &storage.ApplyOperation{ID: 51, ApplyID: apply.ID, Deployment: "orders-001", State: state.ApplyOperation.Completed, ExternalID: "remote-orders-001"}
			parked := &storage.ApplyOperation{ID: 52, ApplyID: apply.ID, Deployment: "orders-002", State: state.ApplyOperation.WaitingForCutover, ExternalID: "remote-orders-002"}
			queued := &storage.ApplyOperation{ID: 53, ApplyID: apply.ID, Deployment: "orders-003", State: state.ApplyOperation.Pending}
			parkedOpID := parked.ID
			controls := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
				ID: 1, ApplyID: apply.ID, Operation: storage.ControlOperationStop, Status: storage.ControlRequestPending, RequestedBy: "cli:operator",
			}}}
			client.storage = &mockStorage{
				applies: &mockApplyStore{apply: apply},
				tasks: &mockTaskStore{tasks: []*storage.Task{{
					ID: 520, TaskIdentifier: "task-orders-002", ApplyID: apply.ID, ApplyOperationID: &parkedOpID,
					TableName: "orders", Namespace: "orders", DDL: "ALTER TABLE `orders` ADD COLUMN `note` text",
					DDLAction: "alter", State: state.Task.WaitingForCutover,
				}}},
				plans:           &mockPlanStore{plan: &storage.Plan{ID: apply.PlanID, PlanIdentifier: "plan-parked-stop"}},
				logs:            &mockApplyLogStore{},
				controlRequests: controls,
				operations: &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
					completed.ID: completed, parked.ID: parked, queued.ID: queued,
				}},
			}

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			err := client.ResumeApplyOperation(ctx, apply, parked.ID)
			require.NoError(t, err)

			server.mu.Lock()
			startCalls, stopCalls, stopApplyID := server.startCalls, server.stopCalls, server.stopApplyID
			server.mu.Unlock()
			assert.Zero(t, startCalls, "a parked data-plane apply is never started by a drive delivering a stop")
			assert.Equal(t, 1, stopCalls, "the stop is delivered once")
			assert.Equal(t, "remote-orders-002", stopApplyID, "the stop reaches the parked operation's own data-plane apply")
		})
	}
}
