package api

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// supersededCutoverControlStore is a stateful ControlRequestStore that records
// the order of the writes settling a stop and the cutover it superseded, and
// can fail the first cutover settlement the way a transient storage error
// would.
type supersededCutoverControlStore struct {
	storage.ControlRequestStore
	pending         map[storage.ControlOperation]*storage.ApplyControlRequest
	failCutoverOnce bool
	writes          []string
	failureReasons  map[storage.ControlOperation]string
}

func (s *supersededCutoverControlStore) GetPending(_ context.Context, _ int64, op storage.ControlOperation) (*storage.ApplyControlRequest, error) {
	return s.pending[op], nil
}

func (s *supersededCutoverControlStore) CompletePending(_ context.Context, _ int64, op storage.ControlOperation) error {
	if s.pending[op] != nil {
		delete(s.pending, op)
		s.writes = append(s.writes, "complete "+string(op))
	}
	return nil
}

func (s *supersededCutoverControlStore) FailPending(_ context.Context, _ int64, op storage.ControlOperation, reason string) error {
	if op == storage.ControlOperationCutover && s.failCutoverOnce {
		s.failCutoverOnce = false
		return errors.New("transient storage error")
	}
	if s.pending[op] != nil {
		delete(s.pending, op)
		s.writes = append(s.writes, "fail "+string(op))
		s.failureReasons[op] = reason
	}
	return nil
}

// syncingApplyStore serves a fixed apply and records the remote stop sync.
type syncingApplyStore struct {
	storage.ApplyStore
	apply *storage.Apply
}

func (s *syncingApplyStore) Get(_ context.Context, _ int64) (*storage.Apply, error) {
	stored := *s.apply
	return &stored, nil
}

func (s *syncingApplyStore) Update(_ context.Context, apply *storage.Apply) error {
	stored := *apply
	s.apply = &stored
	return nil
}

// Every API path that completes a stop request answers the cutover queued
// before the stop first, while the stop is still pending. When that settlement
// fails, the stop stays pending, so the retry settles the cutover before the
// stop gate comes down and a later start cannot carry the old cutover into a
// table swap (CO-4).
func TestStopCompletionSettlesSupersededCutoverFirst(t *testing.T) {
	stoppedOps := &markPendingStoppedRecordingStore{ops: []*storage.ApplyOperation{
		{ID: 1, Deployment: "region-a", State: state.ApplyOperation.Stopped},
		{ID: 2, Deployment: "region-b", State: state.ApplyOperation.Stopped},
	}}
	cases := []struct {
		name       string
		applyState string
		complete   func(t *testing.T, svc *Service, apply *storage.Apply) error
	}{
		{
			name:       "immediate stop",
			applyState: state.Apply.Stopped,
			complete: func(t *testing.T, svc *Service, apply *storage.Apply) error {
				_, err := svc.completeImmediateStopRequestIfStopped(t.Context(), apply, "cli:bob")
				return err
			},
		},
		{
			name:       "stop resolved before start",
			applyState: state.Apply.Stopped,
			complete: func(t *testing.T, svc *Service, apply *storage.Apply) error {
				return svc.completeResolvedStopBeforeStart(t.Context(), &mockTernClient{}, apply, "cli:carol")
			},
		},
		{
			name:       "remote stop resolved before start",
			applyState: state.Apply.Running,
			complete: func(t *testing.T, svc *Service, apply *storage.Apply) error {
				client := &mockTernClient{isRemote: true, progressResp: &ternv1.ProgressResponse{State: ternv1.State_STATE_STOPPED}}
				return svc.completeResolvedStopBeforeStart(t.Context(), client, apply, "cli:carol")
			},
		},
		{
			name:       "stopped apply settled by the operator",
			applyState: state.Apply.Stopped,
			complete: func(t *testing.T, svc *Service, apply *storage.Apply) error {
				return svc.completePendingControlRequestsIfApplyResolved(t.Context(), svc.driverLogger(1), apply.ID)
			},
		},
		{
			name:       "held-open apply settled by the operator",
			applyState: state.Apply.RunningDegraded,
			complete: func(t *testing.T, svc *Service, apply *storage.Apply) error {
				return svc.completePendingControlRequestsIfApplyResolved(t.Context(), svc.driverLogger(1), apply.ID)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			apply := &storage.Apply{ID: 9, ApplyIdentifier: "apply-stop-cutover", ExternalID: "remote-stop-cutover", Environment: "staging", State: tc.applyState}
			control := &supersededCutoverControlStore{
				pending: map[storage.ControlOperation]*storage.ApplyControlRequest{
					storage.ControlOperationCutover: {ApplyID: apply.ID, Operation: storage.ControlOperationCutover, Status: storage.ControlRequestPending, RequestedBy: "cli:alice"},
					storage.ControlOperationStop:    {ApplyID: apply.ID, Operation: storage.ControlOperationStop, Status: storage.ControlRequestPending, RequestedBy: "cli:bob"},
				},
				failCutoverOnce: true,
				failureReasons:  map[storage.ControlOperation]string{},
			}
			svc := newStopReconcileTestService(&syncingApplyStore{apply: apply}, stoppedOps, control)

			err := tc.complete(t, svc, apply)
			require.ErrorContains(t, err, "transient storage error")
			assert.Empty(t, control.writes, "a failed cutover settlement must leave the stop pending")
			assert.NotNil(t, control.pending[storage.ControlOperationStop])

			require.NoError(t, tc.complete(t, svc, apply))
			assert.Equal(t, []string{"fail cutover", "complete stop"}, control.writes,
				"the cutover queued before the stop is settled before the stop completes")
			assert.Contains(t, control.failureReasons[storage.ControlOperationCutover], "superseded by stop")
		})
	}
}
