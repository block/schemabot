package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
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

// The engine accepts an immediate stop, but SchemaBot's storage fails while it
// marks the durable stop request complete. Operators read the apply log
// through the CLI and API, and it can reach PR comments, so the entry says
// what happened and what to do without the storage error's host and port. The
// server log keeps the raw cause with the apply's identifiers for triage.
func TestImmediateStopCompletionFailureKeepsStorageErrorOutOfApplyLog(t *testing.T) {
	const storageAddress = "10.0.0.5:3306"
	apply := activeTestApply("apply-stop-storage-down")
	apply.ExternalID = "remote-stop-storage-down"
	client := &mockTernClient{stopResp: &ternv1.StopResponse{Accepted: true, StoppedCount: 1}}
	svc, stores := newControlTestServiceWithStores(client, apply, nil)
	stores.applies.err = errors.New("dial tcp " + storageAddress + ": i/o timeout")
	var logs bytes.Buffer
	svc.logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	svc.tryImmediateStopAfterQueue(t.Context(), client, apply, apply.ExternalID, "cli:alice")

	require.NotNil(t, client.stopReq, "the engine receives the immediate stop")
	require.True(t, hasApplyLogMessageContaining(stores.applyLogs.logs, "SchemaBot could not record it as complete; the durable stop request remains pending"),
		"the apply log records that the stop is still pending")
	for _, entry := range stores.applyLogs.logs {
		assert.NotContains(t, entry.Message, storageAddress, "no apply log entry names the storage address")
		assert.NotContains(t, entry.Message, "dial tcp", "no apply log entry carries the storage error text")
	}

	var failures []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(logs.String()), "\n") {
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry), "log line must be JSON: %s", line)
		if entry["msg"] == "immediate stop accepted but durable stop request completion failed; durable stop request remains pending for apply owner retry" {
			failures = append(failures, entry)
		}
	}
	require.Len(t, failures, 1, "the server log records the completion failure; got: %s", logs.String())
	failure := failures[0]
	assert.Equal(t, "ERROR", failure["level"])
	assert.Equal(t, "complete_immediate_stop", failure["operation"])
	assert.Equal(t, apply.ApplyIdentifier, failure["apply_id"])
	assert.Equal(t, apply.Database, failure["database"])
	assert.Equal(t, apply.Environment, failure["environment"])
	assert.Equal(t, "cli:alice", failure["requested_by"])
	assert.Contains(t, failure["error"], storageAddress, "the server log keeps the raw cause")
}
