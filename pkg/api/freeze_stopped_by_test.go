package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/clock"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// stopRequestStore answers the stop request of every apply with stop.
type stopRequestStore struct {
	storage.ControlRequestStore
	stop *storage.ApplyControlRequest
	err  error
}

func (s *stopRequestStore) GetByOperation(_ context.Context, _ int64, operation storage.ControlOperation) (*storage.ApplyControlRequest, error) {
	if operation != storage.ControlOperationStop {
		return nil, nil
	}
	return s.stop, s.err
}

type stoppedByFreezeStorage struct {
	mockStorage
	control storage.ControlRequestStore
	freezes storage.FreezeStore
}

func (m *stoppedByFreezeStorage) ControlRequests() storage.ControlRequestStore { return m.control }
func (m *stoppedByFreezeStorage) Freezes() storage.FreezeStore                 { return m.freezes }

// Status names the freeze that stopped an apply only when the stop recorded
// on the stopped apply is a freeze's, and still names it when the freeze's
// reason cannot be read.
func TestStoppedByFreeze(t *testing.T) {
	freezes := &memoryFreezeStore{clock: clock.NewFake(freezeTestNow), freezes: []*storage.EnvironmentFreeze{
		{Identifier: "freeze-1", Environment: "staging", Running: storage.FreezeRunningStop, Reason: "SEV-1234"},
	}}
	stopBy := func(caller string, status storage.ControlRequestStatus) *storage.ApplyControlRequest {
		return &storage.ApplyControlRequest{Operation: storage.ControlOperationStop, RequestedBy: caller, Status: status}
	}
	stopped := &storage.Apply{ID: 7, ApplyIdentifier: "apply-7", State: state.Apply.Stopped}
	running := &storage.Apply{ID: 7, ApplyIdentifier: "apply-7", State: state.Apply.Running}

	for _, tc := range []struct {
		name    string
		apply   *storage.Apply
		control *stopRequestStore
		freezes storage.FreezeStore
		want    *apitypes.FreezeStopRef
	}{
		{"FreezeStop", stopped, &stopRequestStore{stop: stopBy("freeze:freeze-1", storage.ControlRequestCompleted)}, freezes,
			&apitypes.FreezeStopRef{ID: "freeze-1", Reason: "SEV-1234"}},
		{"FreezeGoneKeepsTheID", stopped, &stopRequestStore{stop: stopBy("freeze:freeze-gone", storage.ControlRequestCompleted)}, freezes,
			&apitypes.FreezeStopRef{ID: "freeze-gone"}},
		{"NoFreezeStoreKeepsTheID", stopped, &stopRequestStore{stop: stopBy("freeze:freeze-1", storage.ControlRequestCompleted)}, nil,
			&apitypes.FreezeStopRef{ID: "freeze-1"}},
		{"OperatorStop", stopped, &stopRequestStore{stop: stopBy("github:alice@acme/app#1", storage.ControlRequestCompleted)}, freezes, nil},
		{"FailedFreezeStop", stopped, &stopRequestStore{stop: stopBy("freeze:freeze-1", storage.ControlRequestFailed)}, freezes, nil},
		{"NoStopRecorded", stopped, &stopRequestStore{}, freezes, nil},
		{"StopUnreadable", stopped, &stopRequestStore{err: errors.New("connection refused")}, freezes, nil},
		{"NotStopped", running, &stopRequestStore{stop: stopBy("freeze:freeze-1", storage.ControlRequestPending)}, freezes, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stor := &stoppedByFreezeStorage{control: tc.control}
			if tc.freezes != nil {
				stor.freezes = tc.freezes
			}
			svc := New(stor, testServerConfig(), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
			assert.Equal(t, tc.want, svc.StoppedByFreeze(t.Context(), tc.apply))
		})
	}
}
