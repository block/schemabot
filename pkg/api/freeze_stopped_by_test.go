package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/clock"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
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
// reason cannot be read. It says when that freeze has ended or been lifted,
// and reads it as in effect when the database clock cannot be read.
func TestStoppedByFreeze(t *testing.T) {
	lifted, ended := freezeTestNow.Add(-time.Minute), freezeTestNow.Add(-time.Minute)
	freezeRows := func() []*storage.EnvironmentFreeze {
		return []*storage.EnvironmentFreeze{
			{Identifier: "freeze-1", Environment: "staging", Running: storage.FreezeRunningStop, Reason: "SEV-1234"},
			{Identifier: "freeze-lifted", Environment: "staging", Running: storage.FreezeRunningStop, Reason: "SEV-1", LiftedAt: &lifted},
			{Identifier: "freeze-ended", Environment: "staging", Running: storage.FreezeRunningStop, Reason: "SEV-2", EndsAt: &ended},
		}
	}
	freezes := &memoryFreezeStore{clock: clock.NewFake(freezeTestNow), freezes: freezeRows()}
	noClock := &memoryFreezeStore{clock: clock.NewFake(freezeTestNow), freezes: freezeRows(), nowErr: errors.New("connection refused")}
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
		{"LiftedFreeze", stopped, &stopRequestStore{stop: stopBy("freeze:freeze-lifted", storage.ControlRequestCompleted)}, freezes,
			&apitypes.FreezeStopRef{ID: "freeze-lifted", Reason: "SEV-1", Ended: true}},
		{"EndedFreeze", stopped, &stopRequestStore{stop: stopBy("freeze:freeze-ended", storage.ControlRequestCompleted)}, freezes,
			&apitypes.FreezeStopRef{ID: "freeze-ended", Reason: "SEV-2", Ended: true}},
		{"ClockUnreadableReadsAsInEffect", stopped, &stopRequestStore{stop: stopBy("freeze:freeze-ended", storage.ControlRequestCompleted)}, noClock,
			&apitypes.FreezeStopRef{ID: "freeze-ended", Reason: "SEV-2"}},
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

// stoppingApplyStore serves the apply as stored for the first read and as
// stopped for every later one, as when a freeze stops it during a poll.
type stoppingApplyStore struct {
	staticApplyStore
	reads int
}

func (s *stoppingApplyStore) GetByApplyIdentifier(ctx context.Context, applyIdentifier string) (*storage.Apply, error) {
	s.reads++
	apply, err := s.staticApplyStore.GetByApplyIdentifier(ctx, applyIdentifier)
	if err != nil || apply == nil || s.reads == 1 {
		return apply, err
	}
	stopped := *apply
	stopped.State = state.Apply.Stopped
	return &stopped, nil
}

// A poll that reaches the engine while the apply runs, and rereads it as
// stopped after a freeze stopped it, says which freeze stopped it, as a poll
// served from storage does, so the watch that exits on it shows the freeze's
// guidance rather than the ordinary resume.
func TestProgressByApplyIDNamesTheFreezeThatStoppedItDuringThePoll(t *testing.T) {
	mock := &mockTernClient{
		isRemote:     true,
		progressResp: &ternv1.ProgressResponse{ApplyId: "remote-apply-freeze", State: ternv1.State_STATE_STOPPED},
	}
	apply := activeTestApply("apply-freeze-live")
	apply.ExternalID = "remote-apply-freeze"
	controls := &memoryControlRequestStore{}
	_, _, err := controls.RequestPending(t.Context(), &storage.ApplyControlRequest{
		ApplyID: apply.ID, Operation: storage.ControlOperationStop, Status: storage.ControlRequestPending,
		RequestedBy: storage.FreezeStopCaller("freeze-live"),
	})
	require.NoError(t, err)
	require.NoError(t, controls.CompletePending(t.Context(), apply.ID, storage.ControlOperationStop))
	svc := New(&mockStorageWithApplyStores{
		applies:  &stoppingApplyStore{staticApplyStore: staticApplyStore{apply: apply}},
		tasks:    &capturingTaskStore{},
		controls: controls,
	}, testServerConfig(), map[string]tern.Client{"default/staging": mock}, slog.New(slog.DiscardHandler))
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/progress/apply/apply-freeze-live", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var resp apitypes.ProgressResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, state.Apply.Stopped, resp.State)
	require.NotNil(t, resp.StoppedByFreeze, "the poll names the freeze that stopped the apply")
	assert.Equal(t, "freeze-live", resp.StoppedByFreeze.ID)
}
