package api

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// Only a stop freeze in effect when it is set sweeps the applies already
// admitted. A continue freeze, and a stop freeze that starts later, carry no
// sweep, so the response never reads as more than happened.
func TestFreezeCreateStopSweepOnlyForAStopFreezeInEffect(t *testing.T) {
	_, mux := newFreezeTestService(t, freezeConfig())

	w := doFreezeRequest(t, mux, freezeAdmin, http.MethodPost, "/api/freezes", `{"environment":"staging","reason":"incident"}`)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	assert.Nil(t, decodeFreeze(t, w).StopSweep, "a continue freeze stops nothing")

	w = doFreezeRequest(t, mux, freezeAdmin, http.MethodPost, "/api/freezes",
		`{"environment":"staging","kind":"planned","running":"stop","starts_at":"2026-11-25T08:00:00Z","ends_at":"2026-11-30T08:00:00Z","reason":"holiday"}`)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	assert.Nil(t, decodeFreeze(t, w).StopSweep, "a stop freeze that starts later stops nothing when it is set")

	w = doFreezeRequest(t, mux, freezeAdmin, http.MethodPost, "/api/freezes", `{"environment":"staging","running":"stop","reason":"bad ddl"}`)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	sweep := decodeFreeze(t, w).StopSweep
	require.NotNil(t, sweep, "a stop freeze in effect reports its sweep even when nothing was in scope")
	assert.Empty(t, sweep.Stopped)
	assert.Empty(t, sweep.NotStopped)
	assert.Empty(t, sweep.Error)
}

// When the sweep cannot list the applies in progress, the freeze is still
// stored and refuses new changes, and the response says nothing was stopped
// and why.
func TestFreezeCreateStopSweepReportsAListFailure(t *testing.T) {
	service, mux := newFreezeTestService(t, freezeConfig())
	service.storage.(*mockStorageWithFreezes).applies = &staticApplyStore{err: errors.New("connection refused")}

	w := doFreezeRequest(t, mux, freezeAdmin, http.MethodPost, "/api/freezes", `{"environment":"staging","running":"stop","reason":"bad ddl"}`)

	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	created := decodeFreeze(t, w)
	require.NotNil(t, created.StopSweep)
	assert.Equal(t, freezeStopSweepListFailed, created.StopSweep.Error)
	assert.Empty(t, created.StopSweep.Stopped)
	stored, err := service.storage.Freezes().Get(t.Context(), created.ID)
	require.NoError(t, err)
	assert.Equal(t, storage.FreezeRunningStop, stored.Running)
}

// The sweep considers an apply in the freeze's environment that is pending,
// stopped (a start may be queued on it) or doing work a stop can end. It
// leaves alone other environments, a pull request rollback, an apply waiting
// for its cutover (the freeze already refuses the cutover) and one that has
// ended.
func TestFreezeStopSweepCandidate(t *testing.T) {
	freeze := &storage.EnvironmentFreeze{Environment: "staging", Running: storage.FreezeRunningStop}
	rollback := &storage.Apply{Environment: "staging", State: state.Apply.Running}
	rollback.SetOptions(storage.ApplyOptions{PinnedRollback: true})

	tests := []struct {
		name  string
		apply *storage.Apply
		want  bool
	}{
		{"Pending", &storage.Apply{Environment: "staging", State: state.Apply.Pending}, true},
		{"Running", &storage.Apply{Environment: "Staging", State: state.Apply.Running}, true},
		{"FailedRetryable", &storage.Apply{Environment: "staging", State: state.Apply.FailedRetryable}, true},
		{"PausedRollout", &storage.Apply{Environment: "staging", State: state.Apply.Paused}, true},
		{"OtherEnvironment", &storage.Apply{Environment: "production", State: state.Apply.Running}, false},
		{"PinnedRollback", rollback, false},
		{"WaitingForCutover", &storage.Apply{Environment: "staging", State: state.Apply.WaitingForCutover}, false},
		{"Stopped", &storage.Apply{Environment: "staging", State: state.Apply.Stopped}, true},
		{"Completed", &storage.Apply{Environment: "staging", State: state.Apply.Completed}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, freezeStopSweepCandidate(freeze, tc.apply))
		})
	}
}
