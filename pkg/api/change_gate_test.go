package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/clock"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func (m *mockStorageWithApplyStores) Freezes() storage.FreezeStore { return m.freezes }

// gateTestFreeze is a freeze of testdb's staging environment in effect at
// freezeTestNow.
func gateTestFreeze(id string) *storage.EnvironmentFreeze {
	return &storage.EnvironmentFreeze{
		Identifier: id, Environment: "staging", Kind: storage.FreezeKindOnDemand, Running: storage.FreezeRunningContinue,
		StartsAt: freezeTestNow.Add(-time.Hour), Reason: "incident 42", CreatedBy: "alice", Source: storage.FreezeSourceManual,
	}
}

// newGateFreezeStore is a freeze store holding freezes, read with a database
// clock at freezeTestNow.
func newGateFreezeStore(t *testing.T, freezes ...*storage.EnvironmentFreeze) *memoryFreezeStore {
	t.Helper()
	store := &memoryFreezeStore{clock: clock.NewFake(freezeTestNow)}
	for _, freeze := range freezes {
		_, err := store.Create(t.Context(), freeze)
		require.NoError(t, err)
	}
	return store
}

// newGateApplyTestService is an apply test service with freezes.
func newGateApplyTestService(t *testing.T, freezes ...*storage.EnvironmentFreeze) (*Service, *capturingApplyStore) {
	t.Helper()
	applies := &capturingApplyStore{}
	svc, _ := newExecuteApplyTestService(&mockTernClient{isRemote: true}, applies)
	svc.storage.(*mockStorageWithApplyStores).freezes = newGateFreezeStore(t, freezes...)
	return svc, applies
}

// newGateControlTestService is a control test service for apply, with freezes,
// serving the HTTP routes.
func newGateControlTestService(t *testing.T, apply *storage.Apply, freezes storage.FreezeStore) (*Service, *http.ServeMux) {
	t.Helper()
	svc := newControlTestService(&mockTernClient{}, apply)
	svc.storage.(*mockStorageWithApplyStores).freezes = freezes
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	return svc, mux
}

func postGateControl(t *testing.T, mux *http.ServeMux, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func decodeGateError(t *testing.T, rec *httptest.ResponseRecorder) apitypes.ErrorResponse {
	t.Helper()
	var resp apitypes.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp), rec.Body.String())
	return resp
}

func changeGateCounters(t *testing.T, reader *sdkmetric.ManualReader, name string) map[string]int64 {
	t.Helper()
	values := map[string]int64{}
	for _, m := range collectFreezeMetrics(t, reader) {
		if m.Name != name {
			continue
		}
		sum, ok := m.Data.(metricdata.Sum[int64])
		require.True(t, ok)
		for _, dp := range sum.DataPoints {
			values[dp.Attributes.Encoded(attribute.DefaultEncoder())] += dp.Value
		}
	}
	return values
}

// An operator applies to staging while an incident freeze is in effect. The
// HTTP API answers 409 change_gate_refused with the freeze as a blocker, no
// apply is created, and the refusal is counted.
func TestChangeGateRefusesApplyDuringFreeze(t *testing.T) {
	reader := newFreezeMetricsReader(t)
	svc, applies := newGateApplyTestService(t, gateTestFreeze("freeze-incident"))

	rec := postJSON(t, svc.handleApply, "/api/apply", ApplyRequest{PlanID: "plan-1", Environment: "staging"})

	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	resp := decodeGateError(t, rec)
	assert.Equal(t, apitypes.ErrCodeChangeGateRefused, resp.ErrorCode)
	assert.Contains(t, resp.Error, "freeze freeze-incident (incident 42, until lifted)")
	assert.Equal(t, []apitypes.ChangeGateBlocker{{Gate: "freeze", ID: "freeze-incident", Reason: "incident 42"}}, resp.Blockers)
	assert.Nil(t, applies.apply, "a refused apply must not be created")
	assert.Equal(t, map[string]int64{"checkpoint=start,environment=staging,gate=freeze": 1}, changeGateCounters(t, reader, "schemabot.change_gate.refusals"))
}

// "In effect" is the database clock's call: a freeze that starts later by the
// database clock does not hold the apply, even if a pod's own clock is ahead.
func TestChangeGateUsesDatabaseClock(t *testing.T) {
	freeze := gateTestFreeze("freeze-later")
	freeze.StartsAt = freezeTestNow.Add(time.Hour)
	svc, applies := newGateApplyTestService(t, freeze)
	require.NoError(t, svc.SetClock(clock.NewFake(freezeTestNow.Add(2*time.Hour))))

	resp, _, err := svc.ExecuteApply(t.Context(), ApplyRequest{PlanID: "plan-1", Environment: "staging"})

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.NotNil(t, applies.apply)
}

// A freeze narrowed to another database does not hold this one.
func TestChangeGateIgnoresFreezeOfAnotherDatabase(t *testing.T) {
	freeze := gateTestFreeze("freeze-orders")
	freeze.Databases = []string{"orders"}
	svc, applies := newGateApplyTestService(t, freeze)

	resp, _, err := svc.ExecuteApply(t.Context(), ApplyRequest{PlanID: "plan-1", Environment: "staging"})

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.NotNil(t, applies.apply)
}

// When SchemaBot cannot read the freezes it fails closed: a retryable 503,
// counted by checkpoint, and no apply.
func TestChangeGateFailsClosedWhenFreezesUnreadable(t *testing.T) {
	reader := newFreezeMetricsReader(t)
	svc, applies := newGateApplyTestService(t)
	svc.storage.(*mockStorageWithApplyStores).freezes = &failingFreezeStore{}

	rec := postJSON(t, svc.handleApply, "/api/apply", ApplyRequest{PlanID: "plan-1", Environment: "staging"})

	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	resp := decodeGateError(t, rec)
	assert.Equal(t, apitypes.ErrCodeChangeGateUnavailable, resp.ErrorCode)
	assert.True(t, apitypes.IsRetryableErrorCode(resp.ErrorCode))
	assert.Equal(t, "SchemaBot could not check for freezes, so this did not start. Try again.", resp.Error)
	assert.Nil(t, applies.apply)
	assert.Equal(t, map[string]int64{"checkpoint=start,environment=staging": 1}, changeGateCounters(t, reader, "schemabot.change_gate.errors"))
}

// A pull request rollback undoes a change, so a freeze never holds it.
func TestChangeGateNeverHoldsPinnedRollback(t *testing.T) {
	svc, applies := newGateApplyTestService(t, gateTestFreeze("freeze-incident"))

	_, _, err := svc.ExecuteApply(t.Context(), ApplyRequest{
		PlanID: "plan-1", Environment: "staging", Options: map[string]string{"rollback": "true"},
		ExpectedLockOwner: "octocat/hello-world#1", ExpectedPendingPlanID: storage.RollbackPendingPlanPrefix + "plan-1",
	})

	require.NoError(t, err)
	assert.NotNil(t, applies.apply)
}

// A deferred apply waits at its cutover when a freeze begins. Cutting over is
// a new step, so it is refused with the freeze as a blocker and nothing is
// queued. After the lift, the same cutover is queued.
func TestChangeGateHoldsDeferredCutover(t *testing.T) {
	apply := activeTestApply("apply-deferred")
	apply.State = state.Apply.WaitingForCutover
	freezes := newGateFreezeStore(t, gateTestFreeze("freeze-incident"))
	svc, mux := newGateControlTestService(t, apply, freezes)
	body := `{"environment": "staging", "apply_id": "apply-deferred", "caller": "cli:bob"}`

	rec := postGateControl(t, mux, "/api/cutover", body)
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	resp := decodeGateError(t, rec)
	assert.Equal(t, apitypes.ErrCodeChangeGateRefused, resp.ErrorCode)
	require.Len(t, resp.Blockers, 1)
	assert.Equal(t, "freeze-incident", resp.Blockers[0].ID)
	pending, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	assert.Nil(t, pending, "a refused cutover must not be queued")

	_, err = freezes.Lift(t.Context(), "freeze-incident", "alice", "resolved")
	require.NoError(t, err)
	rec = postGateControl(t, mux, "/api/cutover", body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	pending, err = svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	assert.NotNil(t, pending)
}

// Starting an apply that waits to be started is a new step too: it is refused
// at the resume checkpoint during the freeze and queued after the lift.
func TestChangeGateHoldsStart(t *testing.T) {
	reader := newFreezeMetricsReader(t)
	apply := activeTestApply("apply-waiting")
	apply.State = state.Apply.WaitingForDeploy
	freezes := newGateFreezeStore(t, gateTestFreeze("freeze-incident"))
	svc, mux := newGateControlTestService(t, apply, freezes)
	body := `{"environment": "staging", "apply_id": "apply-waiting", "caller": "cli:bob"}`

	rec := postGateControl(t, mux, "/api/start", body)
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	assert.Equal(t, apitypes.ErrCodeChangeGateRefused, decodeGateError(t, rec).ErrorCode)
	pending, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.Nil(t, pending, "a refused start must not be queued")
	assert.Equal(t, map[string]int64{"checkpoint=resume,environment=staging,gate=freeze": 1}, changeGateCounters(t, reader, "schemabot.change_gate.refusals"))

	_, err = freezes.Lift(t.Context(), "freeze-incident", "alice", "resolved")
	require.NoError(t, err)
	rec = postGateControl(t, mux, "/api/start", body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	pending, err = svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.NotNil(t, pending)
}

// A start that was queued before the freeze is not a new step: retrying it
// during the freeze answers 202 already_requested, as without a freeze.
func TestChangeGateLetsAQueuedStartRetry(t *testing.T) {
	apply := activeTestApply("apply-queued")
	apply.State = state.Apply.WaitingForDeploy
	freezes := newGateFreezeStore(t)
	svc, mux := newGateControlTestService(t, apply, freezes)
	body := `{"environment": "staging", "apply_id": "apply-queued", "caller": "cli:bob"}`
	require.Equal(t, http.StatusOK, postGateControl(t, mux, "/api/start", body).Code)
	queued, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
	require.NoError(t, err)
	require.NotNil(t, queued)

	_, err = freezes.Create(t.Context(), gateTestFreeze("freeze-incident"))
	require.NoError(t, err)
	rec := postGateControl(t, mux, "/api/start", body)

	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	var resp apitypes.StartResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Accepted)
	assert.Equal(t, startResponseStatusAlreadyRequested, resp.Status)
}
