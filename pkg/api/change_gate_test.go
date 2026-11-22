package api

import (
	"context"
	"encoding/json"
	"errors"
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
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
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
	assert.Contains(t, resp.Error, "staging is frozen by freeze freeze-incident until lifted. Reason: incident 42.")
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
	require.NotNil(t, applies.apply)
	assert.True(t, applies.apply.GetOptions().PinnedRollback, "the apply records that it is a pinned rollback")
}

// The rollback option alone is the caller's to set, so it does not pass the
// gates, and it is not recorded as a pinned rollback.
func TestChangeGateHoldsAnUnpinnedRollbackOption(t *testing.T) {
	svc, applies := newGateApplyTestService(t, gateTestFreeze("freeze-incident"))

	_, _, err := svc.ExecuteApply(t.Context(), ApplyRequest{
		PlanID: "plan-1", Environment: "staging",
		Options: map[string]string{"rollback": "true", "pinned_rollback": "true"},
	})

	require.ErrorAs(t, err, new(*ChangeGateRefusedError))
	assert.Nil(t, applies.apply)
}

// A pinned rollback that was stopped, or deferred its cutover, can still be
// started and cut over during the freeze: the gates never hold a rollback.
func TestChangeGateLetsAPinnedRollbackStartAndCutover(t *testing.T) {
	for _, tc := range []struct {
		name, path string
		state      string
	}{
		{"start", "/api/start", state.Apply.WaitingForDeploy},
		{"cutover", "/api/cutover", state.Apply.WaitingForCutover},
	} {
		t.Run(tc.name, func(t *testing.T) {
			apply := activeTestApply("apply-rollback")
			apply.State = tc.state
			apply.SetOptions(storage.ApplyOptions{Rollback: true, PinnedRollback: true})
			_, mux := newGateControlTestService(t, apply, newGateFreezeStore(t, gateTestFreeze("freeze-incident")))

			rec := postGateControl(t, mux, tc.path, `{"environment": "staging", "apply_id": "apply-rollback", "caller": "cli:bob"}`)

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		})
	}
}

// An apply's stored operation keeps running on a deployment later dropped
// from the rollout configuration, so a freeze of that deployment holds the
// apply's cutover.
func TestChangeGateCoversTheDeploymentsOfStoredOperations(t *testing.T) {
	apply := activeTestApply("apply-eu")
	apply.State = state.Apply.WaitingForCutover
	freeze := gateTestFreeze("freeze-eu")
	freeze.Deployments = []string{"eu"}
	svc, mux := newGateControlTestService(t, apply, newGateFreezeStore(t, freeze))
	svc.storage.(*mockStorageWithApplyStores).operations = &staticApplyOperationStore{
		operations: []*storage.ApplyOperation{{ApplyID: apply.ID, Deployment: "eu"}},
	}

	rec := postGateControl(t, mux, "/api/cutover", `{"environment": "staging", "apply_id": "apply-eu", "caller": "cli:bob"}`)

	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	assert.Equal(t, "freeze-eu", decodeGateError(t, rec).Blockers[0].ID)
}

// If the apply's operations cannot be read, its deployments are unknown, so
// the start is refused with a retryable 503.
func TestChangeGateFailsClosedWhenOperationsUnreadable(t *testing.T) {
	apply := activeTestApply("apply-unknown")
	apply.State = state.Apply.WaitingForDeploy
	svc, mux := newGateControlTestService(t, apply, newGateFreezeStore(t))
	svc.storage.(*mockStorageWithApplyStores).operations = &staticApplyOperationStore{err: errors.New("connection refused")}

	rec := postGateControl(t, mux, "/api/start", `{"environment": "staging", "apply_id": "apply-unknown", "caller": "cli:bob"}`)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	resp := decodeGateError(t, rec)
	assert.Equal(t, apitypes.ErrCodeChangeGateUnavailable, resp.ErrorCode)
	assert.Equal(t, "SchemaBot could not check for freezes, so the schema change was not started. Try again.", resp.Error)
}

// PR commands post a gate error as is, so it reads as the operator-facing
// message and leaves out the storage cause, which Unwrap keeps.
func TestChangeGateUnavailableErrorIsTheOperatorMessage(t *testing.T) {
	cause := errors.New("dial tcp 10.0.0.1:3306: connection refused")
	err := &ChangeGateUnavailableError{Checkpoint: changeGateCheckpointResume, Gate: "freeze", Err: cause}

	assert.Equal(t, "SchemaBot could not check for freezes, so the schema change was not started. Try again.", err.Error())
	assert.ErrorIs(t, err, cause)
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

// Restarting a stopped apply is a new step, on both start branches for a
// stopped apply: the local one and the one that first checks a remote
// executor. Each is refused during the freeze before it records or asks
// anything, and queued after the lift.
// stoppedStartCase is a stopped apply on one of the two start branches:
// local, where SchemaBot runs the tasks, and remote, where it asks the
// remote executor.
type stoppedStartCase struct {
	name   string
	client func() *mockTernClient
	apply  func() *storage.Apply
	tasks  func(*storage.Apply) []*storage.Task
}

func stoppedStartCases() []stoppedStartCase {
	return []stoppedStartCase{
		{
			name:   "local",
			client: func() *mockTernClient { return &mockTernClient{} },
			apply:  func() *storage.Apply { return stoppedTestApply("apply-stopped") },
			tasks: func(apply *storage.Apply) []*storage.Task {
				return []*storage.Task{{ID: 10, TaskIdentifier: "task-stopped", ApplyID: apply.ID, State: state.Task.Stopped}}
			},
		},
		{
			name: "remote",
			client: func() *mockTernClient {
				return &mockTernClient{
					isRemote: true,
					progressResp: &ternv1.ProgressResponse{
						State:  ternv1.State_STATE_STOPPED,
						Tables: []*ternv1.TableProgress{{TableName: "users", Status: state.Task.Stopped}},
					},
				}
			},
			apply: func() *storage.Apply {
				apply := activeTestApply("apply-stopped")
				apply.ExternalID = "remote-apply-stopped"
				return apply
			},
			tasks: func(*storage.Apply) []*storage.Task { return nil },
		},
	}
}

func TestChangeGateHoldsStartOfAStoppedApply(t *testing.T) {
	for _, tc := range stoppedStartCases() {
		t.Run(tc.name, func(t *testing.T) {
			client := tc.client()
			apply := tc.apply()
			svc := newControlTestServiceWithTasks(client, apply, tc.tasks(apply))
			freezes := newGateFreezeStore(t, gateTestFreeze("freeze-incident"))
			svc.storage.(*mockStorageWithApplyStores).freezes = freezes
			mux := http.NewServeMux()
			svc.ConfigureRoutes(mux)
			body := `{"environment": "staging", "apply_id": "apply-stopped", "caller": "cli:bob"}`

			rec := postGateControl(t, mux, "/api/start", body)
			require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
			assert.Equal(t, apitypes.ErrCodeChangeGateRefused, decodeGateError(t, rec).ErrorCode)
			pending, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
			require.NoError(t, err)
			assert.Nil(t, pending, "a refused start must not be queued")
			assert.Nil(t, client.progressReq, "a refused start never asks the remote executor")

			_, err = freezes.Lift(t.Context(), "freeze-incident", "alice", "resolved")
			require.NoError(t, err)
			rec = postGateControl(t, mux, "/api/start", body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			pending, err = svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
			require.NoError(t, err)
			assert.NotNil(t, pending)
		})
	}
}

// epochCheckingApplyStore checks the freeze epoch guard where the SQL store
// does: in the write that creates the apply.
type epochCheckingApplyStore struct {
	storage.ApplyStore
	freezes *memoryFreezeStore
}

func (s epochCheckingApplyStore) CreateWithGroupedOperations(ctx context.Context, apply *storage.Apply, groups []*storage.ApplyOperationWithTasks) (int64, error) {
	if err := s.freezes.checkEpochGuard(ctx); err != nil {
		return 0, err
	}
	return s.ApplyStore.CreateWithGroupedOperations(ctx, apply, groups)
}

// epochCheckingControlStore checks the freeze epoch guard where the SQL store
// does: in the write that records a start or cutover request.
type epochCheckingControlStore struct {
	storage.ControlRequestStore
	freezes *memoryFreezeStore
}

func (s epochCheckingControlStore) RequestPending(ctx context.Context, req *storage.ApplyControlRequest) (*storage.ApplyControlRequest, bool, error) {
	if err := s.freezes.checkEpochGuard(ctx); err != nil {
		return nil, false, err
	}
	return s.ControlRequestStore.RequestPending(ctx, req)
}

// A freeze created after the gate listed the freezes, but before the change
// was written, refuses the change with a retryable 503 and records nothing.
// The retry sees the freeze and is held by it.
func TestChangeGateRefusesAChangeAFreezeLandsBeforeItIsWritten(t *testing.T) {
	landFreezeDuringCheck := func(t *testing.T, freezes *memoryFreezeStore) {
		freezes.afterListInEffect = func() {
			_, err := freezes.Create(t.Context(), gateTestFreeze("freeze-late"))
			require.NoError(t, err)
		}
	}

	t.Run("apply", func(t *testing.T) {
		reader := newFreezeMetricsReader(t)
		svc, applies := newGateApplyTestService(t)
		mock := svc.storage.(*mockStorageWithApplyStores)
		freezes := mock.freezes.(*memoryFreezeStore)
		mock.applies = epochCheckingApplyStore{ApplyStore: applies, freezes: freezes}
		landFreezeDuringCheck(t, freezes)

		_, _, err := svc.ExecuteApply(t.Context(), ApplyRequest{PlanID: "plan-1", Environment: "staging"})

		unavailable, ok := errors.AsType[*ChangeGateUnavailableError](err)
		require.True(t, ok, "got %v", err)
		assert.Equal(t, "SchemaBot could not check for freezes, so this did not start. Try again.", unavailable.Error())
		require.ErrorIs(t, err, storage.ErrFreezeEpochChanged)
		assert.Nil(t, applies.apply, "the apply must not be stored")
		assert.Equal(t, map[string]int64{"checkpoint=start,environment=staging": 1}, changeGateCounters(t, reader, "schemabot.change_gate.errors"))

		_, _, err = svc.ExecuteApply(t.Context(), ApplyRequest{PlanID: "plan-1", Environment: "staging"})
		require.ErrorAs(t, err, new(*ChangeGateRefusedError))
		assert.Nil(t, applies.apply)
	})

	for _, tc := range []struct {
		name, path, state, message string
		operation                  storage.ControlOperation
	}{
		{"start", "/api/start", state.Apply.WaitingForDeploy, "SchemaBot could not check for freezes, so the schema change was not started. Try again.", storage.ControlOperationStart},
		{"cutover", "/api/cutover", state.Apply.WaitingForCutover, "SchemaBot could not check for freezes, so the cutover did not start. Try again.", storage.ControlOperationCutover},
	} {
		t.Run(tc.name, func(t *testing.T) {
			apply := activeTestApply("apply-late")
			apply.State = tc.state
			freezes := newGateFreezeStore(t)
			svc, mux := newGateControlTestService(t, apply, freezes)
			mock := svc.storage.(*mockStorageWithApplyStores)
			mock.controls = epochCheckingControlStore{ControlRequestStore: mock.controls, freezes: freezes}
			landFreezeDuringCheck(t, freezes)
			body := `{"environment": "staging", "apply_id": "apply-late", "caller": "cli:bob"}`

			rec := postGateControl(t, mux, tc.path, body)

			require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
			resp := decodeGateError(t, rec)
			assert.Equal(t, apitypes.ErrCodeChangeGateUnavailable, resp.ErrorCode)
			assert.Equal(t, tc.message, resp.Error)
			pending, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, tc.operation)
			require.NoError(t, err)
			assert.Nil(t, pending, "the request must not be recorded")

			rec = postGateControl(t, mux, tc.path, body)
			require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
			assert.Equal(t, "freeze-late", decodeGateError(t, rec).Blockers[0].ID)
		})
	}

	for _, tc := range stoppedStartCases() {
		t.Run("start stopped "+tc.name, func(t *testing.T) {
			apply := tc.apply()
			svc := newControlTestServiceWithTasks(tc.client(), apply, tc.tasks(apply))
			freezes := newGateFreezeStore(t)
			mock := svc.storage.(*mockStorageWithApplyStores)
			mock.freezes = freezes
			mock.controls = epochCheckingControlStore{ControlRequestStore: mock.controls, freezes: freezes}
			mux := http.NewServeMux()
			svc.ConfigureRoutes(mux)
			landFreezeDuringCheck(t, freezes)
			body := `{"environment": "staging", "apply_id": "apply-stopped", "caller": "cli:bob"}`

			rec := postGateControl(t, mux, "/api/start", body)

			require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
			assert.Equal(t, apitypes.ErrCodeChangeGateUnavailable, decodeGateError(t, rec).ErrorCode)
			pending, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
			require.NoError(t, err)
			assert.Nil(t, pending, "the start must not be queued")

			rec = postGateControl(t, mux, "/api/start", body)
			require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
			assert.Equal(t, "freeze-late", decodeGateError(t, rec).Blockers[0].ID)
		})
	}
}

// When the admission write cannot read the freeze epoch, the change is
// refused with the retryable 503, as when the gate cannot list the freezes,
// and nothing is recorded.
func TestChangeGateFailsClosedWhenTheAdmissionWriteCannotReadTheEpoch(t *testing.T) {
	readFailure := errors.New("lock wait timeout exceeded")

	t.Run("apply", func(t *testing.T) {
		reader := newFreezeMetricsReader(t)
		svc, applies := newGateApplyTestService(t)
		mock := svc.storage.(*mockStorageWithApplyStores)
		freezes := mock.freezes.(*memoryFreezeStore)
		mock.applies = epochCheckingApplyStore{ApplyStore: applies, freezes: freezes}
		freezes.epochCheckErr = readFailure

		_, _, err := svc.ExecuteApply(t.Context(), ApplyRequest{PlanID: "plan-1", Environment: "staging"})

		unavailable, ok := errors.AsType[*ChangeGateUnavailableError](err)
		require.True(t, ok, "got %v", err)
		assert.Equal(t, "SchemaBot could not check for freezes, so this did not start. Try again.", unavailable.Error())
		require.ErrorIs(t, err, readFailure, "the cause is kept")
		assert.Nil(t, applies.apply, "the apply must not be stored")
		assert.Equal(t, map[string]int64{"checkpoint=start,environment=staging": 1}, changeGateCounters(t, reader, "schemabot.change_gate.errors"))
	})

	for _, tc := range []struct {
		name, path, state, message string
		operation                  storage.ControlOperation
	}{
		{"start", "/api/start", state.Apply.WaitingForDeploy, "SchemaBot could not check for freezes, so the schema change was not started. Try again.", storage.ControlOperationStart},
		{"cutover", "/api/cutover", state.Apply.WaitingForCutover, "SchemaBot could not check for freezes, so the cutover did not start. Try again.", storage.ControlOperationCutover},
	} {
		t.Run(tc.name, func(t *testing.T) {
			apply := activeTestApply("apply-unreadable")
			apply.State = tc.state
			freezes := newGateFreezeStore(t)
			svc, mux := newGateControlTestService(t, apply, freezes)
			mock := svc.storage.(*mockStorageWithApplyStores)
			mock.controls = epochCheckingControlStore{ControlRequestStore: mock.controls, freezes: freezes}
			freezes.epochCheckErr = readFailure

			rec := postGateControl(t, mux, tc.path, `{"environment": "staging", "apply_id": "apply-unreadable", "caller": "cli:bob"}`)

			require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
			resp := decodeGateError(t, rec)
			assert.Equal(t, apitypes.ErrCodeChangeGateUnavailable, resp.ErrorCode)
			assert.Equal(t, tc.message, resp.Error)
			pending, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, tc.operation)
			require.NoError(t, err)
			assert.Nil(t, pending, "the request must not be recorded")
		})
	}
}

// A refusal reads as whole sentences: what freezes the environment, until
// when, each reason after "Reason:", and when to retry, so a reason never runs
// into the text around it.
func TestChangeGateRefusalReadsAsSentences(t *testing.T) {
	ends := time.Date(2026, 11, 22, 18, 0, 0, 0, time.FixedZone("PST", -8*60*60))
	freeze := func(id, reason string, endsAt *time.Time) apitypes.ChangeGateBlocker {
		return apitypes.ChangeGateBlocker{Gate: "freeze", ID: id, Reason: reason, EndsAt: endsAt}
	}
	for name, tc := range map[string]struct {
		blockers []apitypes.ChangeGateBlocker
		want     string
	}{
		"no blocker named": {
			want: "production is frozen. Retry after it ends or is lifted.",
		},
		"one freeze until lifted": {
			blockers: []apitypes.ChangeGateBlocker{freeze("frz-1", "rpc error: code = Unavailable during the outage", nil)},
			want:     "production is frozen by freeze frz-1 until lifted. Reason: rpc error: code = Unavailable during the outage. Retry after it ends or is lifted.",
		},
		"a reason that ends a sentence and one with no visible text": {
			blockers: []apitypes.ChangeGateBlocker{freeze("frz-1", "SEV-1234!", nil), freeze("frz-2", " \u200b ", &ends)},
			want:     "production is frozen by 2 freezes. Freeze frz-1 lasts until lifted. Reason: SEV-1234! Freeze frz-2 lasts until 2026-11-23T02:00:00Z. Retry after they end or are lifted.",
		},
	} {
		refused := &ChangeGateRefusedError{Environment: "production", Blockers: tc.blockers}
		assert.Equal(t, tc.want, refused.Blocked(), name)
	}
}
