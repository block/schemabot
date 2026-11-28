package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/clock"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func (m *mockStorageWithApplyStores) FreezeOverrides() storage.FreezeOverrideStore {
	return m.overrides
}

func (m *mockStorageWithFreezes) FreezeOverrides() storage.FreezeOverrideStore { return m.overrides }

// memoryFreezeOverrideStore is an in-memory storage.FreezeOverrideStore with
// the SQL store's contract: no self-approval, no second approval, no approval
// past expiry, and ListApproved filtered by target, approval and expiry.
type memoryFreezeOverrideStore struct {
	mu       sync.Mutex
	clock    clock.Clock
	requests []*storage.FreezeOverrideRequest
	// listErr, createErr and getErr fail ListApproved, Create and Get.
	listErr, createErr, getErr error
}

func (m *memoryFreezeOverrideStore) Create(_ context.Context, request *storage.FreezeOverrideRequest) (*storage.FreezeOverrideRequest, error) {
	if m.createErr != nil {
		return nil, m.createErr
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stored := *request
	stored.Environment = storage.CanonicalKey(stored.Environment)
	stored.CreatedAt = m.clock.Now().UTC()
	m.requests = append(m.requests, &stored)
	copied := stored
	return &copied, nil
}

func (m *memoryFreezeOverrideStore) CreateOnReason(ctx context.Context, request *storage.FreezeOverrideRequest) (*storage.FreezeOverrideRequest, error) {
	created, err := m.Create(ctx, request)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stored := m.requests[len(m.requests)-1]
	approvedAt := stored.CreatedAt
	stored.ApprovedBy, stored.ApprovedAt = stored.RequestedBy, &approvedAt
	created.ApprovedBy, created.ApprovedAt = stored.ApprovedBy, &approvedAt
	return created, nil
}

func (m *memoryFreezeOverrideStore) Get(_ context.Context, identifier string) (*storage.FreezeOverrideRequest, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, request := range m.requests {
		if request.Identifier == identifier {
			copied := *request
			return &copied, nil
		}
	}
	return nil, storage.ErrFreezeOverrideNotFound
}

func (m *memoryFreezeOverrideStore) Approve(_ context.Context, identifier, approver string) (*storage.FreezeOverrideRequest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, request := range m.requests {
		if request.Identifier != identifier {
			continue
		}
		now := m.clock.Now().UTC()
		switch {
		case request.IsPersonOf(approver):
			return nil, storage.ErrFreezeOverrideSelfApproval
		case request.ApprovedAt != nil:
			return nil, storage.ErrFreezeOverrideAlreadyApproved
		case !request.ExpiresAt.After(now):
			return nil, storage.ErrFreezeOverrideExpired
		}
		request.ApprovedBy, request.ApprovedAt = approver, &now
		copied := *request
		return &copied, nil
	}
	return nil, storage.ErrFreezeOverrideNotFound
}

func (m *memoryFreezeOverrideStore) ListApproved(_ context.Context, target storage.FreezeOverrideTarget) ([]*storage.FreezeOverrideRequest, error) {
	return m.list(target, true)
}

func (m *memoryFreezeOverrideStore) ListPending(_ context.Context, target storage.FreezeOverrideTarget) ([]*storage.FreezeOverrideRequest, error) {
	return m.list(target, false)
}

func (m *memoryFreezeOverrideStore) list(target storage.FreezeOverrideTarget, approvedOnly bool) ([]*storage.FreezeOverrideRequest, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var approved []*storage.FreezeOverrideRequest
	for _, request := range m.requests {
		if (request.ApprovedAt != nil) == approvedOnly && request.ExpiresAt.After(m.clock.Now().UTC()) &&
			request.Environment == storage.CanonicalKey(target.Environment) && request.Action == target.Action && request.TargetID == target.TargetID {
			copied := *request
			approved = append(approved, &copied)
		}
	}
	slices.Reverse(approved) // newest first, standing in for most recently approved
	return approved, nil
}

func (m *memoryFreezeOverrideStore) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.requests)
}

var (
	overrideRequester = &auth.User{Subject: "rita", Groups: []string{"schema-admins"}}
	overrideApprover  = "ida"
)

// withOverrides gives svc an override store sharing its freeze store's clock,
// under the approval override policy, which most override tests exercise. A
// test of the reason policy sets it after this.
func withOverrides(svc *Service) *memoryFreezeOverrideStore {
	svc.config.Freeze.OverridePolicy = FreezeOverridePolicyApproval
	mock := svc.storage.(*mockStorageWithApplyStores)
	store := &memoryFreezeOverrideStore{clock: mock.freezes.(*memoryFreezeStore).clock}
	mock.overrides = store
	return store
}

// postAs posts a JSON body as a verified user.
func postAs(t *testing.T, mux *http.ServeMux, user *auth.User, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(auth.WithVerifiedUser(t.Context(), user), http.MethodPost, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func serveRoutes(svc *Service) *http.ServeMux {
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	return mux
}

const (
	overrideApplyBody = `{"plan_id":"plan-1","environment":"staging","override":"  fix for incident 42  "}`
	overrideControl   = `{"environment":"staging","apply_id":"apply-stopped","caller":"cli:rita","override":"fix for incident 42"}`
)

// decodeApplyResponse decodes the body of an accepted POST /api/apply.
func decodeApplyResponse(t *testing.T, rec *httptest.ResponseRecorder) apitypes.ApplyResponse {
	t.Helper()
	var resp apitypes.ApplyResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp), rec.Body.String())
	return resp
}

// approveOverride records the approval a second person gives.
func approveOverride(t *testing.T, store *memoryFreezeOverrideStore, id string) {
	t.Helper()
	_, err := store.Approve(t.Context(), id, overrideApprover)
	require.NoError(t, err)
}

// An operator applies during a freeze with an override reason. SchemaBot
// records a request naming the blockers and the verified requester, and
// refuses. After a second person approves, the same command is admitted, the
// apply records the request, and the override is counted instead of a refusal.
func TestFreezeOverrideLetsAnApplyThrough(t *testing.T) {
	reader := newFreezeMetricsReader(t)
	svc, applies := newGateApplyTestService(t, gateTestFreeze("freeze-incident"))
	store := withOverrides(svc)
	mux := serveRoutes(svc)

	rec := postAs(t, mux, overrideRequester, "/api/apply", overrideApplyBody)

	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	resp := decodeGateError(t, rec)
	require.NotNil(t, resp.OverrideRequest)
	id := resp.OverrideRequest.ID
	assert.True(t, strings.HasPrefix(id, "ovr-"), id)
	assert.Contains(t, resp.Error, "Override request "+id+" needs a freeze approver other than you")
	stored, err := store.Get(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, []string{"freeze freeze-incident"}, stored.Blockers)
	assert.Equal(t, "rita", stored.RequestedBy)
	assert.Equal(t, "fix for incident 42", stored.Reason)
	assert.Equal(t, []string{"staging", "apply", gateApplyTarget(t, svc)}, []string{stored.Environment, stored.Action, stored.TargetID})
	assert.Equal(t, freezeTestNow.Add(storage.FreezeOverrideMaxLifetime), stored.ExpiresAt)
	assert.Nil(t, applies.apply, "a refused apply must not be created")
	rec = postAs(t, mux, overrideRequester, "/api/apply", overrideApplyBody)
	require.Equal(t, http.StatusConflict, rec.Code)
	assert.Equal(t, id, decodeGateError(t, rec).OverrideRequest.ID, "asking again keeps the one request")
	assert.NotContains(t, decodeGateError(t, rec).Error, "keeps that reason")
	rec = postAs(t, mux, overrideRequester, "/api/apply", `{"plan_id":"plan-1","environment":"staging","override":"another reason"}`)
	require.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, decodeGateError(t, rec).Error, `You asked for it earlier, and it keeps that reason: "fix for incident 42".`)
	assert.Equal(t, 1, store.count())

	approveOverride(t, store, id)
	other := &auth.User{Subject: "sam", Groups: []string{"schema-admins"}}
	rec = postAs(t, mux, other, "/api/apply", `{"plan_id":"plan-1","environment":"staging"}`)
	require.Equal(t, http.StatusConflict, rec.Code, "only the requester runs on an approval")
	rec = postAs(t, mux, overrideRequester, "/api/apply", overrideApplyBody)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NotNil(t, applies.apply)
	assert.Equal(t, id, applies.apply.GetOptions().FreezeOverride)
	assert.Equal(t, id, decodeApplyResponse(t, rec).FreezeOverride, "the response names the request that admitted the apply")
	assert.Equal(t, 1, store.count(), "an admitted apply records no second request")
	assert.Equal(t, map[string]int64{"checkpoint=start,environment=staging,gate=freeze": 4}, changeGateCounters(t, reader, "schemabot.change_gate.refusals"))
	assert.Equal(t, map[string]int64{"checkpoint=start,environment=staging,gate=freeze": 1}, changeGateCounters(t, reader, "schemabot.change_gate.overrides"))
}

// An approval names the blockers it covers. A freeze created after the
// request refuses again, and the caller needs a new request.
func TestFreezeOverrideDoesNotCoverANewFreeze(t *testing.T) {
	svc, applies := newGateApplyTestService(t, gateTestFreeze("freeze-incident"))
	store := withOverrides(svc)
	mux := serveRoutes(svc)
	first := decodeGateError(t, postAs(t, mux, overrideRequester, "/api/apply", overrideApplyBody)).OverrideRequest
	require.NotNil(t, first)
	approveOverride(t, store, first.ID)
	_, err := svc.storage.Freezes().Create(t.Context(), gateTestFreeze("freeze-second"))
	require.NoError(t, err)

	rec := postAs(t, mux, overrideRequester, "/api/apply", overrideApplyBody)

	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	second := decodeGateError(t, rec).OverrideRequest
	require.NotNil(t, second)
	assert.NotEqual(t, first.ID, second.ID)
	assert.ElementsMatch(t, []string{"freeze freeze-incident", "freeze freeze-second"}, second.Blockers)
	assert.Nil(t, applies.apply)
}

// One approval covers one environment, action and target: another plan, or an
// apply's cutover after its start was approved, is refused.
func TestFreezeOverrideCoversOneActionAndTarget(t *testing.T) {
	svc, _ := newGateApplyTestService(t, gateTestFreeze("freeze-incident"))
	store := withOverrides(svc)
	request, err := store.Create(t.Context(), &storage.FreezeOverrideRequest{
		Identifier: "ovr-start", Environment: "staging", Action: storage.FreezeOverrideActionStart, TargetID: "apply-1",
		Database: "testdb", Blockers: []string{"freeze freeze-incident"}, Reason: "fix", RequestedBy: "rita", ExpiresAt: freezeTestNow.Add(time.Hour),
	})
	require.NoError(t, err)
	approveOverride(t, store, request.Identifier)

	for _, tc := range []struct {
		name, action, target string
		admitted             bool
	}{
		{"same start", storage.FreezeOverrideActionStart, "apply-1", true},
		{"cutover of the same apply", storage.FreezeOverrideActionCutover, "apply-1", false},
		{"start of another apply", storage.FreezeOverrideActionStart, "apply-2", false},
		{"apply of a plan", storage.FreezeOverrideActionApply, "apply-1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, admitted, err := svc.checkChangeGates(auth.WithVerifiedUser(t.Context(), overrideRequester), changeGateInput{
				Checkpoint: changeGateCheckpointResume, Environment: "staging", Deployments: []string{DefaultDeployment},
				Database: "testdb", Action: tc.action, TargetID: tc.target,
			}, "cli:rita")
			if tc.admitted {
				require.NoError(t, err)
				assert.Equal(t, "ovr-start", admitted.Identifier)
				return
			}
			require.ErrorAs(t, err, new(*ChangeGateRefusedError))
			assert.Nil(t, admitted)
		})
	}
}

type overrideControlCase struct {
	name, path string
	operation  storage.ControlOperation
	build      func() (*mockTernClient, *storage.Apply, []*storage.Task)
}

func overrideControlCases() []overrideControlCase {
	cases := []overrideControlCase{
		{"start waiting", "/api/start", storage.ControlOperationStart, func() (*mockTernClient, *storage.Apply, []*storage.Task) {
			apply := activeTestApply("apply-stopped")
			apply.State = state.Apply.WaitingForDeploy
			return &mockTernClient{}, apply, nil
		}},
		{"cutover", "/api/cutover", storage.ControlOperationCutover, func() (*mockTernClient, *storage.Apply, []*storage.Task) {
			apply := activeTestApply("apply-stopped")
			apply.State = state.Apply.WaitingForCutover
			return &mockTernClient{}, apply, nil
		}},
	}
	for _, tc := range stoppedStartCases() {
		cases = append(cases, overrideControlCase{"start stopped " + tc.name, "/api/start", storage.ControlOperationStart,
			func() (*mockTernClient, *storage.Apply, []*storage.Task) {
				apply := tc.apply()
				return tc.client(), apply, tc.tasks(apply)
			}})
	}
	return cases
}

// A start or cutover during a freeze follows the same two-person path: the
// refusal names a request, an approval admits the repeat, and the apply log
// names the request, its people and its blockers.
func TestFreezeOverrideLetsAStartAndACutoverThrough(t *testing.T) {
	for _, tc := range overrideControlCases() {
		t.Run(tc.name, func(t *testing.T) {
			client, apply, tasks := tc.build()
			svc := newControlTestServiceWithTasks(client, apply, tasks)
			mock := svc.storage.(*mockStorageWithApplyStores)
			mock.freezes = newGateFreezeStore(t, gateTestFreeze("freeze-incident"))
			logs := &capturingApplyLogStore{}
			mock.applyLogs = logs
			store := withOverrides(svc)
			mux := serveRoutes(svc)

			rec := postAs(t, mux, overrideRequester, tc.path, overrideControl)
			require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
			request := decodeGateError(t, rec).OverrideRequest
			require.NotNil(t, request)
			assert.Equal(t, strings.TrimPrefix(tc.path, "/api/"), request.Action)
			assert.Equal(t, "apply-stopped", request.TargetID)
			pending, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, tc.operation)
			require.NoError(t, err)
			assert.Nil(t, pending, "a refused change must not be queued")

			approveOverride(t, store, request.ID)
			rec = postAs(t, mux, overrideRequester, tc.path, overrideControl)

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			pending, err = svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, tc.operation)
			require.NoError(t, err)
			assert.NotNil(t, pending)
			assert.True(t, hasApplyLogMessageContaining(logs.logs,
				"Freeze override "+request.ID+" admitted this change: requested by rita, approved by ida, covering freeze freeze-incident"))
		})
	}
}

// Under the reason policy a start or cutover goes ahead on its own reason, with
// no approver. Without a reason it is refused and the refusal says a reason
// would let it through. The admitted command records its override, and a
// start's override is the one the claim checkpoint honors.
func TestFreezeOverrideOnReasonLetsAStartAndACutoverThrough(t *testing.T) {
	for _, tc := range overrideControlCases() {
		t.Run(tc.name, func(t *testing.T) {
			client, apply, tasks := tc.build()
			svc := newControlTestServiceWithTasks(client, apply, tasks)
			mock := svc.storage.(*mockStorageWithApplyStores)
			mock.freezes = newGateFreezeStore(t, gateTestFreeze("freeze-incident"))
			logs := &capturingApplyLogStore{}
			mock.applyLogs = logs
			store := withOverrides(svc)
			svc.config.Freeze.OverridePolicy = "" // the default
			mux := serveRoutes(svc)
			noReason := strings.Replace(overrideControl, `,"override":"fix for incident 42"`, "", 1)

			rec := postAs(t, mux, overrideRequester, tc.path, noReason)
			require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
			refusal := decodeGateError(t, rec)
			assert.True(t, refusal.OverrideOnReason, "the refusal says a reason would let it through")
			assert.Nil(t, refusal.OverrideRequest)
			assert.Zero(t, store.count())

			rec = postAs(t, mux, overrideRequester, tc.path, overrideControl)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Equal(t, 1, store.count())
			stored := store.requests[0]
			assert.True(t, stored.OnReason())
			assert.Equal(t, strings.TrimPrefix(tc.path, "/api/"), stored.Action)
			assert.Equal(t, "apply-stopped", stored.TargetID)
			pending, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, tc.operation)
			require.NoError(t, err)
			require.NotNil(t, pending)
			assert.True(t, hasApplyLogMessageContaining(logs.logs,
				"Freeze override "+stored.Identifier+" admitted this change on the requester's reason: requested by rita"))
			if tc.operation == storage.ControlOperationStart {
				honored, err := svc.approvedOverridesForApply(t.Context(), apply)
				require.NoError(t, err)
				require.Len(t, honored, 1, "the claim checkpoint honors the start's override")
				assert.Equal(t, stored.Identifier, honored[0].Identifier)
			}

		})
	}
}

// An apply admitted on a reason does not carry that reason to its cutover:
// the override is keyed by action and target, so a cutover with no reason is
// refused although the apply's own override names the same freeze.
func TestFreezeOverrideOnReasonOfAnApplyDoesNotAdmitItsCutover(t *testing.T) {
	apply := activeTestApply("apply-stopped")
	apply.State = state.Apply.WaitingForCutover
	svc := newControlTestServiceWithTasks(&mockTernClient{}, apply, nil)
	mock := svc.storage.(*mockStorageWithApplyStores)
	mock.freezes = newGateFreezeStore(t, gateTestFreeze("freeze-incident"))
	store := withOverrides(svc)
	svc.config.Freeze.OverridePolicy = FreezeOverridePolicyReason
	own, err := store.CreateOnReason(t.Context(), &storage.FreezeOverrideRequest{
		Identifier: "ovr-apply", Environment: "staging", Action: storage.FreezeOverrideActionApply, TargetID: "testdb/@schema",
		Database: "testdb", Blockers: []string{"freeze freeze-incident"}, Reason: "fix for incident 42", RequestedBy: "rita",
		ExpiresAt: freezeTestNow.Add(time.Hour),
	})
	require.NoError(t, err)
	apply.SetOptions(storage.ApplyOptions{FreezeOverride: own.Identifier})

	rec := postAs(t, serveRoutes(svc), overrideRequester, "/api/cutover",
		strings.Replace(overrideControl, `,"override":"fix for incident 42"`, "", 1))

	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	assert.Equal(t, 1, store.count(), "nothing new is recorded without a reason")
}

// A gate that cannot read the approved requests, or record a new one, fails
// closed with the retryable 503; an override never gets past it.
func TestFreezeOverrideFailsClosedOnStorageErrors(t *testing.T) {
	t.Run("list approved", func(t *testing.T) {
		reader := newFreezeMetricsReader(t)
		svc, applies := newGateApplyTestService(t, gateTestFreeze("freeze-incident"))
		store := withOverrides(svc)
		store.listErr = errors.New("connection refused")

		rec := postAs(t, serveRoutes(svc), overrideRequester, "/api/apply", overrideApplyBody)

		require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
		assert.Equal(t, apitypes.ErrCodeChangeGateUnavailable, decodeGateError(t, rec).ErrorCode)
		assert.Zero(t, store.count())
		assert.Nil(t, applies.apply)
		assert.Equal(t, map[string]int64{"checkpoint=start,environment=staging": 1}, changeGateCounters(t, reader, "schemabot.change_gate.errors"))
	})
	t.Run("create", func(t *testing.T) {
		svc, applies := newGateApplyTestService(t, gateTestFreeze("freeze-incident"))
		store := withOverrides(svc)
		store.createErr = errors.New("connection refused")

		rec := postAs(t, serveRoutes(svc), overrideRequester, "/api/apply", overrideApplyBody)

		require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
		assert.Equal(t, apitypes.ErrCodeChangeGateUnavailable, decodeGateError(t, rec).ErrorCode)
		assert.Zero(t, store.count())
		assert.Nil(t, applies.apply)
	})
}

// A reason longer than 500 characters is a 400 before any gate runs, on all
// three routes. A reason of only whitespace is no reason: the change is
// refused with no request.
func TestFreezeOverrideReasonIsValidated(t *testing.T) {
	tooLong := `"override":"` + strings.Repeat("é", storage.FreezeOverrideReasonMax+1) + `"`
	atLimit := `"override":"` + strings.Repeat("é", storage.FreezeOverrideReasonMax) + `"`
	blank := `"override":"   "`
	for _, tc := range []struct{ path, state, body string }{
		{"/api/apply", "", `{"plan_id":"plan-1","environment":"staging",`},
		{"/api/cutover", state.Apply.WaitingForCutover, `{"environment":"staging","apply_id":"apply-stopped",`},
		{"/api/start", state.Apply.WaitingForDeploy, `{"environment":"staging","apply_id":"apply-stopped",`},
	} {
		t.Run(tc.path, func(t *testing.T) {
			apply := activeTestApply("apply-stopped")
			apply.State = tc.state
			svc := newControlTestService(&mockTernClient{}, apply)
			if tc.path == "/api/apply" {
				svc, _ = newExecuteApplyTestService(&mockTernClient{isRemote: true}, &capturingApplyStore{})
			}
			mock := svc.storage.(*mockStorageWithApplyStores)
			mock.freezes = newGateFreezeStore(t, gateTestFreeze("freeze-incident"))
			store := withOverrides(svc)
			mux := serveRoutes(svc)

			rec := postAs(t, mux, overrideRequester, tc.path, tc.body+tooLong+"}")
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			assert.Equal(t, apitypes.ErrCodeInvalidRequest, decodeGateError(t, rec).ErrorCode)

			rec = postAs(t, mux, overrideRequester, tc.path, tc.body+blank+"}")
			require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
			assert.Nil(t, decodeGateError(t, rec).OverrideRequest)
			assert.Zero(t, store.count())

			// An unverified asker would be recorded as nobody, and could
			// approve their own request.
			req := httptest.NewRequestWithContext(auth.WithUser(t.Context(), overrideRequester), http.MethodPost, tc.path, strings.NewReader(tc.body+atLimit+"}"))
			req.Header.Set("Content-Type", "application/json")
			rec = httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
			assert.Zero(t, store.count())

			rec = postAs(t, mux, overrideRequester, tc.path, tc.body+atLimit+"}")
			require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
			assert.NotNil(t, decodeGateError(t, rec).OverrideRequest)
		})
	}
}

// The override recorded on an apply is the server's to set: a caller's
// freeze_override option is dropped, as pinned_rollback is.
func TestFreezeOverrideOptionIsNotTheCallers(t *testing.T) {
	svc, applies := newGateApplyTestService(t)

	_, _, err := svc.ExecuteApply(t.Context(), ApplyRequest{
		PlanID: "plan-1", Environment: "staging", Options: map[string]string{"freeze_override": "ovr-forged"},
	})

	require.NoError(t, err)
	require.NotNil(t, applies.apply)
	assert.Empty(t, applies.apply.GetOptions().FreezeOverride)
}

// An approval that admitted the check does not outlast a freeze created before
// the apply is written: the write refuses, and the retry sees a blocker the
// approval does not name.
func TestFreezeOverrideDoesNotCoverAFreezeLandingBeforeTheWrite(t *testing.T) {
	svc, applies := newGateApplyTestService(t, gateTestFreeze("freeze-incident"))
	mock := svc.storage.(*mockStorageWithApplyStores)
	freezes := mock.freezes.(*memoryFreezeStore)
	mock.applies = epochCheckingApplyStore{ApplyStore: applies, freezes: freezes}
	store := withOverrides(svc)
	request, err := store.Create(t.Context(), &storage.FreezeOverrideRequest{
		Identifier: "ovr-1", Environment: "staging", Action: storage.FreezeOverrideActionApply, TargetID: gateApplyTarget(t, svc),
		Database: "testdb", Blockers: []string{"freeze freeze-incident"}, Reason: "fix", RequestedBy: "rita", ExpiresAt: freezeTestNow.Add(time.Hour),
	})
	require.NoError(t, err)
	approveOverride(t, store, request.Identifier)
	freezes.afterListInEffect = func() {
		_, err := freezes.Create(t.Context(), gateTestFreeze("freeze-late"))
		require.NoError(t, err)
	}

	rita := auth.WithVerifiedUser(t.Context(), overrideRequester)
	_, _, err = svc.ExecuteApply(rita, ApplyRequest{PlanID: "plan-1", Environment: "staging"})
	require.ErrorIs(t, err, storage.ErrFreezeEpochChanged)
	assert.Nil(t, applies.apply)

	_, _, err = svc.ExecuteApply(rita, ApplyRequest{PlanID: "plan-1", Environment: "staging"})
	require.ErrorAs(t, err, new(*ChangeGateRefusedError))
	assert.Nil(t, applies.apply)
}

// newOverrideApproveService is a freeze service with an override request
// from rita, the way the gate records one.
func newOverrideApproveService(t *testing.T, config *ServerConfig) (*Service, *http.ServeMux, *memoryFreezeOverrideStore, *clock.Fake) {
	t.Helper()
	svc, mux := newFreezeTestService(t, config)
	mock := svc.storage.(*mockStorageWithFreezes)
	fake := mock.freezes.(*memoryFreezeStore).clock.(*clock.Fake)
	store := &memoryFreezeOverrideStore{clock: fake}
	mock.overrides = store
	_, err := store.Create(t.Context(), &storage.FreezeOverrideRequest{
		Identifier: "ovr-1", Environment: "staging", Action: storage.FreezeOverrideActionApply, TargetID: "plan-1",
		Database: "testdb", Blockers: []string{"freeze freeze-incident"}, Reason: "fix", RequestedBy: "rita", ExpiresAt: freezeTestNow.Add(time.Hour),
	})
	require.NoError(t, err)
	return svc, mux, store, fake
}

func approveRoute(t *testing.T, mux *http.ServeMux, user *auth.User, id string) *httptest.ResponseRecorder {
	t.Helper()
	return doFreezeRequest(t, mux, user, http.MethodPost, "/api/freezes/overrides/"+id+"/approve", `{}`)
}

// Only a verified freeze-group member who is not the requester approves.
// Write groups do not grant approval, and nobody approves their own request.
func TestFreezeOverrideApproveAuthorization(t *testing.T) {
	_, mux, store, _ := newOverrideApproveService(t, freezeConfig())

	other := &auth.User{Subject: "rita", Groups: []string{"incident-commanders"}}
	rec := approveRoute(t, mux, other, "ovr-1")
	require.Equal(t, http.StatusForbidden, rec.Code, "the requester may not approve")
	assert.Contains(t, rec.Body.String(), "other than the person who asked")

	rec = approveRoute(t, mux, freezeAdmin, "ovr-1")
	require.Equal(t, http.StatusForbidden, rec.Code, "a write group does not approve")
	assert.Contains(t, rec.Body.String(), "freeze group (incident-commanders)")

	req := httptest.NewRequestWithContext(auth.WithUser(t.Context(), freezeMember), http.MethodPost, "/api/freezes/overrides/ovr-1/approve", strings.NewReader(`{}`))
	unverified := httptest.NewRecorder()
	mux.ServeHTTP(unverified, req)
	require.Equal(t, http.StatusForbidden, unverified.Code)
	assert.Contains(t, unverified.Body.String(), "needs a verified caller")

	rec = approveRoute(t, mux, freezeMember, "ovr-nope")
	require.Equal(t, http.StatusNotFound, rec.Code)

	stored, err := store.Get(t.Context(), "ovr-1")
	require.NoError(t, err)
	assert.Nil(t, stored.ApprovedAt, "no denied attempt approves")
}

// An approver approves once. The request reads back approved, and one past its
// expiry cannot be approved.
func TestFreezeOverrideApproveAndRead(t *testing.T) {
	_, mux, store, fake := newOverrideApproveService(t, freezeConfig())
	expiring, err := store.Create(t.Context(), &storage.FreezeOverrideRequest{
		Identifier: "ovr-2", Environment: "staging", Action: storage.FreezeOverrideActionCutover, TargetID: "apply-1",
		Database: "testdb", Blockers: []string{"freeze freeze-incident"}, Reason: "fix", RequestedBy: "rita", ExpiresAt: freezeTestNow.Add(time.Minute),
	})
	require.NoError(t, err)

	rec := approveRoute(t, mux, freezeMember, "ovr-1")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var approved apitypes.FreezeOverrideRequest
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &approved))
	assert.Equal(t, "ida", approved.ApprovedBy)
	assert.Equal(t, "rita", approved.RequestedBy)
	assert.Equal(t, []string{"freeze freeze-incident"}, approved.Blockers)

	assert.Equal(t, http.StatusConflict, approveRoute(t, mux, freezeMember, "ovr-1").Code, "a second approval conflicts")

	rec = doFreezeRequest(t, mux, nil, http.MethodGet, "/api/freezes/overrides/ovr-1", "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"approved_by":"ida"`)
	assert.Equal(t, http.StatusNotFound, doFreezeRequest(t, mux, nil, http.MethodGet, "/api/freezes/overrides/ovr-nope", "").Code)

	fake.Advance(time.Hour)
	assert.Equal(t, http.StatusConflict, approveRoute(t, mux, freezeMember, expiring.Identifier).Code, "an expired request cannot be approved")
}

// Only a freeze group grants approval. With none configured nobody may
// approve, a write-group member included, and the denial says so.
func TestAuthorizeFreezeApprove(t *testing.T) {
	assert.False(t, (&ServerConfig{}).AuthorizeFreezeApprove(nil).Allowed)
	writeGroupsOnly := &ServerConfig{Auth: AuthConfig{Type: "forward_auth", ForwardAuth: ForwardAuthSettings{WriteGroups: []string{"schema-admins"}}}}
	assert.False(t, writeGroupsOnly.AuthorizeFreezeApprove(freezeAdmin).Allowed)

	config := scopedWriteConfig() // operator groups, no freeze groups
	assert.False(t, config.AuthorizeFreezeApprove(freezeAdmin).Allowed)
	_, mux, _, _ := newOverrideApproveService(t, config)
	rec := approveRoute(t, mux, freezeAdmin, "ovr-1")
	require.Equal(t, http.StatusForbidden, rec.Code)
	assert.Contains(t, rec.Body.String(), "none are configured")
}

// gateApplyTarget is the override target of the gate tests' apply.
func gateApplyTarget(t *testing.T, svc *Service) string {
	t.Helper()
	target, err := svc.applyOverrideTargetFor(t.Context(), executeApplyTestPlan(), ApplyRequest{PlanID: "plan-1", Environment: "staging"})
	require.NoError(t, err)
	return target
}

// An apply's override covers what each member would run, not one plan: a
// repeated command plans afresh and must still find its approval, and any
// change to a member's work needs a new one.
func TestApplyOverrideTargetIsTheSchemaNotThePlan(t *testing.T) {
	on := func(deployment string, plan *storage.Plan) applyMember {
		return applyMember{Target: routing.ExecutionTarget{Deployment: deployment, Target: plan.Target}, Plan: plan}
	}
	key := func(target string, members ...applyMember) string {
		return applyOverrideTarget("testdb", target, members)
	}
	plan := executeApplyTestPlan()
	replanned := executeApplyTestPlan()
	replanned.PlanIdentifier = "plan-2"
	base := key("", on("main", plan))
	assert.Equal(t, base, key("", on("main", replanned)))

	changed := executeApplyTestPlan()
	changed.SchemaFiles = schema.SchemaFiles{"testdb": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY, email text)"}}}
	assert.NotEqual(t, base, key("", on("main", changed)))
	assert.NotEqual(t, base, key("member-a", on("main", plan)))

	// The same files on another member, after a routing change.
	assert.NotEqual(t, base, key("", on("other", plan)))

	// ignore_tables and drop_tables change what a member runs.
	ignoring := executeApplyTestPlan()
	ignoring.RecordIgnoreTables([]string{"audit"})
	assert.NotEqual(t, base, key("", on("main", ignoring)))
	dropping := executeApplyTestPlan()
	dropping.RecordDropTables([]string{"legacy"})
	assert.NotEqual(t, base, key("", on("main", dropping)))

	// A rollout that gained a member is a new target, and so is one whose
	// secondary member now runs a different schema.
	both := key("", on("main", plan), on("eu", plan))
	assert.NotEqual(t, base, both)
	assert.Equal(t, both, key("", on("eu", plan), on("main", plan)), "member order does not matter")
	assert.NotEqual(t, both, key("", on("main", plan), on("eu", changed)))
}

// The gate records only a verified requester. An in-process caller with no
// verified identity is refused with no request recorded.
func TestFreezeOverrideNeedsAVerifiedRequester(t *testing.T) {
	svc, applies := newGateApplyTestService(t, gateTestFreeze("freeze-incident"))
	store := withOverrides(svc)

	_, _, err := svc.ExecuteApply(t.Context(), ApplyRequest{PlanID: "plan-1", Environment: "staging", Override: "fix"})

	refused, ok := errors.AsType[*ChangeGateRefusedError](err)
	require.True(t, ok, "got %v", err)
	assert.Nil(t, refused.OverrideRequest)
	assert.Contains(t, refused.Error(), "asking for one needs a verified caller")
	assert.Zero(t, store.count())
	assert.Nil(t, applies.apply)
}

// An apply no freeze held names no override in its response.
func TestApplyResponseOmitsFreezeOverrideWhenNoFreezeHeldIt(t *testing.T) {
	svc, applies := newGateApplyTestService(t)
	withOverrides(svc)

	rec := postAs(t, serveRoutes(svc), overrideRequester, "/api/apply", overrideApplyBody)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NotNil(t, applies.apply)
	assert.Empty(t, applies.apply.GetOptions().FreezeOverride)
	assert.Equal(t, applies.apply.ApplyIdentifier, decodeApplyResponse(t, rec).ApplyID)
	assert.NotContains(t, rec.Body.String(), "freeze_override")
}

// Under the reason override policy, the default, an operator's own reason
// admits the change at once, like an audited deploy override. The request is stored approved by
// its requester, with the reason, the apply records it, and the override is
// counted. Without a reason the change is still refused, and the reason does
// not carry over: a retry gives its own, like a deploy override.
func TestFreezeOverrideOnReasonLetsAnApplyThrough(t *testing.T) {
	reader := newFreezeMetricsReader(t)
	svc, applies := newGateApplyTestService(t, gateTestFreeze("freeze-incident"))
	store := withOverrides(svc)
	svc.config.Freeze.OverridePolicy = "" // the default
	logs := &capturingApplyLogStore{}
	svc.storage.(*mockStorageWithApplyStores).applyLogs = logs
	mux := serveRoutes(svc)

	rec := postAs(t, mux, overrideRequester, "/api/apply", `{"plan_id":"plan-1","environment":"staging"}`)
	require.Equal(t, http.StatusConflict, rec.Code, "a freeze still refuses a change with no reason")
	assert.Nil(t, decodeGateError(t, rec).OverrideRequest)
	assert.Zero(t, store.count())

	rec = postAs(t, mux, overrideRequester, "/api/apply", overrideApplyBody)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NotNil(t, applies.apply)
	require.Equal(t, 1, store.count())
	id := applies.apply.GetOptions().FreezeOverride
	assert.Equal(t, id, decodeApplyResponse(t, rec).FreezeOverride, "the response names the request that admitted the apply")
	stored, err := store.Get(t.Context(), id)
	require.NoError(t, err)
	assert.True(t, stored.OnReason())
	assert.Equal(t, "rita", stored.RequestedBy)
	assert.Equal(t, "rita", stored.ApprovedBy)
	assert.Equal(t, "fix for incident 42", stored.Reason)
	assert.Equal(t, []string{"freeze freeze-incident"}, stored.Blockers)
	assert.Equal(t, freezeTestNow.Add(storage.FreezeOverrideMaxLifetime), stored.ExpiresAt)
	assert.True(t, hasApplyLogMessageContaining(logs.logs,
		"Freeze override "+id+` admitted this change on the requester's reason: requested by rita, covering freeze freeze-incident, reason "fix for incident 42"`),
		"the apply log records the reason that admitted it")

	applies.apply = nil
	rec = postAs(t, mux, overrideRequester, "/api/apply", `{"plan_id":"plan-1","environment":"staging"}`)
	require.Equal(t, http.StatusConflict, rec.Code, "a reason covers only the command that gave it, not a retry")
	assert.Nil(t, applies.apply)

	rec = postAs(t, mux, overrideRequester, "/api/apply", overrideApplyBody)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, 2, store.count(), "a retry with its own reason records its own override")
	assert.NotEqual(t, id, applies.apply.GetOptions().FreezeOverride)
	assert.Equal(t, applies.apply.GetOptions().FreezeOverride, decodeApplyResponse(t, rec).FreezeOverride)

	other := &auth.User{Subject: "sam", Groups: []string{"schema-admins"}}
	rec = postAs(t, mux, other, "/api/apply", `{"plan_id":"plan-1","environment":"staging"}`)
	require.Equal(t, http.StatusConflict, rec.Code, "another person's override never admits sam")

	assert.Equal(t, map[string]int64{"checkpoint=start,environment=staging,gate=freeze": 3}, changeGateCounters(t, reader, "schemabot.change_gate.refusals"))
	assert.Equal(t, map[string]int64{"checkpoint=start,environment=staging,gate=freeze": 2}, changeGateCounters(t, reader, "schemabot.change_gate.overrides"))
}

// Under the reason policy an unverified caller has no one to record the
// reason against, so the change is refused and nothing is stored.
func TestFreezeOverrideOnReasonNeedsAVerifiedRequester(t *testing.T) {
	svc, applies := newGateApplyTestService(t, gateTestFreeze("freeze-incident"))
	store := withOverrides(svc)
	svc.config.Freeze.OverridePolicy = FreezeOverridePolicyReason

	_, _, err := svc.ExecuteApply(t.Context(), ApplyRequest{PlanID: "plan-1", Environment: "staging", Override: "fix"})

	refused, ok := errors.AsType[*ChangeGateRefusedError](err)
	require.True(t, ok, "got %v", err)
	assert.Nil(t, refused.OverrideRequest)
	assert.True(t, refused.OverrideUnrecorded)
	assert.Zero(t, store.count())
	assert.Nil(t, applies.apply)
}

// A storage failure while recording the override fails closed.
func TestFreezeOverrideOnReasonFailsClosedOnStorageErrors(t *testing.T) {
	svc, applies := newGateApplyTestService(t, gateTestFreeze("freeze-incident"))
	store := withOverrides(svc)
	svc.config.Freeze.OverridePolicy = FreezeOverridePolicyReason
	store.createErr = errors.New("connection refused")

	_, _, err := svc.ExecuteApply(auth.WithVerifiedUser(t.Context(), overrideRequester), ApplyRequest{PlanID: "plan-1", Environment: "staging", Override: "fix"})

	unavailable, ok := errors.AsType[*ChangeGateUnavailableError](err)
	require.True(t, ok, "got %v", err)
	assert.Equal(t, "override", unavailable.Gate)
	assert.Nil(t, applies.apply)
}

func TestFreezeOverridePolicyIsValidated(t *testing.T) {
	for _, policy := range []string{"", FreezeOverridePolicyApproval, FreezeOverridePolicyReason} {
		config := testServerConfig()
		config.Freeze.OverridePolicy = policy
		require.NoError(t, config.Validate(), policy)
	}
	config := testServerConfig()
	config.Freeze.OverridePolicy = "self"
	require.ErrorContains(t, config.Validate(), "freeze.override_policy")
	assert.True(t, FreezeConfig{}.OverrideOnReason(), "the default admits on a reason")
	assert.False(t, FreezeConfig{OverridePolicy: FreezeOverridePolicyApproval}.OverrideOnReason())
	assert.True(t, FreezeConfig{OverridePolicy: FreezeOverridePolicyReason}.OverrideOnReason())
	for _, unknown := range []string{"self", "Approval", "Reason", "approvel"} {
		assert.False(t, FreezeConfig{OverridePolicy: unknown}.OverrideOnReason(),
			"an unvalidated value %q falls back to approval", unknown)
	}
}

// A server switched from reason back to approval honors only overrides a
// second person approved. An override admitted on its reason before the
// switch admits no new command, and asking again records a request for an
// approver.
func TestFreezeOverrideOnReasonDoesNotOutliveTheApprovalPolicy(t *testing.T) {
	svc, applies := newGateApplyTestService(t, gateTestFreeze("freeze-incident"))
	store := withOverrides(svc)
	svc.config.Freeze.OverridePolicy = FreezeOverridePolicyReason
	mux := serveRoutes(svc)
	require.Equal(t, http.StatusOK, postAs(t, mux, overrideRequester, "/api/apply", overrideApplyBody).Code)
	require.Equal(t, 1, store.count())
	applies.apply = nil

	svc.config.Freeze.OverridePolicy = FreezeOverridePolicyApproval

	rec := postAs(t, mux, overrideRequester, "/api/apply", `{"plan_id":"plan-1","environment":"staging"}`)
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	rec = postAs(t, mux, overrideRequester, "/api/apply", overrideApplyBody)
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	requested := decodeGateError(t, rec).OverrideRequest
	require.NotNil(t, requested, "asking again records a request for an approver")
	assert.Equal(t, 2, store.count())
	assert.Nil(t, applies.apply)
}
