package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// postApplyForRefusal sends body to POST /api/apply through the service's
// routes and decodes the error response the refusal carries.
func postApplyForRefusal(t *testing.T, svc *Service, body string) (int, apitypes.ErrorResponse) {
	t.Helper()
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/apply", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	var resp apitypes.ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp), "refusal body must be an error response")
	return w.Code, resp
}

// multiTargetApplyHTTPService serves the multi-target testapp/production
// environment over HTTP: the apply's plan is the primary's, and each member
// plan in plans is found by the member lookup apply creation runs.
func multiTargetApplyHTTPService(primary *storage.Plan, members []*storage.Plan) (*Service, *capturingApplyStore, *capturingTaskStore) {
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"testapp": {
				Type:         storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{"production": multiTargetEnv()},
			},
		},
	}
	applies := &capturingApplyStore{}
	tasks := &capturingTaskStore{}
	applies.taskStore = tasks
	plans := &listingPlanStore{mockPlanLookupStore: mockPlanLookupStore{plan: primary}, plans: members}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{
		plans:     plans,
		applies:   applies,
		tasks:     tasks,
		locks:     &emptyLockStore{},
		applyLogs: &noopApplyLogStore{},
		controls:  &memoryControlRequestStore{},
	}, cfg, map[string]tern.Client{"eu/production": &mockTernClient{}}, logger)
	return svc, applies, tasks
}

// An apply on a plan that carries a change the engine refuses can never
// succeed, whatever the caller retries with. It is refused as a 422 with the
// plan_blocked code, so clients stop retrying and server-error alerting does
// not count a request the caller has to fix.
func TestApplyHandler_BlockedPlanIsUnprocessable(t *testing.T) {
	plan := executeApplyTestPlan()
	plan.Namespaces["testdb"].Tables[0].ExecutionMode = "blocked"
	plan.Namespaces["testdb"].Tables[0].ModeReason = "requires privileges unavailable to the engine"
	applies := &capturingApplyStore{}
	svc, tasks := newQueueApplyTestService(plan, &mockTernClient{}, applies)

	// allow_unsafe cannot unlock a blocked change, so the refusal is the same
	// with or without it.
	for _, body := range []string{
		`{"plan_id":"plan-1","environment":"staging"}`,
		`{"plan_id":"plan-1","environment":"staging","options":{"allow_unsafe":"true"}}`,
	} {
		status, resp := postApplyForRefusal(t, svc, body)

		assert.Equal(t, http.StatusUnprocessableEntity, status, body)
		assert.Equal(t, apitypes.ErrCodePlanBlocked, resp.ErrorCode, body)
		assert.Equal(t, `apply rejected: stored plan plan-1 contains a blocked change for table "users": requires privileges unavailable to the engine`, resp.Error, body)
	}
	assert.False(t, apitypes.IsRetryableErrorCode(apitypes.ErrCodePlanBlocked))
	assert.Nil(t, applies.apply, "a blocked plan must not store an apply")
	assert.Empty(t, tasks.tasks)
}

// An apply on a plan with an unsafe change that did not consent to it is the
// caller's to fix by retrying with allow_unsafe=true, so it is a 400 with the
// unsafe_opt_in_required code a client can key its remedy on.
func TestApplyHandler_UnsafePlanWithoutOptInIsBadRequest(t *testing.T) {
	t.Run("table change", func(t *testing.T) {
		plan := executeApplyTestPlan()
		plan.Namespaces["testdb"].Tables[0] = storage.TableChange{
			Namespace: "testdb",
			Table:     "legacy_users",
			DDL:       "DROP TABLE `legacy_users`",
			Operation: "drop",
		}
		applies := &capturingApplyStore{}
		svc, tasks := newQueueApplyTestService(plan, &mockTernClient{}, applies)

		status, resp := postApplyForRefusal(t, svc, `{"plan_id":"plan-1","environment":"staging"}`)

		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, apitypes.ErrCodeUnsafeOptInRequired, resp.ErrorCode)
		assert.Equal(t, `apply rejected: stored plan plan-1 contains unsafe change for table "legacy_users": DROP TABLE removes all data; retry with allow_unsafe=true`, resp.Error)
		assert.Nil(t, applies.apply, "an unconsented unsafe change must not store an apply")
		assert.Empty(t, tasks.tasks)
	})

	t.Run("VSchema change", func(t *testing.T) {
		plan := vschemaGateTestPlan(map[string]string{
			storage.PlanMetadataVSchemaChanged:   "true",
			storage.PlanMetadataVSchemaDeletions: `[{"kind":"vindex","name":"email_idx","reason":"removing vindex email_idx changes query routing"}]`,
		})
		svc, applies, tasks := newVSchemaGateTestService(plan)

		status, resp := postApplyForRefusal(t, svc, `{"plan_id":"plan-1","environment":"staging"}`)

		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, apitypes.ErrCodeUnsafeOptInRequired, resp.ErrorCode)
		assert.Equal(t, `apply rejected: stored plan plan-1 contains an unsafe VSchema change in namespace "testdb": removing vindex email_idx changes query routing; retry with allow_unsafe=true`, resp.Error)
		assert.Nil(t, applies.apply, "an unconsented unsafe VSchema change must not store an apply")
		assert.Empty(t, tasks.tasks)
	})
	assert.False(t, apitypes.IsRetryableErrorCode(apitypes.ErrCodeUnsafeOptInRequired))
}

// A rollout member planned against its own live schema can carry a change the
// primary's plan does not. Its refusal names the member and gets the same
// status and code as the primary's would, because the caller's remedy is the
// same.
func TestApplyHandler_MemberPlanRefusalsAreClientErrors(t *testing.T) {
	primary := primaryPlanRow("testapp-001")
	primary.Environment = "production"

	t.Run("blocked member plan", func(t *testing.T) {
		member := memberPlanWithChange(storage.TableChange{
			Namespace:     "testapp",
			Table:         "orders",
			Operation:     "alter",
			DDL:           "ALTER TABLE `orders` ADD COLUMN `region` varchar(16)",
			ExecutionMode: "blocked",
			ModeReason:    "the engine refuses this statement",
		})
		svc, applies, tasks := multiTargetApplyHTTPService(primary, []*storage.Plan{member})

		status, resp := postApplyForRefusal(t, svc, `{"plan_id":"plan-primary","environment":"production"}`)

		assert.Equal(t, http.StatusUnprocessableEntity, status)
		assert.Equal(t, apitypes.ErrCodePlanBlocked, resp.ErrorCode)
		assert.Equal(t, `apply rejected: rollout member eu/testapp-002: stored plan plan-second contains a blocked change for table "orders": the engine refuses this statement`, resp.Error)
		assert.Nil(t, applies.apply, "a blocked member plan must not store an apply")
		assert.Empty(t, tasks.tasks)
	})

	t.Run("unsafe member plan without opt-in", func(t *testing.T) {
		member := memberPlanWithChange(storage.TableChange{
			Namespace: "testapp",
			Table:     "legacy_orders",
			Operation: "drop",
			DDL:       "DROP TABLE `legacy_orders`",
		})
		svc, applies, tasks := multiTargetApplyHTTPService(primary, []*storage.Plan{member})

		status, resp := postApplyForRefusal(t, svc, `{"plan_id":"plan-primary","environment":"production"}`)

		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, apitypes.ErrCodeUnsafeOptInRequired, resp.ErrorCode)
		assert.Equal(t, `apply rejected: rollout member eu/testapp-002: stored plan plan-second contains unsafe change for table "legacy_orders": DROP TABLE removes all data; retry with allow_unsafe=true`, resp.Error)
		assert.Nil(t, applies.apply, "an unconsented unsafe member plan must not store an apply")
		assert.Empty(t, tasks.tasks)
	})
}
