package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// productionPaymentsStorage resolves every stored object to the payments
// database in production. The payments-team operator's grant covers payments
// but only in staging, so any route that authorizes on these objects must deny
// on the environment alone. The rollback source chain (completed apply -> plan
// with captured original files -> latest completed task) is consistent so the
// rollback route reaches its authorization check rather than failing input
// validation first.
//
// Every write the plan, apply, and lock stores accept is appended to writes,
// so a test can require that a denied request changed nothing. A write through
// a store method the fixture does not implement panics, which fails the test
// just as loudly.
func productionPaymentsStorage(writes *[]string) *mockStorageWithApplyStores {
	sourcePlan := &storage.Plan{
		ID:          7,
		Database:    "payments",
		Environment: "production",
		Namespaces: map[string]*storage.NamespacePlanData{
			"payments": {OriginalFilesCaptured: true},
		},
	}
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-1",
		Database:        "payments",
		DatabaseType:    "mysql",
		Environment:     "production",
		State:           state.Apply.Completed,
		PlanID:          7,
	}
	task := &storage.Task{
		ApplyID:      42,
		PlanID:       7,
		Database:     "payments",
		DatabaseType: "mysql",
		Environment:  "production",
		State:        state.Task.Completed,
	}
	return &mockStorageWithApplyStores{
		plans: &writeRecordingPlanStore{
			sweepPlanStore: sweepPlanStore{
				mockPlanLookupStore: mockPlanLookupStore{plan: &storage.Plan{
					PlanIdentifier: "plan-1",
					Database:       "payments",
					Environment:    "production",
				}},
				byID: sourcePlan,
			},
			writes: writes,
		},
		applies: &writeRecordingApplyStore{staticApplyStore: staticApplyStore{apply: apply}, writes: writes},
		tasks:   &staticTaskStore{tasks: []*storage.Task{task}},
		locks:   &writeRecordingLockStore{writes: writes},
	}
}

// writeRecordingPlanStore records the plan writes the shared plan doubles
// accept silently.
type writeRecordingPlanStore struct {
	sweepPlanStore
	writes *[]string
}

func (s *writeRecordingPlanStore) Create(context.Context, *storage.Plan) (int64, error) {
	*s.writes = append(*s.writes, "plans.Create")
	return 0, nil
}

func (s *writeRecordingPlanStore) UpdateRoute(context.Context, string, string, string, string) error {
	*s.writes = append(*s.writes, "plans.UpdateRoute")
	return nil
}

func (s *writeRecordingPlanStore) Delete(context.Context, int64) error {
	*s.writes = append(*s.writes, "plans.Delete")
	return nil
}

func (s *writeRecordingPlanStore) DeleteByPR(context.Context, string, int) error {
	*s.writes = append(*s.writes, "plans.DeleteByPR")
	return nil
}

// writeRecordingApplyStore records the apply write the shared apply double
// accepts silently.
type writeRecordingApplyStore struct {
	staticApplyStore
	writes *[]string
}

func (s *writeRecordingApplyStore) SetRevertSkipped(context.Context, int64, time.Time) error {
	*s.writes = append(*s.writes, "applies.SetRevertSkipped")
	return nil
}

// writeRecordingLockStore records lock releases.
type writeRecordingLockStore struct {
	storage.LockStore
	writes *[]string
}

func (s *writeRecordingLockStore) Release(context.Context, string, string, string) error {
	*s.writes = append(*s.writes, "locks.Release")
	return nil
}

func (s *writeRecordingLockStore) ForceRelease(context.Context, string, string) error {
	*s.writes = append(*s.writes, "locks.ForceRelease")
	return nil
}

func (s *writeRecordingLockStore) ForceReleaseByID(context.Context, int64, string, string) error {
	*s.writes = append(*s.writes, "locks.ForceReleaseByID")
	return nil
}

// serveAsPaymentsOperator sends one request through the full route table as a
// member of payments-team, whose grant covers the payments database in staging
// only, and returns the status and the decoded error message.
func serveAsPaymentsOperator(t *testing.T, mux *http.ServeMux, method, path, body string) (int, string) {
	t.Helper()
	operator := &auth.User{Subject: "bob", Groups: []string{"payments-team"}}
	ctx := auth.WithUser(t.Context(), operator)
	req := httptest.NewRequestWithContext(ctx, method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	var resp apitypes.ErrorResponse
	if rec.Code >= http.StatusBadRequest {
		require.NoErrorf(t, json.Unmarshal(rec.Body.Bytes(), &resp),
			"error response must be JSON; got body %q", rec.Body.String())
	}
	return rec.Code, resp.Error
}

// productionPaymentsMux serves the full route table over a fresh
// productionPaymentsStorage, so each request sees its own write record.
func productionPaymentsMux() (*http.ServeMux, *[]string) {
	writes := &[]string{}
	svc := New(productionPaymentsStorage(writes), scopedWriteConfig(), nil, slog.New(slog.DiscardHandler))
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	return mux, writes
}

// environmentDenial is the message only the environment branch of the
// operator grant check produces.
const environmentDenial = `on database "payments" is not allowed in environment "production" for your operator grant (allowed: staging)`

// environmentScopeFixtures is a request body, against production payments
// work, for every write-tier route that authorizes on an environment.
var environmentScopeFixtures = map[string]string{
	"POST /api/plan":          `{"database":"payments","environment":"production","type":"mysql","schema_files":{"default":{"files":{"t.sql":"CREATE TABLE t (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)"}}}}`,
	"POST /api/apply":         `{"plan_id":"plan-1","environment":"production"}`,
	"POST /api/cutover":       `{"apply_id":"apply-1","environment":"production"}`,
	"POST /api/stop":          `{"apply_id":"apply-1","environment":"production"}`,
	"POST /api/cancel":        `{"apply_id":"apply-1","environment":"production"}`,
	"POST /api/start":         `{"apply_id":"apply-1","environment":"production"}`,
	"POST /api/release":       `{"apply_id":"apply-1","environment":"production"}`,
	"POST /api/revert":        `{"apply_id":"apply-1","environment":"production"}`,
	"POST /api/skip-revert":   `{"apply_id":"apply-1","environment":"production"}`,
	"POST /api/rollback/plan": `{"apply_id":"apply-1","environment":"production"}`,
}

// An operator grant can be limited to some environments, and the middleware
// admits the operator before the target environment is known. This sweep walks
// the complete route table and drives every write-tier route as an operator
// whose grant covers the target database but not its environment. Each
// environment-scoped route must answer 403 with the environment denial and
// write nothing. Lock routes carry no environment and admin-only routes are
// denied on the admin requirement, so both are listed as exempt with the
// reason. A mutating route in neither list fails the sweep until its
// environment handling is decided.
func TestMutatingRoutesDenyOperatorOutsideGrantedEnvironment(t *testing.T) {
	exempt := map[string]string{
		"POST /api/locks/acquire":        "database locks carry no environment; the database grant alone decides them",
		"DELETE /api/locks":              "database locks carry no environment; the database grant alone decides them",
		"POST /api/settings":             "admin-only; TestMutatingRoutesDenyScopedOperatorByDefault covers the denial",
		"POST /api/freezes":              "freeze writes need a write group or a freeze group; TestMutatingRoutesDenyScopedOperatorByDefault covers the denial",
		"POST /api/freezes/{id}/lift":    "freeze writes need a write group or a freeze group; TestMutatingRoutesDenyScopedOperatorByDefault covers the denial",
		"POST /api/checks/scan":          "admin-only; TestMutatingRoutesDenyScopedOperatorByDefault covers the denial",
		"POST /api/checks/synthesize":    "admin-only; TestMutatingRoutesDenyScopedOperatorByDefault covers the denial",
		"POST /api/checks/repos":         "admin-only; TestMutatingRoutesDenyScopedOperatorByDefault covers the denial",
		"POST /api/storage/schema/plan":  "admin-only; TestMutatingRoutesDenyScopedOperatorByDefault covers the denial",
		"POST /api/storage/schema/apply": "admin-only; TestMutatingRoutesDenyScopedOperatorByDefault covers the denial",

		"POST /api/freezes/overrides/{id}/approve": "needs a freeze group; TestFreezeOverrideApproveAuthorization covers the denial",
	}

	svc := New(productionPaymentsStorage(&[]string{}), scopedWriteConfig(), nil, slog.New(slog.DiscardHandler))
	used := make(map[string]bool)
	for _, route := range svc.apiRoutes() {
		method, path, found := strings.Cut(route.pattern, " ")
		require.Truef(t, found, "route pattern %q must be METHOD /path", route.pattern)
		if auth.TierForRequest(method, path) == auth.TierRead {
			continue
		}
		if _, ok := exempt[route.pattern]; ok {
			// Checked against the route table by the unused-exemption loop below.
			used[route.pattern] = true
			continue
		}

		t.Run(fmt.Sprintf("%s %s", method, path), func(t *testing.T) {
			body, ok := environmentScopeFixtures[route.pattern]
			require.Truef(t, ok,
				"mutating route %q has no environment-scope fixture: add one here if it acts on an environment, or list it as exempt with the reason",
				route.pattern)
			used[route.pattern] = true
			mux, writes := productionPaymentsMux()

			code, message := serveAsPaymentsOperator(t, mux, method, path, body)

			assert.Equalf(t, http.StatusForbidden, code,
				"an operator whose grant excludes production must be denied with 403; got %d message %q", code, message)
			assert.Contains(t, message, environmentDenial)
			assert.Empty(t, *writes, "a denied request must write nothing")
		})
	}

	for pattern := range environmentScopeFixtures {
		assert.Truef(t, used[pattern], "fixture %q matches no registered mutating route; remove it", pattern)
	}
	for pattern := range exempt {
		assert.Truef(t, used[pattern], "exemption %q matches no registered mutating route; remove it", pattern)
	}
}

// An operator allowed only in staging cannot reach production work by naming
// staging in the request body. Routes that act on a stored apply or plan must
// reject the request and write nothing, never authorize it against the
// environment the caller chose. Today they answer 400 naming the stored
// environment; authorizing on the stored environment first would answer the
// 403 environment denial instead, which is equally safe, so either passes.
// Every environment-scoped route needs a case here or an exemption saying why
// it has no stored environment to contradict.
func TestStoredProductionWorkRejectsBodyNamingGrantedEnvironment(t *testing.T) {
	controlBody := `{"apply_id":"apply-1","environment":"staging"}`
	const applyMismatch = `apply "apply-1" belongs to environment "production", not "staging"`
	type mismatch struct {
		body string
		want string
	}
	cases := map[string]mismatch{
		"POST /api/apply":         {`{"plan_id":"plan-1","environment":"staging"}`, `plan plan-1 was created for environment "production", not "staging"`},
		"POST /api/cutover":       {controlBody, applyMismatch},
		"POST /api/stop":          {controlBody, applyMismatch},
		"POST /api/cancel":        {controlBody, applyMismatch},
		"POST /api/start":         {controlBody, applyMismatch},
		"POST /api/release":       {controlBody, applyMismatch},
		"POST /api/revert":        {controlBody, applyMismatch},
		"POST /api/skip-revert":   {controlBody, applyMismatch},
		"POST /api/rollback/plan": {controlBody, applyMismatch},
	}
	exempt := map[string]string{
		"POST /api/plan": "a plan creates new work: the body's environment is the only one, so there is nothing stored to contradict",
	}

	for route := range environmentScopeFixtures {
		_, hasCase := cases[route]
		_, isExempt := exempt[route]
		assert.Truef(t, hasCase != isExempt,
			"environment-scoped route %q needs exactly one of a mismatch case or an exemption with the reason", route)
	}
	for route := range cases {
		assert.Containsf(t, environmentScopeFixtures, route, "mismatch case %q names no environment-scoped route; remove it", route)
	}
	for route := range exempt {
		assert.Containsf(t, environmentScopeFixtures, route, "mismatch exemption %q names no environment-scoped route; remove it", route)
	}

	for route, tc := range cases {
		t.Run(route, func(t *testing.T) {
			method, path, found := strings.Cut(route, " ")
			require.True(t, found)
			mux, writes := productionPaymentsMux()

			code, message := serveAsPaymentsOperator(t, mux, method, path, tc.body)

			switch code {
			case http.StatusBadRequest:
				assert.Contains(t, message, tc.want)
			case http.StatusForbidden:
				assert.Contains(t, message, environmentDenial)
			default:
				assert.Failf(t, "a body naming staging for production work must be rejected",
					"want 400 naming the stored environment or 403 with the environment denial; got %d message %q", code, message)
			}
			assert.Empty(t, *writes, "a rejected request must write nothing")
		})
	}
}
