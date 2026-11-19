package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
func productionPaymentsStorage() *mockStorageWithApplyStores {
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
		plans: &sweepPlanStore{
			mockPlanLookupStore: mockPlanLookupStore{plan: &storage.Plan{
				PlanIdentifier: "plan-1",
				Database:       "payments",
				Environment:    "production",
			}},
			byID: sourcePlan,
		},
		applies: &staticApplyStore{apply: apply},
		tasks:   &staticTaskStore{tasks: []*storage.Task{task}},
		locks:   &recordingLockStore{},
	}
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

// An operator grant can be limited to some environments, and the middleware
// admits the operator before the target environment is known. This sweep walks
// the complete route table and drives every write-tier route as an operator
// whose grant covers the target database but not its environment. Each
// environment-scoped route must answer 403 with the environment denial. Lock
// routes carry no environment and admin-only routes are denied on the admin
// requirement, so both are listed as exempt with the reason. A mutating route
// in neither list fails the sweep until its environment handling is decided.
func TestMutatingRoutesDenyOperatorOutsideGrantedEnvironment(t *testing.T) {
	controlBody := `{"apply_id":"apply-1","environment":"production"}`
	fixtures := map[string]string{
		"POST /api/plan":          `{"database":"payments","environment":"production","type":"mysql","schema_files":{"default":{"files":{"t.sql":"CREATE TABLE t (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)"}}}}`,
		"POST /api/apply":         `{"plan_id":"plan-1","environment":"production"}`,
		"POST /api/cutover":       controlBody,
		"POST /api/stop":          controlBody,
		"POST /api/cancel":        controlBody,
		"POST /api/start":         controlBody,
		"POST /api/release":       controlBody,
		"POST /api/revert":        controlBody,
		"POST /api/skip-revert":   controlBody,
		"POST /api/rollback/plan": controlBody,
	}
	exempt := map[string]string{
		"POST /api/locks/acquire":        "database locks carry no environment; the database grant alone decides them",
		"DELETE /api/locks":              "database locks carry no environment; the database grant alone decides them",
		"POST /api/settings":             "admin-only; TestMutatingRoutesDenyScopedOperatorByDefault covers the denial",
		"POST /api/checks/scan":          "admin-only; TestMutatingRoutesDenyScopedOperatorByDefault covers the denial",
		"POST /api/checks/synthesize":    "admin-only; TestMutatingRoutesDenyScopedOperatorByDefault covers the denial",
		"POST /api/checks/repos":         "admin-only; TestMutatingRoutesDenyScopedOperatorByDefault covers the denial",
		"POST /api/storage/schema/plan":  "admin-only; TestMutatingRoutesDenyScopedOperatorByDefault covers the denial",
		"POST /api/storage/schema/apply": "admin-only; TestMutatingRoutesDenyScopedOperatorByDefault covers the denial",
	}

	svc := New(productionPaymentsStorage(), scopedWriteConfig(), nil, slog.New(slog.DiscardHandler))
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	used := make(map[string]bool)
	for _, route := range svc.apiRoutes() {
		method, path, found := strings.Cut(route.pattern, " ")
		require.Truef(t, found, "route pattern %q must be METHOD /path", route.pattern)
		if auth.TierForRequest(method, path) == auth.TierRead {
			continue
		}

		t.Run(fmt.Sprintf("%s %s", method, path), func(t *testing.T) {
			if reason, ok := exempt[route.pattern]; ok {
				used[route.pattern] = true
				t.Skipf("exempt from the environment sweep: %s", reason)
			}
			body, ok := fixtures[route.pattern]
			require.Truef(t, ok,
				"mutating route %q has no environment-scope fixture: add one here if it acts on an environment, or list it as exempt with the reason",
				route.pattern)
			used[route.pattern] = true

			code, message := serveAsPaymentsOperator(t, mux, method, path, body)

			assert.Equalf(t, http.StatusForbidden, code,
				"an operator whose grant excludes production must be denied with 403; got %d message %q", code, message)
			assert.Contains(t, message, `on database "payments" is not allowed in environment "production" for your operator grant (allowed: staging)`)
		})
	}

	for pattern := range fixtures {
		assert.Truef(t, used[pattern], "fixture %q matches no registered mutating route; remove it", pattern)
	}
	for pattern := range exempt {
		assert.Truef(t, used[pattern], "exemption %q matches no registered mutating route; remove it", pattern)
	}
}

// An operator allowed only in staging cannot reach production work by naming
// staging in the request body. Routes that act on a stored apply or plan check
// the body's environment against the stored one before anything runs, so the
// request is rejected with a 400 that names the stored environment, never
// authorized against the environment the caller chose.
func TestStoredProductionWorkRejectsBodyNamingGrantedEnvironment(t *testing.T) {
	controlBody := `{"apply_id":"apply-1","environment":"staging"}`
	const applyMismatch = `apply "apply-1" belongs to environment "production", not "staging"`
	cases := []struct {
		route string
		body  string
		want  string
	}{
		{"POST /api/apply", `{"plan_id":"plan-1","environment":"staging"}`, `plan plan-1 was created for environment "production", not "staging"`},
		{"POST /api/cutover", controlBody, applyMismatch},
		{"POST /api/stop", controlBody, applyMismatch},
		{"POST /api/cancel", controlBody, applyMismatch},
		{"POST /api/start", controlBody, applyMismatch},
		{"POST /api/release", controlBody, applyMismatch},
		{"POST /api/revert", controlBody, applyMismatch},
		{"POST /api/skip-revert", controlBody, applyMismatch},
		{"POST /api/rollback/plan", controlBody, applyMismatch},
	}

	svc := New(productionPaymentsStorage(), scopedWriteConfig(), nil, slog.New(slog.DiscardHandler))
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	for _, tc := range cases {
		t.Run(tc.route, func(t *testing.T) {
			method, path, found := strings.Cut(tc.route, " ")
			require.True(t, found)

			code, message := serveAsPaymentsOperator(t, mux, method, path, tc.body)

			assert.Equalf(t, http.StatusBadRequest, code,
				"a body naming staging for production work must be rejected with 400; got %d message %q", code, message)
			assert.Contains(t, message, tc.want)
		})
	}
}
