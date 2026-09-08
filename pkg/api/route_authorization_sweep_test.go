package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// sweepPlanStore serves the sweep's stored-plan resolutions: Get resolves the
// apply path's plan lookup, GetByID resolves the rollback source plan.
type sweepPlanStore struct {
	mockPlanLookupStore
	byID *storage.Plan
}

func (s *sweepPlanStore) GetByID(context.Context, int64) (*storage.Plan, error) {
	return s.byID, nil
}

// staticTaskStore returns a fixed task list for any database.
type staticTaskStore struct {
	storage.TaskStore
	tasks []*storage.Task
}

func (s *staticTaskStore) GetByDatabase(context.Context, string) ([]*storage.Task, error) {
	return s.tasks, nil
}

// The forward-auth middleware admits every operator-group member to every
// write-tier route before the target database is known, so per-database scope
// exists only if each mutating handler individually re-establishes it. This
// sweep walks the service's complete route table with the same tier rule the
// middleware applies, drives every write-tier route as a scoped operator whose
// grant does not cover the fixture's target, and requires a 403. A mutating
// route without a fixture fails the sweep — a new endpoint cannot ship until
// it proves it denies scoped operators by default.
func TestMutatingRoutesDenyScopedOperatorByDefault(t *testing.T) {
	// Every stored object resolves to the orders database, which the
	// payments-team operator's grant does not cover. The rollback source
	// chain (completed apply -> plan with captured original files -> latest
	// completed task) is fully consistent so the rollback route reaches its
	// authorization check rather than failing input validation first.
	sourcePlan := &storage.Plan{
		ID:          7,
		Database:    "orders",
		Environment: "staging",
		Namespaces: map[string]*storage.NamespacePlanData{
			"orders": {OriginalFilesCaptured: true},
		},
	}
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-1",
		Database:        "orders",
		DatabaseType:    "mysql",
		Environment:     "staging",
		State:           state.Apply.Completed,
		PlanID:          7,
	}
	task := &storage.Task{
		ApplyID:      42,
		PlanID:       7,
		Database:     "orders",
		DatabaseType: "mysql",
		Environment:  "staging",
		State:        state.Task.Completed,
	}
	st := &mockStorageWithApplyStores{
		plans: &sweepPlanStore{
			mockPlanLookupStore: mockPlanLookupStore{plan: &storage.Plan{
				PlanIdentifier: "plan-1",
				Database:       "orders",
				Environment:    "staging",
			}},
			byID: sourcePlan,
		},
		applies: &staticApplyStore{apply: apply},
		tasks:   &staticTaskStore{tasks: []*storage.Task{task}},
		locks:   &recordingLockStore{},
	}

	controlBody := `{"apply_id":"apply-1","environment":"staging"}`
	lockBody := `{"database":"orders","database_type":"mysql","owner":"sweep"}`
	fixtures := map[string]string{
		"POST /api/plan":              `{"database":"orders","environment":"staging","type":"mysql","schema_files":{"default":{"files":{"t.sql":"CREATE TABLE t (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)"}}}}`,
		"POST /api/apply":             `{"plan_id":"plan-1","environment":"staging"}`,
		"POST /api/cutover":           controlBody,
		"POST /api/stop":              controlBody,
		"POST /api/cancel":            controlBody,
		"POST /api/start":             controlBody,
		"POST /api/release":           controlBody,
		"POST /api/revert":            controlBody,
		"POST /api/skip-revert":       controlBody,
		"POST /api/rollback/plan":     controlBody,
		"POST /api/locks/acquire":     lockBody,
		"DELETE /api/locks":           lockBody,
		"POST /api/settings":          `{"key":"pause_applies","value":"true"}`,
		"POST /api/checks/scan":       `{}`,
		"POST /api/checks/synthesize": `{}`,
		"POST /api/checks/repos":      `{}`,
		"POST /api/webhooks/redrive":  `{}`,
	}

	svc := New(st, scopedWriteConfig(), nil, slog.New(slog.DiscardHandler))
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	operator := &auth.User{Subject: "bob", Groups: []string{"payments-team"}}

	used := make(map[string]bool)
	for _, route := range svc.apiRoutes() {
		method, path, found := strings.Cut(route.pattern, " ")
		require.Truef(t, found, "route pattern %q must be METHOD /path", route.pattern)
		if auth.TierForRequest(method, path) == auth.TierRead {
			continue
		}

		t.Run(fmt.Sprintf("%s %s", method, path), func(t *testing.T) {
			body, ok := fixtures[route.pattern]
			require.Truef(t, ok,
				"mutating route %q has no scoped-operator denial fixture: add one here and ensure the handler enforces an authorizeDirect* check before mutating anything",
				route.pattern)
			used[route.pattern] = true

			ctx := auth.WithUser(t.Context(), operator)
			req := httptest.NewRequestWithContext(ctx, method, path, strings.NewReader(body))
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			assert.Equalf(t, http.StatusForbidden, rec.Code,
				"a scoped operator outside their grant must be denied with 403; got %d body %q", rec.Code, rec.Body.String())
		})
	}

	for pattern := range fixtures {
		assert.Truef(t, used[pattern], "fixture %q matches no registered mutating route; remove it", pattern)
	}
}
