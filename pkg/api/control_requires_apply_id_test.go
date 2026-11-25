package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// A control request that names no apply is refused before SchemaBot looks for
// one, even when the environment holds exactly one active apply it could have
// meant (CO-11). The route list comes from the storage registry, so a new
// control operation is held to the rule without anyone extending this test.
// Rollback is checked beside them: it acts on the apply it reverses.
func TestEveryControlRouteRefusesARequestWithoutAnApplyID(t *testing.T) {
	routes := make([]string, 0, len(storage.ControlOperations())+1)
	for _, op := range storage.ControlOperations() {
		routes = append(routes, "/api/"+op.Command())
	}
	routes = append(routes, "/api/rollback/plan")

	for _, route := range routes {
		t.Run(route, func(t *testing.T) {
			mock := &mockTernClient{}
			svc, stores := newControlTestServiceWithStores(mock, activeTestApply("apply-the-only-active-one"), nil)
			mux := http.NewServeMux()
			svc.ConfigureRoutes(mux)

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, route, strings.NewReader(`{"environment": "staging"}`))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			assert.Contains(t, w.Body.String(), "apply_id is required")
			assert.Empty(t, stores.controls.requests, "a refused request must record no control request")
		})
	}
}
