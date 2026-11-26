package api

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// A control request that names no apply is refused before SchemaBot looks for
// one, even when the environment holds exactly one active apply it could have
// meant (CO-11). The handler reads no apply at all: a lookup it made and then
// discarded would still be the guess the rule forbids. The route list comes from the storage registry, so a new
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
			applies := &countingApplyStore{staticApplyStore: staticApplyStore{apply: activeTestApply("apply-the-only-active-one")}}
			controls := &memoryControlRequestStore{}
			svc := New(&mockStorageWithApplyStores{
				applies:   applies,
				tasks:     &capturingTaskStore{},
				controls:  controls,
				applyLogs: &capturingApplyLogStore{},
			}, testServerConfig(), map[string]tern.Client{"default/staging": &mockTernClient{}},
				slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
			mux := http.NewServeMux()
			svc.ConfigureRoutes(mux)

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, route, strings.NewReader(`{"environment": "staging"}`))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			assert.Contains(t, w.Body.String(), "apply_id is required")
			assert.Empty(t, controls.requests, "a refused request must record no control request")
			assert.Zero(t, applies.reads.Load(), "the handler looked up an apply it was not given")
		})
	}
}

// countingApplyStore counts every apply read a handler makes. Only the reads
// below are implemented, so any other apply store call panics and fails the
// test rather than going uncounted.
type countingApplyStore struct {
	staticApplyStore
	reads atomic.Int64
}

func (s *countingApplyStore) GetInProgress(ctx context.Context) ([]*storage.Apply, error) {
	s.reads.Add(1)
	return s.staticApplyStore.GetInProgress(ctx)
}

func (s *countingApplyStore) GetUnsettled(ctx context.Context) ([]*storage.Apply, error) {
	s.reads.Add(1)
	return s.staticApplyStore.GetUnsettled(ctx)
}

func (s *countingApplyStore) GetByApplyIdentifier(ctx context.Context, applyIdentifier string) (*storage.Apply, error) {
	s.reads.Add(1)
	return s.staticApplyStore.GetByApplyIdentifier(ctx, applyIdentifier)
}

func (s *countingApplyStore) Get(ctx context.Context, id int64) (*storage.Apply, error) {
	s.reads.Add(1)
	return s.staticApplyStore.Get(ctx, id)
}
