package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
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
	"github.com/block/schemabot/pkg/storage"
)

// memoryFreezeStore is an in-memory storage.FreezeStore with the SQL store's
// lift semantics.
type memoryFreezeStore struct {
	mu      sync.Mutex
	freezes []*storage.EnvironmentFreeze
}

func (m *memoryFreezeStore) Create(_ context.Context, freeze *storage.EnvironmentFreeze) (*storage.EnvironmentFreeze, error) {
	if err := freeze.Validate(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stored := *freeze
	stored.CreatedAt = time.Now().UTC()
	m.freezes = append(m.freezes, &stored)
	copied := stored
	return &copied, nil
}

func (m *memoryFreezeStore) Get(_ context.Context, identifier string) (*storage.EnvironmentFreeze, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, freeze := range m.freezes {
		if freeze.Identifier == identifier {
			copied := *freeze
			return &copied, nil
		}
	}
	return nil, storage.ErrFreezeNotFound
}

func (m *memoryFreezeStore) Lift(_ context.Context, identifier, liftedBy, reason string, at time.Time) (*storage.EnvironmentFreeze, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, freeze := range m.freezes {
		if freeze.Identifier != identifier {
			continue
		}
		switch {
		case freeze.LiftedAt != nil:
			return nil, storage.ErrFreezeAlreadyLifted
		case freeze.EndsAt != nil && !at.Before(*freeze.EndsAt):
			return nil, storage.ErrFreezeEnded
		}
		freeze.LiftedBy, freeze.LiftReason, freeze.LiftedAt = liftedBy, reason, &at
		copied := *freeze
		return &copied, nil
	}
	return nil, storage.ErrFreezeNotFound
}

func (m *memoryFreezeStore) ListInEffect(_ context.Context, environment string, at time.Time) ([]*storage.EnvironmentFreeze, error) {
	return m.filter(environment, func(f *storage.EnvironmentFreeze) bool { return f.InEffectAt(at) }), nil
}

func (m *memoryFreezeStore) ListCurrent(_ context.Context, environment string, at time.Time) ([]*storage.EnvironmentFreeze, error) {
	return m.filter(environment, func(f *storage.EnvironmentFreeze) bool {
		return f.LiftedAt == nil && (f.EndsAt == nil || at.Before(*f.EndsAt))
	}), nil
}

func (m *memoryFreezeStore) History(_ context.Context, environment string, limit int) ([]*storage.EnvironmentFreeze, error) {
	all := m.filter(environment, func(*storage.EnvironmentFreeze) bool { return true })
	slices.Reverse(all)
	return all[:min(limit, len(all))], nil
}

func (m *memoryFreezeStore) filter(environment string, keep func(*storage.EnvironmentFreeze) bool) []*storage.EnvironmentFreeze {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*storage.EnvironmentFreeze
	for _, freeze := range m.freezes {
		if (environment == "" || freeze.Environment == environment) && keep(freeze) {
			copied := *freeze
			out = append(out, &copied)
		}
	}
	return out
}

type mockStorageWithFreezes struct {
	mockStorage
	freezes storage.FreezeStore
}

func (m *mockStorageWithFreezes) Freezes() storage.FreezeStore { return m.freezes }

var freezeTestNow = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

func newFreezeTestService(t *testing.T, config *ServerConfig) (*Service, *http.ServeMux) {
	t.Helper()
	service := New(&mockStorageWithFreezes{freezes: &memoryFreezeStore{}}, config, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, service.SetClock(clock.NewFake(freezeTestNow)))
	mux := http.NewServeMux()
	service.ConfigureRoutes(mux)
	return service, mux
}

// freezeConfig is scopedWriteConfig with a freeze group: incident-commanders
// may set and lift freezes, nothing else.
func freezeConfig() *ServerConfig {
	config := scopedWriteConfig()
	config.Freeze.Groups = []string{"incident-commanders"}
	return config
}

func doFreezeRequest(t *testing.T, mux *http.ServeMux, user *auth.User, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	ctx := t.Context()
	if user != nil {
		ctx = auth.WithVerifiedUser(ctx, user)
	}
	req := httptest.NewRequestWithContext(ctx, method, path, strings.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func decodeFreeze(t *testing.T, w *httptest.ResponseRecorder) apitypes.Freeze {
	t.Helper()
	var freeze apitypes.Freeze
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &freeze), w.Body.String())
	return freeze
}

var (
	freezeAdmin    = &auth.User{Subject: "alice", Groups: []string{"schema-admins"}}
	freezeMember   = &auth.User{Subject: "ida", Groups: []string{"incident-commanders"}}
	freezeOperator = &auth.User{Subject: "olga", Groups: []string{"payments-team"}}
)

// An on-demand freeze set with only an environment and a reason starts now,
// has no end, and only freeze admins may override it. A planned freeze lets
// anyone override by default.
func TestFreezeCreateDefaults(t *testing.T) {
	_, mux := newFreezeTestService(t, freezeConfig())

	w := doFreezeRequest(t, mux, freezeAdmin, http.MethodPost, "/api/freezes", `{"environment":"Staging","reason":"incident"}`)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	onDemand := decodeFreeze(t, w)
	assert.True(t, strings.HasPrefix(onDemand.ID, "freeze-"))
	assert.Equal(t, "staging", onDemand.Environment)
	assert.Equal(t, storage.FreezeKindOnDemand, onDemand.Kind)
	assert.Equal(t, storage.FreezeOverrideAdmins, onDemand.Override)
	assert.True(t, onDemand.StartsAt.Equal(freezeTestNow))
	assert.Nil(t, onDemand.EndsAt)
	assert.Equal(t, "alice", onDemand.CreatedBy)
	assert.Equal(t, storage.FreezeSourceManual, onDemand.Source)
	assert.True(t, onDemand.InEffect)

	w = doFreezeRequest(t, mux, freezeAdmin, http.MethodPost, "/api/freezes",
		`{"environment":"production","kind":"planned","starts_at":"2026-11-25T08:00:00Z","ends_at":"2026-11-30T08:00:00Z","reason":"holiday"}`)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	planned := decodeFreeze(t, w)
	assert.Equal(t, storage.FreezeOverrideAnyone, planned.Override)
	assert.False(t, planned.InEffect, "a planned freeze takes effect at its start")
}

// A freeze request that names something SchemaBot does not know, or that
// could never take effect, is refused with the reason and stores nothing.
func TestFreezeCreateRejectsInvalidRequests(t *testing.T) {
	_, mux := newFreezeTestService(t, freezeConfig())

	tests := map[string]struct{ body, want string }{
		"unknown environment":  {`{"environment":"qa","reason":"x"}`, `environment "qa" is not served by this instance`},
		"unknown database":     {`{"environment":"staging","databases":["ledger"],"reason":"x"}`, `database "ledger" is not configured`},
		"database without env": {`{"environment":"production","databases":["orders"],"reason":"x"}`, `database "orders" has no environment "production"`},
		"missing reason":       {`{"environment":"staging"}`, "reason is required"},
		"planned without end":  {`{"environment":"staging","kind":"planned","reason":"x"}`, "planned freeze needs an end time"},
		"bad override":         {`{"environment":"staging","override":"everyone","reason":"x"}`, "freeze override"},
		"end in the past":      {`{"environment":"staging","ends_at":"2026-11-19T00:00:00Z","reason":"x"}`, "end time must be in the future"},
		"unknown field":        {`{"environment":"staging","reason":"x","scope":"all"}`, "unknown field"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			w := doFreezeRequest(t, mux, freezeAdmin, http.MethodPost, "/api/freezes", tt.body)
			require.Equal(t, http.StatusBadRequest, w.Code)
			var response apitypes.ErrorResponse
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
			assert.Contains(t, response.Error, tt.want)
		})
	}

	w := doFreezeRequest(t, mux, freezeAdmin, http.MethodGet, "/api/freezes", "")
	require.Equal(t, http.StatusOK, w.Code)
	var list apitypes.FreezeListResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &list))
	assert.Empty(t, list.Recent)
}

// A freeze-group member may set and lift freezes. A database operator, whose
// grant covers one database, may not: a freeze reaches past it.
func TestFreezeWriteAuthorization(t *testing.T) {
	_, mux := newFreezeTestService(t, freezeConfig())

	w := doFreezeRequest(t, mux, freezeMember, http.MethodPost, "/api/freezes", `{"environment":"staging","reason":"incident"}`)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	freeze := decodeFreeze(t, w)
	assert.Equal(t, "ida", freeze.CreatedBy)

	w = doFreezeRequest(t, mux, freezeOperator, http.MethodPost, "/api/freezes", `{"environment":"staging","reason":"incident"}`)
	require.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "schema-admins, incident-commanders")

	w = doFreezeRequest(t, mux, freezeOperator, http.MethodPost, "/api/freezes/"+freeze.ID+"/lift", `{}`)
	require.Equal(t, http.StatusForbidden, w.Code)

	w = doFreezeRequest(t, mux, freezeMember, http.MethodPost, "/api/freezes/"+freeze.ID+"/lift", `{"reason":"resolved"}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	lifted := decodeFreeze(t, w)
	assert.Equal(t, "ida", lifted.LiftedBy)
	assert.Equal(t, "resolved", lifted.LiftReason)
	assert.False(t, lifted.InEffect)
}

// A freeze group grants freezes and nothing else: its members still need a
// write group for every other admin write.
func TestFreezeGroupGrantsNoOtherWrite(t *testing.T) {
	_, mux := newFreezeTestService(t, freezeConfig())

	w := doFreezeRequest(t, mux, freezeMember, http.MethodPost, "/api/settings", `{"key":"pause_applies","value":"true"}`)
	require.Equal(t, http.StatusForbidden, w.Code)
}

// Lifting a freeze twice, or one that already ended, is a conflict, and a
// freeze that does not exist is not found.
func TestFreezeLiftConflicts(t *testing.T) {
	service, mux := newFreezeTestService(t, freezeConfig())
	end := freezeTestNow.Add(time.Hour)
	ended, err := service.storage.Freezes().Create(t.Context(), &storage.EnvironmentFreeze{
		Identifier: "freeze-ended", Environment: "staging", Kind: storage.FreezeKindPlanned,
		Override: storage.FreezeOverrideAnyone, StartsAt: freezeTestNow.Add(-time.Hour), EndsAt: &end,
		Reason: "window", CreatedBy: "alice", Source: storage.FreezeSourceManual,
	})
	require.NoError(t, err)

	w := doFreezeRequest(t, mux, freezeAdmin, http.MethodPost, "/api/freezes", `{"environment":"staging","reason":"incident"}`)
	require.Equal(t, http.StatusCreated, w.Code)
	freeze := decodeFreeze(t, w)
	require.Equal(t, http.StatusOK, doFreezeRequest(t, mux, freezeAdmin, http.MethodPost, "/api/freezes/"+freeze.ID+"/lift", `{}`).Code)

	w = doFreezeRequest(t, mux, freezeAdmin, http.MethodPost, "/api/freezes/"+freeze.ID+"/lift", `{}`)
	assert.Equal(t, http.StatusConflict, w.Code)
	assert.Contains(t, w.Body.String(), "already lifted")

	service.clock.(*clock.Fake).Advance(2 * time.Hour)
	w = doFreezeRequest(t, mux, freezeAdmin, http.MethodPost, "/api/freezes/"+ended.Identifier+"/lift", `{}`)
	assert.Equal(t, http.StatusConflict, w.Code)
	assert.Contains(t, w.Body.String(), "already ended")

	assert.Equal(t, http.StatusNotFound, doFreezeRequest(t, mux, freezeAdmin, http.MethodPost, "/api/freezes/freeze-missing/lift", `{}`).Code)
	assert.Equal(t, http.StatusNotFound, doFreezeRequest(t, mux, freezeAdmin, http.MethodGet, "/api/freezes/freeze-missing", "").Code)
}

// The list splits current freezes into in effect and scheduled, and the
// recent list holds every freeze of the environment, newest first.
func TestFreezeList(t *testing.T) {
	_, mux := newFreezeTestService(t, freezeConfig())

	w := doFreezeRequest(t, mux, freezeAdmin, http.MethodPost, "/api/freezes", `{"environment":"staging","reason":"incident"}`)
	require.Equal(t, http.StatusCreated, w.Code)
	active := decodeFreeze(t, w)
	w = doFreezeRequest(t, mux, freezeAdmin, http.MethodPost, "/api/freezes",
		`{"environment":"staging","kind":"planned","starts_at":"2026-11-25T08:00:00Z","ends_at":"2026-11-30T08:00:00Z","reason":"holiday"}`)
	require.Equal(t, http.StatusCreated, w.Code)
	scheduled := decodeFreeze(t, w)
	w = doFreezeRequest(t, mux, freezeAdmin, http.MethodPost, "/api/freezes", `{"environment":"production","reason":"other"}`)
	require.Equal(t, http.StatusCreated, w.Code)

	w = doFreezeRequest(t, mux, freezeOperator, http.MethodGet, "/api/freezes?environment=Staging", "")
	require.Equal(t, http.StatusOK, w.Code, "reading freezes needs no freeze grant")
	var list apitypes.FreezeListResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &list))
	require.Len(t, list.InEffect, 1)
	assert.Equal(t, active.ID, list.InEffect[0].ID)
	require.Len(t, list.Scheduled, 1)
	assert.Equal(t, scheduled.ID, list.Scheduled[0].ID)
	require.Len(t, list.Recent, 2)

	w = doFreezeRequest(t, mux, freezeAdmin, http.MethodGet, "/api/freezes?recent=0", "")
	assert.Equal(t, http.StatusBadRequest, w.Code)

	w = doFreezeRequest(t, mux, freezeAdmin, http.MethodGet, "/api/freezes/"+active.ID, "")
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, active.ID, decodeFreeze(t, w).ID)
}

func TestServerConfig_ValidateFreeze(t *testing.T) {
	require.NoError(t, freezeConfig().Validate())

	config := freezeConfig()
	config.Freeze.Groups = []string{"incident-commanders", "incident-commanders"}
	require.ErrorContains(t, config.Validate(), "freeze.groups")

	config = testServerConfig()
	config.Freeze.Groups = []string{"incident-commanders"}
	require.ErrorContains(t, config.Validate(), "freeze.groups requires auth type forward_auth")
}

// A staging-only instance names production in its promotion order but does
// not serve it, so it refuses a production freeze instead of recording it in
// its own store, where the production instance would never see it.
func TestFreezeCreateRefusesPeerEnvironment(t *testing.T) {
	config := freezeConfig()
	config.AllowedEnvironments = []string{"staging"}
	config.EnvironmentOrder = []string{"staging", "production"}
	_, mux := newFreezeTestService(t, config)

	w := doFreezeRequest(t, mux, freezeAdmin, http.MethodPost, "/api/freezes", `{"environment":"production","reason":"incident"}`)
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	assert.Contains(t, w.Body.String(), `environment \"production\" is not served by this instance`)
}

// A direct-to-pod request admitted with caller-supplied identity headers may
// still freeze, but the freeze never names the asserted subject (AZ-3).
func TestFreezeCreateRecordsUnverifiedCaller(t *testing.T) {
	_, mux := newFreezeTestService(t, freezeConfig())

	ctx := auth.WithUser(t.Context(), &auth.User{Subject: "mallory", Groups: []string{"schema-admins"}})
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/freezes", strings.NewReader(`{"environment":"staging","reason":"incident"}`))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	assert.Equal(t, unverifiedFreezeCaller, decodeFreeze(t, w).CreatedBy)
}
