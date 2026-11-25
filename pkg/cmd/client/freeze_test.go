package client

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

// freezeRequest is what a freeze test server saw.
type freezeRequest struct {
	method, path, query string
	body                map[string]any
}

// freezeServer answers every request with status and body and records the
// last one.
func freezeServer(t *testing.T, status int, body any) (string, *freezeRequest) {
	t.Helper()
	seen := &freezeRequest{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.method, seen.path, seen.query = r.Method, r.URL.Path, r.URL.RawQuery
		raw, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		seen.body = nil
		if len(raw) > 0 {
			assert.NoError(t, json.Unmarshal(raw, &seen.body))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		assert.NoError(t, json.NewEncoder(w).Encode(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL, seen
}

func TestCreateFreezeSendsTheRequest(t *testing.T) {
	end := time.Date(2026, 12, 1, 9, 0, 0, 0, time.UTC)
	url, seen := freezeServer(t, http.StatusCreated, apitypes.Freeze{ID: "freeze-1", Environment: "production"})

	got, err := CreateFreeze(url, apitypes.CreateFreezeRequest{
		Environment: "production", Reason: "SEV-1", Running: "stop",
		Deployments: []string{"payments"}, Databases: []string{"orders"}, Kind: "planned", EndsAt: &end,
	})

	require.NoError(t, err)
	assert.Equal(t, "freeze-1", got.ID)
	assert.Equal(t, http.MethodPost, seen.method)
	assert.Equal(t, "/api/freezes", seen.path)
	assert.Equal(t, "SEV-1", seen.body["reason"])
	assert.Equal(t, "production", seen.body["environment"])
	assert.Equal(t, "stop", seen.body["running"])
	assert.Equal(t, "planned", seen.body["kind"])
	assert.Equal(t, []any{"payments"}, seen.body["deployments"])
	assert.Equal(t, []any{"orders"}, seen.body["databases"])
	assert.Equal(t, "2026-12-01T09:00:00Z", seen.body["ends_at"])
}

func TestListFreezesNarrowsByEnvironment(t *testing.T) {
	url, seen := freezeServer(t, http.StatusOK, apitypes.FreezeListResponse{Recent: []apitypes.Freeze{{ID: "freeze-1"}}})

	got, err := ListFreezes(url, "production")
	require.NoError(t, err)
	require.Len(t, got.Recent, 1)
	assert.Equal(t, http.MethodGet, seen.method)
	assert.Equal(t, "/api/freezes", seen.path)
	assert.Equal(t, "environment=production", seen.query)

	_, err = ListFreezes(url, "")
	require.NoError(t, err)
	assert.Empty(t, seen.query)
}

func TestGetFreezeReadsOneFreeze(t *testing.T) {
	url, seen := freezeServer(t, http.StatusOK, apitypes.Freeze{ID: "freeze-1"})

	got, err := GetFreeze(url, "freeze-1")
	require.NoError(t, err)
	assert.Equal(t, "freeze-1", got.ID)
	assert.Equal(t, http.MethodGet, seen.method)
	assert.Equal(t, "/api/freezes/freeze-1", seen.path)
}

func TestLiftFreezePostsTheReason(t *testing.T) {
	url, seen := freezeServer(t, http.StatusOK, apitypes.Freeze{ID: "freeze-1"})

	_, err := LiftFreeze(url, "freeze-1", "SEV resolved")
	require.NoError(t, err)
	assert.Equal(t, http.MethodPost, seen.method)
	assert.Equal(t, "/api/freezes/freeze-1/lift", seen.path)
	assert.Equal(t, "SEV resolved", seen.body["reason"])
}

func TestGetFreezeOverrideReadsOneRequest(t *testing.T) {
	url, seen := freezeServer(t, http.StatusOK, apitypes.FreezeOverrideRequest{ID: "ovr-1"})

	got, err := GetFreezeOverride(url, "ovr-1")
	require.NoError(t, err)
	assert.Equal(t, "ovr-1", got.ID)
	assert.Equal(t, http.MethodGet, seen.method)
	assert.Equal(t, "/api/freezes/overrides/ovr-1", seen.path)
}

func TestApproveFreezeOverridePostsToTheApproveRoute(t *testing.T) {
	url, seen := freezeServer(t, http.StatusOK, apitypes.FreezeOverrideRequest{ID: "ovr-1", ApprovedBy: "erin"})

	got, err := ApproveFreezeOverride(url, "ovr-1")
	require.NoError(t, err)
	assert.Equal(t, "erin", got.ApprovedBy)
	assert.Equal(t, http.MethodPost, seen.method)
	assert.Equal(t, "/api/freezes/overrides/ovr-1/approve", seen.path)
}

// The server's message reaches the caller whole for each status the freeze
// routes use to refuse.
func TestFreezeCallsDecodeServerErrors(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotFound, http.StatusConflict} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			url, _ := freezeServer(t, status, apitypes.ErrorResponse{Error: "server says no"})

			calls := map[string]func() error{
				"create":   func() error { _, err := CreateFreeze(url, apitypes.CreateFreezeRequest{}); return err },
				"list":     func() error { _, err := ListFreezes(url, ""); return err },
				"get":      func() error { _, err := GetFreeze(url, "freeze-1"); return err },
				"lift":     func() error { _, err := LiftFreeze(url, "freeze-1", ""); return err },
				"override": func() error { _, err := GetFreezeOverride(url, "ovr-1"); return err },
				"approve":  func() error { _, err := ApproveFreezeOverride(url, "ovr-1"); return err },
			}
			for name, call := range calls {
				var apiErr *APIError
				require.ErrorAs(t, call(), &apiErr, name)
				assert.Equal(t, status, apiErr.Status, name)
				assert.Equal(t, "server says no", apiErr.Message, name)
			}
		})
	}
}
