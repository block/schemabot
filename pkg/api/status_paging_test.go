package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func newStatusPagingServer(t *testing.T, applies *recentApplyStore, operations storage.ApplyOperationStore) *http.ServeMux {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{applies: applies, operations: operations}, testServerConfig(), nil, logger)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	return mux
}

func getStatusPage(t *testing.T, mux *http.ServeMux, query url.Values) (int, apitypes.StatusResponse, apitypes.ErrorResponse) {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status?"+query.Encode(), nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	var resp apitypes.StatusResponse
	var errResp apitypes.ErrorResponse
	if w.Code == http.StatusOK {
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	} else {
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errResp))
	}
	return w.Code, resp, errResp
}

func pagingTestApplies(now time.Time) []*storage.Apply {
	applies := make([]*storage.Apply, 0, 3)
	for _, id := range []int64{30, 20, 10} {
		applies = append(applies, &storage.Apply{
			ID:              id,
			ApplyIdentifier: map[int64]string{30: "apply-30", 20: "apply-20", 10: "apply-10"}[id],
			Database:        "orders",
			Environment:     "staging",
			Deployment:      "primary",
			Engine:          storage.EngineSpirit,
			State:           state.Apply.Completed,
			CreatedAt:       now,
			UpdatedAt:       now,
		})
	}
	return applies
}

// A page requested with starting_after asks storage for the applies after the
// named one, under the same filters, and echoes starting_after so the caller
// can tell it from a first page. A truncated page reports has_more so the
// caller knows to ask again after its last apply.
func TestHandleStatusStartingAfterContinuesAfterTheNamedApply(t *testing.T) {
	applies := &recentApplyStore{applies: pagingTestApplies(time.Now().UTC())}
	mux := newStatusPagingServer(t, applies, nil)

	code, first, errResp := getStatusPage(t, mux, url.Values{"environment": {"staging"}, "database": {"orders"}, "last": {"24h"}, "limit": {"2"}})
	require.Equal(t, http.StatusOK, code, errResp.Error)
	assert.True(t, first.HasMore)
	require.Len(t, first.Applies, 2)
	assert.Equal(t, "apply-20", first.Applies[1].ApplyID)
	require.Len(t, applies.filters, 1)
	assert.Zero(t, applies.filters[0].AfterID, "the first page starts at the top")
	assert.Empty(t, first.StartingAfter, "a first page continues from nothing")

	code, second, errResp := getStatusPage(t, mux, url.Values{"environment": {"staging"}, "database": {"orders"}, "last": {"24h"}, "limit": {"2"}, "starting_after": {"apply-20"}})
	require.Equal(t, http.StatusOK, code, errResp.Error)
	require.Len(t, applies.filters, 2)
	next := applies.filters[1]
	assert.Equal(t, int64(20), next.AfterID)
	assert.Equal(t, "staging", next.Environment)
	assert.Equal(t, "orders", next.Database)
	assert.False(t, next.UpdatedSince.IsZero(), "the window still applies")
	assert.Equal(t, "apply-20", second.StartingAfter, "the page names the apply it continues from")
}

// starting_after must name an apply from the same listing. One that does not
// exist, or that belongs to another environment, database, or deployment, is
// refused with a message naming the mismatch, before any page is read.
func TestHandleStatusStartingAfterRefusesAnApplyFromAnotherListing(t *testing.T) {
	applies := &recentApplyStore{applies: pagingTestApplies(time.Now().UTC())}
	operations := &staticApplyOperationStore{operations: []*storage.ApplyOperation{{ApplyID: 20, Deployment: "secondary"}}}
	mux := newStatusPagingServer(t, applies, operations)

	for name, tc := range map[string]struct {
		query url.Values
		want  string
	}{
		"unknown apply": {
			query: url.Values{"starting_after": {"apply-missing"}},
			want:  "starting_after apply apply-missing does not exist; pass the last apply ID from the previous page",
		},
		"another environment": {
			query: url.Values{"environment": {"production"}, "starting_after": {"apply-20"}},
			want:  "starting_after apply apply-20 is in environment staging, not production; repeat the previous page's filters",
		},
		"another database": {
			query: url.Values{"database": {"billing"}, "starting_after": {"apply-20"}},
			want:  "starting_after apply apply-20 is for database orders, not billing; repeat the previous page's filters",
		},
		"another deployment": {
			query: url.Values{"deployment": {"tertiary"}, "starting_after": {"apply-20"}},
			want:  "starting_after apply apply-20 does not touch deployment tertiary; repeat the previous page's filters",
		},
	} {
		t.Run(name, func(t *testing.T) {
			before := len(applies.filters)
			code, _, errResp := getStatusPage(t, mux, tc.query)
			assert.Equal(t, http.StatusBadRequest, code)
			assert.Equal(t, tc.want, errResp.Error)
			assert.Len(t, applies.filters, before, "a refused page never reads the list")
		})
	}

	// The anchor's own deployment and any of its operations' both count.
	for _, deployment := range []string{"primary", "secondary"} {
		code, _, errResp := getStatusPage(t, mux, url.Values{"deployment": {deployment}, "starting_after": {"apply-20"}})
		assert.Equal(t, http.StatusOK, code, "deployment %s: %s", deployment, errResp.Error)
	}
}

// The state of the starting_after apply is not checked against the state
// filters: it may have changed since the page that showed it.
func TestHandleStatusStartingAfterAllowsAnAnchorWhoseStateMoved(t *testing.T) {
	applies := &recentApplyStore{applies: pagingTestApplies(time.Now().UTC())}
	mux := newStatusPagingServer(t, applies, nil)

	code, _, errResp := getStatusPage(t, mux, url.Values{"active": {"true"}, "starting_after": {"apply-20"}})
	require.Equal(t, http.StatusOK, code, errResp.Error)
	require.Len(t, applies.filters, 1)
	assert.Equal(t, int64(20), applies.filters[0].AfterID)
}

// active=false lists only terminal applies, the complement of active=true, so
// a caller lists the two halves with one request each. The response echoes the
// filter, which tells a caller the server applied it, and an absent parameter
// filters nothing and echoes nothing.
func TestHandleStatusActiveFilterEcho(t *testing.T) {
	for _, tc := range []struct {
		raw          string
		activeOnly   bool
		finishedOnly bool
	}{
		{raw: "true", activeOnly: true},
		{raw: "false", finishedOnly: true},
		{raw: ""},
	} {
		applies := &recentApplyStore{applies: pagingTestApplies(time.Now().UTC())}
		mux := newStatusPagingServer(t, applies, nil)
		query := url.Values{}
		if tc.raw != "" {
			query.Set("active", tc.raw)
		}

		code, resp, errResp := getStatusPage(t, mux, query)
		require.Equal(t, http.StatusOK, code, errResp.Error)
		require.Len(t, applies.filters, 1)
		assert.Equal(t, tc.activeOnly, applies.filters[0].ActiveOnly, "active=%q", tc.raw)
		assert.Equal(t, tc.finishedOnly, applies.filters[0].FinishedOnly, "active=%q", tc.raw)
		if tc.raw == "" {
			assert.Nil(t, resp.Active, "no filter is echoed when none was asked for")
			continue
		}
		require.NotNil(t, resp.Active, "active=%q is echoed", tc.raw)
		assert.Equal(t, tc.activeOnly, *resp.Active)
	}
}
