package api

import (
	"encoding/json"
	"errors"
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
	"github.com/block/schemabot/pkg/storage"
)

func getHistoryPage(t *testing.T, mux *http.ServeMux, database string, query url.Values) (int, apitypes.DatabaseHistoryResponse, apitypes.ErrorResponse) {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/history/"+database+"?"+query.Encode(), nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	var resp apitypes.DatabaseHistoryResponse
	var errResp apitypes.ErrorResponse
	if w.Code == http.StatusOK {
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	} else {
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errResp))
	}
	return w.Code, resp, errResp
}

// A history page asks storage for one page of the database's applies plus one,
// so a truncated page reports has_more. The next page names the last apply it
// saw in starting_after, storage continues below that apply, and the response
// echoes it so the caller can tell it from a first page.
func TestHandleDatabaseHistoryPagesAfterTheNamedApply(t *testing.T) {
	applies := &recentApplyStore{applies: pagingTestApplies(time.Now().UTC())}
	mux := newStatusPagingServer(t, applies, nil)

	code, first, errResp := getHistoryPage(t, mux, "orders", url.Values{"environment": {"staging"}, "limit": {"2"}})
	require.Equal(t, http.StatusOK, code, errResp.Error)
	assert.True(t, first.HasMore)
	assert.Equal(t, 2, first.Limit)
	assert.Equal(t, maxStatusLimit, first.MaxLimit)
	assert.Empty(t, first.StartingAfter, "a first page continues from nothing")
	require.Len(t, first.Applies, 2)
	assert.Equal(t, "apply-30", first.Applies[0].ApplyID)
	assert.Equal(t, "apply-20", first.Applies[1].ApplyID)
	require.Len(t, applies.filters, 1)
	assert.Equal(t, 3, applies.filters[0].Limit, "one more than the page, to tell whether more follow")
	assert.Equal(t, "orders", applies.filters[0].Database)
	assert.Equal(t, "staging", applies.filters[0].Environment)
	assert.Zero(t, applies.filters[0].AfterID, "the first page starts at the top")

	code, second, errResp := getHistoryPage(t, mux, "orders", url.Values{"environment": {"staging"}, "limit": {"2"}, "starting_after": {"apply-20"}})
	require.Equal(t, http.StatusOK, code, errResp.Error)
	require.Len(t, applies.filters, 2)
	assert.Equal(t, int64(20), applies.filters[1].AfterID)
	assert.Equal(t, "orders", applies.filters[1].Database)
	assert.Equal(t, "apply-20", second.StartingAfter, "the page names the apply it continues from")
}

// A history request that names no limit gets a full page at the cap, so a
// client that never pages still sees the whole history of any database that
// fits in one page.
func TestHandleDatabaseHistoryDefaultsToAFullPage(t *testing.T) {
	applies := &recentApplyStore{applies: pagingTestApplies(time.Now().UTC())}
	mux := newStatusPagingServer(t, applies, nil)

	code, resp, errResp := getHistoryPage(t, mux, "orders", url.Values{})
	require.Equal(t, http.StatusOK, code, errResp.Error)
	assert.False(t, resp.HasMore)
	assert.Equal(t, maxStatusLimit, resp.Limit)
	assert.Len(t, resp.Applies, 3)
	require.Len(t, applies.filters, 1)
	assert.Equal(t, maxStatusLimit+1, applies.filters[0].Limit)
}

// starting_after must name an apply from the same history. One that does not
// exist, or that belongs to another database or environment, is refused with a
// message naming the mismatch before any page is read; so is a limit that is
// not a positive number.
func TestHandleDatabaseHistoryRefusesAPageFromAnotherListing(t *testing.T) {
	applies := &recentApplyStore{applies: pagingTestApplies(time.Now().UTC())}
	mux := newStatusPagingServer(t, applies, nil)

	for name, tc := range map[string]struct {
		database string
		query    url.Values
		want     string
	}{
		"unknown apply": {
			database: "orders",
			query:    url.Values{"starting_after": {"apply-missing"}},
			want:     "starting_after apply apply-missing does not exist; pass the last apply ID from the previous page",
		},
		"another environment": {
			database: "orders",
			query:    url.Values{"environment": {"production"}, "starting_after": {"apply-20"}},
			want:     "starting_after apply apply-20 is in environment staging, not production; repeat the previous page's filters",
		},
		"another database": {
			database: "billing",
			query:    url.Values{"starting_after": {"apply-20"}},
			want:     "starting_after apply apply-20 is for database orders, not billing; repeat the previous page's filters",
		},
		"limit not a number": {
			database: "orders",
			query:    url.Values{"limit": {"lots"}},
			want:     "limit must be a positive integer",
		},
	} {
		t.Run(name, func(t *testing.T) {
			before := len(applies.filters)
			code, _, errResp := getHistoryPage(t, mux, tc.database, tc.query)
			assert.Equal(t, http.StatusBadRequest, code)
			assert.Equal(t, tc.want, errResp.Error)
			assert.Len(t, applies.filters, before, "a refused page never reads the history")
		})
	}
}

// A storage failure answers 500 with a fixed message; the storage error goes
// to the server log, never into the response.
func TestHandleDatabaseHistoryKeepsTheStorageErrorOutOfTheResponse(t *testing.T) {
	applies := &recentApplyStore{err: errors.New("dial tcp 10.0.0.7:3306: connection refused")}
	mux := newStatusPagingServer(t, applies, nil)

	code, _, errResp := getHistoryPage(t, mux, "orders", url.Values{})
	assert.Equal(t, http.StatusInternalServerError, code)
	assert.Equal(t, "failed to get apply history", errResp.Error)
}

// newHistoryServer serves the history from a store that filters on database,
// environment and starting_after, so a test sees what a page actually holds.
func newHistoryServer(t *testing.T, applies storage.ApplyStore) *http.ServeMux {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{applies: applies}, testServerConfig(), nil, logger)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	return mux
}

// A database with exactly a page's worth of applies fits in one page, so the
// page reports no more and a client never asks for an empty one.
func TestHistoryExactFitPageHasNoMore(t *testing.T) {
	mux := newHistoryServer(t, &staticApplyStore{applies: pagingTestApplies(time.Now().UTC())})

	code, resp, errResp := getHistoryPage(t, mux, "orders", url.Values{"environment": {"staging"}, "limit": {"3"}})
	require.Equal(t, http.StatusOK, code, errResp.Error)
	assert.Len(t, resp.Applies, 3)
	assert.False(t, resp.HasMore, "three applies in a page of three leave nothing more")
}

// A page after the oldest apply is an empty list, not null, reports no more,
// and echoes the apply it continued from.
func TestHistoryPageAfterTheOldestIsEmpty(t *testing.T) {
	mux := newHistoryServer(t, &staticApplyStore{applies: pagingTestApplies(time.Now().UTC())})

	code, resp, errResp := getHistoryPage(t, mux, "orders", url.Values{"environment": {"staging"}, "starting_after": {"apply-10"}})
	require.Equal(t, http.StatusOK, code, errResp.Error)
	assert.NotNil(t, resp.Applies)
	assert.Empty(t, resp.Applies)
	assert.False(t, resp.HasMore)
	assert.Equal(t, "apply-10", resp.StartingAfter)
}

// An apply created between two page requests sits above every page already
// read, so the next page holds the same applies it would have without it.
func TestHistoryInsertBetweenPagesLeavesTheNextPageAlone(t *testing.T) {
	store := &staticApplyStore{applies: pagingTestApplies(time.Now().UTC())}
	mux := newHistoryServer(t, store)

	code, first, errResp := getHistoryPage(t, mux, "orders", url.Values{"environment": {"staging"}, "limit": {"2"}})
	require.Equal(t, http.StatusOK, code, errResp.Error)
	require.Len(t, first.Applies, 2)

	newer := *store.applies[0]
	newer.ID, newer.ApplyIdentifier = 40, "apply-40"
	store.applies = append([]*storage.Apply{&newer}, store.applies...)

	code, second, errResp := getHistoryPage(t, mux, "orders", url.Values{"environment": {"staging"}, "limit": {"2"}, "starting_after": {"apply-20"}})
	require.Equal(t, http.StatusOK, code, errResp.Error)
	require.Len(t, second.Applies, 1)
	assert.Equal(t, "apply-10", second.Applies[0].ApplyID)
	assert.False(t, second.HasMore)
}
