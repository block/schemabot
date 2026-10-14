package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
)

// pagedServer serves a fixed sequence of JSON pages keyed by the request's
// starting_after, echoing it back as a paging server does, and records each
// request's query so tests can check that every page repeats the listing's
// filters. A legacy server ignores starting_after: it serves the first page
// for every request and echoes nothing.
type pagedServer struct {
	mu      sync.Mutex
	pages   map[string]any
	legacy  bool
	queries []map[string]string
}

func newPagedServer(t *testing.T, path string, pages map[string]any) (*pagedServer, *httptest.Server) {
	t.Helper()
	t.Setenv("SCHEMABOT_TOKEN", "fictional-paging-test")
	ps := &pagedServer{pages: pages}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ps.mu.Lock()
		defer ps.mu.Unlock()
		if r.URL.Path != path {
			http.Error(w, "unexpected path "+r.URL.Path, http.StatusNotFound)
			return
		}
		query := map[string]string{}
		for key := range r.URL.Query() {
			query[key] = r.URL.Query().Get(key)
		}
		ps.queries = append(ps.queries, query)
		key := query["starting_after"]
		if ps.legacy {
			key = ""
		}
		page, ok := ps.pages[key]
		if !ok {
			http.Error(w, "unknown starting_after "+key, http.StatusBadRequest)
			return
		}
		body, err := json.Marshal(page)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var fields map[string]any
		if err := json.Unmarshal(body, &fields); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if key != "" {
			fields["starting_after"] = key
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(fields); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}))
	t.Cleanup(server.Close)
	return ps, server
}

// setLegacy makes the server ignore starting_after from the next request on.
func (ps *pagedServer) setLegacy() {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.legacy = true
}

// statusPage is a status response listing the given applies, completed unless
// named in running.
func statusPage(hasMore bool, running []string, identifiers ...string) apitypes.StatusResponse {
	page := apitypes.StatusResponse{
		Limit:       len(identifiers),
		MaxLimit:    1000,
		HasMore:     hasMore,
		StateCounts: map[string]int{state.Apply.Completed: 5, state.Apply.Running: 1},
	}
	for _, identifier := range identifiers {
		applyState := state.Apply.Completed
		if slices.Contains(running, identifier) {
			applyState = state.Apply.Running
			page.ActiveCount++
		}
		page.Applies = append(page.Applies, &apitypes.ActiveApplyResponse{
			ApplyID: identifier, Database: "orders", Environment: "staging", State: applyState, Engine: "spirit", Caller: "cli",
		})
	}
	return page
}

// status --all --json requests each page after the last apply of the one
// before and prints one response whose applies array holds every page, so one
// command answers questions about the whole history. Every page request
// repeats the listing's filters, and an apply a later page repeats is listed
// once.
func TestStatusCmdAllMergesEveryPage(t *testing.T) {
	ps, server := newPagedServer(t, "/api/status", map[string]any{
		"":        statusPage(true, []string{"apply-6"}, "apply-6", "apply-5"),
		"apply-5": statusPage(true, []string{"apply-4"}, "apply-4", "apply-3"),
		"apply-3": statusPage(false, nil, "apply-3", "apply-2", "apply-1"),
	})

	cmd := &StatusCmd{Environment: "staging", Last: "2d", All: true, JSON: true}
	var runErr error
	output := captureOutput(t, func() { runErr = cmd.Run(&Globals{Endpoint: server.URL}) })
	require.NoError(t, runErr)

	var merged apitypes.StatusResponse
	require.NoError(t, json.Unmarshal([]byte(output), &merged))
	identifiers := make([]string, 0, len(merged.Applies))
	for _, apply := range merged.Applies {
		identifiers = append(identifiers, apply.ApplyID)
	}
	assert.Equal(t, []string{"apply-6", "apply-5", "apply-4", "apply-3", "apply-2", "apply-1"}, identifiers)
	assert.False(t, merged.HasMore)
	assert.Equal(t, 2, merged.ActiveCount, "every page's running applies are counted")
	assert.Equal(t, map[string]int{state.Apply.Completed: 5, state.Apply.Running: 1}, merged.StateCounts)

	require.Len(t, ps.queries, 3)
	for i, query := range ps.queries {
		assert.Equal(t, "staging", query["environment"], "page %d repeats the environment", i)
		assert.Equal(t, "48h0m0s", query["last"], "page %d repeats the window", i)
		assert.Equal(t, "1000", query["limit"], "page %d asks for full pages", i)
	}
	assert.Empty(t, ps.queries[0]["starting_after"])
	assert.Equal(t, "apply-5", ps.queries[1]["starting_after"])
	assert.Equal(t, "apply-3", ps.queries[2]["starting_after"])
}

// A server that predates paging ignores starting_after and serves its first
// page again, which may hold applies created since the previous request
// rather than the anchor itself. It never echoes starting_after, so --all
// fails rather than presenting the wrong pages as the whole history.
func TestStatusCmdAllRefusesAServerThatIgnoresPaging(t *testing.T) {
	ps, server := newPagedServer(t, "/api/status", map[string]any{"": statusPage(true, nil, "apply-3", "apply-2")})
	ps.setLegacy()

	cmd := &StatusCmd{All: true, JSON: true}
	var runErr error
	captureOutput(t, func() { runErr = cmd.Run(&Globals{Endpoint: server.URL}) })
	require.EqualError(t, runErr, "the server did not continue after apply apply-2; it predates paging, so --all cannot list every apply")
}

// A server that predates paging ignores starting_after and serves its first
// page, which need not list the apply the operator asked to start after once
// newer applies push it down. It never echoes starting_after, so the command
// fails rather than presenting the first page as the next one.
func TestStatusCmdAfterRefusesAServerThatIgnoresPaging(t *testing.T) {
	ps, server := newPagedServer(t, "/api/status", map[string]any{"": statusPage(true, nil, "apply-5", "apply-4")})
	ps.setLegacy()

	var runErr error
	captureOutput(t, func() { runErr = (&StatusCmd{After: "apply-3"}).Run(&Globals{Endpoint: server.URL}) })
	require.EqualError(t, runErr, "the server did not continue after apply apply-3; it predates paging, so it cannot show the next page")
}

// A truncated page below the server's cap points at --limit, as a larger page
// shows more. At the cap, and on a page already reached with --after, it names
// the --after apply that starts the next page instead.
func TestStatusCmdTruncatedFooter(t *testing.T) {
	underCap := statusPage(true, nil, "apply-4", "apply-3")
	atCap := statusPage(true, nil, "apply-4", "apply-3")
	atCap.MaxLimit = 2
	_, server := newPagedServer(t, "/api/status", map[string]any{
		"":        underCap,
		"apply-3": statusPage(true, nil, "apply-2", "apply-1"),
	})
	_, cappedServer := newPagedServer(t, "/api/status", map[string]any{"": atCap})

	first := captureOutput(t, func() {
		require.NoError(t, (&StatusCmd{Environment: "staging", Limit: 2}).Run(&Globals{Endpoint: server.URL}))
	})
	assert.Contains(t, first, "Showing the 2 most recent schema changes. Use --limit N to show more.")
	assert.NotContains(t, first, "--after")

	capped := captureOutput(t, func() {
		require.NoError(t, (&StatusCmd{Environment: "staging", Limit: 5}).Run(&Globals{Endpoint: cappedServer.URL}))
	})
	assert.Contains(t, capped, "Showing the 2 most recent schema changes. Add --after apply-3 for the next 2.")

	continued := captureOutput(t, func() {
		require.NoError(t, (&StatusCmd{Environment: "staging", Limit: 2, After: "apply-3"}).Run(&Globals{Endpoint: server.URL}))
	})
	assert.Contains(t, continued, "Showing 2 more schema changes. Add --after apply-1 for the next 2.")
}

func TestStatusCmdPagingFlagsOnlyApplyToTheList(t *testing.T) {
	err := (&StatusCmd{ApplyIDArg: "apply-1", All: true}).Run(&Globals{Endpoint: "http://localhost:1"})
	require.EqualError(t, err, "--after and --all page the recent applies list; drop them to show one apply")

	err = (&StatusCmd{Database: "orders", After: "apply-1"}).Run(&Globals{Endpoint: "http://localhost:1"})
	require.EqualError(t, err, "--after and --all page the recent applies list; -d already shows every apply for the database")
}
