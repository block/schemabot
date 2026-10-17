package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
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
// filters. A request with an active filter is served the page keyed
// "active=<value>:<starting_after>" when there is one, and the filter is
// echoed back. A legacy server ignores starting_after and the active filter:
// it serves the first page for every request and echoes neither.
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
		active, filtered := query["active"]
		if ps.legacy {
			key = ""
			filtered = false
		}
		page, ok := ps.pages[key]
		if filtered {
			if activePage, found := ps.pages["active="+active+":"+key]; found {
				page, ok = activePage, true
			}
		}
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
		if filtered {
			fields["active"] = active == "true"
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
// named in running. A completed apply ran for four minutes.
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

// activeStatusPage is the response to the default view's request for every
// active apply: the given applies, all running.
func activeStatusPage(identifiers ...string) apitypes.StatusResponse {
	page := statusPage(false, identifiers, identifiers...)
	page.Limit = 1000
	page.StateCounts = map[string]int{state.Apply.Running: len(identifiers)}
	return page
}

// status --all --json requests each page after the last apply of the one
// before and prints one response whose applies array holds every page, so one
// command answers questions about the whole history. Every page request
// repeats the listing's filters.
func TestStatusCmdAllMergesEveryPage(t *testing.T) {
	ps, server := newPagedServer(t, "/api/status", map[string]any{
		"":        statusPage(true, []string{"apply-6"}, "apply-6", "apply-5"),
		"apply-5": statusPage(true, []string{"apply-4"}, "apply-4", "apply-3"),
		"apply-3": statusPage(false, nil, "apply-2", "apply-1"),
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
		assert.NotContains(t, query, "active", "page %d lists every apply, active or not", i)
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
		"":             underCap,
		"apply-3":      statusPage(true, nil, "apply-2", "apply-1"),
		"active=true:": activeStatusPage(),
	})
	_, cappedServer := newPagedServer(t, "/api/status", map[string]any{"": atCap, "active=true:": activeStatusPage()})

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

// The default view lists every active apply above the finished ones, from two
// requests carrying the same filters: the finished applies a page at a time,
// and the active ones in full. The footer pages the finished applies alone.
func TestStatusCmdShowsActiveAboveFinished(t *testing.T) {
	ps, server := newPagedServer(t, "/api/status", map[string]any{
		"active=false:": statusPage(true, nil, "apply-4", "apply-3"),
		"active=true:":  activeStatusPage("apply-6", "apply-5"),
	})

	output := captureOutput(t, func() {
		require.NoError(t, (&StatusCmd{Environment: "staging", Last: "2d", Limit: 2}).Run(&Globals{Endpoint: server.URL}))
	})

	activeHeader := strings.Index(output, "2 active schema changes")
	recentHeader := strings.Index(output, "Recent schema changes")
	require.NotEqual(t, -1, activeHeader, output)
	require.Greater(t, recentHeader, activeHeader, "active applies come first:\n%s", output)
	activeSection, recentSection := output[activeHeader:recentHeader], output[recentHeader:]
	assert.Contains(t, activeSection, "apply-6")
	assert.Contains(t, activeSection, "apply-5")
	assert.Contains(t, recentSection, "apply-4")
	assert.Contains(t, recentSection, "apply-3")
	assert.Contains(t, recentSection, "Showing the 2 most recent schema changes. Use --limit N to show more.")

	require.Len(t, ps.queries, 2)
	assert.Equal(t, "false", ps.queries[0]["active"], "the first request pages the finished applies")
	assert.Equal(t, "2", ps.queries[0]["limit"])
	assert.Equal(t, "true", ps.queries[1]["active"], "the second lists the active applies")
	assert.Equal(t, "1000", ps.queries[1]["limit"], "every active apply is listed, not --limit of them")
	for i, query := range ps.queries {
		assert.Equal(t, "staging", query["environment"], "request %d keeps the environment", i)
		assert.Equal(t, "48h0m0s", query["last"], "request %d keeps the window", i)
	}
}

// A page reached with --after continues the finished applies alone; the
// active ones were listed on the first page.
func TestStatusCmdAfterPagesOnlyFinishedApplies(t *testing.T) {
	ps, server := newPagedServer(t, "/api/status", map[string]any{
		"active=false:apply-3": statusPage(false, nil, "apply-2", "apply-1"),
	})

	output := captureOutput(t, func() {
		require.NoError(t, (&StatusCmd{Environment: "staging", Limit: 2, After: "apply-3"}).Run(&Globals{Endpoint: server.URL}))
	})
	assert.NotContains(t, output, "active schema change")
	assert.Contains(t, output, "Recent schema changes")
	assert.Contains(t, output, "apply-1")

	require.Len(t, ps.queries, 1)
	assert.Equal(t, "false", ps.queries[0]["active"])
	assert.Equal(t, "apply-3", ps.queries[0]["starting_after"])
}

// With nothing active, the default view says so in one line above the
// finished applies.
func TestStatusCmdSaysWhenNothingIsActive(t *testing.T) {
	_, server := newPagedServer(t, "/api/status", map[string]any{
		"active=false:": statusPage(false, nil, "apply-2", "apply-1"),
		"active=true:":  activeStatusPage(),
	})

	output := captureOutput(t, func() {
		require.NoError(t, (&StatusCmd{Environment: "staging"}).Run(&Globals{Endpoint: server.URL}))
	})
	assert.Contains(t, output, "No active schema changes")
	assert.Less(t, strings.Index(output, "No active schema changes"), strings.Index(output, "Recent schema changes"))
}

// A state filter already says which applies to list, so the list is one table
// from one request, with no active section.
func TestStatusCmdStateFilterListsOneTable(t *testing.T) {
	ps, server := newPagedServer(t, "/api/status", map[string]any{
		"": statusPage(false, []string{"apply-2"}, "apply-2"),
	})

	output := captureOutput(t, func() {
		require.NoError(t, (&StatusCmd{Environment: "staging", State: "running"}).Run(&Globals{Endpoint: server.URL}))
	})
	assert.Contains(t, output, "1 active schema change")
	assert.NotContains(t, output, "Recent schema changes")

	require.Len(t, ps.queries, 1)
	assert.NotContains(t, ps.queries[0], "active")
	assert.Equal(t, "running", ps.queries[0]["state"])
}

// A server that predates the finished-only filter ignores active=false and
// lists everything, in its own order. The view renders that list as one table
// rather than repeat its active applies in a second section.
func TestStatusCmdRendersAServerWithoutTheFinishedFilterAsOneTable(t *testing.T) {
	ps, server := newPagedServer(t, "/api/status", map[string]any{
		"": statusPage(false, []string{"apply-3"}, "apply-3", "apply-2"),
	})
	ps.setLegacy()

	output := captureOutput(t, func() {
		require.NoError(t, (&StatusCmd{Environment: "staging"}).Run(&Globals{Endpoint: server.URL}))
	})
	assert.Contains(t, output, "1 active schema change")
	assert.Contains(t, output, "apply-2")
	assert.NotContains(t, output, "Recent schema changes")
	require.Len(t, ps.queries, 1, "the active applies are already in the server's list")
}

func plansPage(hasMore bool, identifiers ...string) apitypes.PlansResponse {
	page := apitypes.PlansResponse{Limit: len(identifiers), MaxLimit: 200, HasMore: hasMore}
	for _, identifier := range identifiers {
		page.Plans = append(page.Plans, &apitypes.PlanSummaryResponse{PlanID: identifier, Database: "orders", Environment: "staging"})
	}
	return page
}

// list-plans --all --json requests each page after the last plan of the one
// before and merges every page into one plans array, repeating the listing's
// filters on every request. A truncated human page ends with the next page's
// command.
func TestPlansCmdPaging(t *testing.T) {
	ps, server := newPagedServer(t, "/api/plans", map[string]any{
		"":       plansPage(true, "plan-4", "plan-3"),
		"plan-3": plansPage(false, "plan-2", "plan-1"),
	})

	var runErr error
	output := captureOutput(t, func() {
		runErr = (&PlansCmd{Environment: "staging", Repository: "org/repo", PR: 7, All: true, JSON: true}).Run(&Globals{Endpoint: server.URL})
	})
	require.NoError(t, runErr)
	var merged apitypes.PlansResponse
	require.NoError(t, json.Unmarshal([]byte(output), &merged))
	identifiers := make([]string, 0, len(merged.Plans))
	for _, plan := range merged.Plans {
		identifiers = append(identifiers, plan.PlanID)
	}
	assert.Equal(t, []string{"plan-4", "plan-3", "plan-2", "plan-1"}, identifiers)
	assert.False(t, merged.HasMore)
	require.Len(t, ps.queries, 2)
	for i, query := range ps.queries {
		assert.Equal(t, "org/repo", query["repository"], "page %d repeats the repository", i)
		assert.Equal(t, "7", query["pull_request"], "page %d repeats the pull request", i)
		assert.Equal(t, "200", query["limit"], "page %d asks for full pages", i)
	}
	assert.Empty(t, ps.queries[0]["starting_after"])
	assert.Equal(t, "plan-3", ps.queries[1]["starting_after"])

	human := captureOutput(t, func() {
		require.NoError(t, (&PlansCmd{Environment: "staging", Repository: "org/repo", PR: 7}).Run(&Globals{Endpoint: server.URL}))
	})
	assert.Contains(t, human, "Showing the 2 most recent plans. Use --limit N to show more.")
	assert.NotContains(t, human, "--after", "below the cap a larger --limit shows more")

	continued := captureOutput(t, func() {
		require.NoError(t, (&PlansCmd{Environment: "staging", Repository: "org/repo", PR: 7, After: "plan-3"}).Run(&Globals{Endpoint: server.URL}))
	})
	assert.Contains(t, continued, "plan-1")
	assert.NotContains(t, continued, "Use --limit", "the last page has no next page")
	assert.NotContains(t, continued, "--after", "the last page has no next page")
}

// At the server's cap a larger --limit cannot show more, so a truncated plan
// list names the --after plan that starts the next page, as does a page
// already reached with --after.
func TestPlansCmdTruncatedFooterAtTheCap(t *testing.T) {
	atCap := plansPage(true, "plan-4", "plan-3")
	atCap.MaxLimit = 2
	continued := plansPage(true, "plan-2", "plan-1")
	_, server := newPagedServer(t, "/api/plans", map[string]any{"": atCap, "plan-3": continued})

	first := captureOutput(t, func() {
		require.NoError(t, (&PlansCmd{Environment: "staging", Limit: 5}).Run(&Globals{Endpoint: server.URL}))
	})
	assert.Contains(t, first, "Showing the 2 most recent plans. Add --after plan-3 for the next 2.")
	assert.NotContains(t, first, "Use --limit")

	next := captureOutput(t, func() {
		require.NoError(t, (&PlansCmd{Environment: "staging", Limit: 2, After: "plan-3"}).Run(&Globals{Endpoint: server.URL}))
	})
	assert.Contains(t, next, "Showing 2 more plans. Add --after plan-1 for the next 2.")
}

// A server that predates paging ignores starting_after and serves its first
// page, which need not list the plan the operator asked to start after once
// newer plans push it down. It never echoes starting_after, so both a single
// page and --all fail rather than presenting the wrong page as the next one.
func TestPlansCmdRefusesAServerThatIgnoresPaging(t *testing.T) {
	ps, server := newPagedServer(t, "/api/plans", map[string]any{"": plansPage(true, "plan-5", "plan-4")})
	ps.setLegacy()

	var runErr error
	captureOutput(t, func() { runErr = (&PlansCmd{After: "plan-3"}).Run(&Globals{Endpoint: server.URL}) })
	require.EqualError(t, runErr, "the server did not continue after plan plan-3; it predates paging, so it cannot show the next page")

	captureOutput(t, func() { runErr = (&PlansCmd{All: true, JSON: true}).Run(&Globals{Endpoint: server.URL}) })
	require.EqualError(t, runErr, "the server did not continue after plan plan-4; it predates paging, so --all cannot list every plan")

	require.EqualError(t, (&PlansCmd{PlanIDArg: "plan-1", After: "plan-2"}).Run(&Globals{Endpoint: server.URL}),
		"--after and --all page the plan list; drop them to show one plan")
}
