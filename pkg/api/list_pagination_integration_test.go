//go:build integration

package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/storage/storagetest"
)

// pagingServer is a SchemaBot API server over real MySQL storage, for tests
// that walk a paged listing over HTTP.
type pagingServer struct {
	db    *sql.DB
	store storage.Storage
	mux   *http.ServeMux
}

func newPagingServer(t *testing.T) *pagingServer {
	t.Helper()
	dsn := newStorageDatabaseWithSchema(t).DSN
	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err)
	// The service owns the handle and closes it; this close only covers an
	// early failure before the service exists, so its error is discarded.
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.PingContext(t.Context()))
	store := mysqlstore.New(db)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(store, testServerConfig(), nil, logger)
	t.Cleanup(func() { utils.CloseAndLog(svc) })
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	return &pagingServer{db: db, store: store, mux: mux}
}

func (s *pagingServer) get(t *testing.T, path string, query url.Values, into any) {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path+"?"+query.Encode(), nil)
	w := httptest.NewRecorder()
	s.mux.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), into))
}

// walkStatus requests the first page of query, then each next page after the
// last apply of the one before, until the listing ends. It calls between after
// each page that has a successor and returns how many times each apply was
// listed.
func (s *pagingServer) walkStatus(t *testing.T, query url.Values, between func(page int)) (map[string]int, []apitypes.StatusResponse) {
	t.Helper()
	seen := map[string]int{}
	var pages []apitypes.StatusResponse
	for page := 0; ; page++ {
		require.Less(t, page, 100, "the walk must end")
		var resp apitypes.StatusResponse
		s.get(t, "/api/status", query, &resp)
		pages = append(pages, resp)
		for _, apply := range resp.Applies {
			seen[apply.ApplyID]++
		}
		if !resp.HasMore {
			return seen, pages
		}
		require.NotEmpty(t, resp.Applies, "a truncated page lists applies")
		if between != nil {
			between(page)
		}
		query.Set("starting_after", resp.Applies[len(resp.Applies)-1].ApplyID)
	}
}

// seedStatusFleet stores one in-flight apply followed by settled ones in
// staging, spread across a few databases, plus production applies the walks
// filter out. The in-flight apply is the oldest row, so it is the last apply a
// walk reaches. The settled applies are moved a day into the past, so they
// read as history rather than as activity from the instant the walk runs. It
// returns the staging apply identifiers and the in-flight apply.
func (s *pagingServer) seedStatusFleet(t *testing.T, settled int) (map[string]bool, *storage.Apply) {
	t.Helper()
	store := s.store
	inFlight := storagetest.CreateApplyWithStateAndEnv(t, store, storagetest.CreateLock(t, store, "fleet_inflight", storage.DatabaseTypeMySQL),
		"apply-fleet-inflight", 1, state.Apply.Running, "staging")
	want := map[string]bool{inFlight.ApplyIdentifier: true}
	locks := []*storage.Lock{
		storagetest.CreateLock(t, store, "fleet_orders", storage.DatabaseTypeMySQL),
		storagetest.CreateLock(t, store, "fleet_billing", storage.DatabaseTypeMySQL),
		storagetest.CreateLock(t, store, "fleet_catalog", storage.DatabaseTypeMySQL),
	}
	for i := range settled {
		applyState := state.Apply.Completed
		if i%50 == 0 {
			applyState = state.Apply.Failed
		}
		apply := storagetest.CreateApplyWithStateAndEnv(t, store, locks[i%len(locks)], fmt.Sprintf("apply-fleet-%04d", i), int64(100+i), applyState, "staging")
		want[apply.ApplyIdentifier] = true
	}
	for i := range 5 {
		storagetest.CreateApplyWithStateAndEnv(t, store, locks[0], fmt.Sprintf("apply-fleet-production-%d", i), int64(10_000+i), state.Apply.Completed, "production")
	}
	_, err := s.db.ExecContext(t.Context(),
		"UPDATE `applies` SET updated_at = updated_at - INTERVAL 1 DAY WHERE apply_identifier <> ?", inFlight.ApplyIdentifier)
	require.NoError(t, err)
	return want, inFlight
}

func assertListedOnce(t *testing.T, want map[string]bool, seen map[string]int) {
	t.Helper()
	for identifier := range want {
		if !assert.Equal(t, 1, seen[identifier], "%s must be listed exactly once", identifier) {
			return
		}
	}
	assert.Len(t, seen, len(want), "only applies matching the filters are listed")
}

// More applies match an environment than the server serves in one page. An
// operator walks every page at the page cap and gets each apply exactly once,
// with the state counts on every page covering the whole listing.
func TestStatusPageWalkListsEveryApplyAcrossTheCap(t *testing.T) {
	server := newPagingServer(t)
	want, _ := server.seedStatusFleet(t, maxStatusLimit+150)

	seen, pages := server.walkStatus(t, url.Values{"environment": {"staging"}, "limit": {fmt.Sprint(maxStatusLimit)}}, nil)
	require.Len(t, pages, 2)
	assert.Len(t, pages[0].Applies, maxStatusLimit)
	assert.Len(t, pages[1].Applies, len(want)-maxStatusLimit)
	assertListedOnce(t, want, seen)
	for i, page := range pages {
		total := 0
		for _, n := range page.StateCounts {
			total += n
		}
		assert.Equal(t, len(want), total, "page %d counts every matching apply", i)
	}
}

// An operator walks the finished applies in small pages, the way the default
// status view does, while the fleet keeps moving: the in-flight apply finishes
// and new applies are created between pages. The order is creation, which
// never changes, so every settled apply is listed exactly once, the apply that
// finished before the walk reached it is listed too, and the new applies land
// above the first page and are not listed.
func TestStatusPageWalkSurvivesChangesMidWalk(t *testing.T) {
	server := newPagingServer(t)
	want, inFlight := server.seedStatusFleet(t, 240)
	newLock := storagetest.CreateLock(t, server.store, "fleet_new", storage.DatabaseTypeMySQL)

	seen, pages := server.walkStatus(t, url.Values{"environment": {"staging"}, "active": {"false"}, "limit": {"25"}}, func(page int) {
		if page == 0 {
			finished, err := server.store.Applies().Get(t.Context(), inFlight.ID)
			require.NoError(t, err)
			finished.State = state.Apply.Completed
			require.NoError(t, server.store.Applies().Update(t.Context(), finished))
		}
		if page < 3 {
			storagetest.CreateApplyWithStateAndEnv(t, server.store, newLock, fmt.Sprintf("apply-fleet-new-%d", page), int64(20_000+page), state.Apply.Completed, "staging")
		}
	})
	assert.Greater(t, len(pages), 5, "the walk spans many pages")
	for i, page := range pages {
		require.NotNil(t, page.Active, "page %d echoes the active filter", i)
		assert.False(t, *page.Active, "page %d lists finished applies", i)
	}
	assertListedOnce(t, want, seen)
}

// Three applies are running and an operator pages through the active ones one
// at a time. The apply the first page ended on finishes before the next
// request. It keeps its place in the order, so the walk still reaches the
// running applies after it.
func TestStatusActiveWalkContinuesAfterAnApplyThatFinished(t *testing.T) {
	server := newPagingServer(t)
	var running []*storage.Apply
	for i := range 3 {
		lock := storagetest.CreateLock(t, server.store, fmt.Sprintf("fleet_running_%d", i), storage.DatabaseTypeMySQL)
		running = append(running, storagetest.CreateApplyWithStateAndEnv(t, server.store, lock, fmt.Sprintf("apply-fleet-running-%d", i), int64(10+i), state.Apply.Running, "staging"))
	}

	seen, pages := server.walkStatus(t, url.Values{"environment": {"staging"}, "active": {"true"}, "limit": {"1"}}, func(page int) {
		if page != 0 {
			return
		}
		anchor, err := server.store.Applies().Get(t.Context(), running[2].ID)
		require.NoError(t, err)
		anchor.State = state.Apply.Completed
		require.NoError(t, server.store.Applies().Update(t.Context(), anchor))
	})
	require.NotEmpty(t, pages)
	require.NotEmpty(t, pages[0].Applies)
	assert.Equal(t, running[2].ApplyIdentifier, pages[0].Applies[0].ApplyID, "newest first")
	assertListedOnce(t, map[string]bool{
		running[0].ApplyIdentifier: true, running[1].ApplyIdentifier: true, running[2].ApplyIdentifier: true,
	}, seen)
}

// The page walk keeps every filter the first page was listed under: here a
// database and a state filter together, against a fleet where most applies
// match neither.
func TestStatusPageWalkKeepsFilters(t *testing.T) {
	server := newPagingServer(t)
	server.seedStatusFleet(t, 300)

	var everything apitypes.StatusResponse
	server.get(t, "/api/status", url.Values{"environment": {"staging"}, "database": {"fleet_orders"}, "state": {"completed"}, "limit": {fmt.Sprint(maxStatusLimit)}}, &everything)
	require.False(t, everything.HasMore)
	want := map[string]bool{}
	for _, apply := range everything.Applies {
		require.Equal(t, "fleet_orders", apply.Database)
		require.Equal(t, state.Apply.Completed, apply.State)
		want[apply.ApplyID] = true
	}
	require.Greater(t, len(want), 30)

	seen, pages := server.walkStatus(t, url.Values{"environment": {"staging"}, "database": {"fleet_orders"}, "state": {"completed"}, "limit": {"30"}}, nil)
	assert.Greater(t, len(pages), 1)
	assertListedOnce(t, want, seen)
}

// createPagingPlan stores a plan for the plans page walk, created at the given
// instant.
func (s *pagingServer) createPagingPlan(t *testing.T, identifier, database string, createdAt time.Time) {
	t.Helper()
	_, err := s.store.Plans().Create(t.Context(), &storage.Plan{
		PlanIdentifier: identifier,
		Database:       database,
		DatabaseType:   storage.DatabaseTypeMySQL,
		Environment:    "staging",
		Repository:     "org/repo",
		PullRequest:    7,
		CreatedAt:      createdAt,
	})
	require.NoError(t, err)
}

// The plan listing caps a page far below the status list. Walking it page by
// page from each page's last plan lists every plan exactly once, including
// plans that share a creation second across a page boundary, and a plan stored
// while the walk runs is newer than every page after the first, so the walk
// does not list it.
func TestPlansPageWalkListsEveryPlanOnce(t *testing.T) {
	server := newPagingServer(t)
	base := time.Now().UTC().Truncate(time.Second).Add(-time.Hour)
	want := map[string]bool{}
	for i := range maxPlansLimit + 30 {
		identifier := fmt.Sprintf("plan-walk-%04d", i)
		// Several plans per second, so pages break inside a second.
		server.createPagingPlan(t, identifier, "orders", base.Add(time.Duration(i/4)*time.Second))
		want[identifier] = true
	}

	seen := map[string]int{}
	query := url.Values{"environment": {"staging"}, "repository": {"org/repo"}, "pull_request": {"7"}, "limit": {"40"}}
	var pages int
	for {
		require.Less(t, pages, 100, "the walk must end")
		var resp apitypes.PlansResponse
		server.get(t, "/api/plans", query, &resp)
		pages++
		assert.Equal(t, query.Get("starting_after"), resp.StartingAfter, "page %d names the plan it continues from", pages)
		for _, plan := range resp.Plans {
			seen[plan.PlanID]++
		}
		if !resp.HasMore {
			break
		}
		if pages == 1 {
			server.createPagingPlan(t, "plan-walk-created-mid-walk", "orders", time.Now().UTC())
		}
		query.Set("starting_after", resp.Plans[len(resp.Plans)-1].PlanID)
	}
	assert.Greater(t, pages, 5)
	assert.Zero(t, seen["plan-walk-created-mid-walk"])
	assertListedOnce(t, want, seen)
}

// A plans page continues only the listing its starting_after plan came from:
// a plan that does not exist, or whose database, environment, repository, or
// pull request differs from the request's filters, is refused with a 400 that
// names the mismatch.
func TestPlansPageRefusesAPlanFromAnotherListing(t *testing.T) {
	server := newPagingServer(t)
	server.createPagingPlan(t, "plan-orders", "orders", time.Now().UTC().Add(-time.Minute))

	for _, tc := range []struct {
		name  string
		query url.Values
		want  string
	}{
		{"unknown plan", url.Values{"starting_after": {"plan-missing"}}, "starting_after plan plan-missing does not exist; pass the last plan ID from the previous page"},
		{"database", url.Values{"database": {"billing"}, "starting_after": {"plan-orders"}}, "starting_after plan plan-orders is for database orders, not billing; repeat the previous page's filters"},
		{"environment", url.Values{"environment": {"production"}, "starting_after": {"plan-orders"}}, "starting_after plan plan-orders is in environment staging, not production; repeat the previous page's filters"},
		{"repository", url.Values{"repository": {"org/other"}, "starting_after": {"plan-orders"}}, "starting_after plan plan-orders is from repository org/repo, not org/other; repeat the previous page's filters"},
		{"pull request", url.Values{"repository": {"org/repo"}, "pull_request": {"8"}, "starting_after": {"plan-orders"}}, "starting_after plan plan-orders is from pull request 7, not 8; repeat the previous page's filters"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/plans?"+tc.query.Encode(), nil)
			w := httptest.NewRecorder()
			server.mux.ServeHTTP(w, req)
			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			assert.Contains(t, w.Body.String(), tc.want)
		})
	}

	var resp apitypes.PlansResponse
	server.get(t, "/api/plans", url.Values{"database": {"orders"}, "environment": {"staging"}, "repository": {"org/repo"}, "pull_request": {"7"}, "starting_after": {"plan-orders"}}, &resp)
	assert.Empty(t, resp.Plans, "nothing is older than the only plan")
	assert.False(t, resp.HasMore)
}
