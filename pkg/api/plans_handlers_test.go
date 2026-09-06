package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
)

// listCapturingPlanStore records the ListPlansOptions each List call receives
// and serves a canned result, so handler tests can assert both the storage
// filter the server built and the response it rendered.
type listCapturingPlanStore struct {
	mockPlanLookupStore
	listOpts  []storage.ListPlansOptions
	listPlans []*storage.Plan
	listErr   error
}

func (s *listCapturingPlanStore) List(_ context.Context, opts storage.ListPlansOptions) ([]*storage.Plan, error) {
	s.listOpts = append(s.listOpts, opts)
	return s.listPlans, s.listErr
}

func newPlansTestServer(t *testing.T, plans storage.PlanStore) *http.ServeMux {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithPlanLookup{plans: plans}, testServerConfig(), nil, logger)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	return mux
}

// storedTestPlans returns one PR-generated MySQL plan with mixed change
// operations and one ad-hoc Vitess plan carrying a VSchema change and
// per-shard changes, newest first as the store would return them.
func storedTestPlans(now time.Time) []*storage.Plan {
	return []*storage.Plan{
		{
			PlanIdentifier: "plan-200",
			Database:       "orders",
			DatabaseType:   storage.DatabaseTypeMySQL,
			Deployment:     "primary",
			Environment:    "staging",
			Repository:     "org/repo",
			PullRequest:    42,
			SchemaPath:     "schema/orders",
			HeadSHA:        "abc1234",
			CreatedAt:      now,
			Namespaces: map[string]*storage.NamespacePlanData{
				"orders": {Tables: []storage.TableChange{
					{Table: "users", DDL: "ALTER TABLE `users` DROP COLUMN `email`;", Operation: "alter", IsUnsafe: true, UnsafeReason: "DROP COLUMN removes data"},
					{Table: "events", DDL: "CREATE TABLE `events` (`id` bigint unsigned NOT NULL);", Operation: "create"},
					{Table: "legacy", DDL: "ALTER TABLE `legacy` MODIFY `id` bigint;", Operation: "alter", ExecutionMode: "blocked", ModeReason: "unsupported by engine"},
				}},
			},
		},
		{
			PlanIdentifier: "plan-100",
			Database:       "commerce",
			DatabaseType:   storage.DatabaseTypeVitess,
			Environment:    "production",
			CreatedAt:      now.Add(-time.Minute),
			Namespaces: map[string]*storage.NamespacePlanData{
				"commerce": {
					Tables:    []storage.TableChange{{Table: "carts", DDL: "ALTER TABLE `carts` ADD COLUMN `note` varchar(255);", Operation: "alter"}},
					Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded": true}`},
				},
			},
			Shards: []storage.ShardPlan{
				{Namespace: "commerce", Shard: "-80", Changes: []storage.TableChange{{Table: "carts", DDL: "ALTER TABLE `carts` ADD COLUMN `note` varchar(255);", Operation: "alter"}}},
			},
		},
	}
}

// TestPlansListHandler exercises the stored-plan listing: the server passes
// the query filters through to storage with one extra row requested for
// truncation detection, and each returned plan is summarized with change
// counts rather than full plan content.
func TestPlansListHandler(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	store := &listCapturingPlanStore{listPlans: storedTestPlans(now)}
	mux := newPlansTestServer(t, store)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet,
		"/api/plans?limit=1&database=OrDeRs&environment=StAgInG&repository=Org/Repo&pull_request=42&last=24h", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var resp apitypes.PlansResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	require.Len(t, store.listOpts, 1)
	opts := store.listOpts[0]
	assert.Equal(t, 2, opts.Limit, "server should request one extra row to detect truncation")
	assert.Equal(t, "orders", opts.Database)
	assert.Equal(t, "staging", opts.Environment)
	assert.Equal(t, "org/repo", opts.Repository)
	assert.Equal(t, 42, opts.PullRequest)
	assert.WithinDuration(t, time.Now().Add(-24*time.Hour), opts.Since, time.Minute)

	assert.Equal(t, 1, resp.Limit)
	assert.Equal(t, maxPlansLimit, resp.MaxLimit)
	assert.True(t, resp.HasMore, "two rows for limit 1 means the listing was truncated")
	assert.Equal(t, "24h0m0s", resp.Last)
	require.Len(t, resp.Plans, 1)

	summary := resp.Plans[0]
	assert.Equal(t, "plan-200", summary.PlanID)
	assert.Equal(t, "orders", summary.Database)
	assert.Equal(t, storage.DatabaseTypeMySQL, summary.DatabaseType)
	assert.Equal(t, "primary", summary.Deployment)
	assert.Equal(t, "staging", summary.Environment)
	assert.Equal(t, "org/repo", summary.Repository)
	assert.Equal(t, 42, summary.PullRequest)
	assert.Equal(t, "abc1234", summary.HeadSHA)
	assert.Equal(t, map[string]int{"alter": 2, "create": 1}, summary.ChangeCounts)
	assert.Equal(t, 1, summary.UnsafeCount)
	assert.Equal(t, 1, summary.BlockedCount)
	assert.Zero(t, summary.VSchemaChangeCount)
}

// TestPlansListHandlerSummarizesAdHocAndVSchemaPlans verifies that a plan
// without PR provenance lists as ad-hoc (no repository or PR fields) and that
// a namespace carrying a VSchema artifact is counted as a VSchema change.
func TestPlansListHandlerSummarizesAdHocAndVSchemaPlans(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	store := &listCapturingPlanStore{listPlans: storedTestPlans(now)}
	mux := newPlansTestServer(t, store)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/plans", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var resp apitypes.PlansResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	require.Len(t, store.listOpts, 1)
	assert.Equal(t, defaultPlansLimit+1, store.listOpts[0].Limit)
	assert.False(t, resp.HasMore)
	require.Len(t, resp.Plans, 2)

	adHoc := resp.Plans[1]
	assert.Equal(t, "plan-100", adHoc.PlanID)
	assert.Empty(t, adHoc.Repository)
	assert.Zero(t, adHoc.PullRequest)
	assert.Equal(t, map[string]int{"alter": 1}, adHoc.ChangeCounts)
	assert.Equal(t, 1, adHoc.VSchemaChangeCount)
}

// TestPlansListHandlerRejectsBadParams verifies that malformed query
// parameters fail the request with a clear 400 instead of silently listing
// with a partial filter.
func TestPlansListHandlerRejectsBadParams(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		wantMsg string
	}{
		{name: "zero limit", target: "/api/plans?limit=0", wantMsg: "limit must be a positive integer"},
		{name: "non-numeric limit", target: "/api/plans?limit=abc", wantMsg: "limit must be a positive integer"},
		{name: "bad last", target: "/api/plans?last=yesterday", wantMsg: "last must be a positive duration"},
		{name: "pr without repository", target: "/api/plans?pull_request=42", wantMsg: "pull_request requires repository"},
		{name: "non-numeric pr", target: "/api/plans?repository=org/repo&pull_request=abc", wantMsg: "pull_request must be a positive integer"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &listCapturingPlanStore{}
			mux := newPlansTestServer(t, store)

			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, tc.target, nil)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			assert.Equal(t, http.StatusBadRequest, w.Code)
			assert.Contains(t, w.Body.String(), tc.wantMsg)
			assert.Empty(t, store.listOpts, "a rejected request must not reach storage")
		})
	}
}

// TestPlanGetHandler verifies that fetching one stored plan reconstructs the
// full plan content in the POST /api/plan response shape: table changes carry
// their namespace, shards are preserved, and the engine is derived from the
// database type.
func TestPlanGetHandler(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	plan := storedTestPlans(now)[1]
	mux := newPlansTestServer(t, &mockPlanLookupStore{plan: plan})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/plans/plan-100", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var resp apitypes.StoredPlanResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	assert.Equal(t, "plan-100", resp.PlanID)
	assert.Equal(t, "commerce", resp.Database)
	require.NotNil(t, resp.Plan)
	assert.Equal(t, storage.EnginePlanetScale, resp.Plan.Engine)
	require.Len(t, resp.Plan.Changes, 1)

	change := resp.Plan.Changes[0]
	assert.Equal(t, "commerce", change.Namespace)
	assert.True(t, change.HasVSchemaChange(), "a stored VSchema artifact must surface as a VSchema change")
	require.Len(t, change.TableChanges, 1)
	assert.Equal(t, "carts", change.TableChanges[0].TableName)
	assert.Equal(t, "commerce", change.TableChanges[0].Namespace, "table changes without a stored namespace must inherit their namespace key")
	assert.Equal(t, "alter", change.TableChanges[0].ChangeType)
	assert.Contains(t, change.TableChanges[0].DDL, "ADD COLUMN `note`")

	require.Len(t, resp.Plan.Shards, 1)
	assert.Equal(t, "-80", resp.Plan.Shards[0].Shard)
	require.Len(t, resp.Plan.Shards[0].Changes, 1)
	assert.Equal(t, "carts", resp.Plan.Shards[0].Changes[0].TableName)
}

// A stored plan's per-table size estimates survive the read path, so a plan
// fetched later reports the same sizes as the response that planned it.
func TestPlanGetHandlerCarriesStoredSizeEstimates(t *testing.T) {
	rows, largest, sizeBytes := int64(13_100_000), int64(3_400_000), int64(6_200_000_000)
	plan := &storage.Plan{
		PlanIdentifier: "plan-sized",
		Database:       "commerce",
		DatabaseType:   storage.DatabaseTypeVitess,
		Environment:    "production",
		CreatedAt:      time.Now().UTC().Truncate(time.Second),
		Namespaces: map[string]*storage.NamespacePlanData{
			"commerce": {Tables: []storage.TableChange{{
				Table:            "carts",
				DDL:              "ALTER TABLE `carts` ADD INDEX `idx_note`(`note`);",
				Operation:        "alter",
				EstimatedRows:    &rows,
				LargestShardRows: &largest,
				EstimatedBytes:   &sizeBytes,
				ShardCount:       4,
			}}},
		},
	}
	mux := newPlansTestServer(t, &mockPlanLookupStore{plan: plan})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/plans/plan-sized", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var resp apitypes.StoredPlanResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	require.NotNil(t, resp.Plan)
	require.Len(t, resp.Plan.Changes, 1)
	require.Len(t, resp.Plan.Changes[0].TableChanges, 1)
	change := resp.Plan.Changes[0].TableChanges[0]
	require.NotNil(t, change.EstimatedRows)
	assert.Equal(t, rows, *change.EstimatedRows)
	require.NotNil(t, change.LargestShardRows)
	assert.Equal(t, largest, *change.LargestShardRows)
	require.NotNil(t, change.EstimatedBytes)
	assert.Equal(t, sizeBytes, *change.EstimatedBytes)
	assert.Equal(t, 4, change.ShardCount)
}

// TestPlanGetHandlerDistinguishesMissingFromStorageError verifies the two
// failure modes stay separate: an unknown plan identifier is a 404, while a
// storage failure is a 500 and never reads as "plan not found".
func TestPlanGetHandlerDistinguishesMissingFromStorageError(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		mux := newPlansTestServer(t, &mockPlanLookupStore{})
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/plans/plan-missing", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusNotFound, w.Code)
		assert.Contains(t, w.Body.String(), "plan not found: plan-missing")
	})

	t.Run("storage error", func(t *testing.T) {
		mux := newPlansTestServer(t, &mockPlanLookupStore{err: assert.AnError})
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/plans/plan-100", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.NotContains(t, w.Body.String(), "not found")
	})
}

// TestPlansListHandlerStorageError verifies a storage failure surfaces as a
// 500 with a sanitized message rather than an empty 200 listing.
func TestPlansListHandlerStorageError(t *testing.T) {
	store := &listCapturingPlanStore{listErr: assert.AnError}
	mux := newPlansTestServer(t, store)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/plans", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "failed to list plans")
}
