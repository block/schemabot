package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/caller"
	"github.com/block/schemabot/pkg/storage"
)

func TestCallPullSchemaAPI(t *testing.T) {
	var gotReq apitypes.PullSchemaRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/pull", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotReq))

		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(apitypes.PullSchemaResponse{
			Database:    "orders",
			Type:        "mysql",
			Environment: "production",
			TableCount:  1,
			Namespaces: map[string]*apitypes.PulledNamespace{
				"orders": {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
			},
		}))
	}))
	t.Cleanup(server.Close)

	result, err := CallPullSchemaAPIWithOptions(server.URL, "orders", "mysql", "production", PullSchemaOptions{
		Namespaces:    []string{"orders_production", "orders_audit_production"},
		CatalogDetail: "detailed",
		Lint:          true,
	})
	require.NoError(t, err)

	assert.Equal(t, apitypes.PullSchemaRequest{Database: "orders", Type: "mysql", Environment: "production", Namespaces: []string{"orders_production", "orders_audit_production"}, CatalogDetail: "detailed", Lint: true}, gotReq)
	require.NotNil(t, result)
	assert.Equal(t, "orders", result.Database)
	assert.Equal(t, "mysql", result.Type)
	assert.Equal(t, "production", result.Environment)
	assert.Equal(t, int32(1), result.TableCount)
	assert.Equal(t, "CREATE TABLE `users` (`id` bigint NOT NULL);\n", result.Namespaces["orders"].Tables["users"])
}

func TestLogRequestQueryValuesAreEncoded(t *testing.T) {
	var requests []*http.Request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Clone(t.Context()))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"logs": []any{}, "sources": []any{}, "errors": []any{}, "target": r.URL.Query().Get("target"), "shard": r.URL.Query().Get("shard")}))
	}))
	t.Cleanup(server.Close)

	_, err := GetLogs(server.URL, "", "", "apply/a & b", LogScope{Target: "shop/a & b", Shard: "-80"}, 17)
	require.NoError(t, err)
	_, err = GetDeploymentLogs(server.URL, "apply/a & b", "data plane/one", LogScope{Target: "shop/a & b", Shard: "80-"}, 23)
	require.NoError(t, err)
	require.Len(t, requests, 2)
	assert.Equal(t, "apply/a & b", requests[0].URL.Query().Get("apply_id"))
	assert.Equal(t, "17", requests[0].URL.Query().Get("limit"))
	assert.Equal(t, "shop/a & b", requests[0].URL.Query().Get("target"))
	assert.Equal(t, "-80", requests[0].URL.Query().Get("shard"))
	assert.Equal(t, "apply/a & b", requests[1].URL.Query().Get("apply_id"))
	assert.Equal(t, "data plane/one", requests[1].URL.Query().Get("deployment"))
	assert.Equal(t, "23", requests[1].URL.Query().Get("limit"))
	assert.Equal(t, "shop/a & b", requests[1].URL.Query().Get("target"))
	assert.Equal(t, "80-", requests[1].URL.Query().Get("shard"))
}

func TestCallPullSchemaAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte(`{"error":"database not configured"}`))
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	result, err := CallPullSchemaAPI(server.URL, "orders", "mysql", "production")
	require.Error(t, err)

	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "database not configured")
}

func TestListDatabases(t *testing.T) {
	var gotType, gotName string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/api/databases", r.URL.Path)
		gotType = r.URL.Query().Get("type")
		gotName = r.URL.Query().Get("name")
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(apitypes.DatabaseListResponse{
			Databases: []*apitypes.DatabaseResponse{
				{
					Database: "orders",
					Type:     "mysql",
					Environments: []*apitypes.DatabaseEnvironmentResponse{
						{Environment: "production", Deployments: []string{"pie"}},
					},
				},
			},
		}))
	}))
	t.Cleanup(server.Close)

	result, err := ListDatabases(server.URL, ListDatabasesOptions{Type: "mysql", Name: "ord"})
	require.NoError(t, err)

	assert.Equal(t, "mysql", gotType)
	assert.Equal(t, "ord", gotName)
	require.NotNil(t, result)
	require.Len(t, result.Databases, 1)
	assert.Equal(t, "orders", result.Databases[0].Database)
	assert.Equal(t, "mysql", result.Databases[0].Type)
	assert.Equal(t, []string{"pie"}, result.Databases[0].Environments[0].Deployments)
}

func TestGetStatusWithOptions(t *testing.T) {
	var gotLimit, gotEnvironment, gotFailed, gotLast, gotState string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotLimit = r.URL.Query().Get("limit")
		gotEnvironment = r.URL.Query().Get("environment")
		gotFailed = r.URL.Query().Get("failed")
		gotLast = r.URL.Query().Get("last")
		gotState = r.URL.Query().Get("state")
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"active_count":0,"limit":50,"applies":[]}`))
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	result, err := GetStatus(server.URL, StatusOptions{
		Limit:       50,
		Environment: "staging",
		Failed:      true,
		Last:        24 * time.Hour,
	})
	require.NoError(t, err)

	assert.Equal(t, "50", gotLimit)
	assert.Equal(t, "staging", gotEnvironment)
	assert.Equal(t, "true", gotFailed)
	assert.Equal(t, "24h0m0s", gotLast)
	assert.Empty(t, gotState)
	assert.Equal(t, 50, result.Limit)

	_, err = GetStatus(server.URL, StatusOptions{State: "running"})
	require.NoError(t, err)
	assert.Equal(t, "running", gotState)
	assert.Empty(t, gotFailed)
}

// The apply/rollback preflight asks only for applies still holding a target, so
// deciding whether one database is busy never depends on settled history.
func TestCheckActiveSchemaChangeRequestsActiveOnly(t *testing.T) {
	var gotActive, gotEnvironment string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotActive = r.URL.Query().Get("active")
		gotEnvironment = r.URL.Query().Get("environment")
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"active_count":1,"limit":1000,"applies":[` +
			`{"apply_id":"apply-busy","database":"orders","environment":"staging","state":"running"}]}`))
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	active, err := CheckActiveSchemaChange(server.URL, "orders", "staging")
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.Equal(t, "apply-busy", active.ApplyID)
	assert.Equal(t, "running", active.State)
	assert.Equal(t, "true", gotActive)
	assert.Equal(t, "staging", gotEnvironment)
}

// The preflight compares the operator's flags against stored keys. Either side
// can arrive in a different case — the flags as typed, a stored row in the
// spelling it was written with before storage folded its keys — and the busy
// apply must be found either way rather than the database reported idle.
func TestCheckActiveSchemaChangeFoldsKeysBeforeComparing(t *testing.T) {
	cases := map[string]struct {
		stored                string
		database, environment string
	}{
		"flags typed in another case":        {stored: `"database":"orders","environment":"staging"`, database: "Orders", environment: "Staging"},
		"stored row spelled in another case": {stored: `"database":"Orders","environment":"Staging"`, database: "orders", environment: "staging"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, err := w.Write([]byte(`{"active_count":1,"limit":1000,"applies":[` +
					`{"apply_id":"apply-busy",` + tc.stored + `,"state":"running"}]}`))
				assert.NoError(t, err)
			}))
			t.Cleanup(server.Close)

			active, err := CheckActiveSchemaChange(server.URL, tc.database, tc.environment)
			require.NoError(t, err)
			require.NotNil(t, active)
			assert.Equal(t, "apply-busy", active.ApplyID)
			assert.Equal(t, "running", active.State)
		})
	}
}

// pagedActiveServer serves active applies in pages of the given size from
// apply-1 on, echoing starting_after as a paging server does, and records each
// request's starting_after. The matching apply is the one named busy; every
// other apply belongs to another database, as a server that ignores the
// database filter would list them. Every page must ask for the active applies
// of database orders in staging, on the given deployment.
func pagedActiveServer(t *testing.T, total, pageSize int, busy, deployment string, requests *[]string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		assert.Equal(t, "true", query.Get("active"), "every page asks only for active applies")
		assert.Equal(t, "orders", query.Get("database"), "every page is narrowed to the database")
		assert.Equal(t, "staging", query.Get("environment"))
		assert.Equal(t, deployment, query.Get("deployment"), "every page keeps the deployment filter")
		after := query.Get("starting_after")
		*requests = append(*requests, after)
		start := 0
		if after != "" {
			_, err := fmt.Sscanf(after, "apply-%d", &start)
			assert.NoError(t, err)
		}
		resp := apitypes.StatusResponse{Limit: pageSize, StartingAfter: after}
		for n := start + 1; n <= total && len(resp.Applies) < pageSize; n++ {
			id := fmt.Sprintf("apply-%d", n)
			database := "billing"
			if id == busy {
				database = "orders"
			}
			resp.Applies = append(resp.Applies, &apitypes.ActiveApplyResponse{ApplyID: id, Database: database, Environment: "staging", State: "running"})
		}
		resp.HasMore = start+len(resp.Applies) < total
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	t.Cleanup(server.Close)
	return server
}

// The preflight keeps reading while the server reports more active applies, so
// a busy database whose apply is not on the first page is still found.
func TestCheckActiveSchemaChangeFindsAnApplyOnALaterPage(t *testing.T) {
	var requests []string
	server := pagedActiveServer(t, 5, 2, "apply-5", "", &requests)

	active, err := CheckActiveSchemaChange(server.URL, "orders", "staging")
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.Equal(t, "apply-5", active.ApplyID)
	assert.Equal(t, "running", active.State)
	assert.Equal(t, []string{"", "apply-2", "apply-4"}, requests, "each page continues after the last apply of the one before")
}

// A preflight scoped to one deployment keeps that filter on every page it
// reads, so a later page lists the same deployment's applies.
func TestCheckActiveSchemaChangeOnDeploymentPagesWithTheDeploymentFilter(t *testing.T) {
	var requests []string
	server := pagedActiveServer(t, 5, 2, "apply-5", "eu", &requests)

	active, err := CheckActiveSchemaChangeOnDeployment(server.URL, "orders", "staging", "EU")
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.Equal(t, "apply-5", active.ApplyID)
	assert.Equal(t, []string{"", "apply-2", "apply-4"}, requests)
}

// A server that echoes the anchor but serves the same page again would keep the
// preflight asking for it until the page bound. The repeat is an error as soon
// as it is seen.
func TestCheckActiveSchemaChangeErrorsWhenAPageRepeats(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(apitypes.StatusResponse{
			StartingAfter: r.URL.Query().Get("starting_after"),
			HasMore:       true,
			Applies:       []*apitypes.ActiveApplyResponse{{ApplyID: "apply-1", Database: "billing", Environment: "staging", State: "running"}},
		}))
	}))
	t.Cleanup(server.Close)

	active, err := CheckActiveSchemaChange(server.URL, "orders", "staging")
	require.Error(t, err)
	assert.Nil(t, active)
	assert.Contains(t, err.Error(), "served the page ending at apply apply-1 again")
	assert.Equal(t, 2, requests, "the repeat is caught on the second page")
}

// Having read every page without finding the database is the only way the
// preflight reports it idle.
func TestCheckActiveSchemaChangeReportsIdleOnlyAfterReadingEveryPage(t *testing.T) {
	var requests []string
	server := pagedActiveServer(t, 5, 2, "", "", &requests)

	active, err := CheckActiveSchemaChange(server.URL, "orders", "staging")
	require.NoError(t, err)
	assert.Nil(t, active)
	assert.Equal(t, []string{"", "apply-2", "apply-4"}, requests)
}

// A server that predates paging ignores starting_after and serves its first page
// again. That page is not the one after the anchor, so the preflight errors
// rather than reporting a database idle that it never saw the rest of.
func TestCheckActiveSchemaChangeErrorsWhenTheServerIgnoresPaging(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"has_more":true,"applies":[` +
			`{"apply_id":"apply-1","database":"billing","environment":"staging","state":"running"}]}`))
		assert.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	active, err := CheckActiveSchemaChange(server.URL, "orders", "staging")
	require.Error(t, err)
	assert.Nil(t, active)
	assert.Contains(t, err.Error(), "the server did not continue after apply apply-1")
}

// A page that reports more applies but lists none gives nothing to continue
// from, which is an error and not an idle database.
func TestCheckActiveSchemaChangeErrorsWhenAMorePageIsEmpty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"has_more":true,"applies":[]}`))
		assert.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	active, err := CheckActiveSchemaChange(server.URL, "orders", "staging")
	require.Error(t, err)
	assert.Nil(t, active)
	assert.Contains(t, err.Error(), "listed none to continue from")
}

// The page bound ends an endless listing with an error rather than a silent
// "no active schema change".
func TestCheckActiveSchemaChangeErrorsAtThePageBound(t *testing.T) {
	var requests []string
	server := pagedActiveServer(t, maxActiveSchemaChangePages+5, 1, "", "", &requests)

	active, err := CheckActiveSchemaChange(server.URL, "orders", "staging")
	require.Error(t, err)
	assert.Nil(t, active)
	assert.Contains(t, err.Error(), "stopped without reading them all")
	assert.Len(t, requests, maxActiveSchemaChangePages)
}

func TestReadSchemaFiles_RegularDirectories(t *testing.T) {
	dir := t.TempDir()

	// Create two keyspace subdirectories with .sql files
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "ks_unsharded"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ks_unsharded", "users.sql"), []byte("CREATE TABLE users (id INT)"), 0o644))

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "ks_sharded"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ks_sharded", "orders.sql"), []byte("CREATE TABLE orders (id INT)"), 0o644))

	result, _, err := ReadSchemaFiles(dir, "", nil)
	require.NoError(t, err)

	require.Contains(t, result, "ks_unsharded")
	require.Contains(t, result, "ks_sharded")
	assert.Equal(t, "CREATE TABLE users (id INT)", result["ks_unsharded"].Files["users.sql"])
	assert.Equal(t, "CREATE TABLE orders (id INT)", result["ks_sharded"].Files["orders.sql"])
}

func TestReadSchemaFiles_SymlinkedDirectories(t *testing.T) {
	dir := t.TempDir()

	// Create a real keyspace directory
	realDir := filepath.Join(dir, "ks_real")
	require.NoError(t, os.MkdirAll(realDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(realDir, "users.sql"), []byte("CREATE TABLE users (id INT)"), 0o644))

	// Create a symlink to the real directory (simulates shared schema across keyspaces)
	symlinkDir := filepath.Join(dir, "ks_symlink")
	require.NoError(t, os.Symlink(realDir, symlinkDir))

	result, _, err := ReadSchemaFiles(dir, "", nil)
	require.NoError(t, err)

	// Both the real directory and the symlink should be read
	require.Contains(t, result, "ks_real", "real directory should be read")
	require.Contains(t, result, "ks_symlink", "symlinked directory should be read")
	assert.Equal(t, "CREATE TABLE users (id INT)", result["ks_real"].Files["users.sql"])
	assert.Equal(t, "CREATE TABLE users (id INT)", result["ks_symlink"].Files["users.sql"])
}

func TestReadSchemaFiles_MixedRealAndSymlinked(t *testing.T) {
	dir := t.TempDir()

	// Real keyspace with its own schema
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "commerce"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "commerce", "settings.sql"), []byte("CREATE TABLE settings (id INT)"), 0o644))

	// Real sharded keyspace
	shardedDir := filepath.Join(dir, "commerce_sharded")
	require.NoError(t, os.MkdirAll(shardedDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(shardedDir, "orders.sql"), []byte("CREATE TABLE orders (id INT)"), 0o644))

	// Two more sharded keyspaces sharing the same schema via symlinks
	require.NoError(t, os.Symlink(shardedDir, filepath.Join(dir, "commerce_sharded_001")))
	require.NoError(t, os.Symlink(shardedDir, filepath.Join(dir, "commerce_sharded_002")))

	result, _, err := ReadSchemaFiles(dir, "", nil)
	require.NoError(t, err)

	// All four keyspaces should be present
	require.Len(t, result, 4)
	require.Contains(t, result, "commerce")
	require.Contains(t, result, "commerce_sharded")
	require.Contains(t, result, "commerce_sharded_001")
	require.Contains(t, result, "commerce_sharded_002")

	// Symlinked keyspaces should have the same content as the real one
	assert.Equal(t, result["commerce_sharded"].Files["orders.sql"], result["commerce_sharded_001"].Files["orders.sql"])
	assert.Equal(t, result["commerce_sharded"].Files["orders.sql"], result["commerce_sharded_002"].Files["orders.sql"])
}

func TestReadSchemaFiles_SkipsNonSchemaFiles(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "mydb"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mydb", "users.sql"), []byte("CREATE TABLE users (id INT)"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mydb", "README.md"), []byte("ignore me"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mydb", "vschema.json"), []byte("{}"), 0o644))

	result, _, err := ReadSchemaFiles(dir, "", nil)
	require.NoError(t, err)

	require.Contains(t, result, "mydb")
	assert.Contains(t, result["mydb"].Files, "users.sql")
	assert.Contains(t, result["mydb"].Files, "vschema.json")
	assert.NotContains(t, result["mydb"].Files, "README.md")
}

func TestReadSchemaFiles_IgnoreNamespaces(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "ks_unsharded"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ks_unsharded", "users.sql"), []byte("CREATE TABLE users (id INT)"), 0o644))

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "local_fixtures"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "local_fixtures", "widgets.sql"), []byte("CREATE TABLE widgets (id INT)"), 0o644))

	result, removed, err := ReadSchemaFiles(dir, "", []string{"local_fixtures"})
	require.NoError(t, err)

	require.Contains(t, result, "ks_unsharded")
	assert.NotContains(t, result, "local_fixtures")
	assert.Equal(t, []string{"local_fixtures"}, removed)
}

// A flat-layout schema directory has exactly one namespace — the directory
// name itself — so ignoring it removes every schema file. The plan call must
// fail with an error naming the exclusion rather than planning an empty
// desired state, which on a live database would read as "drop everything".
func TestCallPlanAPI_IgnoreSoleFlatNamespace(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "orders")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "users.sql"), []byte("CREATE TABLE users (id INT)"), 0o644))

	_, ignored, err := CallPlanAPI("http://unreachable.invalid", "orders", "mysql", "development", dir, "", 0, PlanExclusions{Namespaces: []string{"orders"}}, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "after excluding ignored namespaces")
	assert.Contains(t, err.Error(), "orders")
	assert.Equal(t, []string{"orders"}, ignored)
}

// The namespaces removed by ignore_namespaces must reach the server on the
// plan request: their files are already absent from schema_files, and the
// server needs the ignored list to refuse engine shapes that cannot honor
// the exclusion.
func TestCallPlanAPI_SendsIgnoredNamespaces(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "payments"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "payments", "users.sql"), []byte("CREATE TABLE users (id INT)"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "local_fixtures"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "local_fixtures", "widgets.sql"), []byte("CREATE TABLE widgets (id INT)"), 0o644))

	var gotReq apitypes.PlanRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/plan", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotReq))

		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(apitypes.PlanResponse{}))
	}))
	t.Cleanup(server.Close)

	_, ignored, err := CallPlanAPI(server.URL, "orders", "mysql", "development", dir, "", 0, PlanExclusions{Namespaces: []string{"local_fixtures"}}, false)
	require.NoError(t, err)

	assert.Equal(t, []string{"local_fixtures"}, ignored)
	assert.Equal(t, []string{"local_fixtures"}, gotReq.IgnoredNamespaces)
	require.Contains(t, gotReq.SchemaFiles, "payments")
	assert.NotContains(t, gotReq.SchemaFiles, "local_fixtures")
}

// ignore_tables cannot be expressed by omission the way ignore_namespaces can
// — the tables live on the target, not in the repository — so the entries have
// to reach the server on the request itself or the plan proposes dropping the
// very tables the config withholds.
func TestCallPlanAPI_SendsIgnoreTables(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "payments"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "payments", "users.sql"), []byte("CREATE TABLE users (id INT)"), 0o644))

	var gotReq apitypes.PlanRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotReq))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(apitypes.PlanResponse{}))
	}))
	t.Cleanup(server.Close)

	_, _, err := CallPlanAPI(server.URL, "orders", "mysql", "development", dir, "", 0,
		PlanExclusions{Tables: []string{"flyway_schema_history", "legacy_audit_log"}}, false)
	require.NoError(t, err)

	assert.Equal(t, []string{"flyway_schema_history", "legacy_audit_log"}, gotReq.IgnoreTables)
	assert.Contains(t, gotReq.SchemaFiles, "payments",
		"withholding a live table removes nothing from the declared schema")
}

// drop_tables reaches the server only on the request: the planner skips the
// listed tables by name, so a request without the entries prints a clean plan
// for a config that asks for a drop. A drop is not an exclusion, so the
// entries never spill into ignore_tables.
func TestCallPlanAPI_SendsDropTables(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "payments"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "payments", "users.sql"), []byte("CREATE TABLE users (id INT)"), 0o644))

	var gotReq apitypes.PlanRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotReq))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(apitypes.PlanResponse{}))
	}))
	t.Cleanup(server.Close)

	_, _, err := CallPlanAPI(server.URL, "orders", "mysql", "development", dir, "", 0,
		PlanExclusions{DropTables: []string{"_users_old"}}, false)
	require.NoError(t, err)

	assert.Equal(t, []string{"_users_old"}, gotReq.DropTables)
	assert.Empty(t, gotReq.IgnoreTables, "a drop is not an exclusion")
}

// A plan made for an apply that will hand the engine every ALTER at once has
// to say so on the request. The grouping decides what the engine predicts
// about a copy already on the target, so a plan that leaves it off describes a
// different apply than the one the operator is about to run.
func TestCallPlanAPI_SendsGroupedExecution(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "payments"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "payments", "users.sql"), []byte("CREATE TABLE users (id INT)"), 0o644))

	var gotReq apitypes.PlanRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotReq))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(apitypes.PlanResponse{}))
	}))
	t.Cleanup(server.Close)

	_, _, err := CallPlanAPI(server.URL, "orders", "mysql", "development", dir, "", 0, PlanExclusions{}, true)
	require.NoError(t, err)
	assert.True(t, gotReq.GroupedExecution, "the grouping the apply will use reaches the server")

	gotReq = apitypes.PlanRequest{}
	_, _, err = CallPlanAPI(server.URL, "orders", "mysql", "development", dir, "", 0, PlanExclusions{}, false)
	require.NoError(t, err)
	assert.False(t, gotReq.GroupedExecution, "a caller that has not chosen leaves the plan on the ungrouped default")
}

// The server refuses a rollout-wide plan or apply of an environment with more
// than one member unless the caller says it reads every member's plan. Each
// helper says so only when its caller decides it: the helpers that take the
// statement send exactly what they are given, and every other helper says it
// does not, so a caller that never reads the rollout is refused rather than
// shown the primary's plan as the whole rollout's.
func TestPlanAndApplyHelpers_SendRendersRolloutOnlyWhenTheCallerSaysSo(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "payments"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "payments", "users.sql"), []byte("CREATE TABLE users (id INT)"), 0o644))
	files := map[string]*apitypes.SchemaFiles{"payments": {Files: map[string]string{"users.sql": "CREATE TABLE users (id INT)"}}}

	var sent struct {
		path           string
		rendersRollout bool
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			RendersRollout bool `json:"renders_rollout"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		sent.path, sent.rendersRollout = r.URL.Path, body.RendersRollout
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/apply" {
			require.NoError(t, json.NewEncoder(w).Encode(apitypes.ApplyResponse{Accepted: true}))
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(apitypes.PlanResponse{}))
	}))
	t.Cleanup(server.Close)

	for _, tc := range []struct {
		name     string
		call     func() error
		wantPath string
		want     bool
	}{
		{"CallPlanAPI", func() error {
			_, _, err := CallPlanAPI(server.URL, "orders", "mysql", "development", dir, "", 0, PlanExclusions{}, false)
			return err
		}, "/api/plan", false},
		{"CallPlanAPIWithFiles", func() error {
			_, err := CallPlanAPIWithFiles(server.URL, "orders", "mysql", "development", files, "", 0)
			return err
		}, "/api/plan", false},
		{"CallPlanAPIForTarget for a caller that renders the rollout", func() error {
			_, _, err := CallPlanAPIForTarget(server.URL, "orders", "mysql", "development", dir, "", 0, PlanExclusions{}, false, "", true)
			return err
		}, "/api/plan", true},
		{"CallPlanAPIForTarget for a caller that does not", func() error {
			_, _, err := CallPlanAPIForTarget(server.URL, "orders", "mysql", "development", dir, "", 0, PlanExclusions{}, false, "", false)
			return err
		}, "/api/plan", false},
		{"CallPlanAPIWithContext for a caller that renders the rollout", func() error {
			_, _, err := CallPlanAPIWithContext(t.Context(), server.URL, "orders", "mysql", "development", dir, "", 0, PlanExclusions{}, false, true)
			return err
		}, "/api/plan", true},
		{"CallPlanAPIWithContext for a caller that does not", func() error {
			_, _, err := CallPlanAPIWithContext(t.Context(), server.URL, "orders", "mysql", "development", dir, "", 0, PlanExclusions{}, false, false)
			return err
		}, "/api/plan", false},
		{"CallApplyAPI", func() error {
			_, err := CallApplyAPI(server.URL, "plan-1", "development", "", nil)
			return err
		}, "/api/apply", false},
		{"CallApplyAPIForTarget for a caller that renders the rollout", func() error {
			_, err := CallApplyAPIForTarget(server.URL, "plan-1", "development", "", "", true, nil)
			return err
		}, "/api/apply", true},
		{"CallApplyAPIForTarget for a caller that does not", func() error {
			_, err := CallApplyAPIForTarget(server.URL, "plan-1", "development", "", "", false, nil)
			return err
		}, "/api/apply", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sent.path, sent.rendersRollout = "", !tc.want
			require.NoError(t, tc.call())
			assert.Equal(t, tc.wantPath, sent.path)
			assert.Equal(t, tc.want, sent.rendersRollout)
		})
	}
}

// A CLI owner is the ownership token the lock API matches byte-exactly against
// the folded spelling it stored at acquire. An operator on a host whose name
// carries uppercase characters must still recognize their own lock, so the
// owner is generated already folded.
func TestGenerateCLIOwnerIsCanonical(t *testing.T) {
	t.Run("uppercase hostname and username fold", func(t *testing.T) {
		owner := cliOwner("JDoe", "MacBook.local")
		assert.Equal(t, "cli:jdoe@macbook.local", owner)
		assert.Equal(t, storage.CanonicalKey(owner), owner, "owner matches the spelling the lock API stores")

		user, host, ok := caller.SplitCLI(owner)
		require.True(t, ok, "folding keeps the owner CLI-shaped")
		assert.Equal(t, "jdoe", user)
		assert.Equal(t, "macbook.local", host)
	})

	t.Run("generated owner is already folded", func(t *testing.T) {
		owner := GenerateCLIOwner()
		assert.Equal(t, storage.CanonicalKey(owner), owner)
	})
}

// A server that predates scoped reads ignores target and shard and serves the
// whole apply's log. The client refuses that answer rather than present every
// target's or shard's entries as the one's history, and still accepts an
// unscoped read from the same server.
func TestScopedLogReadRefusesAnUnscopedAnswer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"apply_id":"apply-a","logs":[],"sources":[],"errors":[]}`))
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	_, err := GetLogs(server.URL, "", "", "apply-a", LogScope{Target: "shop-002"}, 10)
	require.ErrorContains(t, err, "does not support reading one target's logs")
	_, err = GetDeploymentLogs(server.URL, "apply-a", "region-a", LogScope{Target: "shop-002"}, 10)
	require.ErrorContains(t, err, "does not support reading one target's logs")
	_, err = GetLogs(server.URL, "", "", "apply-a", LogScope{Shard: "-80"}, 10)
	require.ErrorContains(t, err, "does not support reading one shard's logs and returned the whole apply's; upgrade the server or omit --shard")
	_, err = GetDeploymentLogs(server.URL, "apply-a", "region-a", LogScope{Shard: "-80"}, 10)
	require.ErrorContains(t, err, "does not support reading one shard's logs and returned the whole apply's; upgrade the server or omit --shard")

	_, err = GetLogs(server.URL, "", "", "apply-a", LogScope{}, 10)
	require.NoError(t, err)
	_, err = GetDeploymentLogs(server.URL, "apply-a", "region-a", LogScope{}, 10)
	require.NoError(t, err)
}

// A server that answers a shard read with another shard's log is refused too,
// naming both shards, rather than shown as the requested shard's history.
func TestScopedLogReadRefusesAnotherShardsAnswer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"apply_id":"apply-a","shard":"80-","logs":[],"sources":[],"errors":[]}`))
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	_, err := GetLogs(server.URL, "", "", "apply-a", LogScope{Shard: "-80"}, 10)
	require.EqualError(t, err, `the server answered a read of shard "-80" with shard "80-"'s logs`)
	_, err = GetDeploymentLogs(server.URL, "apply-a", "region-a", LogScope{Shard: "-80"}, 10)
	require.EqualError(t, err, `the server answered a read of shard "-80" with shard "80-"'s logs`)
}

// Cancelling the context abandons a log read that is still waiting on the
// server, so a followed tail ends as soon as the operator interrupts it rather
// than when the server answers or the request times out.
func TestLogReadsStopWhenTheContextIsCancelled(t *testing.T) {
	// cancelDeadline bounds how long a cancelled read may take to return. The
	// server never answers on its own, so a read that ignores the context
	// would wait for the client's request timeout instead.
	const cancelDeadline = 5 * time.Second

	reads := map[string]func(ctx context.Context, endpoint string) error{
		"GetLogsCtx": func(ctx context.Context, endpoint string) error {
			_, err := GetLogsCtx(ctx, endpoint, "", "", "apply-a", LogScope{}, 10)
			return err
		},
		"GetDeploymentLogsCtx": func(ctx context.Context, endpoint string) error {
			_, err := GetDeploymentLogsCtx(ctx, endpoint, "apply-a", "region-a", LogScope{}, 10)
			return err
		},
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			arrived := make(chan struct{}, 1)
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				arrived <- struct{}{}
				select {
				case <-r.Context().Done():
				case <-release:
				}
			}))
			t.Cleanup(server.Close)
			// Cleanups run last-registered first, so the handler is released
			// before the server waits for it to return.
			t.Cleanup(func() { close(release) })

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- read(ctx, server.URL) }()

			select {
			case <-arrived:
			case <-time.After(cancelDeadline):
				require.FailNow(t, "the log read never reached the server")
			}
			cancel()

			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(cancelDeadline):
				require.FailNow(t, "the log read kept waiting on the server after its context was cancelled")
			}
		})
	}
}
