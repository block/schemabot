//go:build integration

package localscale_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/localscale"
	"github.com/block/schemabot/pkg/psclient"
)

const (
	testOrg = "test-org"
	testDB  = "testdb"
)

var (
	testContainer *localscale.LocalScaleContainer
	testClient    psclient.PSClient
	testLogger    *slog.Logger
)

func TestMain(m *testing.M) {
	testLogger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

	var err error
	testContainer, err = localscale.RunContainer(ctx, localscale.ContainerConfig{
		Orgs: map[string]localscale.ContainerOrgConfig{
			testOrg: {Databases: map[string]localscale.ContainerDatabaseConfig{
				testDB: {Keyspaces: []localscale.ContainerKeyspaceConfig{
					{Name: "testapp", Shards: 1},
					{Name: "testapp_sharded", Shards: 2},
				}},
			}},
		},
		Reuse: os.Getenv("DEBUG") == "1",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start LocalScale container: %v\n", err)
		cancel()
		os.Exit(1)
	}

	// Create real PS SDK client pointing at our container.
	testClient, err = psclient.NewPSClientWithBaseURL("test", "test", testContainer.URL())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create PS client: %v\n", err)
		cancel()
		os.Exit(1)
	}

	// Seed initial schema via admin endpoints
	if err := seedInitialSchema(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to seed initial schema: %v\n", err)
		cancel()
		os.Exit(1)
	}

	code := m.Run()
	if os.Getenv("DEBUG") != "1" {
		_ = testContainer.Terminate(ctx)
	}
	cancel()
	os.Exit(code)
}

func seedInitialSchema(ctx context.Context) error {
	// Seed schema from testdata directory (VSchema first, then DDL per keyspace).
	if err := testContainer.SchemaDir(ctx, testOrg, testDB, "testdata/schema"); err != nil {
		return fmt.Errorf("seed schema dir: %w", err)
	}

	// Seed sequence data via vtgate exec (DML, no DDL strategy needed)
	for _, seq := range []string{"users_seq", "orders_seq", "products_seq"} {
		if _, err := testContainer.VtgateExec(ctx, testOrg, testDB, "testapp",
			fmt.Sprintf("INSERT INTO %s (id, next_id, cache) VALUES (0, 1, 1000) ON DUPLICATE KEY UPDATE id=id", seq),
		); err != nil {
			return fmt.Errorf("seed %s: %w", seq, err)
		}
	}

	// Seed some data
	for i := 1; i <= 10; i++ {
		if _, err := testContainer.VtgateExec(ctx, testOrg, testDB, "testapp_sharded",
			"INSERT IGNORE INTO users (id, email, full_name) VALUES (?, ?, ?)",
			i, fmt.Sprintf("user%d@example.com", i), fmt.Sprintf("User %d", i),
		); err != nil {
			return fmt.Errorf("seed users: %w", err)
		}
		if _, err := testContainer.VtgateExec(ctx, testOrg, testDB, "testapp_sharded",
			"INSERT IGNORE INTO orders (id, user_id, total_cents, status) VALUES (?, ?, ?, 'pending')",
			i, i, i*1000,
		); err != nil {
			return fmt.Errorf("seed orders: %w", err)
		}
		if _, err := testContainer.VtgateExec(ctx, testOrg, testDB, "testapp_sharded",
			"INSERT IGNORE INTO products (id, name, description, price_cents, sku) VALUES (?, ?, ?, ?, ?)",
			i, fmt.Sprintf("Product %d", i), fmt.Sprintf("Description %d", i), i*500, fmt.Sprintf("SKU-%d", i),
		); err != nil {
			return fmt.Errorf("seed products: %w", err)
		}
	}

	testLogger.Info("seeded initial schema and data")
	return nil
}

func TestListKeyspaces(t *testing.T) {
	ctx := t.Context()

	keyspaces, err := testClient.ListKeyspaces(ctx, &ps.ListKeyspacesRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       "main",
	})
	require.NoError(t, err, "ListKeyspaces error")

	require.GreaterOrEqual(t, len(keyspaces), 2, "expected at least 2 keyspaces")

	names := map[string]bool{}
	for _, ks := range keyspaces {
		names[ks.Name] = true
	}
	if !names["testapp"] {
		assert.Fail(t, "expected keyspace 'testapp'")
	}
	if !names["testapp_sharded"] {
		assert.Fail(t, "expected keyspace 'testapp_sharded'")
	}
}

func TestGetBranchSchema(t *testing.T) {
	ctx := t.Context()

	// Query schema for testapp_sharded keyspace
	schema, err := testClient.GetBranchSchema(ctx, &ps.BranchSchemaRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       "main",
		Keyspace:     "testapp_sharded",
	})
	require.NoError(t, err, "GetBranchSchema error")

	require.GreaterOrEqual(t, len(schema), 3, "expected at least 3 tables (users, orders, products)")

	tableNames := map[string]bool{}
	for _, s := range schema {
		tableNames[s.Name] = true
		assert.NotEmpty(t, s.Raw, "table %s has empty CREATE statement", s.Name)
	}
	for _, expected := range []string{"users", "orders", "products"} {
		assert.True(t, tableNames[expected], "expected table %q in schema", expected)
	}

	// Also check testapp (unsharded) keyspace has sequence tables
	seqSchema, err := testClient.GetBranchSchema(ctx, &ps.BranchSchemaRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       "main",
		Keyspace:     "testapp",
	})
	require.NoError(t, err, "GetBranchSchema (testapp) error")

	seqNames := map[string]bool{}
	for _, s := range seqSchema {
		seqNames[s.Name] = true
	}
	for _, expected := range []string{"users_seq", "orders_seq", "products_seq"} {
		assert.True(t, seqNames[expected], "expected sequence table %q in testapp schema", expected)
	}
}

func TestBranchLifecycle(t *testing.T) {
	ctx := t.Context()

	// Get the default main branch
	main, err := testClient.GetBranch(ctx, &ps.GetDatabaseBranchRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       "main",
	})
	require.NoError(t, err, "GetBranch(main) error")
	if !main.Ready {
		assert.Fail(t, "expected main branch to be ready")
	}

	// Create a new branch
	branchName := fmt.Sprintf("test-branch-%d", time.Now().UnixNano())
	branch, err := testClient.CreateBranch(ctx, &ps.CreateDatabaseBranchRequest{
		Organization: testOrg,
		Database:     testDB,
		Name:         branchName,
		ParentBranch: "main",
		Region:       "us-east-1",
	})
	require.NoError(t, err, "CreateBranch error")
	assert.Equal(t, branchName, branch.Name, "branch name")
	if branch.Ready {
		assert.Fail(t, "expected new branch to not be ready immediately")
	}

	// Wait for branch to become ready
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		got, err := testClient.GetBranch(ctx, &ps.GetDatabaseBranchRequest{
			Organization: testOrg,
			Database:     testDB,
			Branch:       branchName,
		})
		require.NoError(t, err, "GetBranch error")
		if got.Ready {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Verify it's ready now
	got, err := testClient.GetBranch(ctx, &ps.GetDatabaseBranchRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       branchName,
	})
	require.NoError(t, err, "GetBranch error")
	if !got.Ready {
		assert.Fail(t, "expected branch to be ready after waiting")
	}

	// Creating a duplicate branch should fail
	_, err = testClient.CreateBranch(ctx, &ps.CreateDatabaseBranchRequest{
		Organization: testOrg,
		Database:     testDB,
		Name:         branchName,
		ParentBranch: "main",
	})
	assert.Error(t, err, "expected error creating duplicate branch")
}

func TestGetKeyspaceVSchema(t *testing.T) {
	ctx := t.Context()

	// Fetch VSchema for the sharded keyspace — we applied one in seedInitialSchema
	vs, err := testClient.GetKeyspaceVSchema(ctx, &ps.GetKeyspaceVSchemaRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       "main",
		Keyspace:     "testapp_sharded",
	})
	require.NoError(t, err, "GetKeyspaceVSchema error")
	require.NotNil(t, vs, "expected non-nil VSchema for testapp_sharded")
	require.NotEmpty(t, vs.Raw, "expected non-empty VSchema.Raw for testapp_sharded")

	// VSchema should mention "hash" vindex and "users" table
	assert.Contains(t, vs.Raw, "hash", "VSchema should contain 'hash' vindex")
	assert.Contains(t, vs.Raw, "users", "VSchema should contain 'users' table")

	t.Logf("VSchema for testapp_sharded: %s", vs.Raw[:min(len(vs.Raw), 200)])

	// Fetch VSchema for the unsharded keyspace
	unshardedVS, err := testClient.GetKeyspaceVSchema(ctx, &ps.GetKeyspaceVSchemaRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       "main",
		Keyspace:     "testapp",
	})
	require.NoError(t, err, "GetKeyspaceVSchema (testapp) error")
	require.NotNil(t, unshardedVS, "expected non-nil VSchema for testapp")
	require.NotEmpty(t, unshardedVS.Raw, "expected non-empty VSchema.Raw for testapp")
	assert.Contains(t, unshardedVS.Raw, "users_seq", "VSchema should contain 'users_seq'")

	// Non-existent keyspace should return empty or "{}"
	emptyVS, err := testClient.GetKeyspaceVSchema(ctx, &ps.GetKeyspaceVSchemaRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       "main",
		Keyspace:     "nonexistent",
	})
	require.NoError(t, err, "GetKeyspaceVSchema (nonexistent) error")
	assert.True(t, emptyVS == nil || emptyVS.Raw == "" || emptyVS.Raw == "{}", "expected empty VSchema for nonexistent keyspace, got: %v", emptyVS)
}

// waitForBranchReady polls until a branch is ready or the deadline is exceeded.
func waitForBranchReady(t *testing.T, ctx context.Context, branchName string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		got, err := testClient.GetBranch(ctx, &ps.GetDatabaseBranchRequest{
			Organization: testOrg,
			Database:     testDB,
			Branch:       branchName,
		})
		require.NoError(t, err, "GetBranch(%s)", branchName)
		if got.Ready {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.Failf(t, "branch not ready", "branch %q not ready after 15s", branchName)
}

// --- Test helpers ---

// createBranch creates a branch with a unique name from the given prefix,
// waits for it to become ready, and returns the branch name.
func createBranch(t *testing.T, ctx context.Context, prefix string) string {
	t.Helper()
	branchName := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	_, err := testClient.CreateBranch(ctx, &ps.CreateDatabaseBranchRequest{
		Organization: testOrg,
		Database:     testDB,
		Name:         branchName,
		ParentBranch: "main",
	})
	require.NoError(t, err, "CreateBranch")
	waitForBranchReady(t, ctx, branchName)
	return branchName
}

// applyBranchDDL applies DDL statements to a branch via MySQL connection
// (CreateBranchPassword → connect → execute DDL).
func applyBranchDDL(t *testing.T, ctx context.Context, branchName string, ddl map[string][]string) {
	t.Helper()
	pw, err := testClient.CreateBranchPassword(ctx, &ps.DatabaseBranchPasswordRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       branchName,
	})
	require.NoError(t, err, "CreateBranchPassword")

	for keyspace, stmts := range ddl {
		dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", pw.Username, pw.PlainText, pw.Hostname, keyspace)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err, "open branch MySQL for %s", keyspace)
		require.NoError(t, db.PingContext(ctx), "ping branch MySQL for %s", keyspace)
		for _, stmt := range stmts {
			_, err := db.ExecContext(ctx, stmt)
			require.NoError(t, err, "execute DDL in %s: %s", keyspace, stmt)
		}
		utils.CloseAndLog(db)
	}
}

// applyBranchVSchema applies VSchema changes to a branch via UpdateKeyspaceVSchema.
func applyBranchVSchema(t *testing.T, ctx context.Context, branchName string, vschema map[string]json.RawMessage) {
	t.Helper()
	for keyspace, vs := range vschema {
		_, err := testClient.UpdateKeyspaceVSchema(ctx, &ps.UpdateKeyspaceVSchemaRequest{
			Organization: testOrg,
			Database:     testDB,
			Branch:       branchName,
			Keyspace:     keyspace,
			VSchema:      string(vs),
		})
		require.NoError(t, err, "UpdateKeyspaceVSchema for %s", keyspace)
	}
}

// queryBranchVSchema queries the vschema_data for a branch from the metadata DB
// and returns it as a parsed map of keyspace → raw VSchema JSON.
func queryBranchVSchema(t *testing.T, ctx context.Context, branchName string) map[string]json.RawMessage {
	t.Helper()
	result, err := testContainer.MetadataQuery(ctx,
		"SELECT vschema_data FROM localscale_branches WHERE name = ?", branchName)
	require.NoError(t, err, "query vschema_data for branch %s", branchName)
	require.Greater(t, len(result.Rows), 0, "expected branch row for %s", branchName)
	vschemaStr, _ := result.Rows[0][0].(string)
	require.NotEmpty(t, vschemaStr, "expected non-empty vschema_data for branch %s", branchName)
	var vschemaMap map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(vschemaStr), &vschemaMap), "parse vschema_data for branch %s", branchName)
	return vschemaMap
}

// --- Branch database mechanics tests ---

// TestBranchDatabaseSchemaSnapshot verifies that CreateBranch creates real MySQL databases
// and snapshots schema from vtgate into them.
func TestBranchDatabaseSchemaSnapshot(t *testing.T) {
	ctx := t.Context()
	branchName := createBranch(t, ctx, "snapshot")

	// Verify sharded keyspace branch DB has the same tables as vtgate
	result, err := testContainer.BranchDBQuery(ctx, branchName, "testapp_sharded", "SHOW TABLES")
	require.NoError(t, err, "SHOW TABLES on branch sharded DB")
	shardedTables := map[string]bool{}
	for _, row := range result.Rows {
		if len(row) > 0 {
			if name, ok := row[0].(string); ok {
				shardedTables[name] = true
			}
		}
	}
	for _, expected := range []string{"users", "orders", "products"} {
		assert.True(t, shardedTables[expected], "expected table %q in branch sharded DB, got: %v", expected, shardedTables)
	}

	// Verify unsharded keyspace branch DB has sequence tables
	result, err = testContainer.BranchDBQuery(ctx, branchName, "testapp", "SHOW TABLES")
	require.NoError(t, err, "SHOW TABLES on branch unsharded DB")
	seqTables := map[string]bool{}
	for _, row := range result.Rows {
		if len(row) > 0 {
			if name, ok := row[0].(string); ok {
				seqTables[name] = true
			}
		}
	}
	for _, expected := range []string{"users_seq", "orders_seq", "products_seq"} {
		assert.True(t, seqTables[expected], "expected sequence table %q in branch unsharded DB, got: %v", expected, seqTables)
	}

	// Verify VSchema was snapshotted on the branch row
	vschemaMap := queryBranchVSchema(t, ctx, branchName)
	assert.Contains(t, vschemaMap, "testapp", "expected VSchema data for 'testapp' keyspace")
	assert.Contains(t, vschemaMap, "testapp_sharded", "expected VSchema data for 'testapp_sharded' keyspace")
}

// TestBranchDDLExecution verifies that DDL applied via MySQL connection executes in branch databases.
func TestBranchDDLExecution(t *testing.T) {
	ctx := t.Context()
	branchName := createBranch(t, ctx, "ddl-exec")

	// Apply ALTER TABLE to add a column in the branch database
	applyBranchDDL(t, ctx, branchName, map[string][]string{
		"testapp_sharded": {"ALTER TABLE users ADD COLUMN branch_test_col varchar(100)"},
	})

	// Verify column exists in branch database (not in vtgate/main)
	result, err := testContainer.BranchDBQuery(ctx, branchName, "testapp_sharded",
		"SHOW COLUMNS FROM users LIKE 'branch_test_col'")
	require.NoError(t, err, "SHOW COLUMNS")
	require.Greater(t, len(result.Rows), 0, "expected 'branch_test_col' column in branch DB after ALTER TABLE")

	// Apply CREATE TABLE for a new table
	applyBranchDDL(t, ctx, branchName, map[string][]string{
		"testapp_sharded": {
			"CREATE TABLE branch_new_table (id bigint NOT NULL PRIMARY KEY, name varchar(255))",
		},
	})

	// Verify new table exists in branch database
	result, err = testContainer.BranchDBQuery(ctx, branchName, "testapp_sharded",
		"SHOW TABLES LIKE 'branch_new_table'")
	require.NoError(t, err, "SHOW TABLES")
	require.Greater(t, len(result.Rows), 0, "expected 'branch_new_table' in branch DB after CREATE TABLE")
}

// TestListKeyspacesShardCounts verifies that ListKeyspaces returns the correct
// shard count and sharded flag from the config rather than hardcoding Shards=1.
func TestListKeyspacesShardCounts(t *testing.T) {
	ctx := t.Context()

	keyspaces, err := testClient.ListKeyspaces(ctx, &ps.ListKeyspacesRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       "main",
	})
	require.NoError(t, err, "ListKeyspaces error")
	require.GreaterOrEqual(t, len(keyspaces), 2)

	byName := make(map[string]*ps.Keyspace, len(keyspaces))
	for _, ks := range keyspaces {
		byName[ks.Name] = ks
	}

	// testapp: 1 shard, unsharded
	testappKs, ok := byName["testapp"]
	require.True(t, ok, "expected keyspace 'testapp'")
	assert.Equal(t, 1, testappKs.Shards, "testapp should have 1 shard")
	assert.False(t, testappKs.Sharded, "testapp should not be sharded")

	// testapp_sharded: 2 shards, sharded
	shardedKs, ok := byName["testapp_sharded"]
	require.True(t, ok, "expected keyspace 'testapp_sharded'")
	assert.Equal(t, 2, shardedKs.Shards, "testapp_sharded should have 2 shards")
	assert.True(t, shardedKs.Sharded, "testapp_sharded should be sharded")
}

// TestGetBranchSchemaOnNonMainBranch verifies that GetBranchSchema returns
// branch-specific schema (not main) for non-main branches.
func TestGetBranchSchemaOnNonMainBranch(t *testing.T) {
	ctx := t.Context()
	branchName := createBranch(t, ctx, "schema-branch")

	// Apply DDL to add a column and a table only in the branch
	applyBranchDDL(t, ctx, branchName, map[string][]string{
		"testapp_sharded": {
			"ALTER TABLE users ADD COLUMN branch_only_col varchar(100)",
			"CREATE TABLE branch_only_table (id bigint NOT NULL PRIMARY KEY, val text)",
		},
	})

	// Get schema for the branch — should reflect the DDL changes
	branchSchema, err := testClient.GetBranchSchema(ctx, &ps.BranchSchemaRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       branchName,
		Keyspace:     "testapp_sharded",
	})
	require.NoError(t, err, "GetBranchSchema (branch)")

	branchTableNames := make(map[string]bool, len(branchSchema))
	for _, s := range branchSchema {
		branchTableNames[s.Name] = true
	}
	assert.True(t, branchTableNames["branch_only_table"], "branch schema should include branch_only_table")

	// Verify the ALTER TABLE column exists in the branch schema for users table
	for _, s := range branchSchema {
		if s.Name == "users" {
			assert.Contains(t, s.Raw, "branch_only_col", "branch users table should contain branch_only_col")
		}
	}

	// Get schema for main — should NOT have the branch-only changes
	mainSchema, err := testClient.GetBranchSchema(ctx, &ps.BranchSchemaRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       "main",
		Keyspace:     "testapp_sharded",
	})
	require.NoError(t, err, "GetBranchSchema (main)")

	mainTableNames := make(map[string]bool, len(mainSchema))
	for _, s := range mainSchema {
		mainTableNames[s.Name] = true
	}
	assert.False(t, mainTableNames["branch_only_table"], "main schema should NOT include branch_only_table")

	for _, s := range mainSchema {
		if s.Name == "users" {
			assert.NotContains(t, s.Raw, "branch_only_col", "main users table should NOT contain branch_only_col")
		}
	}
}

// TestGetKeyspaceVSchemaOnNonMainBranch verifies that GetKeyspaceVSchema returns
// branch-specific VSchema (from the branch row) for non-main branches.
func TestGetKeyspaceVSchemaOnNonMainBranch(t *testing.T) {
	ctx := t.Context()
	branchName := createBranch(t, ctx, "vschema-branch")

	// Apply a VSchema change to the branch — add a custom vindex
	customVSchema := json.RawMessage(`{
		"sharded": true,
		"vindexes": {
			"hash": {"type": "hash"},
			"branch_test_vdx": {"type": "hash"}
		},
		"tables": {
			"users": {
				"column_vindexes": [{"column": "id", "name": "hash"}]
			}
		}
	}`)
	applyBranchVSchema(t, ctx, branchName, map[string]json.RawMessage{"testapp_sharded": customVSchema})

	// Get VSchema for the branch — should include the custom vindex
	branchVS, err := testClient.GetKeyspaceVSchema(ctx, &ps.GetKeyspaceVSchemaRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       branchName,
		Keyspace:     "testapp_sharded",
	})
	require.NoError(t, err, "GetKeyspaceVSchema (branch)")
	require.NotNil(t, branchVS, "expected non-nil VSchema for branch")
	assert.Contains(t, branchVS.Raw, "branch_test_vdx", "branch VSchema should contain 'branch_test_vdx'")

	// Get VSchema for main — should NOT have the custom vindex
	mainVS, err := testClient.GetKeyspaceVSchema(ctx, &ps.GetKeyspaceVSchemaRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       "main",
		Keyspace:     "testapp_sharded",
	})
	require.NoError(t, err, "GetKeyspaceVSchema (main)")
	require.NotNil(t, mainVS, "expected non-nil VSchema for main")
	assert.NotContains(t, mainVS.Raw, "branch_test_vdx", "main VSchema should NOT contain 'branch_test_vdx'")
}
