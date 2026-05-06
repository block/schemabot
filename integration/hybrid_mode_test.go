//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	schemabotapi "github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// TestHybridMode_LocalAndGRPC verifies that a single SchemaBot Service instance
// can route requests to both a LocalClient (for databases registered in the
// "databases" config section) and a GRPCClient (for databases routed via
// "tern_deployments") simultaneously.
//
// Setup:
//   - "local-db" is registered in config.Databases with a real MySQL testcontainer
//   - "grpc-db" is routed via config.TernDeployments to an in-process gRPC server
//     backed by a separate LocalClient (simulating a remote Tern service)
//   - Both use the same SchemaBot storage instance
//
// The test verifies:
//  1. A plan for "local-db" goes through the LocalClient path
//  2. A plan for "grpc-db" goes through the GRPCClient path
//  3. An apply for "local-db" creates local apply records (no external_id)
//  4. An apply for "grpc-db" creates apply records with external_id set
//  5. Both complete successfully from the same Service
func TestHybridMode_LocalAndGRPC(t *testing.T) {
	ctx := t.Context()

	// =========================================================================
	// Create two target databases: one for local mode, one for gRPC mode
	// =========================================================================

	targetDB, err := sql.Open("mysql", targetDSN+"&multiStatements=true")
	require.NoError(t, err, "open target db")
	t.Cleanup(func() { utils.CloseAndLog(targetDB) })

	localDBName := fmt.Sprintf("hybrid_local_%d", time.Now().UnixNano()%100000)
	grpcDBName := fmt.Sprintf("hybrid_grpc_%d", time.Now().UnixNano()%100000)

	_, err = targetDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+localDBName)
	require.NoError(t, err, "create local database")
	_, err = targetDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+grpcDBName)
	require.NoError(t, err, "create grpc database")

	t.Cleanup(func() {
		_, _ = targetDB.ExecContext(ctx, "DROP DATABASE IF EXISTS "+localDBName)
		_, _ = targetDB.ExecContext(ctx, "DROP DATABASE IF EXISTS "+grpcDBName)
	})

	baseCfg, err := mysql.ParseDSN(targetDSN)
	require.NoError(t, err, "parse target DSN")

	localCfg := baseCfg.Clone()
	localCfg.DBName = localDBName
	localDSN := localCfg.FormatDSN()

	grpcCfg := baseCfg.Clone()
	grpcCfg.DBName = grpcDBName
	grpcTargetDSN := grpcCfg.FormatDSN()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// =========================================================================
	// Set up gRPC Tern server for the gRPC-mode database
	// =========================================================================

	ternGRPCAddr, err := startTernGRPCForDB(t, grpcTargetDSN, grpcDBName)
	require.NoError(t, err, "start tern grpc for grpc-db")

	// =========================================================================
	// Set up SchemaBot with hybrid config: databases + tern_deployments
	// =========================================================================

	schemabotDB, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err, "open schemabot db")
	clearStorageDB(t, schemabotDB)
	schemabotStorage := mysqlstore.New(schemabotDB)
	t.Cleanup(func() { utils.CloseAndLog(schemabotDB) })

	// Create a GRPCClient for the gRPC deployment (with storage for polling)
	grpcClient, err := tern.NewGRPCClient(tern.Config{
		Address: ternGRPCAddr,
		Storage: schemabotStorage,
	})
	require.NoError(t, err, "create grpc client")
	t.Cleanup(func() { utils.CloseAndLog(grpcClient) })

	// Hybrid config: local database + gRPC deployment
	serverConfig := &schemabotapi.ServerConfig{
		Databases: map[string]schemabotapi.DatabaseConfig{
			localDBName: {
				Type: "mysql",
				Environments: map[string]schemabotapi.EnvironmentConfig{
					"staging": {DSN: localDSN},
				},
			},
		},
		TernDeployments: schemabotapi.TernConfig{
			"default": {"staging": ternGRPCAddr},
		},
	}

	// Validate that hybrid config passes validation
	require.NoError(t, serverConfig.Validate(), "hybrid config should be valid")

	// Pre-register the gRPC client for the default deployment.
	// The LocalClient for localDBName will be created lazily by TernClient().
	svc := schemabotapi.New(schemabotStorage, serverConfig, map[string]tern.Client{
		"default/staging": grpcClient,
	}, logger)
	t.Cleanup(func() { utils.CloseAndLog(svc) })

	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "localhost:0")
	require.NoError(t, err, "listen")
	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { utils.CloseAndLog(server) })

	addr := listener.Addr().String()

	// Wait for server readiness
	waitForHTTPReady(t, addr, 5*time.Second)

	// =========================================================================
	// Test 1: Plan for local-mode database (uses LocalClient)
	// =========================================================================

	localSchemaSQL := `CREATE TABLE local_items (
	id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`

	localPlanResp := postJSON(t, "http://"+addr+"/api/plan", map[string]any{
		"database":    localDBName,
		"environment": "staging",
		"type":        "mysql",
		"schema_files": map[string]any{
			"default": map[string]any{
				"files": map[string]string{
					"local_items.sql": localSchemaSQL,
				},
			},
		},
		"repository":   "test/hybrid-local",
		"pull_request": 1,
	})
	localPlanID, ok := localPlanResp["plan_id"].(string)
	require.True(t, ok && localPlanID != "", "local plan should return plan_id: %v", localPlanResp)
	t.Logf("Local plan ID: %s", localPlanID)

	// =========================================================================
	// Test 2: Plan for gRPC-mode database (uses GRPCClient)
	// =========================================================================

	grpcSchemaSQL := `CREATE TABLE grpc_items (
	id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	label VARCHAR(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`

	grpcPlanResp := postJSON(t, "http://"+addr+"/api/plan", map[string]any{
		"database":    grpcDBName,
		"environment": "staging",
		"type":        "mysql",
		"schema_files": map[string]any{
			"default": map[string]any{
				"files": map[string]string{
					"grpc_items.sql": grpcSchemaSQL,
				},
			},
		},
		"repository":   "test/hybrid-grpc",
		"pull_request": 2,
	})
	grpcPlanID, ok := grpcPlanResp["plan_id"].(string)
	require.True(t, ok && grpcPlanID != "", "grpc plan should return plan_id: %v", grpcPlanResp)
	t.Logf("gRPC plan ID: %s", grpcPlanID)

	// Both plans should have changes (CREATE TABLE)
	localChanges, _ := localPlanResp["changes"].([]any)
	grpcChanges, _ := grpcPlanResp["changes"].([]any)
	require.NotEmpty(t, localChanges, "local plan should have changes")
	require.NotEmpty(t, grpcChanges, "grpc plan should have changes")

	// =========================================================================
	// Test 3: Apply for local-mode database
	// =========================================================================

	localApplyResp := postJSON(t, "http://"+addr+"/api/apply", map[string]any{
		"plan_id":     localPlanID,
		"environment": "staging",
		"database":    localDBName,
	})
	require.Equal(t, true, localApplyResp["accepted"], "local apply not accepted: %v", localApplyResp)
	localApplyID, ok := localApplyResp["apply_id"].(string)
	require.True(t, ok && localApplyID != "", "local apply should return apply_id: %v", localApplyResp)
	t.Logf("Local apply ID: %s", localApplyID)

	// =========================================================================
	// Test 4: Apply for gRPC-mode database
	// =========================================================================

	grpcApplyResp := postJSON(t, "http://"+addr+"/api/apply", map[string]any{
		"plan_id":     grpcPlanID,
		"environment": "staging",
	})
	require.Equal(t, true, grpcApplyResp["accepted"], "grpc apply not accepted: %v", grpcApplyResp)
	grpcApplyID, ok := grpcApplyResp["apply_id"].(string)
	require.True(t, ok && grpcApplyID != "", "grpc apply should return apply_id: %v", grpcApplyResp)
	t.Logf("gRPC apply ID: %s", grpcApplyID)

	// =========================================================================
	// Test 5: Wait for both applies to complete
	// =========================================================================

	waitForState(t, "http://"+addr, localApplyID, "completed", 15*time.Second)
	waitForState(t, "http://"+addr, grpcApplyID, "completed", 15*time.Second)

	// =========================================================================
	// Test 6: Verify routing correctness via storage records
	// =========================================================================

	// Local apply: should NOT have external_id (LocalClient creates records directly)
	localApply, err := schemabotStorage.Applies().GetByApplyIdentifier(ctx, localApplyID)
	require.NoError(t, err, "get local apply")
	require.NotNil(t, localApply, "local apply not found")
	assert.Empty(t, localApply.ExternalID,
		"local-mode apply should NOT have external_id (LocalClient manages its own records)")
	assert.Equal(t, localDBName, localApply.Database,
		"local apply should be for the local database")
	assert.Equal(t, state.Apply.Completed, localApply.State,
		"local apply should be completed")
	t.Logf("Local apply: external_id=%q (empty = local mode confirmed)", localApply.ExternalID)

	// gRPC apply: SHOULD have external_id (GRPCClient stores Tern's ID)
	grpcApply, err := schemabotStorage.Applies().GetByApplyIdentifier(ctx, grpcApplyID)
	require.NoError(t, err, "get grpc apply")
	require.NotNil(t, grpcApply, "grpc apply not found")
	assert.NotEmpty(t, grpcApply.ExternalID,
		"gRPC-mode apply SHOULD have external_id (SchemaBot stores Tern's apply_id)")
	assert.NotEqual(t, grpcApplyID, grpcApply.ExternalID,
		"apply_identifier and external_id should differ in gRPC mode")
	assert.Equal(t, grpcDBName, grpcApply.Database,
		"grpc apply should be for the grpc database")
	t.Logf("gRPC apply: external_id=%q (non-empty = gRPC mode confirmed)", grpcApply.ExternalID)

	// Wait for pollForCompletion to sync gRPC apply state to storage
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		grpcApply, err = schemabotStorage.Applies().GetByApplyIdentifier(ctx, grpcApplyID)
		require.NoError(t, err, "refresh grpc apply")
		if grpcApply.State == state.Apply.Completed {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.Equal(t, state.Apply.Completed, grpcApply.State,
		"gRPC apply should be completed in local storage")

	// =========================================================================
	// Test 7: Verify tasks were created for both applies
	// =========================================================================

	localTasks, err := schemabotStorage.Tasks().GetByApplyID(ctx, localApply.ID)
	require.NoError(t, err, "get local tasks")
	require.NotEmpty(t, localTasks, "local apply should have tasks")
	for _, task := range localTasks {
		assert.Equal(t, state.Task.Completed, task.State,
			"local task %s should be completed", task.TaskIdentifier)
	}

	grpcTasks, err := schemabotStorage.Tasks().GetByApplyID(ctx, grpcApply.ID)
	require.NoError(t, err, "get grpc tasks")
	require.NotEmpty(t, grpcTasks, "gRPC apply should have tasks")
	for _, task := range grpcTasks {
		assert.Equal(t, state.Task.Completed, task.State,
			"gRPC task %s should be completed", task.TaskIdentifier)
	}

	// =========================================================================
	// Test 8: Verify the actual tables exist in their respective databases
	// =========================================================================

	localDB, err := sql.Open("mysql", localDSN)
	require.NoError(t, err, "open local target db")
	t.Cleanup(func() { utils.CloseAndLog(localDB) })

	var localTableName string
	err = localDB.QueryRowContext(ctx, "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'local_items'", localDBName).Scan(&localTableName)
	require.NoError(t, err, "local_items table should exist in local database")
	assert.Equal(t, "local_items", localTableName)

	grpcTargetDB, err := sql.Open("mysql", grpcTargetDSN)
	require.NoError(t, err, "open grpc target db")
	t.Cleanup(func() { utils.CloseAndLog(grpcTargetDB) })

	var grpcTableName string
	err = grpcTargetDB.QueryRowContext(ctx, "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'grpc_items'", grpcDBName).Scan(&grpcTableName)
	require.NoError(t, err, "grpc_items table should exist in grpc database")
	assert.Equal(t, "grpc_items", grpcTableName)
}

// TestHybridMode_TernDeployment verifies that TernDeployment returns the
// correct deployment key for webhook routing in hybrid mode. TernDeployment
// is repo-based (for webhooks); the database-based routing happens in
// resolveDeployment (tested indirectly in TestHybridMode_LocalAndGRPC).
func TestHybridMode_TernDeployment(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	serverConfig := &schemabotapi.ServerConfig{
		Databases: map[string]schemabotapi.DatabaseConfig{
			"local-db": {
				Type:         "mysql",
				Environments: map[string]schemabotapi.EnvironmentConfig{"staging": {DSN: "root@tcp(localhost)/localdb"}},
			},
		},
		TernDeployments: schemabotapi.TernConfig{
			"default": {"staging": "localhost:9090"},
		},
	}

	svc := schemabotapi.New(nil, serverConfig, nil, logger)

	// TernDeployment returns the default deployment when TernDeployments is
	// configured, regardless of which repo is requesting. Repos can override
	// this via repos.*.default_tern_deployment in config.
	dep := svc.TernDeployment("some-repo")
	assert.Equal(t, "default", dep,
		"TernDeployment should return 'default' when TernDeployments is configured")
}

// startTernGRPCForDB starts a gRPC Tern server backed by a LocalClient for
// the given target database. Returns the gRPC address. Uses t.Cleanup for
// resource management.
func startTernGRPCForDB(t *testing.T, appDSN, dbName string) (string, error) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Open Tern storage (reuse the shared tern storage container)
	storageDB, err := sql.Open("mysql", ternStorageDSN)
	if err != nil {
		return "", fmt.Errorf("open tern storage: %w", err)
	}
	t.Cleanup(func() { utils.CloseAndLog(storageDB) })
	storage := mysqlstore.New(storageDB)

	// Create LocalClient backed by Tern storage
	localClient, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  dbName,
		Type:      "mysql",
		TargetDSN: appDSN,
	}, storage, logger)
	if err != nil {
		return "", fmt.Errorf("create local client: %w", err)
	}
	t.Cleanup(func() { utils.CloseAndLog(localClient) })

	// Wrap in gRPC server
	grpcSrv := grpc.NewServer()
	ternGRPCServer := newGRPCServer(localClient)
	registerGRPCServer(ternGRPCServer, grpcSrv)

	grpcListener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	if err != nil {
		return "", fmt.Errorf("listen grpc: %w", err)
	}
	go func() { _ = grpcSrv.Serve(grpcListener) }()
	t.Cleanup(func() { grpcSrv.GracefulStop() })

	grpcAddress := grpcListener.Addr().String()

	// Wait for server ready
	conn, err := grpc.NewClient(grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", fmt.Errorf("create grpc client: %w", err)
	}
	defer func() { _ = conn.Close() }()

	readyCtx, readyCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer readyCancel()
	conn.Connect()
	for conn.GetState() != connectivity.Ready {
		if !conn.WaitForStateChange(readyCtx, conn.GetState()) {
			return "", fmt.Errorf("gRPC server not ready: context expired")
		}
	}

	return grpcAddress, nil
}

// waitForHTTPReady polls the health endpoint until it returns 200 or the
// timeout expires.
func waitForHTTPReady(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			require.Fail(t, "timeout waiting for HTTP server readiness")
		}
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/health", nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}
