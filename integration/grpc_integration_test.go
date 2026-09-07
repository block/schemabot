//go:build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/e2e/testutil"
	schemabotapi "github.com/block/schemabot/pkg/api"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	schemabotmysql "github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// =============================================================================
// gRPC Tests (SchemaBot → Tern)
// =============================================================================

func TestGRPC_Health(t *testing.T) {
	require.NoError(t, grpcClient.Health(t.Context()), "Health()")
}

// =============================================================================
// SchemaBot → Tern E2E Test
// This test verifies that SchemaBot HTTP server can call Tern via gRPC
// =============================================================================

func TestSchemaBot_TernHealth(t *testing.T) {
	schemabotAddr := startSchemaBot(t, grpcAddr)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+schemabotAddr+"/tern-health/default/staging", nil)
	require.NoError(t, err, "create request")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "GET /tern-health")
	t.Cleanup(func() { utils.CloseAndLog(resp.Body) })

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result), "decode response")
	assert.Equal(t, "ok", result["status"])
	assert.Equal(t, "default", result["deployment"])
	assert.Equal(t, "staging", result["environment"])
}

func TestSchemaBot_TernHealth_UnknownDeployment(t *testing.T) {
	schemabotAddr := startSchemaBot(t, grpcAddr)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+schemabotAddr+"/tern-health/unknown/staging", nil)
	require.NoError(t, err, "create request")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "GET /tern-health")
	t.Cleanup(func() { utils.CloseAndLog(resp.Body) })

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestSchemaBot_Health(t *testing.T) {
	schemabotAddr := startSchemaBot(t, grpcAddr)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+schemabotAddr+"/health", nil)
	require.NoError(t, err, "create request")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "GET /health")
	t.Cleanup(func() { utils.CloseAndLog(resp.Body) })

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result), "decode response")
	assert.Equal(t, "ok", result["status"])
}

func startSchemaBot(t *testing.T, ternGRPCAddr string) string {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Create MySQL storage for SchemaBot
	db, err := sql.Open("block-mysql", schemabotDSN)
	require.NoError(t, err, "open schemabot db")
	t.Cleanup(func() { utils.CloseAndLog(db) })
	storage := schemabotmysql.New(db)

	// Create Tern gRPC client for SchemaBot
	ternClient, err := tern.NewGRPCClient(tern.Config{
		Address: ternGRPCAddr,
		Storage: storage,
	})
	require.NoError(t, err, "create tern client")
	t.Cleanup(func() { utils.CloseAndLog(ternClient) })

	// Create SchemaBot with real MySQL storage and real Tern client
	serverConfig := &schemabotapi.ServerConfig{
		Databases: map[string]schemabotapi.DatabaseConfig{
			"testapp": {
				Type: "mysql",
				Environments: map[string]schemabotapi.EnvironmentConfig{
					"staging":    {Target: "testapp-staging", Deployment: "default"},
					"production": {Target: "testapp-production", Deployment: "default"},
				},
			},
		},
		TernDeployments: schemabotapi.TernConfig{
			"default": {"staging": ternGRPCAddr, "production": ternGRPCAddr},
		},
	}
	svc := schemabotapi.New(storage, serverConfig, map[string]tern.Client{
		"default/staging": ternClient,
	}, logger)
	startTestOperator(t, svc)
	t.Cleanup(func() { utils.CloseAndLog(svc) })

	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	require.NoError(t, err, "listen")

	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { utils.CloseAndLog(server) })

	// Wait for server to be ready by polling health endpoint
	addr := listener.Addr().String()
	healthCtx, healthCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer healthCancel()
	for {
		require.NoError(t, healthCtx.Err(), "schemabot server did not become healthy in time")
		req, err := http.NewRequestWithContext(healthCtx, http.MethodGet, "http://"+addr+"/health", nil)
		require.NoError(t, err, "create health request")
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	return addr
}

func waitForStoredExternalID(t *testing.T, applies storage.ApplyStore, applyIdentifier string, timeout time.Duration) *storage.Apply {
	t.Helper()

	var result *storage.Apply
	testutil.Poll(t, timeout, 20*time.Millisecond,
		func() bool {
			apply, err := applies.GetByApplyIdentifier(t.Context(), applyIdentifier)
			require.NoError(t, err, "get apply by identifier")
			result = apply
			return apply != nil && apply.ExternalID != ""
		},
		func() string {
			if result == nil {
				return fmt.Sprintf("apply %s not found in storage within %s", applyIdentifier, timeout)
			}
			return fmt.Sprintf("apply %s was not dispatched within %s", applyIdentifier, timeout)
		},
	)
	return result
}

// TestGRPC_ExternalID_StoredOnApply verifies that when SchemaBot calls a remote
// Tern via gRPC, the apply_id returned by the remote Tern is stored as
// external_id on the apply record in SchemaBot's storage.
func TestGRPC_ExternalID_StoredOnApply(t *testing.T) {
	ctx := t.Context()

	// Create a unique target database for this test
	targetDB, err := sql.Open("block-mysql", targetDSN+"&multiStatements=true")
	require.NoError(t, err, "open target db")
	defer utils.CloseAndLog(targetDB)

	appDBName := fmt.Sprintf("extid_%d", time.Now().UnixNano()%100000)
	_, err = targetDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdentifier(appDBName))
	require.NoError(t, err, "create app database")
	defer func() {
		_, _ = targetDB.ExecContext(context.WithoutCancel(t.Context()), "DROP DATABASE IF EXISTS "+quoteIdentifier(appDBName))
	}()

	appCfg, err := mysql.ParseDSN(targetDSN)
	require.NoError(t, err, "parse target DSN")
	appCfg.DBName = appDBName
	appDSN := appCfg.FormatDSN()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Set up a separate Tern gRPC server for this test (backed by LocalClient)
	ternGRPCAddr, err := startTernGRPC(ctx, appDSN, ternStorageDSN)
	require.NoError(t, err, "start tern grpc")

	// Create SchemaBot with its own storage and a GRPCClient to remote Tern
	schemabotDB, err := sql.Open("block-mysql", schemabotDSN)
	require.NoError(t, err, "open schemabot db")
	defer utils.CloseAndLog(schemabotDB)
	schemabotStorage := schemabotmysql.New(schemabotDB)

	ternClient, err := tern.NewGRPCClient(tern.Config{
		Address: ternGRPCAddr,
		Storage: schemabotStorage,
	})
	require.NoError(t, err, "create tern client")
	defer utils.CloseAndLog(ternClient)

	serverConfig := &schemabotapi.ServerConfig{
		Databases: map[string]schemabotapi.DatabaseConfig{
			appDBName: {
				Type: "mysql",
				Environments: map[string]schemabotapi.EnvironmentConfig{
					"staging": {Target: appDBName + "-target", Deployment: "default"},
				},
			},
		},
		TernDeployments: schemabotapi.TernConfig{
			"default": {"staging": ternGRPCAddr},
		},
	}
	svc := schemabotapi.New(schemabotStorage, serverConfig, map[string]tern.Client{
		"default/staging": ternClient,
	}, logger)
	startTestOperator(t, svc)
	defer utils.CloseAndLog(svc)

	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "localhost:0")
	require.NoError(t, err, "listen")
	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()
	defer utils.CloseAndLog(server)
	schemabotAddr := listener.Addr().String()
	time.Sleep(50 * time.Millisecond)

	// Step 1: Plan
	schemaSQL := `CREATE TABLE items (
		id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL
	);`
	planReq := map[string]any{
		"database":    appDBName,
		"environment": "staging",
		"type":        "mysql",
		"schema_files": map[string]any{
			"default": map[string]any{
				"files": map[string]string{
					"items.sql": schemaSQL,
				},
			},
		},
		"repository":   "test/extid",
		"pull_request": 1,
	}
	planBody, _ := json.Marshal(planReq)
	planHTTPReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+schemabotAddr+"/api/plan", bytes.NewReader(planBody))
	planHTTPReq.Header.Set("Content-Type", "application/json")
	planResp, err := http.DefaultClient.Do(planHTTPReq)
	require.NoError(t, err, "POST /api/plan")
	t.Cleanup(func() { utils.CloseAndLog(planResp.Body) })
	if planResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(planResp.Body)
		require.Failf(t, "plan failed", "(%d): %s", planResp.StatusCode, body)
	}
	var planResult map[string]any
	require.NoError(t, json.NewDecoder(planResp.Body).Decode(&planResult), "decode plan response")
	planID, ok := planResult["plan_id"].(string)
	require.True(t, ok && planID != "", "plan response missing plan_id: %v", planResult)

	// Step 2: Apply
	applyReq := map[string]any{
		"plan_id":     planID,
		"environment": "staging",
	}
	applyBody, _ := json.Marshal(applyReq)
	applyHTTPReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+schemabotAddr+"/api/apply", bytes.NewReader(applyBody))
	applyHTTPReq.Header.Set("Content-Type", "application/json")
	applyResp, err := http.DefaultClient.Do(applyHTTPReq)
	require.NoError(t, err, "POST /api/apply")
	t.Cleanup(func() { utils.CloseAndLog(applyResp.Body) })
	if applyResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(applyResp.Body)
		require.Failf(t, "apply failed", "(%d): %s", applyResp.StatusCode, body)
	}
	var applyResult map[string]any
	require.NoError(t, json.NewDecoder(applyResp.Body).Decode(&applyResult), "decode apply response")
	require.Equal(t, true, applyResult["accepted"], "apply not accepted: %v", applyResult)
	applyID, ok := applyResult["apply_id"].(string)
	require.True(t, ok && applyID != "", "apply response missing apply_id: %v", applyResult)

	// Step 3: Operator dispatches the queued apply and stores remote Tern's id.
	storedApply := waitForStoredExternalID(t, schemabotStorage.Applies(), applyID, 10*time.Second)
	assert.NotEmpty(t, storedApply.ExternalID, "external_id is empty, expected it to be set from remote Tern's apply_id")
	assert.NotEqual(t, applyID, storedApply.ExternalID,
		"apply_identifier (HTTP) and external_id (Tern) should differ — SchemaBot generates its own identifier")
	t.Logf("Verified: apply_identifier=%s external_id=%s (different)", applyID, storedApply.ExternalID)
	waitForState(t, "http://"+schemabotAddr, applyID, "completed", 30*time.Second)
}

// TestGRPC_TaskStateUpdatedOnCompletion verifies that when a gRPC apply
// completes, the per-task records in SchemaBot's local storage are updated to
// their terminal state.
func TestGRPC_TaskStateUpdatedOnCompletion(t *testing.T) {
	ctx := t.Context()

	// Create a unique target database for this test. The close cleanup is
	// registered before the drop cleanup so it runs after it (cleanups run
	// LIFO): the drop still has a live handle.
	targetDB, err := sql.Open("block-mysql", targetDSN+"&multiStatements=true")
	require.NoError(t, err, "open target db")
	t.Cleanup(func() { utils.CloseAndLog(targetDB) })

	appDBName := fmt.Sprintf("taskst_%d", time.Now().UnixNano()%100000)
	_, err = targetDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdentifier(appDBName))
	require.NoError(t, err, "create app database")
	t.Cleanup(func() {
		// Cleanup runs after the test context is cancelled, so the drop detaches
		// from it: a context derived from a cancelled parent is born cancelled and
		// the database would outlive the run.
		dropCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		if _, err := targetDB.ExecContext(dropCtx, "DROP DATABASE IF EXISTS "+quoteIdentifier(appDBName)); err != nil {
			t.Logf("cleanup: drop database %s: %v", appDBName, err)
		}
	})

	appCfg, err := mysql.ParseDSN(targetDSN)
	require.NoError(t, err, "parse target DSN")
	appCfg.DBName = appDBName
	appDSN := appCfg.FormatDSN()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Set up a separate Tern gRPC server for this test (backed by LocalClient)
	ternGRPCAddr, err := startTernGRPC(ctx, appDSN, ternStorageDSN)
	require.NoError(t, err, "start tern grpc")

	// Create SchemaBot with its own storage and a GRPCClient to remote Tern.
	// The GRPCClient must have storage so pollForCompletion can update tasks.
	schemabotDB, err := sql.Open("block-mysql", schemabotDSN)
	require.NoError(t, err, "open schemabot db")
	defer utils.CloseAndLog(schemabotDB)
	schemabotStorage := schemabotmysql.New(schemabotDB)

	ternClient, err := tern.NewGRPCClient(tern.Config{
		Address: ternGRPCAddr,
		Storage: schemabotStorage,
	})
	require.NoError(t, err, "create tern client")
	defer utils.CloseAndLog(ternClient)

	serverConfig := &schemabotapi.ServerConfig{
		Databases: map[string]schemabotapi.DatabaseConfig{
			appDBName: {
				Type: "mysql",
				Environments: map[string]schemabotapi.EnvironmentConfig{
					"staging": {Target: appDBName + "-target", Deployment: "default"},
				},
			},
		},
		TernDeployments: schemabotapi.TernConfig{
			"default": {"staging": ternGRPCAddr},
		},
	}
	svc := schemabotapi.New(schemabotStorage, serverConfig, map[string]tern.Client{
		"default/staging": ternClient,
	}, logger)
	startTestOperator(t, svc)
	defer utils.CloseAndLog(svc)

	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "localhost:0")
	require.NoError(t, err, "listen")
	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()
	defer utils.CloseAndLog(server)
	schemabotAddr := listener.Addr().String()

	// Wait for HTTP server readiness
	testutil.Poll(t, 5*time.Second, 10*time.Millisecond,
		func() bool {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+schemabotAddr+"/health", nil)
			require.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return false
			}
			_ = resp.Body.Close()
			return resp.StatusCode == http.StatusOK
		},
		func() string { return "schemabot HTTP server did not become healthy" },
	)

	// Step 1: Plan — create a new table
	schemaSQL := `CREATE TABLE task_state_test (
		id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL
	);`
	planResp := postJSON(t, "http://"+schemabotAddr+"/api/plan", map[string]any{
		"database":    appDBName,
		"environment": "staging",
		"type":        "mysql",
		"schema_files": map[string]any{
			"default": map[string]any{
				"files": map[string]string{
					"task_state_test.sql": schemaSQL,
				},
			},
		},
		"repository":   "test/taskstate",
		"pull_request": 147,
	})
	planID, ok := planResp["plan_id"].(string)
	require.True(t, ok && planID != "", "plan response missing plan_id: %v", planResp)

	// Step 2: Apply
	applyResp := postJSON(t, "http://"+schemabotAddr+"/api/apply", map[string]any{
		"plan_id":     planID,
		"environment": "staging",
	})
	require.True(t, applyResp["accepted"] == true, "apply not accepted: %v", applyResp)
	applyID, ok := applyResp["apply_id"].(string)
	require.True(t, ok && applyID != "", "apply response missing apply_id: %v", applyResp)

	// Step 3: Wait for the apply to complete
	waitForState(t, "http://"+schemabotAddr, applyID, "completed", 15*time.Second)

	// Step 4: Wait for the local apply record to be updated by pollForCompletion.
	// waitForState polls the HTTP API (which calls remote Tern's Progress
	// RPC), but the local storage update happens asynchronously in the poller goroutine.
	var storedApply *storage.Apply
	testutil.Poll(t, 5*time.Second, 100*time.Millisecond,
		func() bool {
			storedApply, err = schemabotStorage.Applies().GetByApplyIdentifier(ctx, applyID)
			require.NoError(t, err, "get apply by identifier")
			require.NotNil(t, storedApply, "apply not found")
			return storedApply.State == state.Apply.Completed
		},
		func() string {
			return fmt.Sprintf("apply record should be completed in local storage, last state: %q", storedApply.State)
		},
	)

	// Step 5: Verify task records in SchemaBot's storage have terminal state.
	tasks, err := schemabotStorage.Tasks().GetByApplyID(ctx, storedApply.ID)
	require.NoError(t, err, "get tasks by apply ID")
	require.NotEmpty(t, tasks, "expected task records for apply %s", applyID)

	for _, task := range tasks {
		assert.Equal(t, state.Task.Completed, task.State,
			"task %s (table=%s) should be completed, but is %q — pollForCompletion did not sync terminal task state",
			task.TaskIdentifier, task.TableName, task.State)
	}
}

// TestGRPC_FailedTableErrorSurfacesInTaskRecord drives a schema change through a
// remote Tern over gRPC that fails at the engine: adding a unique index over
// duplicate rows. The control plane never polls the engine directly, so the PR
// comment and CLI can only name the table whose engine error caused the failure
// if the remote's per-table error reaches SchemaBot's own stored task row. The
// driver that holds the apply's lease is the only writer of that reason, so
// driving the apply to terminal must be sufficient on its own: the failed table
// carries the engine error on its task record, not just on the apply record.
func TestGRPC_FailedTableErrorSurfacesInTaskRecord(t *testing.T) {
	ctx := t.Context()

	// Create a unique target database for this test. The close cleanup is
	// registered before the drop cleanup so it runs after it (cleanups run
	// LIFO): the drop still has a live handle.
	targetDB, err := sql.Open("block-mysql", targetDSN+"&multiStatements=true")
	require.NoError(t, err, "open target db")
	t.Cleanup(func() { utils.CloseAndLog(targetDB) })
	require.NoError(t, targetDB.PingContext(ctx), "ping target db")

	appDBName := fmt.Sprintf("tblerr_%d", time.Now().UnixNano()%100000)
	_, err = targetDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdentifier(appDBName))
	require.NoError(t, err, "create app database")
	t.Cleanup(func() {
		// Cleanup runs after the test context is cancelled, so the drop detaches
		// from it: a context derived from a cancelled parent is born cancelled and
		// the database would outlive the run.
		dropCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		if _, err := targetDB.ExecContext(dropCtx, "DROP DATABASE IF EXISTS "+quoteIdentifier(appDBName)); err != nil {
			t.Logf("cleanup: drop database %s: %v", appDBName, err)
		}
	})

	// Seed a table with duplicate values so adding a unique index must fail in
	// the engine with this table's own error.
	_, err = targetDB.ExecContext(ctx, "CREATE TABLE "+quoteIdentifier(appDBName)+".`dup_users` ("+
		"`id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, "+
		"`name` VARCHAR(255) NOT NULL"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err, "create seeded table")
	_, err = targetDB.ExecContext(ctx, "INSERT INTO "+quoteIdentifier(appDBName)+".`dup_users` (`name`) VALUES ('a'), ('a')")
	require.NoError(t, err, "seed duplicate rows")

	appCfg, err := mysql.ParseDSN(targetDSN)
	require.NoError(t, err, "parse target DSN")
	appCfg.DBName = appDBName
	appDSN := appCfg.FormatDSN()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Set up a separate Tern gRPC server for this test (backed by LocalClient)
	ternGRPCAddr, err := startTernGRPC(ctx, appDSN, ternStorageDSN)
	require.NoError(t, err, "start tern grpc")

	// Create SchemaBot with its own storage and a GRPCClient to remote Tern.
	// The service owns both handles once it is built — its Close closes the
	// storage, which closes this DB, and every Tern client it was given. These
	// closes only cover an early failure before the service exists, so they
	// discard the error the service's own close makes inevitable.
	schemabotDB, err := sql.Open("block-mysql", schemabotDSN)
	require.NoError(t, err, "open schemabot db")
	defer func() { _ = schemabotDB.Close() }()
	require.NoError(t, schemabotDB.PingContext(ctx), "ping schemabot db")
	schemabotStorage := schemabotmysql.New(schemabotDB)

	ternClient, err := tern.NewGRPCClient(tern.Config{
		Address: ternGRPCAddr,
		Storage: schemabotStorage,
	})
	require.NoError(t, err, "create tern client")
	defer func() { _ = ternClient.Close() }()

	serverConfig := &schemabotapi.ServerConfig{
		Databases: map[string]schemabotapi.DatabaseConfig{
			appDBName: {
				Type: "mysql",
				Environments: map[string]schemabotapi.EnvironmentConfig{
					"staging": {Target: appDBName + "-target", Deployment: "default"},
				},
			},
		},
		TernDeployments: schemabotapi.TernConfig{
			"default": {"staging": ternGRPCAddr},
		},
	}
	svc := schemabotapi.New(schemabotStorage, serverConfig, map[string]tern.Client{
		"default/staging": ternClient,
	}, logger)
	startTestOperator(t, svc)
	defer utils.CloseAndLog(svc)

	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "localhost:0")
	require.NoError(t, err, "listen")
	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()
	defer utils.CloseAndLog(server)
	schemabotAddr := listener.Addr().String()

	// Wait for HTTP server readiness
	testutil.Poll(t, 5*time.Second, 10*time.Millisecond,
		func() bool {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+schemabotAddr+"/health", nil)
			require.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return false
			}
			_ = resp.Body.Close()
			return resp.StatusCode == http.StatusOK
		},
		func() string { return "schemabot HTTP server did not become healthy" },
	)

	// Step 1: Plan — add a unique index the seeded duplicate rows cannot satisfy,
	// alongside a second table whose change is unaffected by that failure.
	schemaSQL := `CREATE TABLE dup_users (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		UNIQUE KEY uniq_name (name)
	);`
	bystanderSQL := `CREATE TABLE audit_log (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		note VARCHAR(255) NOT NULL
	);`
	planResp := postJSON(t, "http://"+schemabotAddr+"/api/plan", map[string]any{
		"database":    appDBName,
		"environment": "staging",
		"type":        "mysql",
		"schema_files": map[string]any{
			"default": map[string]any{
				"files": map[string]string{
					"dup_users.sql": schemaSQL,
					"audit_log.sql": bystanderSQL,
				},
			},
		},
		"repository":   "test/tableerror",
		"pull_request": 148,
	})
	planID, ok := planResp["plan_id"].(string)
	require.True(t, ok && planID != "", "plan response missing plan_id: %v", planResp)

	// Step 2: Apply
	applyResp := postJSON(t, "http://"+schemabotAddr+"/api/apply", map[string]any{
		"plan_id":     planID,
		"environment": "staging",
	})
	require.True(t, applyResp["accepted"] == true, "apply not accepted: %v", applyResp)
	applyID, ok := applyResp["apply_id"].(string)
	require.True(t, ok && applyID != "", "apply response missing apply_id: %v", applyResp)

	// The engine classifies the duplicate-row failure retryable, so the remote
	// operator would re-run the whole engine attempt until the recovery budget
	// is spent, and the control plane correctly keeps polling through those
	// pauses — a bounded sequence of full engine runs far longer than a test
	// deadline should cover. Spend the budget on the remote apply row up front
	// so the first retryable failure goes through the real expiry pass
	// immediately, settling the remote apply and its tasks to the same terminal
	// states exhaustion would reach, with the engine error intact.
	ternDB, err := sql.Open("block-mysql", ternStorageDSN)
	require.NoError(t, err, "open tern storage db")
	t.Cleanup(func() { utils.CloseAndLog(ternDB) })
	require.NoError(t, ternDB.PingContext(ctx), "ping tern storage db")
	var remoteApplyID string
	testutil.Poll(t, 10*time.Second, 50*time.Millisecond,
		func() bool {
			stored, getErr := schemabotStorage.Applies().GetByApplyIdentifier(ctx, applyID)
			require.NoError(t, getErr, "get apply by identifier")
			if stored == nil {
				return false
			}
			remoteApplyID = stored.ExternalID
			return remoteApplyID != ""
		},
		func() string { return "apply never recorded a remote apply id" },
	)
	spendResult, err := ternDB.ExecContext(ctx,
		"UPDATE applies SET attempt = ? WHERE apply_identifier = ?",
		storage.MaxRecoveryAttempts, remoteApplyID)
	require.NoError(t, err, "spend the remote apply's recovery budget")
	spent, err := spendResult.RowsAffected()
	require.NoError(t, err, "read spent recovery budget rows")
	require.EqualValues(t, 1, spent, "expected to spend the recovery budget on remote apply %s", remoteApplyID)

	// Step 3: Wait for the apply to fail on the duplicate rows. With the
	// recovery budget spent, the first retryable failure expires to permanent
	// failed instead of entering the recovery loop, so the deadline covers a
	// single engine run plus the expiry pass.
	waitForState(t, "http://"+schemabotAddr, applyID, "failed", 30*time.Second)

	// Step 4: Wait for the local apply record to be updated by pollForCompletion.
	var storedApply *storage.Apply
	testutil.Poll(t, 5*time.Second, 100*time.Millisecond,
		func() bool {
			storedApply, err = schemabotStorage.Applies().GetByApplyIdentifier(ctx, applyID)
			require.NoError(t, err, "get apply by identifier")
			require.NotNil(t, storedApply, "apply not found")
			return storedApply.State == state.Apply.Failed
		},
		func() string {
			return fmt.Sprintf("apply record should be failed in local storage, last state: %q", storedApply.State)
		},
	)
	require.NotEmpty(t, storedApply.ErrorMessage, "failed apply record should carry the engine error")

	// Step 5: The failed table's own engine error must be on SchemaBot's stored
	// task row — the record the PR comment and CLI render from. Progress reads
	// mirror per-table state and counters from the remote but never the reason,
	// so a reason on this row can only have been written by the driver holding
	// the apply's lease.
	tasks, err := schemabotStorage.Tasks().GetByApplyID(ctx, storedApply.ID)
	require.NoError(t, err, "get tasks by apply ID")
	require.NotEmpty(t, tasks, "expected task records for apply %s", applyID)

	var dupTask, bystanderTask *storage.Task
	for _, task := range tasks {
		if task.Shard != "" {
			continue
		}
		switch task.TableName {
		case "dup_users":
			dupTask = task
		case "audit_log":
			bystanderTask = task
		}
	}
	require.NotNil(t, dupTask, "expected a task record for table dup_users")
	require.NotNil(t, bystanderTask, "expected a task record for table audit_log")
	assert.Equal(t, state.Task.Failed, dupTask.State,
		"task %s should be failed, but is %q", dupTask.TaskIdentifier, dupTask.State)
	assert.NotEmpty(t, dupTask.ErrorMessage,
		"failed task should carry the engine's own reason for this table, mirrored from the remote per-table progress")
	// The apply-level error names the table it blames ("table X failed: ..."),
	// so its absence here shows the reason came from this table's own remote
	// progress rather than from the apply-level error being stamped over it.
	assert.NotContains(t, dupTask.ErrorMessage, "table "+dupTask.TableName+" failed",
		"per-table reason should reach the task row directly, not by way of the apply-level error")

	// The other table did not fail, so its row must stay error-free: an apply
	// that stamps its failure across every task tells the operator the schema
	// change broke without telling them which table to look at.
	assert.NotEqual(t, state.Task.Failed, bystanderTask.State,
		"table %s did not fail, but its task is %q", bystanderTask.TableName, bystanderTask.State)
	assert.Empty(t, bystanderTask.ErrorMessage,
		"only the table that failed should carry an error, so the root cause stays identifiable")
}

// TestGRPC_ServerSideTargetPlan verifies that HTTP plan succeeds when the
// target is configured server-side. The Tern proto still exposes target for
// remote implementations, but SchemaBot HTTP callers do not provide it.
func TestGRPC_ServerSideTargetPlan(t *testing.T) {
	// Part 1: Verify HTTP API plans use the server-configured target.
	schemabotAddr := startSchemaBot(t, grpcAddr)

	schemaSQL := `CREATE TABLE target_test (
		id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL
	);`
	planReq := map[string]any{
		"database":    "testapp",
		"environment": "staging",
		"type":        "mysql",
		"schema_files": map[string]any{
			"default": map[string]any{
				"files": map[string]string{
					"target_test.sql": schemaSQL,
				},
			},
		},
		"repository":   "test/target",
		"pull_request": 1,
	}
	planBody, err := json.Marshal(planReq)
	require.NoError(t, err, "marshal plan request")

	planHTTPReq, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+schemabotAddr+"/api/plan", bytes.NewReader(planBody))
	require.NoError(t, err, "create plan request")
	planHTTPReq.Header.Set("Content-Type", "application/json")

	planResp, err := http.DefaultClient.Do(planHTTPReq)
	require.NoError(t, err, "POST /api/plan with target")
	t.Cleanup(func() { utils.CloseAndLog(planResp.Body) })

	if planResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(planResp.Body)
		require.Failf(t, "plan failed", "(%d): %s", planResp.StatusCode, body)
	}

	var planResult map[string]any
	require.NoError(t, json.NewDecoder(planResp.Body).Decode(&planResult), "decode plan response")
	assert.NotEmpty(t, planResult["plan_id"], "plan should return plan_id")

	// Part 2: Verify target is accessible on the proto request via gRPC client
	ternReq := &ternv1.PlanRequest{
		Database:    "testapp",
		Type:        "mysql",
		Environment: "staging",
		Target:      "test-cluster-001",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"testapp": {Files: map[string]string{"target_test.sql": schemaSQL}},
		},
	}
	assert.Equal(t, "test-cluster-001", ternReq.GetTarget(), "target should be accessible via proto getter")

	grpcResp, err := grpcClient.Plan(t.Context(), ternReq)
	require.NoError(t, err, "gRPC Plan with target")
	assert.NotEmpty(t, grpcResp.PlanId, "gRPC plan with target should return plan_id")
}

// TestGRPC_ServerSideDeploymentStoredOnApply verifies that the server-resolved
// deployment is persisted on the apply record.
func TestGRPC_ServerSideDeploymentStoredOnApply(t *testing.T) {
	ctx := t.Context()

	targetDB, err := sql.Open("block-mysql", targetDSN+"&multiStatements=true")
	require.NoError(t, err, "open target db")
	defer utils.CloseAndLog(targetDB)

	appDBName := fmt.Sprintf("deploy_%d", time.Now().UnixNano()%100000)
	_, err = targetDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdentifier(appDBName))
	require.NoError(t, err, "create app database")
	defer func() {
		_, _ = targetDB.ExecContext(context.WithoutCancel(t.Context()), "DROP DATABASE IF EXISTS "+quoteIdentifier(appDBName))
	}()

	appCfg, err := mysql.ParseDSN(targetDSN)
	require.NoError(t, err, "parse target DSN")
	appCfg.DBName = appDBName
	appDSN := appCfg.FormatDSN()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	ternGRPCAddr, err := startTernGRPC(ctx, appDSN, ternStorageDSN)
	require.NoError(t, err, "start tern grpc")

	schemabotDB, err := sql.Open("block-mysql", schemabotDSN)
	require.NoError(t, err, "open schemabot db")
	defer utils.CloseAndLog(schemabotDB)
	schemabotStorage := schemabotmysql.New(schemabotDB)

	ternClient, err := tern.NewGRPCClient(tern.Config{
		Address: ternGRPCAddr,
		Storage: schemabotStorage,
	})
	require.NoError(t, err, "create tern client")
	defer utils.CloseAndLog(ternClient)

	// Configure the database target with a named deployment.
	serverConfig := &schemabotapi.ServerConfig{
		Databases: map[string]schemabotapi.DatabaseConfig{
			appDBName: {
				Type: "mysql",
				Environments: map[string]schemabotapi.EnvironmentConfig{
					"staging": {Target: appDBName + "-target", Deployment: "us-west"},
				},
			},
		},
		TernDeployments: schemabotapi.TernConfig{
			"us-west": {"staging": ternGRPCAddr},
		},
	}
	svc := schemabotapi.New(schemabotStorage, serverConfig, map[string]tern.Client{
		"us-west/staging": ternClient,
	}, logger)
	startTestOperator(t, svc)
	defer utils.CloseAndLog(svc)

	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "localhost:0")
	require.NoError(t, err, "listen")
	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()
	defer utils.CloseAndLog(server)
	schemabotAddr := listener.Addr().String()
	time.Sleep(50 * time.Millisecond)

	schemaSQL := "CREATE TABLE widgets (\n\tid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,\n\tname VARCHAR(255) NOT NULL\n);"
	planReq := map[string]any{
		"database":    appDBName,
		"environment": "staging",
		"type":        "mysql",
		"schema_files": map[string]any{
			"default": map[string]any{
				"files": map[string]string{"widgets.sql": schemaSQL},
			},
		},
		"repository":   "test/deployment",
		"pull_request": 1,
	}
	planBody, _ := json.Marshal(planReq)
	planHTTPReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+schemabotAddr+"/api/plan", bytes.NewReader(planBody))
	planHTTPReq.Header.Set("Content-Type", "application/json")
	planResp, err := http.DefaultClient.Do(planHTTPReq)
	require.NoError(t, err, "POST /api/plan")
	t.Cleanup(func() { utils.CloseAndLog(planResp.Body) })
	if planResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(planResp.Body)
		require.Failf(t, "plan failed", "(%d): %s", planResp.StatusCode, body)
	}
	var planResult map[string]any
	require.NoError(t, json.NewDecoder(planResp.Body).Decode(&planResult), "decode plan response")
	planID, ok := planResult["plan_id"].(string)
	require.True(t, ok && planID != "", "plan response missing plan_id: %v", planResult)

	applyReq := map[string]any{
		"plan_id":     planID,
		"environment": "staging",
	}
	applyBody, _ := json.Marshal(applyReq)
	applyHTTPReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+schemabotAddr+"/api/apply", bytes.NewReader(applyBody))
	applyHTTPReq.Header.Set("Content-Type", "application/json")
	applyResp, err := http.DefaultClient.Do(applyHTTPReq)
	require.NoError(t, err, "POST /api/apply")
	t.Cleanup(func() { utils.CloseAndLog(applyResp.Body) })
	if applyResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(applyResp.Body)
		require.Failf(t, "apply failed", "(%d): %s", applyResp.StatusCode, body)
	}
	var applyResult map[string]any
	require.NoError(t, json.NewDecoder(applyResp.Body).Decode(&applyResult), "decode apply response")
	require.Equal(t, true, applyResult["accepted"], "apply not accepted: %v", applyResult)
	applyID, ok := applyResult["apply_id"].(string)
	require.True(t, ok && applyID != "", "apply response missing apply_id: %v", applyResult)

	storedApply, err := schemabotStorage.Applies().GetByApplyIdentifier(ctx, applyID)
	require.NoError(t, err, "get apply by identifier")
	require.NotNil(t, storedApply, "apply %s not found in storage", applyID)
	assert.Equal(t, "us-west", storedApply.Deployment, "deployment should be stored on the apply")
}
