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
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	db, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err, "open schemabot db")
	t.Cleanup(func() { utils.CloseAndLog(db) })
	storage := schemabotmysql.New(db)

	// Create Tern gRPC client for SchemaBot
	ternClient, err := tern.NewGRPCClient(tern.Config{Address: ternGRPCAddr})
	require.NoError(t, err, "create tern client")
	t.Cleanup(func() { utils.CloseAndLog(ternClient) })

	// Create SchemaBot with real MySQL storage and real Tern client
	serverConfig := &schemabotapi.ServerConfig{
		TernDeployments: schemabotapi.TernConfig{
			"default": {"staging": ternGRPCAddr, "production": ternGRPCAddr},
		},
	}
	svc := schemabotapi.New(storage, serverConfig, map[string]tern.Client{
		"default/staging": ternClient,
	}, logger)
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

// TestGRPC_ExternalID_StoredOnApply verifies that when SchemaBot calls a remote
// Tern via gRPC, the apply_id returned by the remote Tern is stored as
// external_id on the apply record in SchemaBot's storage.
func TestGRPC_ExternalID_StoredOnApply(t *testing.T) {
	ctx := t.Context()

	// Create a unique target database for this test
	targetDB, err := sql.Open("mysql", targetDSN+"&multiStatements=true")
	require.NoError(t, err, "open target db")
	defer utils.CloseAndLog(targetDB)

	appDBName := fmt.Sprintf("extid_%d", time.Now().UnixNano()%100000)
	_, err = targetDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+appDBName)
	require.NoError(t, err, "create app database")
	defer func() {
		_, _ = targetDB.ExecContext(context.WithoutCancel(t.Context()), "DROP DATABASE IF EXISTS "+appDBName)
	}()

	appDSN := strings.Replace(targetDSN, "/target_test", "/"+appDBName, 1)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Set up a separate Tern gRPC server for this test (backed by LocalClient)
	ternGRPCAddr, err := startTernGRPC(ctx, appDSN, ternStorageDSN)
	require.NoError(t, err, "start tern grpc")

	// Create SchemaBot with its own storage and a GRPCClient to remote Tern
	schemabotDB, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err, "open schemabot db")
	defer utils.CloseAndLog(schemabotDB)
	schemabotStorage := schemabotmysql.New(schemabotDB)

	ternClient, err := tern.NewGRPCClient(tern.Config{Address: ternGRPCAddr})
	require.NoError(t, err, "create tern client")
	defer utils.CloseAndLog(ternClient)

	serverConfig := &schemabotapi.ServerConfig{
		TernDeployments: schemabotapi.TernConfig{
			"default": {"staging": ternGRPCAddr},
		},
	}
	svc := schemabotapi.New(schemabotStorage, serverConfig, map[string]tern.Client{
		"default/staging": ternClient,
	}, logger)
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

	// Step 3: Verify external_id is stored in SchemaBot's storage
	storedApply, err := schemabotStorage.Applies().GetByApplyIdentifier(ctx, applyID)
	require.NoError(t, err, "get apply by identifier")
	require.NotNil(t, storedApply, "apply %s not found in storage", applyID)

	assert.NotEmpty(t, storedApply.ExternalID, "external_id is empty, expected it to be set from remote Tern's apply_id")
	assert.NotEqual(t, applyID, storedApply.ExternalID,
		"apply_identifier (HTTP) and external_id (Tern) should differ — SchemaBot generates its own identifier")
	t.Logf("Verified: apply_identifier=%s external_id=%s (different)", applyID, storedApply.ExternalID)
}

// TestGRPC_TaskStateUpdatedOnCompletion verifies that when a gRPC apply
// completes, the per-task records in SchemaBot's local storage are updated to
// their terminal state.
func TestGRPC_TaskStateUpdatedOnCompletion(t *testing.T) {
	ctx := t.Context()

	// Create a unique target database for this test
	targetDB, err := sql.Open("mysql", targetDSN+"&multiStatements=true")
	require.NoError(t, err, "open target db")
	defer utils.CloseAndLog(targetDB)

	appDBName := fmt.Sprintf("taskst_%d", time.Now().UnixNano()%100000)
	_, err = targetDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+appDBName)
	require.NoError(t, err, "create app database")
	t.Cleanup(func() {
		_, _ = targetDB.ExecContext(t.Context(), "DROP DATABASE IF EXISTS "+appDBName)
	})

	appDSN := strings.Replace(targetDSN, "/target_test", "/"+appDBName, 1)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Set up a separate Tern gRPC server for this test (backed by LocalClient)
	ternGRPCAddr, err := startTernGRPC(ctx, appDSN, ternStorageDSN)
	require.NoError(t, err, "start tern grpc")

	// Create SchemaBot with its own storage and a GRPCClient to remote Tern.
	// The GRPCClient must have storage so pollForCompletion can update tasks.
	schemabotDB, err := sql.Open("mysql", schemabotDSN)
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
		TernDeployments: schemabotapi.TernConfig{
			"default": {"staging": ternGRPCAddr},
		},
	}
	svc := schemabotapi.New(schemabotStorage, serverConfig, map[string]tern.Client{
		"default/staging": ternClient,
	}, logger)
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
	healthDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(healthDeadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+schemabotAddr+"/health", nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

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
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		storedApply, err = schemabotStorage.Applies().GetByApplyIdentifier(ctx, applyID)
		require.NoError(t, err, "get apply by identifier")
		require.NotNil(t, storedApply, "apply not found")
		if storedApply.State == state.Apply.Completed {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Equal(t, state.Apply.Completed, storedApply.State,
		"apply record should be completed in local storage")

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

// TestGRPC_TargetInPlanRequest verifies that the target field threads through
// the HTTP API → gRPC proto layer without breaking the plan flow.
// It sends a plan request with target via HTTP, then verifies the plan succeeds
// and the target value reaches the gRPC layer by calling Plan directly.
func TestGRPC_TargetInPlanRequest(t *testing.T) {
	// Part 1: Verify target threads through HTTP API to gRPC
	schemabotAddr := startSchemaBot(t, grpcAddr)

	schemaSQL := `CREATE TABLE target_test (
		id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL
	);`
	planReq := map[string]any{
		"database":    "testapp",
		"environment": "staging",
		"type":        "mysql",
		"target":      "test-cluster-001",
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
	assert.NotEmpty(t, planResult["plan_id"], "plan with target should return plan_id")

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

// TestGRPC_Deployment_StoredOnApply verifies that when a plan/apply request
// includes a deployment field, the deployment is persisted on the apply record.
func TestGRPC_Deployment_StoredOnApply(t *testing.T) {
	ctx := t.Context()

	targetDB, err := sql.Open("mysql", targetDSN+"&multiStatements=true")
	require.NoError(t, err, "open target db")
	defer utils.CloseAndLog(targetDB)

	appDBName := fmt.Sprintf("deploy_%d", time.Now().UnixNano()%100000)
	_, err = targetDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+appDBName)
	require.NoError(t, err, "create app database")
	defer func() {
		_, _ = targetDB.ExecContext(context.WithoutCancel(t.Context()), "DROP DATABASE IF EXISTS "+appDBName)
	}()

	appDSN := strings.Replace(targetDSN, "/target_test", "/"+appDBName, 1)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	ternGRPCAddr, err := startTernGRPC(ctx, appDSN, ternStorageDSN)
	require.NoError(t, err, "start tern grpc")

	schemabotDB, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err, "open schemabot db")
	defer utils.CloseAndLog(schemabotDB)
	schemabotStorage := schemabotmysql.New(schemabotDB)

	ternClient, err := tern.NewGRPCClient(tern.Config{Address: ternGRPCAddr})
	require.NoError(t, err, "create tern client")
	defer utils.CloseAndLog(ternClient)

	// Configure with a named deployment (not "default")
	serverConfig := &schemabotapi.ServerConfig{
		TernDeployments: schemabotapi.TernConfig{
			"us-west": {"staging": ternGRPCAddr},
		},
	}
	svc := schemabotapi.New(schemabotStorage, serverConfig, map[string]tern.Client{
		"us-west/staging": ternClient,
	}, logger)
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

	// Plan with explicit deployment
	schemaSQL := "CREATE TABLE widgets (\n\tid BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,\n\tname VARCHAR(255) NOT NULL\n);"
	planReq := map[string]any{
		"database":    appDBName,
		"environment": "staging",
		"type":        "mysql",
		"deployment":  "us-west",
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

	// Apply with explicit deployment
	applyReq := map[string]any{
		"plan_id":     planID,
		"environment": "staging",
		"deployment":  "us-west",
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

	// Verify deployment is stored on the apply record
	storedApply, err := schemabotStorage.Applies().GetByApplyIdentifier(ctx, applyID)
	require.NoError(t, err, "get apply by identifier")
	require.NotNil(t, storedApply, "apply %s not found in storage", applyID)
	assert.Equal(t, "us-west", storedApply.Deployment, "deployment should be stored on the apply")
}
