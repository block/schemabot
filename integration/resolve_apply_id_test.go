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
	schemabotmysql "github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// TestResolveApplyID_ControlOperations verifies the full gRPC mode flow:
//
//  1. Plan + Apply through SchemaBot HTTP API → GRPCClient → remote Tern
//  2. SchemaBot generates its own apply_identifier, stores Tern's as external_id
//  3. apply_identifier != external_id (the whole point of resolveApplyID)
//  4. Progress (with apply_id, by apply path, by database) all work correctly
//  5. Apply completes end-to-end (Spirit creates the table)
//
// Uses a real Tern gRPC server backed by LocalClient + Spirit + MySQL.
func TestResolveApplyID_ControlOperations(t *testing.T) {
	ctx := t.Context()

	// 1. Use the global Tern gRPC server (set up by TestMain).
	// It targets "testdb" on the target MySQL — use unique table names to avoid conflicts.
	appDBName := "testdb"
	ternGRPCAddr := grpcAddr

	// 2. Create SchemaBot with real MySQL storage + GRPCClient.
	schemabotDB, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err, "open schemabot db")
	t.Cleanup(func() { utils.CloseAndLog(schemabotDB) })

	clearStorageDB(t, schemabotDB)
	st := schemabotmysql.New(schemabotDB)

	ternClient, err := tern.NewGRPCClient(tern.Config{Address: ternGRPCAddr, Storage: st})
	require.NoError(t, err, "create tern client")
	t.Cleanup(func() { utils.CloseAndLog(ternClient) })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	serverConfig := &schemabotapi.ServerConfig{
		TernDeployments: schemabotapi.TernConfig{
			"default": {"staging": ternGRPCAddr},
		},
	}
	svc := schemabotapi.New(st, serverConfig, map[string]tern.Client{
		"default/staging": ternClient,
	}, logger)
	t.Cleanup(func() { utils.CloseAndLog(svc) })

	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	httpLis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "localhost:0")
	require.NoError(t, err, "listen")
	httpSrv := &http.Server{Handler: mux}
	go func() { _ = httpSrv.Serve(httpLis) }()
	t.Cleanup(func() { utils.CloseAndLog(httpSrv) })
	baseURL := "http://" + httpLis.Addr().String()

	// Wait for HTTP server to be ready (poll, no sleep).
	readyDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(readyDeadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/health", nil)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 3. Plan: create a table with a unique name to avoid conflicts with other tests.
	tableName := fmt.Sprintf("resolve_%d", time.Now().UnixNano()%100000)
	schemaSQL := fmt.Sprintf(`CREATE TABLE %s (
		id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		name VARCHAR(255) NOT NULL
	);`, tableName)
	planReq := map[string]any{
		"database":    appDBName,
		"environment": "staging",
		"type":        "mysql",
		"schema_files": map[string]any{
			"default": map[string]any{
				"files": map[string]string{
					tableName + ".sql": schemaSQL,
				},
			},
		},
		"repository":   "test/resolve",
		"pull_request": 1,
	}
	planCode, planBody := doPost(t, ctx, baseURL, "/api/plan", planReq)
	require.Equal(t, http.StatusOK, planCode, string(planBody))
	var planResult map[string]any
	require.NoError(t, json.Unmarshal(planBody, &planResult))
	planID, ok := planResult["plan_id"].(string)
	require.True(t, ok && planID != "", "plan response missing plan_id: %v", planResult)
	// Verify plan has changes (should produce CREATE TABLE for the new table).
	changes, _ := planResult["changes"].([]any)
	require.NotEmpty(t, changes, "plan should have DDL changes, got: %s", string(planBody))

	// 4. Apply.
	applyCode, applyBody := doPost(t, ctx, baseURL, "/api/apply", map[string]any{
		"plan_id":     planID,
		"environment": "staging",
	})
	require.Equal(t, http.StatusOK, applyCode, string(applyBody))
	var applyResult map[string]any
	require.NoError(t, json.Unmarshal(applyBody, &applyResult))
	require.Equal(t, true, applyResult["accepted"], "apply not accepted: %v", applyResult)
	applyIdentifier, ok := applyResult["apply_id"].(string)
	require.True(t, ok && applyIdentifier != "", "apply response missing apply_id")

	// 5. Verify apply_identifier != external_id.
	storedApply, err := st.Applies().GetByApplyIdentifier(ctx, applyIdentifier)
	require.NoError(t, err)
	require.NotNil(t, storedApply, "apply not found in storage")
	require.NotEmpty(t, storedApply.ExternalID, "external_id should be set by remote Tern")
	require.NotEqual(t, applyIdentifier, storedApply.ExternalID,
		"apply_identifier and external_id must differ — resolveApplyID translates between them")
	t.Logf("apply_identifier=%s external_id=%s", applyIdentifier, storedApply.ExternalID)

	// 6. Progress with apply_id query param.
	t.Run("progress with apply_id resolves external_id", func(t *testing.T) {
		code, body := doGet(t, ctx, baseURL, "/api/progress/"+appDBName+"?environment=staging&apply_id="+applyIdentifier)
		t.Logf("progress response (%d): %s", code, string(body))
		require.Equal(t, http.StatusOK, code, string(body))

		var progress map[string]any
		require.NoError(t, json.Unmarshal(body, &progress))
		state, _ := progress["state"].(string)
		assert.NotEmpty(t, state, "expected non-empty state in progress response")
	})

	// 7. Progress by apply path.
	t.Run("progress by apply path resolves external_id", func(t *testing.T) {
		code, body := doGet(t, ctx, baseURL, "/api/progress/apply/"+applyIdentifier)
		require.Equal(t, http.StatusOK, code, string(body))

		var progress map[string]any
		require.NoError(t, json.Unmarshal(body, &progress))
		state, _ := progress["state"].(string)
		assert.NotEmpty(t, state)
	})

	// 8. Progress by database (no apply_id).
	t.Run("progress by database finds active apply", func(t *testing.T) {
		code, body := doGet(t, ctx, baseURL, "/api/progress/"+appDBName+"?environment=staging")
		require.Equal(t, http.StatusOK, code, string(body))

		var progress map[string]any
		require.NoError(t, json.Unmarshal(body, &progress))
		state, _ := progress["state"].(string)
		assert.NotEmpty(t, state)
	})

	// 9. Wait for completion.
	waitForState(t, baseURL, appDBName, "staging", "completed", 3*time.Minute)

	// 10. Verify the table was actually created.
	testdbDSN := strings.Replace(targetDSN, "/target_test", "/testdb", 1)
	appDB, err := sql.Open("mysql", testdbDSN)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(appDB) })

	var count int
	err = appDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		appDBName, tableName).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "expected %q table to exist after apply", tableName)
}

// doPost sends a POST request and returns status code + body.
func doPost(t *testing.T, ctx context.Context, baseURL, path string, body any) (int, []byte) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(b))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, respBody
}

// doGet sends a GET request and returns status code + body.
func doGet(t *testing.T, ctx context.Context, baseURL, path string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, respBody
}
