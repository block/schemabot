//go:build integration

package localruntime

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"gopkg.in/yaml.v3"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/serve"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/testutil"
)

const runtimeDeadline = 30 * time.Second
const testToken = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// A real CLI process runs an online index build, shuts down with the apply
// active, and recovers against the same durable storage. A later hard crash
// also recovers without losing the deferred cutover or changing the data.
func TestLocalRuntimeRecovery(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "schemabot")
	buildCtx, cancelBuild := context.WithTimeout(t.Context(), runtimeDeadline)
	defer cancelBuild()
	build := exec.CommandContext(buildCtx, "go", "build", "-o", binary, "../../pkg/cmd")
	output, err := build.CombinedOutput()
	require.NoError(t, err, string(output))
	container, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: testutil.MySQLContainerRequest("mysql:8.0", "app"), Started: true,
	})
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, container)
	dsn, err := testutil.MySQLDSN(t.Context(), container, "app", "parseTime=true")
	require.NoError(t, err)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, testutil.PingMySQL(t.Context(), db))
	execSQL(t, db, "CREATE DATABASE `schemabot_state`")
	execSQL(t, db, "CREATE TABLE `orders` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, `status` varchar(32) NOT NULL, `payload` varchar(255) NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	execSQL(t, db, "INSERT INTO `orders` (`status`, `payload`) VALUES ('open', REPEAT('x', 255))")
	for range 19 {
		execSQL(t, db, "INSERT INTO `orders` (`status`, `payload`) SELECT `status`, `payload` FROM `orders`")
	}
	storageDSN, err := testutil.MySQLDSN(t.Context(), container, "schemabot_state", "parseTime=true")
	require.NoError(t, err)
	config := serve.LocalConfig{Storage: api.StorageConfig{DSN: storageDSN}, Databases: map[string]api.DatabaseConfig{
		"app": {Type: "mysql", Environments: map[string]api.EnvironmentConfig{"development": {DSN: dsn}}},
	}}
	data, err := yaml.Marshal(config)
	require.NoError(t, err)
	dir := t.TempDir()
	configPath := filepath.Join(dir, "runtime.yaml")
	tokenPath := filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(configPath, data, 0600))
	require.NoError(t, os.WriteFile(tokenPath, []byte(testToken), 0600))
	start := func() *localProcess { return startLocal(t, binary, configPath, tokenPath) }
	process := start()
	// A conflicting listener fails before storage boot or execution, and leaves
	// the running process reachable.
	collisionCtx, cancelCollision := context.WithTimeout(t.Context(), runtimeDeadline)
	defer cancelCollision()
	collision := exec.CommandContext(collisionCtx, binary, "local", "serve", "--config", configPath, "--token-file", tokenPath, "--listen", strings.TrimPrefix(process.endpoint, "http://"))
	output, err = collision.CombinedOutput()
	require.Error(t, err)
	require.NoError(t, collisionCtx.Err())
	assert.Contains(t, string(output), "listen for local runtime")
	request(t, process.endpoint, http.MethodGet, "/health", testToken, nil, http.StatusOK, nil)
	// Authentication covers probes as well as writes, and the CLI can use its
	// ordinary endpoint/token flags against the local host.
	request(t, process.endpoint, http.MethodGet, "/health", "", nil, http.StatusUnauthorized, nil)
	request(t, process.endpoint, http.MethodPost, "/api/apply", "", apitypes.ApplyRequest{}, http.StatusUnauthorized, nil)
	cliCtx, cancel := context.WithTimeout(t.Context(), runtimeDeadline)
	defer cancel()
	cli := exec.CommandContext(cliCtx, binary, "databases", "--endpoint", process.endpoint, "--token", testToken)
	output, err = cli.CombinedOutput()
	require.NoError(t, err, string(output))
	assert.Contains(t, string(output), "app")
	var plan apitypes.PlanResponse
	request(t, process.endpoint, http.MethodPost, "/api/plan", testToken, apitypes.PlanRequest{
		Database: "app", Environment: "development", Type: "mysql", SchemaFiles: map[string]*apitypes.SchemaFiles{
			"app": {Files: map[string]string{"orders.sql": "CREATE TABLE `orders` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, `status` varchar(32) NOT NULL, `payload` varchar(255) NOT NULL, PRIMARY KEY (`id`), KEY `idx_status` (`status`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"}},
		},
	}, http.StatusOK, &plan)
	require.Empty(t, plan.Errors)
	require.Len(t, plan.Changes, 1)
	var apply apitypes.ApplyResponse
	request(t, process.endpoint, http.MethodPost, "/api/apply", testToken, apitypes.ApplyRequest{
		PlanID: plan.PlanID, Environment: "development", Caller: "untrusted-name", Options: map[string]string{"defer_cutover": "true", "skip_revert": "true"},
	}, http.StatusOK, &apply)
	require.True(t, apply.Accepted, apply.ErrorMessage)
	progress := waitProgress(t, process.endpoint, apply.ApplyID, func(p apitypes.ProgressResponse) bool {
		for _, table := range p.Tables {
			if table.RowsCopied > 0 && table.RowsCopied < 524288 {
				return true
			}
		}
		return false
	})
	assert.Equal(t, "local-runtime", progress.Caller)
	process.stop(t, syscall.SIGTERM)
	process = start()
	waitProgress(t, process.endpoint, apply.ApplyID, func(p apitypes.ProgressResponse) bool { return state.IsState(p.State, state.Apply.WaitingForCutover) })
	process.stop(t, syscall.SIGKILL)
	// Advance the persisted heartbeat past the existing lease window after the
	// process is confirmed dead, avoiding a wall-clock lease wait in the test.
	execSQL(t, db, "UPDATE `schemabot_state`.`applies` SET `updated_at` = NOW() - INTERVAL 1 HOUR")
	execSQL(t, db, "UPDATE `schemabot_state`.`apply_operations` SET `updated_at` = NOW() - INTERVAL 1 HOUR")
	process = start()
	waitProgress(t, process.endpoint, apply.ApplyID, func(p apitypes.ProgressResponse) bool { return state.IsState(p.State, state.Apply.WaitingForCutover) })
	var control apitypes.ControlResponse
	request(t, process.endpoint, http.MethodPost, "/api/cutover", testToken, apitypes.ControlRequest{ApplyID: apply.ApplyID, Environment: "development"}, http.StatusOK, &control)
	require.True(t, control.Accepted, control.ErrorMessage)
	waitProgress(t, process.endpoint, apply.ApplyID, func(p apitypes.ProgressResponse) bool { return state.IsState(p.State, state.Apply.Completed) })
	var count, incorrectRows int64
	queryCtx, queryCancel := context.WithTimeout(t.Context(), runtimeDeadline)
	defer queryCancel()
	require.NoError(t, db.QueryRowContext(queryCtx, "SELECT COUNT(*), SUM(`payload` <> REPEAT('x', 255) OR `status` <> 'open') FROM `orders`").Scan(&count, &incorrectRows))
	assert.EqualValues(t, 524288, count)
	assert.Zero(t, incorrectRows)
	var name, ddl string
	require.NoError(t, db.QueryRowContext(queryCtx, "SHOW CREATE TABLE `orders`").Scan(&name, &ddl))
	assert.Contains(t, ddl, "KEY `idx_status` (`status`)")
	process.stop(t, syscall.SIGTERM)
}

func execSQL(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), runtimeDeadline)
	defer cancel()
	_, err := db.ExecContext(ctx, query)
	require.NoError(t, err)
}

type localProcess struct {
	cmd      *exec.Cmd
	done     chan error
	endpoint string
	stopped  bool
}

func startLocal(t *testing.T, binary, config, token string) *localProcess {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), binary, "local", "serve", "--config", config, "--token-file", token)
	// An unrelated broken profile must never authenticate a server invocation.
	cmd.Env = append(os.Environ(), "SCHEMABOT_PROFILE=unrelated-profile")
	logs, err := os.CreateTemp(t.TempDir(), "runtime-*.log")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, logs.Close()) })
	cmd.Stderr = logs
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	p := &localProcess{cmd: cmd, done: make(chan error, 1)}
	go func() { p.done <- cmd.Wait() }()
	t.Cleanup(func() {
		if !p.stopped {
			require.NoError(t, cmd.Process.Kill())
			select {
			case <-p.done:
			case <-time.After(runtimeDeadline):
				require.FailNow(t, "local child did not exit")
			}
		}
		if t.Failed() {
			data, err := os.ReadFile(logs.Name())
			require.NoError(t, err)
			t.Log(string(data))
		}
	})
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			ready <- scanner.Text()
		} else {
			ready <- ""
		}
	}()
	select {
	case line := <-ready:
		var record struct {
			State    string `json:"state"`
			Endpoint string `json:"endpoint"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &record), line)
		require.Equal(t, "ready", record.State)
		require.True(t, strings.HasPrefix(record.Endpoint, "http://127.0.0.1:"))
		p.endpoint = record.Endpoint
	case <-time.After(runtimeDeadline):
		require.FailNow(t, "local runtime did not become ready")
	}
	return p
}

func (p *localProcess) stop(t *testing.T, signal os.Signal) {
	t.Helper()
	require.NoError(t, p.cmd.Process.Signal(signal))
	select {
	case err := <-p.done:
		p.stopped = true
		if signal == syscall.SIGTERM {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	case <-time.After(runtimeDeadline):
		require.FailNow(t, "local runtime did not stop")
	}
}

func request(t *testing.T, endpoint, method, path, token string, payload any, want int, result any) {
	t.Helper()
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		require.NoError(t, err)
		body = bytes.NewReader(data)
	}
	ctx, cancel := context.WithTimeout(t.Context(), runtimeDeadline)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, endpoint+path, body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, want, resp.StatusCode, string(data))
	if result != nil {
		require.NoError(t, json.Unmarshal(data, result), string(data))
	}
}

func waitProgress(t *testing.T, endpoint, id string, predicate func(apitypes.ProgressResponse) bool) apitypes.ProgressResponse {
	t.Helper()
	var progress apitypes.ProgressResponse
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(runtimeDeadline)
	defer deadline.Stop()
	for {
		request(t, endpoint, http.MethodGet, fmt.Sprintf("/api/progress/apply/%s", id), testToken, nil, http.StatusOK, &progress)
		require.False(t, state.IsState(progress.State, state.Apply.Failed), progress.ErrorMessage)
		if predicate(progress) {
			return progress
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			require.FailNow(t, "progress deadline exceeded", "%+v", progress)
		}
	}
}
