//go:build integration

package localruntime

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	runtimehost "github.com/block/schemabot/pkg/localruntime"
	"github.com/block/schemabot/pkg/localsetup"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/testutil"
)

// Independent CLI invocations share one detached runtime. The same profile
// carries a real plan and apply on either engine, then reconnects after a
// graceful stop without losing completed work.
func TestSupervisorEngines(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "schemabot")
	buildCtx, cancelBuild := context.WithTimeout(t.Context(), runtimeDeadline)
	defer cancelBuild()
	output, err := exec.CommandContext(buildCtx, "go", "build", "-o", binary, "../../pkg/cmd").CombinedOutput()
	require.NoError(t, err, string(output))
	for _, engine := range []string{"mysql", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			storageDSN, targetDSN, db := supervisorDatabase(t, engine)
			home := t.TempDir()
			dir := filepath.Join(home, ".schemabot", "runtimes", "shared")
			manager := runtimehost.Manager{Dir: dir, Binary: binary, Version: "dev"}
			changed, err := localsetup.Register(manager, localsetup.Registration{
				Database: "app", Environment: "development", Engine: engine,
				Storage:    api.StorageConfig{Dialect: engine, DSN: storageDSN},
				Connection: api.EnvironmentConfig{DSN: targetDSN},
			})
			require.NoError(t, err)
			require.True(t, changed)
			require.NoError(t, os.WriteFile(filepath.Join(home, ".schemabot", "config.yaml"), []byte("profiles:\n  alpha:\n    local_runtime: shared\n  beta:\n    local_runtime: shared\n"), 0600))
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), runtimeDeadline)
				defer cancel()
				err := manager.Stop(ctx)
				if err != nil {
					logs, readErr := os.ReadFile(filepath.Join(dir, "runtime.log"))
					assert.NoError(t, readErr)
					t.Log(string(logs))
				}
				assert.NoError(t, err)
			})
			ctx, cancel := context.WithTimeout(t.Context(), runtimeDeadline)
			defer cancel()
			type result struct {
				output []byte
				err    error
			}
			results := make(chan result, 2)
			var wg sync.WaitGroup
			for _, profile := range []string{"alpha", "beta"} {
				wg.Go(func() {
					cmd := exec.CommandContext(ctx, binary, "databases", "--profile", profile)
					cmd.Env = append(os.Environ(), "HOME="+home, "SCHEMABOT_ENDPOINT=", "SCHEMABOT_TOKEN=", "SCHEMABOT_PROFILE=")
					out, err := cmd.CombinedOutput()
					results <- result{out, err}
				})
			}
			wg.Wait()
			close(results)
			for r := range results {
				require.NoError(t, r.err, string(r.output))
				assert.Contains(t, string(r.output), "app")
			}
			// Both initiating processes have exited; the runtime is still ready.
			connection, err := manager.Ensure(ctx)
			require.NoError(t, err)
			again, err := manager.Ensure(ctx)
			require.NoError(t, err)
			assert.Equal(t, connection.Generation, again.Generation)
			namespace := "app"
			if engine == "postgres" {
				namespace = "public"
			}
			var plan apitypes.PlanResponse
			request(t, connection.Endpoint, http.MethodPost, "/api/plan", connection.Token, apitypes.PlanRequest{Database: "app", Environment: "development", Type: engine, SchemaFiles: map[string]*apitypes.SchemaFiles{namespace: {Files: map[string]string{"widgets.sql": "CREATE TABLE widgets (id bigint NOT NULL PRIMARY KEY, name text NOT NULL);"}}}}, http.StatusOK, &plan)
			require.Empty(t, plan.Errors)
			require.Len(t, plan.Changes, 1)
			var apply apitypes.ApplyResponse
			request(t, connection.Endpoint, http.MethodPost, "/api/apply", connection.Token, apitypes.ApplyRequest{PlanID: plan.PlanID, Environment: "development"}, http.StatusOK, &apply)
			require.True(t, apply.Accepted, apply.ErrorMessage)
			// waitProgress uses the foreground fixture token; supervised generations
			// have their own credentials, so inspect with the shared request helper.
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, connection.Endpoint+"/api/progress/apply/"+apply.ApplyID, nil)
				if !assert.NoError(collect, err) {
					return
				}
				req.Header.Set("Authorization", "Bearer "+connection.Token)
				resp, err := http.DefaultClient.Do(req)
				if !assert.NoError(collect, err) {
					return
				}
				defer resp.Body.Close()
				var progress apitypes.ProgressResponse
				if assert.NoError(collect, json.NewDecoder(resp.Body).Decode(&progress)) {
					assert.True(collect, state.IsState(progress.State, state.Apply.Completed))
				}
			}, runtimeDeadline, 100*time.Millisecond)
			verifyCtx, cancelVerify := context.WithTimeout(t.Context(), runtimeDeadline)
			defer cancelVerify()
			var count int
			require.NoError(t, db.QueryRowContext(verifyCtx, "SELECT COUNT(*) FROM widgets").Scan(&count))
			assert.Zero(t, count)
			// Exported after startup: the running child cannot inherit this variable.
			t.Setenv("LIVE_NEW_TARGET", targetDSN)
			addition := localsetup.Registration{Database: "billing", Environment: "development", Engine: engine, Storage: api.StorageConfig{DSN: storageDSN, Dialect: engine}, Connection: api.EnvironmentConfig{DSN: "env:LIVE_NEW_TARGET"}}
			if engine == "mysql" {
				// Hold a real online copy at cutover while another database is added.
				execSQL(t, db, "INSERT INTO widgets (id,name) VALUES (1,'keep me')")
				for i := range 19 {
					execSQL(t, db, fmt.Sprintf("INSERT INTO widgets (id,name) SELECT id + %d,name FROM widgets", 1<<i))
				}
				var onlinePlan apitypes.PlanResponse
				request(t, connection.Endpoint, http.MethodPost, "/api/plan", connection.Token, apitypes.PlanRequest{Database: "app", Environment: "development", Type: engine, SchemaFiles: map[string]*apitypes.SchemaFiles{namespace: {Files: map[string]string{"widgets.sql": "CREATE TABLE widgets (id bigint NOT NULL PRIMARY KEY, name text NOT NULL, INDEX idx_name (name(10)));"}}}}, http.StatusOK, &onlinePlan)
				require.Empty(t, onlinePlan.Errors)
				require.Len(t, onlinePlan.Changes, 1)
				var onlineApply apitypes.ApplyResponse
				request(t, connection.Endpoint, http.MethodPost, "/api/apply", connection.Token, apitypes.ApplyRequest{PlanID: onlinePlan.PlanID, Environment: "development", Options: map[string]string{"defer_cutover": "true", "skip_revert": "true"}}, http.StatusOK, &onlineApply)
				require.True(t, onlineApply.Accepted, onlineApply.ErrorMessage)
				waitSupervisedApply(t, connection, onlineApply.ApplyID, state.Apply.WaitingForCutover)
				changed, err = localsetup.Register(manager, addition)
				require.NoError(t, err)
				require.True(t, changed)
				waitSupervisedApply(t, connection, onlineApply.ApplyID, state.Apply.WaitingForCutover)
				var control apitypes.ControlResponse
				request(t, connection.Endpoint, http.MethodPost, "/api/cutover", connection.Token, apitypes.ControlRequest{ApplyID: onlineApply.ApplyID, Environment: "development"}, http.StatusOK, &control)
				require.True(t, control.Accepted, control.ErrorMessage)
				waitSupervisedApply(t, connection, onlineApply.ApplyID, state.Apply.Completed)
				var kept int
				require.NoError(t, db.QueryRowContext(verifyCtx, "SELECT COUNT(*) FROM widgets WHERE name='keep me'").Scan(&kept))
				require.Equal(t, 524288, kept)
			} else {
				changed, err = localsetup.Register(manager, addition)
				require.NoError(t, err)
				require.True(t, changed)
			}
			updated, err := manager.Ensure(verifyCtx)
			require.NoError(t, err)
			require.Equal(t, connection.Generation, updated.Generation)
			require.Equal(t, connection.PID, updated.PID)
			changed, err = localsetup.Register(manager, addition)
			require.NoError(t, err)
			require.False(t, changed)
			saved, err := runtimehost.ReadPrivate(filepath.Join(dir, "runtime.yaml"))
			require.NoError(t, err)
			require.Contains(t, string(saved), "env:LIVE_NEW_TARGET")
			var catalog apitypes.DatabaseListResponse
			request(t, connection.Endpoint, http.MethodGet, "/api/databases", connection.Token, nil, http.StatusOK, &catalog)
			require.Len(t, catalog.Databases, 2)
			var billingPlan apitypes.PlanResponse
			request(t, connection.Endpoint, http.MethodPost, "/api/plan", connection.Token, apitypes.PlanRequest{Database: "billing", Environment: "development", Type: engine, SchemaFiles: map[string]*apitypes.SchemaFiles{namespace: {Files: map[string]string{"widgets.sql": "CREATE TABLE widgets (id bigint NOT NULL PRIMARY KEY, name text NOT NULL);"}}}}, http.StatusOK, &billingPlan)
			require.Empty(t, billingPlan.Errors)

			require.NoError(t, manager.Stop(verifyCtx))
			record, err := manager.Status(verifyCtx)
			require.NoError(t, err)
			assert.Equal(t, "stopped", record.State)
			restarted, err := manager.Ensure(verifyCtx)
			require.NoError(t, err)
			assert.NotEqual(t, connection.Generation, restarted.Generation)
			var progress apitypes.ProgressResponse
			request(t, restarted.Endpoint, http.MethodGet, "/api/progress/apply/"+apply.ApplyID, restarted.Token, nil, http.StatusOK, &progress)
			assert.True(t, state.IsState(progress.State, state.Apply.Completed))
		})
	}
}

func supervisorDatabase(t *testing.T, engine string) (string, string, *sql.DB) {
	t.Helper()
	if engine == "postgres" {
		storageDSN, _ := testutil.StartPostgres(t, "runtime_state")
		targetDSN, db := testutil.StartPostgres(t, "app")
		return storageDSN, targetDSN, db
	}
	container, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{ContainerRequest: testutil.MySQLContainerRequest("mysql:8.0", "app"), Started: true})
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, container)
	dsn, err := testutil.MySQLDSN(t.Context(), container, "app", "parseTime=true")
	require.NoError(t, err)
	db, err := mysqlconn.Open(dsn)
	require.NoError(t, err)
	require.NoError(t, testutil.PingMySQL(t.Context(), db))
	t.Cleanup(func() { assert.NoError(t, db.Close()) })
	execSQL(t, db, "CREATE DATABASE `runtime_state`")
	storageDSN, err := testutil.MySQLDSN(t.Context(), container, "runtime_state", "parseTime=true")
	require.NoError(t, err)
	return storageDSN, dsn, db
}

func waitSupervisedApply(t *testing.T, connection runtimehost.Connection, applyID, wanted string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var progress apitypes.ProgressResponse
		request(t, connection.Endpoint, http.MethodGet, "/api/progress/apply/"+applyID, connection.Token, nil, http.StatusOK, &progress)
		return state.IsState(progress.State, wanted)
	}, runtimeDeadline, 100*time.Millisecond)
}
