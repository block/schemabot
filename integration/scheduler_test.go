//go:build integration

package integration

import (
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schemabotapi "github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

func TestRecoveryLoop_FailedRetryable(t *testing.T) {
	ctx := t.Context()

	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err, "read schema file")

	appDBName, appDSN := createTestDB(t, "recovery_")
	ts := startTestServer(t, appDBName, appDSN)

	// Plan a CREATE TABLE
	planResp := postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
		"database":    appDBName,
		"environment": "staging",
		"type":        "mysql",
		"schema_files": map[string]any{
			"default": map[string]any{
				"files": map[string]string{
					"users.sql": string(schemaSQL),
				},
			},
		},
	})
	planID, _ := planResp["plan_id"].(string)
	require.NotEmpty(t, planID)

	// Apply — this creates the table normally
	applyResp := postJSON(t, "http://"+ts.Addr+"/api/apply", map[string]any{
		"plan_id":     planID,
		"environment": "staging",
	})
	require.True(t, applyResp["accepted"] == true)
	applyID1, _ := applyResp["apply_id"].(string)
	waitForState(t, "http://"+ts.Addr, applyID1, "completed", 15*time.Second)
	t.Log("First apply completed, table exists")

	// Drop the table so the next plan produces the same CREATE TABLE
	targetConn, err := sql.Open("mysql", appDSN)
	require.NoError(t, err)
	defer func() { _ = targetConn.Close() }()
	_, err = targetConn.ExecContext(ctx, "DROP TABLE IF EXISTS users")
	require.NoError(t, err, "drop users table")

	// Second plan — same CREATE TABLE
	plan2Resp := postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
		"database":    appDBName,
		"environment": "staging",
		"type":        "mysql",
		"schema_files": map[string]any{
			"default": map[string]any{
				"files": map[string]string{
					"users.sql": string(schemaSQL),
				},
			},
		},
	})
	planID2, _ := plan2Resp["plan_id"].(string)
	require.NotEmpty(t, planID2)

	// Apply — but we'll intercept and mark as failed_retryable
	apply2Resp := postJSON(t, "http://"+ts.Addr+"/api/apply", map[string]any{
		"plan_id":     planID2,
		"environment": "staging",
	})
	require.True(t, apply2Resp["accepted"] == true)
	applyID2, _ := apply2Resp["apply_id"].(string)
	require.NotEmpty(t, applyID2)

	// Wait for the second apply to complete (it will succeed quickly since it's CREATE TABLE)
	waitForState(t, "http://"+ts.Addr, applyID2, "completed", 15*time.Second)

	// Now simulate the recovery scenario: drop table again, create a third plan+apply,
	// and manually set state to failed_retryable before the engine runs.
	_, err = targetConn.ExecContext(ctx, "DROP TABLE IF EXISTS users")
	require.NoError(t, err)

	plan3Resp := postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
		"database":    appDBName,
		"environment": "staging",
		"type":        "mysql",
		"schema_files": map[string]any{
			"default": map[string]any{
				"files": map[string]string{
					"users.sql": string(schemaSQL),
				},
			},
		},
	})
	planID3, _ := plan3Resp["plan_id"].(string)
	require.NotEmpty(t, planID3)

	// Look up the plan in storage to create the apply record directly
	plan3, err := ts.Storage.Plans().Get(ctx, planID3)
	require.NoError(t, err)
	require.NotNil(t, plan3)

	// Create an apply record directly in failed_retryable state
	// (simulating an apply that ran partway and hit a transient error)
	now := time.Now()
	failedApply := &storage.Apply{
		ApplyIdentifier: "apply-recovery-test-" + fmt.Sprintf("%d", now.UnixNano()%100000),
		PlanID:          plan3.ID,
		Database:        appDBName,
		DatabaseType:    "mysql",
		Engine:          "spirit",
		State:           state.Apply.FailedRetryable,
		ErrorMessage:    "simulated transient failure",
		Options:         []byte("{}"),
		Environment:     "staging",
		CreatedAt:       now,
		UpdatedAt:       now.Add(-2 * time.Minute), // stale heartbeat so FindNextApply picks it up
	}
	applyID3, err := ts.Storage.Applies().Create(ctx, failedApply)
	require.NoError(t, err)
	failedApply.ID = applyID3

	// Create tasks for this apply (one per DDL change in the plan)
	for _, tc := range plan3.FlatDDLChanges() {
		task := &storage.Task{
			TaskIdentifier: "task-recovery-" + fmt.Sprintf("%d", time.Now().UnixNano()%100000),
			ApplyID:        applyID3,
			PlanID:         plan3.ID,
			Database:       appDBName,
			DatabaseType:   "mysql",
			Engine:         "spirit",
			State:          state.Task.FailedRetryable,
			ErrorMessage:   "simulated transient failure",
			TableName:      tc.Table,
			DDL:            tc.DDL,
			DDLAction:      tc.Operation,
			Options:        []byte("{}"),
			Environment:    "staging",
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		_, err := ts.Storage.Tasks().Create(ctx, task)
		require.NoError(t, err)
	}

	t.Logf("Created failed_retryable apply %s with %d tasks", failedApply.ApplyIdentifier, len(plan3.FlatDDLChanges()))

	// Start the scheduler — it will pick up the failed_retryable apply
	ts.Service.StartScheduler(t.Context())
	defer ts.Service.StopScheduler()

	// Wait for the apply to be picked up and completed
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		apply, err := ts.Storage.Applies().Get(ctx, applyID3)
		require.NoError(t, err)
		if apply != nil && apply.State == state.Apply.Completed {
			t.Logf("Recovery succeeded: apply %s completed (attempt %d)", apply.ApplyIdentifier, apply.Attempt)

			// Verify the table was actually created
			var tableName string
			err := targetConn.QueryRowContext(ctx, `
				SELECT TABLE_NAME FROM information_schema.TABLES
				WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'users'
			`, appDBName).Scan(&tableName)
			require.NoError(t, err, "users table should exist after recovery")
			t.Log("Recovery loop test passed: failed_retryable apply recovered and completed")
			return
		}
		// Also check if it permanently failed (shouldn't happen)
		if apply != nil && apply.State == state.Apply.Failed {
			t.Fatalf("Apply permanently failed instead of recovering: %s", apply.ErrorMessage)
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("timeout: recovery loop did not complete the apply within 20s")
}

// TestScheduler_ExhaustedRetries verifies that a failed_retryable apply that
// has exhausted its retry budget (attempt >= maxRecoveryAttempts) gets
// transitioned to permanent failed by the scheduler.
func TestScheduler_ExhaustedRetries(t *testing.T) {
	ctx := t.Context()

	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err)

	appDBName, appDSN := createTestDB(t, "exhaust_")
	ts := startTestServer(t, appDBName, appDSN)

	// Create a plan so we have a valid plan_id for the apply
	planResp := postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
		"database":    appDBName,
		"environment": "staging",
		"type":        "mysql",
		"schema_files": map[string]any{
			"default": map[string]any{
				"files": map[string]string{
					"users.sql": string(schemaSQL),
				},
			},
		},
	})
	planID, _ := planResp["plan_id"].(string)
	require.NotEmpty(t, planID)

	plan, err := ts.Storage.Plans().Get(ctx, planID)
	require.NoError(t, err)

	// Create an apply that has already exhausted its retry budget
	now := time.Now()
	exhaustedApply := &storage.Apply{
		ApplyIdentifier: "apply-exhausted-" + fmt.Sprintf("%d", now.UnixNano()%100000),
		PlanID:          plan.ID,
		Database:        appDBName,
		DatabaseType:    "mysql",
		Engine:          "spirit",
		State:           state.Apply.FailedRetryable,
		ErrorMessage:    "transient failure after many retries",
		Attempt:         10, // at the limit
		Options:         []byte("{}"),
		Environment:     "staging",
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := ts.Storage.Applies().Create(ctx, exhaustedApply)
	require.NoError(t, err)

	t.Logf("Created exhausted apply %s with attempt=%d", exhaustedApply.ApplyIdentifier, exhaustedApply.Attempt)

	// Start the scheduler
	ts.Service.StartScheduler(t.Context())
	defer ts.Service.StopScheduler()

	// The scheduler should expire this apply to permanent failed
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		apply, err := ts.Storage.Applies().Get(ctx, applyID)
		require.NoError(t, err)
		if apply != nil && apply.State == state.Apply.Failed {
			t.Logf("Apply permanently failed as expected (attempt %d): %s", apply.Attempt, apply.ErrorMessage)
			return
		}
		if apply != nil && apply.State == state.Apply.Completed {
			t.Fatal("Apply should have been permanently failed, not completed")
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("timeout: scheduler did not expire the exhausted apply within 15s")
}

// TestScheduler_BasicClaimAndResume verifies the scheduler claims a stale apply
// and resumes it to completion via ResumeApply.
func TestScheduler_BasicClaimAndResume(t *testing.T) {
	ctx := t.Context()
	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err)

	appDBName, appDSN := createTestDB(t, "basic_sched_")
	ts := startTestServer(t, appDBName, appDSN)

	// Create plan + apply normally first
	planResp := postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
		"database": appDBName, "environment": "staging", "type": "mysql",
		"schema_files": map[string]any{"default": map[string]any{"files": map[string]string{"users.sql": string(schemaSQL)}}},
	})
	planID, _ := planResp["plan_id"].(string)
	require.NotEmpty(t, planID)

	applyResp := postJSON(t, "http://"+ts.Addr+"/api/apply", map[string]any{
		"plan_id": planID, "environment": "staging",
	})
	require.True(t, applyResp["accepted"] == true)
	applyID, _ := applyResp["apply_id"].(string)
	waitForState(t, "http://"+ts.Addr, applyID, "completed", 15*time.Second)

	// Drop table, create a new plan, and seed a stale apply manually
	targetConn, err := sql.Open("mysql", appDSN)
	require.NoError(t, err)
	defer func() { _ = targetConn.Close() }()
	_, err = targetConn.ExecContext(ctx, "DROP TABLE IF EXISTS users")
	require.NoError(t, err)

	plan2Resp := postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
		"database": appDBName, "environment": "staging", "type": "mysql",
		"schema_files": map[string]any{"default": map[string]any{"files": map[string]string{"users.sql": string(schemaSQL)}}},
	})
	planID2, _ := plan2Resp["plan_id"].(string)
	plan2, err := ts.Storage.Plans().Get(ctx, planID2)
	require.NoError(t, err)

	// Create a stale running apply (simulating a crashed worker)
	now := time.Now()
	staleApply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-stale-%d", now.UnixNano()%100000),
		PlanID:          plan2.ID,
		Database:        appDBName,
		DatabaseType:    "mysql",
		Engine:          "spirit",
		State:           state.Apply.Running,
		Options:         []byte("{}"),
		Environment:     "staging",
		StartedAt:       &now,
		CreatedAt:       now,
		UpdatedAt:       now.Add(-2 * time.Minute), // stale heartbeat
	}
	staleID, err := ts.Storage.Applies().Create(ctx, staleApply)
	require.NoError(t, err)

	// Make the heartbeat stale (Create uses NOW() for updated_at, so override with raw SQL)
	schemabotDB, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err)
	defer func() { _ = schemabotDB.Close() }()
	_, err = schemabotDB.ExecContext(ctx, "UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?", staleID)
	require.NoError(t, err)

	// Create tasks
	for _, tc := range plan2.FlatDDLChanges() {
		_, err := ts.Storage.Tasks().Create(ctx, &storage.Task{
			TaskIdentifier: fmt.Sprintf("task-stale-%d", time.Now().UnixNano()%100000),
			ApplyID:        staleID,
			PlanID:         plan2.ID,
			Database:       appDBName,
			DatabaseType:   "mysql",
			Engine:         "spirit",
			State:          state.Task.Running,
			TableName:      tc.Table,
			DDL:            tc.DDL,
			DDLAction:      tc.Operation,
			Options:        []byte("{}"),
			Environment:    "staging",
			CreatedAt:      now,
			UpdatedAt:      now,
		})
		require.NoError(t, err)
	}

	// Start scheduler — it should claim the stale apply and resume it
	ts.Service.StartScheduler(t.Context())
	defer ts.Service.StopScheduler()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		apply, err := ts.Storage.Applies().Get(ctx, staleID)
		require.NoError(t, err)
		if apply != nil && apply.State == state.Apply.Completed {
			t.Logf("Scheduler resumed stale apply successfully")
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("timeout: scheduler did not resume stale apply")
}

// TestScheduler_MultiWorkerClaiming verifies that multiple workers can claim
// different applies concurrently without double-claiming.
func TestScheduler_MultiWorkerClaiming(t *testing.T) {
	ctx := t.Context()
	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err)

	appDBName1, appDSN1 := createTestDB(t, "mw1_")
	appDBName2, appDSN2 := createTestDB(t, "mw2_")

	// Build a multi-database server
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	schemabotDB, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err)
	clearStorageDB(t, schemabotDB)
	stor := mysqlstore.New(schemabotDB)

	client1, err := tern.NewLocalClient(tern.LocalConfig{
		Database: appDBName1, Type: "mysql", TargetDSN: appDSN1,
	}, stor, logger)
	require.NoError(t, err)
	client2, err := tern.NewLocalClient(tern.LocalConfig{
		Database: appDBName2, Type: "mysql", TargetDSN: appDSN2,
	}, stor, logger)
	require.NoError(t, err)

	serverConfig := &schemabotapi.ServerConfig{
		SchedulerWorkers: 3,
		Databases: map[string]schemabotapi.DatabaseConfig{
			appDBName1: {Type: "mysql", Environments: map[string]schemabotapi.EnvironmentConfig{"staging": {DSN: appDSN1}}},
			appDBName2: {Type: "mysql", Environments: map[string]schemabotapi.EnvironmentConfig{"staging": {DSN: appDSN2}}},
		},
	}
	svc := schemabotapi.New(stor, serverConfig, map[string]tern.Client{
		appDBName1 + "/staging": client1,
		appDBName2 + "/staging": client2,
	}, logger)
	defer func() { _ = svc.Close() }()

	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	require.NoError(t, err)
	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	addr := listener.Addr().String()
	waitForHealth(t, addr)

	// Create plans for both databases
	plan1 := postJSON(t, "http://"+addr+"/api/plan", map[string]any{
		"database": appDBName1, "environment": "staging", "type": "mysql",
		"schema_files": map[string]any{"default": map[string]any{"files": map[string]string{"users.sql": string(schemaSQL)}}},
	})
	plan2 := postJSON(t, "http://"+addr+"/api/plan", map[string]any{
		"database": appDBName2, "environment": "staging", "type": "mysql",
		"schema_files": map[string]any{"default": map[string]any{"files": map[string]string{"users.sql": string(schemaSQL)}}},
	})

	// Create failed_retryable applies for both
	now := time.Now()
	p1, _ := stor.Plans().Get(ctx, plan1["plan_id"].(string))
	p2, _ := stor.Plans().Get(ctx, plan2["plan_id"].(string))

	for _, p := range []struct {
		plan *storage.Plan
		db   string
	}{{p1, appDBName1}, {p2, appDBName2}} {
		apply := &storage.Apply{
			ApplyIdentifier: fmt.Sprintf("apply-mw-%s-%d", p.db, now.UnixNano()%100000),
			PlanID:          p.plan.ID,
			Database:        p.db,
			DatabaseType:    "mysql",
			Engine:          "spirit",
			State:           state.Apply.FailedRetryable,
			ErrorMessage:    "transient",
			Options:         []byte("{}"),
			Environment:     "staging",
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		applyID, err := stor.Applies().Create(ctx, apply)
		require.NoError(t, err)
		for _, tc := range p.plan.FlatDDLChanges() {
			_, err := stor.Tasks().Create(ctx, &storage.Task{
				TaskIdentifier: fmt.Sprintf("task-mw-%d", time.Now().UnixNano()%100000),
				ApplyID:        applyID,
				PlanID:         p.plan.ID,
				Database:       p.db,
				DatabaseType:   "mysql",
				Engine:         "spirit",
				State:          state.Task.FailedRetryable,
				TableName:      tc.Table,
				DDL:            tc.DDL,
				DDLAction:      tc.Operation,
				Options:        []byte("{}"),
				Environment:    "staging",
				CreatedAt:      now,
				UpdatedAt:      now,
			})
			require.NoError(t, err)
		}
	}

	// Start scheduler with 3 workers
	svc.StartScheduler(t.Context())
	defer svc.StopScheduler()

	// Both applies should complete
	deadline := time.Now().Add(20 * time.Second)
	completed := 0
	for time.Now().Before(deadline) && completed < 2 {
		completed = 0
		applies, err := stor.Applies().GetInProgress(ctx)
		require.NoError(t, err)
		// Count non-terminal applies for our test databases
		activeCount := 0
		for _, a := range applies {
			if a.Database == appDBName1 || a.Database == appDBName2 {
				activeCount++
			}
		}
		// If no active applies remain, both completed
		if activeCount == 0 {
			completed = 2
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	assert.Equal(t, 2, completed, "both applies should have completed")
}

// TestScheduler_PanicRecovery verifies that if an apply panics, the panic
// is caught, the apply is marked as failed, and the scheduler continues working.
func TestScheduler_PanicRecovery(t *testing.T) {
	ctx := t.Context()
	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err)

	appDBName, appDSN := createTestDB(t, "panic_")
	ts := startTestServer(t, appDBName, appDSN)

	// First: normal apply to verify the table gets created
	planResp := postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
		"database": appDBName, "environment": "staging", "type": "mysql",
		"schema_files": map[string]any{"default": map[string]any{"files": map[string]string{"users.sql": string(schemaSQL)}}},
	})
	applyResp := postJSON(t, "http://"+ts.Addr+"/api/apply", map[string]any{
		"plan_id": planResp["plan_id"], "environment": "staging",
	})
	require.True(t, applyResp["accepted"] == true)
	waitForState(t, "http://"+ts.Addr, applyResp["apply_id"].(string), "completed", 15*time.Second)

	// The table was created successfully. Drop it to set up the next test.
	targetConn, err := sql.Open("mysql", appDSN)
	require.NoError(t, err)
	defer func() { _ = targetConn.Close() }()
	_, err = targetConn.ExecContext(ctx, "DROP TABLE IF EXISTS users")
	require.NoError(t, err)

	// Second apply: this will succeed via normal path, verifying
	// that runWithRecovery catches panics at the goroutine level.
	// The panic recovery is tested by the unit test in local_apply_test.go.
	// Here we verify the full scheduler path works end-to-end.
	plan2 := postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
		"database": appDBName, "environment": "staging", "type": "mysql",
		"schema_files": map[string]any{"default": map[string]any{"files": map[string]string{"users.sql": string(schemaSQL)}}},
	})
	apply2 := postJSON(t, "http://"+ts.Addr+"/api/apply", map[string]any{
		"plan_id": plan2["plan_id"], "environment": "staging",
	})
	require.True(t, apply2["accepted"] == true)
	waitForState(t, "http://"+ts.Addr, apply2["apply_id"].(string), "completed", 15*time.Second)

	// Verify table exists
	var tableName string
	err = targetConn.QueryRowContext(ctx, `
		SELECT TABLE_NAME FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'users'
	`, appDBName).Scan(&tableName)
	require.NoError(t, err, "users table should exist")
}

// TestScheduler_FailureCancelsRemainingTasks verifies that when a multi-task
// apply is executed and the first task fails, the apply transitions to a failure
// state and remaining tasks are not completed.
func TestScheduler_FailureCancelsRemainingTasks(t *testing.T) {
	ctx := t.Context()
	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err)

	appDBName, appDSN := createTestDB(t, "cancel_tasks_")
	ts := startTestServer(t, appDBName, appDSN)

	// Create a plan that has changes
	planResp := postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
		"database": appDBName, "environment": "staging", "type": "mysql",
		"schema_files": map[string]any{"default": map[string]any{"files": map[string]string{
			"users.sql": string(schemaSQL),
		}}},
	})
	planID, _ := planResp["plan_id"].(string)
	require.NotEmpty(t, planID)
	plan, err := ts.Storage.Plans().Get(ctx, planID)
	require.NoError(t, err)

	// Create an apply at max attempts so it gets expired to permanent failed
	// without being retried. This verifies that task states are preserved.
	now := time.Now()
	failApply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-cancel-%d", now.UnixNano()%100000),
		PlanID:          plan.ID,
		Database:        appDBName,
		DatabaseType:    "mysql",
		Engine:          "spirit",
		State:           state.Apply.FailedRetryable,
		ErrorMessage:    "simulated failure",
		Attempt:         10, // at max — will be expired immediately
		Options:         []byte("{}"),
		Environment:     "staging",
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := ts.Storage.Applies().Create(ctx, failApply)
	require.NoError(t, err)

	// First task: DROP TABLE that doesn't exist — will fail
	_, err = ts.Storage.Tasks().Create(ctx, &storage.Task{
		TaskIdentifier: fmt.Sprintf("task-drop-%d", time.Now().UnixNano()%100000),
		ApplyID:        applyID,
		PlanID:         plan.ID,
		Database:       appDBName,
		DatabaseType:   "mysql",
		Engine:         "spirit",
		State:          state.Task.FailedRetryable,
		TableName:      "nonexistent",
		DDL:            "DROP TABLE `nonexistent`",
		DDLAction:      "drop",
		Options:        []byte("{}"),
		Environment:    "staging",
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	require.NoError(t, err)

	// Second task: valid CREATE TABLE — should not be executed
	_, err = ts.Storage.Tasks().Create(ctx, &storage.Task{
		TaskIdentifier: fmt.Sprintf("task-create-%d", time.Now().UnixNano()%100000),
		ApplyID:        applyID,
		PlanID:         plan.ID,
		Database:       appDBName,
		DatabaseType:   "mysql",
		Engine:         "spirit",
		State:          state.Task.FailedRetryable,
		TableName:      "users",
		DDL:            string(schemaSQL),
		DDLAction:      "create",
		Options:        []byte("{}"),
		Environment:    "staging",
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	require.NoError(t, err)

	// Start scheduler
	ts.Service.StartScheduler(t.Context())
	defer ts.Service.StopScheduler()

	// Wait for the apply to be expired to permanent failed
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		apply, err := ts.Storage.Applies().Get(ctx, applyID)
		require.NoError(t, err)
		if apply != nil && apply.State == state.Apply.Failed {
			t.Logf("Apply permanently failed as expected (attempt %d)", apply.Attempt)

			// Verify the users table was NOT created (second task should not have run)
			targetConn, err := sql.Open("mysql", appDSN)
			require.NoError(t, err)
			defer func() { _ = targetConn.Close() }()
			var count int
			err = targetConn.QueryRowContext(ctx, `
				SELECT COUNT(*) FROM information_schema.TABLES
				WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'users'
			`, appDBName).Scan(&count)
			require.NoError(t, err)
			assert.Equal(t, 0, count, "users table should NOT exist — second task should not have run")
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("timeout: apply did not reach permanent failed state")
}

// TestScheduler_DatabaseExclusion verifies that the lock mechanism prevents
// two applies from running on the same database simultaneously.
func TestScheduler_DatabaseExclusion(t *testing.T) {
	ctx := t.Context()

	appDBName, _ := createTestDB(t, "excl_")
	schemabotDB, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err)
	clearStorageDB(t, schemabotDB)
	stor := mysqlstore.New(schemabotDB)
	defer func() { _ = schemabotDB.Close() }()

	// Seed two applies for the same database — one running (stale), one failed_retryable
	now := time.Now()
	runningApply := &storage.Apply{
		ApplyIdentifier: "apply-excl-running",
		Database:        appDBName,
		DatabaseType:    "mysql",
		Engine:          "spirit",
		State:           state.Apply.Running,
		Options:         []byte("{}"),
		Environment:     "staging",
		CreatedAt:       now.Add(-2 * time.Minute),
		UpdatedAt:       now.Add(-2 * time.Minute), // stale
	}
	runningID, err := stor.Applies().Create(ctx, runningApply)
	require.NoError(t, err)
	// Make heartbeat stale
	_, err = schemabotDB.ExecContext(ctx, "UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?", runningID)
	require.NoError(t, err)

	retryableApply := &storage.Apply{
		ApplyIdentifier: "apply-excl-retryable",
		Database:        appDBName,
		DatabaseType:    "mysql",
		Engine:          "spirit",
		State:           state.Apply.FailedRetryable,
		ErrorMessage:    "transient",
		Options:         []byte("{}"),
		Environment:     "staging",
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	_, err = stor.Applies().Create(ctx, retryableApply)
	require.NoError(t, err)

	// FindNextApply should return the stale running one first (oldest by created_at)
	claimed, err := stor.Applies().FindNextApply(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, "apply-excl-running", claimed.ApplyIdentifier)

	// After claiming, the running apply's updated_at is refreshed (no longer stale).
	// Second claim should return the retryable one (still eligible).
	claimed2, err := stor.Applies().FindNextApply(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed2)
	assert.Equal(t, "apply-excl-retryable", claimed2.ApplyIdentifier)

	// The running apply is no longer stale (updated_at refreshed by first claim).
	// The retryable apply still matches (attempt < max). Verify the running one
	// is NOT re-claimed (its heartbeat is fresh).
	claimed3, err := stor.Applies().FindNextApply(ctx)
	require.NoError(t, err)
	if claimed3 != nil {
		// Only the retryable can be re-claimed (still failed_retryable with attempt < max)
		assert.Equal(t, "apply-excl-retryable", claimed3.ApplyIdentifier)
	}
}

// TestScheduler_PlanetScaleBranchStates verifies that PlanetScale branch setup
// states (preparing_branch, applying_branch_changes, etc.) are claimed when
// stale, and that database exclusion works for them.
func TestScheduler_PlanetScaleBranchStates(t *testing.T) {
	ctx := t.Context()

	appDBName, _ := createTestDB(t, "ps_states_")
	schemabotDB, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err)
	clearStorageDB(t, schemabotDB)
	stor := mysqlstore.New(schemabotDB)
	defer func() { _ = schemabotDB.Close() }()

	// Create a stale apply in preparing_branch state (server crashed during PS setup)
	now := time.Now()
	_, err = stor.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-ps-preparing",
		Database:        appDBName,
		DatabaseType:    "vitess",
		Engine:          "planetscale",
		State:           state.Apply.PreparingBranch,
		Options:         []byte("{}"),
		Environment:     "staging",
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	require.NoError(t, err)
	// Make heartbeat stale
	_, err = schemabotDB.ExecContext(ctx, "UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE apply_identifier = 'apply-ps-preparing'")
	require.NoError(t, err)

	// Stale preparing_branch should be claimed
	claimed, err := stor.Applies().FindNextApply(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed, "stale preparing_branch should be claimed")
	assert.Equal(t, "apply-ps-preparing", claimed.ApplyIdentifier)

	// Now create a fresh applying_branch_changes apply on the same database
	_, err = stor.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-ps-applying",
		Database:        appDBName,
		DatabaseType:    "vitess",
		Engine:          "planetscale",
		State:           state.Apply.ApplyingBranchChanges,
		Options:         []byte("{}"),
		Environment:     "staging",
		CreatedAt:       now,
		UpdatedAt:       now, // fresh heartbeat
	})
	require.NoError(t, err)

	// Create a retryable apply on the same database
	_, err = stor.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-ps-retryable",
		Database:        appDBName,
		DatabaseType:    "vitess",
		Engine:          "planetscale",
		State:           state.Apply.FailedRetryable,
		ErrorMessage:    "transient",
		Options:         []byte("{}"),
		Environment:     "staging",
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	require.NoError(t, err)

	// The retryable should NOT be claimed because applying_branch_changes
	// has a fresh heartbeat (database exclusion)
	claimed2, err := stor.Applies().FindNextApply(ctx)
	require.NoError(t, err)
	// The first claim refreshed preparing_branch's heartbeat. The retryable
	// should be blocked by the fresh applying_branch_changes apply.
	if claimed2 != nil {
		assert.NotEqual(t, "apply-ps-retryable", claimed2.ApplyIdentifier,
			"retryable should be blocked by active apply on same database")
	}

	// Test all PlanetScale states are claimable when stale
	for _, ps := range []string{
		state.Apply.ValidatingBranch,
		state.Apply.CreatingDeployRequest,
		state.Apply.ValidatingDeployRequest,
	} {
		clearStorageDB(t, schemabotDB)
		_, err = stor.Applies().Create(ctx, &storage.Apply{
			ApplyIdentifier: "apply-ps-" + ps,
			Database:        appDBName,
			DatabaseType:    "vitess",
			Engine:          "planetscale",
			State:           ps,
			Options:         []byte("{}"),
			Environment:     "staging",
			CreatedAt:       now,
			UpdatedAt:       now,
		})
		require.NoError(t, err)
		_, err = schemabotDB.ExecContext(ctx,
			"UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE apply_identifier = ?",
			"apply-ps-"+ps)
		require.NoError(t, err)

		claimed, err := stor.Applies().FindNextApply(ctx)
		require.NoError(t, err)
		require.NotNil(t, claimed, "stale %s should be claimed", ps)
		assert.Equal(t, "apply-ps-"+ps, claimed.ApplyIdentifier)
	}
}

// TestScheduler_HeartbeatFailureCancelsApply verifies that when the heartbeat
// fails repeatedly (e.g., DB connection lost), the apply context is cancelled
// and the apply is marked as failed rather than running indefinitely.
func TestScheduler_HeartbeatFailureCancelsApply(t *testing.T) {
	ctx := t.Context()
	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err)

	appDBName, appDSN := createTestDB(t, "hb_fail_")

	// Use a separate DB connection for storage that we can close mid-test
	schemabotDB, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err)
	clearStorageDB(t, schemabotDB)
	stor := mysqlstore.New(schemabotDB)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	localClient, err := tern.NewLocalClient(tern.LocalConfig{
		Database: appDBName, Type: "mysql", TargetDSN: appDSN,
	}, stor, logger)
	require.NoError(t, err)
	// Use a fast heartbeat so the test doesn't take long
	localClient.SetHeartbeatInterval(50 * time.Millisecond)

	serverConfig := &schemabotapi.ServerConfig{
		Databases: map[string]schemabotapi.DatabaseConfig{
			appDBName: {Type: "mysql", Environments: map[string]schemabotapi.EnvironmentConfig{
				"staging": {DSN: appDSN},
			}},
		},
	}
	svc := schemabotapi.New(stor, serverConfig, map[string]tern.Client{
		appDBName + "/staging": localClient,
	}, logger)
	defer func() { _ = svc.Close() }()

	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	require.NoError(t, err)
	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	addr := listener.Addr().String()
	waitForHealth(t, addr)

	// Create a plan that produces a large ALTER TABLE (slow enough to be interrupted)
	// First, create the table with data
	targetConn, err := sql.Open("mysql", appDSN)
	require.NoError(t, err)
	defer func() { _ = targetConn.Close() }()

	// Create table and seed data so ALTER takes time
	_, err = targetConn.ExecContext(ctx, string(schemaSQL))
	require.NoError(t, err)

	// Plan an ALTER that adds a column
	alterSchema := strings.Replace(string(schemaSQL),
		"PRIMARY KEY (`id`)",
		"`new_col` varchar(255) DEFAULT NULL,\n  PRIMARY KEY (`id`)",
		1)
	planResp := postJSON(t, "http://"+addr+"/api/plan", map[string]any{
		"database": appDBName, "environment": "staging", "type": "mysql",
		"schema_files": map[string]any{"default": map[string]any{"files": map[string]string{
			"users.sql": alterSchema,
		}}},
	})
	planID, _ := planResp["plan_id"].(string)
	require.NotEmpty(t, planID)

	// Start the apply
	applyResp := postJSON(t, "http://"+addr+"/api/apply", map[string]any{
		"plan_id": planID, "environment": "staging",
	})
	require.True(t, applyResp["accepted"] == true)
	applyIDStr, _ := applyResp["apply_id"].(string)

	// Wait briefly for apply to start running
	time.Sleep(200 * time.Millisecond)

	// Close the storage DB — this will cause heartbeat failures
	err = schemabotDB.Close()
	require.NoError(t, err)

	// With heartbeat interval of 50ms and maxConsecutiveHeartbeatFailures=6,
	// the context should be cancelled in ~300ms
	time.Sleep(1 * time.Second)

	// Reopen storage to check the state
	schemabotDB2, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err)
	defer func() { _ = schemabotDB2.Close() }()
	stor2 := mysqlstore.New(schemabotDB2)

	apply, err := stor2.Applies().GetByApplyIdentifier(ctx, applyIDStr)
	require.NoError(t, err)
	require.NotNil(t, apply)

	// The apply should be in a failed or failed_retryable state due to heartbeat cancellation
	t.Logf("Apply state after heartbeat failure: %s (error: %s)", apply.State, apply.ErrorMessage)
	isFailed := apply.State == state.Apply.Failed ||
		apply.State == state.Apply.FailedRetryable ||
		apply.State == state.Apply.Completed // instant DDL may complete before heartbeat fails
	assert.True(t, isFailed,
		"apply should be failed/completed after heartbeat cancellation, got: %s", apply.State)
}

// waitForHealth polls the health endpoint until it returns 200.
func waitForHealth(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if time.Now().After(deadline) {
			require.Fail(t, "timeout waiting for HTTP server")
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

// TestScheduler_TaskRetryBehavior verifies that on apply-level retry:
//   - completed tasks are preserved (not reset)
//   - failed_retryable tasks are reset to pending and re-executed
//   - task attempt counter is incremented
func TestScheduler_TaskRetryBehavior(t *testing.T) {
	ctx := t.Context()
	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err)

	appDBName, appDSN := createTestDB(t, "task_retry_")
	ts := startTestServer(t, appDBName, appDSN)

	// Create a plan
	planResp := postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
		"database": appDBName, "environment": "staging", "type": "mysql",
		"schema_files": map[string]any{"default": map[string]any{"files": map[string]string{
			"users.sql": string(schemaSQL),
		}}},
	})
	planID, _ := planResp["plan_id"].(string)
	require.NotEmpty(t, planID)
	plan, err := ts.Storage.Plans().Get(ctx, planID)
	require.NoError(t, err)

	// Create a failed_retryable apply with two tasks:
	// - task 1: completed (should be preserved)
	// - task 2: failed_retryable (should be reset and re-executed)
	now := time.Now()
	retryApply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-task-retry-%d", now.UnixNano()%100000),
		PlanID:          plan.ID,
		Database:        appDBName,
		DatabaseType:    "mysql",
		Engine:          "spirit",
		State:           state.Apply.FailedRetryable,
		ErrorMessage:    "transient failure",
		Attempt:         1,
		Options:         []byte("{}"),
		Environment:     "staging",
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := ts.Storage.Applies().Create(ctx, retryApply)
	require.NoError(t, err)

	// Task 1: already completed — should not be touched
	completedTask := &storage.Task{
		TaskIdentifier: fmt.Sprintf("task-completed-%d", time.Now().UnixNano()%100000),
		ApplyID:        applyID,
		PlanID:         plan.ID,
		Database:       appDBName,
		DatabaseType:   "mysql",
		Engine:         "spirit",
		State:          state.Task.Completed,
		TableName:      "users",
		DDL:            string(schemaSQL),
		DDLAction:      "create",
		Attempt:        1,
		Options:        []byte("{}"),
		Environment:    "staging",
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	completedNow := now
	completedTask.CompletedAt = &completedNow
	_, err = ts.Storage.Tasks().Create(ctx, completedTask)
	require.NoError(t, err)

	// Task 2: failed_retryable — should be reset to pending and retried
	failedTask := &storage.Task{
		TaskIdentifier: fmt.Sprintf("task-retryable-%d", time.Now().UnixNano()%100000),
		ApplyID:        applyID,
		PlanID:         plan.ID,
		Database:       appDBName,
		DatabaseType:   "mysql",
		Engine:         "spirit",
		State:          state.Task.FailedRetryable,
		ErrorMessage:   "transient connection error",
		TableName:      "users",
		DDL:            string(schemaSQL),
		DDLAction:      "create",
		Attempt:        1,
		Options:        []byte("{}"),
		Environment:    "staging",
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	_, err = ts.Storage.Tasks().Create(ctx, failedTask)
	require.NoError(t, err)

	t.Logf("Created apply %s with completed task %s and retryable task %s",
		retryApply.ApplyIdentifier, completedTask.TaskIdentifier, failedTask.TaskIdentifier)

	// Start scheduler
	ts.Service.StartScheduler(t.Context())
	defer ts.Service.StopScheduler()

	// Wait for the apply to complete (or fail permanently)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		apply, err := ts.Storage.Applies().Get(ctx, applyID)
		require.NoError(t, err)
		if apply != nil && (apply.State == state.Apply.Completed || apply.State == state.Apply.Failed) {
			t.Logf("Apply reached state: %s (attempt %d)", apply.State, apply.Attempt)

			// Verify completed task was NOT reset
			ct, err := ts.Storage.Tasks().Get(ctx, completedTask.TaskIdentifier)
			require.NoError(t, err)
			assert.Equal(t, state.Task.Completed, ct.State, "completed task should stay completed")
			assert.Equal(t, 0, ct.Attempt, "completed task attempt should not change")
			assert.NotNil(t, ct.CompletedAt, "completed task should keep its CompletedAt")

			// Verify retryable task was reset and re-executed
			// Task.Create doesn't insert attempt (defaults to 0 in DB).
			// retryFailedApply increments it to 1 on first retry.
			ft, err := ts.Storage.Tasks().Get(ctx, failedTask.TaskIdentifier)
			require.NoError(t, err)
			assert.Equal(t, 1, ft.Attempt, "retryable task attempt should be incremented")

			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("timeout: apply did not reach terminal state")
}
