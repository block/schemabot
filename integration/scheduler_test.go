//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schemabotapi "github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// These tests exercise scheduler behavior at two levels: the full worker loop
// in the resume tests, and the atomic claim query through FindNextApply.
// Scheduler workers use FindNextApply before calling ResumeApply, so direct
// calls keep claim policy tests focused without waiting for ticks.

type schedulerClaimFixture struct {
	appDBName string
	storageDB *sql.DB
	store     *mysqlstore.Storage
}

type blockingResumeClient struct {
	tern.Client

	started chan struct{}
	release <-chan struct{}
}

func newBlockingResumeClient(client tern.Client, release <-chan struct{}) *blockingResumeClient {
	return &blockingResumeClient{
		Client:  client,
		started: make(chan struct{}, 1),
		release: release,
	}
}

func (c *blockingResumeClient) ResumeApply(ctx context.Context, apply *storage.Apply) error {
	select {
	case c.started <- struct{}{}:
	default:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.release:
	}

	return c.Client.ResumeApply(ctx, apply)
}

func (c *blockingResumeClient) waitForResume(t *testing.T, timeout time.Duration) {
	t.Helper()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-c.started:
	case <-timer.C:
		require.Failf(t, "timeout", "scheduler did not claim blocked apply within %s", timeout)
	}
}

// newSchedulerClaimFixture creates a real target database plus a clean SchemaBot
// metadata store. The claim-policy tests write apply rows directly into storage
// so they can test scheduler decisions without depending on worker timing.
func newSchedulerClaimFixture(t *testing.T, appDBPrefix string) *schedulerClaimFixture {
	t.Helper()

	appDBName, _ := createTestDB(t, appDBPrefix)
	storageDB, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err)
	require.NoError(t, storageDB.PingContext(t.Context()))
	clearStorageDB(t, storageDB)
	t.Cleanup(func() {
		utils.CloseAndLog(storageDB)
	})

	return &schedulerClaimFixture{
		appDBName: appDBName,
		storageDB: storageDB,
		store:     mysqlstore.New(storageDB),
	}
}

func (f *schedulerClaimFixture) resetStorage(t *testing.T) {
	t.Helper()
	clearStorageDB(t, f.storageDB)
}

func TestScheduler_BasicClaimAndResume(t *testing.T) {
	ctx := t.Context()
	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err)

	appDBName, appDSN := createTestDB(t, "basic_sched_")
	ts := startTestServer(t, appDBName, appDSN)

	// First apply the schema normally so the target database reaches the desired state.
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
	ts.Service.StopScheduler()

	// Remove the table so the second plan contains DDL that recovery can resume.
	targetConn, err := sql.Open("mysql", appDSN)
	require.NoError(t, err)
	require.NoError(t, targetConn.PingContext(ctx))
	defer utils.CloseAndLog(targetConn)
	_, err = targetConn.ExecContext(ctx, "DROP TABLE IF EXISTS users")
	require.NoError(t, err)

	plan2Resp := postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
		"database": appDBName, "environment": "staging", "type": "mysql",
		"schema_files": map[string]any{"default": map[string]any{"files": map[string]string{"users.sql": string(schemaSQL)}}},
	})
	planID2, _ := plan2Resp["plan_id"].(string)
	plan2, err := ts.Storage.Plans().Get(ctx, planID2)
	require.NoError(t, err)

	// Seed storage with a stale running apply and running tasks, matching the
	// state left behind when a worker stops heartbeating before completing.
	now := time.Now()
	staleApply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-stale-%d", now.UnixNano()%100000),
		PlanID:          plan2.ID,
		Database:        appDBName,
		DatabaseType:    "mysql",
		Deployment:      appDBName,
		Engine:          "spirit",
		State:           state.Apply.Running,
		Options:         []byte("{}"),
		Environment:     "staging",
		StartedAt:       &now,
		CreatedAt:       now,
		UpdatedAt:       now.Add(-2 * time.Minute),
	}
	staleID, err := ts.Storage.Applies().Create(ctx, staleApply)
	require.NoError(t, err)

	schemabotDB, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err)
	require.NoError(t, schemabotDB.PingContext(ctx))
	defer utils.CloseAndLog(schemabotDB)
	_, err = schemabotDB.ExecContext(ctx, "UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?", staleID)
	require.NoError(t, err)

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

	// Scheduler recovery should claim the stale apply and resume it to completion.
	ts.Service.StartScheduler(t.Context())
	defer ts.Service.StopScheduler()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		apply, err := ts.Storage.Applies().Get(ctx, staleID)
		require.NoError(t, err)
		if apply != nil && apply.State == state.Apply.Completed {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("timeout: scheduler did not resume stale apply")
}

func TestScheduler_ClaimOrdering(t *testing.T) {
	ctx := t.Context()

	fixture := newSchedulerClaimFixture(t, "ord1_")
	db1Name := fixture.appDBName
	db2Name, _ := createTestDB(t, "ord2_")
	stor := fixture.store
	schemabotDB := fixture.storageDB

	now := time.Now()
	olderID, err := stor.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-order-older",
		Database:        db1Name,
		DatabaseType:    "mysql",
		Deployment:      db1Name,
		Engine:          "spirit",
		State:           state.Apply.Running,
		Options:         []byte("{}"),
		Environment:     "staging",
		CreatedAt:       now.Add(-2 * time.Minute),
		UpdatedAt:       now.Add(-2 * time.Minute),
	})
	require.NoError(t, err)
	newerID, err := stor.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-order-newer",
		Database:        db2Name,
		DatabaseType:    "mysql",
		Deployment:      db2Name,
		Engine:          "spirit",
		State:           state.Apply.Running,
		Options:         []byte("{}"),
		Environment:     "staging",
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "UPDATE applies SET created_at = NOW() - INTERVAL 2 MINUTE, updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?", olderID)
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "UPDATE applies SET created_at = NOW() - INTERVAL 1 MINUTE, updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?", newerID)
	require.NoError(t, err)

	// The scheduler claim path should pick the oldest stale apply first.
	claimed, err := stor.Applies().FindNextApply(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, "apply-order-older", claimed.ApplyIdentifier)

	// After the first target is claimed, the scheduler can claim the next stale target.
	claimed2, err := stor.Applies().FindNextApply(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed2)
	assert.Equal(t, "apply-order-newer", claimed2.ApplyIdentifier)
}

func TestScheduler_ClaimableStates(t *testing.T) {
	fixture := newSchedulerClaimFixture(t, "claim_states_")
	appDBName := fixture.appDBName
	stor := fixture.store
	schemabotDB := fixture.storageDB

	cases := []struct {
		name         string
		applyState   string
		databaseType string
		engine       string
		wantClaim    bool
	}{
		{name: "pending", applyState: state.Apply.Pending, databaseType: "mysql", engine: "spirit", wantClaim: true},
		{name: "running", applyState: state.Apply.Running, databaseType: "mysql", engine: "spirit", wantClaim: true},
		{name: "waiting for deploy", applyState: state.Apply.WaitingForDeploy, databaseType: "vitess", engine: "planetscale", wantClaim: true},
		{name: "waiting for cutover", applyState: state.Apply.WaitingForCutover, databaseType: "vitess", engine: "planetscale", wantClaim: true},
		{name: "cutting over", applyState: state.Apply.CuttingOver, databaseType: "vitess", engine: "planetscale", wantClaim: true},
		{name: "revert window", applyState: state.Apply.RevertWindow, databaseType: "vitess", engine: "planetscale", wantClaim: true},
		{name: "completed", applyState: state.Apply.Completed, databaseType: "mysql", engine: "spirit"},
		{name: "failed", applyState: state.Apply.Failed, databaseType: "mysql", engine: "spirit"},
		{name: "stopped", applyState: state.Apply.Stopped, databaseType: "mysql", engine: "spirit"},
		{name: "reverted", applyState: state.Apply.Reverted, databaseType: "vitess", engine: "planetscale"},
		{name: "preparing branch", applyState: state.Apply.PreparingBranch, databaseType: "vitess", engine: "planetscale"},
		{name: "applying branch changes", applyState: state.Apply.ApplyingBranchChanges, databaseType: "vitess", engine: "planetscale"},
		{name: "validating branch", applyState: state.Apply.ValidatingBranch, databaseType: "vitess", engine: "planetscale"},
		{name: "creating deploy request", applyState: state.Apply.CreatingDeployRequest, databaseType: "vitess", engine: "planetscale"},
		{name: "validating deploy request", applyState: state.Apply.ValidatingDeployRequest, databaseType: "vitess", engine: "planetscale"},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			fixture.resetStorage(t)

			applyIdentifier := fmt.Sprintf("apply-claim-state-%d", i)
			now := time.Now()
			applyID, err := stor.Applies().Create(ctx, &storage.Apply{
				ApplyIdentifier: applyIdentifier,
				Database:        appDBName,
				DatabaseType:    tc.databaseType,
				Deployment:      appDBName,
				Engine:          tc.engine,
				State:           tc.applyState,
				Options:         []byte("{}"),
				Environment:     "staging",
				CreatedAt:       now,
				UpdatedAt:       now,
			})
			require.NoError(t, err)
			if tc.applyState == state.Apply.Pending {
				// Pending applies become scheduler work only after task creation
				// finishes; a bare pending apply is still in request setup.
				_, err := stor.Tasks().Create(ctx, &storage.Task{
					TaskIdentifier: fmt.Sprintf("task-%s", applyIdentifier),
					ApplyID:        applyID,
					Database:       appDBName,
					DatabaseType:   tc.databaseType,
					Engine:         tc.engine,
					State:          state.Task.Pending,
					TableName:      "claimable_pending",
					DDL:            "ALTER TABLE claimable_pending ADD COLUMN note VARCHAR(255)",
					DDLAction:      "alter",
					Options:        []byte("{}"),
					Environment:    "staging",
					CreatedAt:      now,
					UpdatedAt:      now,
				})
				require.NoError(t, err)
			}
			_, err = schemabotDB.ExecContext(ctx,
				"UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE apply_identifier = ?",
				applyIdentifier)
			require.NoError(t, err)

			// The scheduler should claim only queued or stale applies in states it can resume safely.
			claimed, err := stor.Applies().FindNextApply(ctx)
			require.NoError(t, err)
			if tc.wantClaim {
				require.NotNil(t, claimed)
				assert.Equal(t, applyIdentifier, claimed.ApplyIdentifier)
			} else {
				assert.Nil(t, claimed)
			}
		})
	}
}

func TestScheduler_ClaimRefreshesHeartbeat(t *testing.T) {
	ctx := t.Context()

	fixture := newSchedulerClaimFixture(t, "claim_heartbeat_")
	appDBName := fixture.appDBName
	stor := fixture.store
	schemabotDB := fixture.storageDB

	applyID, err := stor.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-claim-refreshes-heartbeat",
		Database:        appDBName,
		DatabaseType:    "mysql",
		Deployment:      appDBName,
		Engine:          "spirit",
		State:           state.Apply.Running,
		Options:         []byte("{}"),
		Environment:     "staging",
	})
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?", applyID)
	require.NoError(t, err)

	beforeClaim, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, beforeClaim)

	// Claiming is also the scheduler's lease renewal; it keeps another worker from immediately reclaiming the same apply.
	claimed, err := stor.Applies().FindNextApply(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, "apply-claim-refreshes-heartbeat", claimed.ApplyIdentifier)

	afterClaim, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, afterClaim)
	assert.True(t, afterClaim.UpdatedAt.After(beforeClaim.UpdatedAt), "claim should refresh the apply heartbeat")

	reclaimed, err := stor.Applies().FindNextApply(ctx)
	require.NoError(t, err)
	assert.Nil(t, reclaimed, "freshly claimed apply should not be claimable again")
}

func TestScheduler_ExpiresRetryableBudget(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	fixture := newSchedulerClaimFixture(t, "retry_budget_")
	appDBName := fixture.appDBName
	stor := fixture.store

	now := time.Now()
	applyID, err := stor.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-retry-budget-exhausted",
		Database:        appDBName,
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      appDBName,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.FailedRetryable,
		ErrorMessage:    "temporary engine failure",
		Options:         []byte("{}"),
		Attempt:         999,
		Environment:     "staging",
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	require.NoError(t, err)
	_, err = stor.Tasks().Create(ctx, &storage.Task{
		TaskIdentifier: "task-retry-budget-exhausted",
		ApplyID:        applyID,
		Database:       appDBName,
		DatabaseType:   storage.DatabaseTypeMySQL,
		Engine:         storage.EngineSpirit,
		State:          state.Task.FailedRetryable,
		ErrorMessage:   "temporary engine failure",
		TableName:      "users",
		Namespace:      appDBName,
		DDL:            "ALTER TABLE users ADD COLUMN retry_note VARCHAR(255)",
		DDLAction:      "alter",
		Options:        []byte("{}"),
		Environment:    "staging",
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	require.NoError(t, err)

	svc := schemabotapi.New(stor, &schemabotapi.ServerConfig{SchedulerWorkers: 1}, nil, logger)
	require.NoError(t, svc.SetSchedulerPollInterval(50*time.Millisecond))
	svc.StartScheduler(ctx)
	defer svc.StopScheduler()

	// The scheduler should convert retry-waiting work to permanent failure once
	// the retry budget is exhausted, instead of leaving it claimable forever.
	require.Eventually(t, func() bool {
		apply, err := stor.Applies().Get(ctx, applyID)
		require.NoError(t, err)
		return apply != nil && apply.State == state.Apply.Failed
	}, 5*time.Second, 100*time.Millisecond)

	tasks, err := stor.Tasks().GetByApplyID(ctx, applyID)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, state.Task.Failed, tasks[0].State)
}

func TestScheduler_MultipleWorkersResumeDifferentTargets(t *testing.T) {
	ctx := t.Context()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	db1Name, db1DSN := createTestDB(t, "multi_worker_a_")
	db2Name, db2DSN := createTestDB(t, "multi_worker_b_")

	schemabotDB, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err)
	require.NoError(t, schemabotDB.PingContext(ctx))
	clearStorageDB(t, schemabotDB)
	defer utils.CloseAndLog(schemabotDB)
	stor := mysqlstore.New(schemabotDB)

	client1, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  db1Name,
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: db1DSN,
	}, stor, logger)
	require.NoError(t, err)
	client2, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  db2Name,
		Type:      storage.DatabaseTypeMySQL,
		TargetDSN: db2DSN,
	}, stor, logger)
	require.NoError(t, err)

	plan1 := planCreateTableForScheduler(t, client1, stor, db1Name, "scheduler_worker_a")
	plan2 := planCreateTableForScheduler(t, client2, stor, db2Name, "scheduler_worker_b")
	apply1ID := seedStaleSchedulerApply(t, stor, schemabotDB, db1Name, plan1, time.Now().Add(-3*time.Minute))
	apply2ID := seedStaleSchedulerApply(t, stor, schemabotDB, db2Name, plan2, time.Now().Add(-2*time.Minute))

	blockedResume := make(chan struct{})
	var releaseBlockedResume sync.Once
	releaseBlockedClient := func() {
		releaseBlockedResume.Do(func() {
			close(blockedResume)
		})
	}

	// The first client blocks after the scheduler claims its apply. That keeps
	// one worker occupied across the next poll, so completion of the second
	// apply proves another worker can claim independent work.
	blockingClient1 := newBlockingResumeClient(client1, blockedResume)

	svc := schemabotapi.New(stor, &schemabotapi.ServerConfig{
		SchedulerWorkers: 2,
		Databases: map[string]schemabotapi.DatabaseConfig{
			db1Name: {
				Type: "mysql",
				Environments: map[string]schemabotapi.EnvironmentConfig{
					"staging": {DSN: db1DSN},
				},
			},
			db2Name: {
				Type: "mysql",
				Environments: map[string]schemabotapi.EnvironmentConfig{
					"staging": {DSN: db2DSN},
				},
			},
		},
	}, map[string]tern.Client{
		db1Name + "/staging": blockingClient1,
		db2Name + "/staging": client2,
	}, logger)

	schedulerPollInterval := 500 * time.Millisecond
	require.NoError(t, svc.SetSchedulerPollInterval(schedulerPollInterval))

	svc.StartScheduler(ctx)
	defer func() {
		releaseBlockedClient()
		svc.StopScheduler()
	}()

	blockingClient1.waitForResume(t, 5*time.Second)

	// A worker can miss work on the startup claim and pick it up on the next
	// poll. The important behavior is that the second apply completes while the
	// first worker is still blocked.
	waitForSchedulerAppliesCompleted(t, stor, []int64{apply2ID}, schedulerPollInterval+5*time.Second)

	blockedApply, err := stor.Applies().Get(ctx, apply1ID)
	require.NoError(t, err)
	require.NotNil(t, blockedApply)
	assert.Equal(t, state.Apply.Running, blockedApply.State)

	releaseBlockedClient()
	waitForSchedulerAppliesCompleted(t, stor, []int64{apply1ID}, 5*time.Second)
}

func planCreateTableForScheduler(t *testing.T, client tern.Client, stor *mysqlstore.Storage, dbName, tableName string) *storage.Plan {
	t.Helper()

	resp, err := client.Plan(t.Context(), &ternv1.PlanRequest{
		Database:    dbName,
		Type:        storage.DatabaseTypeMySQL,
		Environment: "staging",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			dbName: {
				Files: map[string]string{
					tableName + ".sql": fmt.Sprintf(`
CREATE TABLE %s (
	id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(255) NOT NULL,
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
`, tableName),
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.PlanId)

	plan, err := stor.Plans().Get(t.Context(), resp.PlanId)
	require.NoError(t, err)
	require.NotNil(t, plan)
	return plan
}

func seedStaleSchedulerApply(
	t *testing.T,
	stor *mysqlstore.Storage,
	db *sql.DB,
	dbName string,
	plan *storage.Plan,
	createdAt time.Time,
) int64 {
	t.Helper()

	now := time.Now()
	applyID, err := stor.Applies().Create(t.Context(), &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-multi-worker-%s", dbName),
		PlanID:          plan.ID,
		Database:        dbName,
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      dbName,
		Engine:          "spirit",
		State:           state.Apply.Running,
		Options:         []byte("{}"),
		Environment:     "staging",
		StartedAt:       &now,
	})
	require.NoError(t, err)

	for _, tc := range plan.FlatDDLChanges() {
		_, err := stor.Tasks().Create(t.Context(), &storage.Task{
			TaskIdentifier: fmt.Sprintf("task-multi-worker-%s-%s", dbName, tc.Table),
			ApplyID:        applyID,
			PlanID:         plan.ID,
			Database:       dbName,
			DatabaseType:   storage.DatabaseTypeMySQL,
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

	_, err = db.ExecContext(
		t.Context(),
		"UPDATE applies SET created_at = ?, updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?",
		createdAt,
		applyID,
	)
	require.NoError(t, err)

	return applyID
}

func waitForSchedulerAppliesCompleted(t *testing.T, stor *mysqlstore.Storage, applyIDs []int64, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	completed := make(map[int64]bool, len(applyIDs))
	for time.Now().Before(deadline) {
		for _, applyID := range applyIDs {
			if completed[applyID] {
				continue
			}
			apply, err := stor.Applies().Get(t.Context(), applyID)
			require.NoError(t, err)
			if apply != nil && apply.State == state.Apply.Completed {
				completed[applyID] = true
			}
		}
		if len(completed) == len(applyIDs) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	states := make(map[int64]string, len(applyIDs))
	for _, applyID := range applyIDs {
		apply, err := stor.Applies().Get(t.Context(), applyID)
		require.NoError(t, err)
		if apply != nil {
			states[applyID] = apply.State
		}
	}
	require.Failf(t, "timeout", "scheduler did not complete all applies within %s; states: %v", timeout, states)
}

func TestScheduler_DatabaseExclusionScopedByEnvironment(t *testing.T) {
	ctx := t.Context()

	fixture := newSchedulerClaimFixture(t, "env_excl_")
	appDBName := fixture.appDBName
	stor := fixture.store
	schemabotDB := fixture.storageDB

	now := time.Now()
	_, err := stor.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-env-active-staging",
		Database:        appDBName,
		DatabaseType:    "mysql",
		Deployment:      appDBName,
		Engine:          "spirit",
		State:           state.Apply.Running,
		Options:         []byte("{}"),
		Environment:     "staging",
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	require.NoError(t, err)

	productionID, err := stor.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-env-stale-production",
		Database:        appDBName,
		DatabaseType:    "mysql",
		Deployment:      appDBName,
		Engine:          "spirit",
		State:           state.Apply.Running,
		Options:         []byte("{}"),
		Environment:     "production",
		CreatedAt:       now.Add(-time.Minute),
		UpdatedAt:       now.Add(-time.Minute),
	})
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?", productionID)
	require.NoError(t, err)

	// The scheduler should allow a stale apply when the active apply is for another environment.
	claimed, err := stor.Applies().FindNextApply(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, "apply-env-stale-production", claimed.ApplyIdentifier)
}

func TestScheduler_PlanetScaleSetupStatesNotClaimed(t *testing.T) {
	ctx := t.Context()

	fixture := newSchedulerClaimFixture(t, "ps_states_")
	appDBName := fixture.appDBName
	stor := fixture.store
	schemabotDB := fixture.storageDB

	now := time.Now()
	for _, ps := range []string{
		state.Apply.PreparingBranch,
		state.Apply.ApplyingBranchChanges,
		state.Apply.ValidatingBranch,
		state.Apply.CreatingDeployRequest,
		state.Apply.ValidatingDeployRequest,
	} {
		fixture.resetStorage(t)
		_, err := stor.Applies().Create(ctx, &storage.Apply{
			ApplyIdentifier: "apply-ps-" + ps,
			Database:        appDBName,
			DatabaseType:    "vitess",
			Deployment:      appDBName,
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

		// The scheduler should leave PlanetScale setup states unclaimed until resume metadata can be hydrated.
		claimed, err := stor.Applies().FindNextApply(ctx)
		require.NoError(t, err)
		assert.Nil(t, claimed, "stale %s should not be claimed without persisted resume metadata", ps)
	}
}
