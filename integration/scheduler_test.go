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

const schedulerTestPollInterval = 200 * time.Millisecond

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
	require.NoError(t, ts.Service.SetSchedulerPollInterval(schedulerTestPollInterval))
	ts.Service.StartScheduler(t.Context())
	defer ts.Service.StopScheduler()

	waitForSchedulerAppliesCompleted(t, ts.Storage, []int64{staleID}, schedulerTestPollInterval+5*time.Second)
}

func TestScheduler_QueuedApplyDispatchesToCompletion(t *testing.T) {
	ctx := t.Context()
	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err)

	appDBName, appDSN := createTestDB(t, "queued_sched_")
	ts := startTestServer(t, appDBName, appDSN)
	ts.Service.StopScheduler()

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
	applyIdentifier, _ := applyResp["apply_id"].(string)
	require.NotEmpty(t, applyIdentifier)

	queuedApply, err := ts.Storage.Applies().GetByApplyIdentifier(ctx, applyIdentifier)
	require.NoError(t, err)
	require.NotNil(t, queuedApply)
	assert.Equal(t, state.Apply.Pending, queuedApply.State)
	assert.Nil(t, queuedApply.StartedAt)

	tasks, err := ts.Storage.Tasks().GetByApplyID(ctx, queuedApply.ID)
	require.NoError(t, err)
	require.NotEmpty(t, tasks)
	for _, task := range tasks {
		assert.Equal(t, state.Task.Pending, task.State)
	}

	// Once workers start, the scheduler should claim the queued apply,
	// dispatch the local engine work, and drive the apply to completion.
	require.NoError(t, ts.Service.SetSchedulerPollInterval(schedulerTestPollInterval))
	ts.Service.StartScheduler(ctx)
	defer ts.Service.StopScheduler()

	waitForSchedulerAppliesCompleted(t, ts.Storage, []int64{queuedApply.ID}, schedulerTestPollInterval+5*time.Second)
}

func TestScheduler_PendingClaimCrashWindowDispatchesToCompletion(t *testing.T) {
	ctx := t.Context()
	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err)

	appDBName, appDSN := createTestDB(t, "pending_crash_")
	ts := startTestServer(t, appDBName, appDSN)
	ts.Service.StopScheduler()

	planResp := postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
		"database": appDBName, "environment": "staging", "type": "mysql",
		"schema_files": map[string]any{"default": map[string]any{"files": map[string]string{"users.sql": string(schemaSQL)}}},
	})
	planID, _ := planResp["plan_id"].(string)
	require.NotEmpty(t, planID)
	plan, err := ts.Storage.Plans().Get(ctx, planID)
	require.NoError(t, err)
	require.NotNil(t, plan)

	now := time.Now()
	applyID, err := ts.Storage.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-pending-crash-%d", now.UnixNano()%100000),
		PlanID:          plan.ID,
		Database:        appDBName,
		DatabaseType:    storage.DatabaseTypeMySQL,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Running,
		Options:         []byte("{}"),
		Environment:     "staging",
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	require.NoError(t, err)

	schemabotDB, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err)
	require.NoError(t, schemabotDB.PingContext(ctx))
	defer utils.CloseAndLog(schemabotDB)
	_, err = schemabotDB.ExecContext(ctx, "UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?", applyID)
	require.NoError(t, err)

	for _, tc := range plan.FlatDDLChanges() {
		_, err := ts.Storage.Tasks().Create(ctx, &storage.Task{
			TaskIdentifier: fmt.Sprintf("task-pending-crash-%d", time.Now().UnixNano()%100000),
			ApplyID:        applyID,
			PlanID:         plan.ID,
			Database:       appDBName,
			DatabaseType:   storage.DatabaseTypeMySQL,
			Engine:         storage.EngineSpirit,
			State:          state.Task.Pending,
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

	// This is the durable state left if a worker claimed a queued apply and
	// crashed before starting the engine. Recovery should dispatch it exactly
	// like a fresh pending apply, not look for existing engine metadata.
	require.NoError(t, ts.Service.SetSchedulerPollInterval(schedulerTestPollInterval))
	ts.Service.StartScheduler(ctx)
	defer ts.Service.StopScheduler()

	waitForSchedulerAppliesCompleted(t, ts.Storage, []int64{applyID}, schedulerTestPollInterval+5*time.Second)
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
		{name: "failed retryable", applyState: state.Apply.FailedRetryable, databaseType: "mysql", engine: "spirit", wantClaim: true},
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
			applyID, err := stor.Applies().Create(ctx, &storage.Apply{
				ApplyIdentifier: applyIdentifier,
				Database:        appDBName,
				DatabaseType:    tc.databaseType,
				Engine:          tc.engine,
				State:           tc.applyState,
				Options:         []byte("{}"),
				Environment:     "staging",
			})
			require.NoError(t, err)
			if tc.applyState == state.Apply.Pending {
				// Pending represents a fully queued apply only after tasks are
				// persisted; workers ignore partially written apply rows.
				now := time.Now()
				_, err = stor.Tasks().Create(ctx, &storage.Task{
					TaskIdentifier: "task-" + applyIdentifier,
					ApplyID:        applyID,
					Database:       appDBName,
					DatabaseType:   tc.databaseType,
					Engine:         tc.engine,
					State:          state.Task.Pending,
					Environment:    "staging",
					TableName:      "users",
					DDLAction:      "CREATE",
					CreatedAt:      now,
					UpdatedAt:      now,
				})
				require.NoError(t, err)
			}
			_, err = schemabotDB.ExecContext(ctx,
				"UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE apply_identifier = ?",
				applyIdentifier)
			require.NoError(t, err)

			// The scheduler should only claim stale applies in states that recovery can resume safely.
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

// TestScheduler_RetryableApplyResumesToCompletion seeds storage with a
// retryable apply and retryable task, then starts the scheduler. The scheduler
// should claim the apply, reset retryable work, dispatch it through Tern, and
// complete both the apply and task while incrementing attempt counters once.
func TestScheduler_RetryableApplyResumesToCompletion(t *testing.T) {
	ctx := t.Context()
	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err)

	appDBName, appDSN := createTestDB(t, "retryable_sched_")
	ts := startTestServer(t, appDBName, appDSN)
	ts.Service.StopScheduler()

	planResp := postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
		"database": appDBName, "environment": "staging", "type": "mysql",
		"schema_files": map[string]any{"default": map[string]any{"files": map[string]string{"users.sql": string(schemaSQL)}}},
	})
	planID, _ := planResp["plan_id"].(string)
	require.NotEmpty(t, planID)
	plan, err := ts.Storage.Plans().Get(ctx, planID)
	require.NoError(t, err)
	require.NotNil(t, plan)

	// The persisted state represents an apply whose previous engine dispatch
	// stopped on a retryable error before scheduler recovery picked it up.
	now := time.Now()
	retryApply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-retryable-%d", now.UnixNano()%100000),
		PlanID:          plan.ID,
		Database:        appDBName,
		DatabaseType:    storage.DatabaseTypeMySQL,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.FailedRetryable,
		ErrorMessage:    "transient engine error",
		Options:         []byte("{}"),
		Environment:     "staging",
	}
	applyID, err := ts.Storage.Applies().Create(ctx, retryApply)
	require.NoError(t, err)

	for _, tc := range plan.FlatDDLChanges() {
		_, err := ts.Storage.Tasks().Create(ctx, &storage.Task{
			TaskIdentifier: fmt.Sprintf("task-retryable-%d", time.Now().UnixNano()%100000),
			ApplyID:        applyID,
			PlanID:         plan.ID,
			Database:       appDBName,
			DatabaseType:   storage.DatabaseTypeMySQL,
			Engine:         storage.EngineSpirit,
			State:          state.Task.FailedRetryable,
			ErrorMessage:   "transient engine error",
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

	require.NoError(t, ts.Service.SetSchedulerPollInterval(schedulerTestPollInterval))
	ts.Service.StartScheduler(ctx)
	defer ts.Service.StopScheduler()

	waitForSchedulerAppliesCompleted(t, ts.Storage, []int64{applyID}, schedulerTestPollInterval+5*time.Second)

	apply, err := ts.Storage.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	assert.Equal(t, 1, apply.Attempt)

	tasks, err := ts.Storage.Tasks().GetByApplyID(ctx, applyID)
	require.NoError(t, err)
	require.NotEmpty(t, tasks)
	for _, task := range tasks {
		assert.Equal(t, state.Task.Completed, task.State)
		assert.Equal(t, 1, task.Attempt)
	}
}

// TestScheduler_RetryableApplyClaimCrashWindowResumesToCompletion models the
// state left if a worker claims failed_retryable work and crashes before Tern
// prepares the retry. The apply is already leased as running, but the task still
// carries failed_retryable; recovery should still use the retry path.
func TestScheduler_RetryableApplyClaimCrashWindowResumesToCompletion(t *testing.T) {
	ctx := t.Context()
	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err)

	appDBName, appDSN := createTestDB(t, "retry_crash_")
	ts := startTestServer(t, appDBName, appDSN)
	ts.Service.StopScheduler()

	planResp := postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
		"database": appDBName, "environment": "staging", "type": "mysql",
		"schema_files": map[string]any{"default": map[string]any{"files": map[string]string{"users.sql": string(schemaSQL)}}},
	})
	planID, _ := planResp["plan_id"].(string)
	require.NotEmpty(t, planID)
	plan, err := ts.Storage.Plans().Get(ctx, planID)
	require.NoError(t, err)
	require.NotNil(t, plan)

	now := time.Now()
	applyID, err := ts.Storage.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-retry-crash-%d", now.UnixNano()%100000),
		PlanID:          plan.ID,
		Database:        appDBName,
		DatabaseType:    storage.DatabaseTypeMySQL,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Running,
		ErrorMessage:    "transient engine error",
		Attempt:         1,
		Options:         []byte("{}"),
		Environment:     "staging",
		StartedAt:       &now,
	})
	require.NoError(t, err)

	schemabotDB, err := sql.Open("mysql", schemabotDSN)
	require.NoError(t, err)
	require.NoError(t, schemabotDB.PingContext(ctx))
	defer utils.CloseAndLog(schemabotDB)
	_, err = schemabotDB.ExecContext(ctx, "UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?", applyID)
	require.NoError(t, err)

	for _, tc := range plan.FlatDDLChanges() {
		_, err := ts.Storage.Tasks().Create(ctx, &storage.Task{
			TaskIdentifier: fmt.Sprintf("task-retry-crash-%d", time.Now().UnixNano()%100000),
			ApplyID:        applyID,
			PlanID:         plan.ID,
			Database:       appDBName,
			DatabaseType:   storage.DatabaseTypeMySQL,
			Engine:         storage.EngineSpirit,
			State:          state.Task.FailedRetryable,
			ErrorMessage:   "transient engine error",
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

	require.NoError(t, ts.Service.SetSchedulerPollInterval(schedulerTestPollInterval))
	ts.Service.StartScheduler(ctx)
	defer ts.Service.StopScheduler()

	waitForSchedulerAppliesCompleted(t, ts.Storage, []int64{applyID}, schedulerTestPollInterval+5*time.Second)

	apply, err := ts.Storage.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	assert.Equal(t, 1, apply.Attempt)

	tasks, err := ts.Storage.Tasks().GetByApplyID(ctx, applyID)
	require.NoError(t, err)
	require.NotEmpty(t, tasks)
	for _, task := range tasks {
		assert.Equal(t, state.Task.Completed, task.State)
		assert.Equal(t, 1, task.Attempt)
	}
}

// TestScheduler_ExhaustedRetryableApplyExpiresToFailed seeds a retryable apply
// at the retry limit. The scheduler should stop retrying it and finalize the
// apply and unfinished tasks as failed.
func TestScheduler_ExhaustedRetryableApplyExpiresToFailed(t *testing.T) {
	ctx := t.Context()
	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err)

	appDBName, appDSN := createTestDB(t, "retry_exhaust_")
	ts := startTestServer(t, appDBName, appDSN)
	ts.Service.StopScheduler()

	planResp := postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
		"database": appDBName, "environment": "staging", "type": "mysql",
		"schema_files": map[string]any{"default": map[string]any{"files": map[string]string{"users.sql": string(schemaSQL)}}},
	})
	planID, _ := planResp["plan_id"].(string)
	require.NotEmpty(t, planID)
	plan, err := ts.Storage.Plans().Get(ctx, planID)
	require.NoError(t, err)
	require.NotNil(t, plan)

	// Attempt is already at the retry limit, so this apply is eligible for
	// expiration instead of another scheduler dispatch.
	now := time.Now()
	applyID, err := ts.Storage.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-exhausted-%d", now.UnixNano()%100000),
		PlanID:          plan.ID,
		Database:        appDBName,
		DatabaseType:    storage.DatabaseTypeMySQL,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.FailedRetryable,
		ErrorMessage:    "transient engine error after retries",
		Attempt:         10,
		Options:         []byte("{}"),
		Environment:     "staging",
	})
	require.NoError(t, err)

	taskID := fmt.Sprintf("task-exhausted-%d", time.Now().UnixNano()%100000)
	_, err = ts.Storage.Tasks().Create(ctx, &storage.Task{
		TaskIdentifier: taskID,
		ApplyID:        applyID,
		PlanID:         plan.ID,
		Database:       appDBName,
		DatabaseType:   storage.DatabaseTypeMySQL,
		Engine:         storage.EngineSpirit,
		State:          state.Task.FailedRetryable,
		ErrorMessage:   "transient engine error after retries",
		TableName:      "users",
		DDL:            string(schemaSQL),
		DDLAction:      "create",
		Options:        []byte("{}"),
		Environment:    "staging",
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	require.NoError(t, err)

	require.NoError(t, ts.Service.SetSchedulerPollInterval(schedulerTestPollInterval))
	ts.Service.StartScheduler(ctx)
	defer ts.Service.StopScheduler()

	deadline := time.Now().Add(schedulerTestPollInterval + 5*time.Second)
	for time.Now().Before(deadline) {
		apply, err := ts.Storage.Applies().Get(ctx, applyID)
		require.NoError(t, err)
		if apply != nil && apply.State == state.Apply.Failed {
			assert.NotNil(t, apply.CompletedAt)
			task, err := ts.Storage.Tasks().Get(ctx, taskID)
			require.NoError(t, err)
			require.NotNil(t, task)
			assert.Equal(t, state.Task.Failed, task.State)
			assert.NotNil(t, task.CompletedAt)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Fail(t, "scheduler did not expire exhausted retryable apply")
}

// TestScheduler_FindNextApplyDoesNotReclaimRetryableImmediately verifies the
// storage claim lease for retryable applies. After one worker claims the row,
// the refreshed heartbeat should prevent another immediate claim of the same
// apply.
func TestScheduler_FindNextApplyDoesNotReclaimRetryableImmediately(t *testing.T) {
	ctx := t.Context()

	fixture := newSchedulerClaimFixture(t, "retry_claim_")
	stor := fixture.store

	_, err := stor.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-retryable-exclusive",
		Database:        fixture.appDBName,
		DatabaseType:    storage.DatabaseTypeMySQL,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.FailedRetryable,
		ErrorMessage:    "transient engine error",
		Options:         []byte("{}"),
		Environment:     "staging",
	})
	require.NoError(t, err)

	claimed, err := stor.Applies().FindNextApply(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, "apply-retryable-exclusive", claimed.ApplyIdentifier)
	assert.Equal(t, state.Apply.FailedRetryable, claimed.State)
	assert.Equal(t, 1, claimed.Attempt)

	claimedAgain, err := stor.Applies().FindNextApply(ctx)
	require.NoError(t, err)
	assert.Nil(t, claimedAgain, "claimed retryable apply should have a fresh active lease")
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

	schedulerPollInterval := schedulerTestPollInterval
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
