//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/e2e/testutil"
	schemabotapi "github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// These tests exercise operator behavior at two levels: the full driver loop
// in the resume tests, and the atomic claim query through
// FindNextApplyOperation. Operator drivers claim an apply_operations row
// before driving it, so direct calls keep claim policy tests focused without
// waiting for ticks.

type operatorClaimFixture struct {
	appDBName string
	storageDB *sql.DB
	store     storage.Storage
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

func (c *blockingResumeClient) ResumeApplyOperation(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	select {
	case c.started <- struct{}{}:
	default:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.release:
	}

	return c.Client.ResumeApplyOperation(ctx, apply, applyOperationID)
}

func (c *blockingResumeClient) waitForResume(t *testing.T, timeout time.Duration) {
	t.Helper()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-c.started:
	case <-timer.C:
		require.Failf(t, "timeout", "operator did not claim blocked apply within %s", timeout)
	}
}

// newOperatorClaimFixture creates a real target database plus a clean SchemaBot
// metadata store. The claim-policy tests write apply rows directly into storage
// so they can test operator decisions without depending on driver timing.
func newOperatorClaimFixture(t *testing.T, appDBPrefix string) *operatorClaimFixture {
	t.Helper()

	appDBName, _ := createTestDB(t, appDBPrefix)
	storageDB, err := sql.Open("block-mysql", schemabotDSN)
	require.NoError(t, err)
	require.NoError(t, storageDB.PingContext(t.Context()))
	clearStorageDB(t, storageDB)
	t.Cleanup(func() {
		utils.CloseAndLog(storageDB)
	})

	return &operatorClaimFixture{
		appDBName: appDBName,
		storageDB: storageDB,
		store:     mysqlstore.New(storageDB),
	}
}

func (f *operatorClaimFixture) resetStorage(t *testing.T) {
	t.Helper()
	clearStorageDB(t, f.storageDB)
}

func TestOperator_BasicClaimAndResume(t *testing.T) {
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
	ts.Service.StopOperator()

	// Remove the table so the second plan contains DDL that recovery can resume.
	targetConn, err := sql.Open("block-mysql", appDSN)
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

	// Seed storage with a stale running apply, its operation row, and running
	// tasks, matching the state left behind when a driver stops heartbeating
	// before completing.
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

	staleOpID, err := ts.Storage.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:    staleID,
		Deployment: appDBName,
		Target:     appDBName,
	})
	require.NoError(t, err)
	require.NoError(t, ts.Storage.ApplyOperations().MarkStarted(ctx, staleOpID))

	schemabotDB, err := sql.Open("block-mysql", schemabotDSN)
	require.NoError(t, err)
	require.NoError(t, schemabotDB.PingContext(ctx))
	defer utils.CloseAndLog(schemabotDB)
	_, err = schemabotDB.ExecContext(ctx, "UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?", staleID)
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "UPDATE apply_operations SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?", staleOpID)
	require.NoError(t, err)

	for _, tc := range plan2.FlatDDLChanges() {
		_, err := ts.Storage.Tasks().Create(ctx, &storage.Task{
			TaskIdentifier:   fmt.Sprintf("task-stale-%d", time.Now().UnixNano()%100000),
			ApplyID:          staleID,
			ApplyOperationID: &staleOpID,
			PlanID:           plan2.ID,
			Database:         appDBName,
			DatabaseType:     "mysql",
			Engine:           "spirit",
			State:            state.Task.Running,
			TableName:        tc.Table,
			DDL:              tc.DDL,
			DDLAction:        tc.Operation,
			Options:          []byte("{}"),
			Environment:      "staging",
			CreatedAt:        now,
			UpdatedAt:        now,
		})
		require.NoError(t, err)
	}

	// Operator recovery should claim the stale apply and resume it to completion.
	ts.Service.StartOperator(t.Context())
	defer ts.Service.StopOperator()

	var lastState string
	testutil.Poll(t, 20*time.Second, 500*time.Millisecond,
		func() bool {
			apply, err := ts.Storage.Applies().Get(ctx, staleID)
			require.NoError(t, err)
			if apply != nil {
				lastState = apply.State
			}
			return apply != nil && apply.State == state.Apply.Completed
		},
		func() string {
			return fmt.Sprintf("operator did not resume stale apply %d, last state: %q", staleID, lastState)
		},
	)

	// The claim must appear in the apply's durable log so an operator reading
	// the timeline sees why new state transitions follow the stale period.
	logs, err := ts.Storage.ApplyLogs().List(ctx, storage.ApplyLogFilter{ApplyID: staleID})
	require.NoError(t, err)
	claimLogged := false
	for _, entry := range logs {
		if strings.Contains(entry.Message, "Operator claimed apply to resume it") {
			claimLogged = true
			assert.Equal(t, storage.LogLevelInfo, entry.Level)
			assert.Equal(t, storage.LogSourceSchemaBot, entry.Source)
			break
		}
	}
	assert.True(t, claimLogged, "apply_logs should record the operator claim for apply %d", staleID)
}

// TestOperator_OperatorClaimsOperationToCompletion exercises the operation-level
// claim loop. An apply created through the normal flow dual-writes exactly one
// apply_operations row; the operator claims that row, acquires the parent apply
// lease, drives the schema change to completion, and marks the operation row
// completed.
func TestOperator_OperatorClaimsOperationToCompletion(t *testing.T) {
	ctx := t.Context()
	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err)

	appDBName, appDSN := createTestDB(t, "operator_claim_")
	ts := startTestServer(t, appDBName, appDSN)

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
	require.NotEmpty(t, applyID)

	waitForState(t, "http://"+ts.Addr, applyID, "completed", 20*time.Second)

	apply, err := ts.Storage.Applies().GetByApplyIdentifier(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)

	// The operator marks the operation terminal after the parent apply is
	// persisted completed, so poll for the operation's terminal state rather
	// than reading it the instant the apply finishes.
	require.Eventually(t, func() bool {
		ops, err := ts.Storage.ApplyOperations().ListByApply(ctx, apply.ID)
		if err != nil || len(ops) != 1 {
			return false
		}
		return state.IsState(ops[0].State, state.ApplyOperation.Completed)
	}, 10*time.Second, 100*time.Millisecond,
		"operator should mark the claimed operation completed after the apply finishes")

	ops, err := ts.Storage.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, ops, 1, "apply-create dual-write emits exactly one operation row")
	assert.Equal(t, state.ApplyOperation.Completed, ops[0].State, "operator marks the claimed operation completed")
	assert.Equal(t, apply.Deployment, ops[0].Deployment)
	require.NotNil(t, ops[0].CompletedAt, "completed operation stamps completed_at")

	// applies.state is derived from its child operation rows: the parent must
	// equal DeriveApplyState of the operation states it owns.
	finalApply, err := ts.Storage.Applies().Get(ctx, apply.ID)
	require.NoError(t, err)
	require.NotNil(t, finalApply)
	derived := state.DeriveApplyState([]string{ops[0].State})
	assert.Equal(t, state.Apply.Completed, derived)
	assert.Equal(t, derived, finalApply.State, "parent apply state is derived from its child operations")
}

// TestOperator_OperatorSkipsOperationWhenParentTerminal covers the safety case
// where an apply_operations row is still pending while its parent apply is
// already terminal — for example operation-level claiming is enabled after the
// apply finished, or the parent reached a terminal state via another path. The
// rollout's outcome is settled, so the operator must not claim the row: it
// stays pending and unleased, and it must not sit at the head of the claim
// queue shadowing live work — an apply submitted behind it still runs to
// completion.
func TestOperator_OperatorSkipsOperationWhenParentTerminal(t *testing.T) {
	ctx := t.Context()
	appDBName, appDSN := createTestDB(t, "operator_skip_terminal_")
	ts := startTestServer(t, appDBName, appDSN)

	// A pending operation under an already-completed parent, created before any
	// live work so it sits ahead of it in claim order.
	now := time.Now()
	deadParentID, err := ts.Storage.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-terminal-parent",
		Database:        appDBName,
		DatabaseType:    "mysql",
		Deployment:      appDBName,
		Engine:          "spirit",
		State:           state.Apply.Completed,
		Options:         []byte("{}"),
		Environment:     "staging",
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	require.NoError(t, err)

	deadOpID, err := ts.Storage.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:    deadParentID,
		Deployment: appDBName,
		Target:     appDBName,
		State:      state.ApplyOperation.Pending,
	})
	require.NoError(t, err)

	// Live work submitted behind the dead row completes: the operator's claim
	// skips the dead row instead of wedging on it.
	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err)
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
	liveApplyID, _ := applyResp["apply_id"].(string)
	require.NotEmpty(t, liveApplyID)

	waitForState(t, "http://"+ts.Addr, liveApplyID, "completed", 20*time.Second)

	// The dead row is untouched: still pending, never claimed, and its settled
	// parent still reads completed.
	deadOp, err := ts.Storage.ApplyOperations().Get(ctx, deadOpID)
	require.NoError(t, err)
	require.NotNil(t, deadOp)
	assert.Equal(t, state.ApplyOperation.Pending, deadOp.State, "a pending operation under a terminal parent is never claimed")
	assert.Empty(t, deadOp.LeaseOwner, "no driver leases the dead row")
	assert.Nil(t, deadOp.StartedAt, "the dead row is never started")

	deadParent, err := ts.Storage.Applies().Get(ctx, deadParentID)
	require.NoError(t, err)
	require.NotNil(t, deadParent)
	assert.Equal(t, state.Apply.Completed, deadParent.State, "the settled parent's outcome is untouched")
}

// TestOperator_ReconcilesClaimedOperationWhoseParentSettled covers an operation
// a driver claimed while its parent apply was still going somewhere, whose
// parent settled before the driver got to it — the row was under way, so the
// stale-lease recovery arm reaches it regardless of the parent's state. The
// driver cannot claim a settled parent, and leaving the row non-terminal would
// re-lease it on every poll forever, so it mirrors the parent's outcome onto the
// operation instead.
func TestOperator_ReconcilesClaimedOperationWhoseParentSettled(t *testing.T) {
	ctx := t.Context()
	appDBName, appDSN := createTestDB(t, "operator_settled_parent_")
	ts := startTestServer(t, appDBName, appDSN)

	now := time.Now()
	parentID, err := ts.Storage.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-settled-parent",
		Database:        appDBName,
		DatabaseType:    "mysql",
		Deployment:      appDBName,
		Engine:          "spirit",
		State:           state.Apply.Completed,
		Options:         []byte("{}"),
		Environment:     "staging",
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	require.NoError(t, err)

	opID, err := ts.Storage.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:    parentID,
		Deployment: appDBName,
		Target:     appDBName,
		State:      state.ApplyOperation.Running,
	})
	require.NoError(t, err)

	// Age the operation's heartbeat past the lease staleness window so the
	// driver that was running it is no longer presumed live. Written directly
	// because every storage write path refreshes updated_at, which is the
	// heartbeat itself.
	storageDB, err := sql.Open("block-mysql", schemabotDSN)
	require.NoError(t, err, "open schemabot db")
	require.NoError(t, storageDB.PingContext(ctx))
	t.Cleanup(func() {
		utils.CloseAndLog(storageDB)
	})
	_, err = storageDB.ExecContext(ctx,
		`UPDATE apply_operations SET updated_at = NOW() - INTERVAL ? SECOND WHERE id = ?`,
		int64((2 * storage.ApplyLeaseStaleAfter).Seconds()), opID)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		op, err := ts.Storage.ApplyOperations().Get(ctx, opID)
		require.NoError(t, err)
		require.NotNil(t, op)
		return state.IsState(op.State, state.ApplyOperation.Completed)
	}, 30*time.Second, 200*time.Millisecond,
		"the claimed operation must be reconciled to its settled parent's state instead of being re-claimed forever")

	parent, err := ts.Storage.Applies().Get(ctx, parentID)
	require.NoError(t, err)
	require.NotNil(t, parent)
	assert.Equal(t, state.Apply.Completed, parent.State, "the settled parent's outcome is untouched")
}

// TestOperator_StartResumesStoppedApplyWhoseDeploymentsNeverStarted covers a
// rollout stopped before any of its deployments began: the apply is stopped and
// every operation row is still pending, so there is no already-started row for
// the resume to go through. An accepted start request must still be serviced —
// the operator claims the never-started operation and drives the deployment to
// completion — otherwise the request would be durably recorded and then acted on
// by nothing, and the only escape would be cancelling and losing the rollout.
func TestOperator_StartResumesStoppedApplyWhoseDeploymentsNeverStarted(t *testing.T) {
	ctx := t.Context()
	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err)

	appDBName, appDSN := createTestDB(t, "start_resumes_never_started_")
	ts := startTestServer(t, appDBName, appDSN)

	// Apply the schema normally so a plan exists, then drop the table so a
	// replan yields DDL the resumed operation actually has to run.
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
	firstApplyID, _ := applyResp["apply_id"].(string)
	require.NotEmpty(t, firstApplyID)
	waitForState(t, "http://"+ts.Addr, firstApplyID, "completed", 20*time.Second)
	ts.Service.StopOperator()

	targetConn, err := sql.Open("block-mysql", appDSN)
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

	// The wedge shape: a stopped apply whose one deployment never started, so
	// its operation row is still pending. The tasks carry the stop, which is
	// where a stopped rollout's remaining work lives.
	now := time.Now()
	applyDBID, err := ts.Storage.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-start-never-started",
		PlanID:          plan2.ID,
		Database:        appDBName,
		DatabaseType:    "mysql",
		Deployment:      appDBName,
		Engine:          "spirit",
		State:           state.Apply.Stopped,
		Options:         []byte("{}"),
		Environment:     "staging",
		StartedAt:       &now,
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	require.NoError(t, err)

	opID, err := ts.Storage.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:    applyDBID,
		Deployment: appDBName,
		Target:     appDBName,
		State:      state.ApplyOperation.Pending,
	})
	require.NoError(t, err)

	for _, tc := range plan2.FlatDDLChanges() {
		_, err := ts.Storage.Tasks().Create(ctx, &storage.Task{
			TaskIdentifier:   fmt.Sprintf("task-start-never-started-%d", time.Now().UnixNano()),
			ApplyID:          applyDBID,
			ApplyOperationID: &opID,
			PlanID:           plan2.ID,
			Database:         appDBName,
			DatabaseType:     "mysql",
			Engine:           "spirit",
			State:            state.Task.Stopped,
			Namespace:        tc.Namespace,
			TableName:        tc.Table,
			DDL:              tc.DDL,
			DDLAction:        tc.Operation,
			Options:          []byte("{}"),
			Environment:      "staging",
			CreatedAt:        now,
			UpdatedAt:        now,
		})
		require.NoError(t, err)
	}

	_, _, err = ts.Storage.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     applyDBID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "test",
	})
	require.NoError(t, err)

	ts.Service.StartOperator(t.Context())
	defer ts.Service.StopOperator()

	require.Eventually(t, func() bool {
		op, err := ts.Storage.ApplyOperations().Get(ctx, opID)
		require.NoError(t, err)
		return op != nil && state.IsState(op.State, state.ApplyOperation.Completed)
	}, 30*time.Second, 200*time.Millisecond,
		"a start request must resume a stopped rollout whose deployments never started")

	// The rollout really ran: the parent settles from its operation row and the
	// dropped table is back on the target.
	parent, err := ts.Storage.Applies().Get(ctx, applyDBID)
	require.NoError(t, err)
	require.NotNil(t, parent)
	assert.Equal(t, state.Apply.Completed, parent.State, "the resumed apply settles from its completed operation")

	var tableName string
	require.NoError(t, targetConn.QueryRowContext(ctx, "SHOW TABLES LIKE 'users'").Scan(&tableName),
		"the resumed deployment must have run its DDL against the target")
	assert.Equal(t, "users", tableName)

	// The start request is consumed rather than left pending forever.
	require.Eventually(t, func() bool {
		req, err := ts.Storage.ControlRequests().GetPending(ctx, applyDBID, storage.ControlOperationStart)
		return err == nil && req == nil
	}, 10*time.Second, 100*time.Millisecond,
		"the serviced start request is completed")
}

// TestOperator_ProjectsApplyLeftBehindItsSettledOperations covers a parent apply
// left behind its own children: a drive wrote its terminal operation row and
// then stopped before projecting that outcome onto the parent, so every
// operation is terminal while the apply itself is still running. No operation
// claim arm matches a fully settled operation set, and until the parent settles
// the one-active-apply guard refuses every new apply for that target. Once the
// crashed drive's lease goes stale the operator must derive the parent from its
// operations and release the target, leaving the operation rows untouched.
func TestOperator_ProjectsApplyLeftBehindItsSettledOperations(t *testing.T) {
	ctx := t.Context()
	appDBName, appDSN := createTestDB(t, "operator_projection_")
	ts := startTestServer(t, appDBName, appDSN)

	now := time.Now()
	applyID, err := ts.Storage.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-settled-ops-orphan",
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

	opID, err := ts.Storage.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:    applyID,
		Deployment: appDBName,
		Target:     appDBName,
		State:      state.ApplyOperation.Completed,
	})
	require.NoError(t, err)

	// Age the apply's heartbeat past the lease staleness window so the crashed
	// drive is no longer presumed live. Written directly because every storage
	// write path refreshes updated_at, which is the heartbeat itself.
	storageDB, err := sql.Open("block-mysql", schemabotDSN)
	require.NoError(t, err, "open schemabot db")
	require.NoError(t, storageDB.PingContext(ctx))
	t.Cleanup(func() {
		utils.CloseAndLog(storageDB)
	})
	_, err = storageDB.ExecContext(ctx,
		`UPDATE applies SET updated_at = NOW() - INTERVAL ? SECOND WHERE id = ?`,
		int64((2 * storage.ApplyLeaseStaleAfter).Seconds()), applyID)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		apply, err := ts.Storage.Applies().Get(ctx, applyID)
		if err != nil || apply == nil {
			return false
		}
		return state.IsState(apply.State, state.Apply.Completed)
	}, 30*time.Second, 200*time.Millisecond,
		"operator should derive the parent apply from its already-settled operations")

	apply, err := ts.Storage.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	assert.Equal(t, state.Apply.Completed, apply.State, "the parent settles to the state its operations derive")
	require.NotNil(t, apply.CompletedAt, "a parent settled to a terminal state stamps completed_at")

	op, err := ts.Storage.ApplyOperations().Get(ctx, opID)
	require.NoError(t, err)
	require.NotNil(t, op)
	assert.Equal(t, state.ApplyOperation.Completed, op.State,
		"the repair derives the parent from the operations and never rewrites them")
}

// TestOperator_StopReconciliationDrivesTaskStop covers the operation-claim stop
// path for an apply whose operation row is still pending when a stop is
// requested — the window before the operator claims and drives the operation.
// Stop reconciliation must drive the data-plane stop so the task settles to
// stopped, not only flip the operation and apply rows. Otherwise the apply reads
// stopped while its task stays non-terminal, and a later start finds no stopped
// task to resume.
func TestOperator_StopReconciliationDrivesTaskStop(t *testing.T) {
	ctx := t.Context()
	appDBName, appDSN := createTestDB(t, "stop_reconcile_drive_")
	ts := startTestServer(t, appDBName, appDSN)

	now := time.Now()
	applyID, err := ts.Storage.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-stop-reconcile-drive",
		Database:        appDBName,
		DatabaseType:    "mysql",
		Deployment:      appDBName,
		Engine:          "spirit",
		State:           state.Apply.Pending,
		Options:         []byte("{}"),
		Environment:     "staging",
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	require.NoError(t, err)

	opID, err := ts.Storage.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:    applyID,
		Deployment: appDBName,
		Target:     appDBName,
		State:      state.ApplyOperation.Pending,
	})
	require.NoError(t, err)

	const taskIdentifier = "task-stop-reconcile-drive"
	_, err = ts.Storage.Tasks().Create(ctx, &storage.Task{
		TaskIdentifier:   taskIdentifier,
		ApplyID:          applyID,
		ApplyOperationID: &opID,
		Database:         appDBName,
		DatabaseType:     "mysql",
		Engine:           "spirit",
		Environment:      "staging",
		State:            state.Task.Pending,
		TableName:        "users",
		CreatedAt:        now,
		UpdatedAt:        now,
	})
	require.NoError(t, err)

	_, _, err = ts.Storage.ControlRequests().RequestPending(ctx, &storage.ApplyControlRequest{
		ApplyID:     applyID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "test",
	})
	require.NoError(t, err)

	// The operator drives the data-plane stop, settling the task to stopped
	// before terminalizing the operation and apply rows. Wait for the full
	// settled state in one condition: the operation and apply rows are
	// terminalized after the task stop, so polling them independently would
	// race that window. The load-bearing assertion is task == stopped: without
	// the fix the operator flips the operation/apply rows but never drives the
	// task, so this condition never holds and the test fails.
	require.Eventually(t, func() bool {
		task, err := ts.Storage.Tasks().Get(ctx, taskIdentifier)
		if err != nil || task == nil || !state.IsState(task.State, state.Task.Stopped) {
			return false
		}
		op, err := ts.Storage.ApplyOperations().Get(ctx, opID)
		if err != nil || op == nil || !state.IsState(op.State, state.ApplyOperation.Stopped) {
			return false
		}
		apply, err := ts.Storage.Applies().Get(ctx, applyID)
		if err != nil || apply == nil || !state.IsState(apply.State, state.Apply.Stopped) {
			return false
		}
		return true
	}, 10*time.Second, 100*time.Millisecond,
		"stop reconciliation must drive the task to stopped and terminalize the operation and apply rows")

	task, err := ts.Storage.Tasks().Get(ctx, taskIdentifier)
	require.NoError(t, err)
	require.NotNil(t, task)
	assert.Equal(t, state.Task.Stopped, task.State, "task settles to stopped via the data-plane drive")

	op, err := ts.Storage.ApplyOperations().Get(ctx, opID)
	require.NoError(t, err)
	require.NotNil(t, op)
	assert.Equal(t, state.ApplyOperation.Stopped, op.State, "operation row is terminalized to stopped")

	apply, err := ts.Storage.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	assert.Equal(t, state.Apply.Stopped, apply.State, "apply settles to stopped")

	// The stop request is completed once the apply has stopped.
	require.Eventually(t, func() bool {
		req, err := ts.Storage.ControlRequests().GetPending(ctx, applyID, storage.ControlOperationStop)
		return err == nil && req == nil
	}, 5*time.Second, 100*time.Millisecond,
		"pending stop request is completed after the apply stops")
}

// TestOperator_OperationDeploymentDrivesByOperationDeployment proves the
// operation-claim path routes the drive by the claimed apply_operations row's
// own deployment, not the parent apply's stored deployment. The parent apply's
// deployment is only the primary deployment for legacy queries; the operation
// deployment is the routing key. Here the parent's stored deployment is a
// non-routable placeholder while the operation's deployment is the real,
// configured target, so the operator must still drive the operation to
// completion via its own deployment.
func TestOperator_OperationDeploymentDrivesByOperationDeployment(t *testing.T) {
	ctx := t.Context()
	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err)

	appDBName, appDSN := createTestDB(t, "op_deploy_mismatch_")
	ts := startTestServer(t, appDBName, appDSN)

	// Apply the schema normally so the target reaches the desired state, then
	// drop the table so a replan yields DDL a resumed operation could run.
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
	require.NotEmpty(t, applyID)
	waitForState(t, "http://"+ts.Addr, applyID, "completed", 20*time.Second)
	ts.Service.StopOperator()

	targetConn, err := sql.Open("block-mysql", appDSN)
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

	// Seed a resumable apply whose stored (primary) deployment is a non-routable
	// placeholder, but whose child operation carries the real, configured
	// deployment. The operation's tasks are present and runnable, so the drive
	// must route by the operation's deployment and reach completion.
	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-op-route-%d", now.UnixNano()%100000),
		PlanID:          plan2.ID,
		Database:        appDBName,
		DatabaseType:    "mysql",
		Deployment:      appDBName + "-primary",
		Engine:          "spirit",
		State:           state.Apply.Running,
		Options:         []byte("{}"),
		Environment:     "staging",
		StartedAt:       &now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyDBID, err := ts.Storage.Applies().Create(ctx, apply)
	require.NoError(t, err)

	opID, err := ts.Storage.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:    applyDBID,
		Deployment: appDBName,
		Target:     appDBName,
		State:      state.ApplyOperation.Running,
		StartedAt:  &now,
	})
	require.NoError(t, err)

	for _, tc := range plan2.FlatDDLChanges() {
		_, err := ts.Storage.Tasks().Create(ctx, &storage.Task{
			TaskIdentifier:   fmt.Sprintf("task-op-route-%d", time.Now().UnixNano()),
			ApplyID:          applyDBID,
			ApplyOperationID: &opID,
			PlanID:           plan2.ID,
			Database:         appDBName,
			DatabaseType:     "mysql",
			Engine:           "spirit",
			State:            state.Task.Running,
			TableName:        tc.Table,
			DDL:              tc.DDL,
			DDLAction:        tc.Operation,
			Options:          []byte("{}"),
			Environment:      "staging",
			CreatedAt:        now,
			UpdatedAt:        now,
		})
		require.NoError(t, err)
	}

	// Age both rows so the operation-claim loop leases the operation on its
	// first poll.
	schemabotDB, err := sql.Open("block-mysql", schemabotDSN)
	require.NoError(t, err)
	require.NoError(t, schemabotDB.PingContext(ctx))
	defer utils.CloseAndLog(schemabotDB)
	_, err = schemabotDB.ExecContext(ctx, "UPDATE applies SET updated_at = NOW() - INTERVAL 10 MINUTE WHERE id = ?", applyDBID)
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "UPDATE apply_operations SET updated_at = NOW() - INTERVAL 10 MINUTE WHERE id = ?", opID)
	require.NoError(t, err)

	ts.Service.StartOperator(t.Context())
	defer ts.Service.StopOperator()

	// The drive routes by the operation's deployment, so the operation reaches
	// completed even though it diverges from the parent apply's stored
	// deployment.
	require.Eventually(t, func() bool {
		op, err := ts.Storage.ApplyOperations().Get(ctx, opID)
		require.NoError(t, err)
		return op != nil && state.IsState(op.State, state.ApplyOperation.Completed)
	}, 20*time.Second, 200*time.Millisecond,
		"operator should drive the operation to completion via its own deployment")

	op, err := ts.Storage.ApplyOperations().Get(ctx, opID)
	require.NoError(t, err)
	require.NotNil(t, op)
	require.NotNil(t, op.CompletedAt, "completed operation stamps completed_at")

	// The parent apply is derived from its operation rows, so it settles
	// completed regardless of its non-routable primary deployment.
	parent, err := ts.Storage.Applies().Get(ctx, applyDBID)
	require.NoError(t, err)
	require.NotNil(t, parent)
	assert.Equal(t, state.Apply.Completed, parent.State,
		"parent apply state is derived from its child operation, which completed")
}

// TestOperator_OperationMissingDeploymentFailsClosed covers the safety guard on
// the operation-claim path: an apply_operations row with no deployment has no
// routing key, so the operator must claim it but never drive it to a terminal
// state — driving via any default deployment could run the wrong target.
func TestOperator_OperationMissingDeploymentFailsClosed(t *testing.T) {
	ctx := t.Context()
	schemaSQL, err := os.ReadFile("testdata/myapp/mysql/schema/users.sql")
	require.NoError(t, err)

	appDBName, appDSN := createTestDB(t, "op_missing_deploy_")
	ts := startTestServer(t, appDBName, appDSN)

	planResp := postJSON(t, "http://"+ts.Addr+"/api/plan", map[string]any{
		"database": appDBName, "environment": "staging", "type": "mysql",
		"schema_files": map[string]any{"default": map[string]any{"files": map[string]string{"users.sql": string(schemaSQL)}}},
	})
	planID, _ := planResp["plan_id"].(string)
	require.NotEmpty(t, planID)
	plan, err := ts.Storage.Plans().Get(ctx, planID)
	require.NoError(t, err)
	ts.Service.StopOperator()

	// Seed a resumable apply and a child operation whose deployment is empty.
	// The operation's tasks are present and runnable, so the only thing stopping
	// the drive is the missing-deployment guard.
	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-no-deploy-%d", now.UnixNano()%100000),
		PlanID:          plan.ID,
		Database:        appDBName,
		DatabaseType:    "mysql",
		Deployment:      appDBName,
		Engine:          "spirit",
		State:           state.Apply.Running,
		Options:         []byte("{}"),
		Environment:     "staging",
		StartedAt:       &now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyDBID, err := ts.Storage.Applies().Create(ctx, apply)
	require.NoError(t, err)

	opID, err := ts.Storage.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:    applyDBID,
		Deployment: "",
		Target:     appDBName,
		State:      state.ApplyOperation.Running,
		StartedAt:  &now,
	})
	require.NoError(t, err)

	for _, tc := range plan.FlatDDLChanges() {
		_, err := ts.Storage.Tasks().Create(ctx, &storage.Task{
			TaskIdentifier:   fmt.Sprintf("task-no-deploy-%d", time.Now().UnixNano()),
			ApplyID:          applyDBID,
			ApplyOperationID: &opID,
			PlanID:           plan.ID,
			Database:         appDBName,
			DatabaseType:     "mysql",
			Engine:           "spirit",
			State:            state.Task.Running,
			TableName:        tc.Table,
			DDL:              tc.DDL,
			DDLAction:        tc.Operation,
			Options:          []byte("{}"),
			Environment:      "staging",
			CreatedAt:        now,
			UpdatedAt:        now,
		})
		require.NoError(t, err)
	}

	// Age both rows so the operation-claim loop leases the operation on its
	// first poll.
	schemabotDB, err := sql.Open("block-mysql", schemabotDSN)
	require.NoError(t, err)
	require.NoError(t, schemabotDB.PingContext(ctx))
	defer utils.CloseAndLog(schemabotDB)
	_, err = schemabotDB.ExecContext(ctx, "UPDATE applies SET updated_at = NOW() - INTERVAL 10 MINUTE WHERE id = ?", applyDBID)
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "UPDATE apply_operations SET updated_at = NOW() - INTERVAL 10 MINUTE WHERE id = ?", opID)
	require.NoError(t, err)

	staleOp, err := ts.Storage.ApplyOperations().Get(ctx, opID)
	require.NoError(t, err)
	require.NotNil(t, staleOp)
	seededUpdatedAt := staleOp.UpdatedAt

	ts.Service.StartOperator(t.Context())
	defer ts.Service.StopOperator()

	// The operator must actually claim the operation — claiming refreshes its
	// heartbeat — so we know the missing-deployment guard was reached.
	require.Eventually(t, func() bool {
		op, err := ts.Storage.ApplyOperations().Get(ctx, opID)
		require.NoError(t, err)
		return op != nil && op.UpdatedAt.After(seededUpdatedAt)
	}, 10*time.Second, 200*time.Millisecond,
		"operator should claim the stale operation, reaching the missing-deployment guard")

	// Having reached the guard, it must never drive the operation to a terminal
	// state, because it has no deployment to route by.
	neverTerminalDeadline := time.NewTimer(3 * time.Second)
	defer neverTerminalDeadline.Stop()
	neverTerminalPoll := time.NewTicker(200 * time.Millisecond)
	defer neverTerminalPoll.Stop()

	observingOperation := true
	for observingOperation {
		select {
		case <-neverTerminalDeadline.C:
			observingOperation = false
		case <-neverTerminalPoll.C:
			op, err := ts.Storage.ApplyOperations().Get(ctx, opID)
			require.NoError(t, err)
			if op != nil && state.IsTerminalApplyState(op.State) {
				require.Failf(t, "operation reached terminal state", "operator must fail closed and never drive an operation with no deployment; got state %q", op.State)
			}
		}
	}

	op, err := ts.Storage.ApplyOperations().Get(ctx, opID)
	require.NoError(t, err)
	require.NotNil(t, op)
	assert.Nil(t, op.CompletedAt, "failed-closed operation must not stamp completed_at")

	parent, err := ts.Storage.Applies().Get(ctx, applyDBID)
	require.NoError(t, err)
	require.NotNil(t, parent)
	assert.NotEqual(t, state.Apply.Completed, parent.State,
		"parent apply must not be driven to completion through an operation with no deployment")
}

// TestOperator_OperationWithoutTasksFailsClosed covers the fail-closed
// terminalization on the operation-claim path. When a claimed operation resolves
// to no tasks, the drive cannot make progress: the operator must mark that
// operation failed (so it is not re-leased forever once its heartbeat goes
// stale) and re-derive the parent apply's state from its operations, rather than
// leaving the operation running indefinitely.
func TestOperator_OperationWithoutTasksFailsClosed(t *testing.T) {
	ctx := t.Context()
	appDBName, appDSN := createTestDB(t, "op_no_tasks_")
	ts := startTestServer(t, appDBName, appDSN)
	ts.Service.StopOperator()

	// Seed a resumable apply and a child operation whose deployment matches the
	// parent (so the drive is attempted) but with no tasks scoped to it.
	now := time.Now()
	apply := &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-no-tasks-%d", now.UnixNano()%100000),
		Database:        appDBName,
		DatabaseType:    "mysql",
		Deployment:      appDBName,
		Engine:          "spirit",
		State:           state.Apply.Running,
		Options:         []byte("{}"),
		Environment:     "staging",
		StartedAt:       &now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyDBID, err := ts.Storage.Applies().Create(ctx, apply)
	require.NoError(t, err)

	opID, err := ts.Storage.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:    applyDBID,
		Deployment: appDBName,
		Target:     appDBName,
		State:      state.ApplyOperation.Running,
		StartedAt:  &now,
	})
	require.NoError(t, err)

	// Age both rows so the operation-claim loop leases the operation and its
	// parent apply on the first poll.
	schemabotDB, err := sql.Open("block-mysql", schemabotDSN)
	require.NoError(t, err)
	require.NoError(t, schemabotDB.PingContext(ctx))
	defer utils.CloseAndLog(schemabotDB)
	_, err = schemabotDB.ExecContext(ctx, "UPDATE applies SET updated_at = NOW() - INTERVAL 10 MINUTE WHERE id = ?", applyDBID)
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "UPDATE apply_operations SET updated_at = NOW() - INTERVAL 10 MINUTE WHERE id = ?", opID)
	require.NoError(t, err)

	ts.Service.StartOperator(t.Context())
	defer ts.Service.StopOperator()

	// The operator must terminalize the task-less operation as failed rather
	// than re-leasing it forever.
	require.Eventually(t, func() bool {
		op, err := ts.Storage.ApplyOperations().Get(ctx, opID)
		require.NoError(t, err)
		return op != nil && state.IsState(op.State, state.ApplyOperation.Failed)
	}, 10*time.Second, 200*time.Millisecond,
		"operator should fail closed and mark a task-less operation failed")

	op, err := ts.Storage.ApplyOperations().Get(ctx, opID)
	require.NoError(t, err)
	require.NotNil(t, op)
	assert.Equal(t, state.ApplyOperation.Failed, op.State)
	require.NotNil(t, op.CompletedAt, "a failed operation stamps completed_at")

	// The parent apply state is derived from its child operations, so a single
	// failed operation drives the parent to failed.
	parent, err := ts.Storage.Applies().Get(ctx, applyDBID)
	require.NoError(t, err)
	require.NotNil(t, parent)
	assert.Equal(t, state.Apply.Failed, parent.State,
		"parent apply is derived from its operations and must reflect the failed child")
}

func TestOperator_ClaimOrdering(t *testing.T) {
	ctx := t.Context()

	fixture := newOperatorClaimFixture(t, "ord1_")
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

	olderOpID := seedStartedOperation(t, stor, olderID, db1Name)
	newerOpID := seedStartedOperation(t, stor, newerID, db2Name)
	_, err = schemabotDB.ExecContext(ctx, "UPDATE apply_operations SET created_at = NOW() - INTERVAL 2 MINUTE, updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?", olderOpID)
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "UPDATE apply_operations SET created_at = NOW() - INTERVAL 1 MINUTE, updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?", newerOpID)
	require.NoError(t, err)

	// The operator claim path should pick the oldest stale operation first.
	claimed, err := stor.ApplyOperations().FindNextApplyOperation(ctx, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, olderID, claimed.ApplyID)

	// After the first target is claimed, the operator can claim the next stale target.
	claimed2, err := stor.ApplyOperations().FindNextApplyOperation(ctx, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimed2)
	assert.Equal(t, newerID, claimed2.ApplyID)
}

// seedStartedOperation inserts a running apply_operations row for the apply so
// claim-policy tests can exercise FindNextApplyOperation against work that
// looks like a drive already started it.
func seedStartedOperation(t *testing.T, stor storage.Storage, applyID int64, deployment string) int64 {
	t.Helper()

	opID, err := stor.ApplyOperations().Insert(t.Context(), &storage.ApplyOperation{
		ApplyID:    applyID,
		Deployment: deployment,
		Target:     deployment,
	})
	require.NoError(t, err)
	require.NoError(t, stor.ApplyOperations().MarkStarted(t.Context(), opID))
	return opID
}

func TestOperator_ClaimableStates(t *testing.T) {
	fixture := newOperatorClaimFixture(t, "claim_states_")
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
		{name: "reverting", applyState: state.Apply.Reverting, databaseType: "vitess", engine: "planetscale", wantClaim: true},
		{name: "skipping revert", applyState: state.Apply.SkippingRevert, databaseType: "vitess", engine: "planetscale", wantClaim: true},
		{name: "completed", applyState: state.Apply.Completed, databaseType: "mysql", engine: "spirit"},
		{name: "failed", applyState: state.Apply.Failed, databaseType: "mysql", engine: "spirit"},
		{name: "stopped", applyState: state.Apply.Stopped, databaseType: "mysql", engine: "spirit"},
		{name: "reverted", applyState: state.Apply.Reverted, databaseType: "vitess", engine: "planetscale"},
		{name: "preparing branch", applyState: state.Apply.PreparingBranch, databaseType: "vitess", engine: "planetscale", wantClaim: true},
		{name: "applying branch changes", applyState: state.Apply.ApplyingBranchChanges, databaseType: "vitess", engine: "planetscale", wantClaim: true},
		{name: "validating branch", applyState: state.Apply.ValidatingBranch, databaseType: "vitess", engine: "planetscale", wantClaim: true},
		{name: "creating deploy request", applyState: state.Apply.CreatingDeployRequest, databaseType: "vitess", engine: "planetscale", wantClaim: true},
		{name: "validating deploy request", applyState: state.Apply.ValidatingDeployRequest, databaseType: "vitess", engine: "planetscale", wantClaim: true},
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

			// The operation row carries the same state as its parent apply, so
			// the case exercises the claim query's per-state contract at the
			// row the operator actually claims.
			opID, err := stor.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
				ApplyID:    applyID,
				Deployment: appDBName,
				Target:     appDBName,
			})
			require.NoError(t, err)
			if tc.applyState != state.Apply.Pending {
				require.NoError(t, stor.ApplyOperations().UpdateState(ctx, opID, tc.applyState))
			}
			_, err = schemabotDB.ExecContext(ctx,
				"UPDATE apply_operations SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?",
				opID)
			require.NoError(t, err)

			// The operator should claim only queued or stale operations in states it can resume safely.
			claimed, err := stor.ApplyOperations().FindNextApplyOperation(ctx, "test-owner")
			require.NoError(t, err)
			if tc.wantClaim {
				require.NotNil(t, claimed)
				assert.Equal(t, opID, claimed.ID)
			} else {
				assert.Nil(t, claimed)
			}
		})
	}
}

func TestOperator_ClaimRefreshesHeartbeat(t *testing.T) {
	ctx := t.Context()

	fixture := newOperatorClaimFixture(t, "claim_heartbeat_")
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
	opID := seedStartedOperation(t, stor, applyID, appDBName)
	_, err = schemabotDB.ExecContext(ctx, "UPDATE apply_operations SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?", opID)
	require.NoError(t, err)

	beforeClaim, err := stor.ApplyOperations().Get(ctx, opID)
	require.NoError(t, err)
	require.NotNil(t, beforeClaim)

	// Claiming is also the operator's lease renewal; it keeps another driver from immediately reclaiming the same operation.
	claimed, err := stor.ApplyOperations().FindNextApplyOperation(ctx, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, opID, claimed.ID)

	afterClaim, err := stor.ApplyOperations().Get(ctx, opID)
	require.NoError(t, err)
	require.NotNil(t, afterClaim)
	assert.True(t, afterClaim.UpdatedAt.After(beforeClaim.UpdatedAt), "claim should refresh the operation heartbeat")

	reclaimed, err := stor.ApplyOperations().FindNextApplyOperation(ctx, "test-owner")
	require.NoError(t, err)
	assert.Nil(t, reclaimed, "freshly claimed operation should not be claimable again")
}

func TestOperator_ExpiresRetryableBudget(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	fixture := newOperatorClaimFixture(t, "retry_budget_")
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

	svc := schemabotapi.New(stor, &schemabotapi.ServerConfig{Drivers: 1}, nil, logger)
	require.NoError(t, svc.SetOperatorPollInterval(50*time.Millisecond))
	svc.StartOperator(ctx)
	defer svc.StopOperator()

	// The operator should convert retry-waiting work to permanent failure once
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

func TestOperator_MultipleWorkersResumeDifferentTargets(t *testing.T) {
	ctx := t.Context()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	db1Name, db1DSN := createTestDB(t, "multi_worker_a_")
	db2Name, db2DSN := createTestDB(t, "multi_worker_b_")

	schemabotDB, err := sql.Open("block-mysql", schemabotDSN)
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

	plan1 := planCreateTableForOperator(t, client1, stor, db1Name, "operator_worker_a")
	plan2 := planCreateTableForOperator(t, client2, stor, db2Name, "operator_worker_b")
	apply1ID := seedStaleOperatorApply(t, stor, schemabotDB, db1Name, plan1, time.Now().Add(-3*time.Minute))
	apply2ID := seedStaleOperatorApply(t, stor, schemabotDB, db2Name, plan2, time.Now().Add(-2*time.Minute))

	blockedResume := make(chan struct{})
	var releaseBlockedResume sync.Once
	releaseBlockedClient := func() {
		releaseBlockedResume.Do(func() {
			close(blockedResume)
		})
	}

	// The first client blocks after the operator claims its apply. That keeps
	// one driver occupied across the next poll, so completion of the second
	// apply proves another driver can claim independent work.
	blockingClient1 := newBlockingResumeClient(client1, blockedResume)

	svc := schemabotapi.New(stor, &schemabotapi.ServerConfig{
		Drivers: 2,
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

	operatorPollInterval := 500 * time.Millisecond
	require.NoError(t, svc.SetOperatorPollInterval(operatorPollInterval))

	svc.StartOperator(ctx)
	defer func() {
		releaseBlockedClient()
		svc.StopOperator()
	}()

	blockingClient1.waitForResume(t, 5*time.Second)

	// A driver can miss work on the startup claim and pick it up on the next
	// poll. The important behavior is that the second apply completes while the
	// first driver is still blocked.
	waitForOperatorAppliesCompleted(t, stor, []int64{apply2ID}, operatorPollInterval+5*time.Second)

	blockedApply, err := stor.Applies().Get(ctx, apply1ID)
	require.NoError(t, err)
	require.NotNil(t, blockedApply)
	assert.Equal(t, state.Apply.Running, blockedApply.State)

	releaseBlockedClient()
	waitForOperatorAppliesCompleted(t, stor, []int64{apply1ID}, 5*time.Second)
}

func planCreateTableForOperator(t *testing.T, client tern.Client, stor storage.Storage, dbName, tableName string) *storage.Plan {
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

func seedStaleOperatorApply(
	t *testing.T,
	stor storage.Storage,
	db *sql.DB,
	dbName string,
	plan *storage.Plan,
	createdAt time.Time,
) int64 {
	t.Helper()

	now := time.Now()
	applyID, err := stor.Applies().Create(t.Context(), &storage.Apply{
		ApplyIdentifier: fmt.Sprintf("apply-multi-driver-%s", dbName),
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

	opID := seedStartedOperation(t, stor, applyID, dbName)

	for _, tc := range plan.FlatDDLChanges() {
		_, err := stor.Tasks().Create(t.Context(), &storage.Task{
			TaskIdentifier:   fmt.Sprintf("task-multi-driver-%s-%s", dbName, tc.Table),
			ApplyID:          applyID,
			ApplyOperationID: &opID,
			PlanID:           plan.ID,
			Database:         dbName,
			DatabaseType:     storage.DatabaseTypeMySQL,
			Engine:           "spirit",
			State:            state.Task.Running,
			TableName:        tc.Table,
			DDL:              tc.DDL,
			DDLAction:        tc.Operation,
			Options:          []byte("{}"),
			Environment:      "staging",
			CreatedAt:        now,
			UpdatedAt:        now,
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
	_, err = db.ExecContext(
		t.Context(),
		"UPDATE apply_operations SET created_at = ?, updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?",
		createdAt,
		opID,
	)
	require.NoError(t, err)

	return applyID
}

func waitForOperatorAppliesCompleted(t *testing.T, stor storage.Storage, applyIDs []int64, timeout time.Duration) {
	t.Helper()

	completed := make(map[int64]bool, len(applyIDs))
	testutil.Poll(t, timeout, 100*time.Millisecond,
		func() bool {
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
			return len(completed) == len(applyIDs)
		},
		func() string {
			states := make(map[int64]string, len(applyIDs))
			for _, applyID := range applyIDs {
				apply, err := stor.Applies().Get(t.Context(), applyID)
				require.NoError(t, err)
				if apply != nil {
					states[applyID] = apply.State
				}
			}
			return fmt.Sprintf("operator did not complete all applies within %s; states: %v", timeout, states)
		},
	)
}

func TestOperator_DatabaseExclusionScopedByEnvironment(t *testing.T) {
	ctx := t.Context()

	fixture := newOperatorClaimFixture(t, "env_excl_")
	appDBName := fixture.appDBName
	stor := fixture.store
	schemabotDB := fixture.storageDB

	now := time.Now()
	stagingID, err := stor.Applies().Create(ctx, &storage.Apply{
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
	seedStartedOperation(t, stor, stagingID, appDBName)

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
	productionOpID := seedStartedOperation(t, stor, productionID, appDBName)
	_, err = schemabotDB.ExecContext(ctx, "UPDATE apply_operations SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE id = ?", productionOpID)
	require.NoError(t, err)

	// The operator should allow a stale operation when the active apply on the
	// same database belongs to another environment.
	claimed, err := stor.ApplyOperations().FindNextApplyOperation(ctx, "test-owner")
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, productionID, claimed.ApplyID)
}

// A pending operation row left under an apply that has already settled will
// never be claimed: the parent's verdict is recorded and no driver revisits it.
// The stranded-operation reaper settles the row to the parent's outcome so
// operators reading pending work see only work that can still run, and it does
// this without rewriting the parent apply, whose recorded verdict and timestamps
// stay exactly as the drive that settled it left them.
func TestOperator_ReaperSettlesStrandedOperationsUnderSettledApplies(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	fixture := newOperatorClaimFixture(t, "stranded_ops_")
	appDBName := fixture.appDBName
	stor := fixture.store

	now := time.Now()
	applyID, err := stor.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-stranded-operation",
		Database:        appDBName,
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      appDBName,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Completed,
		Options:         []byte("{}"),
		Environment:     "staging",
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	require.NoError(t, err)

	operationID, err := stor.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
		ApplyID:    applyID,
		Deployment: appDBName,
		Target:     appDBName,
	})
	require.NoError(t, err)

	// Age the parent past the quiescence bound so its rows read as stranded
	// rather than as the tail of a drive that is still writing.
	_, err = fixture.storageDB.ExecContext(ctx,
		"UPDATE applies SET updated_at = NOW() - INTERVAL 1 HOUR WHERE id = ?", applyID)
	require.NoError(t, err)

	before, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, before)

	// The claim ladder must not touch the row itself: a pending operation under
	// a terminal parent is gated off FindNextApplyOperation, so only the reaper
	// can settle it. Running the full operator alongside the reaper proves the
	// two do not fight over the stranded row.
	svc := schemabotapi.New(stor, &schemabotapi.ServerConfig{
		Drivers: 1,
	}, nil, logger)
	require.NoError(t, svc.SetOperatorPollInterval(50*time.Millisecond))
	require.NoError(t, svc.SetStrandedReaperInterval(50*time.Millisecond))
	svc.StartOperator(ctx)
	defer svc.StopOperator()

	require.Eventually(t, func() bool {
		op, err := stor.ApplyOperations().Get(ctx, operationID)
		require.NoError(t, err)
		return op != nil && op.State == state.Apply.Completed
	}, 5*time.Second, 100*time.Millisecond, "the reaper should settle the stranded operation to its parent's outcome")

	op, err := stor.ApplyOperations().Get(ctx, operationID)
	require.NoError(t, err)
	assert.NotNil(t, op.CompletedAt, "a settled operation carries a completion time")

	after, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, after)
	assert.Equal(t, before.State, after.State, "the reaper records nothing on the parent apply")
	assert.True(t, before.UpdatedAt.Equal(after.UpdatedAt), "the parent apply row is not rewritten: %s != %s", before.UpdatedAt, after.UpdatedAt)
}
