package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/auth"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

func TestExecuteReapplyReappliesFailedApplyWhenPlanStillMatches(t *testing.T) {
	sourcePlan := reapplyTestPlan("plan-source", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	apply := reapplyTestApply("apply-reapply", sourcePlan)
	plans := newReapplyPlanStore(sourcePlan)
	applies := &reapplyApplyStore{apply: apply}
	client := &mockTernClient{planResp: reapplyPlanResponse("plan-fresh", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")}
	locks := newReapplyLockStore()
	svc := newReapplyTestService(t, client, plans, applies, locks, reapplyTestTask(apply, "users", state.Task.Failed))

	resp, err := svc.ExecuteReapply(t.Context(), apitypes.ControlRequest{
		Environment: "staging",
		ApplyID:     apply.ApplyIdentifier,
		Caller:      "operator@example.com",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Accepted)
	assert.Equal(t, apitypes.ReapplyStatusReapplied, resp.Status)
	assert.Equal(t, apply.ApplyIdentifier, resp.ApplyID)
	assert.Equal(t, sourcePlan.PlanIdentifier, resp.PlanID)
	assert.Equal(t, apply.ID, applies.reappliedApplyID)
	assert.Equal(t, locks.lock.ID, applies.reapplyLockID)
	assert.Equal(t, "operator@example.com", locks.lock.Owner)
	assert.Equal(t, sourcePlan.Database, client.planReq.Database)
	assert.Equal(t, sourcePlan.Environment, client.planReq.Environment)
}

// A reapply is attributed to the authenticated caller when the request carries
// a real identity, so the apply log records the verified user rather than the
// client-supplied caller string.
func TestExecuteReapplyAttributesAuthenticatedCaller(t *testing.T) {
	sourcePlan := reapplyTestPlan("plan-source", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	apply := reapplyTestApply("apply-reapply", sourcePlan)
	plans := newReapplyPlanStore(sourcePlan)
	applies := &reapplyApplyStore{apply: apply}
	client := &mockTernClient{planResp: reapplyPlanResponse("plan-fresh", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")}
	logs := &capturingApplyLogStore{}
	locks := newReapplyLockStore()
	svc := newReapplyTestService(t, client, plans, applies, locks, reapplyTestTask(apply, "users", state.Task.Failed), storage.ApplyLogStore(logs))

	ctx := auth.WithUser(t.Context(), &auth.User{Subject: "bob@example.com"})
	resp, err := svc.ExecuteReapply(ctx, apitypes.ControlRequest{
		Environment: "staging",
		ApplyID:     apply.ApplyIdentifier,
		Caller:      "client-supplied",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Accepted)
	assert.True(t, hasApplyLogMessageContaining(logs.logs, "Reapply requested by user (caller: bob@example.com)"),
		"the apply log must attribute the reapply to the authenticated caller")
	require.NotNil(t, locks.lock)
	assert.Equal(t, "bob@example.com", locks.lock.Owner,
		"the reapply lock must be owned by the authenticated caller, not the client-supplied string")
}

func TestExecuteReapplyRejectsChangedPlanWithoutReapplying(t *testing.T) {
	sourcePlan := reapplyTestPlan("plan-source", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	apply := reapplyTestApply("apply-reapply", sourcePlan)
	plans := newReapplyPlanStore(sourcePlan)
	applies := &reapplyApplyStore{apply: apply}
	client := &mockTernClient{planResp: reapplyPlanResponse("plan-fresh", "ALTER TABLE `users` ADD COLUMN `phone` varchar(255)")}
	locks := newReapplyLockStore()
	svc := newReapplyTestService(t, client, plans, applies, locks, reapplyTestTask(apply, "users", state.Task.Failed))

	resp, err := svc.ExecuteReapply(t.Context(), apitypes.ControlRequest{
		Environment: "staging",
		ApplyID:     apply.ApplyIdentifier,
		Caller:      "operator@example.com",
	})
	require.ErrorIs(t, err, storage.ErrApplyNotReappliable)
	assert.Nil(t, resp)
	assert.Zero(t, applies.reappliedApplyID)
	assert.Nil(t, locks.lock)
}

func TestExecuteReapplyAcceptsRemainingWorkAfterCompletedTask(t *testing.T) {
	sourcePlan := reapplyTestPlanWithChanges("plan-source", []storage.TableChange{
		{Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", Operation: "alter"},
		{Table: "orders", DDL: "ALTER TABLE `orders` ADD COLUMN `status` varchar(32)", Operation: "alter"},
	})
	apply := reapplyTestApply("apply-reapply", sourcePlan)
	plans := newReapplyPlanStore(sourcePlan)
	applies := &reapplyApplyStore{apply: apply}
	client := &mockTernClient{planResp: reapplyPlanResponseForChanges("plan-fresh", []*ternv1.TableChange{{
		TableName:  "orders",
		Ddl:        "ALTER TABLE `orders` ADD COLUMN `status` varchar(32)",
		ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
	}})}
	svc := newReapplyTestService(t, client, plans, applies, newReapplyLockStore(),
		reapplyTestTask(apply, "users", state.Task.Completed),
		reapplyTestTask(apply, "orders", state.Task.Failed),
	)

	resp, err := svc.ExecuteReapply(t.Context(), apitypes.ControlRequest{
		Environment: "staging",
		ApplyID:     apply.ApplyIdentifier,
		Caller:      "operator@example.com",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Accepted)
	assert.Equal(t, apply.ID, applies.reappliedApplyID)
}

func TestExecuteReapplyRejectsFailedWorkOutsidePrimaryDeployment(t *testing.T) {
	sourcePlan := reapplyTestPlan("plan-source", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	apply := reapplyTestApply("apply-reapply", sourcePlan)
	plans := newReapplyPlanStore(sourcePlan)
	applies := &reapplyApplyStore{apply: apply}
	client := &mockTernClient{planResp: reapplyPlanResponse("plan-fresh", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")}
	completedOpID := int64(101)
	failedOpID := int64(102)
	svc := newReapplyTestService(t, client, plans, applies, newReapplyLockStore(),
		reapplyTestOperation(completedOpID, apply.ID, sourcePlan.Deployment, state.ApplyOperation.Completed),
		reapplyTestOperation(failedOpID, apply.ID, "secondary", state.ApplyOperation.Failed),
		reapplyTestOperationTask(apply, completedOpID, "users", state.Task.Completed),
		reapplyTestOperationTask(apply, failedOpID, "users", state.Task.Failed),
	)

	resp, err := svc.ExecuteReapply(t.Context(), apitypes.ControlRequest{
		Environment: "staging",
		ApplyID:     apply.ApplyIdentifier,
		Caller:      "operator@example.com",
	})
	require.ErrorIs(t, err, storage.ErrApplyNotReappliable)
	assert.Nil(t, resp)
	assert.Zero(t, applies.reappliedApplyID)
}

// A failure older than the reapply freshness window is rejected before any
// re-plan runs: the world has had too long to move, so the operator must make
// an explicit new apply decision instead of reviving stale work.
func TestExecuteReapplyRejectsStaleFailure(t *testing.T) {
	sourcePlan := reapplyTestPlan("plan-source", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	apply := reapplyTestApply("apply-reapply", sourcePlan)
	staleCompletedAt := time.Now().AddDate(0, 0, -(storage.ReapplyFailureFreshnessDays + 1))
	apply.CompletedAt = &staleCompletedAt
	plans := newReapplyPlanStore(sourcePlan)
	applies := &reapplyApplyStore{apply: apply}
	client := &mockTernClient{planResp: reapplyPlanResponse("plan-fresh", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")}
	svc := newReapplyTestService(t, client, plans, applies, newReapplyLockStore(), reapplyTestTask(apply, "users", state.Task.Failed))

	resp, err := svc.ExecuteReapply(t.Context(), apitypes.ControlRequest{
		Environment: "staging",
		ApplyID:     apply.ApplyIdentifier,
		Caller:      "operator@example.com",
	})
	require.ErrorIs(t, err, storage.ErrApplyNotReappliable)
	assert.Contains(t, err.Error(), "failed more than")
	assert.Nil(t, resp)
	assert.Zero(t, applies.reappliedApplyID)
	assert.Nil(t, client.planReq, "a stale failure must be rejected before any re-plan runs")
}

// A reapply request must name the environment the apply belongs to; a mismatch
// is rejected before any state or plan work so a caller cannot act on an apply
// in an environment they did not intend.
func TestExecuteReapplyRejectsEnvironmentMismatch(t *testing.T) {
	sourcePlan := reapplyTestPlan("plan-source", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	apply := reapplyTestApply("apply-reapply", sourcePlan)
	plans := newReapplyPlanStore(sourcePlan)
	applies := &reapplyApplyStore{apply: apply}
	client := &mockTernClient{planResp: reapplyPlanResponse("plan-fresh", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")}
	svc := newReapplyTestService(t, client, plans, applies, newReapplyLockStore(), reapplyTestTask(apply, "users", state.Task.Failed))

	resp, err := svc.ExecuteReapply(t.Context(), apitypes.ControlRequest{
		Environment: "production",
		ApplyID:     apply.ApplyIdentifier,
		Caller:      "operator@example.com",
	})
	require.Error(t, err)
	assert.Equal(t, http.StatusBadRequest, reapplyHTTPStatus(err))
	assert.Contains(t, err.Error(), `belongs to environment "staging", not "production"`)
	assert.Nil(t, resp)
	assert.Zero(t, applies.reappliedApplyID)
	assert.Nil(t, client.planReq, "an environment mismatch must be rejected before any re-plan runs")
}

func TestExecuteReapplyRejectsCancelledApply(t *testing.T) {
	sourcePlan := reapplyTestPlan("plan-source", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	apply := reapplyTestApply("apply-reapply", sourcePlan)
	apply.State = state.Apply.Cancelled
	plans := newReapplyPlanStore(sourcePlan)
	applies := &reapplyApplyStore{apply: apply}
	client := &mockTernClient{planResp: reapplyPlanResponse("plan-fresh", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")}
	svc := newReapplyTestService(t, client, plans, applies, newReapplyLockStore())

	resp, err := svc.ExecuteReapply(t.Context(), apitypes.ControlRequest{
		Environment: "staging",
		ApplyID:     apply.ApplyIdentifier,
		Caller:      "operator@example.com",
	})
	require.ErrorIs(t, err, storage.ErrApplyNotReappliable)
	assert.Nil(t, resp)
	assert.Zero(t, applies.reappliedApplyID)
	assert.Nil(t, client.planReq)
}

func TestExecuteReapplyRejectsLockHeldByAnotherOwner(t *testing.T) {
	sourcePlan := reapplyTestPlan("plan-source", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	apply := reapplyTestApply("apply-reapply", sourcePlan)
	plans := newReapplyPlanStore(sourcePlan)
	applies := &reapplyApplyStore{apply: apply}
	client := &mockTernClient{planResp: reapplyPlanResponse("plan-fresh", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")}
	locks := newReapplyLockStore()
	locks.lock = &storage.Lock{ID: 99, DatabaseName: sourcePlan.Database, DatabaseType: sourcePlan.DatabaseType, Owner: "other-owner"}
	svc := newReapplyTestService(t, client, plans, applies, locks, reapplyTestTask(apply, "users", state.Task.Failed))

	resp, err := svc.ExecuteReapply(t.Context(), apitypes.ControlRequest{
		Environment: "staging",
		ApplyID:     apply.ApplyIdentifier,
		Caller:      "operator@example.com",
	})
	require.ErrorIs(t, err, storage.ErrLockHeld)
	assert.Nil(t, resp)
	assert.Zero(t, applies.reappliedApplyID)
}

func TestExecuteReapplyForceTakesOverLockHeldByAnotherOwner(t *testing.T) {
	sourcePlan := reapplyTestPlan("plan-source", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	apply := reapplyTestApply("apply-reapply", sourcePlan)
	plans := newReapplyPlanStore(sourcePlan)
	applies := &reapplyApplyStore{apply: apply}
	client := &mockTernClient{planResp: reapplyPlanResponse("plan-fresh", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")}
	locks := newReapplyLockStore()
	locks.lock = &storage.Lock{ID: 99, DatabaseName: sourcePlan.Database, DatabaseType: sourcePlan.DatabaseType, Owner: "other-owner"}
	svc := newReapplyTestService(t, client, plans, applies, locks, reapplyTestTask(apply, "users", state.Task.Failed))

	resp, err := svc.ExecuteReapply(t.Context(), apitypes.ControlRequest{
		Environment: "staging",
		ApplyID:     apply.ApplyIdentifier,
		Caller:      "operator@example.com",
		Force:       true,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Accepted)
	assert.Equal(t, apply.ID, applies.reappliedApplyID)
	assert.Equal(t, "operator@example.com", locks.lock.Owner)
	assert.NotEqual(t, int64(99), applies.reapplyLockID)
}

func TestFilterPlanToReappliableWorkDropsEmptyNamespaces(t *testing.T) {
	sourcePlan := &storage.Plan{
		Database:     "testdb",
		DatabaseType: storage.DatabaseTypeMySQL,
		Deployment:   DefaultDeployment,
		Target:       "testdb",
		Environment:  "staging",
		Namespaces: map[string]*storage.NamespacePlanData{
			"completed_ns": {Tables: []storage.TableChange{{Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", Operation: "alter"}}},
			"failed_ns":    {Tables: []storage.TableChange{{Table: "orders", DDL: "ALTER TABLE `orders` ADD COLUMN `status` varchar(32)", Operation: "alter"}}},
		},
	}
	freshPlan := &storage.Plan{
		Database:     sourcePlan.Database,
		DatabaseType: sourcePlan.DatabaseType,
		Deployment:   sourcePlan.Deployment,
		Target:       sourcePlan.Target,
		Environment:  sourcePlan.Environment,
		Namespaces: map[string]*storage.NamespacePlanData{
			"failed_ns": {Tables: []storage.TableChange{{Table: "orders", DDL: "ALTER TABLE `orders` ADD COLUMN `status` varchar(32)", Operation: "alter"}}},
		},
	}

	failedTaskKey := reapplyTaskKey{Namespace: "failed_ns", Table: "orders"}
	filtered := filterPlanToReappliableWork(sourcePlan, reapplyWorkSelection{taskKeys: map[reapplyTaskKey]bool{failedTaskKey: true}})

	assert.NotContains(t, filtered.Namespaces, "completed_ns")
	equivalent, err := plansEquivalentForReapply(filtered, freshPlan)
	require.NoError(t, err)
	assert.True(t, equivalent)
}

// A failed task pointing at an operation row that no longer exists is a storage
// inconsistency (apply_operation_id is not a foreign key). Reapply fails closed
// and surfaces it rather than silently dropping the task, which could requeue a
// partial reapply or report no failed work while the task stays failed.
func TestExecuteReapplyRejectsTaskWithMissingOperationRow(t *testing.T) {
	sourcePlan := reapplyTestPlan("plan-source", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	apply := reapplyTestApply("apply-reapply", sourcePlan)
	plans := newReapplyPlanStore(sourcePlan)
	applies := &reapplyApplyStore{apply: apply}
	client := &mockTernClient{planResp: reapplyPlanResponse("plan-fresh", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")}
	// The failed task references operation 999, which is never registered.
	svc := newReapplyTestService(t, client, plans, applies, newReapplyLockStore(),
		reapplyTestOperationTask(apply, 999, "users", state.Task.Failed),
	)

	resp, err := svc.ExecuteReapply(t.Context(), apitypes.ControlRequest{
		Environment: "staging",
		ApplyID:     apply.ApplyIdentifier,
		Caller:      "operator@example.com",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing operation row")
	assert.Nil(t, resp)
	assert.Zero(t, applies.reappliedApplyID)
}

// Artifacts that are nil in one plan and an empty map in the other describe the
// same (no) work; reapply must treat the plans as equivalent rather than reject
// on a nil-vs-{} JSON difference.
func TestPlansEquivalentForReapplyTreatsNilAndEmptyArtifactsEqual(t *testing.T) {
	planWith := func(artifacts map[string]string) *storage.Plan {
		return &storage.Plan{
			Database:     "testdb",
			DatabaseType: storage.DatabaseTypeMySQL,
			Deployment:   DefaultDeployment,
			Target:       "testdb",
			Environment:  "staging",
			Namespaces: map[string]*storage.NamespacePlanData{
				"testdb": {
					Tables:    []storage.TableChange{{Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", Operation: "alter"}},
					Artifacts: artifacts,
				},
			},
		}
	}

	equivalent, err := plansEquivalentForReapply(planWith(nil), planWith(map[string]string{}))
	require.NoError(t, err)
	assert.True(t, equivalent)
}

func TestFilterPlanToReappliableWorkKeepsNamespaceTableForShardTask(t *testing.T) {
	tableChange := storage.TableChange{Table: "orders", DDL: "ALTER TABLE `orders` ADD COLUMN `status` varchar(32)", Operation: "alter"}
	sourcePlan := &storage.Plan{
		Database:     "testdb",
		DatabaseType: storage.DatabaseTypeStrata,
		Deployment:   DefaultDeployment,
		Target:       "testdb",
		Environment:  "staging",
		Namespaces: map[string]*storage.NamespacePlanData{
			"commerce": {Tables: []storage.TableChange{tableChange}},
		},
		Shards: []storage.ShardPlan{{
			Namespace: "commerce",
			Shard:     "-80",
			Changes:   []storage.TableChange{tableChange},
		}},
	}
	freshPlan := &storage.Plan{
		Database:     sourcePlan.Database,
		DatabaseType: sourcePlan.DatabaseType,
		Deployment:   sourcePlan.Deployment,
		Target:       sourcePlan.Target,
		Environment:  sourcePlan.Environment,
		Namespaces: map[string]*storage.NamespacePlanData{
			"commerce": {Tables: []storage.TableChange{tableChange}},
		},
		Shards: []storage.ShardPlan{{
			Namespace: "commerce",
			Shard:     "-80",
			Changes:   []storage.TableChange{tableChange},
		}},
	}

	failedShardTaskKey := reapplyTaskKey{Namespace: "commerce", Shard: "-80", Table: "orders"}
	filtered := filterPlanToReappliableWork(sourcePlan, reapplyWorkSelection{taskKeys: map[reapplyTaskKey]bool{failedShardTaskKey: true}})

	require.Contains(t, filtered.Namespaces, "commerce")
	assert.Equal(t, []storage.TableChange{tableChange}, filtered.Namespaces["commerce"].Tables)
	equivalent, err := plansEquivalentForReapply(filtered, freshPlan)
	require.NoError(t, err)
	assert.True(t, equivalent)
}

func TestPlansEquivalentForReapplyNormalizesEmptyNamespacesAndArtifacts(t *testing.T) {
	left := &storage.Plan{
		Database:     "testdb",
		DatabaseType: storage.DatabaseTypeMySQL,
		Deployment:   DefaultDeployment,
		Target:       "testdb",
		Environment:  "staging",
		Namespaces: map[string]*storage.NamespacePlanData{
			"empty":   {},
			"routing": {Artifacts: map[string]string{}},
		},
	}
	right := &storage.Plan{
		Database:     left.Database,
		DatabaseType: left.DatabaseType,
		Deployment:   left.Deployment,
		Target:       left.Target,
		Environment:  left.Environment,
		Namespaces:   map[string]*storage.NamespacePlanData{},
	}

	equivalent, err := plansEquivalentForReapply(left, right)
	require.NoError(t, err)
	assert.True(t, equivalent)
}

func newReapplyTestService(t *testing.T, client tern.Client, plans storage.PlanStore, applies storage.ApplyStore, locks storage.LockStore, rows ...any) *Service {
	t.Helper()
	var tasks []*storage.Task
	var operations []*storage.ApplyOperation
	logStore := storage.ApplyLogStore(&noopApplyLogStore{})
	for _, row := range rows {
		switch v := row.(type) {
		case *storage.Task:
			tasks = append(tasks, v)
		case *storage.ApplyOperation:
			operations = append(operations, v)
		case storage.ApplyLogStore:
			logStore = v
		default:
			require.Failf(t, "unknown reapply test row", "%T", row)
		}
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorageWithApplyStores{
		plans:      plans,
		applies:    applies,
		locks:      locks,
		tasks:      &capturingTaskStore{tasks: tasks},
		applyLogs:  logStore,
		operations: &staticApplyOperationStore{operations: operations},
	}, testServerConfig(), map[string]tern.Client{
		"default/staging": client,
	}, logger)
}

func reapplyTestPlan(planID, ddl string) *storage.Plan {
	return reapplyTestPlanWithChanges(planID, []storage.TableChange{{Table: "users", DDL: ddl, Operation: "alter"}})
}

func reapplyTestPlanWithChanges(planID string, changes []storage.TableChange) *storage.Plan {
	return &storage.Plan{
		ID:             42,
		PlanIdentifier: planID,
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     DefaultDeployment,
		Target:         "testdb",
		Repository:     "owner/repo",
		PullRequest:    123,
		SchemaPath:     "schema",
		Environment:    "staging",
		SchemaFiles: schema.SchemaFiles{
			"testdb": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}},
		},
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {Tables: changes},
		},
		HeadSHA:   "abc123",
		CreatedAt: time.Now(),
	}
}

func reapplyTestApply(applyID string, plan *storage.Plan) *storage.Apply {
	completedAt := time.Now()
	return &storage.Apply{
		ID:              7,
		ApplyIdentifier: applyID,
		PlanID:          plan.ID,
		Database:        plan.Database,
		DatabaseType:    plan.DatabaseType,
		Deployment:      plan.Deployment,
		Environment:     plan.Environment,
		State:           state.Apply.Failed,
		CompletedAt:     &completedAt,
	}
}

func reapplyPlanResponse(planID, ddl string) *ternv1.PlanResponse {
	return reapplyPlanResponseForChanges(planID, []*ternv1.TableChange{{
		TableName:  "users",
		Ddl:        ddl,
		ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
	}})
}

func reapplyPlanResponseForChanges(planID string, changes []*ternv1.TableChange) *ternv1.PlanResponse {
	return &ternv1.PlanResponse{
		PlanId: planID,
		Changes: []*ternv1.SchemaChange{{
			Namespace:    "testdb",
			TableChanges: changes,
		}},
	}
}

func reapplyTestTask(apply *storage.Apply, tableName, taskState string) *storage.Task {
	completedAt := time.Now()
	task := &storage.Task{
		TaskIdentifier: "task_" + tableName,
		ApplyID:        apply.ID,
		PlanID:         apply.PlanID,
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Environment:    apply.Environment,
		State:          taskState,
		Namespace:      "testdb",
		TableName:      tableName,
	}
	if state.IsState(taskState, state.Task.Completed) {
		task.CompletedAt = &completedAt
	}
	return task
}

func reapplyTestOperation(id, applyID int64, deployment, operationState string) *storage.ApplyOperation {
	return &storage.ApplyOperation{
		ID:         id,
		ApplyID:    applyID,
		Deployment: deployment,
		State:      operationState,
	}
}

func reapplyTestOperationTask(apply *storage.Apply, operationID int64, tableName, taskState string) *storage.Task {
	task := reapplyTestTask(apply, tableName, taskState)
	task.ApplyOperationID = &operationID
	return task
}

func (s *capturingTaskStore) GetByApplyOperationID(_ context.Context, applyOperationID int64) ([]*storage.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var tasks []*storage.Task
	for _, task := range s.tasks {
		if task.ApplyOperationID != nil && *task.ApplyOperationID == applyOperationID {
			tasks = append(tasks, task)
		}
	}
	return tasks, s.err
}

type reapplyPlanStore struct {
	storage.PlanStore
	plansByID         map[int64]*storage.Plan
	plansByIdentifier map[string]*storage.Plan
	nextID            int64
}

func newReapplyPlanStore(source *storage.Plan) *reapplyPlanStore {
	return &reapplyPlanStore{
		plansByID:         map[int64]*storage.Plan{source.ID: source},
		plansByIdentifier: map[string]*storage.Plan{source.PlanIdentifier: source},
		nextID:            source.ID + 1,
	}
}

func (s *reapplyPlanStore) Create(_ context.Context, plan *storage.Plan) (int64, error) {
	if _, exists := s.plansByIdentifier[plan.PlanIdentifier]; exists {
		return 0, storage.ErrPlanIDExists
	}
	stored := *plan
	stored.ID = s.nextID
	s.nextID++
	s.plansByID[stored.ID] = &stored
	s.plansByIdentifier[stored.PlanIdentifier] = &stored
	return stored.ID, nil
}

func (s *reapplyPlanStore) Get(_ context.Context, planIdentifier string) (*storage.Plan, error) {
	return s.plansByIdentifier[planIdentifier], nil
}

func (s *reapplyPlanStore) GetByID(_ context.Context, id int64) (*storage.Plan, error) {
	return s.plansByID[id], nil
}

type reapplyApplyStore struct {
	storage.ApplyStore
	apply            *storage.Apply
	reappliedApplyID int64
	reapplyLockID    int64
}

func (s *reapplyApplyStore) GetByApplyIdentifier(_ context.Context, applyIdentifier string) (*storage.Apply, error) {
	if s.apply != nil && s.apply.ApplyIdentifier == applyIdentifier {
		return s.apply, nil
	}
	return nil, nil
}

func (s *reapplyApplyStore) ReapplyFailed(_ context.Context, applyID int64, lockID int64) (*storage.Apply, error) {
	if s.apply == nil || s.apply.ID != applyID {
		return nil, storage.ErrApplyNotFound
	}
	if !state.IsState(s.apply.State, state.Apply.Failed) {
		return nil, fmt.Errorf("apply %s is %s: %w", s.apply.ApplyIdentifier, s.apply.State, storage.ErrApplyNotReappliable)
	}
	s.reappliedApplyID = applyID
	s.reapplyLockID = lockID
	reapplied := *s.apply
	reapplied.LockID = lockID
	reapplied.State = state.Apply.FailedRetryable
	reapplied.CompletedAt = nil
	return &reapplied, nil
}

type reapplyLockStore struct {
	storage.LockStore
	lock   *storage.Lock
	nextID int64
}

func newReapplyLockStore() *reapplyLockStore {
	return &reapplyLockStore{nextID: 1}
}

func (s *reapplyLockStore) Acquire(_ context.Context, lock *storage.Lock) error {
	if s.lock != nil {
		if s.lock.Owner != lock.Owner {
			return storage.ErrLockHeld
		}
		return nil
	}
	stored := *lock
	stored.ID = s.nextID
	s.nextID++
	s.lock = &stored
	return nil
}

func (s *reapplyLockStore) Get(_ context.Context, database, dbType string) (*storage.Lock, error) {
	if s.lock != nil && s.lock.DatabaseName == database && s.lock.DatabaseType == dbType {
		return s.lock, nil
	}
	return nil, nil
}

func (s *reapplyLockStore) ForceRelease(_ context.Context, database, dbType string) error {
	if s.lock == nil || s.lock.DatabaseName != database || s.lock.DatabaseType != dbType {
		return storage.ErrLockNotFound
	}
	s.lock = nil
	return nil
}

func (s *reapplyLockStore) Release(_ context.Context, database, dbType, owner string) error {
	if s.lock == nil || s.lock.DatabaseName != database || s.lock.DatabaseType != dbType {
		return storage.ErrLockNotFound
	}
	if s.lock.Owner != owner {
		return storage.ErrLockNotOwned
	}
	s.lock = nil
	return nil
}
