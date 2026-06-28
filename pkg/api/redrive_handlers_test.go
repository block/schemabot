package api

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

func TestExecuteRedriveRedrivesFailedApplyWhenPlanStillMatches(t *testing.T) {
	sourcePlan := redriveTestPlan("plan-source", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	apply := redriveTestApply("apply-redrive", sourcePlan)
	plans := newRedrivePlanStore(sourcePlan)
	applies := &redriveApplyStore{apply: apply}
	client := &mockTernClient{planResp: redrivePlanResponse("plan-fresh", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")}
	svc := newRedriveTestService(t, client, plans, applies, redriveTestTask(apply, "users", state.Task.Failed))

	resp, err := svc.ExecuteRedrive(t.Context(), apitypes.ControlRequest{
		Environment: "staging",
		ApplyID:     apply.ApplyIdentifier,
		Caller:      "operator@example.com",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Accepted)
	assert.Equal(t, apitypes.RedriveStatusRedriven, resp.Status)
	assert.Equal(t, apply.ApplyIdentifier, resp.ApplyID)
	assert.Equal(t, sourcePlan.PlanIdentifier, resp.PlanID)
	assert.Equal(t, apply.ID, applies.redrivenApplyID)
	assert.Equal(t, sourcePlan.Database, client.planReq.Database)
	assert.Equal(t, sourcePlan.Environment, client.planReq.Environment)
}

func TestExecuteRedriveRejectsChangedPlanWithoutRedriving(t *testing.T) {
	sourcePlan := redriveTestPlan("plan-source", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	apply := redriveTestApply("apply-redrive", sourcePlan)
	plans := newRedrivePlanStore(sourcePlan)
	applies := &redriveApplyStore{apply: apply}
	client := &mockTernClient{planResp: redrivePlanResponse("plan-fresh", "ALTER TABLE `users` ADD COLUMN `phone` varchar(255)")}
	svc := newRedriveTestService(t, client, plans, applies, redriveTestTask(apply, "users", state.Task.Failed))

	resp, err := svc.ExecuteRedrive(t.Context(), apitypes.ControlRequest{
		Environment: "staging",
		ApplyID:     apply.ApplyIdentifier,
		Caller:      "operator@example.com",
	})
	require.ErrorIs(t, err, storage.ErrApplyNotRedrivable)
	assert.Nil(t, resp)
	assert.Zero(t, applies.redrivenApplyID)
}

func TestExecuteRedriveAcceptsRemainingWorkAfterCompletedTask(t *testing.T) {
	sourcePlan := redriveTestPlanWithChanges("plan-source", []storage.TableChange{
		{Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", Operation: "alter"},
		{Table: "orders", DDL: "ALTER TABLE `orders` ADD COLUMN `status` varchar(32)", Operation: "alter"},
	})
	apply := redriveTestApply("apply-redrive", sourcePlan)
	plans := newRedrivePlanStore(sourcePlan)
	applies := &redriveApplyStore{apply: apply}
	client := &mockTernClient{planResp: redrivePlanResponseForChanges("plan-fresh", []*ternv1.TableChange{{
		TableName:  "orders",
		Ddl:        "ALTER TABLE `orders` ADD COLUMN `status` varchar(32)",
		ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
	}})}
	svc := newRedriveTestService(t, client, plans, applies,
		redriveTestTask(apply, "users", state.Task.Completed),
		redriveTestTask(apply, "orders", state.Task.Failed),
	)

	resp, err := svc.ExecuteRedrive(t.Context(), apitypes.ControlRequest{
		Environment: "staging",
		ApplyID:     apply.ApplyIdentifier,
		Caller:      "operator@example.com",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Accepted)
	assert.Equal(t, apply.ID, applies.redrivenApplyID)
}

func TestExecuteRedriveRejectsFailedWorkOutsidePrimaryDeployment(t *testing.T) {
	sourcePlan := redriveTestPlan("plan-source", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	apply := redriveTestApply("apply-redrive", sourcePlan)
	plans := newRedrivePlanStore(sourcePlan)
	applies := &redriveApplyStore{apply: apply}
	client := &mockTernClient{planResp: redrivePlanResponse("plan-fresh", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")}
	completedOpID := int64(101)
	failedOpID := int64(102)
	svc := newRedriveTestService(t, client, plans, applies,
		redriveTestOperation(completedOpID, apply.ID, sourcePlan.Deployment, state.ApplyOperation.Completed),
		redriveTestOperation(failedOpID, apply.ID, "secondary", state.ApplyOperation.Failed),
		redriveTestOperationTask(apply, completedOpID, "users", state.Task.Completed),
		redriveTestOperationTask(apply, failedOpID, "users", state.Task.Failed),
	)

	resp, err := svc.ExecuteRedrive(t.Context(), apitypes.ControlRequest{
		Environment: "staging",
		ApplyID:     apply.ApplyIdentifier,
		Caller:      "operator@example.com",
	})
	require.ErrorIs(t, err, storage.ErrApplyNotRedrivable)
	assert.Nil(t, resp)
	assert.Zero(t, applies.redrivenApplyID)
}

func TestExecuteRedriveRejectsCancelledApply(t *testing.T) {
	sourcePlan := redriveTestPlan("plan-source", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	apply := redriveTestApply("apply-redrive", sourcePlan)
	apply.State = state.Apply.Cancelled
	plans := newRedrivePlanStore(sourcePlan)
	applies := &redriveApplyStore{apply: apply}
	client := &mockTernClient{planResp: redrivePlanResponse("plan-fresh", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")}
	svc := newRedriveTestService(t, client, plans, applies)

	resp, err := svc.ExecuteRedrive(t.Context(), apitypes.ControlRequest{
		Environment: "staging",
		ApplyID:     apply.ApplyIdentifier,
		Caller:      "operator@example.com",
	})
	require.ErrorIs(t, err, storage.ErrApplyNotRedrivable)
	assert.Nil(t, resp)
	assert.Zero(t, applies.redrivenApplyID)
	assert.Nil(t, client.planReq)
}

func TestFilterPlanToRedrivableWorkDropsEmptyNamespaces(t *testing.T) {
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

	failedTaskKey := redriveTaskKey{Namespace: "failed_ns", Table: "orders"}
	filtered := filterPlanToRedrivableWork(sourcePlan, redriveWorkSelection{taskKeys: map[redriveTaskKey]bool{failedTaskKey: true}})

	assert.NotContains(t, filtered.Namespaces, "completed_ns")
	assert.True(t, plansEquivalentForRedrive(filtered, freshPlan))
}

func newRedriveTestService(t *testing.T, client tern.Client, plans storage.PlanStore, applies storage.ApplyStore, rows ...any) *Service {
	t.Helper()
	var tasks []*storage.Task
	var operations []*storage.ApplyOperation
	for _, row := range rows {
		switch v := row.(type) {
		case *storage.Task:
			tasks = append(tasks, v)
		case *storage.ApplyOperation:
			operations = append(operations, v)
		default:
			require.Failf(t, "unknown redrive test row", "%T", row)
		}
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorageWithApplyStores{
		plans:      plans,
		applies:    applies,
		tasks:      &capturingTaskStore{tasks: tasks},
		applyLogs:  &noopApplyLogStore{},
		operations: &staticApplyOperationStore{operations: operations},
	}, testServerConfig(), map[string]tern.Client{
		"default/staging": client,
	}, logger)
}

func redriveTestPlan(planID, ddl string) *storage.Plan {
	return redriveTestPlanWithChanges(planID, []storage.TableChange{{Table: "users", DDL: ddl, Operation: "alter"}})
}

func redriveTestPlanWithChanges(planID string, changes []storage.TableChange) *storage.Plan {
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

func redriveTestApply(applyID string, plan *storage.Plan) *storage.Apply {
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

func redrivePlanResponse(planID, ddl string) *ternv1.PlanResponse {
	return redrivePlanResponseForChanges(planID, []*ternv1.TableChange{{
		TableName:  "users",
		Ddl:        ddl,
		ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
	}})
}

func redrivePlanResponseForChanges(planID string, changes []*ternv1.TableChange) *ternv1.PlanResponse {
	return &ternv1.PlanResponse{
		PlanId: planID,
		Changes: []*ternv1.SchemaChange{{
			Namespace:    "testdb",
			TableChanges: changes,
		}},
	}
}

func redriveTestTask(apply *storage.Apply, tableName, taskState string) *storage.Task {
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

func redriveTestOperation(id, applyID int64, deployment, operationState string) *storage.ApplyOperation {
	return &storage.ApplyOperation{
		ID:         id,
		ApplyID:    applyID,
		Deployment: deployment,
		State:      operationState,
	}
}

func redriveTestOperationTask(apply *storage.Apply, operationID int64, tableName, taskState string) *storage.Task {
	task := redriveTestTask(apply, tableName, taskState)
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

type redrivePlanStore struct {
	storage.PlanStore
	plansByID         map[int64]*storage.Plan
	plansByIdentifier map[string]*storage.Plan
	nextID            int64
}

func newRedrivePlanStore(source *storage.Plan) *redrivePlanStore {
	return &redrivePlanStore{
		plansByID:         map[int64]*storage.Plan{source.ID: source},
		plansByIdentifier: map[string]*storage.Plan{source.PlanIdentifier: source},
		nextID:            source.ID + 1,
	}
}

func (s *redrivePlanStore) Create(_ context.Context, plan *storage.Plan) (int64, error) {
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

func (s *redrivePlanStore) Get(_ context.Context, planIdentifier string) (*storage.Plan, error) {
	return s.plansByIdentifier[planIdentifier], nil
}

func (s *redrivePlanStore) GetByID(_ context.Context, id int64) (*storage.Plan, error) {
	return s.plansByID[id], nil
}

type redriveApplyStore struct {
	storage.ApplyStore
	apply           *storage.Apply
	redrivenApplyID int64
}

func (s *redriveApplyStore) GetByApplyIdentifier(_ context.Context, applyIdentifier string) (*storage.Apply, error) {
	if s.apply != nil && s.apply.ApplyIdentifier == applyIdentifier {
		return s.apply, nil
	}
	return nil, nil
}

func (s *redriveApplyStore) RedriveFailed(_ context.Context, applyID int64) (*storage.Apply, error) {
	if s.apply == nil || s.apply.ID != applyID {
		return nil, storage.ErrApplyNotFound
	}
	if !state.IsState(s.apply.State, state.Apply.Failed) {
		return nil, fmt.Errorf("apply %s is %s: %w", s.apply.ApplyIdentifier, s.apply.State, storage.ErrApplyNotRedrivable)
	}
	s.redrivenApplyID = applyID
	redriven := *s.apply
	redriven.State = state.Apply.FailedRetryable
	redriven.CompletedAt = nil
	return &redriven, nil
}
