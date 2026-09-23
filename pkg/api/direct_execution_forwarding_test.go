package api

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// directExecutionServerConfig is a control plane holding one server-wide
// policy and routing its database to a deployment that runs the statement
// somewhere else.
func directExecutionServerConfig() *ServerConfig {
	return &ServerConfig{
		DirectExecution: &DirectExecutionConfig{Enabled: true, MaxTableRows: 10000, LockAcquisitionTimeout: "5s"},
		Databases: map[string]DatabaseConfig{
			"payments": {
				Type:         storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{"staging": {Target: "payments-staging-target", Deployment: DefaultDeployment}},
			},
		},
		TernDeployments: TernConfig{DefaultDeployment: {"staging": "localhost:9090"}},
	}
}

// The deployment that plans the change is not the one whose configuration
// states the policy, so the policy travels on the request: a statement the
// engine refuses is judged there under this server's bound rather than under
// nothing at all.
func TestExecutePlanStatesTheResolvedDirectExecutionPolicy(t *testing.T) {
	mockClient := &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan-direct"}}
	svc := New(&mockStorageWithPlanLookup{plans: &capturingPlanStore{}}, directExecutionServerConfig(), map[string]tern.Client{
		DefaultDeployment + "/staging": mockClient,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	_, err := svc.ExecutePlan(t.Context(), PlanRequest{
		Database:    "payments",
		Environment: "staging",
		Type:        storage.DatabaseTypeMySQL,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"payments": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}},
		},
	})
	require.NoError(t, err)

	require.NotNil(t, mockClient.planReq)
	policy := mockClient.planReq.GetDirectExecution()
	require.NotNil(t, policy, "the plan request must state the policy the verdict is judged under")
	assert.True(t, policy.GetEnabled())
	assert.Equal(t, int64(10000), policy.GetMaxTableRows())
	assert.Equal(t, int64(5), policy.GetLockAcquisitionTimeoutSeconds())
}

// The apply records the policy it was admitted under, because the drive that
// routes the statement can be a later one, on another pod, after this server
// has been reconfigured.
func TestQueuedApplyRecordsTheResolvedDirectExecutionPolicy(t *testing.T) {
	plan := &storage.Plan{
		ID:             42,
		PlanIdentifier: "plan-direct",
		Database:       "payments",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     DefaultDeployment,
		Target:         "payments-staging-target",
		Environment:    "staging",
		Namespaces: map[string]*storage.NamespacePlanData{
			"payments": {Tables: []storage.TableChange{{
				Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` DROP PRIMARY KEY, ADD PRIMARY KEY (`account_id`)",
			}}},
		},
	}
	applies := &capturingApplyStore{}
	tasks := &capturingTaskStore{}
	applies.taskStore = tasks
	stor := &mockStorageWithApplyStores{
		plans:     &mockPlanLookupStore{plan: plan},
		applies:   applies,
		tasks:     tasks,
		locks:     &emptyLockStore{},
		applyLogs: &noopApplyLogStore{},
	}
	svc := New(stor, directExecutionServerConfig(), map[string]tern.Client{
		DefaultDeployment + "/staging": &mockTernClient{isRemote: true},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	resp, _, err := svc.ExecuteApply(t.Context(), ApplyRequest{PlanID: "plan-direct", Environment: "staging"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.Accepted)

	require.NotNil(t, applies.apply)
	assert.Equal(t, state.Apply.Pending, applies.apply.State)
	assert.Equal(t, &storage.DirectExecutionPolicy{
		Enabled:                       true,
		MaxTableRows:                  10000,
		LockAcquisitionTimeoutSeconds: 5,
	}, applies.apply.GetOptions().DirectExecution)
}

// A rollback re-plans under the policy its source apply was admitted under,
// read off that apply rather than resolved again from configuration. A
// rollback runs after the change it reverses, so an operator who narrowed or
// withdrew the grant in between would otherwise find the statement that undoes
// a direct change refused, leaving the schema they are trying to walk back on
// the target.
func TestExecuteRollbackPlanStatesTheSourceApplysDirectExecutionPolicy(t *testing.T) {
	plans := &rollbackSourcePlanStore{source: rollbackSourcePlan()}
	client := &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan_rollback"}}

	// The grant the apply ran under is gone from this server's configuration.
	config := directExecutionServerConfig()
	config.DirectExecution = nil
	svc := New(&mockStorageWithPlanLookup{plans: plans}, config, map[string]tern.Client{
		DefaultDeployment + "/staging": client,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	apply := &storage.Apply{
		ID: 1, ApplyIdentifier: "apply_rollback", PlanID: 10,
		Database: "payments", DatabaseType: storage.DatabaseTypeMySQL,
		Repository: "org/repo", PullRequest: 7, Environment: "staging",
		Deployment: DefaultDeployment, State: state.Apply.Completed,
	}
	apply.SetOptions(storage.ApplyOptions{DirectExecution: &storage.DirectExecutionPolicy{
		Enabled:                       true,
		MaxTableRows:                  10000,
		LockAcquisitionTimeoutSeconds: 5,
	}})

	_, err := svc.ExecuteRollbackPlanForApply(t.Context(), apply)
	require.NoError(t, err)

	require.NotNil(t, client.planReq)
	policy := client.planReq.GetDirectExecution()
	require.NotNil(t, policy, "the rollback re-plan must state the policy its source apply was admitted under")
	assert.True(t, policy.GetEnabled())
	assert.Equal(t, int64(10000), policy.GetMaxTableRows())
	assert.Equal(t, int64(5), policy.GetLockAcquisitionTimeoutSeconds())
}

// An apply admitted with no policy stated leaves the rollback's re-plan
// stating none either, so the server that runs the statement judges it under
// its own configuration rather than under a grant this one has since acquired.
func TestExecuteRollbackPlanStatesNoPolicyWhenTheSourceApplyRecordedNone(t *testing.T) {
	plans := &rollbackSourcePlanStore{source: rollbackSourcePlan()}
	client := &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan_rollback"}}

	// This server holds a grant the apply never ran under.
	svc := New(&mockStorageWithPlanLookup{plans: plans}, directExecutionServerConfig(), map[string]tern.Client{
		DefaultDeployment + "/staging": client,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	apply := &storage.Apply{
		ID: 1, ApplyIdentifier: "apply_rollback", PlanID: 10,
		Database: "payments", DatabaseType: storage.DatabaseTypeMySQL,
		Repository: "org/repo", PullRequest: 7, Environment: "staging",
		Deployment: DefaultDeployment, State: state.Apply.Completed,
	}

	_, err := svc.ExecuteRollbackPlanForApply(t.Context(), apply)
	require.NoError(t, err)

	require.NotNil(t, client.planReq)
	assert.Nil(t, client.planReq.GetDirectExecution(),
		"a grant acquired after the apply must not reach the rollback of a change it never covered")
}
