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

// The plan row records the policy its execution verdicts were judged under, so
// the apply created from it can run the statements under the same one rather
// than under whatever the configuration says at admission.
func TestStoredPlanRecordsTheResolvedDirectExecutionPolicy(t *testing.T) {
	plans := &capturingPlanStore{}
	svc := New(&mockStorageWithPlanLookup{plans: plans}, directExecutionServerConfig(), map[string]tern.Client{
		DefaultDeployment + "/staging": &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan-direct"}},
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

	require.NotNil(t, plans.created)
	assert.Equal(t, &storage.DirectExecutionPolicy{
		Enabled:                       true,
		MaxTableRows:                  10000,
		LockAcquisitionTimeoutSeconds: 5,
	}, plans.created.DirectExecution)
}

// A server holding no grant still records an answer on the plan row. Without
// it, nothing on the row distinguishes a plan judged under no grant from one
// stored before the column existed, and the second is the only case that may
// fall back to configuration.
func TestStoredPlanRecordsAnOptOutWhenNoGrantIsInForce(t *testing.T) {
	plans := &capturingPlanStore{}
	config := directExecutionServerConfig()
	config.DirectExecution = nil
	svc := New(&mockStorageWithPlanLookup{plans: plans}, config, map[string]tern.Client{
		DefaultDeployment + "/staging": &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan-direct"}},
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

	require.NotNil(t, plans.created)
	assert.Equal(t, &storage.DirectExecutionPolicy{Enabled: false}, plans.created.DirectExecution)
}

// directExecutionApplyService admits an apply from plan against config.
func directExecutionApplyService(t *testing.T, plan *storage.Plan, config *ServerConfig) *capturingApplyStore {
	t.Helper()
	applies := &capturingApplyStore{}
	tasks := &capturingTaskStore{}
	applies.taskStore = tasks
	svc := New(&mockStorageWithApplyStores{
		plans:     &mockPlanLookupStore{plan: plan},
		applies:   applies,
		tasks:     tasks,
		locks:     &emptyLockStore{},
		applyLogs: &noopApplyLogStore{},
	}, config, map[string]tern.Client{
		DefaultDeployment + "/staging": &mockTernClient{isRemote: true},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	resp, _, err := svc.ExecuteApply(t.Context(), ApplyRequest{PlanID: plan.PlanIdentifier, Environment: "staging"})
	require.NoError(t, err)
	require.True(t, resp.Accepted)
	require.NotNil(t, applies.apply)
	return applies
}

// directExecutionPlan is a stored plan carrying one direct-executable change.
func directExecutionPlan(policy *storage.DirectExecutionPolicy) *storage.Plan {
	return &storage.Plan{
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
		DirectExecution: policy,
	}
}

// The plan's recorded opt-out is what the apply runs under, even on a server
// that has since acquired a grant. The operator reviewed a plan whose refused
// statement was blocked; a grant arriving between review and apply must not
// turn that same statement into one the engine runs.
func TestApplyRunsUnderThePlansRecordedOptOut(t *testing.T) {
	applies := directExecutionApplyService(t,
		directExecutionPlan(&storage.DirectExecutionPolicy{Enabled: false}),
		directExecutionServerConfig())

	assert.Equal(t, &storage.DirectExecutionPolicy{Enabled: false},
		applies.apply.GetOptions().DirectExecution)
}

// The plan's recorded grant is likewise what the apply runs under after the
// configuration withdraws it, so a reviewed verdict stays executable rather
// than being refused at apply under a bound the operator never saw.
func TestApplyRunsUnderThePlansRecordedGrant(t *testing.T) {
	config := directExecutionServerConfig()
	config.DirectExecution = nil
	applies := directExecutionApplyService(t, directExecutionPlan(&storage.DirectExecutionPolicy{
		Enabled:                       true,
		MaxTableRows:                  10000,
		LockAcquisitionTimeoutSeconds: 5,
	}), config)

	assert.Equal(t, &storage.DirectExecutionPolicy{
		Enabled:                       true,
		MaxTableRows:                  10000,
		LockAcquisitionTimeoutSeconds: 5,
	}, applies.apply.GetOptions().DirectExecution)
}

// A plan stored before the row carried a policy has none to honor, and its
// apply resolves one from configuration the way admission always did.
func TestApplyResolvesFromConfigWhenThePlanRecordedNoPolicy(t *testing.T) {
	applies := directExecutionApplyService(t, directExecutionPlan(nil), directExecutionServerConfig())

	assert.Equal(t, &storage.DirectExecutionPolicy{
		Enabled:                       true,
		MaxTableRows:                  10000,
		LockAcquisitionTimeoutSeconds: 5,
	}, applies.apply.GetOptions().DirectExecution)
}

// A rollback's own plan row records the source apply's policy, which is what
// carries the grant across the confirm step: the rollback apply is admitted
// from the pinned plan, without the source apply in hand.
func TestStoredRollbackPlanRecordsTheSourceApplysDirectExecutionPolicy(t *testing.T) {
	plans := &rollbackSourcePlanStore{source: rollbackSourcePlan()}
	config := directExecutionServerConfig()
	config.DirectExecution = nil
	svc := New(&mockStorageWithPlanLookup{plans: plans}, config, map[string]tern.Client{
		DefaultDeployment + "/staging": &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan_rollback"}},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	apply := &storage.Apply{
		ID: 1, ApplyIdentifier: "apply_rollback", PlanID: 10,
		Database: "payments", DatabaseType: storage.DatabaseTypeMySQL,
		Repository: "org/repo", PullRequest: 7, Environment: "staging",
		Deployment: DefaultDeployment, State: state.Apply.Completed,
	}
	admitted := &storage.DirectExecutionPolicy{
		Enabled:                       true,
		MaxTableRows:                  10000,
		LockAcquisitionTimeoutSeconds: 5,
	}
	apply.SetOptions(storage.ApplyOptions{DirectExecution: admitted})

	_, err := svc.ExecuteRollbackPlanForApply(t.Context(), apply)
	require.NoError(t, err)

	require.NotNil(t, plans.created)
	assert.Equal(t, admitted, plans.created.DirectExecution)
}

// A source apply that recorded no policy leaves its rollback plan recording
// none either, so the reversal resolves one the same way the forward apply
// did rather than inheriting a grant that never covered it.
func TestStoredRollbackPlanRecordsNoPolicyWhenTheSourceApplyRecordedNone(t *testing.T) {
	plans := &rollbackSourcePlanStore{source: rollbackSourcePlan()}
	svc := New(&mockStorageWithPlanLookup{plans: plans}, directExecutionServerConfig(), map[string]tern.Client{
		DefaultDeployment + "/staging": &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan_rollback"}},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	apply := &storage.Apply{
		ID: 1, ApplyIdentifier: "apply_rollback", PlanID: 10,
		Database: "payments", DatabaseType: storage.DatabaseTypeMySQL,
		Repository: "org/repo", PullRequest: 7, Environment: "staging",
		Deployment: DefaultDeployment, State: state.Apply.Completed,
	}

	_, err := svc.ExecuteRollbackPlanForApply(t.Context(), apply)
	require.NoError(t, err)

	require.NotNil(t, plans.created)
	assert.Nil(t, plans.created.DirectExecution)
}
