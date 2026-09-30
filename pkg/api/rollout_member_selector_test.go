package api

import (
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// paymentsMembers is a targets list of three members behind one deployment.
func paymentsMembers() []routing.ExecutionTarget {
	return []routing.ExecutionTarget{
		{DatabaseType: storage.DatabaseTypeMySQL, Deployment: "prod", Target: "payments-001"},
		{DatabaseType: storage.DatabaseTypeMySQL, Deployment: "prod", Target: "payments-002"},
		{DatabaseType: storage.DatabaseTypeMySQL, Deployment: "prod", Target: "payments-003"},
	}
}

func TestSelectRolloutMember_BareTargetNarrowsToThatMember(t *testing.T) {
	member, err := selectRolloutMember("payments", "production", paymentsMembers(), "payments-002")
	require.NoError(t, err)
	assert.Equal(t, "prod", member.Deployment)
	assert.Equal(t, "payments-002", member.Target)
}

func TestSelectRolloutMember_MemberIDNarrowsToThatMember(t *testing.T) {
	member, err := selectRolloutMember("payments", "production", paymentsMembers(), "prod/payments-003")
	require.NoError(t, err)
	assert.Equal(t, "prod/payments-003", member.MemberID())
}

// An unknown target is the operator's typo, so the refusal names what they
// could have typed, capped so a long targets list stays readable.
func TestSelectRolloutMember_UnknownTargetListsValidTargets(t *testing.T) {
	_, err := selectRolloutMember("payments", "production", paymentsMembers(), "payments-009")
	require.Error(t, err)
	var selErr *RolloutMemberSelectionError
	require.ErrorAs(t, err, &selErr)
	assert.Equal(t, []string{"payments-001", "payments-002", "payments-003"}, selErr.Valid)
	assert.Empty(t, selErr.Matches)
	assert.Equal(t, `target "payments-009" is not a rollout member of database "payments" environment "production"; valid targets: payments-001, payments-002, payments-003`, err.Error())
}

func TestSelectRolloutMember_LongTargetListIsCapped(t *testing.T) {
	var members []routing.ExecutionTarget
	for i := 1; i <= 13; i++ {
		members = append(members, routing.ExecutionTarget{Deployment: "prod", Target: fmt.Sprintf("payments-%03d", i)})
	}
	_, err := selectRolloutMember("payments", "production", members, "payments-099")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "payments-001, payments-002")
	assert.Contains(t, err.Error(), "payments-010 (and 3 more)")
	assert.NotContains(t, err.Error(), "payments-011")
}

// Two deployments can address targets of the same name. The bare name then
// names two members, and picking one would run a schema change against a
// target the operator did not choose, so it is refused in favor of the
// qualified form.
func TestSelectRolloutMember_AmbiguousBareNameIsRefused(t *testing.T) {
	members := []routing.ExecutionTarget{
		{Deployment: "us-east", Target: "orders"},
		{Deployment: "us-west", Target: "orders"},
	}
	_, err := selectRolloutMember("orders", "production", members, "orders")
	require.Error(t, err)
	var selErr *RolloutMemberSelectionError
	require.ErrorAs(t, err, &selErr)
	assert.Equal(t, []string{"us-east/orders", "us-west/orders"}, selErr.Matches)
	assert.Contains(t, err.Error(), "ambiguous")
	assert.Contains(t, err.Error(), "deployment/target")

	member, err := selectRolloutMember("orders", "production", members, "us-west/orders")
	require.NoError(t, err)
	assert.Equal(t, "us-west", member.Deployment)
}

func TestSelectRolloutMember_SingleTargetEnvironment(t *testing.T) {
	members := []routing.ExecutionTarget{{Deployment: "prod", Target: "orders-main"}}

	member, err := selectRolloutMember("orders", "production", members, "orders-main")
	require.NoError(t, err)
	assert.Equal(t, "orders-main", member.Target)

	_, err = selectRolloutMember("orders", "production", members, "orders-replica")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "valid targets: orders-main")
}

// narrowingServerConfig routes payments/production to three targets behind
// one deployment, so the primary is payments-001.
func narrowingServerConfig() *ServerConfig {
	return &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"payments": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{"production": {
					Deployment: DefaultDeployment,
					Targets:    targetNames("payments-001", "payments-002", "payments-003"),
				}},
			},
		},
		TernDeployments: TernConfig{DefaultDeployment: {"production": "localhost:9090"}},
	}
}

// A narrowed plan is made against the member it names, not the primary, and
// says so on the response so the apply that follows is narrowed with it.
func TestExecutePlan_TargetNarrowsThePlanToThatMember(t *testing.T) {
	mockClient := &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan-narrowed"}}
	plans := &capturingPlanStore{}
	svc := New(&mockStorageWithPlanLookup{plans: plans}, narrowingServerConfig(), map[string]tern.Client{
		DefaultDeployment + "/production": mockClient,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	resp, err := svc.ExecutePlan(t.Context(), PlanRequest{
		Database:    "payments",
		Environment: "production",
		Type:        storage.DatabaseTypeMySQL,
		Target:      "payments-002",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"payments": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}},
		},
	})
	require.NoError(t, err)

	require.NotNil(t, mockClient.planReq)
	assert.Equal(t, "payments-002", mockClient.planReq.GetTarget())
	assert.Equal(t, "payments-002", resp.Target)
	assert.Equal(t, DefaultDeployment+"/payments-002", resp.NarrowedTo)
	require.NotNil(t, plans.created)
	assert.Equal(t, "payments-002", plans.created.Target)
}

func TestExecutePlan_UnknownTargetIsRefusedBeforePlanning(t *testing.T) {
	mockClient := &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan-unused"}}
	svc := New(&mockStorageWithPlanLookup{plans: &capturingPlanStore{}}, narrowingServerConfig(), map[string]tern.Client{
		DefaultDeployment + "/production": mockClient,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	_, err := svc.ExecutePlan(t.Context(), PlanRequest{
		Database:    "payments",
		Environment: "production",
		Type:        storage.DatabaseTypeMySQL,
		Target:      "payments-009",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"payments": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}},
		},
	})
	var selErr *RolloutMemberSelectionError
	require.ErrorAs(t, err, &selErr)
	assert.Nil(t, mockClient.planReq, "no member is planned when the target names none")
}

// narrowingApplyService wires the stores apply creation reaches, and returns the
// apply store so a test can read the apply and operations it created.
func narrowingApplyService(t *testing.T) (*Service, *capturingApplyStore) {
	t.Helper()
	applies := &capturingApplyStore{}
	tasks := &capturingTaskStore{}
	applies.taskStore = tasks
	svc := New(&mockStorageWithApplyStores{
		plans:     &listingPlanStore{},
		applies:   applies,
		tasks:     tasks,
		locks:     &emptyLockStore{},
		applyLogs: &noopApplyLogStore{},
		controls:  &memoryControlRequestStore{},
	}, narrowingServerConfig(), map[string]tern.Client{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return svc, applies
}

// memberPlan is a CLI plan made against one payments member, carrying one
// change and no pull request review round.
func memberPlan(target string) *storage.Plan {
	return &storage.Plan{
		ID:             20,
		PlanIdentifier: "plan-" + target,
		Database:       "payments",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     DefaultDeployment,
		Target:         target,
		Environment:    "production",
		Namespaces: map[string]*storage.NamespacePlanData{
			"payments": {Tables: []storage.TableChange{{
				Namespace: "payments", Table: "users", Operation: "alter",
				DDL: "ALTER TABLE `users` ADD COLUMN `region` varchar(16)",
			}}},
		},
	}
}

// Applying payments-002 alone creates one operation, on payments-002, and
// records the narrowing on the apply so a rollback can tell it changed one
// member.
func TestCreateStoredApply_TargetNarrowsTheRolloutToOneMember(t *testing.T) {
	svc, applies := narrowingApplyService(t)

	apply, _, err := svc.createStoredApply(t.Context(), memberPlan("payments-002"),
		ApplyRequest{Environment: "production", Target: "payments-002"}, nil, "apply-narrowed")
	require.NoError(t, err)

	assert.Equal(t, DefaultDeployment, apply.Deployment)
	assert.Equal(t, DefaultDeployment+"/payments-002", apply.GetOptions().NarrowedTo)
	require.Len(t, applies.operations, 1, "a narrowed apply touches its one member")
	assert.Equal(t, "payments-002", applies.operations[0].Target)
	assert.Equal(t, DefaultDeployment, applies.operations[0].Deployment)
}

// The plan the apply runs must be the one made for the member it names:
// pairing payments-001's plan with payments-002 would run DDL that target was
// never planned for.
func TestCreateStoredApply_TargetMustNameThePlansOwnMember(t *testing.T) {
	svc, applies := narrowingApplyService(t)

	_, _, err := svc.createStoredApply(t.Context(), memberPlan("payments-001"),
		ApplyRequest{Environment: "production", Target: "payments-002"}, nil, "apply-mismatched")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "payments-002")
	assert.Contains(t, err.Error(), "re-plan")
	assert.Nil(t, applies.apply, "no apply is stored for a mismatched member")
}

func TestCreateStoredApply_UnknownTargetIsRefused(t *testing.T) {
	svc, applies := narrowingApplyService(t)

	_, _, err := svc.createStoredApply(t.Context(), memberPlan("payments-002"),
		ApplyRequest{Environment: "production", Target: "payments-009"}, nil, "apply-unknown")
	var selErr *RolloutMemberSelectionError
	require.ErrorAs(t, err, &selErr)
	assert.Nil(t, applies.apply)
}

// A plan made for payments-002 describes that target's schema alone. Running
// it across the rollout would apply it to members nothing planned it for, so a
// rollout-wide apply of it is refused and names the target to apply it with.
func TestCreateStoredApply_RolloutWideApplyOfANonPrimaryPlanIsRefused(t *testing.T) {
	svc, applies := narrowingApplyService(t)

	_, _, err := svc.createStoredApply(t.Context(), memberPlan("payments-002"),
		ApplyRequest{Environment: "production"}, nil, "apply-rollout-wide")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "runs the whole rollout")
	assert.Contains(t, err.Error(), "apply it with target "+DefaultDeployment+"/payments-002")
	assert.Nil(t, applies.apply)
}

// The caller cannot mark an apply narrowed through its options: the record is
// what rollback trusts, so only apply creation writes it.
func TestCreateStoredApply_NarrowingIsNotReadFromCallerOptions(t *testing.T) {
	svc, _ := narrowingApplyService(t)

	apply, _, err := svc.createStoredApply(t.Context(), memberPlan("payments-002"),
		ApplyRequest{Environment: "production", Target: "payments-002"},
		map[string]string{"narrowed_to": DefaultDeployment + "/payments-001"}, "apply-options")
	require.NoError(t, err)
	assert.Equal(t, DefaultDeployment+"/payments-002", apply.GetOptions().NarrowedTo)
}

// A rollback plan is applied to the whole rollout, so reverting an apply that
// changed payments-002 alone would run the reversal on payments-001 and
// payments-003, which never received the change. It is refused before any
// planning.
func TestExecuteRollbackPlan_NarrowedApplyIsRefused(t *testing.T) {
	svc, _ := narrowingApplyService(t)
	apply := &storage.Apply{
		ApplyIdentifier: "apply-narrowed",
		Database:        "payments",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "production",
		Deployment:      DefaultDeployment,
		State:           state.Apply.Completed,
		Options:         storage.MarshalApplyOptions(storage.ApplyOptions{NarrowedTo: DefaultDeployment + "/payments-002"}),
	}

	_, err := svc.ExecuteRollbackPlanForApply(t.Context(), apply)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ran on rollout member "+DefaultDeployment+"/payments-002 only")
	assert.Contains(t, err.Error(), "rollback of a narrowed apply is not supported")
}
