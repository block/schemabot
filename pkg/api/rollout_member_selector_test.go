package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
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
	assert.Equal(t, DefaultDeployment+"/payments-002", plans.created.NarrowedTo, "the plan row records the narrowing, so apply creation can hold the plan to its member")
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
	var mismatch *PlanMemberMismatchError
	require.ErrorAs(t, err, &mismatch, "a mismatched pairing is the caller's request, reported as invalid rather than a server failure")
	assert.Equal(t, DefaultDeployment+"/payments-001", mismatch.PlanMember)
	assert.Equal(t, DefaultDeployment+"/payments-002", mismatch.ApplyMember)
	assert.Contains(t, err.Error(), "re-plan with target "+DefaultDeployment+"/payments-002")
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
	var mismatch *PlanMemberMismatchError
	require.ErrorAs(t, err, &mismatch)
	assert.Equal(t, DefaultDeployment+"/payments-001", mismatch.ApplyMember, "a rollout-wide apply runs from the rollout primary's plan")
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

// singleTargetServerConfig routes orders/production to one target, so the
// rollout is that one member.
func singleTargetServerConfig() *ServerConfig {
	return &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"orders": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{"production": {
					Deployment: DefaultDeployment,
					Target:     "orders-main",
				}},
			},
		},
		TernDeployments: TernConfig{DefaultDeployment: {"production": "localhost:9090"}},
	}
}

func usersSchemaFiles(namespace string) map[string]*ternv1.SchemaFiles {
	return map[string]*ternv1.SchemaFiles{
		namespace: {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}},
	}
}

// Naming the only target of a single-target environment selects the whole
// rollout, so neither the plan nor the apply is narrowed, and the apply can be
// rolled back like any other.
func TestTargetOfASingleTargetEnvironmentDoesNotNarrow(t *testing.T) {
	mockClient := &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan-orders"}}
	plans := &capturingPlanStore{}
	planSvc := New(&mockStorageWithPlanLookup{plans: plans}, singleTargetServerConfig(), map[string]tern.Client{
		DefaultDeployment + "/production": mockClient,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	resp, err := planSvc.ExecutePlan(t.Context(), PlanRequest{
		Database: "orders", Environment: "production", Type: storage.DatabaseTypeMySQL,
		Target: "orders-main", SchemaFiles: usersSchemaFiles("orders"),
	})
	require.NoError(t, err)
	assert.Equal(t, "orders-main", resp.Target)
	assert.Empty(t, resp.NarrowedTo)
	require.NotNil(t, plans.created)
	assert.Empty(t, plans.created.NarrowedTo)

	applies := &capturingApplyStore{}
	tasks := &capturingTaskStore{}
	applies.taskStore = tasks
	applySvc := New(&mockStorageWithApplyStores{
		plans: &listingPlanStore{}, applies: applies, tasks: tasks, locks: &emptyLockStore{},
		applyLogs: &noopApplyLogStore{}, controls: &memoryControlRequestStore{},
	}, singleTargetServerConfig(), map[string]tern.Client{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	plan := memberPlan("orders-main")
	plan.Database = "orders"
	apply, _, err := applySvc.createStoredApply(t.Context(), plan,
		ApplyRequest{Environment: "production", Target: "orders-main"}, nil, "apply-orders")
	require.NoError(t, err)
	assert.Empty(t, apply.GetOptions().NarrowedTo, "an apply of the only member is an apply of the whole rollout")
	require.Len(t, applies.operations, 1)
	assert.Equal(t, "orders-main", applies.operations[0].Target)
}

// A plan narrowed to the rollout primary holds the same DDL an ordinary plan
// of the primary would, but it was reviewed as covering that one member. Its
// row records the narrowing, so an apply of the whole rollout from it is
// refused rather than fanned out to members nobody reviewed it for.
func TestCreateStoredApply_RolloutWideApplyOfAPlanNarrowedToThePrimaryIsRefused(t *testing.T) {
	svc, applies := narrowingApplyService(t)
	plan := memberPlan("payments-001")
	plan.NarrowedTo = DefaultDeployment + "/payments-001"

	_, _, err := svc.createStoredApply(t.Context(), plan, ApplyRequest{Environment: "production"}, nil, "apply-rollout-wide")
	var mismatch *PlanMemberMismatchError
	require.ErrorAs(t, err, &mismatch)
	assert.True(t, mismatch.PlanNarrowed)
	assert.Contains(t, err.Error(), "was narrowed to rollout member "+DefaultDeployment+"/payments-001")
	assert.Contains(t, err.Error(), "apply it with target "+DefaultDeployment+"/payments-001")
	assert.Nil(t, applies.apply, "no apply is stored for a narrowed plan applied rollout-wide")

	apply, _, err := svc.createStoredApply(t.Context(), plan,
		ApplyRequest{Environment: "production", Target: "payments-001"}, nil, "apply-narrowed")
	require.NoError(t, err, "the plan applies to the member it was narrowed to")
	assert.Equal(t, DefaultDeployment+"/payments-001", apply.GetOptions().NarrowedTo)
	require.Len(t, applies.operations, 1)
	assert.Equal(t, "payments-001", applies.operations[0].Target)
}

// A planner sharing the service's storage stores a plan's row first, with the
// route it knows and no narrowing. The service keeps that row and records the
// narrowing on it, since that is what holds an apply of the plan to its one
// member. A row that already records a different narrowing, the whole rollout
// or another member, is not this plan and fails it.
func TestExecutePlan_NarrowedPlanRecordsItsNarrowingOnTheRowAlreadyStored(t *testing.T) {
	member := DefaultDeployment + "/payments-002"
	cases := []struct {
		name       string
		storedTo   string
		wantRouted []string
		wantErr    string
	}{
		{name: "row the planner stored without a narrowing takes the plan's", storedTo: "",
			wantRouted: []string{"plan-narrowed " + DefaultDeployment + "/payments-002 narrowed to " + member}},
		{name: "row already narrowed to the member is the same plan delivered twice", storedTo: member},
		{name: "row narrowed to another member fails the plan", storedTo: DefaultDeployment + "/payments-003",
			wantErr: "collides with a stored plan narrowed to \"" + DefaultDeployment + "/payments-003\""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan-narrowed"}}
			plans := &capturingPlanStore{
				mockPlanLookupStore: mockPlanLookupStore{plan: &storage.Plan{
					PlanIdentifier: "plan-narrowed", Database: "payments", Environment: "production",
					Deployment: DefaultDeployment, Target: "payments-002", NarrowedTo: tc.storedTo,
				}},
				createErr: storage.ErrPlanIDExists,
			}
			svc := New(&mockStorageWithPlanLookup{plans: plans}, narrowingServerConfig(), map[string]tern.Client{
				DefaultDeployment + "/production": mockClient,
			}, slog.New(slog.NewTextHandler(io.Discard, nil)))

			resp, err := svc.ExecutePlan(t.Context(), PlanRequest{
				Database: "payments", Environment: "production", Type: storage.DatabaseTypeMySQL,
				Target: "payments-002", SchemaFiles: usersSchemaFiles("payments"),
			})
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				assert.Empty(t, plans.routed, "a row on another narrowing is left as stored")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, member, resp.NarrowedTo)
			assert.Equal(t, tc.wantRouted, plans.routed)
		})
	}
}

// A plan of the whole rollout whose identifier names a row already narrowed to
// one member is not that plan: returning it would hand the caller a plan that
// applies to one member only.
func TestExecutePlan_RolloutWidePlanRefusedWhenItsStoredRowIsNarrowed(t *testing.T) {
	mockClient := &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan-rollout"}}
	plans := &capturingPlanStore{
		mockPlanLookupStore: mockPlanLookupStore{plan: &storage.Plan{
			PlanIdentifier: "plan-rollout", Database: "payments", Environment: "production",
			Deployment: DefaultDeployment, Target: "payments-001", NarrowedTo: DefaultDeployment + "/payments-001",
		}},
		createErr: storage.ErrPlanIDExists,
	}
	svc := New(&mockStorageWithPlanLookup{plans: plans}, narrowingServerConfig(), map[string]tern.Client{
		DefaultDeployment + "/production": mockClient,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	_, err := svc.ExecutePlan(t.Context(), PlanRequest{
		Database: "payments", Environment: "production", Type: storage.DatabaseTypeMySQL,
		SchemaFiles: usersSchemaFiles("payments"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plan plan-rollout narrowed to \"\" collides with a stored plan narrowed to \""+DefaultDeployment+"/payments-001\"")
	assert.Empty(t, plans.routed)
}

// serveNarrowing posts body to path on a mux serving svc's routes.
func serveNarrowing(t *testing.T, svc *Service, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func requireInvalidRequest(t *testing.T, w *httptest.ResponseRecorder, wantMessage string) {
	t.Helper()
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	var body apitypes.ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	assert.Equal(t, apitypes.ErrCodeInvalidRequest, body.ErrorCode)
	assert.Contains(t, body.Error, wantMessage)
}

// A target that names no member, and a plan paired with a member it was not
// made for, are the caller's requests and answered as invalid, not as a server
// failure, so the caller is told what to change.
func TestApplyHandler_RefusesMisnamedAndMismatchedTargetsAsInvalidRequests(t *testing.T) {
	cases := []struct {
		name        string
		planTarget  string
		narrowedTo  string
		body        string
		wantMessage string
	}{
		{
			name:        "unknown target",
			planTarget:  "payments-002",
			body:        `{"plan_id":"plan-payments-002","environment":"production","target":"payments-009"}`,
			wantMessage: `target "payments-009" is not a rollout member`,
		},
		{
			name:        "target other than the plan's member",
			planTarget:  "payments-001",
			body:        `{"plan_id":"plan-payments-001","environment":"production","target":"payments-002"}`,
			wantMessage: "re-plan with target " + DefaultDeployment + "/payments-002",
		},
		{
			name:        "narrowed plan applied rollout-wide",
			planTarget:  "payments-001",
			narrowedTo:  DefaultDeployment + "/payments-001",
			body:        `{"plan_id":"plan-payments-001","environment":"production"}`,
			wantMessage: "was narrowed to rollout member " + DefaultDeployment + "/payments-001",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, applies := narrowingApplyService(t)
			plan := memberPlan(tc.planTarget)
			plan.NarrowedTo = tc.narrowedTo
			svc.storage.(*mockStorageWithApplyStores).plans = &listingPlanStore{mockPlanLookupStore: mockPlanLookupStore{plan: plan}}

			requireInvalidRequest(t, serveNarrowing(t, svc, "/api/apply", tc.body), tc.wantMessage)
			assert.Nil(t, applies.apply)
		})
	}
}

func TestPlanHandler_RefusesAnUnknownTargetAsAnInvalidRequest(t *testing.T) {
	mockClient := &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan-unused"}}
	svc := New(&mockStorageWithPlanLookup{plans: &capturingPlanStore{}}, narrowingServerConfig(), map[string]tern.Client{
		DefaultDeployment + "/production": mockClient,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	w := serveNarrowing(t, svc, "/api/plan", `{"database":"payments","environment":"production","type":"mysql","target":"payments-009","schema_files":{"payments":{"files":{"users.sql":"CREATE TABLE users (id bigint primary key)"}}}}`)
	requireInvalidRequest(t, w, `target "payments-009" is not a rollout member`)
	assert.Nil(t, mockClient.planReq)
}

// A rollout-wide apply ran its plan from payments-001, then the targets list
// was reordered so payments-002 is first. The rollback is planned against
// payments-001, and the rollout now runs only from payments-002's plan, so the
// rollback could reach the rollout only by reverting payments-001 alone. It is
// refused before planning, with the order the apply ran under as the remedy
// rather than a target to revert by itself. In the order the apply ran under,
// the rollback is planned against payments-001 as before.
func TestExecuteRollbackPlan_RefusedAfterTheRolloutPrimaryMoved(t *testing.T) {
	source := memberPlan("payments-001")
	source.Namespaces["payments"].OriginalFiles = map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}
	source.Namespaces["payments"].OriginalFilesCaptured = true
	apply := &storage.Apply{
		ApplyIdentifier: "apply-rollout",
		PlanID:          source.ID,
		Database:        "payments",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "production",
		Deployment:      DefaultDeployment,
		State:           state.Apply.Completed,
	}
	cases := []struct {
		name    string
		targets []string
		wantErr string
	}{
		{
			name:    "reordered",
			targets: []string{"payments-002", "payments-001", "payments-003"},
			wantErr: "apply apply-rollout ran across the rollout from rollout primary " + DefaultDeployment + "/payments-001, but the rollout primary is now " + DefaultDeployment + "/payments-002; " +
				"a rollback made against " + DefaultDeployment + "/payments-001 cannot be applied to the rollout, and applying it to that target alone would leave the others on the applied schema. " +
				"Restore the rollout order the apply ran under (deployment_order, or the order of the targets list) so " + DefaultDeployment + "/payments-001 is first, then retry the rollback",
		},
		{name: "in the order the apply ran under", targets: []string{"payments-001", "payments-002", "payments-003"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := narrowingServerConfig()
			env := cfg.Databases["payments"].Environments["production"]
			env.Targets = targetNames(tc.targets...)
			cfg.Databases["payments"].Environments["production"] = env
			mockClient := &mockTernClient{isRemote: true, planResp: &ternv1.PlanResponse{PlanId: "plan-rollback"}}
			svc := New(&mockStorageWithPlanLookup{plans: &rollbackSourcePlanStore{source: source}}, cfg, map[string]tern.Client{
				DefaultDeployment + "/production": mockClient,
			}, slog.New(slog.NewTextHandler(io.Discard, nil)))

			_, err := svc.ExecuteRollbackPlanForApply(t.Context(), apply)
			if tc.wantErr == "" {
				require.NoError(t, err)
				require.NotNil(t, mockClient.planReq)
				assert.Equal(t, "payments-001", mockClient.planReq.Target)
				return
			}
			require.Error(t, err)
			assert.Equal(t, tc.wantErr, err.Error())
			assert.Equal(t, http.StatusConflict, controlOperationHTTPStatus(err))
			assert.Nil(t, mockClient.planReq, "nothing is planned for a rollback that cannot reach the rollout")
		})
	}
}

// memberDiffClient answers each rollout member's PlanDiff by its target, so
// members behind one deployment can hold different live schemas.
type memberDiffClient struct {
	*mockTernClient
	diffs    map[string]*ternv1.PlanDiffResponse
	diffErrs map[string]error
	mu       sync.Mutex
	diffed   []string
}

func (c *memberDiffClient) PlanDiff(_ context.Context, req *ternv1.PlanRequest) (*ternv1.PlanDiffResponse, error) {
	c.mu.Lock()
	c.diffed = append(c.diffed, req.Target)
	c.mu.Unlock()
	if err := c.diffErrs[req.Target]; err != nil {
		return nil, err
	}
	if diff, ok := c.diffs[req.Target]; ok {
		return diff, nil
	}
	return &ternv1.PlanDiffResponse{Engine: ternv1.Engine_ENGINE_SPIRIT}, nil
}

// A plan of the whole rollout is made against its primary. When the primary
// is at the desired schema, the other members are diffed against their own
// live schemas: the plan is up to date only when every one of them is too, so
// a converged rollout (onboarding's verification, a re-run of an apply that
// already landed) reads as up to date. A member that still needs the change,
// for example after an apply narrowed to the primary landed it there alone, or
// a member that could not be diffed, turns the plan into an error naming that
// member. A plan with changes, a narrowed plan, and a plan of a single-target
// environment are returned as they are, without diffing anyone else.
func TestPlanHandler_ConvergedPrimaryIsUpToDateOnlyWhenEveryMemberIs(t *testing.T) {
	withChanges := &ternv1.PlanResponse{PlanId: "plan-changes", Changes: []*ternv1.SchemaChange{{
		Namespace: "payments",
		TableChanges: []*ternv1.TableChange{{
			TableName: "users", Ddl: "ALTER TABLE `users` ADD COLUMN `region` varchar(16)", ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
		}},
	}}}
	cases := []struct {
		name       string
		config     *ServerConfig
		database   string
		target     string
		planResp   *ternv1.PlanResponse
		diffs      map[string]*ternv1.PlanDiffResponse
		diffErrs   map[string]error
		wantDiffed []string
		wantErrors []string
	}{
		{
			name:       "rollout-wide plan with every member converged",
			config:     narrowingServerConfig(),
			database:   "payments",
			planResp:   &ternv1.PlanResponse{PlanId: "plan-converged"},
			wantDiffed: []string{"payments-002", "payments-003"},
		},
		{
			name:       "rollout-wide plan with a converged primary and a member that still needs the change",
			config:     narrowingServerConfig(),
			database:   "payments",
			planResp:   &ternv1.PlanResponse{PlanId: "plan-converged"},
			diffs:      map[string]*ternv1.PlanDiffResponse{"payments-003": alterUsersDiff("ALTER TABLE `users` ADD COLUMN `region` varchar(16)")},
			wantDiffed: []string{"payments-002", "payments-003"},
			wantErrors: []string{"rollout primary " + DefaultDeployment + "/payments-001 is at the desired schema, but 1 other rollout members still need changes: payments-003. " +
				"The rollout is not up to date; plan and apply each of them with its target"},
		},
		{
			name:       "rollout-wide plan with a converged primary and a member that could not be diffed",
			config:     narrowingServerConfig(),
			database:   "payments",
			planResp:   &ternv1.PlanResponse{PlanId: "plan-converged"},
			diffErrs:   map[string]error{"payments-002": errors.New("dial tcp 10.0.0.7:3306: connection refused")},
			wantDiffed: []string{"payments-002", "payments-003"},
			wantErrors: []string{"rollout primary " + DefaultDeployment + "/payments-001 is at the desired schema, but 1 other rollout members could not be planned: payments-002. " +
				"The rollout is not reported as up to date until each of them is planned; see the server logs for why"},
		},
		{name: "rollout-wide plan with changes", config: narrowingServerConfig(), database: "payments", planResp: withChanges},
		{name: "narrowed plan with no changes", config: narrowingServerConfig(), database: "payments", target: "payments-001", planResp: &ternv1.PlanResponse{PlanId: "plan-narrowed"}},
		{name: "single-target environment", config: singleTargetServerConfig(), database: "orders", planResp: &ternv1.PlanResponse{PlanId: "plan-orders"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := &memberDiffClient{
				mockTernClient: &mockTernClient{isRemote: true, planResp: tc.planResp},
				diffs:          tc.diffs,
				diffErrs:       tc.diffErrs,
			}
			svc := New(&mockStorageWithPlanLookup{plans: &capturingPlanStore{}}, tc.config, map[string]tern.Client{
				DefaultDeployment + "/production": client,
			}, slog.New(slog.NewTextHandler(io.Discard, nil)))

			body, err := json.Marshal(apitypes.PlanRequest{
				Database: tc.database, Environment: "production", Type: storage.DatabaseTypeMySQL, Target: tc.target,
				SchemaFiles: map[string]*apitypes.SchemaFiles{tc.database: {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}}},
			})
			require.NoError(t, err)
			w := serveNarrowing(t, svc, "/api/plan", string(body))
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			var resp apitypes.PlanResponse
			require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
			if tc.wantErrors == nil {
				assert.Empty(t, resp.Errors)
			} else {
				assert.Equal(t, tc.wantErrors, resp.Errors)
			}
			assert.NotContains(t, w.Body.String(), "10.0.0.7", "a member's diff error stays in the server logs")
			client.mu.Lock()
			defer client.mu.Unlock()
			assert.ElementsMatch(t, tc.wantDiffed, client.diffed, "only the members other than the primary are diffed, and only when the primary is converged")
		})
	}
}
