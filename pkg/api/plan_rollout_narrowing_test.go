package api

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// storingPlanStore stores every created plan where a later lookup by primary
// plan identifier finds it, the way a plan response and the apply created from
// it share one store.
type storingPlanStore struct {
	listingPlanStore
}

func (s *storingPlanStore) Create(_ context.Context, plan *storage.Plan) (int64, error) {
	s.plans = append(s.plans, plan)
	return int64(len(s.plans)), nil
}

// Get returns a plan this store holds under the identifier, or the seeded
// lookup plan, which stands in for the primary plan its planner stored.
func (s *storingPlanStore) Get(ctx context.Context, planIdentifier string) (*storage.Plan, error) {
	for _, plan := range s.plans {
		if plan.PlanIdentifier == planIdentifier {
			return plan, nil
		}
	}
	return s.mockPlanLookupStore.Get(ctx, planIdentifier)
}

// countingDiffClient diffs every member to the same plan and counts the
// diffs, which the rollout runs concurrently across members.
type countingDiffClient struct {
	*mockTernClient
	mu    sync.Mutex
	diffs int
}

func (c *countingDiffClient) PlanDiff(context.Context, *ternv1.PlanRequest) (*ternv1.PlanDiffResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.diffs++
	return addRegionDiff(), nil
}

func (c *countingDiffClient) diffCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.diffs
}

// paymentsPlanRequest plans the payments schema in production, optionally
// narrowed to one rollout member.
func paymentsPlanRequest(target string) PlanRequest {
	return PlanRequest{
		Database:    "payments",
		Environment: "production",
		Type:        storage.DatabaseTypeMySQL,
		Target:      target,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"payments": {Files: map[string]string{"users.sql": "CREATE TABLE `users` (id bigint primary key)"}},
		},
	}
}

// addRegionPlan is the plan of one payments member that adds the region
// column.
func addRegionPlan(planID string) *ternv1.PlanResponse {
	return &ternv1.PlanResponse{
		PlanId: planID,
		Engine: ternv1.Engine_ENGINE_SPIRIT,
		Changes: []*ternv1.SchemaChange{{
			Namespace: "payments",
			TableChanges: []*ternv1.TableChange{{
				TableName:  "users",
				ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
				Ddl:        "ALTER TABLE `users` ADD COLUMN `region` varchar(16)",
				Namespace:  "payments",
			}},
		}},
	}
}

func addRegionDiff() *ternv1.PlanDiffResponse {
	plan := addRegionPlan("")
	return &ternv1.PlanDiffResponse{Engine: plan.Engine, Changes: plan.Changes}
}

// rolloutPlanningService routes payments/production to three targets behind
// one deployment, each planned against its own schema, and returns the tern
// client the other members are diffed through.
func rolloutPlanningService(t *testing.T, plans storage.PlanStore, applies *capturingApplyStore) (*Service, *countingDiffClient) {
	t.Helper()
	client := &countingDiffClient{mockTernClient: &mockTernClient{}}
	tasks := &capturingTaskStore{}
	applies.taskStore = tasks
	svc := New(&mockStorageWithApplyStores{
		plans:     plans,
		applies:   applies,
		tasks:     tasks,
		locks:     &emptyLockStore{},
		applyLogs: &noopApplyLogStore{},
		controls:  &memoryControlRequestStore{},
	}, narrowingServerConfig(), map[string]tern.Client{
		DefaultDeployment + "/production": client,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return svc, client
}

// A plan narrowed to payments-002 speaks for that target alone. The rollout is
// not planned beside it, so the response carries no rollout block that could
// be read as every target's plan, and no member plan is stored bound to it for
// a rollout-wide apply to pick up.
func TestPlanRollout_NarrowedPlanPlansNoOtherMember(t *testing.T) {
	plans := &storingPlanStore{}
	svc, client := rolloutPlanningService(t, plans, &capturingApplyStore{})

	rollout, err := svc.planRollout(t.Context(), paymentsPlanRequest("payments-002"), addRegionPlan("plan-payments-002"),
		&apitypes.PlanResponse{Deployment: DefaultDeployment, Target: "payments-002", NarrowedTo: DefaultDeployment + "/payments-002"})
	require.NoError(t, err)

	assert.Nil(t, rollout, "a narrowed plan carries no rollout block")
	assert.Zero(t, client.diffCount(), "no other member is planned beside a narrowed plan")
	assert.Empty(t, plans.plans, "no member plan is stored bound to a narrowed plan")
}

// The other members are planned only beside the rollout primary's plan: a plan
// made for payments-002 without a selector is not a baseline the rollout can
// be grouped against, so it is refused rather than reported as the rollout.
func TestPlanRollout_PlanOfANonPrimaryMemberIsRefused(t *testing.T) {
	plans := &storingPlanStore{}
	svc, client := rolloutPlanningService(t, plans, &capturingApplyStore{})

	_, err := svc.planRollout(t.Context(), paymentsPlanRequest(""), addRegionPlan("plan-payments-002"),
		&apitypes.PlanResponse{Deployment: DefaultDeployment, Target: "payments-002"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "made for rollout member "+DefaultDeployment+"/payments-002")
	assert.Contains(t, err.Error(), "not the rollout primary "+DefaultDeployment+"/payments-001")
	assert.Zero(t, client.diffCount())
	assert.Empty(t, plans.plans)
}

// The rollout plan an operator reviews on the CLI is the primary's plan with
// every other member's plan stored beside it, so the rollout-wide apply that
// follows passes the primary-plan check and runs each target's own plan.
func TestCreateStoredApply_RolloutWideApplyOfTheRolloutPlanRunsEveryMember(t *testing.T) {
	plans := &storingPlanStore{listingPlanStore{mockPlanLookupStore: mockPlanLookupStore{plan: memberPlan("payments-001")}}}
	applies := &capturingApplyStore{}
	svc, client := rolloutPlanningService(t, plans, applies)

	rollout, err := svc.planRollout(t.Context(), paymentsPlanRequest(""), addRegionPlan("plan-payments-001"),
		&apitypes.PlanResponse{Deployment: DefaultDeployment, Target: "payments-001"})
	require.NoError(t, err)
	require.NotNil(t, rollout)
	assert.Equal(t, 3, rollout.Members)
	assert.Equal(t, 2, client.diffCount(), "payments-002 and payments-003 are each planned against their own schema")
	require.Len(t, plans.plans, 2, "payments-002 and payments-003 each have a stored plan")

	apply, _, err := svc.createStoredApply(t.Context(), memberPlan("payments-001"),
		ApplyRequest{Environment: "production"}, nil, "apply-rollout")
	require.NoError(t, err)

	assert.Empty(t, apply.GetOptions().NarrowedTo, "a rollout-wide apply is not narrowed")
	targets := make([]string, 0, len(applies.operations))
	for _, op := range applies.operations {
		targets = append(targets, op.Target)
	}
	assert.ElementsMatch(t, []string{"payments-001", "payments-002", "payments-003"}, targets)
}
