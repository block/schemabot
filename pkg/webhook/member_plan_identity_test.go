package webhook

import (
	"context"
	"errors"
	"testing"

	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// planIdentityStorage serves plan rows by row id and records every lookup, so a
// test can pin both the identifiers the comment resolves and the reads it makes
// to resolve them.
type planIdentityStorage struct {
	emptyStorage
	store *planIdentityPlanStore
}

func (s *planIdentityStorage) Plans() storage.PlanStore { return s.store }

type planIdentityPlanStore struct {
	storage.PlanStore
	plans map[int64]*storage.Plan
	err   error
	reads []int64
}

func (s *planIdentityPlanStore) GetByID(_ context.Context, id int64) (*storage.Plan, error) {
	s.reads = append(s.reads, id)
	if s.err != nil {
		return nil, s.err
	}
	return s.plans[id], nil
}

func planIdentityStore(plans map[int64]*storage.Plan) *planIdentityPlanStore {
	return &planIdentityPlanStore{plans: plans}
}

// A rollout whose members were planned independently names each member's plan,
// so an operator can tie a running member to the block they reviewed. The
// primary carries no plan of its own and runs the apply's.
func TestResolvePlanIdentifiers_NamesEachMembersPlan(t *testing.T) {
	store := planIdentityStore(map[int64]*storage.Plan{
		7:  {ID: 7, PlanIdentifier: "plan_reviewed"},
		42: {ID: 42, PlanIdentifier: "plan_3344"},
	})
	apply := &storage.Apply{ApplyIdentifier: "apply-1", PlanID: 7}
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "primary"},
		{ID: 2, Deployment: "eu-west", PlanID: 42},
	}

	byOp := resolvePlanIdentifiers(t.Context(), &planIdentityStorage{store: store}, apply, ops)

	assert.Equal(t, map[int64]string{1: "plan_reviewed", 2: "plan_3344"}, byOp)
}

// Members that share one plan name nothing: the same identifier under every
// member tells the reader nothing, so the comment neither renders it nor reads
// the row it would come from.
func TestResolvePlanIdentifiers_ConvergedRolloutReadsNoPlans(t *testing.T) {
	store := planIdentityStore(map[int64]*storage.Plan{7: {ID: 7, PlanIdentifier: "plan_reviewed"}})
	apply := &storage.Apply{ApplyIdentifier: "apply-1", PlanID: 7}
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "primary"},
		{ID: 2, Deployment: "eu-west", PlanID: 7},
	}

	byOp := resolvePlanIdentifiers(t.Context(), &planIdentityStorage{store: store}, apply, ops)

	assert.Empty(t, byOp)
	assert.Empty(t, store.reads, "a converged rollout reads no plan rows")
}

// Each distinct plan is read once however many members run it.
func TestResolvePlanIdentifiers_ReadsEachPlanOnce(t *testing.T) {
	store := planIdentityStore(map[int64]*storage.Plan{
		7:  {ID: 7, PlanIdentifier: "plan_reviewed"},
		42: {ID: 42, PlanIdentifier: "plan_3344"},
	})
	apply := &storage.Apply{ApplyIdentifier: "apply-1", PlanID: 7}
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "primary"},
		{ID: 2, Deployment: "eu-west", PlanID: 42},
		{ID: 3, Deployment: "ap-south", PlanID: 42},
	}

	byOp := resolvePlanIdentifiers(t.Context(), &planIdentityStorage{store: store}, apply, ops)

	require.Len(t, byOp, 3)
	assert.Equal(t, "plan_3344", byOp[3])
	assert.ElementsMatch(t, []int64{7, 42}, store.reads)
}

// A plan row that cannot be read leaves its members unnamed rather than failing
// the comment: the identifier is context, and the rollout still has progress,
// errors, and next actions to show.
func TestResolvePlanIdentifiers_UnreadablePlanLeavesMembersUnnamed(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store *planIdentityPlanStore
	}{
		{"load fails", &planIdentityPlanStore{err: errors.New("storage read failed")}},
		{"row missing", planIdentityStore(map[int64]*storage.Plan{7: {ID: 7, PlanIdentifier: "plan_reviewed"}})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			apply := &storage.Apply{ApplyIdentifier: "apply-1", PlanID: 7}
			ops := []*storage.ApplyOperation{
				{ID: 1, Deployment: "primary"},
				{ID: 2, Deployment: "eu-west", PlanID: 42},
			}

			byOp := resolvePlanIdentifiers(t.Context(), &planIdentityStorage{store: tc.store}, apply, ops)

			assert.NotContains(t, byOp, int64(2), "the unreadable plan names no member")
		})
	}
}

// An operation with no resolvable plan contributes no plan to the rollout's
// distinct set. Counting it would make a converged rollout look divergent and
// start naming a plan under every member, on the strength of a member whose plan
// could not be read at all.
func TestResolvePlanIdentifiers_UnresolvableOperationDoesNotSplitAConvergedRollout(t *testing.T) {
	store := planIdentityStore(map[int64]*storage.Plan{7: {ID: 7, PlanIdentifier: "plan_reviewed"}})
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "primary", PlanID: 7},
		{ID: 2, Deployment: "eu-west", PlanID: 7},
		{ID: 3, Deployment: "ap-south"},
	}

	byOp := resolvePlanIdentifiers(t.Context(), &planIdentityStorage{store: store}, nil, ops)

	assert.Empty(t, byOp)
	assert.Empty(t, store.reads, "the members that do have a plan all run the same one")
}

// An operation that names no plan, on an apply that names none either, is not
// executable. It is left unnamed while its siblings are still named.
func TestResolvePlanIdentifiers_OperationWithoutAPlanIsSkipped(t *testing.T) {
	store := planIdentityStore(map[int64]*storage.Plan{
		42: {ID: 42, PlanIdentifier: "plan_3344"},
		43: {ID: 43, PlanIdentifier: "plan_3345"},
	})
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "primary"},
		{ID: 2, Deployment: "eu-west", PlanID: 42},
		{ID: 3, Deployment: "ap-south", PlanID: 43},
	}

	byOp := resolvePlanIdentifiers(t.Context(), &planIdentityStorage{store: store}, &storage.Apply{ApplyIdentifier: "apply-1"}, ops)

	assert.Equal(t, map[int64]string{2: "plan_3344", 3: "plan_3345"}, byOp)
}

// A member waiting its turn has no progress, no VSchema state, and no deploy
// request, but it does have a plan. The display projection carries it: a queued
// member is exactly the one a reader checks to see what it is about to run.
func TestResolveDisplayByOperation_NamesThePlanOfAMemberWithNoOtherState(t *testing.T) {
	store := planIdentityStore(map[int64]*storage.Plan{
		7:  {ID: 7, PlanIdentifier: "plan_reviewed"},
		42: {ID: 42, PlanIdentifier: "plan_3344"},
	})
	apply := &storage.Apply{ApplyIdentifier: "apply-1", PlanID: 7, Engine: storage.EngineSpirit}
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "primary"},
		{ID: 2, Deployment: "eu-west", PlanID: 42},
	}

	byOp := resolveDisplayByOperation(t.Context(), &planIdentityStorage{store: store}, apply, ops)

	require.Contains(t, byOp, int64(2))
	assert.Equal(t, "plan_3344", byOp[2].PlanIdentifier)
}

// The member's <details> body names the plan it runs beside the identifiers it
// already carries, so the reader does not have to hold a fourth line in view.
func TestBuildDeploymentDetail_CarriesThePlanIdentifier(t *testing.T) {
	detail := buildDeploymentDetail(
		&storage.Apply{ApplyIdentifier: "apply-1", Database: "orders", Environment: "production"},
		&storage.ApplyOperation{ID: 2, Deployment: "eu-west"},
		nil,
		operationDisplay{PlanIdentifier: "plan_3344"},
		nil,
		"",
	)

	assert.Equal(t, "plan_3344", detail.PlanID)
}
