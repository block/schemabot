package api

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// recordingPlanStore captures every plan row a rollup stores, so a test can
// assert both how many member plans were persisted and what each one holds.
type recordingPlanStore struct {
	mockPlanLookupStore
	created   []*storage.Plan
	createErr error
}

func (s *recordingPlanStore) Create(_ context.Context, plan *storage.Plan) (int64, error) {
	if s.createErr != nil {
		return 0, s.createErr
	}
	s.created = append(s.created, plan)
	return int64(len(s.created)), nil
}

// multiTargetService builds a database whose production environment addresses
// two distinct targets through one deployment, which is the shape that makes its
// members independently planned.
func multiTargetService(t *testing.T, client *mockTernClient, plans storage.PlanStore) *Service {
	t.Helper()
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"testapp": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"production": {
						Deployment: "eu",
						Targets:    []string{"testapp-001", "testapp-002"},
					},
				},
			},
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorageWithPlanLookup{plans: plans}, cfg, map[string]tern.Client{
		"eu/production": client,
	}, logger)
}

// mirroredService builds the two-deployment, one-target shape whose members are
// expected to hold the same schema, wired to a plan store so a test can assert
// no per-member plan is written for it.
func mirroredService(t *testing.T, eu, us *mockTernClient, plans storage.PlanStore) *Service {
	t.Helper()
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"testapp": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"production": {
						Deployments: map[string]DeploymentTarget{
							"eu": {Target: "testapp"},
							"us": {Target: "testapp"},
						},
						DeploymentOrder: []string{"eu", "us"},
					},
				},
			},
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorageWithPlanLookup{plans: plans}, cfg, map[string]tern.Client{
		"eu/production": eu,
		"us/production": us,
	}, logger)
}

func multiTargetMember(target string) routing.ExecutionTarget {
	return routing.ExecutionTarget{Deployment: "eu", Target: target}
}

// Each target of a multi-target environment holds its own schema, so the second
// target legitimately plans different DDL than the reviewed primary. The review
// stays clean and the second target's plan is persisted as a row of its own, so
// an apply has something to run for it.
func TestRollupReviewTimeDrift_IndependentMemberPlanIsPersisted(t *testing.T) {
	reviewed := reviewedUsersPlan("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	secondDDL := "ALTER TABLE `users` ADD COLUMN `phone` varchar(32)"
	plans := &recordingPlanStore{}
	svc := multiTargetService(t, &mockTernClient{planDiffResp: alterUsersDiff(secondDDL)}, plans)

	rollup, err := svc.RollupReviewTimeDrift(t.Context(), planDiffReq(t), reviewed, multiTargetMember("testapp-001"))
	require.NoError(t, err)
	assert.True(t, rollup.Clean, "targets that hold their own schemas must not block each other")
	require.Len(t, rollup.Entries, 2)

	assert.Equal(t, DeploymentPlanned, rollup.Entries[0].Class)
	assert.Empty(t, rollup.Entries[0].PlanIdentifier, "the primary runs the plan its apply is created from")

	assert.Equal(t, "testapp-002", rollup.Entries[1].Target)
	assert.Equal(t, DeploymentPlanned, rollup.Entries[1].Class)
	require.NotEmpty(t, rollup.Entries[1].PlanIdentifier)
	assert.True(t, strings.HasPrefix(rollup.Entries[1].PlanIdentifier, "plan-"),
		"member plan identifier %q should be a minted plan id", rollup.Entries[1].PlanIdentifier)

	require.Len(t, plans.created, 1, "only the non-primary member needs a plan row of its own")
	stored := plans.created[0]
	assert.Equal(t, rollup.Entries[1].PlanIdentifier, stored.PlanIdentifier)
	assert.Equal(t, "testapp", stored.Database)
	assert.Equal(t, "eu", stored.Deployment)
	assert.Equal(t, "testapp-002", stored.Target)
	assert.Equal(t, "production", stored.Environment)
	require.Contains(t, stored.Namespaces, "testapp")
	require.Len(t, stored.Namespaces["testapp"].Tables, 1)
	assert.Equal(t, "users", stored.Namespaces["testapp"].Tables[0].Table)
	assert.Contains(t, stored.Namespaces["testapp"].Tables[0].DDL, "ADD COLUMN `phone`")
	assert.Equal(t, reviewed.PlanId, stored.PrimaryPlanIdentifier,
		"a member's plan is bound to the reviewed plan it was produced alongside")
}

// A commit can be planned more than once, and each round stores its own plan per
// member with the same route and the same head SHA. The reviewed plan's
// identifier is what tells the rounds apart, so a member plan that cannot be
// attributed to one is not stored at all: the member then has no plan for the
// round and blocks the review, rather than being paired with a plan from a round
// the operator never saw.
func TestRollupReviewTimeDrift_MemberPlanNeedsAReviewRoundToBindTo(t *testing.T) {
	reviewed := reviewedUsersPlan("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	reviewed.PlanId = ""
	plans := &recordingPlanStore{}
	svc := multiTargetService(t, &mockTernClient{planDiffResp: alterUsersDiff("ALTER TABLE `users` ADD COLUMN `phone` varchar(32)")}, plans)

	_, err := svc.RollupReviewTimeDrift(t.Context(), planDiffReq(t), reviewed, multiTargetMember("testapp-001"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no identifier to bind member plans to")
	assert.Empty(t, plans.created)
}

// A namespace the caller withheld per the config's ignore_namespaces is
// excluded from the desired state, not absent from it. A member planned against
// its own live schema is planned from the same partial desired state as the
// primary, or the data plane reads the omission as intent to remove and the
// member's stored plan carries DROPs for namespaces the configuration excludes.
func TestRollupReviewTimeDrift_MemberDiffCarriesTheRequestContract(t *testing.T) {
	client := &mockTernClient{planDiffResp: alterUsersDiff("ALTER TABLE `users` ADD COLUMN `phone` varchar(32)")}
	svc := multiTargetService(t, client, &recordingPlanStore{})

	req := planDiffReq(t)
	req.IgnoredNamespaces = []string{"legacy_reports"}
	req.GroupedExecution = true

	_, err := svc.RollupReviewTimeDrift(t.Context(), req, reviewedUsersPlan("ALTER TABLE `users` ADD COLUMN `email` varchar(255)"), multiTargetMember("testapp-001"))
	require.NoError(t, err)

	require.NotNil(t, client.planDiffReq)
	assert.Equal(t, []string{"legacy_reports"}, client.planDiffReq.IgnoredNamespaces)
	require.NotNil(t, client.planDiffReq.GroupedExecution, "the grouping choice is always stated, never left absent")
	assert.True(t, *client.planDiffReq.GroupedExecution)
}

// The execution-mode verdict crosses the wire as free-form text and everything
// except "blocked" runs on the engine's default path. A member's plan reaches
// SchemaBot from the diff RPC rather than the plan RPC, so the vocabulary is
// enforced where that diff arrives: an unrecognized verdict is persisted blocked
// rather than applyable, and the review reports the refusal it stored. A member
// planned against its own schema is the only place its refused DDL surfaces at
// review, so a count taken before the verdict was settled would tell the
// operator the plan runs and then store one that cannot.
func TestRollupReviewTimeDrift_MemberPlanBlocksUnrecognizedVerdict(t *testing.T) {
	diff := alterUsersDiff("ALTER TABLE `users` ADD COLUMN `phone` varchar(32)")
	diff.Changes[0].TableChanges[0].ExecutionMode = "future-mode"
	diff.Changes[0].TableChanges[0].ModeReason = "untrusted planner reason"
	plans := &recordingPlanStore{}
	svc := multiTargetService(t, &mockTernClient{planDiffResp: diff}, plans)

	rollup, err := svc.RollupReviewTimeDrift(t.Context(), planDiffReq(t), reviewedUsersPlan("ALTER TABLE `users` ADD COLUMN `email` varchar(255)"), multiTargetMember("testapp-001"))
	require.NoError(t, err)
	require.Len(t, rollup.Entries, 2)
	assert.Equal(t, 1, rollup.Entries[1].Blocked,
		"the review must publish the refusal it stores, not the verdict the planner sent")

	require.Len(t, plans.created, 1)
	table := plans.created[0].Namespaces["testapp"].Tables[0]
	assert.Equal(t, engine.ExecutionModeBlocked, table.ExecutionMode)
	assert.Contains(t, table.ModeReason, `"future-mode"`)
	assert.NotContains(t, table.ModeReason, "untrusted planner reason")
}

// A member whose plan cannot be stored has nothing an apply could run, so it is
// reclassified as errored and blocks the review rather than passing on a plan
// that was never persisted.
func TestRollupReviewTimeDrift_MemberPlanStoreFailureBlocks(t *testing.T) {
	reviewed := reviewedUsersPlan("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	plans := &recordingPlanStore{createErr: errors.New("storage unavailable")}
	svc := multiTargetService(t, &mockTernClient{planDiffResp: alterUsersDiff("ALTER TABLE `users` ADD COLUMN `phone` varchar(32)")}, plans)

	rollup, err := svc.RollupReviewTimeDrift(t.Context(), planDiffReq(t), reviewed, multiTargetMember("testapp-001"))
	require.NoError(t, err)
	assert.False(t, rollup.Clean, "a member whose plan was not stored must block the review")
	require.Len(t, rollup.Entries, 2)
	assert.Equal(t, DeploymentErrored, rollup.Entries[1].Class)
	assert.Empty(t, rollup.Entries[1].PlanIdentifier)
	require.Error(t, rollup.Entries[1].Err)
	assert.Contains(t, rollup.Entries[1].Err.Error(), "eu/testapp-002")
	assert.Contains(t, rollup.Entries[1].Err.Error(), "storage unavailable")
}

// A member that could not be planned at all never reaches plan storage: it
// already blocks, and there is no plan to write.
func TestRollupReviewTimeDrift_UnplannableMemberStoresNothing(t *testing.T) {
	reviewed := reviewedUsersPlan("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
	plans := &recordingPlanStore{}
	svc := multiTargetService(t, &mockTernClient{planDiffErr: errors.New("target unreachable")}, plans)

	rollup, err := svc.RollupReviewTimeDrift(t.Context(), planDiffReq(t), reviewed, multiTargetMember("testapp-001"))
	require.NoError(t, err)
	assert.False(t, rollup.Clean)
	require.Len(t, rollup.Entries, 2)
	assert.Equal(t, DeploymentErrored, rollup.Entries[1].Class)
	assert.Empty(t, plans.created, "an unplannable member has no plan to store")
}

// Deployments that are expected to hold the same schema all run the reviewed
// plan, so no second plan row is written for them.
func TestRollupReviewTimeDrift_MirroredMembersStoreNoMemberPlan(t *testing.T) {
	ddl := "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
	plans := &recordingPlanStore{}
	svc := mirroredService(t, &mockTernClient{}, &mockTernClient{planDiffResp: alterUsersDiff(ddl)}, plans)

	rollup, err := svc.RollupReviewTimeDrift(t.Context(), planDiffReq(t), reviewedUsersPlan(ddl), productionMember("eu"))
	require.NoError(t, err)
	assert.True(t, rollup.Clean)
	require.Len(t, rollup.Entries, 2)
	assert.Equal(t, DeploymentMatch, rollup.Entries[1].Class)
	assert.Empty(t, rollup.Entries[1].PlanIdentifier)
	assert.Empty(t, plans.created, "mirrored members run the reviewed plan")
}
