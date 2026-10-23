package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/apitypes"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// A reviewer reads the aggregate check to decide whether a PR is ready to
// merge. For an environment of several targets the Change column names the
// targets with work, so the check answers which targets have the change
// without opening the plan comment. A plan calls them pending. An apply's row
// is kept through its completion, so it names the targets the apply runs on
// instead, which stays true once the row reads Applied.
func TestCheckRowNamesRolloutTargets(t *testing.T) {
	shops := []string{"shop-001", "shop-002", "shop-003", "shop-004"}
	someDone := memberWork{pending: 2, members: 4, names: []string{"shop-001", "shop-004"}}
	primaryDone := memberWork{pending: 2, members: 4, names: []string{"shop-003", "shop-004"}}

	for _, tc := range []struct {
		name        string
		work        memberWork
		applying    bool
		planSummary string
		want        string
	}{
		{name: "single target keeps the plan summary", work: memberWork{pending: 1, members: 1, names: shops[:1]}, planSummary: "1 alter", want: "1 alter"},
		{name: "no rollup keeps the plan summary", planSummary: "1 alter", want: "1 alter"},
		{name: "every target already has the change", work: memberWork{members: 4}, want: ""},
		{name: "plan pending on every target", work: memberWork{pending: 4, members: 4, names: shops}, planSummary: "1 alter", want: "1 alter · pending on all 4 targets"},
		{name: "plan pending on some targets", work: someDone, planSummary: "1 alter", want: "1 alter · pending on 2 of 4 targets: `shop-001`, `shop-004`"},
		{name: "plan with the primary already done", work: primaryDone, want: "2 of 4 targets need this change: `shop-003`, `shop-004`"},
		{name: "plan with one target left", work: memberWork{pending: 1, members: 4, names: []string{"shop-004"}}, want: "1 of 4 targets needs this change: `shop-004`"},
		{
			name:        "many targets are counted past the named ones",
			work:        memberWork{pending: 5, members: 6, names: []string{"shop-002", "shop-003", "shop-004", "shop-005", "shop-006"}},
			planSummary: "1 alter",
			want:        "1 alter · pending on 5 of 6 targets: `shop-002`, `shop-003`, `shop-004` and 2 more",
		},
		{name: "apply of every target", work: memberWork{pending: 4, members: 4, names: shops}, applying: true, planSummary: "1 alter", want: "1 alter · all 4 targets"},
		{name: "apply of some targets", work: someDone, applying: true, planSummary: "1 alter", want: "1 alter · 2 of 4 targets: `shop-001`, `shop-004`"},
		{name: "apply with the primary already done", work: primaryDone, applying: true, want: "2 of 4 targets: `shop-003`, `shop-004`"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outcome := reviewDriftOutcome{state: driftClean, work: tc.work, dispatchesApply: tc.applying}
			assert.Equal(t, tc.want, outcome.changeSummary(tc.planSummary))
		})
	}
}

// After `schemabot apply -e production --target shop-002`, the check row
// names shop-002 the way the plan and apply comments do: by its deployment,
// qualified with the target only when the deployment addresses several.
func TestNarrowedApplyCheckSummaryNamesTheTarget(t *testing.T) {
	h := shopRolloutHandler()

	assert.Equal(t, "rolling out `shop-002` alone; apply the rest of the environment to finish",
		narrowedApplyCheckSummary(h.narrowedTargetName("shop-002/shop", "shop", "production")))
	assert.Equal(t, "`shop-002` applied; apply the rest of the environment to finish",
		narrowedApplyCompletedSummary(h.narrowedTargetName("shop-002/shop", "shop", "production")))
	assert.Equal(t, "region-a/shop-002", h.narrowedTargetName("region-a/shop-002", "shop", "staging"),
		"a deployment addressing several targets is qualified with the target")
	assert.Equal(t, "shop-002/shop", h.narrowedTargetName("shop-002/shop", "shop", "qa"),
		"an environment that cannot be resolved names the member in full")
}

// An apply's row is kept from the moment the apply starts through its
// completion, so its Change column names the targets the apply runs on, never
// the targets a plan found pending. That holds when only the primary target
// has work, and when a plan of the whole environment rewrote the row between
// the apply command and its confirmation: the claim writes the apply's own
// summary over it. A row with nothing to say keeps what it has.
func TestApplyRowNamesTheTargetsItRuns(t *testing.T) {
	h := shopRolloutHandler()
	schema := &ghclient.SchemaRequestResult{Type: "mysql", Database: "shop"}
	alter := &apitypes.PlanResponse{Changes: []*apitypes.SchemaChangeResponse{{
		Namespace: "shop",
		TableChanges: []*apitypes.TableChangeResponse{{
			TableName: "orders", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD COLUMN `note` varchar(255)",
		}},
	}}}
	primaryOnly := memberWork{pending: 1, members: 4, names: []string{"shop-001"}, primary: "shop-001"}

	outcome := primaryPlanApplyOutcome(reviewDriftOutcome{state: driftClean, work: primaryOnly})
	assert.Equal(t, driftNotEvaluated, outcome.state, "an apply of the primary plan never clears a stored drift block")
	assert.Equal(t, "1 alter · 1 of 4 targets: `shop-001`", outcome.changeSummary("1 alter"))

	summary := h.rolloutApplyChangeSummary(schema, alter, "production", memberWork{pending: 3, members: 4, names: []string{"shop-001", "shop-003", "shop-004"}})
	assert.Equal(t, applyChangeSummary{text: "1 alter · 3 of 4 targets: `shop-001`, `shop-003`, `shop-004`", targets: 4, withWork: 3}, summary)

	replanned := &storage.Check{ChangeSummary: "1 alter · pending on 3 of 4 targets: `shop-001`, `shop-003`, `shop-004`", RolloutTargets: 4, RolloutTargetsWithWork: 3}
	summary.applyTo(replanned)
	assert.Equal(t, "1 alter · 3 of 4 targets: `shop-001`, `shop-003`, `shop-004`", replanned.ChangeSummary)
	assert.Equal(t, 4, replanned.RolloutTargets)
	assert.Equal(t, 3, replanned.RolloutTargetsWithWork)

	narrowed := &apitypes.PlanResponse{NarrowedTo: "shop-002/shop"}
	assert.Equal(t, applyChangeSummary{text: "rolling out `shop-002` alone; apply the rest of the environment to finish"},
		h.rolloutApplyChangeSummary(schema, narrowed, "production", primaryOnly))

	kept := &storage.Check{ChangeSummary: "1 alter", RolloutTargets: 0}
	applyChangeSummary{}.applyTo(kept)
	assert.Equal(t, "1 alter", kept.ChangeSummary, "an empty summary leaves the stored one")
}

// shopRolloutHandler serves a database whose production environment has two
// deployments of one target each, and whose staging environment has one
// deployment addressing two targets.
func shopRolloutHandler() *Handler {
	cfg := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			"shop": {
				Type: "mysql",
				Environments: map[string]api.EnvironmentConfig{
					"production": {Deployments: map[string]api.DeploymentTarget{
						"shop-001": {Target: "shop"},
						"shop-002": {Target: "shop"},
					}},
					"staging": {Deployments: map[string]api.DeploymentTarget{
						"region-a": {Targets: []api.TargetEntry{{Target: "shop-001"}, {Target: "shop-002"}}},
					}},
				},
			},
		},
	}
	return &Handler{service: api.New(nil, cfg, nil, testLogger()), logger: testLogger()}
}

// The aggregate title is what the PR's merge box shows next to the check name.
// When one row holds the check, the title says how many of its rollout's
// targets the work is on. Titles over several rows, and rows that name no
// rollout of several targets, keep their counts of applies.
func TestAggregateTitleNamesRolloutTargets(t *testing.T) {
	rollout := func(status, conclusion string, targets, withWork int) *storage.Check {
		return &storage.Check{
			DatabaseType: "mysql", DatabaseName: "orders", HasChanges: true, Status: status, Conclusion: conclusion,
			RolloutTargets: targets, RolloutTargetsWithWork: withWork,
		}
	}
	other := &storage.Check{DatabaseType: "mysql", DatabaseName: "inventory", HasChanges: true, Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired}

	for _, tc := range []struct {
		name   string
		checks []*storage.Check
		want   string
	}{
		{name: "pending on some targets", checks: []*storage.Check{rollout(checkStatusCompleted, checkConclusionActionRequired, 4, 3)}, want: "1 apply pending on 3 of 4 targets"},
		{name: "pending on every target", checks: []*storage.Check{rollout(checkStatusCompleted, checkConclusionActionRequired, 4, 4)}, want: "1 apply pending on all 4 targets"},
		{name: "running on some targets", checks: []*storage.Check{rollout(checkStatusInProgress, "", 4, 2)}, want: "Apply in progress on 2 of 4 targets"},
		{name: "single target", checks: []*storage.Check{rollout(checkStatusCompleted, checkConclusionActionRequired, 0, 0)}, want: "1 apply pending"},
		{name: "several databases", checks: []*storage.Check{rollout(checkStatusCompleted, checkConclusionActionRequired, 4, 3), other}, want: "2 applies pending"},
		{name: "complete", checks: []*storage.Check{rollout(checkStatusCompleted, checkConclusionSuccess, 4, 3)}, want: "All applies complete"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conclusion, _ := computeAggregate(tc.checks, nil)
			title, _ := aggregateSummary(tc.checks, conclusion, nil)
			assert.Equal(t, tc.want, title)
		})
	}
}
