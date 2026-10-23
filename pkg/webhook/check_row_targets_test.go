package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/api"
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
	h := &Handler{service: api.New(nil, cfg, nil, testLogger()), logger: testLogger()}

	assert.Equal(t, "rolling out `shop-002` alone; apply the rest of the environment to finish",
		narrowedApplyCheckSummary(h.narrowedTargetName("shop-002/shop", "shop", "production")))
	assert.Equal(t, "`shop-002` applied; apply the rest of the environment to finish",
		narrowedApplyCompletedSummary(h.narrowedTargetName("shop-002/shop", "shop", "production")))
	assert.Equal(t, "region-a/shop-002", h.narrowedTargetName("region-a/shop-002", "shop", "staging"),
		"a deployment addressing several targets is qualified with the target")
	assert.Equal(t, "shop-002/shop", h.narrowedTargetName("shop-002/shop", "shop", "qa"),
		"an environment that cannot be resolved names the member in full")
}
