package api

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/storage"
)

// A multi-target rollout runs one deployment at a time and cuts each target
// over as its table finishes, so apply creation refuses --defer-cutover on it
// and refuses an apply that spans several deployments when one of them has
// several targets. Every other shape keeps what it ran before: a deployment
// with one target, deployments that each address one target, and several
// members on one target, whatever options they carry.
func TestRefuseUnsupportedRolloutShape(t *testing.T) {
	target := func(deployment, name string) routing.ExecutionTarget {
		return routing.ExecutionTarget{DatabaseType: storage.DatabaseTypeMySQL, Deployment: deployment, Target: name}
	}
	tests := []struct {
		name         string
		targets      []routing.ExecutionTarget
		deferCutover bool
		refusal      RolloutShapeRefusal
		message      string
	}{
		{
			name:         "one target defers cutover",
			targets:      []routing.ExecutionTarget{target("eu", "orders-001")},
			deferCutover: true,
		},
		{
			name:    "one deployment with several targets",
			targets: []routing.ExecutionTarget{target("eu", "orders-001"), target("eu", "orders-002")},
		},
		{
			name:         "one deployment with several targets defers cutover",
			targets:      []routing.ExecutionTarget{target("eu", "orders-001"), target("eu", "orders-002"), target("eu", "orders-003")},
			deferCutover: true,
			refusal:      RolloutDeferCutoverRefused,
			message:      "testapp/production rolls out to 3 targets, and --defer-cutover is not supported on an apply to more than one target; apply again without --defer-cutover, or apply one target with --target",
		},
		{
			name:         "deployments that each address one target defer cutover",
			targets:      []routing.ExecutionTarget{target("eu", "orders-eu"), target("us", "orders-us")},
			deferCutover: true,
		},
		{
			name:         "several members on one target defer cutover",
			targets:      []routing.ExecutionTarget{target("eu", "orders-001"), target("eu", "orders-001")},
			deferCutover: true,
		},
		{
			name:    "several deployments, one with several targets",
			targets: []routing.ExecutionTarget{target("eu", "orders-001"), target("eu", "orders-002"), target("us", "orders-003")},
			refusal: RolloutMultiTargetDeploymentsRefused,
			message: "testapp/production rolls out to 3 targets across 2 deployments, and an apply to more than one deployment is not supported yet when a deployment has several targets; apply one target at a time, starting with --target orders-001",
		},
		{
			name:         "several deployments, one with several targets, defers cutover",
			targets:      []routing.ExecutionTarget{target("eu", "orders-001"), target("eu", "orders-002"), target("us", "orders-003")},
			deferCutover: true,
			refusal:      RolloutMultiTargetDeploymentsRefused,
			message:      "testapp/production rolls out to 3 targets across 2 deployments, and an apply to more than one deployment is not supported yet when a deployment has several targets; apply one target at a time, starting with --target orders-001",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := RefuseUnsupportedRolloutShape("testapp", "production", tt.targets, tt.deferCutover)
			if tt.refusal == 0 {
				assert.NoError(t, err)
				return
			}
			refused, ok := errors.AsType[*RolloutShapeRefusedError](err)
			require.True(t, ok, "want a rollout shape refusal, got %v", err)
			assert.Equal(t, tt.refusal, refused.Refusal)
			assert.Equal(t, tt.message, refused.Error())
		})
	}
}

// An apply with --defer-cutover of a deployment that rolls out to two targets
// is refused as a 400 the caller can act on. Nothing is stored, and the refusal
// is counted apart from server errors.
func TestApplyHandler_MultiTargetDeferCutoverIsRefused(t *testing.T) {
	primary := primaryPlanRow("testapp-001")
	primary.Environment = "production"
	member := memberPlanRow("plan-second", "testapp-002", "plan-primary")
	svc, applies, tasks := multiTargetApplyHTTPService(primary, []*storage.Plan{member})
	outcomes := recordApplyOutcomes(t)

	status, resp := postApplyForRefusal(t, svc, `{"plan_id":"plan-primary","environment":"production","renders_rollout":true,"options":{"defer_cutover":"true"}}`)

	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, apitypes.ErrCodeInvalidRequest, resp.ErrorCode)
	assert.Equal(t, "apply rejected: testapp/production rolls out to 2 targets, and --defer-cutover is not supported on an apply to more than one target; apply again without --defer-cutover, or apply one target with --target", resp.Error)
	assert.Nil(t, applies.apply, "a refused rollout shape must not store an apply")
	assert.Empty(t, tasks.tasks)
	assert.Equal(t, map[string]int64{"rejected": 1}, outcomes.statuses(t), "a refusal is counted apart from server errors")
}

// Apply creation reads the rollout's deployments from the server config, so an
// environment whose deployments map gives one deployment two targets is
// refused at apply creation, while the plan of it is not.
func TestCreateStoredApply_MultiTargetDeploymentsAreRefused(t *testing.T) {
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"testapp": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{"production": {
					Deployments: map[string]DeploymentTarget{
						"eu": {Targets: targetNames("testapp-001", "testapp-002")},
						"us": {Target: "testapp-003"},
					},
					DeploymentOrder: []string{"eu", "us"},
				}},
			},
		},
	}
	svc := multiTargetApplyService(t, &listingPlanStore{})
	svc.config = cfg

	_, _, err := svc.createStoredApply(t.Context(), primaryPlanRow("testapp-001"), ApplyRequest{Environment: "production"}, nil, "apply-multi-deployment")

	refused, ok := errors.AsType[*RolloutShapeRefusedError](err)
	require.True(t, ok, "want a rollout shape refusal, got %v", err)
	assert.Equal(t, RolloutMultiTargetDeploymentsRefused, refused.Refusal)
	assert.Equal(t, 3, refused.Targets)
	assert.Equal(t, 2, refused.Deployments)
	assert.Equal(t, "testapp-001", refused.FirstTarget)
}

// A plan carries the refusal of the rollout's shape that holds whatever the
// apply's options, so the CLI refuses it before it prompts or locks. An apply
// across deployments, one of them with several targets, carries it; a
// deployment with several targets alone does not, since only --defer-cutover
// would refuse its apply.
func TestRolloutShapeRefusal(t *testing.T) {
	target := func(deployment, name string) routing.ExecutionTarget {
		return routing.ExecutionTarget{DatabaseType: storage.DatabaseTypeMySQL, Deployment: deployment, Target: name}
	}
	assert.Empty(t, rolloutShapeRefusal("testapp", "production", []routing.ExecutionTarget{target("eu", "orders-001"), target("eu", "orders-002")}))
	// A single-target deployment first in the rollout's order is where the
	// one-target-at-a-time apply starts, so the order the config sets holds.
	assert.Equal(t,
		"testapp/production rolls out to 3 targets across 2 deployments, and an apply to more than one deployment is not supported yet when a deployment has several targets; apply one target at a time, starting with --target orders-003",
		rolloutShapeRefusal("testapp", "production", []routing.ExecutionTarget{target("us", "orders-003"), target("eu", "orders-001"), target("eu", "orders-002")}))
	assert.Equal(t,
		"testapp/production rolls out to 3 targets across 2 deployments, and an apply to more than one deployment is not supported yet when a deployment has several targets; apply one target at a time, starting with --target orders-001",
		rolloutShapeRefusal("testapp", "production", []routing.ExecutionTarget{target("eu", "orders-001"), target("eu", "orders-002"), target("us", "orders-003")}))
}
