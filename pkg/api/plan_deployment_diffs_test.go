package api

import (
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// twoDeploymentService builds a testapp/production database that fans out to two
// deployments (eu primary, us), each backed by its own registered mock tern
// client, so PlanDeploymentDiffs can be exercised without live databases.
func twoDeploymentService(t *testing.T, eu, us *mockTernClient) *Service {
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
	return New(&mockStorage{}, cfg, map[string]tern.Client{
		"eu/production": eu,
		"us/production": us,
	}, logger)
}

func planDiffReq(t *testing.T) PlanRequest {
	t.Helper()
	return PlanRequest{
		Database:    "testapp",
		Environment: "production",
		Type:        storage.DatabaseTypeMySQL,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"testapp": {Files: map[string]string{"users.sql": "CREATE TABLE `users` (id bigint primary key)"}},
		},
	}
}

func alterUsersDiff(ddl string) *ternv1.PlanDiffResponse {
	return &ternv1.PlanDiffResponse{
		Engine: ternv1.Engine_ENGINE_SPIRIT,
		Changes: []*ternv1.SchemaChange{{
			Namespace: "testapp",
			TableChanges: []*ternv1.TableChange{{
				TableName:  "users",
				ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
				Ddl:        ddl,
				Namespace:  "testapp",
			}},
		}},
	}
}

// With the reviewed primary plan supplied, the primary member of the rollup is
// the reviewed plan itself (no redundant live-schema read of the primary), and
// only the non-primary deployments run PlanDiff. Results stay in rollout order.
func TestPlanDeploymentDiffs_PrimaryReusesReviewedPlan(t *testing.T) {
	eu := &mockTernClient{}
	us := &mockTernClient{planDiffResp: alterUsersDiff("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")}
	svc := twoDeploymentService(t, eu, us)

	primaryPlan := &ternv1.PlanResponse{
		PlanId: "plan_eu",
		Engine: ternv1.Engine_ENGINE_SPIRIT,
		Changes: []*ternv1.SchemaChange{{
			Namespace: "testapp",
			TableChanges: []*ternv1.TableChange{{
				TableName:  "users",
				ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
				Ddl:        "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
				Namespace:  "testapp",
			}},
		}},
	}

	results, err := svc.PlanDeploymentDiffs(t.Context(), planDiffReq(t), primaryPlan, "eu")
	require.NoError(t, err)
	require.Len(t, results, 2)

	assert.Equal(t, "eu", results[0].Deployment)
	assert.Nil(t, eu.planDiffReq, "primary must reuse the reviewed plan, not re-diff")
	require.NoError(t, results[0].Err)
	assert.Equal(t, primaryPlan.Changes, results[0].Changes)

	assert.Equal(t, "us", results[1].Deployment)
	require.NotNil(t, us.planDiffReq, "non-primary deployment must be diffed")
	assert.Equal(t, "testapp", us.planDiffReq.Target)
	require.NoError(t, results[1].Err)
	require.Len(t, results[1].Changes, 1)
	assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", results[1].Changes[0].TableChanges[0].Ddl)
}

// Without a reviewed plan, every deployment (including the primary) is diffed
// with PlanDiff.
func TestPlanDeploymentDiffs_DiffsAllDeploymentsWhenNoPrimary(t *testing.T) {
	eu := &mockTernClient{planDiffResp: alterUsersDiff("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")}
	us := &mockTernClient{planDiffResp: alterUsersDiff("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")}
	svc := twoDeploymentService(t, eu, us)

	results, err := svc.PlanDeploymentDiffs(t.Context(), planDiffReq(t), nil, "")
	require.NoError(t, err)
	require.Len(t, results, 2)

	require.NotNil(t, eu.planDiffReq, "primary must be diffed when no reviewed plan is supplied")
	require.NotNil(t, us.planDiffReq)
	require.NoError(t, results[0].Err)
	require.NoError(t, results[1].Err)
}

// One deployment failing to diff must not abort the rollup or hide the others:
// its error is captured per result (so the rollup can fail closed on it) while
// the healthy deployment still returns its changes.
func TestPlanDeploymentDiffs_PerDeploymentErrorIsCaptured(t *testing.T) {
	eu := &mockTernClient{planDiffResp: alterUsersDiff("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")}
	us := &mockTernClient{planDiffErr: errors.New("deployment unreachable")}
	svc := twoDeploymentService(t, eu, us)

	results, err := svc.PlanDeploymentDiffs(t.Context(), planDiffReq(t), nil, "")
	require.NoError(t, err, "a single deployment failure must not abort the rollup")
	require.Len(t, results, 2)

	require.NoError(t, results[0].Err)
	require.Len(t, results[0].Changes, 1)

	require.Error(t, results[1].Err)
	assert.Contains(t, results[1].Err.Error(), "deployment unreachable")
	assert.Nil(t, results[1].Changes)
}

// A deployment's PlanDiff can carry per-shard plans, and the deployment and
// shard axes are orthogonal: the producer iterates deployments while each
// deployment's single diff retains its full per-shard set. This guards that the
// producer copies resp.Shards through, so per-shard drift within a deployment
// stays visible to the rollup rather than being flattened away.
func TestPlanDeploymentDiffs_PreservesShards(t *testing.T) {
	usShards := &ternv1.PlanDiffResponse{
		Engine: ternv1.Engine_ENGINE_SPIRIT,
		Shards: []*ternv1.ShardPlan{
			{
				Shard:     "-80",
				Namespace: "testapp",
				Changes: []*ternv1.TableChange{{
					TableName:  "users",
					ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
					Ddl:        "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
					Namespace:  "testapp",
				}},
			},
			{
				Shard:     "80-",
				Namespace: "testapp",
				Changes: []*ternv1.TableChange{{
					TableName:  "users",
					ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
					Ddl:        "ALTER TABLE `users` ADD COLUMN `phone` varchar(32)",
					Namespace:  "testapp",
				}},
			},
		},
	}
	eu := &mockTernClient{}
	us := &mockTernClient{planDiffResp: usShards}
	svc := twoDeploymentService(t, eu, us)

	primaryPlan := &ternv1.PlanResponse{
		PlanId: "plan_eu",
		Engine: ternv1.Engine_ENGINE_SPIRIT,
		Shards: []*ternv1.ShardPlan{{
			Shard:     "0",
			Namespace: "testapp",
			Changes: []*ternv1.TableChange{{
				TableName:  "users",
				ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
				Ddl:        "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
				Namespace:  "testapp",
			}},
		}},
	}

	results, err := svc.PlanDeploymentDiffs(t.Context(), planDiffReq(t), primaryPlan, "eu")
	require.NoError(t, err)
	require.Len(t, results, 2)

	// The primary reuses the reviewed plan's shard set verbatim.
	require.NoError(t, results[0].Err)
	assert.Equal(t, primaryPlan.Shards, results[0].Shards)

	// The non-primary deployment's full two-shard set survives the producer.
	require.NoError(t, results[1].Err)
	require.Len(t, results[1].Shards, 2)
	assert.Equal(t, "-80", results[1].Shards[0].Shard)
	assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", results[1].Shards[0].Changes[0].Ddl)
	assert.Equal(t, "80-", results[1].Shards[1].Shard)
	assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `phone` varchar(32)", results[1].Shards[1].Changes[0].Ddl)
}

// If the reviewed plan's origin deployment no longer matches rollout index 0
// (e.g. deployment_order changed between plan and rollup), reusing that plan as
// the primary baseline would compare deployments against a plan built for a
// different deployment. The producer must fail closed instead.
func TestPlanDeploymentDiffs_PrimaryDeploymentMismatchFailsClosed(t *testing.T) {
	eu := &mockTernClient{}
	us := &mockTernClient{planDiffResp: alterUsersDiff("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")}
	svc := twoDeploymentService(t, eu, us)

	primaryPlan := &ternv1.PlanResponse{PlanId: "plan_eu", Engine: ternv1.Engine_ENGINE_SPIRIT}

	// targets[0] is "eu", but the reviewed plan was created against "us".
	_, err := svc.PlanDeploymentDiffs(t.Context(), planDiffReq(t), primaryPlan, "us")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "primary invariant violated")
	assert.Nil(t, eu.planDiffReq, "must fail before diffing any deployment")
	assert.Nil(t, us.planDiffReq)
}

// A reviewed plan supplied without its origin deployment cannot be verified
// against rollout index 0, so the producer fails closed rather than trusting
// the positional assumption blindly.
func TestPlanDeploymentDiffs_PrimaryPlanWithoutOriginFailsClosed(t *testing.T) {
	eu := &mockTernClient{}
	us := &mockTernClient{planDiffResp: alterUsersDiff("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")}
	svc := twoDeploymentService(t, eu, us)

	primaryPlan := &ternv1.PlanResponse{PlanId: "plan_eu", Engine: ternv1.Engine_ENGINE_SPIRIT}

	_, err := svc.PlanDeploymentDiffs(t.Context(), planDiffReq(t), primaryPlan, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no origin deployment")
}

// A request-level failure (the database/environment resolves no deployments)
// returns an error rather than a per-deployment result.
func TestPlanDeploymentDiffs_UnresolvedTargetsError(t *testing.T) {
	svc := twoDeploymentService(t, &mockTernClient{}, &mockTernClient{})

	_, err := svc.PlanDeploymentDiffs(t.Context(), PlanRequest{
		Database:    "testapp",
		Environment: "staging", // not configured
		Type:        storage.DatabaseTypeMySQL,
	}, nil, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolve deployment targets")
}
