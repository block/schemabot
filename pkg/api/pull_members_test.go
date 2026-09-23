package api

import (
	"errors"
	"log/slog"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

const (
	pullUsersDDL      = "CREATE TABLE `users` (`id` bigint NOT NULL, PRIMARY KEY (`id`));\n"
	pullUsersEmailDDL = "CREATE TABLE `users` (`id` bigint NOT NULL, `email` varchar(255) DEFAULT NULL, PRIMARY KEY (`id`));\n"
	pullAuditsDDL     = "CREATE TABLE `audits` (`id` bigint NOT NULL, PRIMARY KEY (`id`));\n"
)

// pulledTables builds a one-namespace pull response holding the given tables.
func pulledTables(tables map[string]string) *ternv1.PullSchemaResponse {
	return &ternv1.PullSchemaResponse{
		Database:    "testapp",
		Type:        storage.DatabaseTypeMySQL,
		Environment: "production",
		Namespaces:  map[string]*ternv1.PulledNamespace{"testapp": {Tables: tables}},
		TableCount:  int32(len(tables)),
	}
}

// pullTargetService wires one Tern client per deployment for a database whose
// production environment routes to env.
func pullTargetService(t *testing.T, env EnvironmentConfig, clients map[string]tern.Client) *Service {
	t.Helper()
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"testapp": {
				Type:         storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{"production": env},
			},
		},
		TernDeployments: TernConfig{
			"eu": {"production": "eu.example.com:80"},
			"us": {"production": "us.example.com:80"},
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorageWithApplyStores{plans: &staticPlanStore{}, applies: &staticApplyStore{}}, cfg, clients, logger)
}

func pullRequest() apitypes.PullSchemaRequest {
	return apitypes.PullSchemaRequest{Database: "testapp", Environment: "production", Type: storage.DatabaseTypeMySQL}
}

// perTargetPullClient answers each target with its own schema, so a fan-out that
// pulled only one target cannot look like it pulled all of them.
type perTargetPullClient struct {
	mockTernClient
	byTarget map[string]*ternv1.PullSchemaResponse
	errs     map[string]error
}

func newPerTargetPullClient(byTarget map[string]*ternv1.PullSchemaResponse, errs map[string]error) *perTargetPullClient {
	c := &perTargetPullClient{byTarget: byTarget, errs: errs}
	c.pullSchemaHook = func(req *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
		if err, ok := c.errs[req.Target]; ok {
			return nil, err
		}
		resp, ok := c.byTarget[req.Target]
		if !ok {
			return nil, errors.New("no schema seeded for target " + req.Target)
		}
		return resp, nil
	}
	return c
}

// pulledTargets returns the targets a fan-out actually pulled, sorted. Members
// are pulled concurrently, so the order they arrive in is not a property worth
// asserting — which targets were read is.
func (c *perTargetPullClient) pulledTargets() []string {
	c.pullSchemaMu.Lock()
	defer c.pullSchemaMu.Unlock()
	out := make([]string, 0, len(c.pullSchemaReqs))
	for _, req := range c.pullSchemaReqs {
		out = append(out, req.Target)
	}
	sort.Strings(out)
	return out
}

func multiTargetPullEnv() EnvironmentConfig {
	return EnvironmentConfig{Deployment: "eu", Targets: []string{"testapp-001", "testapp-002"}}
}

// An environment whose targets each hold their own schema has no single live
// schema. A pull returns the primary's schema, which is what a caller
// materializes, plus how each other target differs from it.
func TestExecutePullSchema_ReportsPerTargetDivergence(t *testing.T) {
	client := newPerTargetPullClient(map[string]*ternv1.PullSchemaResponse{
		"testapp-001": pulledTables(map[string]string{"users": pullUsersDDL}),
		"testapp-002": pulledTables(map[string]string{"users": pullUsersEmailDDL, "audits": pullAuditsDDL}),
	}, nil)
	svc := pullTargetService(t, multiTargetPullEnv(), map[string]tern.Client{"eu/production": client})

	resp, err := svc.ExecutePullSchema(t.Context(), pullRequest())
	require.NoError(t, err)

	assert.Equal(t, pullUsersDDL, resp.Namespaces["testapp"].Tables["users"], "the response body is the primary's schema")
	assert.Equal(t, []string{"testapp-001", "testapp-002"}, client.pulledTargets(), "every target is pulled")

	require.Len(t, resp.Targets, 2, "every target the environment addresses is named, primary included")

	primary := resp.Targets[0]
	assert.True(t, primary.Primary)
	assert.Equal(t, "eu", primary.Deployment)
	assert.Equal(t, "testapp-001", primary.Target)
	assert.Equal(t, int32(1), primary.TableCount)
	assert.Empty(t, primary.DivergedTables, "the primary is the baseline and never diverges from itself")

	diverged := resp.Targets[1]
	assert.False(t, diverged.Primary)
	assert.Equal(t, "eu", diverged.Deployment)
	assert.Equal(t, "testapp-002", diverged.Target)
	assert.Equal(t, int32(2), diverged.TableCount)
	assert.Equal(t, []apitypes.DivergedTable{
		{Namespace: "testapp", Table: "audits", Difference: apitypes.DivergenceOnlyOnTarget},
		{Namespace: "testapp", Table: "users", Difference: apitypes.DivergenceDiffers},
	}, diverged.DivergedTables)
}

// A table the primary holds and another target does not is the opposite
// one-sided case from an extra table, and the two must never be confused: one
// says the target is behind the schema being materialized, the other says it
// carries something the primary does not.
func TestExecutePullSchema_ReportsTablesMissingFromATarget(t *testing.T) {
	client := newPerTargetPullClient(map[string]*ternv1.PullSchemaResponse{
		"testapp-001": pulledTables(map[string]string{"users": pullUsersDDL, "audits": pullAuditsDDL}),
		"testapp-002": pulledTables(map[string]string{"users": pullUsersDDL}),
	}, nil)
	svc := pullTargetService(t, multiTargetPullEnv(), map[string]tern.Client{"eu/production": client})

	resp, err := svc.ExecutePullSchema(t.Context(), pullRequest())
	require.NoError(t, err)

	require.Len(t, resp.Targets, 2)
	diverged := resp.Targets[1]
	assert.Equal(t, "testapp-002", diverged.Target)
	assert.Equal(t, int32(1), diverged.TableCount)
	assert.Equal(t, []apitypes.DivergedTable{
		{Namespace: "testapp", Table: "audits", Difference: apitypes.DivergenceOnlyOnPrimary},
	}, diverged.DivergedTables)
}

// A caller reconciling an environment against its own shard inventory reads the
// member set off the pull payload, so exactly one target is marked primary and
// the whole configured list is present in configuration order.
func TestExecutePullSchema_NamesEveryTargetExactlyOncePrimaryFirst(t *testing.T) {
	client := newPerTargetPullClient(map[string]*ternv1.PullSchemaResponse{
		"testapp-001": pulledTables(map[string]string{"users": pullUsersDDL}),
		"testapp-002": pulledTables(map[string]string{"users": pullUsersDDL}),
		"testapp-003": pulledTables(map[string]string{"users": pullUsersDDL}),
	}, nil)
	env := EnvironmentConfig{Deployment: "eu", Targets: []string{"testapp-001", "testapp-002", "testapp-003"}}
	svc := pullTargetService(t, env, map[string]tern.Client{"eu/production": client})

	resp, err := svc.ExecutePullSchema(t.Context(), pullRequest())
	require.NoError(t, err)

	named := make([]string, 0, len(resp.Targets))
	primaries := 0
	for _, target := range resp.Targets {
		named = append(named, target.Target)
		if target.Primary {
			primaries++
		}
	}
	assert.Equal(t, []string{"testapp-001", "testapp-002", "testapp-003"}, named)
	assert.Equal(t, 1, primaries, "exactly one target is the baseline the others are compared against")
	assert.True(t, resp.Targets[0].Primary)
}

// Targets that hold the same schema report no diverged tables, which is a
// positive statement that they agree rather than an absence of information.
func TestExecutePullSchema_ConvergedTargetsReportNoDivergedTables(t *testing.T) {
	client := newPerTargetPullClient(map[string]*ternv1.PullSchemaResponse{
		"testapp-001": pulledTables(map[string]string{"users": pullUsersDDL}),
		"testapp-002": pulledTables(map[string]string{"users": pullUsersDDL}),
	}, nil)
	svc := pullTargetService(t, multiTargetPullEnv(), map[string]tern.Client{"eu/production": client})

	resp, err := svc.ExecutePullSchema(t.Context(), pullRequest())
	require.NoError(t, err)

	require.Len(t, resp.Targets, 2)
	assert.Equal(t, "testapp-002", resp.Targets[1].Target)
	assert.Empty(t, resp.Targets[1].DivergedTables)
}

// Two targets holding the same schema written differently are not diverged:
// tables are compared by the parser's canonical form, so formatting never
// reads as a schema difference.
func TestExecutePullSchema_FormattingIsNotDivergence(t *testing.T) {
	client := newPerTargetPullClient(map[string]*ternv1.PullSchemaResponse{
		"testapp-001": pulledTables(map[string]string{"users": pullUsersDDL}),
		"testapp-002": pulledTables(map[string]string{
			"users": "create table `users` (\n  `id` BIGINT not null,\n  primary key (`id`)\n)",
		}),
	}, nil)
	svc := pullTargetService(t, multiTargetPullEnv(), map[string]tern.Client{"eu/production": client})

	resp, err := svc.ExecutePullSchema(t.Context(), pullRequest())
	require.NoError(t, err)

	require.Len(t, resp.Targets, 2)
	assert.Empty(t, resp.Targets[1].DivergedTables, "the same schema written differently is the same schema")
}

// Canonicalization re-renders the parsed tree in the order it was written, so
// two targets holding the same indexes in a different order are reported as
// diverged. The comparison over-reports rather than under-reports: an operator
// is sent to look at a table that turns out to agree, which is the safe
// direction for a report whose whole purpose is to surface differences.
func TestExecutePullSchema_ClauseOrderReadsAsDivergence(t *testing.T) {
	client := newPerTargetPullClient(map[string]*ternv1.PullSchemaResponse{
		"testapp-001": pulledTables(map[string]string{
			"users": "CREATE TABLE `users` (`id` bigint NOT NULL, `a` int, `b` int, PRIMARY KEY (`id`), KEY `ka` (`a`), KEY `kb` (`b`));",
		}),
		"testapp-002": pulledTables(map[string]string{
			"users": "CREATE TABLE `users` (`id` bigint NOT NULL, `a` int, `b` int, PRIMARY KEY (`id`), KEY `kb` (`b`), KEY `ka` (`a`));",
		}),
	}, nil)
	svc := pullTargetService(t, multiTargetPullEnv(), map[string]tern.Client{"eu/production": client})

	resp, err := svc.ExecutePullSchema(t.Context(), pullRequest())
	require.NoError(t, err)

	require.Len(t, resp.Targets, 2)
	require.Len(t, resp.Targets[1].DivergedTables, 1)
	assert.Equal(t, "users", resp.Targets[1].DivergedTables[0].Table)
	assert.Equal(t, "differs", resp.Targets[1].DivergedTables[0].Difference)
}

// The primary is resolved by the caller and the member set is resolved again
// here, so a config reloaded between the two reads could drop the primary out
// of the environment. Reporting the members anyway would return a set with
// nothing marked primary, describing an environment that exists in neither
// config, so the pull fails instead.
// Exercised against pullMemberDivergence directly: the mismatch it guards
// against needs the config to move between the caller's resolution and this
// one, which a single call through ExecutePullSchema cannot stage, since both
// reads would see the same moved config and agree with each other.
func TestPullMemberDivergence_PrimaryMissingFromMembersFailsThePull(t *testing.T) {
	client := newPerTargetPullClient(map[string]*ternv1.PullSchemaResponse{
		"testapp-001": pulledTables(map[string]string{"users": pullUsersDDL}),
		"testapp-002": pulledTables(map[string]string{"users": pullUsersDDL}),
	}, nil)
	svc := pullTargetService(t, multiTargetPullEnv(), map[string]tern.Client{"eu/production": client})

	// The primary the schema was pulled from is not among the members this call
	// resolves, the shape a config reloaded mid-pull would leave behind.
	stalePrimary := routing.ExecutionTarget{
		Deployment:   "eu",
		Target:       "testapp-retired",
		DatabaseType: storage.DatabaseTypeMySQL,
	}

	_, err := svc.pullMemberDivergence(t.Context(), pullRequest(), stalePrimary,
		pulledTables(map[string]string{"users": pullUsersDDL}), []string{"testapp"},
		ternv1.PullCatalogDetail_PULL_CATALOG_DETAIL_BASIC)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "do not include the primary eu/testapp-retired")
	assert.Contains(t, err.Error(), "eu/testapp-001, eu/testapp-002", "the error names the members it did resolve")
}

// A target that cannot be pulled fails the request. Returning the primary's
// schema with that target simply absent would report the environment as
// converged on the strength of a comparison that never happened.
func TestExecutePullSchema_UnreachableTargetFailsThePull(t *testing.T) {
	client := newPerTargetPullClient(
		map[string]*ternv1.PullSchemaResponse{"testapp-001": pulledTables(map[string]string{"users": pullUsersDDL})},
		map[string]error{"testapp-002": errors.New("target unreachable")},
	)
	svc := pullTargetService(t, multiTargetPullEnv(), map[string]tern.Client{"eu/production": client})

	_, err := svc.ExecutePullSchema(t.Context(), pullRequest())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pull rollout member eu/testapp-002")
	assert.Contains(t, err.Error(), "target unreachable")
}

// A pulled table whose DDL is not a CREATE TABLE cannot be compared, so the
// pull fails rather than dropping that table out of the comparison silently.
func TestExecutePullSchema_UncomparableTableFailsThePull(t *testing.T) {
	client := newPerTargetPullClient(map[string]*ternv1.PullSchemaResponse{
		"testapp-001": pulledTables(map[string]string{"users": pullUsersDDL}),
		"testapp-002": pulledTables(map[string]string{"users": "SELECT 1"}),
	}, nil)
	svc := pullTargetService(t, multiTargetPullEnv(), map[string]tern.Client{"eu/production": client})

	_, err := svc.ExecutePullSchema(t.Context(), pullRequest())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "eu/testapp-002")
	assert.Contains(t, err.Error(), "users")
}

// Deployments that are expected to hold the same schema are not compared: a
// difference between them is drift for the review-time rollup to block on, not
// divergence for a pull to describe. Pulling them would cost a round trip per
// deployment to learn what the configuration already asserts.
func TestExecutePullSchema_MirroredDeploymentsAreNotPulledOrCompared(t *testing.T) {
	eu := newPerTargetPullClient(map[string]*ternv1.PullSchemaResponse{"testapp": pulledTables(map[string]string{"users": pullUsersDDL})}, nil)
	us := newPerTargetPullClient(nil, map[string]error{"testapp": errors.New("a mirrored deployment must not be pulled")})
	env := EnvironmentConfig{
		Deployments:     map[string]DeploymentTarget{"eu": {Target: "testapp"}, "us": {Target: "testapp"}},
		DeploymentOrder: []string{"eu", "us"},
	}
	svc := pullTargetService(t, env, map[string]tern.Client{"eu/production": eu, "us/production": us})

	resp, err := svc.ExecutePullSchema(t.Context(), pullRequest())
	require.NoError(t, err)

	assert.Empty(t, resp.Targets, "an environment whose deployments should match reports no per-target divergence")
	assert.Equal(t, []string{"testapp"}, eu.pulledTargets(), "only the primary deployment is pulled")
	assert.Empty(t, us.pulledTargets())
}
