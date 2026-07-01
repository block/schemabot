package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/webhook/action"
)

// Fan-out applies only to actionable unscoped commands. A complete command
// (Found) fans out; a plan without -e fans out as a multi-env plan; but a
// missing-env error for apply/rollback does NOT fan out — otherwise every
// participant on a shared repo would post its own duplicate "missing
// environment" comment (the leader posts that error once).
func TestUnscopedCommandFansOut(t *testing.T) {
	assert.True(t, unscopedCommandFansOut(CommandResult{Found: true, Action: action.Apply}),
		"a complete command fans out")
	assert.True(t, unscopedCommandFansOut(CommandResult{MissingEnv: true, Action: action.Plan}),
		"plan without -e fans out as a multi-env plan")
	assert.False(t, unscopedCommandFansOut(CommandResult{MissingEnv: true, Action: action.Apply}),
		"apply without -e is an error and must not fan out (no duplicate missing-env comments)")
	assert.False(t, unscopedCommandFansOut(CommandResult{MissingEnv: true, Action: action.Rollback}),
		"rollback without -e is an error and must not fan out")
	assert.False(t, unscopedCommandFansOut(CommandResult{}),
		"an unrecognized command does not fan out")
}

// An aggregate participant fans out an unscoped work command: an
// `apply -e <env>` with no -t tenant reaches every participant on a shared
// repo, and each participant self-selects its own databases instead of
// ignoring the command. Only per-tenant -t routing requires an explicit tenant.
func TestFansOutUnscopedCommand(t *testing.T) {
	const repo = "octocat/hello-world"

	participantCfg := &api.ServerConfig{
		Tenant: "tenant-b",
		Repos: map[string]api.RepoConfig{
			repo: {Aggregate: &api.AggregateConfig{Role: api.AggregateRoleParticipant}},
		},
	}
	leaderCfg := &api.ServerConfig{
		Tenant: "tenant-a",
		Repos: map[string]api.RepoConfig{
			repo: {Aggregate: &api.AggregateConfig{
				Role: api.AggregateRoleLeader,
				ExpectedTenants: []api.ExpectedTenant{
					{Tenant: "tenant-b", Paths: []string{"tenant-b/schema"}},
				},
			}},
		},
	}
	noAggregateCfg := &api.ServerConfig{
		Tenant: "tenant-b",
		Repos:  map[string]api.RepoConfig{repo: {}},
	}

	t.Run("participant fans out for its repo", func(t *testing.T) {
		h := &Handler{service: api.New(nil, participantCfg, nil, testLogger())}
		assert.True(t, h.fansOutUnscopedCommand(repo))
	})

	t.Run("tenanted non-participant does not fan out", func(t *testing.T) {
		h := &Handler{service: api.New(nil, noAggregateCfg, nil, testLogger())}
		assert.False(t, h.fansOutUnscopedCommand(repo))
	})

	t.Run("leader does not fan out via this path", func(t *testing.T) {
		// A leader is untenanted in practice and never hits the guard, but the
		// participant predicate must be false for it regardless.
		h := &Handler{service: api.New(nil, leaderCfg, nil, testLogger())}
		assert.False(t, h.fansOutUnscopedCommand(repo))
	})

	t.Run("unconfigured repo does not fan out", func(t *testing.T) {
		h := &Handler{service: api.New(nil, participantCfg, nil, testLogger())}
		assert.False(t, h.fansOutUnscopedCommand("octocat/other-repo"))
	})

	t.Run("no service does not fan out", func(t *testing.T) {
		h := &Handler{}
		assert.False(t, h.fansOutUnscopedCommand(repo))
	})
}

// A tenant-targeted command (-t tenant-b) routes only to the matching isolated
// deployment; a participant for a different tenant must not respond. This
// per-tenant routing is unchanged by the fan-out behavior.
func TestTenantTargetedRoutingUnchanged(t *testing.T) {
	tenantBCfg := &api.ServerConfig{Tenant: "tenant-b"}

	assert.True(t, tenantBCfg.ShouldRespondToTenant("tenant-b"),
		"the owning deployment responds to its own tenant")
	assert.False(t, tenantBCfg.ShouldRespondToTenant("tenant-c"),
		"a deployment does not respond to another tenant's -t command")
	assert.True(t, tenantBCfg.ShouldRespondToTenant(""),
		"an unscoped command is not filtered by tenant ownership")
}
