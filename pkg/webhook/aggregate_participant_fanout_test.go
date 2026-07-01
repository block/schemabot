package webhook

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/webhook/action"
)

// On an aggregate repo, an unscoped fan-out command that resolves to schema this
// deployment doesn't own is a silent no-op — the deployment that does own it
// handles the command, so no "config not authorized" comment is posted. This is
// the leader's behavior on a PR that touches only a participant's database
// (e.g. kgoose): it gates on the participant's check but applies nothing itself.
// A -t-scoped command, a non-aggregate repo, or a real error still surfaces.
func TestSkipUnownedUnscopedCommand(t *testing.T) {
	cfg := &api.ServerConfig{Repos: map[string]api.RepoConfig{
		"octocat/participant-repo": {Aggregate: &api.AggregateConfig{Role: api.AggregateRoleParticipant}},
		"octocat/leader-repo": {Aggregate: &api.AggregateConfig{
			Role:            api.AggregateRoleLeader,
			ExpectedTenants: []api.ExpectedTenant{{Tenant: "tenant-b", Paths: []string{"tenant-b/schema"}, CheckName: "SchemaBot Tenant B"}},
		}},
		"octocat/plain-repo": {},
	}}
	h := &Handler{service: api.New(nil, cfg, nil, testLogger())}
	notOwned := &schemaConfigOutsideAllowedDirsError{Database: "kgoose", SchemaPath: "kgoose/schema"}

	assert.True(t, h.skipUnownedUnscopedCommand("octocat/participant-repo", "", notOwned),
		"a participant silently skips an unowned unscoped command")
	assert.True(t, h.skipUnownedUnscopedCommand("octocat/leader-repo", "", notOwned),
		"a leader silently skips schema it doesn't own (gates on the participant instead)")
	assert.False(t, h.skipUnownedUnscopedCommand("octocat/participant-repo", "tenant-b", notOwned),
		"a -t-scoped command named a deployment, so the error still surfaces")
	assert.False(t, h.skipUnownedUnscopedCommand("octocat/plain-repo", "", notOwned),
		"a non-aggregate repo is a single deployment — the error is useful, keep it")
	assert.False(t, h.skipUnownedUnscopedCommand("octocat/unknown-repo", "", notOwned),
		"an unconfigured repo has no aggregate role — keep the error")
	assert.False(t, h.skipUnownedUnscopedCommand("octocat/participant-repo", "", errors.New("github unavailable")),
		"a real error is not the not-owned case and must still surface")
}

// On an aggregate repo an unscoped command fans out to every deployment, so a
// deployment that finds no pending work (e.g. apply-confirm after its own
// databases already auto-applied) stays silent instead of posting noise. A
// -t-scoped command named a specific deployment, so its "nothing to do" answer
// still surfaces; a non-aggregate repo is a single deployment whose answer is
// useful too.
func TestSilentOnUnscopedFanOut(t *testing.T) {
	cfg := &api.ServerConfig{Repos: map[string]api.RepoConfig{
		"octocat/participant-repo": {Aggregate: &api.AggregateConfig{Role: api.AggregateRoleParticipant}},
		"octocat/leader-repo": {Aggregate: &api.AggregateConfig{
			Role:            api.AggregateRoleLeader,
			ExpectedTenants: []api.ExpectedTenant{{Tenant: "tenant-b", Paths: []string{"tenant-b/schema"}, CheckName: "SchemaBot Tenant B"}},
		}},
		"octocat/plain-repo": {},
	}}
	h := &Handler{service: api.New(nil, cfg, nil, testLogger())}

	assert.True(t, h.silentOnUnscopedFanOut("octocat/participant-repo", ""),
		"a participant with nothing pending stays silent on an unscoped fan-out")
	assert.True(t, h.silentOnUnscopedFanOut("octocat/leader-repo", ""),
		"a leader with nothing pending stays silent on an unscoped fan-out")
	assert.False(t, h.silentOnUnscopedFanOut("octocat/participant-repo", "tenant-b"),
		"a -t-scoped command named a deployment, so its nothing-to-do answer surfaces")
	assert.False(t, h.silentOnUnscopedFanOut("octocat/plain-repo", ""),
		"a non-aggregate repo is a single deployment — its answer is useful")
	assert.False(t, h.silentOnUnscopedFanOut("octocat/unknown-repo", ""),
		"an unconfigured repo has no aggregate role — its answer is useful")
}

// Fan-out applies only to unscoped commands a participant can serve on its own
// databases: plan, apply, apply-confirm, and unlock. A complete command (Found)
// fans out; a plan without -e fans out as a multi-env plan; but a missing-env
// apply does NOT fan out — otherwise every participant on a shared repo would
// post its own duplicate "missing environment" comment (the leader posts that
// error once). Commands that target a single apply owned by one tenant —
// rollback and the lifecycle controls — never fan out; they require an explicit
// -t so only the owning tenant acts instead of every participant reporting
// "apply not found".
func TestUnscopedCommandFansOut(t *testing.T) {
	assert.True(t, unscopedCommandFansOut(CommandResult{Found: true, Action: action.Apply}),
		"a complete apply fans out")
	assert.True(t, unscopedCommandFansOut(CommandResult{Found: true, Action: action.ApplyConfirm}),
		"apply-confirm fans out with apply: it is the confirmation step of the same per-participant flow")
	assert.True(t, unscopedCommandFansOut(CommandResult{MissingEnv: true, Action: action.Plan}),
		"plan without -e fans out as a multi-env plan")
	assert.True(t, unscopedCommandFansOut(CommandResult{Found: true, Action: action.Unlock}),
		"unlock fans out: it releases only this participant's own database locks")
	assert.False(t, unscopedCommandFansOut(CommandResult{MissingEnv: true, Action: action.Apply}),
		"apply without -e is an error and must not fan out (no duplicate missing-env comments)")

	// Commands targeting a single owned apply require an explicit -t.
	for _, a := range []string{
		action.Rollback, action.RollbackConfirm, action.Stop, action.Cancel,
		action.Start, action.Release, action.Cutover, action.SkipRevert, action.Revert,
	} {
		assert.False(t, unscopedCommandFansOut(CommandResult{Found: true, Action: a}),
			"%s targets a single owned apply and must require -t", a)
	}

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
