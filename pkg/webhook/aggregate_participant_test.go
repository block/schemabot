package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/api"
)

// A deployment configured as an aggregate participant for a repo stays silent on
// PRs that touch none of its schema (the leader owns the required check), while
// a leader or a sole SchemaBot with no aggregate config keeps posting its
// passing aggregate so branch protection is never wedged.
func TestIsAggregateParticipant(t *testing.T) {
	cfg := &api.ServerConfig{
		Repos: map[string]api.RepoConfig{
			"octocat/shared-repo": {Aggregate: &api.AggregateConfig{
				Role: api.AggregateRoleLeader,
				ExpectedTenants: []api.ExpectedTenant{
					{Tenant: "tenant-b", Paths: []string{"tenant-b/schema"}},
				},
			}},
			"octocat/participant-repo": {Aggregate: &api.AggregateConfig{
				Role: api.AggregateRoleParticipant,
			}},
			"octocat/plain-repo": {},
		},
	}
	h := &Handler{service: api.New(nil, cfg, nil, testLogger())}

	assert.True(t, h.isAggregateParticipant("octocat/participant-repo"),
		"a participant stays silent on non-schema PRs")
	assert.False(t, h.isAggregateParticipant("octocat/shared-repo"),
		"the leader owns the required check and keeps posting")
	assert.False(t, h.isAggregateParticipant("octocat/plain-repo"),
		"a repo with no aggregate config is a sole deployment and keeps posting")
	assert.False(t, h.isAggregateParticipant("octocat/unknown-repo"),
		"an unconfigured repo keeps posting")
}

// The predicate tolerates a handler with no server config (returns false, so the
// deployment keeps its existing posting behaviour rather than going silent).
func TestIsAggregateParticipantNoConfig(t *testing.T) {
	h := &Handler{service: api.New(nil, nil, nil, testLogger())}
	assert.False(t, h.isAggregateParticipant("octocat/any-repo"))
}
