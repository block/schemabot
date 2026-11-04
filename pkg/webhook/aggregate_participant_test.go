package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
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

// An aggregate leader expects a participant's Check Run when a PR renames one of
// that participant's schema files out of its paths: merging the rename stops
// the default branch declaring the participant's tables, so the leader's
// aggregate must fold the participant (fail-closed) rather than pass on "no
// managed schema changes".
func TestLeaderExpectsParticipantsForRenameOutOfParticipantPaths(t *testing.T) {
	cfg := &api.ServerConfig{
		Repos: map[string]api.RepoConfig{
			"octocat/shared-repo": {Aggregate: &api.AggregateConfig{
				Role: api.AggregateRoleLeader,
				ExpectedTenants: []api.ExpectedTenant{
					{Tenant: "tenant-b", Paths: []string{"tenant-b/schema"}},
				},
			}},
		},
	}
	h := &Handler{service: api.New(nil, cfg, nil, testLogger())}

	tests := []struct {
		name string
		file ghclient.PRFile
	}{
		{"moved out of the participant's directory", ghclient.PRFile{Filename: "archive/users.sql", PreviousFilename: "tenant-b/schema/widgets/users.sql", Status: "renamed"}},
		{"renamed to a non-schema file elsewhere", ghclient.PRFile{Filename: "docs/users.md", PreviousFilename: "tenant-b/schema/widgets/users.sql", Status: "renamed"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.True(t, h.leaderExpectsParticipantsForPR("octocat/shared-repo", []ghclient.PRFile{tt.file}))
		})
	}

	assert.False(t, h.leaderExpectsParticipantsForPR("octocat/shared-repo", []ghclient.PRFile{
		{Filename: "docs/b.md", PreviousFilename: "docs/a.md", Status: "renamed"},
	}), "a rename outside every participant's paths expects no participant")
}

// The predicate tolerates a handler with no server config (returns false, so the
// deployment keeps its existing posting behaviour rather than going silent).
func TestIsAggregateParticipantNoConfig(t *testing.T) {
	h := &Handler{service: api.New(nil, nil, nil, testLogger())}
	assert.False(t, h.isAggregateParticipant("octocat/any-repo"))
}
