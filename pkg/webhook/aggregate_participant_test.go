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

// A PR that renames a participant's schema file out of the participant's managed
// paths (to a disabled name, or to a directory no participant manages) still changes
// what that participant declares, so the leader must keep expecting the
// participant's check and fold it rather than pass on "no managed schema changes".
func TestLeaderExpectsParticipantsForPRCountsVacatedSchemaPath(t *testing.T) {
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
		want bool
	}{
		{
			name: "renamed to a disabled name",
			file: ghclient.PRFile{Filename: "tenant-b/schema/users.sql.disabled", PreviousFilename: "tenant-b/schema/users.sql", Status: "renamed"},
			want: true,
		},
		{
			name: "renamed out of every participant path",
			file: ghclient.PRFile{Filename: "archive/users.sql", PreviousFilename: "tenant-b/schema/users.sql", Status: "renamed"},
			want: true,
		},
		{
			name: "non schema rename outside participant paths",
			file: ghclient.PRFile{Filename: "docs/guide.md", PreviousFilename: "docs/users.md", Status: "renamed"},
		},
		{
			name: "schema rename outside participant paths",
			file: ghclient.PRFile{Filename: "other/users_v2.sql", PreviousFilename: "other/users.sql", Status: "renamed"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, h.leaderExpectsParticipantsForPR("octocat/shared-repo", []ghclient.PRFile{tt.file}))
		})
	}
}

// The predicate tolerates a handler with no server config (returns false, so the
// deployment keeps its existing posting behaviour rather than going silent).
func TestIsAggregateParticipantNoConfig(t *testing.T) {
	h := &Handler{service: api.New(nil, nil, nil, testLogger())}
	assert.False(t, h.isAggregateParticipant("octocat/any-repo"))
}
