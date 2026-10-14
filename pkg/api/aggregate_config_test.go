package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func leaderConfig() *ServerConfig {
	return &ServerConfig{
		Repos: map[string]RepoConfig{
			"octocat/shared-repo": {
				Aggregate: &AggregateConfig{
					Role: AggregateRoleLeader,
					ExpectedTenants: []ExpectedTenant{
						{Tenant: "tenant-a", Paths: []string{"services/a"}, CheckName: "SchemaBot Tenant A"},
						{Tenant: "tenant-b", Paths: []string{"services/b"}, CheckName: "SchemaBot Tenant B"},
						{Tenant: "tenant-c", Paths: []string{"services/c"}, CheckName: "SchemaBot Tenant C"},
					},
				},
			},
		},
	}
}

func TestAggregateRoleAccessors(t *testing.T) {
	c := &ServerConfig{
		Repos: map[string]RepoConfig{
			"octocat/shared-repo": {Aggregate: &AggregateConfig{Role: AggregateRoleLeader, ExpectedTenants: []ExpectedTenant{{Tenant: "tenant-b", Paths: []string{"services/b"}, CheckName: "SchemaBot Tenant B"}}}},
			"octocat/other-repo":  {Aggregate: &AggregateConfig{Role: AggregateRoleParticipant}},
			"octocat/plain":       {},
		},
	}

	assert.Equal(t, AggregateRoleLeader, c.AggregateRoleForRepo("octocat/shared-repo"))
	assert.Equal(t, AggregateRoleParticipant, c.AggregateRoleForRepo("octocat/other-repo"))
	assert.Empty(t, c.AggregateRoleForRepo("octocat/plain"), "no aggregate config means no role")
	assert.Empty(t, c.AggregateRoleForRepo("octocat/unknown"), "unknown repo means no role")

	assert.True(t, c.IsAggregateLeaderForRepo("octocat/shared-repo"))
	assert.False(t, c.IsAggregateLeaderForRepo("octocat/other-repo"), "participant is not the leader")
	assert.False(t, c.IsAggregateLeaderForRepo("octocat/plain"))
}

// The leader derives the per-PR expected-tenant set from the path prefixes its
// expected tenants manage intersected with the PR's changed files — without
// needing to know any tenant's databases.
func TestExpectedTenantsForPR(t *testing.T) {
	c := leaderConfig()

	t.Run("single tenant touched", func(t *testing.T) {
		got := c.ExpectedTenantsForPR("octocat/shared-repo", []string{"services/b/schema/orders.sql"})
		assert.Equal(t, []string{"tenant-b"}, got)
	})

	t.Run("multiple tenants touched", func(t *testing.T) {
		got := c.ExpectedTenantsForPR("octocat/shared-repo", []string{
			"services/a/schema/users.sql",
			"services/b/schema/orders.sql",
		})
		assert.ElementsMatch(t, []string{"tenant-a", "tenant-b"}, got)
	})

	t.Run("no tenant paths touched", func(t *testing.T) {
		got := c.ExpectedTenantsForPR("octocat/shared-repo", []string{"docs/README.md", "services/z/x.sql"})
		assert.Empty(t, got)
	})

	t.Run("not the leader returns nil", func(t *testing.T) {
		participant := &ServerConfig{Repos: map[string]RepoConfig{
			"octocat/shared-repo": {Aggregate: &AggregateConfig{Role: AggregateRoleParticipant}},
		}}
		assert.Nil(t, participant.ExpectedTenantsForPR("octocat/shared-repo", []string{"services/b/x.sql"}))
	})

	t.Run("no aggregate config returns nil", func(t *testing.T) {
		plain := &ServerConfig{Repos: map[string]RepoConfig{"octocat/shared-repo": {}}}
		assert.Nil(t, plain.ExpectedTenantsForPR("octocat/shared-repo", []string{"services/b/x.sql"}))
	})
}

// The leader decides whether a schema directory is some participant's to
// manage from the expected-tenant path prefixes alone: a directory under one
// of them belongs to that participant, a directory under none of them (the
// repository root included) belongs to no participant, and a deployment that
// is not the leader has no expected set to decide from.
func TestExpectedTenantManagesSchemaPath(t *testing.T) {
	c := leaderConfig()

	assert.True(t, c.ExpectedTenantManagesSchemaPath("octocat/shared-repo", "services/b/schema"))
	assert.True(t, c.ExpectedTenantManagesSchemaPath("octocat/shared-repo", "services/a"), "the prefix itself is managed")
	assert.False(t, c.ExpectedTenantManagesSchemaPath("octocat/shared-repo", "services/d/schema"))
	assert.False(t, c.ExpectedTenantManagesSchemaPath("octocat/shared-repo", "."), "a root config is under no participant prefix")
	assert.False(t, c.ExpectedTenantManagesSchemaPath("octocat/shared-repo", "services/bb/schema"), "prefix matching is per directory, not per character")

	participant := &ServerConfig{Repos: map[string]RepoConfig{
		"octocat/shared-repo": {Aggregate: &AggregateConfig{Role: AggregateRoleParticipant}},
	}}
	assert.False(t, participant.ExpectedTenantManagesSchemaPath("octocat/shared-repo", "services/b/schema"), "only the leader holds the expected set")
	assert.False(t, c.ExpectedTenantManagesSchemaPath("octocat/unknown", "services/b/schema"))
}

// The leader resolves each expected participant's Check Run name from the
// expected-tenant entry, so ExpectedParticipantChecksForPR must carry the
// check-name base through alongside the tenant and its paths.
func TestExpectedParticipantChecksForPR(t *testing.T) {
	c := leaderConfig()

	t.Run("returns full entries with check name", func(t *testing.T) {
		got := c.ExpectedParticipantChecksForPR("octocat/shared-repo", []string{
			"services/a/schema/users.sql",
			"services/b/schema/orders.sql",
		})
		assert.ElementsMatch(t, []ExpectedTenant{
			{Tenant: "tenant-a", Paths: []string{"services/a"}, CheckName: "SchemaBot Tenant A"},
			{Tenant: "tenant-b", Paths: []string{"services/b"}, CheckName: "SchemaBot Tenant B"},
		}, got)
	})

	t.Run("no tenant paths touched", func(t *testing.T) {
		got := c.ExpectedParticipantChecksForPR("octocat/shared-repo", []string{"docs/README.md"})
		assert.Empty(t, got)
	})

	t.Run("not the leader returns nil", func(t *testing.T) {
		participant := &ServerConfig{Repos: map[string]RepoConfig{
			"octocat/shared-repo": {Aggregate: &AggregateConfig{Role: AggregateRoleParticipant}},
		}}
		assert.Nil(t, participant.ExpectedParticipantChecksForPR("octocat/shared-repo", []string{"services/b/x.sql"}))
	})
}

func TestValidateAggregateConfig(t *testing.T) {
	t.Run("nil is allowed", func(t *testing.T) {
		require.NoError(t, validateAggregateConfig("octocat/r", nil))
	})

	t.Run("valid leader", func(t *testing.T) {
		require.NoError(t, validateAggregateConfig("octocat/r", &AggregateConfig{
			Role:            AggregateRoleLeader,
			ExpectedTenants: []ExpectedTenant{{Tenant: "tenant-b", Paths: []string{"services/b"}, CheckName: "SchemaBot Tenant B"}},
		}))
	})

	t.Run("valid participant", func(t *testing.T) {
		require.NoError(t, validateAggregateConfig("octocat/r", &AggregateConfig{Role: AggregateRoleParticipant}))
	})

	cases := []struct {
		name    string
		agg     *AggregateConfig
		wantErr string
	}{
		{"empty role", &AggregateConfig{Role: ""}, "role is required"},
		{"invalid role", &AggregateConfig{Role: "boss"}, "is invalid"},
		{
			"participant with expected_tenants",
			&AggregateConfig{Role: AggregateRoleParticipant, ExpectedTenants: []ExpectedTenant{{Tenant: "x", Paths: []string{"a"}}}},
			"must be empty for role",
		},
		{"leader without expected_tenants", &AggregateConfig{Role: AggregateRoleLeader}, "expected_tenants is required"},
		{
			"leader with empty tenant",
			&AggregateConfig{Role: AggregateRoleLeader, ExpectedTenants: []ExpectedTenant{{Tenant: "  ", Paths: []string{"a"}, CheckName: "X"}}},
			"empty tenant",
		},
		{
			"leader with duplicate tenant",
			&AggregateConfig{Role: AggregateRoleLeader, ExpectedTenants: []ExpectedTenant{
				{Tenant: "tenant-b", Paths: []string{"a"}, CheckName: "X"},
				{Tenant: "tenant-b", Paths: []string{"b"}, CheckName: "Y"},
			}},
			"duplicate tenant",
		},
		{
			"leader with empty check_name",
			&AggregateConfig{Role: AggregateRoleLeader, ExpectedTenants: []ExpectedTenant{{Tenant: "tenant-b", Paths: []string{"a"}, CheckName: "   "}}},
			"check_name is required",
		},
		{
			"leader with empty paths",
			&AggregateConfig{Role: AggregateRoleLeader, ExpectedTenants: []ExpectedTenant{{Tenant: "tenant-b", CheckName: "X"}}},
			"paths is empty",
		},
		{
			"leader with absolute path",
			&AggregateConfig{Role: AggregateRoleLeader, ExpectedTenants: []ExpectedTenant{{Tenant: "tenant-b", Paths: []string{"/etc"}, CheckName: "X"}}},
			"invalid value",
		},
		{
			"leader with escaping path",
			&AggregateConfig{Role: AggregateRoleLeader, ExpectedTenants: []ExpectedTenant{{Tenant: "tenant-b", Paths: []string{"../secrets"}, CheckName: "X"}}},
			"invalid value",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAggregateConfig("octocat/r", tc.agg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
