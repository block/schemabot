package api

import (
	"testing"

	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPRSchemaCapServerConfig(t *testing.T) {
	cfg := &ServerConfig{}
	repo := "octocat/hello-world"
	require.NoError(t, cfg.validatePRSchemaCap(repo, RepoConfig{}))
	require.NoError(t, cfg.validatePRSchemaCap(repo, RepoConfig{PRSchemaCap: PRSchemaCapOverride{Label: new("custom")}}))
	require.ErrorContains(t, cfg.validatePRSchemaCap(repo, RepoConfig{PRSchemaCap: PRSchemaCapOverride{Limit: new(-1)}}), "nonnegative")
	require.ErrorContains(t, cfg.validatePRSchemaCap(repo, RepoConfig{PRSchemaCap: PRSchemaCapOverride{Limit: new(5)}}), ".label")
	policy := PRSchemaCapOverride{Limit: new(5), Label: new("schemabot-lsc")}
	require.NoError(t, cfg.validatePRSchemaCap(repo, RepoConfig{PRSchemaCap: policy}))
	require.ErrorContains(t, cfg.validatePRSchemaCap(repo, RepoConfig{
		PRSchemaCap: policy, Aggregate: &AggregateConfig{Role: AggregateRoleParticipant},
	}), ".registrations")
	policy.Registrations = map[string]PRSchemaCapRegistration{
		"orders": {Type: storage.DatabaseTypeMySQL, Paths: []string{"schema/orders"}, Environments: []string{"staging", "production"}},
	}
	require.NoError(t, cfg.validatePRSchemaCap(repo, RepoConfig{
		PRSchemaCap: policy, Aggregate: &AggregateConfig{Role: AggregateRoleParticipant},
	}))
}

// YAML preserves the difference between inherited fields and explicit
// overrides, including zero for an unlimited repository.
func TestPRSchemaCapPolicyInheritance(t *testing.T) {
	const server = `
databases:
  orders:
    type: mysql
    environments:
      production:
        target: orders-production
        deployment: default
tern_deployments:
  default:
    production: "tern-prod:9090"
pr_schema_cap:
  limit: 5
  label: schemabot-lsc
  observe: true
`
	open, err := ParseServerConfig([]byte(server))
	require.NoError(t, err)
	require.Equal(t, PRSchemaCapPolicy{Limit: 5, Label: "schemabot-lsc", Observe: true},
		open.PRSchemaCapForRepo("myorg/unlisted").PRSchemaCapPolicy)
	restricted, err := ParseServerConfig([]byte(server + `
repos:
  myorg/inherited: {}
  myorg/override:
    pr_schema_cap:
      limit: 8
      observe: false
  myorg/label:
    pr_schema_cap:
      label: acknowledge
  myorg/unlimited:
    pr_schema_cap:
      limit: 0
`))
	require.NoError(t, err)
	require.Equal(t, PRSchemaCapPolicy{Limit: 5, Label: "schemabot-lsc", Observe: true},
		restricted.PRSchemaCapForRepo("MyOrg/Inherited").PRSchemaCapPolicy)
	require.Equal(t, PRSchemaCapPolicy{Limit: 8, Label: "schemabot-lsc"},
		restricted.PRSchemaCapForRepo("myorg/override").PRSchemaCapPolicy)
	require.Equal(t, PRSchemaCapPolicy{Limit: 5, Label: "acknowledge", Observe: true},
		restricted.PRSchemaCapForRepo("myorg/label").PRSchemaCapPolicy)
	require.Zero(t, restricted.PRSchemaCapForRepo("myorg/unlimited").Limit)
	require.Zero(t, restricted.PRSchemaCapForRepo("myorg/outside-allowlist").Limit)
	require.Zero(t, open.PRSchemaCapForRepo("").Limit)
}

func TestInheritedPRSchemaCapValidatesRepositoryRequirements(t *testing.T) {
	cfg := &ServerConfig{PRSchemaCap: PRSchemaCapPolicy{Limit: 5, Label: "schemabot-lsc"}}
	require.ErrorContains(t, cfg.validatePRSchemaCap("myorg/shared", RepoConfig{
		Aggregate: &AggregateConfig{Role: AggregateRoleParticipant},
	}), ".registrations")
	require.NoError(t, cfg.validatePRSchemaCap("myorg/shared", RepoConfig{
		Aggregate:   &AggregateConfig{Role: AggregateRoleParticipant},
		PRSchemaCap: PRSchemaCapOverride{Limit: new(0)},
	}))
	require.True(t, cfg.resolvePRSchemaCap(RepoConfig{ListenOnly: true}).Observe)
	require.ErrorContains(t, cfg.validatePRSchemaCap("myorg/observe", RepoConfig{
		ListenOnly: true, PRSchemaCap: PRSchemaCapOverride{Observe: new(false)},
	}), ".observe")
	require.ErrorContains(t, (&ServerConfig{PRSchemaCap: PRSchemaCapPolicy{Limit: 5}}).validateDefaultPRSchemaCap(), "pr_schema_cap.label")
}
