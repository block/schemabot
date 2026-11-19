package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPRSchemaCapRepositoryPolicy(t *testing.T) {
	const repo = "example-org/service"
	cfg := &ServerConfig{Tenant: "east", Repos: map[string]RepoConfig{
		repo: {PRSchemaCap: &PRSchemaCapConfig{Label: "large-plan", Observe: true}},
	}, Apps: map[string]GitHubConfig{"reviewer": {}}}
	require.Equal(t, PRSchemaCapConfig{Label: "large-plan", Observe: true, Limit: 5, ControlPlane: "east"},
		cfg.PRSchemaCapForRepo("example-org/Service"))
	require.Zero(t, cfg.PRSchemaCapForRepo("example-org/other").Limit)

	for _, test := range []struct {
		policy PRSchemaCapConfig
		error  string
	}{
		{PRSchemaCapConfig{Limit: 5}, "exactly one"},
		{PRSchemaCapConfig{Label: "large-plan", ReviewTeam: "example-org/db"}, "exactly one"},
		{PRSchemaCapConfig{ReviewTeam: "partner-org/db"}, "repository's organization"},
		{PRSchemaCapConfig{ReviewTeam: "example-org/db", ReviewApp: "ghost"}, "configured GitHub App"},
		{PRSchemaCapConfig{ReviewTeam: "example-org/db", ReviewApp: "reviewer", RequestTeamReview: true}, ""},
		{PRSchemaCapConfig{Label: "large-plan", Limit: -1}, "limit"},
		{PRSchemaCapConfig{Label: "large-plan", RequestTeamReview: true}, "require review_team"},
	} {
		err := cfg.validatePRSchemaCap(repo, RepoConfig{PRSchemaCap: &test.policy})
		if test.error == "" {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, test.error)
		}
	}
}
