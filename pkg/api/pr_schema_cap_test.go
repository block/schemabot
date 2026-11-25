package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPRSchemaCapRepositoryPolicy(t *testing.T) {
	const repo = "example-org/service"
	cfg := &ServerConfig{Tenant: "east", Repos: map[string]RepoConfig{
		repo: {PRSchemaCap: &PRSchemaCapConfig{Label: "large-plan", Observe: true}},
	}}
	require.Equal(t, PRSchemaCapConfig{Label: "large-plan", Observe: true, Limit: 5, ControlPlane: "east"},
		cfg.PRSchemaCapForRepo("example-org/Service"))
	require.Zero(t, cfg.PRSchemaCapForRepo("example-org/other").Limit)

	for _, test := range []struct {
		policy PRSchemaCapConfig
		error  string
	}{
		{PRSchemaCapConfig{Label: "large-plan"}, ""},
		{PRSchemaCapConfig{Limit: 5}, "label is required"},
		{PRSchemaCapConfig{Label: "large-plan", Limit: -1}, "limit"},
	} {
		err := cfg.validatePRSchemaCap(repo, RepoConfig{PRSchemaCap: &test.policy})
		if test.error == "" {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, test.error)
		}
	}
}
