package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
)

func sourcePolicyTestHandler(t *testing.T, config *api.ServerConfig) *Handler {
	t.Helper()
	service := api.New(nil, config, nil, testLogger())
	return &Handler{service: service, logger: testLogger()}
}

// On a repo with no directory allowlist (open mode), ownership of discovered
// schema configs is partitioned across deployments by the database registry
// when the repo participates in an aggregate check: a config whose database is
// not registered on this deployment belongs to a sibling deployment and is
// dropped, while a registered database keeps planning. Non-aggregate repos and
// deployments that resolve databases dynamically instead of through the
// registry keep unconditional open mode.
func TestShouldProcessSchemaConfigOpenMode(t *testing.T) {
	const repo = "octocat/hello-world"
	leaderRepo := map[string]api.RepoConfig{
		repo: {Aggregate: &api.AggregateConfig{Role: api.AggregateRoleLeader}},
	}

	cases := []struct {
		name     string
		config   *api.ServerConfig
		database string
		want     bool
	}{
		{
			name:     "non-aggregate repo keeps unregistered database",
			config:   &api.ServerConfig{},
			database: "orders",
			want:     true,
		},
		{
			name:     "aggregate leader drops unregistered database",
			config:   &api.ServerConfig{Repos: leaderRepo},
			database: "orders",
			want:     false,
		},
		{
			name: "aggregate participant drops unregistered database",
			config: &api.ServerConfig{
				Repos: map[string]api.RepoConfig{
					repo: {Aggregate: &api.AggregateConfig{Role: api.AggregateRoleParticipant}},
				},
			},
			database: "orders",
			want:     false,
		},
		{
			name: "aggregate repo keeps registered database",
			config: &api.ServerConfig{
				Repos: leaderRepo,
				Databases: map[string]api.DatabaseConfig{
					"orders": {Type: "mysql"},
				},
			},
			database: "orders",
			want:     true,
		},
		{
			// A leader whose only registered databases belong to other repos is
			// still in open mode here (no allowed_dirs entry covers this repo),
			// and a sibling deployment's config stays dropped: this deployment
			// exists on the repo purely to fold the participants' checks.
			name: "fold-only leader with databases for other repos drops sibling config",
			config: &api.ServerConfig{
				Repos: leaderRepo,
				Databases: map[string]api.DatabaseConfig{
					"payments": {
						Type:         "mysql",
						AllowedRepos: []string{"octocat/other"},
						AllowedDirs:  []string{"db/payments/schema"},
					},
				},
			},
			database: "orders",
			want:     false,
		},
		{
			// The registry lookup is by database name only: registering the
			// database for a different repo still keeps its config here. The
			// registry partition assumes fleet-unique database names.
			name: "database registered for another repo still keeps its config",
			config: &api.ServerConfig{
				Repos: leaderRepo,
				Databases: map[string]api.DatabaseConfig{
					"orders": {
						Type:         "mysql",
						AllowedRepos: []string{"octocat/other"},
						AllowedDirs:  []string{"db/orders/schema"},
					},
				},
			},
			database: "orders",
			want:     true,
		},
		{
			// A data-plane deployment resolves databases dynamically, so the
			// registry says nothing about what it manages: open mode keeps every
			// discovered config even on aggregate repos.
			name: "target resolver deployment keeps unregistered database",
			config: &api.ServerConfig{
				Repos:          leaderRepo,
				TargetResolver: api.TargetResolverConfig{Etre: []api.EtreConfig{{}}},
			},
			database: "orders",
			want:     true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := sourcePolicyTestHandler(t, tc.config)
			got := h.shouldProcessSchemaConfig(t.Context(), repo, 7, "headsha", tc.database, "mysql", "db/orders/schema", "test")
			assert.Equal(t, tc.want, got)
		})
	}
}

// A dropped config's error class must match the ownership contract that
// dropped it: on a repo with a directory allowlist the config was outside
// allowed_dirs, while in open mode the only drop reason is the database
// registry, so the database is reported as not configured on this server.
// Both classes count as unowned for unscoped fan-out silencing.
func TestUnownedSchemaConfigError(t *testing.T) {
	const repo = "octocat/hello-world"
	openMode := &api.ServerConfig{}
	allowlisted := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			"orders": {
				Type:         "mysql",
				AllowedRepos: []string{repo},
				AllowedDirs:  []string{"db/orders/schema"},
			},
		},
	}

	t.Run("open mode reports database not configured", func(t *testing.T) {
		h := sourcePolicyTestHandler(t, openMode)
		err := h.unownedSchemaConfigError(repo, "orders", "mysql", "db/other/schema")
		var notConfigured *api.DatabaseNotConfiguredError
		require.ErrorAs(t, err, &notConfigured)
		assert.Equal(t, "orders", notConfigured.Database)
		assert.True(t, isSchemaUnownedByDeploymentError(err))
	})

	t.Run("allowlisted repo reports config outside allowed_dirs", func(t *testing.T) {
		h := sourcePolicyTestHandler(t, allowlisted)
		err := h.unownedSchemaConfigError(repo, "payments", "mysql", "db/payments/schema")
		var outsideDirs *schemaConfigOutsideAllowedDirsError
		require.ErrorAs(t, err, &outsideDirs)
		assert.Equal(t, "payments", outsideDirs.Database)
		assert.Equal(t, "mysql", outsideDirs.DatabaseType)
		assert.Equal(t, "db/payments/schema", outsideDirs.SchemaPath)
		assert.True(t, isSchemaUnownedByDeploymentError(err))
	})

	t.Run("nil discovered config surfaces ErrNoConfig", func(t *testing.T) {
		h := sourcePolicyTestHandler(t, openMode)
		err := h.unownedDiscoveredConfigError(repo, nil, "db/orders/schema")
		assert.ErrorIs(t, err, ghclient.ErrNoConfig)
	})

	t.Run("discovered config reports its database identity", func(t *testing.T) {
		h := sourcePolicyTestHandler(t, openMode)
		config := &ghclient.SchemabotConfig{Database: "orders", Type: "mysql"}
		err := h.unownedDiscoveredConfigError(repo, config, "db/orders/schema")
		var notConfigured *api.DatabaseNotConfiguredError
		require.ErrorAs(t, err, &notConfigured)
		assert.Equal(t, "orders", notConfigured.Database)
	})
}
