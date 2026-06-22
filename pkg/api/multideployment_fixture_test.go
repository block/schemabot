package api

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/storage"
)

const (
	multiDeploymentSchemaBotConfig = "../../deploy/local/config/grpc-schemabot-multideploy.yaml"
	multiDeploymentComposeFile     = "../../deploy/local/docker-compose.grpc-multideploy.yml"
)

// loadMultiDeployFixtureConfig decodes the multi-deployment gRPC fixture config
// the way LoadServerConfigFromFile does — with KnownFields(true) — so an unknown
// or misspelled key in the fixture fails the test instead of being silently
// dropped.
func loadMultiDeployFixtureConfig(t *testing.T) ServerConfig {
	t.Helper()
	data, err := os.ReadFile(multiDeploymentSchemaBotConfig)
	require.NoError(t, err)

	var cfg ServerConfig
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	require.NoError(t, dec.Decode(&cfg))
	return cfg
}

// TestLocalGRPCMultiDeploymentFixtureTopology pins the shape of the
// multi-deployment gRPC e2e fixture: one (database, environment) fans out to two
// deployments (eu, us), each backed by its own remote Tern, rolled out in order
// under a barrier cutover policy. This is the topology the multi-deployment
// ordered-cutover acceptance test runs against.
//
// The fixture is asserted statically rather than booted: the server enforces a
// >1-deployment guard until the multi-deployment orchestration path is enabled,
// so LoadServerConfigFromFile (which calls Validate) would reject this config
// today. The decode mirrors the loader with KnownFields(true) to catch typos,
// then exercises the resolver directly.
func TestLocalGRPCMultiDeploymentFixtureTopology(t *testing.T) {
	cfg := loadMultiDeployFixtureConfig(t)

	db, ok := cfg.Databases["testapp"]
	require.True(t, ok, "fixture must configure the testapp database")
	require.Equal(t, storage.DatabaseTypeMySQL, db.Type)

	env, ok := db.Environments["production"]
	require.True(t, ok, "fixture must configure the production environment")

	// Two deployments of ONE environment — not two environments.
	require.Len(t, env.Deployments, 2)
	assert.Contains(t, env.Deployments, "eu")
	assert.Contains(t, env.Deployments, "us")
	assert.Equal(t, []string{"eu", "us"}, env.DeploymentOrder)
	assert.Equal(t, storage.CutoverPolicyBarrier, env.CutoverPolicy)

	// Every deployment key must resolve to a non-empty per-environment endpoint.
	assert.Equal(t, "tern-eu:9090", cfg.TernDeployments["eu"]["production"])
	assert.Equal(t, "tern-us:9090", cfg.TernDeployments["us"]["production"])

	// The resolver returns the deployments in deployment_order, each carrying its
	// own deployment key against the shared target.
	targets, err := cfg.ResolveDatabaseTargets("testapp", "production")
	require.NoError(t, err)
	assert.Equal(t, []routing.ExecutionTarget{
		{DatabaseType: storage.DatabaseTypeMySQL, Deployment: "eu", Target: "testapp"},
		{DatabaseType: storage.DatabaseTypeMySQL, Deployment: "us", Target: "testapp"},
	}, targets)

	// The lead deployment (first in deployment_order) is the planning primary.
	primary, err := cfg.ResolvePrimaryDatabaseTarget("testapp", "production")
	require.NoError(t, err)
	assert.Equal(t, "eu", primary.Deployment)
}

// TestLocalGRPCMultiDeploymentFixtureComposeConsistency guards against drift
// between the SchemaBot config and the docker-compose stack that serves it: each
// Tern deployment endpoint in the config must map to a compose service of the
// same name, and the SchemaBot service must load the multi-deployment config.
func TestLocalGRPCMultiDeploymentFixtureComposeConsistency(t *testing.T) {
	cfg := loadMultiDeployFixtureConfig(t)

	composeData, err := os.ReadFile(multiDeploymentComposeFile)
	require.NoError(t, err)

	var compose struct {
		Services map[string]yaml.Node `yaml:"services"`
	}
	require.NoError(t, yaml.Unmarshal(composeData, &compose))

	assert.Contains(t, string(composeData), "grpc-schemabot-multideploy.yaml",
		"SchemaBot service must load the multi-deployment config")

	for deployment, endpoints := range cfg.TernDeployments {
		endpoint := endpoints["production"]
		require.NotEmpty(t, endpoint, "deployment %q must have a production endpoint", deployment)
		host, _, found := strings.Cut(endpoint, ":")
		require.True(t, found, "endpoint %q must be host:port", endpoint)
		assert.Contains(t, compose.Services, host,
			"compose must define a service for deployment endpoint host %q", host)
	}
}
