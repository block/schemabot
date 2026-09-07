package serve

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
)

func validLocalConfig() api.ServerConfig {
	return api.ServerConfig{
		Storage:   api.StorageConfig{DSN: "root@tcp(127.0.0.1:3306)/schemabot_state"},
		Databases: map[string]api.DatabaseConfig{"app": {Type: "mysql", Environments: map[string]api.EnvironmentConfig{"development": {DSN: "root@tcp(127.0.0.1:3306)/app"}}}},
	}
}

func TestLocalConfiguration(t *testing.T) {
	cfg := validLocalConfig()
	err := validateLocalConfig(&cfg)
	require.NoError(t, err)
	assert.Equal(t, "root@tcp(127.0.0.1:3306)/schemabot_state", cfg.Storage.DSN)
	assert.Equal(t, "root@tcp(127.0.0.1:3306)/app", cfg.Databases["app"].Environments["development"].DSN)
	for _, tc := range []struct {
		name    string
		mutate  func(*api.ServerConfig)
		message string
	}{
		{"service auth", func(c *api.ServerConfig) { c.Auth.Type = "oidc" }, "private credential"},
		{"legacy GitHub App", func(c *api.ServerConfig) { c.GitHub.PrivateKey = "key" }, "must not configure GitHub Apps"},
		{"named GitHub App", func(c *api.ServerConfig) { c.Apps = map[string]api.GitHubAppConfig{"app": {PrivateKey: "key"}} }, "must not configure GitHub Apps"},
		{"shared validation", func(c *api.ServerConfig) { c.MetricsPort = 65536 }, "metrics_port"},
		{"missing database registry", func(c *api.ServerConfig) { c.Databases = nil }, "databases or target_resolver"},
		{"malformed storage", func(c *api.ServerConfig) { c.Storage.DSN = "invalid" }, "invalid MySQL DSN"},
		{"explicit storage", func(c *api.ServerConfig) { c.Storage.DSN = "" }, "explicit storage"},
		{"preserve storage", func(c *api.ServerConfig) { c.Storage.AllowDestructiveSchemaChanges = true }, "cannot enable destructive"},
		{"separate storage", func(c *api.ServerConfig) { c.Storage.DSN = "other@tcp(alias:3306)/app" }, "different name"},
		{"case-insensitive separation", func(c *api.ServerConfig) { c.Storage.DSN = "other@tcp(alias:3306)/APP" }, "different name"},
		{"named storage", func(c *api.ServerConfig) { c.Storage.DSN = "root@tcp(127.0.0.1:3306)/" }, "must name a database"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := validLocalConfig()
			tc.mutate(&config)
			err := validateLocalConfig(&config)
			require.ErrorContains(t, err, tc.message)
		})
	}
}

func TestLocalListenerRequiresLoopback(t *testing.T) {
	for _, address := range []string{"0.0.0.0:8080", ":8080", "localhost:8080", "192.0.2.1:8080"} {
		err := RunLocal(t.Context(), validLocalConfig(), LocalOptions{Address: address, Token: strings.Repeat("a", 64)})
		require.ErrorContains(t, err, "numeric loopback")
	}
}

func TestLocalConfigurationUsesSharedEngineRouting(t *testing.T) {
	for _, engine := range []string{"mysql", "postgres", "vitess"} {
		t.Run(engine, func(t *testing.T) {
			cfg := validLocalConfig()
			db := cfg.Databases["app"]
			db.Type = engine
			if engine == "postgres" {
				db.Environments["development"] = api.EnvironmentConfig{DSN: "postgres://user:password@localhost/app?sslmode=disable"}
			}
			cfg.Databases["app"] = db
			require.NoError(t, validateLocalConfig(&cfg))
			assert.Equal(t, engine, cfg.Databases["app"].Type)
		})
	}
}

func TestLocalConfigurationPreservesRemoteRouting(t *testing.T) {
	cfg := validLocalConfig()
	cfg.TernDeployments = api.TernConfig{"regional": {"development": "127.0.0.1:9090"}}
	cfg.Databases["app"] = api.DatabaseConfig{Type: "vitess", Environments: map[string]api.EnvironmentConfig{
		"development": {Target: "app", Deployment: "regional"},
	}}
	require.NoError(t, validateLocalConfig(&cfg))
	assert.Equal(t, "127.0.0.1:9090", cfg.TernDeployments["regional"]["development"])
	assert.Equal(t, "regional", cfg.Databases["app"].Environments["development"].Deployment)
}

// An occupied listener must fail before Build touches configuration-dependent
// resources. The unreadable TLS files would make Build fail immediately if it
// ran first, without needing a database or a long connection timeout.
func TestLocalListenerReservedBeforeBuild(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { assert.NoError(t, listener.Close()) }()
	cfg := validLocalConfig()
	missing := filepath.Join(t.TempDir(), "missing.pem")
	cfg.PlanetScale.MTLS = &api.PlanetScaleMTLSConfig{CABundle: missing, ClientCert: missing, ClientKey: missing}
	require.NoError(t, validateLocalConfig(&cfg))
	err = RunLocal(ctx, cfg, LocalOptions{Address: listener.Addr().String(), Token: strings.Repeat("a", 64)})
	require.ErrorContains(t, err, "listen for local runtime")
}
