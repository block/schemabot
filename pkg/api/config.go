package api

import (
	"fmt"
	"os"
	"strconv"

	"github.com/block/schemabot/pkg/secrets"
	"github.com/block/schemabot/pkg/storage"
	"gopkg.in/yaml.v3"
)

// ServerConfig holds the server-side SchemaBot configuration.
// This is loaded from a YAML file specified by SCHEMABOT_CONFIG_FILE.
type ServerConfig struct {
	// Storage configures SchemaBot's internal storage database.
	// If not specified, falls back to MYSQL_DSN environment variable.
	Storage StorageConfig `yaml:"storage"`

	// GitHub configures the GitHub App integration for webhook-driven schema changes.
	// If not set, the webhook endpoint is not registered.
	GitHub GitHubConfig `yaml:"github"`

	// TernDeployments maps deployment names to Tern gRPC endpoints per environment.
	// Use "default" for single-deployment setups.
	TernDeployments TernConfig `yaml:"tern_deployments"`

	// Databases contains registered database configurations per environment.
	// Key format: "database_name" with nested environment configs.
	Databases map[string]DatabaseConfig `yaml:"databases"`

	// Repos holds per-repository configuration.
	Repos map[string]RepoConfig `yaml:"repos"`

	// DefaultReviewers are GitHub teams/users required to review schema changes.
	DefaultReviewers []string `yaml:"default_reviewers"`
}

// GitHubConfig configures the GitHub App used for webhook-driven schema changes.
type GitHubConfig struct {
	// AppID is the GitHub App's numeric ID.
	// Supports secret references: env:VAR, file:/path, secretsmanager:name#key.
	// Falls back to GITHUB_APP_ID environment variable.
	AppID string `yaml:"app_id"`

	// PrivateKey is the PEM-encoded private key for the GitHub App.
	// Supports secret references: env:VAR, file:/path, secretsmanager:name#key.
	PrivateKey string `yaml:"private_key"`

	// WebhookSecret is the HMAC secret for validating webhook signatures.
	// Supports secret references: env:VAR, file:/path, secretsmanager:name#key.
	WebhookSecret string `yaml:"webhook_secret"`
}

// Configured returns true if the GitHub App is configured (app ID and private key are set).
func (g *GitHubConfig) Configured() bool {
	return g.ResolveAppID() != 0 && g.PrivateKey != ""
}

// ResolveAppID resolves the app ID from config (supports secret references),
// falling back to GITHUB_APP_ID env var.
func (g *GitHubConfig) ResolveAppID() int64 {
	resolved, err := secrets.Resolve(g.AppID, "GITHUB_APP_ID")
	if err == nil && resolved != "" {
		n, _ := strconv.ParseInt(resolved, 10, 64)
		return n
	}
	return 0
}

// ResolvePrivateKey resolves the private key value using the secrets resolver.
func (g *GitHubConfig) ResolvePrivateKey() (string, error) {
	return secrets.Resolve(g.PrivateKey, "")
}

// ResolveWebhookSecret resolves the webhook secret value using the secrets resolver.
func (g *GitHubConfig) ResolveWebhookSecret() (string, error) {
	return secrets.Resolve(g.WebhookSecret, "")
}

// StorageConfig configures SchemaBot's internal storage database.
type StorageConfig struct {
	// DSN is the MySQL connection string for SchemaBot's internal database.
	// Can be a direct DSN or a reference (e.g., "env:MYSQL_DSN" to read from env var).
	DSN string `yaml:"dsn"`
}

// DatabaseConfig holds configuration for a registered database.
type DatabaseConfig struct {
	// Type is the database type: "mysql" or "vitess".
	Type string `yaml:"type"`

	// Environments contains per-environment configuration.
	Environments map[string]EnvironmentConfig `yaml:"environments"`
}

// EnvironmentConfig holds per-environment database configuration.
type EnvironmentConfig struct {
	// DSN is the database connection string.
	// Can be a direct DSN or a reference to a secret (e.g., "env:MYSQL_DSN").
	DSN string `yaml:"dsn"`

	// For PlanetScale/Vitess:
	// Organization is the PlanetScale organization name.
	Organization string `yaml:"organization,omitempty"`

	// TokenSecretRef is the reference to the PlanetScale API token secret.
	TokenSecretRef string `yaml:"token_secret_ref,omitempty"`
}

// RepoConfig holds configuration for a specific repository.
type RepoConfig struct {
	// DefaultTernDeployment is the Tern deployment to use for this repo
	// when not overridden by schemabot.yaml in the repo.
	DefaultTernDeployment string `yaml:"default_tern_deployment"`
}

// LoadServerConfig loads the server configuration from the file specified
// by the SCHEMABOT_CONFIG_FILE environment variable.
func LoadServerConfig() (*ServerConfig, error) {
	path := os.Getenv("SCHEMABOT_CONFIG_FILE")
	if path == "" {
		return nil, fmt.Errorf("SCHEMABOT_CONFIG_FILE environment variable not set")
	}

	return LoadServerConfigFromFile(path)
}

// LoadServerConfigFromFile loads the server configuration from the specified file path.
func LoadServerConfigFromFile(path string) (*ServerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	var config ServerConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &config, nil
}

// Validate checks the configuration for required fields and consistency.
func (c *ServerConfig) Validate() error {
	// Either Databases (local mode) or TernDeployments (gRPC mode) must be configured
	if len(c.Databases) == 0 && len(c.TernDeployments) == 0 {
		return fmt.Errorf("either databases or tern_deployments is required")
	}

	// Validate Databases if present (local mode)
	for name, dbConfig := range c.Databases {
		if dbConfig.Type == "" {
			return fmt.Errorf("database %q missing type", name)
		}
		if dbConfig.Type != storage.DatabaseTypeMySQL && dbConfig.Type != storage.DatabaseTypeVitess {
			return fmt.Errorf("database %q has invalid type %q (must be %s or %s)", name, dbConfig.Type, storage.DatabaseTypeMySQL, storage.DatabaseTypeVitess)
		}
		if len(dbConfig.Environments) == 0 {
			return fmt.Errorf("database %q has no environments configured", name)
		}
		for env, envConfig := range dbConfig.Environments {
			if envConfig.DSN == "" {
				return fmt.Errorf("database %q environment %q missing DSN", name, env)
			}
		}
	}

	// Validate TernDeployments if present (gRPC mode)
	for name, endpoints := range c.TernDeployments {
		if len(endpoints) == 0 {
			return fmt.Errorf("deployment %q has no environments configured", name)
		}
		for env, addr := range endpoints {
			if addr == "" {
				return fmt.Errorf("deployment %q environment %q has empty address", name, env)
			}
		}
	}

	// Validate repo configs reference valid deployments
	for repo, repoConfig := range c.Repos {
		if repoConfig.DefaultTernDeployment != "" {
			if _, ok := c.TernDeployments[repoConfig.DefaultTernDeployment]; !ok {
				return fmt.Errorf("repo %q references unknown deployment %q", repo, repoConfig.DefaultTernDeployment)
			}
		}
	}

	return nil
}

// Database returns the database configuration for the given name.
// Returns nil if not found.
func (c *ServerConfig) Database(name string) *DatabaseConfig {
	if db, ok := c.Databases[name]; ok {
		return &db
	}
	return nil
}

// DatabaseEnvironment returns the environment configuration for a database.
// Returns nil if not found.
func (c *ServerConfig) DatabaseEnvironment(database, environment string) *EnvironmentConfig {
	db := c.Database(database)
	if db == nil {
		return nil
	}
	if env, ok := db.Environments[environment]; ok {
		return &env
	}
	return nil
}

// TernDeployment returns the Tern deployment name for the given repository.
// It checks repo-specific config first, then falls back to "default".
func (c *ServerConfig) TernDeployment(repo string) string {
	if repoConfig, ok := c.Repos[repo]; ok && repoConfig.DefaultTernDeployment != "" {
		return repoConfig.DefaultTernDeployment
	}
	return DefaultDeployment
}

// StorageDSN returns the resolved storage DSN.
// It handles special prefixes (env:, file:) to read from various sources.
// Falls back to MYSQL_DSN environment variable if not configured.
func (c *ServerConfig) StorageDSN() (string, error) {
	return secrets.Resolve(c.Storage.DSN, "MYSQL_DSN")
}
