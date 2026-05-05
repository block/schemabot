package api

import (
	"fmt"
	"log/slog"
	"os"
	"slices"
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

	// AllowedEnvironments restricts which environments this SchemaBot instance handles.
	// When set, the instance only processes commands for listed environments and uses
	// the GitHub Checks API to verify prior environments owned by other instances.
	// When empty or nil, all environments are allowed (backwards compatible).
	AllowedEnvironments []string `yaml:"allowed_environments"`

	// RespondToUnscoped controls whether this instance responds to commands
	// that are not scoped to a specific environment. In multi-instance
	// deployments where each repo has multiple GitHub Apps installed, set
	// this to false on all but one instance to prevent duplicate responses.
	//
	// Unscoped commands (only respond when true):
	//   - help          (usage instructions)
	//   - invalid/unknown commands (error responses)
	//
	// Scoped commands (always processed based on allowed_environments):
	//   - plan           (env-scoped, or plans only allowed environments)
	//   - apply          (env-scoped via -e flag)
	//   - apply-confirm  (env-scoped via -e flag)
	//   - rollback       (scoped to an apply ID)
	//   - stop/start     (scoped to an apply ID)
	//   - cutover        (scoped to an apply ID)
	//
	// Defaults to true (respond to all commands).
	RespondToUnscoped *bool `yaml:"respond_to_unscoped"`
}

// GitHubConfig configures the GitHub App used for webhook-driven schema changes.
type GitHubConfig struct {
	// AppID is the GitHub App's numeric ID.
	// Supports secret references: env:VAR, file:/path, secretsmanager:name#key.
	// Falls back to GITHUB_APP_ID environment variable.
	AppID string `yaml:"app-id"`

	// PrivateKey is the PEM-encoded private key for the GitHub App.
	// Supports secret references: env:VAR, file:/path, secretsmanager:name#key.
	PrivateKey string `yaml:"private-key"`

	// WebhookSecret is the HMAC secret for validating webhook signatures.
	// Supports secret references: env:VAR, file:/path, secretsmanager:name#key.
	WebhookSecret string `yaml:"webhook-secret"`
}

// Configured returns true if the GitHub App is configured (app ID and private key are set).
// It actually resolves the private key so that file: or secretsmanager: references that
// point to non-existent resources cause Configured() to return false instead of crashing.
func (g *GitHubConfig) Configured() bool {
	appID := g.ResolveAppID()
	if appID == 0 && g.PrivateKey == "" {
		slog.Info("GitHub App not configured — skipping GitHub setup")
		return false
	}
	if appID == 0 {
		slog.Warn("GitHub App private_key is set but app_id is missing — skipping GitHub setup")
		return false
	}
	if g.PrivateKey == "" {
		slog.Warn("GitHub App app_id is set but private_key is missing — skipping GitHub setup")
		return false
	}
	// Actually resolve the private key — if the file/secret doesn't exist yet,
	// treat GitHub as not configured rather than failing startup.
	pk, err := g.ResolvePrivateKey()
	if err != nil {
		slog.Warn("GitHub App credentials not resolvable — skipping GitHub setup", "error", err)
		return false
	}
	if pk == "" {
		slog.Warn("GitHub App private key resolved to empty — skipping GitHub setup")
		return false
	}
	return true
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

	// RevertWindowDuration is how long to keep the revert window open after a
	// PlanetScale deploy completes (e.g., "30m", "1h"). Defaults to 30m if empty.
	RevertWindowDuration string `yaml:"revert_window_duration,omitempty"`

	// APIURL is the PlanetScale API base URL (e.g., "http://localscale:8080").
	// DSN is the vtgate MySQL endpoint for schema queries and SHOW VITESS_MIGRATIONS.
	APIURL string `yaml:"api_url,omitempty"`

	// TLS configures MySQL TLS for branch connections.
	// When set, registers a named TLS config with the Go MySQL driver.
	// Omit for LocalScale (no TLS) or set for real PlanetScale (mTLS with CA bundle).
	TLS *TLSConfig `yaml:"tls,omitempty"`
}

// TLSConfig holds TLS certificate paths for MySQL connections to PlanetScale branches.
type TLSConfig struct {
	// CABundle is the path to the CA certificate bundle (PEM).
	CABundle string `yaml:"ca_bundle"`

	// ClientCert is the path to the client certificate (PEM).
	ClientCert string `yaml:"client_cert,omitempty"`

	// ClientKey is the path to the client private key (PEM).
	ClientKey string `yaml:"client_key,omitempty"`
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

// IsRepoAllowed returns whether the given repository is permitted to use SchemaBot.
// If the receiver is nil, Repos is empty, or Repos is nil, all repositories are
// allowed (backwards compatible). If Repos is populated, only listed repositories
// are allowed.
func (c *ServerConfig) IsRepoAllowed(repo string) bool {
	if c == nil || len(c.Repos) == 0 {
		return true
	}
	_, ok := c.Repos[repo]
	return ok
}

// IsEnvironmentAllowed returns whether the given environment is handled by this
// SchemaBot instance. If the receiver is nil, AllowedEnvironments is empty, or
// AllowedEnvironments is nil, all environments are allowed (backwards compatible).
func (c *ServerConfig) IsEnvironmentAllowed(env string) bool {
	if c == nil || len(c.AllowedEnvironments) == 0 {
		return true
	}
	return slices.Contains(c.AllowedEnvironments, env)
}

// ShouldRespondToUnscoped returns whether this instance should respond to
// commands not scoped to a specific environment (help, invalid commands).
// Defaults to true when not configured.
func (c *ServerConfig) ShouldRespondToUnscoped() bool {
	if c == nil || c.RespondToUnscoped == nil {
		return true
	}
	return *c.RespondToUnscoped
}

// StorageDSN returns the resolved storage DSN.
// It handles special prefixes (env:, file:) to read from various sources.
// Falls back to MYSQL_DSN environment variable if not configured.
func (c *ServerConfig) StorageDSN() (string, error) {
	return secrets.Resolve(c.Storage.DSN, "MYSQL_DSN")
}
