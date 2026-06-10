package api

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/pendingdrops"
	"github.com/block/schemabot/pkg/secrets"
	"github.com/block/schemabot/pkg/storage"
	gomysql "github.com/go-sql-driver/mysql"
	"gopkg.in/yaml.v3"
)

// DefaultGitHubCheckName is the base GitHub Check Run name used when a
// deployment does not configure a custom name.
const DefaultGitHubCheckName = "SchemaBot"

// ServerConfig holds the server-side SchemaBot configuration.
// This is loaded from a YAML file specified by SCHEMABOT_CONFIG_FILE.
type ServerConfig struct {
	// Storage configures SchemaBot's internal storage database.
	// If not specified, falls back to MYSQL_DSN environment variable.
	Storage StorageConfig `yaml:"storage"`

	// GitHub configures a single GitHub App for webhook-driven schema changes.
	// Mutually exclusive with Apps. If neither is set, the webhook endpoint
	// is not registered.
	GitHub GitHubConfig `yaml:"github"`

	// Apps configures multiple GitHub Apps for webhook-driven schema changes
	// in multi-tenant deployments (e.g. one App per GitHub org). The map key
	// is a stable logical name for the App; per-repo routing references this
	// name via RepoConfig.GitHubApp. Mutually exclusive with GitHub.
	//
	// Every entry in Repos MUST set GitHubApp to one of the keys in this map.
	Apps map[string]GitHubAppConfig `yaml:"apps,omitempty"`

	// TernDeployments maps deployment names to Tern gRPC endpoints per environment.
	// Use "default" for single-deployment setups.
	TernDeployments TernConfig `yaml:"tern_deployments"`

	// Databases contains registered database configurations per environment.
	// Key format: "database_name" with nested environment configs.
	Databases map[string]DatabaseConfig `yaml:"databases"`

	// Repos holds per-repository configuration.
	Repos map[string]RepoConfig `yaml:"repos"`

	// PRCommandAuthorization controls which GitHub users may run SchemaBot
	// apply/apply-confirm PR comment commands. When disabled, existing OSS/local
	// behavior is preserved.
	PRCommandAuthorization PRCommandAuthorizationConfig `yaml:"pr_command_authorization,omitempty"`

	// ReviewPolicy controls whose PR approvals satisfy the review gate before
	// apply/apply-confirm proceeds.
	ReviewPolicy ReviewPolicyConfig `yaml:"review_policy,omitempty"`

	// DefaultReviewers are GitHub teams/users required to review schema changes.
	DefaultReviewers []string `yaml:"default_reviewers"`

	// AllowedEnvironments restricts which environments this SchemaBot instance handles.
	// When set, the instance only processes commands for listed environments and uses
	// the GitHub Checks API to verify prior environments owned by other instances.
	// When empty or nil, all environments are allowed.
	AllowedEnvironments []string `yaml:"allowed_environments"`

	// EnvironmentOrder defines the server-owned promotion order. Defaults to
	// staging before production.
	EnvironmentOrder []string `yaml:"environment_order"`

	// OperatorWorkers is the number of concurrent operator workers that claim
	// and resume applies. Each worker independently polls FindNextApply with
	// FOR UPDATE SKIP LOCKED to prevent races. Defaults to DefaultOperatorWorkers.
	OperatorWorkers int `yaml:"operator_workers"`

	// SchedulerWorkers is the deprecated alias for OperatorWorkers. It is kept
	// so existing config files that still set scheduler_workers continue to
	// load (the YAML decoder runs with KnownFields(true) and would otherwise
	// reject the unknown key). Validate() copies it into OperatorWorkers when
	// the new key is unset and logs a deprecation warning. Remove one release
	// after the operator rename has soaked.
	//
	// Deprecated: use operator_workers.
	SchedulerWorkers int `yaml:"scheduler_workers,omitempty"`

	// OperatorClaimOperations switches scheduler workers to claim work at the
	// apply_operations (per-deployment) level via FindNextApplyOperation instead
	// of the apply level via FindNextApply. While the apply-create dual-write
	// produces exactly one operation per apply, the two paths are equivalent;
	// the operation-level path is the foundation for multi-deployment applies.
	// Defaults to false (apply-level claiming).
	OperatorClaimOperations bool `yaml:"operator_claim_operations,omitempty"`

	// RequirePassingChecks blocks apply when non-SchemaBot PR checks are failing.
	// When enabled (default), SchemaBot verifies that all other checks (CI, linters,
	// security scans) have passed before executing a schema change. Checks with
	// conclusion "neutral" or "skipped" are ignored. SchemaBot's own checks are
	// excluded from the evaluation.
	//
	// Defaults to true when not configured (nil = enabled).
	RequirePassingChecks *bool `yaml:"require_passing_checks"`

	// RequiredChecks narrows the PR checks gate to named checks when any of
	// those checks are present in the PR check statuses. When empty, all
	// non-SchemaBot checks are evaluated.
	RequiredChecks []string `yaml:"required_checks"`

	// RespondToUnscoped controls whether this instance responds to commands
	// that are not scoped to a specific environment. In multi-instance
	// deployments where each repo has multiple GitHub Apps installed, set
	// this to false on all but one instance to prevent duplicate responses.
	//
	// Unscoped commands (only respond when true):
	//   - help          (usage instructions)
	//   - invalid/unknown commands (e.g., "schemabot foobar")
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

	// PendingDrops configures the pending drops quarantine for MySQL/Spirit
	// databases. When enabled (the default), DROP TABLE statements rename the
	// table into the _pending_drops database instead of dropping it. The
	// background cleaner can be run by this process or disabled so another
	// deployment owns permanent cleanup after the retention period.
	PendingDrops PendingDropsConfig `yaml:"pending_drops,omitempty"`
}

// PendingDropsConfig configures the pending drops quarantine for MySQL/Spirit
// databases.
type PendingDropsConfig struct {
	// Enabled controls the quarantine.
	// Defaults to true when not configured (nil = enabled).
	Enabled *bool `yaml:"enabled"`

	// CleanupEnabled controls whether this server process starts the background
	// cleaner. Defaults to true when the quarantine is enabled. Set this to false
	// on frequently redeployed executors when another deployment owns cleanup.
	CleanupEnabled *bool `yaml:"cleanup_enabled"`

	// Retention is how long quarantined tables are kept before the cleaner
	// drops them permanently, as a Go duration string (e.g. "168h").
	// Defaults to 7 days.
	Retention string `yaml:"retention,omitempty"`

	// DryRun makes the cleaner log the tables it would drop without dropping
	// them. The quarantine itself is unaffected.
	DryRun bool `yaml:"dry_run,omitempty"`
}

// PendingDropsEnabled reports whether the pending drops quarantine is enabled.
// Defaults to true when not configured.
func (c *ServerConfig) PendingDropsEnabled() bool {
	return c.PendingDrops.Enabled == nil || *c.PendingDrops.Enabled
}

// PendingDropsCleanupEnabled reports whether this process should run the
// pending drops cleaner. The cleaner never runs when the quarantine is disabled.
func (c *ServerConfig) PendingDropsCleanupEnabled() bool {
	if !c.PendingDropsEnabled() {
		return false
	}
	return c.PendingDrops.CleanupEnabled == nil || *c.PendingDrops.CleanupEnabled
}

// PendingDropsRetention returns the configured retention for quarantined
// tables, falling back to the default when not configured.
func (c *ServerConfig) PendingDropsRetention() (time.Duration, error) {
	if c.PendingDrops.Retention == "" {
		return pendingdrops.DefaultRetention, nil
	}
	d, err := time.ParseDuration(c.PendingDrops.Retention)
	if err != nil {
		return 0, fmt.Errorf("parse pending_drops.retention %q: %w", c.PendingDrops.Retention, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("pending_drops.retention must be positive, got %q", c.PendingDrops.Retention)
	}
	return d, nil
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

	// CheckName is the base name for aggregate GitHub Check Runs published by
	// this App. Environment-scoped deployments append the environment in
	// parentheses, for example "SchemaBot (staging)".
	CheckName string `yaml:"check-name,omitempty"`
}

// CheckRunNameBase returns the configured aggregate GitHub Check Run base name.
func (g GitHubConfig) CheckRunNameBase() string {
	name := strings.TrimSpace(g.CheckName)
	if name == "" {
		return DefaultGitHubCheckName
	}
	return name
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
		slog.Warn("GitHub App private-key is set but app-id is missing — skipping GitHub setup")
		return false
	}
	if g.PrivateKey == "" {
		slog.Warn("GitHub App app-id is set but private-key is missing — skipping GitHub setup")
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

// GitHubAppConfig is one entry in ServerConfig.Apps. It carries the same
// credentials as the single-App GitHubConfig and shares its resolution
// helpers. The enclosing map key is the App's stable logical name used by
// per-repo routing via RepoConfig.GitHubApp.
type GitHubAppConfig = GitHubConfig

// StorageConfig configures SchemaBot's internal storage database.
type StorageConfig struct {
	// DSN is the MySQL connection string for SchemaBot's internal database.
	// Can be a direct DSN or a reference (e.g., "env:MYSQL_DSN" to read from env var).
	DSN string `yaml:"dsn"`

	// DSNFrom builds the MySQL connection string from separate database config
	// and password references. It is mutually exclusive with DSN.
	DSNFrom *DSNFromConfig `yaml:"dsn_from,omitempty"`
}

// DSNFromConfig configures a MySQL DSN assembled from separate secret values.
type DSNFromConfig struct {
	// ConfigRef is a secret reference containing database connection metadata.
	ConfigRef string `yaml:"config_ref"`

	// ConfigPaths selects fields from the referenced config document. Paths are
	// dot-separated YAML map keys and default to host, port, and database.
	ConfigPaths DSNFromConfigPaths `yaml:"config_paths,omitempty"`

	// Username is the database user to include in the generated DSN.
	Username string `yaml:"username"`

	// PasswordRef is a secret reference containing the database user's password.
	PasswordRef string `yaml:"password_ref"`

	// Params are appended as MySQL DSN query parameters.
	Params map[string]string `yaml:"params,omitempty"`
}

// DSNFromConfigPaths configures where to find connection fields in ConfigRef.
type DSNFromConfigPaths struct {
	Host     string `yaml:"host,omitempty"`
	Port     string `yaml:"port,omitempty"`
	Database string `yaml:"database,omitempty"`
}

// DatabaseConfig holds configuration for a registered database.
type DatabaseConfig struct {
	// Type is the database type: "mysql", "vitess", or "strata".
	Type string `yaml:"type"`

	// Environments contains per-environment configuration.
	Environments map[string]EnvironmentConfig `yaml:"environments"`

	// AllowedRepos restricts which trusted GitHub PR repositories may manage
	// this database. Values are exact owner/repo names. A literal "*" allows
	// any trusted repo.
	AllowedRepos []string `yaml:"allowed_repos,omitempty"`

	// AllowedDirs restricts which trusted GitHub PR repo-relative schema
	// directories may manage this database. Values match the directory itself
	// and descendants. A literal "*" allows any trusted schema directory.
	AllowedDirs []string `yaml:"allowed_dirs,omitempty"`

	// OperatorTeams are GitHub teams whose members may run apply/apply-confirm
	// PR comment commands for this database when PR command authorization is enabled.
	OperatorTeams []string `yaml:"operator_teams,omitempty"`

	// OperatorUsers are GitHub users who may run apply/apply-confirm PR comment
	// commands for this database when PR command authorization is enabled.
	OperatorUsers []string `yaml:"operator_users,omitempty"`
}

// PRCommandAuthorizationConfig configures actor authorization for SchemaBot
// apply/apply-confirm GitHub PR comment commands.
type PRCommandAuthorizationConfig struct {
	// Enabled turns on fail-closed actor authorization for apply/apply-confirm
	// PR commands.
	Enabled bool `yaml:"enabled,omitempty"`

	// AdminTeams are GitHub teams whose members may run apply/apply-confirm PR
	// commands for any configured database.
	AdminTeams []string `yaml:"admin_teams,omitempty"`

	// AdminUsers are GitHub users who may run apply/apply-confirm PR commands
	// for any configured database.
	AdminUsers []string `yaml:"admin_users,omitempty"`
}

// ReviewPolicyConfig configures the PR review gate.
type ReviewPolicyConfig struct {
	// Enabled turns on the review gate before apply/apply-confirm.
	Enabled bool `yaml:"enabled,omitempty"`

	// AdminTeams are GitHub teams whose approvals satisfy the review gate for
	// any configured database.
	AdminTeams []string `yaml:"admin_teams,omitempty"`

	// AdminUsers are GitHub users whose approvals satisfy the review gate for
	// any configured database.
	AdminUsers []string `yaml:"admin_users,omitempty"`

	// IncludeDatabaseOperators allows configured database operator_teams and
	// operator_users to satisfy the review gate. Defaults to true.
	IncludeDatabaseOperators *bool `yaml:"include_database_operators,omitempty"`

	// IncludeCodeowners allows matching CODEOWNERS approvals to satisfy the
	// review gate. Defaults to false.
	IncludeCodeowners bool `yaml:"include_codeowners,omitempty"`
}

// EnvironmentConfig holds per-environment database configuration.
type EnvironmentConfig struct {
	// DSN is the database connection string for local mode.
	// Can be a direct DSN or a reference to a secret (e.g., "env:MYSQL_DSN").
	DSN string `yaml:"dsn"`

	// DSNFrom builds the database connection string for local mode from separate
	// database config and password references. It is mutually exclusive with DSN.
	DSNFrom *DSNFromConfig `yaml:"dsn_from,omitempty"`

	// Target is the opaque Tern-facing target identifier for gRPC mode.
	// Mutually exclusive with Deployments.
	Target string `yaml:"target,omitempty"`

	// Deployment is the Tern deployment key for gRPC mode.
	// Mutually exclusive with Deployments.
	Deployment string `yaml:"deployment,omitempty"`

	// Deployments maps a Tern deployment key to its per-deployment target
	// for multi-deployment environments. Each key MUST also appear in the
	// top-level tern_deployments map. Mutually exclusive with the scalar
	// Target/Deployment fields above.
	//
	// Example:
	//   deployments:
	//     payments-a: { target: payments }
	//     payments-b: { target: payments }
	Deployments map[string]DeploymentTarget `yaml:"deployments,omitempty"`

	// DeploymentOrder defines the rollout order of the deployments map for this
	// environment, analogous to the server-wide EnvironmentOrder. When set it
	// must list every key in Deployments exactly once; ResolveDatabaseTargets
	// then returns deployments in this order. When empty, deployments resolve in
	// alphabetical key order. Only meaningful alongside a Deployments map.
	DeploymentOrder []string `yaml:"deployment_order,omitempty"`

	// For PlanetScale/Vitess:
	// Organization is the PlanetScale organization name.
	// sadscan:disable kingfisher.planetscale.2
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

type externalDatabaseEndpoint struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
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
	// EnableChecks controls whether SchemaBot publishes GitHub Check Runs for
	// this repository. Stored check state is still maintained for SchemaBot's
	// own safety gates. Defaults to true when not configured.
	EnableChecks *bool `yaml:"enable_checks,omitempty"`

	// GitHubApp names the App in ServerConfig.Apps that owns webhooks and
	// outbound GitHub API calls for this repository. Required when Apps is
	// configured and must match a key in ServerConfig.Apps. Setting it
	// while only the legacy single-App GitHub field is configured is
	// rejected at config load to fail closed on misconfiguration.
	GitHubApp string `yaml:"github_app,omitempty"`
}

// ResolvedDatabaseTarget is the server-owned routing decision for plan/apply.
type ResolvedDatabaseTarget struct {
	DatabaseType string
	Deployment   string
	Target       string
}

// DeploymentTarget is one entry in EnvironmentConfig.Deployments. It carries
// the per-deployment override values for a multi-deployment environment. The
// enclosing map key identifies the Tern deployment (and must also appear in
// the top-level tern_deployments map).
type DeploymentTarget struct {
	// Target is the opaque Tern-facing target identifier for this deployment.
	Target string `yaml:"target"`
}

var defaultEnvironmentOrder = []string{"staging", "production"}

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
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&config); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &config, nil
}

// resolveDeprecatedOperatorWorkers folds the deprecated scheduler_workers key
// into operator_workers. When only the deprecated key is set it is honored and
// a deprecation warning is logged; setting both keys is rejected so the
// effective value is never ambiguous.
func (c *ServerConfig) resolveDeprecatedOperatorWorkers() error {
	if c.SchedulerWorkers == 0 {
		return nil
	}
	if c.OperatorWorkers != 0 {
		return fmt.Errorf("set only one of operator_workers or scheduler_workers (scheduler_workers is deprecated)")
	}
	slog.Warn("scheduler_workers is deprecated; use operator_workers", "scheduler_workers", c.SchedulerWorkers)
	c.OperatorWorkers = c.SchedulerWorkers
	c.SchedulerWorkers = 0
	return nil
}

// Validate checks the configuration for required fields and consistency.
func (c *ServerConfig) Validate() error {
	if err := c.resolveDeprecatedOperatorWorkers(); err != nil {
		return err
	}

	// The database registry is required in both local mode and gRPC mode:
	// local environments provide DSNs, while remote environments provide the
	// server-owned target/deployment route.
	if len(c.Databases) == 0 {
		return fmt.Errorf("databases is required")
	}

	if err := validateUniqueNames("environment_order", c.EnvironmentOrder); err != nil {
		return err
	}
	if err := validateUniqueNames("required_checks", c.RequiredChecks); err != nil {
		return err
	}
	if err := validatePRCommandAuthorization(c.PRCommandAuthorization); err != nil {
		return err
	}
	if err := validateReviewPolicy(c.ReviewPolicy); err != nil {
		return err
	}
	if err := c.validateGitHubAppsConfig(); err != nil {
		return err
	}
	if c.PendingDropsEnabled() {
		if _, err := c.PendingDropsRetention(); err != nil {
			return err
		}
	}

	// Validate Databases if present. An environment is either local mode
	// (direct DSN) or gRPC mode (server-side target + deployment).
	for name, dbConfig := range c.Databases {
		if dbConfig.Type == "" {
			return fmt.Errorf("database %q missing type", name)
		}
		if err := validateDatabaseSourcePolicy(name, dbConfig); err != nil {
			return err
		}
		if err := validateDatabaseActorAuthorization(name, dbConfig); err != nil {
			return err
		}
		switch dbConfig.Type {
		case storage.DatabaseTypeMySQL, storage.DatabaseTypeVitess, storage.DatabaseTypeStrata:
		default:
			return fmt.Errorf("database %q has invalid type %q (must be %s, %s, or %s)", name, dbConfig.Type, storage.DatabaseTypeMySQL, storage.DatabaseTypeVitess, storage.DatabaseTypeStrata)
		}
		if len(dbConfig.Environments) == 0 {
			return fmt.Errorf("database %q has no environments configured", name)
		}
		for env, envConfig := range dbConfig.Environments {
			hasDSN := envConfig.HasLocalDSN()
			hasScalarRouting := envConfig.Target != "" || envConfig.Deployment != ""
			hasMapRouting := envConfig.Deployments != nil
			if len(envConfig.DeploymentOrder) > 0 && !hasMapRouting {
				return fmt.Errorf("database %q environment %q sets deployment_order without a deployments map", name, env)
			}
			switch {
			case hasDSN && (hasScalarRouting || hasMapRouting):
				return fmt.Errorf("database %q environment %q cannot configure both local DSN and target/deployment(s)", name, env)
			case hasScalarRouting && hasMapRouting:
				return fmt.Errorf("database %q environment %q cannot configure both scalar target/deployment and a deployments map", name, env)
			case hasDSN:
				if err := envConfig.validateLocalDSNConfig(fmt.Sprintf("database %q environment %q", name, env)); err != nil {
					return err
				}
				continue
			case hasMapRouting:
				if len(envConfig.Deployments) == 0 {
					return fmt.Errorf("database %q environment %q deployments map is empty", name, env)
				}
				if len(envConfig.DeploymentOrder) > 0 {
					if err := validateDeploymentOrder(envConfig.Deployments, envConfig.DeploymentOrder, fmt.Sprintf("database %q environment %q", name, env)); err != nil {
						return err
					}
				}
				// The multi-deployment apply path (plan, progress, webhook)
				// is not yet wired to ResolveDatabaseTargets, so accepting a
				// >1-entry deployments map here would silently break every
				// plan/apply with a confusing internal resolver error. Gate
				// it at config load until the orchestration path lands.
				if len(envConfig.Deployments) > 1 {
					return fmt.Errorf("database %q environment %q multi-deployment (>1 entries) is not yet supported by this server; got %d deployments", name, env, len(envConfig.Deployments))
				}
				for deployment, dt := range envConfig.Deployments {
					if deployment == "" {
						return fmt.Errorf("database %q environment %q has a deployments map entry with an empty key", name, env)
					}
					if dt.Target == "" {
						return fmt.Errorf("database %q environment %q deployment %q missing target", name, env, deployment)
					}
					endpoints, ok := c.TernDeployments[deployment]
					if !ok {
						return fmt.Errorf("database %q environment %q references unknown deployment %q", name, env, deployment)
					}
					if endpoints[env] == "" {
						return fmt.Errorf("database %q environment %q deployment %q has no endpoint", name, env, deployment)
					}
				}
				continue
			case !hasScalarRouting:
				return fmt.Errorf("database %q environment %q missing local DSN or target/deployment(s)", name, env)
			case envConfig.Target == "":
				return fmt.Errorf("database %q environment %q missing target", name, env)
			case envConfig.Deployment == "":
				return fmt.Errorf("database %q environment %q missing deployment", name, env)
			}
			endpoints, ok := c.TernDeployments[envConfig.Deployment]
			if !ok {
				return fmt.Errorf("database %q environment %q references unknown deployment %q", name, env, envConfig.Deployment)
			}
			if endpoints[env] == "" {
				return fmt.Errorf("database %q environment %q deployment %q has no endpoint", name, env, envConfig.Deployment)
			}
		}
	}

	if err := c.validateNoLocalRemoteRouteCollision(); err != nil {
		return err
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

	if err := c.Storage.validateLocalDSNConfig("storage"); err != nil {
		return err
	}

	return nil
}

// TernClient uses the same deployment/environment key for remote deployments
// and local database clients. Reject ambiguous config before runtime routing can
// choose the wrong backend.
func (c *ServerConfig) validateNoLocalRemoteRouteCollision() error {
	for database, dbConfig := range c.Databases {
		remoteEnvironments, ok := c.TernDeployments[database]
		if !ok {
			continue
		}
		for environment, envConfig := range dbConfig.Environments {
			if !envConfig.HasLocalDSN() {
				continue
			}
			if remoteEnvironments[environment] == "" {
				continue
			}
			return fmt.Errorf("database %q environment %q uses a local dsn but tern_deployments also defines deployment %q for that environment; rename the database or deployment to avoid ambiguous routing", database, environment, database)
		}
	}
	return nil
}

// validateGitHubAppsConfig validates the multi-App config shape and its
// interaction with the legacy single-App GitHub field and per-repo routing.
//
// Rules (mirroring the documented back-compat matrix):
//   - github: and apps: are mutually exclusive.
//   - If apps: is set, each entry must declare a non-empty app-id, private-key,
//     and webhook-secret, and the map key must be non-empty.
//   - If apps: is set, every entry in repos: MUST set github_app to one of
//     the configured app names.
//   - If apps: is NOT set, repos must not declare github_app (it would be a
//     silently ignored field, which we want to fail closed on).
func (c *ServerConfig) validateGitHubAppsConfig() error {
	hasGitHub := c.GitHub.AppID != "" || c.GitHub.PrivateKey != "" || c.GitHub.WebhookSecret != "" || c.GitHub.CheckName != ""
	hasApps := c.Apps != nil

	if hasGitHub && hasApps {
		return fmt.Errorf("github: and apps: are mutually exclusive; configure one or the other")
	}

	if hasApps {
		if len(c.Apps) == 0 {
			return fmt.Errorf("apps: is configured but contains no entries")
		}
		for name, app := range c.Apps {
			if name == "" {
				return fmt.Errorf("apps: contains an entry with an empty name")
			}
			if app.AppID == "" {
				return fmt.Errorf("app %q missing app-id", name)
			}
			if app.PrivateKey == "" {
				return fmt.Errorf("app %q missing private-key", name)
			}
			if app.WebhookSecret == "" {
				return fmt.Errorf("app %q missing webhook-secret", name)
			}
		}
		for repo, repoConfig := range c.Repos {
			if repoConfig.GitHubApp == "" {
				return fmt.Errorf("repository %q is missing github_app (required when apps: is configured)", repo)
			}
			if _, ok := c.Apps[repoConfig.GitHubApp]; !ok {
				return fmt.Errorf("repository %q references unknown github_app %q", repo, repoConfig.GitHubApp)
			}
		}
		return nil
	}

	// Apps not configured — github_app on a repo would be silently ignored,
	// so reject it explicitly to avoid surprising operators.
	for repo, repoConfig := range c.Repos {
		if repoConfig.GitHubApp != "" {
			return fmt.Errorf("repository %q sets github_app %q but apps: is not configured", repo, repoConfig.GitHubApp)
		}
	}
	return nil
}

func validateUniqueNames(field string, names []string) error {
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			return fmt.Errorf("%s contains an empty value", field)
		}
		if strings.TrimSpace(name) != name {
			return fmt.Errorf("%s contains value %q with leading or trailing whitespace", field, name)
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("%s contains duplicate value %q", field, name)
		}
		seen[name] = struct{}{}
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

// DatabaseEnvironments returns the environments configured server-side for a
// database, ordered by the server-owned promotion order.
func (c *ServerConfig) DatabaseEnvironments(database string) ([]string, error) {
	if c == nil {
		return nil, fmt.Errorf("server config is nil")
	}
	db := c.Database(database)
	if db == nil {
		return nil, fmt.Errorf("database %q is not configured on this server", database)
	}
	environments := make([]string, 0, len(db.Environments))
	for environment := range db.Environments {
		environments = append(environments, environment)
	}
	return c.OrderedEnvironments(environments), nil
}

// ResolveDatabaseTarget returns the complete routing metadata for a configured
// database/environment. Local targets use the database name for deployment and
// target; remote targets use the configured Tern deployment and opaque target.
//
// For environments configured with a deployments map (multi-deployment), this
// returns the single deployment when exactly one is configured and otherwise
// errors. Callers that need the full set MUST use ResolveDatabaseTargets.
func (c *ServerConfig) ResolveDatabaseTarget(database, environment string) (ResolvedDatabaseTarget, error) {
	targets, err := c.ResolveDatabaseTargets(database, environment)
	if err != nil {
		return ResolvedDatabaseTarget{}, err
	}
	if len(targets) != 1 {
		return ResolvedDatabaseTarget{}, fmt.Errorf("database %q environment %q resolves to %d deployments; use ResolveDatabaseTargets", database, environment, len(targets))
	}
	return targets[0], nil
}

// orderedDeploymentKeys returns the keys of a deployments map in rollout order:
// the explicit deployment_order when set, otherwise alphabetical for
// deterministic resolution. When an order is given it is validated to be a
// permutation of the map keys.
func orderedDeploymentKeys(deployments map[string]DeploymentTarget, order []string, context string) ([]string, error) {
	for deployment := range deployments {
		if deployment == "" {
			return nil, fmt.Errorf("%s has a deployments map entry with an empty key", context)
		}
	}
	if len(order) == 0 {
		keys := make([]string, 0, len(deployments))
		for deployment := range deployments {
			keys = append(keys, deployment)
		}
		slices.Sort(keys)
		return keys, nil
	}
	if err := validateDeploymentOrder(deployments, order, context); err != nil {
		return nil, err
	}
	return slices.Clone(order), nil
}

// validateDeploymentOrder checks that deployment_order lists every key in the
// deployments map exactly once, with no empty, duplicate, or unknown entries.
func validateDeploymentOrder(deployments map[string]DeploymentTarget, order []string, context string) error {
	for deployment := range deployments {
		if deployment == "" {
			return fmt.Errorf("%s has a deployments map entry with an empty key", context)
		}
	}
	seen := make(map[string]bool, len(order))
	for _, deployment := range order {
		if deployment == "" {
			return fmt.Errorf("%s deployment_order has an empty entry", context)
		}
		if seen[deployment] {
			return fmt.Errorf("%s deployment_order has duplicate entry %q", context, deployment)
		}
		if _, ok := deployments[deployment]; !ok {
			return fmt.Errorf("%s deployment_order references unknown deployment %q", context, deployment)
		}
		seen[deployment] = true
	}
	for deployment := range deployments {
		if !seen[deployment] {
			return fmt.Errorf("%s deployment_order is missing deployment %q", context, deployment)
		}
	}
	return nil
}

// ResolveDatabaseTargets returns the complete routing metadata for a configured
// database/environment as one or more deployment slices. Single-deployment
// configurations (scalar target/deployment or local DSN) return a one-element
// slice; multi-deployment environments return one element per entry in the
// deployments map, ordered deterministically by deployment key.
func (c *ServerConfig) ResolveDatabaseTargets(database, environment string) ([]ResolvedDatabaseTarget, error) {
	if c == nil {
		return nil, fmt.Errorf("server config is nil")
	}
	dbConfig := c.Database(database)
	if dbConfig == nil {
		return nil, fmt.Errorf("database %q is not configured on this server", database)
	}
	envConfig, ok := dbConfig.Environments[environment]
	if !ok {
		return nil, fmt.Errorf("database %q environment %q is not configured on this server", database, environment)
	}

	if envConfig.HasLocalDSN() {
		return []ResolvedDatabaseTarget{{
			DatabaseType: dbConfig.Type,
			Deployment:   database,
			Target:       database,
		}}, nil
	}

	// A non-nil deployments map is authoritative — fall through to scalar
	// routing only when the map was not configured at all. This mirrors the
	// validation in Validate() so an explicitly empty `deployments: {}` (or
	// one with an empty key) returns the same clear error here.
	if envConfig.Deployments != nil {
		if len(envConfig.Deployments) == 0 {
			return nil, fmt.Errorf("database %q environment %q deployments map is empty", database, environment)
		}
		deployments, err := orderedDeploymentKeys(envConfig.Deployments, envConfig.DeploymentOrder, fmt.Sprintf("database %q environment %q", database, environment))
		if err != nil {
			return nil, err
		}
		out := make([]ResolvedDatabaseTarget, 0, len(deployments))
		for _, deployment := range deployments {
			dt := envConfig.Deployments[deployment]
			if dt.Target == "" {
				return nil, fmt.Errorf("database %q environment %q deployment %q missing target", database, environment, deployment)
			}
			out = append(out, ResolvedDatabaseTarget{
				DatabaseType: dbConfig.Type,
				Deployment:   deployment,
				Target:       dt.Target,
			})
		}
		return out, nil
	}

	if envConfig.Target == "" {
		return nil, fmt.Errorf("database %q environment %q missing server-side target", database, environment)
	}
	if envConfig.Deployment == "" {
		return nil, fmt.Errorf("database %q environment %q missing server-side deployment", database, environment)
	}
	return []ResolvedDatabaseTarget{{
		DatabaseType: dbConfig.Type,
		Deployment:   envConfig.Deployment,
		Target:       envConfig.Target,
	}}, nil
}

// IsRepoAllowed returns whether the given repository is permitted to use SchemaBot.
// If the receiver is nil, Repos is empty, or Repos is nil, all repositories are
// allowed. If Repos is populated, only listed repositories are allowed.
func (c *ServerConfig) IsRepoAllowed(repo string) bool {
	if c == nil || len(c.Repos) == 0 {
		return true
	}
	_, ok := c.Repos[repo]
	return ok
}

// AreChecksEnabled returns whether SchemaBot should publish GitHub Check Runs
// for the given repository. Repositories not present in the server-side repo
// config use the default enabled behavior.
func (c *ServerConfig) AreChecksEnabled(repo string) bool {
	if c == nil || len(c.Repos) == 0 {
		return true
	}
	repoConfig, ok := c.Repos[repo]
	if !ok || repoConfig.EnableChecks == nil {
		return true
	}
	return *repoConfig.EnableChecks
}

// ResolvedGitHubApp identifies which configured GitHub App owns a repository.
// Name is the logical key under ServerConfig.Apps ("default" for the legacy
// single-App shape). Config is a copy of the resolved credentials.
type ResolvedGitHubApp struct {
	Name   string
	Config GitHubAppConfig
}

// ResolveGitHubAppForRepo returns the GitHub App that owns webhooks and
// outbound GitHub API calls for the given repository.
//
// Resolution rules:
//   - If ServerConfig.Apps is configured, the repo MUST be declared in
//     ServerConfig.Repos with a non-empty GitHubApp that names an entry in
//     ServerConfig.Apps. Unknown repos or unknown app names return an error.
//   - Otherwise the legacy single-App ServerConfig.GitHub is returned under
//     the synthetic name "default" so callers can treat both shapes uniformly.
//   - If neither Apps nor a configured GitHub is present, an error is returned.
func (c *ServerConfig) ResolveGitHubAppForRepo(repo string) (ResolvedGitHubApp, error) {
	if c == nil {
		return ResolvedGitHubApp{}, fmt.Errorf("server config is nil")
	}
	if len(c.Apps) > 0 {
		repoConfig, ok := c.Repos[repo]
		if !ok {
			return ResolvedGitHubApp{}, fmt.Errorf("repository %q is not declared in the repos config", repo)
		}
		if repoConfig.GitHubApp == "" {
			return ResolvedGitHubApp{}, fmt.Errorf("repository %q is missing github_app", repo)
		}
		appCfg, ok := c.Apps[repoConfig.GitHubApp]
		if !ok {
			return ResolvedGitHubApp{}, fmt.Errorf("repository %q references unknown github_app %q", repo, repoConfig.GitHubApp)
		}
		return ResolvedGitHubApp{Name: repoConfig.GitHubApp, Config: appCfg}, nil
	}
	if !c.GitHub.Configured() {
		return ResolvedGitHubApp{}, fmt.Errorf("no GitHub App is configured")
	}
	return ResolvedGitHubApp{Name: "default", Config: c.GitHub}, nil
}

// ResolveGitHubAppsByID resolves every configured App's app-id and returns
// a map keyed by the resolved int64 ID. Used at startup by the webhook
// runtime to build the inbound-dispatch table that maps the App ID carried
// in the X-GitHub-Hook-Installation-Target-ID header to the configured App
// name.
//
// Returns an error if any App has an empty or unparseable app-id, or if two
// configured Apps resolve to the same ID — both are ambiguous misconfigurations
// that would make header-keyed dispatch undefined.
//
// Legacy single-App configs (ServerConfig.GitHub set, ServerConfig.Apps empty)
// are also resolved so callers can use a single uniform path; the resulting
// map will contain a single entry under name "default".
func (c *ServerConfig) ResolveGitHubAppsByID() (map[int64]ResolvedGitHubApp, error) {
	if c == nil {
		return nil, fmt.Errorf("server config is nil")
	}
	apps := c.Apps
	if len(apps) == 0 {
		if !c.GitHub.Configured() {
			return nil, fmt.Errorf("no GitHub App is configured")
		}
		apps = map[string]GitHubAppConfig{"default": c.GitHub}
	}
	out := make(map[int64]ResolvedGitHubApp, len(apps))
	for name, app := range apps {
		id := app.ResolveAppID()
		if id == 0 {
			return nil, fmt.Errorf("app %q has empty or unparseable app-id", name)
		}
		if existing, ok := out[id]; ok {
			return nil, fmt.Errorf("apps %q and %q resolve to the same app-id %d", existing.Name, name, id)
		}
		out[id] = ResolvedGitHubApp{Name: name, Config: app}
	}
	return out, nil
}

// IsEnvironmentAllowed returns whether the given environment is handled by this
// SchemaBot instance. If the receiver is nil, AllowedEnvironments is empty, or
// AllowedEnvironments is nil, all environments are allowed.
func (c *ServerConfig) IsEnvironmentAllowed(env string) bool {
	if c == nil || len(c.AllowedEnvironments) == 0 {
		return true
	}
	return slices.Contains(c.AllowedEnvironments, env)
}

// PromotionEnvironmentOrder returns the server-owned environment promotion
// order used by PR apply gating.
func (c *ServerConfig) PromotionEnvironmentOrder() []string {
	if c == nil || len(c.EnvironmentOrder) == 0 {
		return slices.Clone(defaultEnvironmentOrder)
	}
	return slices.Clone(c.EnvironmentOrder)
}

// OrderedEnvironments returns the enabled environments sorted by the server-owned
// promotion order. Unknown environments are appended alphabetically so callers
// get deterministic behavior even before a custom environment_order is added.
func (c *ServerConfig) OrderedEnvironments(enabled []string) []string {
	enabledSet := make(map[string]struct{}, len(enabled))
	for _, env := range enabled {
		if env == "" {
			continue
		}
		enabledSet[env] = struct{}{}
	}

	ordered := make([]string, 0, len(enabledSet))
	for _, env := range c.PromotionEnvironmentOrder() {
		if _, ok := enabledSet[env]; ok {
			ordered = append(ordered, env)
			delete(enabledSet, env)
		}
	}

	if len(enabledSet) == 0 {
		return ordered
	}

	remaining := make([]string, 0, len(enabledSet))
	for env := range enabledSet {
		remaining = append(remaining, env)
	}
	slices.Sort(remaining)
	return append(ordered, remaining...)
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

// ShouldRequirePassingChecks returns whether apply should be blocked when
// non-SchemaBot PR checks are failing. Defaults to true when not configured.
func (c *ServerConfig) ShouldRequirePassingChecks() bool {
	if c == nil || c.RequirePassingChecks == nil {
		return true
	}
	return *c.RequirePassingChecks
}

// IsCheckRequired returns whether a PR check name is part of the configured
// checks gate. When no names are configured, every non-SchemaBot check remains
// in scope.
func (c *ServerConfig) IsCheckRequired(name string) bool {
	if c == nil || len(c.RequiredChecks) == 0 {
		return true
	}
	return slices.Contains(c.RequiredChecks, name)
}

// GitHubCheckNameBaseForRepo returns the aggregate GitHub Check Run base name
// for the App that owns repo.
func (c *ServerConfig) GitHubCheckNameBaseForRepo(repo string) string {
	if c == nil {
		return DefaultGitHubCheckName
	}
	if len(c.Apps) > 0 {
		repoConfig, ok := c.Repos[repo]
		if !ok {
			return DefaultGitHubCheckName
		}
		appConfig, ok := c.Apps[repoConfig.GitHubApp]
		if !ok {
			return DefaultGitHubCheckName
		}
		return appConfig.CheckRunNameBase()
	}
	return c.GitHub.CheckRunNameBase()
}

// StorageDSN returns the resolved storage DSN.
// It handles special prefixes (env:, file:) to read from various sources and
// can build a DSN from separate config/password references.
// Falls back to MYSQL_DSN environment variable if not configured.
func (c *ServerConfig) StorageDSN() (string, error) {
	if c.Storage.DSNFrom != nil {
		return c.Storage.DSNFrom.Resolve()
	}
	return secrets.Resolve(c.Storage.DSN, "MYSQL_DSN")
}

func (c StorageConfig) validateLocalDSNConfig(context string) error {
	if c.DSN != "" && c.DSNFrom != nil {
		return fmt.Errorf("%s cannot configure both dsn and dsn_from", context)
	}
	if c.DSNFrom != nil {
		return c.DSNFrom.Validate(context)
	}
	return nil
}

// HasLocalDSN returns true when the environment should use a local database connection.
func (c EnvironmentConfig) HasLocalDSN() bool {
	return c.DSN != "" || c.DSNFrom != nil
}

func (c EnvironmentConfig) validateLocalDSNConfig(context string) error {
	if c.DSN != "" && c.DSNFrom != nil {
		return fmt.Errorf("%s cannot configure both dsn and dsn_from", context)
	}
	if c.DSNFrom != nil {
		return c.DSNFrom.Validate(context)
	}
	return nil
}

func (c EnvironmentConfig) ResolveDSN() (string, error) {
	if c.DSNFrom != nil {
		return c.DSNFrom.Resolve()
	}
	return secrets.Resolve(c.DSN, "")
}

func (c *DSNFromConfig) Validate(context string) error {
	if c.ConfigRef == "" {
		return fmt.Errorf("%s dsn_from missing config_ref", context)
	}
	paths := c.configPaths()
	if paths.Host == "" {
		return fmt.Errorf("%s dsn_from missing config_paths.host", context)
	}
	if paths.Database == "" {
		return fmt.Errorf("%s dsn_from missing config_paths.database", context)
	}
	if c.Username == "" {
		return fmt.Errorf("%s dsn_from missing username", context)
	}
	if c.PasswordRef == "" {
		return fmt.Errorf("%s dsn_from missing password_ref", context)
	}
	return nil
}

func (c *DSNFromConfig) Resolve() (string, error) {
	if err := c.Validate("database connection"); err != nil {
		return "", err
	}

	configYAML, err := secrets.Resolve(c.ConfigRef, "")
	if err != nil {
		return "", fmt.Errorf("resolve database config reference: %w", err)
	}

	var config any
	if err := yaml.Unmarshal([]byte(configYAML), &config); err != nil {
		return "", fmt.Errorf("parse database config: %w", err)
	}

	paths := c.configPaths()
	host, err := stringAtPath(config, paths.Host)
	if err != nil {
		return "", fmt.Errorf("read database config host: %w", err)
	}
	database, err := stringAtPath(config, paths.Database)
	if err != nil {
		return "", fmt.Errorf("read database config database: %w", err)
	}
	port, err := optionalIntAtPath(config, paths.Port)
	if err != nil {
		return "", fmt.Errorf("read database config port: %w", err)
	}
	endpoint := externalDatabaseEndpoint{Host: host, Port: port, Database: database}
	if err := endpoint.validate(); err != nil {
		return "", err
	}

	password, err := secrets.Resolve(c.PasswordRef, "")
	if err != nil {
		return "", fmt.Errorf("resolve database password reference: %w", err)
	}

	mysqlConfig := gomysql.NewConfig()
	mysqlConfig.Net = "tcp"
	mysqlConfig.Addr = endpoint.address()
	mysqlConfig.User = c.Username
	mysqlConfig.Passwd = password
	mysqlConfig.DBName = endpoint.Database
	mysqlConfig.Params = c.Params

	return mysqlConfig.FormatDSN(), nil
}

func (c *DSNFromConfig) configPaths() DSNFromConfigPaths {
	paths := c.ConfigPaths
	if paths.Host == "" {
		paths.Host = "host"
	}
	if paths.Port == "" {
		paths.Port = "port"
	}
	if paths.Database == "" {
		paths.Database = "database"
	}
	return paths
}

func stringAtPath(document any, path string) (string, error) {
	value, err := valueAtPath(document, path)
	if err != nil {
		return "", err
	}
	text, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("path %q must contain a string", path)
	}
	return text, nil
}

func optionalIntAtPath(document any, path string) (int, error) {
	if path == "" {
		return 0, nil
	}
	value, err := valueAtPath(document, path)
	if err != nil {
		if errors.Is(err, errPathNotFound) {
			return 0, nil
		}
		return 0, err
	}
	switch v := value.(type) {
	case int:
		return v, nil
	case int64:
		return int(v), nil
	case float64:
		if v != float64(int(v)) {
			return 0, fmt.Errorf("path %q must contain an integer", path)
		}
		return int(v), nil
	case string:
		if v == "" {
			return 0, nil
		}
		port, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("path %q must contain an integer: %w", path, err)
		}
		return port, nil
	default:
		return 0, fmt.Errorf("path %q must contain an integer", path)
	}
}

var errPathNotFound = errors.New("path not found")

func valueAtPath(document any, path string) (any, error) {
	current := document
	for segment := range strings.SplitSeq(path, ".") {
		if segment == "" {
			return nil, fmt.Errorf("path %q contains an empty segment", path)
		}
		m, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("path %q segment %q does not select a map", path, segment)
		}
		next, ok := m[segment]
		if !ok {
			return nil, fmt.Errorf("%w: %s", errPathNotFound, path)
		}
		current = next
	}
	return current, nil
}

func (e externalDatabaseEndpoint) validate() error {
	if e.Host == "" {
		return fmt.Errorf("database config missing host")
	}
	if e.Database == "" {
		return fmt.Errorf("database config missing database")
	}
	return nil
}

func (e externalDatabaseEndpoint) address() string {
	if _, _, err := net.SplitHostPort(e.Host); err == nil {
		return e.Host
	}
	if e.Port == 0 {
		return net.JoinHostPort(e.Host, "3306")
	}
	return net.JoinHostPort(e.Host, strconv.Itoa(e.Port))
}
