package webhook

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
)

// silentDiscoveryFailureOnUnscopedFanOut reports whether a failed schema
// discovery for an unscoped (no -t) command should be a logged silent skip
// rather than a PR comment. Two independent outcomes qualify — the discovered
// schema belongs to another deployment, or a participant's partial view cannot
// resolve the named database — and they belong together here because they say
// the same thing to the user: a different deployment owns this command's
// answer, so a reply from this one would land as duplicate noise beside the
// real one. Every other failure still surfaces, and a -t-scoped command
// (tenant != "") always reports, since it named a specific deployment.
func (h *Handler) silentDiscoveryFailureOnUnscopedFanOut(repo, tenant string, err error) bool {
	if tenant != "" {
		return false
	}
	return h.silentUnownedSchemaOnAggregateFanOut(repo, err) ||
		h.silentUnresolvedDatabaseOnParticipantFanOut(repo, err)
}

// silentUnownedSchemaOnAggregateFanOut reports whether a "schema not owned by
// this deployment" error should be silently ignored instead of reported on the
// PR. On an aggregate repo (leader or participant), an unscoped command is a
// fan-out broadcast every installed deployment receives; a deployment that owns
// none of the changed schema is expected to do nothing while the deployment
// that does own it handles the command. Posting "config not authorized" or
// "database not configured" from every non-owning deployment would be exactly
// the noise fan-out removes.
func (h *Handler) silentUnownedSchemaOnAggregateFanOut(repo string, err error) bool {
	config, ok := h.serverConfig()
	if !ok {
		return false
	}
	if config.AggregateRoleForRepo(repo) == "" {
		return false
	}
	return isSchemaUnownedByDeploymentError(err)
}

// silentUnresolvedDatabaseOnParticipantFanOut reports whether a database
// discovery miss should be silently deferred on this deployment. A
// participant's discovery covers only its own slice of the fleet, so an
// authoritative "database not found" from that local view cannot establish
// that the fleet has no matching schema — the aggregate leader, whose view
// spans the fleet, answers instead while the participant stays silent. Only
// the authoritative miss defers: an uncertain outcome (for example a truncated
// repository tree the configured schema directory hints cannot recover) still
// surfaces fail-closed, because the participant might own the database and
// simply be unable to prove it.
func (h *Handler) silentUnresolvedDatabaseOnParticipantFanOut(repo string, err error) bool {
	config, ok := h.serverConfig()
	if !ok {
		return false
	}
	if config.AggregateRoleForRepo(repo) != api.AggregateRoleParticipant {
		return false
	}
	var notFound *ghclient.DatabaseNotFoundError
	return errors.As(err, &notFound)
}

// isSchemaUnownedByDeploymentError reports whether err means the command
// resolved to schema another deployment owns: either the schema config lives
// outside this server's allowed_dirs, or the discovered database has no entry
// in this server's databases registry at all. Under the aggregate contract both
// mean the same thing — this deployment is not the owner — so on unscoped
// fan-out both are silently skipped rather than reported. Anything else is a
// real failure and must still surface.
func isSchemaUnownedByDeploymentError(err error) bool {
	var notOwned *schemaConfigOutsideAllowedDirsError
	if errors.As(err, &notOwned) {
		return true
	}
	var notConfigured *api.DatabaseNotConfiguredError
	return errors.As(err, &notConfigured)
}

// silentOnUnscopedFanOut reports whether a "nothing to do on this deployment"
// outcome for an unscoped (no -t) command should be a logged silent skip rather
// than a PR comment. On an aggregate repo (leader or participant) an unscoped
// command fans out to every deployment, so one that finds no pending work — for
// example apply-confirm after this deployment's own databases already
// auto-applied and released their locks — must stay quiet; only the deployment
// that actually has work to confirm responds. A -t-scoped command (tenant != "")
// named a specific deployment, so its "nothing to do" answer is useful and still
// surfaces.
func (h *Handler) silentOnUnscopedFanOut(repo, tenant string) bool {
	if tenant != "" {
		return false
	}
	config, ok := h.serverConfig()
	if !ok {
		return false
	}
	return config.AggregateRoleForRepo(repo) != ""
}

// silentUsageErrorOnUnscopedFanOut reports whether a usage-error reply (a bad
// or missing flag or argument) to an unscoped (no -t) command should be a
// logged silent skip on this deployment. A usage error is decidable by every
// deployment from the comment text alone, so on an aggregate repo a fan-out
// would post one copy per participant. Unlike a "nothing to do on this
// deployment" outcome (silentOnUnscopedFanOut), no owner is going to answer —
// the command is malformed everywhere — so participants stay silent and the
// leader posts the error exactly once. A -t-scoped command named this
// deployment, so its answer always surfaces.
func (h *Handler) silentUsageErrorOnUnscopedFanOut(repo, tenant string) bool {
	if tenant != "" {
		return false
	}
	config, ok := h.serverConfig()
	if !ok {
		return false
	}
	return config.AggregateRoleForRepo(repo) == api.AggregateRoleParticipant
}

// silentUnknownEnvOnAggregateFanOut reports whether an unknown-environment
// rejection should be a logged silent skip on this deployment. An aggregate
// participant's config holds only its own slice of the fleet's environments,
// so an -e value it does not recognize may be a perfectly valid environment
// served by a sibling deployment — rejecting it from that partial worldview
// posts a spurious "Invalid Environment" next to the sibling's real work.
// Participants therefore defer silently even when the command names their own
// tenant, since the same tenant name can be served in other environments by
// sibling deployments. On unscoped commands the leader, whose environment
// order spans the fleet, still rejects a genuinely unknown value exactly once.
func (h *Handler) silentUnknownEnvOnAggregateFanOut(repo string) bool {
	config, ok := h.serverConfig()
	if !ok {
		return false
	}
	return config.AggregateRoleForRepo(repo) == api.AggregateRoleParticipant
}

// environmentNotConfiguredError reports that the requested environment has no
// entry for the database on this server — a targeting rejection the same
// command will always reproduce, not a transient failure.
type environmentNotConfiguredError struct {
	Database    string
	Environment string
}

func (e *environmentNotConfiguredError) Error() string {
	return fmt.Sprintf("database %q environment %q is not configured on this server", e.Database, e.Environment)
}

type schemaConfigOutsideAllowedDirsError struct {
	Database     string
	DatabaseType string
	SchemaPath   string
}

func (e *schemaConfigOutsideAllowedDirsError) Error() string {
	return fmt.Sprintf("schema config for database %q at %q is outside server allowed_dirs", e.Database, e.SchemaPath)
}

// unownedSchemaConfigError describes why a discovered schema config is not
// this deployment's to process, matching the error class to the ownership
// contract that dropped it. When the repo has a directory allowlist, the
// config was outside it. In open mode (no allowlist for the repo) the only
// drop reason is the database registry, so the database is reported as not
// configured — an allowed_dirs remediation would be misleading on a repo that
// has no allowlist to amend. Both classes count as unowned for unscoped
// fan-out silencing (isSchemaUnownedByDeploymentError); the distinction only
// changes what a -t/-d-scoped command reports.
func (h *Handler) unownedSchemaConfigError(repo, database, databaseType, schemaPath string) error {
	if config, ok := h.serverConfig(); ok && !config.RepoHasSchemaDirAllowlist(repo) {
		return &api.DatabaseNotConfiguredError{Database: database}
	}
	return &schemaConfigOutsideAllowedDirsError{
		Database:     database,
		DatabaseType: databaseType,
		SchemaPath:   schemaPath,
	}
}

// unownedDiscoveredConfigError is unownedSchemaConfigError for a discovered
// config that may have failed to parse: a nil config carries no database
// identity to report, so it surfaces as ErrNoConfig instead.
func (h *Handler) unownedDiscoveredConfigError(repo string, config *ghclient.SchemabotConfig, schemaPath string) error {
	if config == nil {
		return ghclient.ErrNoConfig
	}
	return h.unownedSchemaConfigError(repo, config.Database, string(config.GetType()), schemaPath)
}

func (h *Handler) createManagedSchemaRequestFromPR(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, environment, databaseName, source string) (*ghclient.SchemaRequestResult, error) {
	var schemaResult *ghclient.SchemaRequestResult
	var err error
	if databaseName == "" {
		schemaResult, err = h.createUnscopedSchemaRequestFromPR(ctx, client, repo, pr, environment, source)
	} else {
		schemaResult, err = client.CreateSchemaRequestFromPR(ctx, repo, pr, environment, databaseName, h.validateRequestedDatabaseEnvironment)
	}
	if err != nil {
		return nil, err
	}
	// Re-check the resolved schema root: environment resolution can retarget it
	// (for example through an environment symlink), so ownership must hold for
	// the path the schema files were actually fetched from, not just the
	// discovered config directory.
	if !h.shouldProcessSchemaConfig(ctx, repo, pr, schemaResult.HeadSHA, schemaResult.Database, schemaResult.Type, schemaResult.SchemaPath, source) {
		return nil, h.unownedSchemaConfigError(repo, schemaResult.Database, schemaResult.Type, schemaResult.SchemaPath)
	}
	return schemaResult, nil
}

// createUnscopedSchemaRequestFromPR resolves which database an unscoped
// command (no -d) targets and fetches that database's schema files.
func (h *Handler) createUnscopedSchemaRequestFromPR(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, environment, source string) (*ghclient.SchemaRequestResult, error) {
	config, configDir, err := h.resolveUnscopedManagedConfig(ctx, client, repo, pr, source)
	if err != nil {
		return nil, err
	}
	return client.CreateSchemaRequestForConfig(ctx, repo, pr, environment, config, configDir, h.validateRequestedDatabaseEnvironment)
}

// resolveUnscopedManagedConfig resolves which database an unscoped command
// (no -d) targets. Discovery mirrors auto-plan — changed config files plus the
// nearest config above each changed schema file — and the discovered configs
// are filtered to the ones this deployment manages before deciding
// multiplicity, so on fan-out repos each deployment decides from its own slice:
//
//   - no configs discovered at all: fall back to repo-wide single-config
//     discovery (a changeless PR can still carry commands for its database)
//   - none of the discovered configs managed here: an unowned-schema error,
//     which unscoped fan-out handling downgrades to a silent skip
//   - exactly one managed config: the command targets it, even when the PR
//     also touches configs other deployments own
//   - several managed configs: ErrMultipleConfigs — one apply drives one
//     database, so the user must scope the command with -d
func (h *Handler) resolveUnscopedManagedConfig(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, source string) (*ghclient.SchemabotConfig, string, error) {
	configs, err := client.FindAllConfigsForPR(ctx, repo, pr)
	if err != nil {
		return nil, "", err
	}
	if len(configs) == 0 {
		config, configDir, _, err := client.FindConfigInRepo(ctx, repo, pr)
		if err != nil {
			return nil, "", err
		}
		if !h.configPathManagedByRepo(ctx, repo, pr, "", config, configDir, source) {
			return nil, "", h.unownedDiscoveredConfigError(repo, config, configDir)
		}
		return config, configDir, nil
	}
	// filterManagedDiscoveredConfigs filters in place, so give it a copy —
	// callers may still need the full discovery result.
	managed := h.filterManagedDiscoveredConfigs(ctx, repo, pr, "", source, slices.Clone(configs))
	if len(managed) == 0 {
		h.logger.Info("unscoped command discovered only schema configs this deployment does not manage",
			"repo", repo, "pr", pr, "source", source,
			"discovered_configs", len(configs))
		return nil, "", h.unownedDiscoveredConfigError(repo, configs[0].Config, configs[0].SchemaDir)
	}
	// The directory allowlist is only half the ownership contract: on repos
	// partitioned by database registry rather than allowed_dirs, a config for
	// another deployment's database passes the directory filter here. Narrow to
	// the configs whose database this deployment has registered before deciding
	// multiplicity, so a PR spanning several deployments' databases is not
	// falsely ambiguous for the one that owns a single database in it.
	registered := h.registeredDiscoveredConfigs(managed)
	if len(registered) == 0 {
		if len(managed) == 1 {
			return managed[0].Config, managed[0].SchemaDir, nil
		}
		h.logger.Info("unscoped command discovered only schema configs for databases not registered on this deployment",
			"repo", repo, "pr", pr, "source", source,
			"discovered_configs", len(configs), "managed_configs", len(managed))
		return nil, "", &api.DatabaseNotConfiguredError{Database: managed[0].Config.Database}
	}
	return ghclient.SingleDiscoveredConfig(registered)
}

// registeredDiscoveredConfigs narrows discovered configs to the ones whose
// database has an entry in this deployment's databases registry.
func (h *Handler) registeredDiscoveredConfigs(configs []ghclient.DiscoveredConfig) []ghclient.DiscoveredConfig {
	config, ok := h.serverConfig()
	if !ok {
		return configs
	}
	var registered []ghclient.DiscoveredConfig
	for _, cfg := range configs {
		if config.Database(cfg.Config.Database) != nil {
			registered = append(registered, cfg)
		}
	}
	return registered
}

func (h *Handler) validateRequestedDatabaseEnvironment(database, environment string) error {
	if environment == "" {
		return nil
	}
	environments, err := h.configuredDatabaseEnvironments(database)
	if err != nil {
		return fmt.Errorf("resolve configured environments for database %q: %w", database, err)
	}
	if slices.Contains(environments, environment) {
		return nil
	}
	return &environmentNotConfiguredError{Database: database, Environment: environment}
}

func (h *Handler) configPathManagedByRepo(ctx context.Context, repo string, pr int, headSHA string, config *ghclient.SchemabotConfig, schemaPath, source string) bool {
	if config == nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "schema_config_discovery",
			Repository: repo,
			Status:     "skipped",
		})
		h.logger.Warn("schema config is missing parsed config and will be ignored",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"schema_path", schemaPath, "source", source)
		return false
	}
	return h.shouldProcessSchemaConfig(ctx, repo, pr, headSHA, config.Database, string(config.GetType()), schemaPath, source)
}

func (h *Handler) shouldProcessSchemaConfig(ctx context.Context, repo string, pr int, headSHA, database, databaseType, schemaPath, source string) bool {
	config, ok := h.serverConfig()
	if !ok {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "schema_config_source_policy",
			Repository:   repo,
			Database:     database,
			DatabaseType: databaseType,
			Status:       "error",
		})
		h.logger.Warn("schema config source policy cannot be evaluated because server config is unavailable",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", database, "database_type", databaseType,
			"schema_path", schemaPath, "source", source)
		return true
	}

	if !config.RepoHasSchemaDirAllowlist(repo) {
		// Open mode: with no directory allowlist for the repo, every discovered
		// config is this deployment's to process. On an aggregate-role repo,
		// however, ownership is partitioned across deployments by database
		// registry — a config for a database this deployment has not registered
		// belongs to a sibling deployment. Keeping it would plan a database this
		// deployment cannot resolve and convert routine fan-out into a failing
		// aggregate, so it is dropped: the leader gates on the owner's Check Run
		// via the expected-participants fold, and a participant stays silent.
		// Deployments that resolve databases dynamically instead of through the
		// registry keep open mode as-is — the registry says nothing about what
		// they manage.
		if config.AggregateRoleForRepo(repo) == "" || config.TargetResolver.Enabled() {
			return true
		}
		if config.Database(database) != nil {
			return true
		}
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "schema_config_source_policy",
			Repository:   repo,
			Database:     database,
			DatabaseType: databaseType,
			Status:       "skipped",
		})
		h.logger.Info("schema config on aggregate repo is for a database not registered on this deployment; its owning deployment handles it",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", database, "database_type", databaseType,
			"schema_path", schemaPath, "source", source)
		return false
	}

	if !config.SchemaPathAllowedForRepo(repo, schemaPath) {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "schema_config_source_policy",
			Repository: repo,
			Status:     "skipped",
		})
		// On an aggregate-role repo a config outside this deployment's
		// allowed_dirs is routine fan-out — another deployment owns it. On any
		// other repo nothing will ever act on the config, so warn.
		logDropped := h.logger.Warn
		if config.AggregateRoleForRepo(repo) != "" {
			logDropped = h.logger.Info
		}
		logDropped("schema config is outside repo allowed_dirs and will be ignored",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", database, "database_type", databaseType,
			"schema_path", schemaPath, "source", source)
		return false
	}

	if config.Database(database) == nil {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:    "schema_config_source_policy",
			Repository:   repo,
			Database:     database,
			DatabaseType: databaseType,
			Status:       "error",
		})
		h.logger.Warn("schema config is inside repo allowed_dirs but database is not configured",
			"repo", repo, "pr", pr, "head_sha", headSHA,
			"database", database, "database_type", databaseType,
			"schema_path", schemaPath, "source", source)
	}

	return true
}
