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

// skipUnownedUnscopedCommand reports whether a "schema not owned by this
// deployment" error should be silently ignored instead of reported on the PR.
// On an aggregate repo (leader or participant), an unscoped command (no -t) is a
// fan-out broadcast every installed deployment receives; a deployment that owns
// none of the changed schema is expected to do nothing while the deployment that
// does own it handles the command. Posting "config not authorized" or "database
// not configured" from every non-owning deployment would be exactly the noise
// fan-out removes. Scoped to the not-owned error classes only — real failures
// still surface. A -t-scoped command (tenant != "") always reports, since it
// named a specific deployment.
func (h *Handler) skipUnownedUnscopedCommand(repo, tenant string, err error) bool {
	if tenant != "" {
		return false
	}
	config, ok := h.serverConfig()
	if !ok || config.AggregateRoleForRepo(repo) == "" {
		return false
	}
	return isSchemaUnownedByDeploymentError(err)
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

type schemaConfigOutsideAllowedDirsError struct {
	Database     string
	DatabaseType string
	SchemaPath   string
}

func (e *schemaConfigOutsideAllowedDirsError) Error() string {
	return fmt.Sprintf("schema config for database %q at %q is outside server allowed_dirs", e.Database, e.SchemaPath)
}

func newSchemaConfigOutsideAllowedDirsError(config *ghclient.SchemabotConfig, schemaPath string) error {
	if config == nil {
		return ghclient.ErrNoConfig
	}
	return &schemaConfigOutsideAllowedDirsError{
		Database:     config.Database,
		DatabaseType: string(config.GetType()),
		SchemaPath:   schemaPath,
	}
}

func (h *Handler) createManagedSchemaRequestFromPR(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, environment, databaseName, source string) (*ghclient.SchemaRequestResult, error) {
	schemaResult, err := client.CreateSchemaRequestFromPR(ctx, repo, pr, environment, databaseName, h.validateRequestedDatabaseEnvironment)
	if err != nil {
		return nil, err
	}
	if !h.shouldProcessSchemaConfig(ctx, repo, pr, schemaResult.HeadSHA, schemaResult.Database, schemaResult.Type, schemaResult.SchemaPath, source) {
		return nil, &schemaConfigOutsideAllowedDirsError{
			Database:     schemaResult.Database,
			DatabaseType: schemaResult.Type,
			SchemaPath:   schemaResult.SchemaPath,
		}
	}
	return schemaResult, nil
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
	return fmt.Errorf("database %q environment %q is not configured on this server", database, environment)
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
		return true
	}

	if !config.SchemaPathAllowedForRepo(repo, schemaPath) {
		metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
			Operation:  "schema_config_source_policy",
			Repository: repo,
			Status:     "skipped",
		})
		h.logger.Info("schema config is outside repo allowed_dirs and will be ignored",
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
