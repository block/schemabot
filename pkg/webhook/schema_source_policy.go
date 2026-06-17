package webhook

import (
	"context"
	"fmt"
	"slices"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
)

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
