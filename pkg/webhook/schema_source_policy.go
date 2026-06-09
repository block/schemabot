package webhook

import (
	"context"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
)

func (h *Handler) createManagedSchemaRequestFromPR(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, environment, databaseName, source string) (*ghclient.SchemaRequestResult, error) {
	schemaResult, err := client.CreateSchemaRequestFromPR(ctx, repo, pr, environment, databaseName)
	if err != nil {
		return nil, err
	}
	if !h.shouldProcessSchemaConfig(ctx, repo, pr, schemaResult.HeadSHA, schemaResult.Database, schemaResult.Type, schemaResult.SchemaPath, source) {
		return nil, ghclient.ErrNoConfig
	}
	return schemaResult, nil
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
