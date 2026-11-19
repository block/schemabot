package webhook

import (
	"context"
	"slices"

	ghclient "github.com/block/schemabot/pkg/github"
)

// readListenOnlyPlanInputs reads the GitHub inputs an auto-plan of a
// listen-only repository's database would read if this deployment registered
// the database: the schema files at the pull request head for each environment
// this deployment serves, then one base-branch comparison for all of them.
// The reads are the measurement. The data plane is never called, no check
// state is stored, and nothing is posted. The listen-only transport would
// refuse a write anyway.
//
// An environment whose read fails is logged and skipped. Nothing is gated on
// a listen-only repository, so there is nothing to fail closed, and the
// remaining environments are still worth measuring.
func (h *Handler) readListenOnlyPlanInputs(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, config *ghclient.SchemabotConfig, configDir string) {
	cfg := h.service.Config()
	environments := cfg.OrderedEnvironments(cfg.AllowedEnvironments)
	if len(environments) == 0 {
		// A deployment that serves every environment names none, so read the
		// environments of the promotion order. Each resolves to its own
		// directory where the repository keeps one, and to the config
		// directory otherwise. Reading the config directory alone would miss
		// a repository that keeps one directory per environment, because
		// the reader looks only one namespace level below the root.
		environments = cfg.PromotionEnvironmentOrder()
	}

	schemas := make(map[string]*ghclient.SchemaRequestResult, len(environments))
	schemaFiles := 0
	for _, env := range environments {
		result, err := client.CreateSchemaRequestForConfig(ctx, repo, pr, env, config, configDir, nil)
		if err != nil {
			h.logger.Warn("listen-only plan read failed; this environment's plan cost is only partly measured",
				"repo", repo, "pr", pr, "database", config.Database, "database_type", config.GetType(),
				"environment", env, "config_dir", configDir, "error", err)
			continue
		}
		schemas[env] = result
		for _, namespace := range result.SchemaFiles {
			schemaFiles += len(namespace.GetFiles())
		}
	}
	if len(schemas) > 0 {
		// The comparison's findings name a plan comment and an apply block.
		// A listen-only repository gets neither, and on a busy repository a
		// stale branch is routine, so they are debug lines here.
		h.compareBaseSchemas(ctx, client, repo, pr, schemas, func(msg string, args ...any) {
			h.logger.Debug(msg, append(args, "listen_only", true)...)
		})
	}

	read := make([]string, 0, len(schemas))
	for env := range schemas {
		read = append(read, env)
	}
	slices.Sort(read)
	h.logger.Info("read a listen-only repository's plan inputs for a database this deployment does not register; nothing was planned or posted",
		"repo", repo, "pr", pr, "database", config.Database, "database_type", config.GetType(),
		"config_dir", configDir, "environments", read, "schema_files", schemaFiles)
}
