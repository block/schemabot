package webhook

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
)

// unmanagedServerManagedSchemaChanges returns the changed schema files in the PR
// that live under a directory the server config manages
// (databases.<db>.allowed_dirs) but are not covered by any discovered
// schemabot.yaml. A non-empty result means the PR is changing server-owned
// schema with no managing config — for example because it removed the config
// while keeping schema files.
//
// Returns nil when the repository has no server-side schema-directory allowlist
// (open mode): the server has no authoritative managed-path knowledge there, so
// this protection does not apply and discovery behaves as before. To offboard a
// directory, an operator removes it from databases.<db>.allowed_dirs.
func (h *Handler) unmanagedServerManagedSchemaChanges(repo string, files []ghclient.PRFile, configs []ghclient.DiscoveredConfig) []string {
	config, ok := h.serverConfig()
	if !ok || !config.RepoHasSchemaDirAllowlist(repo) {
		return nil
	}

	var unmanaged []string
	for _, f := range files {
		if !ghclient.IsSchemaFile(f.Filename) {
			continue
		}
		// A schema file being deleted is not an unmanaged change; only files
		// present at the PR head can land DDL without a managing config.
		if strings.EqualFold(f.Status, "removed") {
			continue
		}
		if schemaFileCoveredByConfig(f.Filename, configs) {
			continue
		}
		if config.SchemaPathAllowedForRepo(repo, f.Filename) {
			unmanaged = append(unmanaged, f.Filename)
		}
	}
	return unmanaged
}

// schemaFileCoveredByConfig reports whether filename resolves to one of the
// discovered configs — i.e. a config's directory is an ancestor of (or equal
// to) the file's directory, the same nearest-ancestor rule discovery uses.
func schemaFileCoveredByConfig(filename string, configs []ghclient.DiscoveredConfig) bool {
	dir := path.Dir(filename)
	for _, cfg := range configs {
		// A repo-root config (SchemaDir ".") is an ancestor of every directory,
		// so it covers schema files anywhere in the repo.
		if cfg.SchemaDir == "." || dir == cfg.SchemaDir || strings.HasPrefix(dir, cfg.SchemaDir+"/") {
			return true
		}
	}
	return false
}

// failClosedOnUnmanagedSchemaDir publishes a blocking aggregate naming the
// server-managed directories whose schemabot.yaml is missing and how to resolve
// it: restore the config, or offboard the directory by removing it from
// databases.<db>.allowed_dirs in the server config.
func (h *Handler) failClosedOnUnmanagedSchemaDir(ctx context.Context, client *ghclient.InstallationClient, repo string, pr int, headSHA, source string, unmanagedFiles []string) {
	h.logger.Warn("failing aggregate closed: schema change under server-managed directory has no schemabot.yaml",
		"repo", repo, "pr", pr, "head_sha", headSHA, "source", source,
		"files", strings.Join(unmanagedFiles, ","))
	metrics.RecordStatusCheckOperation(ctx, metrics.StatusCheckOperation{
		Operation:  "managed_dir_missing_config",
		Repository: repo,
		Status:     "blocked",
	})

	message := h.unmanagedSchemaDirMessage(repo, unmanagedFiles)
	h.postFailingAggregatesWithBlock(ctx, client, repo, pr, headSHA,
		h.aggregateMessagesForAllEnvironments(message), managedDirMissingConfigBlock)
}

// unmanagedSchemaDirMessage builds the operator-facing block message, naming
// each unmanaged schema file and the managed database that owns its directory.
func (h *Handler) unmanagedSchemaDirMessage(repo string, unmanagedFiles []string) string {
	config, _ := h.serverConfig()

	lines := make([]string, 0, len(unmanagedFiles))
	for _, f := range unmanagedFiles {
		db := "an unknown database"
		if config != nil {
			if name, ok := config.DatabaseForSchemaPath(repo, f); ok {
				db = fmt.Sprintf("database `%s`", name)
			}
		}
		lines = append(lines, fmt.Sprintf("- `%s` (%s)", f, db))
	}
	sort.Strings(lines)

	return strings.Join([]string{
		"This PR changes schema files under directories SchemaBot manages via server config, but no `schemabot.yaml` config resolves for them:",
		"",
		strings.Join(lines, "\n"),
		"",
		"Restore the `schemabot.yaml` config for these directories, or ask a SchemaBot operator to remove the directory from `allowed_dirs` in the server config to offboard it.",
	}, "\n")
}
