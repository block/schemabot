package github

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// DatabaseType represents the type of database backend.
type DatabaseType string

const (
	DatabaseTypeVitess   DatabaseType = "vitess"
	DatabaseTypeMySQL    DatabaseType = "mysql"
	DatabaseTypeStrata   DatabaseType = "strata"
	DatabaseTypePostgres DatabaseType = "postgres"
)

// SchemabotConfig represents the schemabot.yaml configuration file.
// The presence of this file in a directory indicates that directory contains schema files.
type SchemabotConfig struct {
	Database string       `yaml:"database" json:"database"`
	Name     string       `yaml:"name" json:"name"`
	Type     DatabaseType `yaml:"type,omitempty" json:"type,omitempty"`
}

// GetType returns the database type. Type is always set — FetchConfig rejects empty values.
func (c *SchemabotConfig) GetType() DatabaseType {
	return c.Type
}

// DiscoveredConfig represents a schemabot.yaml config found via Tree API search.
type DiscoveredConfig struct {
	Config    *SchemabotConfig
	Path      string // Full path to schemabot.yaml file
	SchemaDir string // Directory containing schemabot.yaml
}

// ConfigFileName is the name of the schemabot config file.
const ConfigFileName = "schemabot.yaml"

// Config discovery errors.
var (
	ErrNoConfig        = fmt.Errorf("no schemabot.yaml config found")
	ErrInvalidConfig   = fmt.Errorf("invalid schemabot.yaml config found")
	ErrMultipleConfigs = fmt.Errorf("multiple schemabot.yaml configs found - use -d flag to specify database")
)

// Incomplete-data errors signal that GitHub returned a capped or truncated
// listing, so the set of files or directories SchemaBot examined may be partial.
var (
	ErrGitTreeTruncated  = fmt.Errorf("GitHub returned a truncated repository tree; config discovery is incomplete")
	ErrPRFilesIncomplete = fmt.Errorf("GitHub returned the maximum number of pull request files; config discovery is incomplete")
	ErrDirListingCapped  = fmt.Errorf("GitHub returned the maximum number of directory entries; schema discovery is incomplete")
)

// DatabaseNotFoundError indicates the specified database was not found in any config.
type DatabaseNotFoundError struct {
	DatabaseName       string
	AvailableDatabases []string
}

func (e *DatabaseNotFoundError) Error() string {
	if len(e.AvailableDatabases) == 0 {
		return fmt.Sprintf("database '%s' not found", e.DatabaseName)
	}
	return fmt.Sprintf("database '%s' not found. Available databases: %s",
		e.DatabaseName, strings.Join(e.AvailableDatabases, ", "))
}

// InvalidConfigInfo holds information about an invalid config file.
type InvalidConfigInfo struct {
	Path  string
	Error string
}

// FetchConfig fetches and parses the schemabot.yaml config file from a specific path.
func (ic *InstallationClient) FetchConfig(ctx context.Context, repo, configPath, ref string) (*SchemabotConfig, error) {
	content, err := ic.FetchFileContent(ctx, repo, configPath, ref)
	if err != nil {
		if IsNotFoundError(err) {
			return nil, ErrNoConfig
		}
		return nil, fmt.Errorf("fetch config at %s: %w", configPath, err)
	}

	var config SchemabotConfig
	decoder := yaml.NewDecoder(strings.NewReader(content))
	decoder.KnownFields(true)
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("invalid schemabot.yaml at %s: %w", configPath, err)
	}

	if config.Database == "" {
		return nil, fmt.Errorf("invalid schemabot.yaml at %s: database is required", configPath)
	}
	if config.Type == "" {
		return nil, fmt.Errorf("invalid schemabot.yaml at %s: type is required (must be 'vitess', 'mysql', 'strata', or 'postgres')", configPath)
	}
	switch config.Type {
	case DatabaseTypeVitess, DatabaseTypeMySQL, DatabaseTypeStrata, DatabaseTypePostgres:
	default:
		return nil, fmt.Errorf("invalid schemabot.yaml at %s: type must be 'vitess', 'mysql', 'strata', or 'postgres', got '%s'", configPath, config.Type)
	}

	return &config, nil
}

// FindAllConfigsResult contains both valid and invalid config information.
type FindAllConfigsResult struct {
	ValidConfigs   []DiscoveredConfig
	InvalidConfigs []InvalidConfigInfo
}

// FindAllConfigs uses the Tree API to discover all schemabot.yaml config files
// in the repository. When the repository is too large for the Trees API to
// list completely, discovery falls back to scanning the server-configured
// schema directories for the repo; when no such fallback is possible it fails
// closed with ErrGitTreeTruncated.
func (ic *InstallationClient) FindAllConfigs(ctx context.Context, repo, ref string) (*FindAllConfigsResult, error) {
	entries, truncated, err := ic.FetchGitTree(ctx, repo, ref)
	if err != nil {
		return nil, fmt.Errorf("fetch git tree: %w", err)
	}
	if truncated {
		result, ok, hintErr := ic.findConfigsInHintDirs(ctx, repo, ref)
		if hintErr != nil {
			return nil, fmt.Errorf("discover schemabot configs in configured schema dirs of repo %s ref %s: %w", repo, ref, hintErr)
		}
		if !ok {
			return nil, fmt.Errorf("discover schemabot configs in repo %s ref %s: %w", repo, ref, ErrGitTreeTruncated)
		}
		ic.logger.Info("git tree truncated; discovered configs from configured schema dirs",
			"repo", repo, "ref", ref,
			"valid", len(result.ValidConfigs), "invalid", len(result.InvalidConfigs))
		return result, nil
	}

	result := ic.collectConfigsFromTree(ctx, repo, ref, entries)

	ic.logger.Debug("config discovery complete",
		"valid", len(result.ValidConfigs),
		"invalid", len(result.InvalidConfigs),
		"repo", repo,
	)
	return result, nil
}

// collectConfigsFromTree fetches and parses every schemabot.yaml among the
// given tree entries. Unparseable configs are reported as invalid rather than
// failing the whole discovery.
func (ic *InstallationClient) collectConfigsFromTree(ctx context.Context, repo, ref string, entries []TreeEntry) *FindAllConfigsResult {
	result := &FindAllConfigsResult{}
	for _, entry := range entries {
		if entry.Type != "blob" || !strings.HasSuffix(entry.Path, ConfigFileName) {
			continue
		}
		config, err := ic.FetchConfig(ctx, repo, entry.Path, ref)
		if err != nil {
			ic.logger.Warn("failed to parse config", "path", entry.Path, "error", err)
			result.InvalidConfigs = append(result.InvalidConfigs, InvalidConfigInfo{
				Path:  entry.Path,
				Error: err.Error(),
			})
			continue
		}
		result.ValidConfigs = append(result.ValidConfigs, DiscoveredConfig{
			Config:    config,
			Path:      entry.Path,
			SchemaDir: path.Dir(entry.Path),
		})
	}
	return result
}

// findAllConfigsForDatabase discovers configs for a database-scoped lookup.
// With a complete tree it returns the full discovery result (scoped=false).
// With a truncated tree it probes only the named database's configured
// schema directories (scoped=true): absence there is authoritative for the
// database, but the result does not enumerate other databases' configs. When
// the database's directories cannot be probed exhaustively, it fails closed
// with ErrGitTreeTruncated.
func (ic *InstallationClient) findAllConfigsForDatabase(ctx context.Context, repo, ref, databaseName string) (*FindAllConfigsResult, bool, error) {
	entries, truncated, err := ic.FetchGitTree(ctx, repo, ref)
	if err != nil {
		return nil, false, fmt.Errorf("fetch git tree: %w", err)
	}
	if !truncated {
		return ic.collectConfigsFromTree(ctx, repo, ref, entries), false, nil
	}
	result, ok, hintErr := ic.findConfigsInDatabaseHintDirs(ctx, repo, ref, databaseName)
	if hintErr != nil {
		return nil, false, fmt.Errorf("discover schemabot config in configured schema dirs of database %s in repo %s ref %s: %w", databaseName, repo, ref, hintErr)
	}
	if !ok {
		return nil, false, fmt.Errorf("discover schemabot config for database %s in repo %s ref %s: %w", databaseName, repo, ref, ErrGitTreeTruncated)
	}
	ic.logger.Info("git tree truncated; scoped config discovery to the database's configured schema dirs",
		"repo", repo, "ref", ref, "database", databaseName,
		"valid", len(result.ValidConfigs), "invalid", len(result.InvalidConfigs))
	return result, true, nil
}

// findConfigsInHintDirs discovers schemabot.yaml configs by scanning the
// subtree of each server-configured schema directory for the repo. It serves
// repositories whose full recursive tree GitHub truncates, where a whole-repo
// scan is impossible. The scan is recursive per directory, so configs nested
// below a configured directory are found (allowed_dirs matching is
// prefix-based). The scan only runs when the hints are exhaustive — every
// policy-valid config location is covered by a hinted directory — because a
// partial probe returned as authoritative could report a single config where
// a full scan would find several, or omit a database whose config exists.
//
// ok is false when the client has no directory hints for the repo, the hints
// are not exhaustive, or the hinted directories contain no config files at
// all: an empty result cannot distinguish "repo has no configs" from "the
// hints do not cover the configs", so the caller must keep failing closed
// with the truncation error instead of reporting ErrNoConfig.
func (ic *InstallationClient) findConfigsInHintDirs(ctx context.Context, repo, ref string) (*FindAllConfigsResult, bool, error) {
	if ic.configDirHints == nil {
		ic.logger.Debug("no config dir hints configured; cannot scan truncated repo", "repo", repo, "ref", ref)
		return nil, false, nil
	}
	hints, exhaustive := ic.configDirHints.SchemaDirHintsForRepo(repo)
	if !exhaustive {
		ic.logger.Warn("configured schema dirs do not cover every policy-valid config location (a database allows configs outside any probe-able directory); keeping fail-closed truncation error",
			"repo", repo, "ref", ref, "hint_dirs", len(hints))
		return nil, false, nil
	}
	if len(hints) == 0 {
		ic.logger.Debug("no config dir hints for repo; cannot scan truncated repo", "repo", repo, "ref", ref)
		return nil, false, nil
	}

	result, err := ic.scanHintDirsForConfigs(ctx, repo, ref, hints)
	if err != nil {
		return nil, false, err
	}

	if len(result.ValidConfigs) == 0 && len(result.InvalidConfigs) == 0 {
		ic.logger.Warn("configured schema dirs contain no configs; keeping fail-closed truncation error",
			"repo", repo, "ref", ref, "hint_dirs", len(hints))
		return nil, false, nil
	}
	return result, true, nil
}

// findConfigsInDatabaseHintDirs discovers schemabot.yaml configs by scanning
// only the named database's configured schema directories, for lookups that
// already know which database they want. ok is false when the client has no
// hints or the database's directories are not exhaustive (its config could
// live outside any probe-able directory) — the caller must keep failing
// closed with the truncation error. Unlike the repo-wide scan, an empty
// result with exhaustive directories IS authoritative: no policy-valid
// location for this database's config was left unscanned.
func (ic *InstallationClient) findConfigsInDatabaseHintDirs(ctx context.Context, repo, ref, databaseName string) (*FindAllConfigsResult, bool, error) {
	if ic.configDirHints == nil {
		ic.logger.Debug("no config dir hints configured; cannot scan truncated repo",
			"repo", repo, "ref", ref, "database", databaseName)
		return nil, false, nil
	}
	hints, exhaustive := ic.configDirHints.SchemaDirHintsForDatabase(repo, databaseName)
	if !exhaustive {
		ic.logger.Warn("database's configured schema dirs do not cover every policy-valid config location; keeping fail-closed truncation error",
			"repo", repo, "ref", ref, "database", databaseName, "hint_dirs", len(hints))
		return nil, false, nil
	}

	result, err := ic.scanHintDirsForConfigs(ctx, repo, ref, hints)
	if err != nil {
		return nil, false, err
	}
	return result, true, nil
}

// scanHintDirsForConfigs scans the recursive subtree of each directory for
// schemabot.yaml files. The scan is recursive per directory, so configs
// nested below a directory are found (allowed_dirs matching is prefix-based).
func (ic *InstallationClient) scanHintDirsForConfigs(ctx context.Context, repo, ref string, dirs []string) (*FindAllConfigsResult, error) {
	result := &FindAllConfigsResult{}
	seen := make(map[string]struct{})
	// Hint directories often share path prefixes; memoize the shallow
	// per-level fetches so shared levels cost one API call per scan.
	levelCache := make(map[string][]TreeEntry)
	for _, dir := range dirs {
		entries, found, err := ic.fetchGitSubtreeCached(ctx, repo, ref, dir, levelCache)
		if err != nil {
			return nil, fmt.Errorf("scan configured schema dir %s: %w", dir, err)
		}
		if !found {
			ic.logger.Debug("configured schema dir does not exist at ref; skipping",
				"repo", repo, "ref", ref, "dir", dir)
			continue
		}
		for _, entry := range entries {
			if entry.Type != "blob" || !isConfigFile(entry.Path) {
				continue
			}
			configPath := dir + "/" + entry.Path
			// Nested hint directories can surface the same config twice.
			if _, ok := seen[configPath]; ok {
				continue
			}
			seen[configPath] = struct{}{}

			config, err := ic.FetchConfig(ctx, repo, configPath, ref)
			if err != nil {
				ic.logger.Warn("failed to parse config", "path", configPath, "error", err)
				result.InvalidConfigs = append(result.InvalidConfigs, InvalidConfigInfo{
					Path:  configPath,
					Error: err.Error(),
				})
				continue
			}
			result.ValidConfigs = append(result.ValidConfigs, DiscoveredConfig{
				Config:    config,
				Path:      configPath,
				SchemaDir: path.Dir(configPath),
			})
		}
	}
	return result, nil
}

// FindConfigByDatabaseName finds a schemabot.yaml config by database name:
// first among the PR's changed files, then by searching the repository.
func (ic *InstallationClient) FindConfigByDatabaseName(ctx context.Context, repo string, pr int, databaseName string) (*SchemabotConfig, string, error) {
	config, configDir, found, err := ic.FindConfigByDatabaseNameInPR(ctx, repo, pr, databaseName)
	if err != nil {
		return nil, "", err
	}
	if found {
		return config, configDir, nil
	}
	return ic.FindConfigByDatabaseNameInRepo(ctx, repo, pr, databaseName)
}

// FindConfigByDatabaseNameInPR finds the named database's schemabot.yaml
// among the configs reachable from the PR's changed files. found is false
// when the PR does not touch that database's schema — resolving it then
// requires the repository-wide search (FindConfigByDatabaseNameInRepo),
// which callers may want to gate or avoid.
func (ic *InstallationClient) FindConfigByDatabaseNameInPR(ctx context.Context, repo string, pr int, databaseName string) (*SchemabotConfig, string, bool, error) {
	prInfo, err := ic.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		return nil, "", false, fmt.Errorf("fetch PR info: %w", err)
	}

	files, err := ic.FetchPRFiles(ctx, repo, pr)
	if err != nil {
		return nil, "", false, fmt.Errorf("fetch PR files: %w", err)
	}

	prConfigs, err := ic.FindConfigsForPRFiles(ctx, repo, prInfo.HeadSHA, files)
	if err != nil {
		return nil, "", false, fmt.Errorf("find configs for PR: %w", err)
	}
	config, configDir, ok, err := selectConfigByDatabaseName(databaseName, prConfigs)
	if err != nil {
		return nil, "", false, err
	}
	return config, configDir, ok, nil
}

// FindConfigByDatabaseNameInRepo finds the named database's schemabot.yaml by
// searching the repository at the PR's head. When the repository tree is
// truncated, the search probes only the named database's configured schema
// directories, so it stays cheap regardless of how many databases the server
// configures for the repo.
func (ic *InstallationClient) FindConfigByDatabaseNameInRepo(ctx context.Context, repo string, pr int, databaseName string) (*SchemabotConfig, string, error) {
	prInfo, err := ic.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		return nil, "", fmt.Errorf("fetch PR info: %w", err)
	}

	result, scoped, err := ic.findAllConfigsForDatabase(ctx, repo, prInfo.HeadSHA, databaseName)
	if err != nil {
		return nil, "", fmt.Errorf("find configs: %w", err)
	}

	if len(result.ValidConfigs) == 0 && len(result.InvalidConfigs) > 0 {
		var invalidPaths []string
		for _, ic := range result.InvalidConfigs {
			invalidPaths = append(invalidPaths, ic.Path)
		}
		return nil, "", fmt.Errorf("%w at %s", ErrInvalidConfig, strings.Join(invalidPaths, ", "))
	}

	if scoped {
		config, configDir, ok, err := selectConfigByDatabaseName(databaseName, result.ValidConfigs)
		if err != nil {
			return nil, "", err
		}
		if !ok {
			// The scoped scan covered every directory this database's config
			// could live in, so absence is authoritative. Other databases
			// were not enumerated, so no available-databases list is offered.
			return nil, "", &DatabaseNotFoundError{DatabaseName: databaseName}
		}
		ic.logger.Debug("found config for database via scoped truncated-tree probe", "database", databaseName, "path", configDir)
		return config, configDir, nil
	}

	if len(result.ValidConfigs) == 0 {
		return nil, "", ErrNoConfig
	}

	config, configDir, ok, err := selectConfigByDatabaseName(databaseName, result.ValidConfigs)
	if err != nil {
		return nil, "", err
	}
	if !ok {
		var available []string
		for _, dc := range result.ValidConfigs {
			available = append(available, dc.Config.Database)
		}
		return nil, "", &DatabaseNotFoundError{DatabaseName: databaseName, AvailableDatabases: available}
	}

	ic.logger.Debug("found config for database", "database", databaseName, "path", configDir)
	return config, configDir, nil
}

// FindConfigForPR finds the schemabot.yaml config by searching changed config files and directories of changed schema files.
func (ic *InstallationClient) FindConfigForPR(ctx context.Context, repo string, pr int) (*SchemabotConfig, string, error) {
	prInfo, err := ic.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		return nil, "", fmt.Errorf("fetch PR info: %w", err)
	}

	configs, err := ic.findConfigsChangedInPR(ctx, repo, pr, prInfo.HeadSHA)
	if err != nil {
		return nil, "", err
	}
	if len(configs) == 1 {
		return configs[0].Config, configs[0].SchemaDir, nil
	}
	if len(configs) > 1 {
		var databases []string
		for _, dc := range configs {
			databases = append(databases, fmt.Sprintf("`%s` (%s)", dc.Config.Database, dc.SchemaDir))
		}
		return nil, "", fmt.Errorf("%w: %s", ErrMultipleConfigs, strings.Join(databases, ", "))
	}

	files, err := ic.FetchPRFiles(ctx, repo, pr)
	if err != nil {
		return nil, "", fmt.Errorf("fetch PR files: %w", err)
	}

	var schemaFiles []string
	for _, file := range files {
		if IsSchemaFile(file.Filename) {
			schemaFiles = append(schemaFiles, file.Filename)
		}
	}

	if len(schemaFiles) == 0 {
		return nil, "", ErrNoConfig
	}

	// Collect all unique directories to search for config
	dirsToSearch := make(map[string]bool)
	for _, file := range schemaFiles {
		dir := path.Dir(file)
		for dir != "." && dir != "" {
			dirsToSearch[dir] = true
			dir = path.Dir(dir)
		}
		dirsToSearch["."] = true
	}

	// Search for config starting from shallowest directory
	var bestConfig *SchemabotConfig
	var bestConfigDir string
	bestDepth := -1

	for dir := range dirsToSearch {
		configPath := dir + "/" + ConfigFileName
		if dir == "." {
			configPath = ConfigFileName
		}

		config, err := ic.FetchConfig(ctx, repo, configPath, prInfo.HeadSHA)
		if err != nil {
			if errors.Is(err, ErrNoConfig) {
				continue
			}
			// Config not found at this path is expected — the search
			// walks up parent directories. Other errors (auth failures,
			// rate limits, server errors) must propagate.
			return nil, "", err
		}

		depth := strings.Count(dir, "/")
		if dir == "." {
			depth = 0
		}

		if bestConfig == nil || depth < bestDepth {
			bestConfig = config
			bestConfigDir = dir
			bestDepth = depth
		}
	}

	if bestConfig == nil {
		return nil, "", ErrNoConfig
	}

	return bestConfig, bestConfigDir, nil
}

// FindAllConfigsForPR finds ALL schemabot.yaml configs that apply to the changed files in a PR.
func (ic *InstallationClient) FindAllConfigsForPR(ctx context.Context, repo string, pr int) ([]DiscoveredConfig, error) {
	prInfo, err := ic.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("fetch PR info: %w", err)
	}

	files, err := ic.FetchPRFiles(ctx, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("fetch PR files: %w", err)
	}
	return ic.FindConfigsForPRFiles(ctx, repo, prInfo.HeadSHA, files)
}

// FindConfigsForPRFiles discovers the schemabot.yaml configs that manage the
// given already-fetched PR files at ref. Callers that have the changed files in
// hand use this directly to avoid re-listing them; FindAllConfigsForPR is the
// convenience wrapper that fetches the files first.
func (ic *InstallationClient) FindConfigsForPRFiles(ctx context.Context, repo, ref string, files []PRFile) ([]DiscoveredConfig, error) {
	// Changed schema files usually share ancestor directories (adding or
	// deleting a schema tree touches many files under one root), so probe
	// results are memoized per directory: each unique directory is fetched at
	// most once per discovery pass. This bounds the GitHub call volume by the
	// number of unique directories rather than changed files × directory
	// depth, keeping discovery for large PRs within the webhook processing
	// deadline.
	probes := make(configProbeCache)

	configsByPath := make(map[string]DiscoveredConfig)
	for _, file := range files {
		if !isConfigFile(file.Filename) {
			continue
		}
		if isRemovedPRFile(file.Status) {
			continue
		}
		configDir := path.Dir(file.Filename)
		config, err := ic.FetchConfig(ctx, repo, file.Filename, ref)
		if err != nil {
			return nil, fmt.Errorf("fetch changed config %s: %w", file.Filename, err)
		}
		configsByPath[configDir] = newDiscoveredConfig(config, configDir)
		probes[configDir] = config
	}

	var filenames []string
	for _, f := range files {
		filenames = append(filenames, f.Filename)
	}
	schemaFiles := filterSchemaFiles(filenames)
	for _, schemaFile := range schemaFiles {
		config, configDir, err := ic.findNearestConfig(ctx, repo, ref, schemaFile, probes)
		if err != nil {
			return nil, err
		}
		if config != nil {
			if _, exists := configsByPath[configDir]; !exists {
				configsByPath[configDir] = newDiscoveredConfig(config, configDir)
			}
		}
	}

	return sortedConfigs(configsByPath), nil
}

// FindConfigInRepo searches for schemabot.yaml config files using the Tree API.
func (ic *InstallationClient) FindConfigInRepo(ctx context.Context, repo string, pr int) (*SchemabotConfig, string, []InvalidConfigInfo, error) {
	prInfo, err := ic.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		return nil, "", nil, fmt.Errorf("fetch PR info: %w", err)
	}

	result, err := ic.FindAllConfigs(ctx, repo, prInfo.HeadSHA)
	if err != nil {
		return nil, "", nil, fmt.Errorf("search for configs: %w", err)
	}

	if len(result.ValidConfigs) == 0 && len(result.InvalidConfigs) > 0 {
		return nil, "", result.InvalidConfigs, ErrInvalidConfig
	}

	if len(result.ValidConfigs) == 0 {
		return nil, "", nil, ErrNoConfig
	}

	if len(result.ValidConfigs) == 1 {
		dc := result.ValidConfigs[0]
		return dc.Config, dc.SchemaDir, result.InvalidConfigs, nil
	}

	var databases []string
	for _, dc := range result.ValidConfigs {
		databases = append(databases, fmt.Sprintf("`%s` (%s)", dc.Config.Database, dc.SchemaDir))
	}
	return nil, "", result.InvalidConfigs, fmt.Errorf("%w: %s", ErrMultipleConfigs, strings.Join(databases, ", "))
}

// IsSchemaFile reports whether filename is a SchemaBot-managed schema file
// (a .sql DDL file or a vschema.json).
func IsSchemaFile(filename string) bool {
	return strings.HasSuffix(filename, ".sql") || strings.HasSuffix(filename, "vschema.json")
}

// HasSchemaInputFiles reports whether a changed file list touches inputs that
// should refresh the visible SchemaBot plan.
func HasSchemaInputFiles(files []PRFile) bool {
	for _, file := range files {
		if isSchemaInputFile(file.Filename) || isSchemaInputFile(file.PreviousFilename) {
			return true
		}
	}
	return false
}

func isSchemaInputFile(filename string) bool {
	return IsSchemaFile(filename) || isConfigFile(filename)
}

func isConfigFile(filename string) bool {
	return path.Base(filename) == ConfigFileName
}

func isRemovedPRFile(status string) bool {
	return strings.EqualFold(status, "removed")
}

func filterSchemaFiles(files []string) []string {
	var result []string
	for _, file := range files {
		if IsSchemaFile(file) {
			result = append(result, file)
		}
	}
	return result
}

// configProbeCache memoizes per-directory config probe results within one
// discovery pass. A key that maps to nil records a directory known to have no
// schemabot.yaml, so the walk never re-fetches it for another file.
//
// Keys are directory paths only, so a cache is valid for exactly one
// (repo, ref) pair — always ref a single immutable commit SHA and never reuse
// a cache across refs. Not safe for concurrent use; probing in parallel
// requires external synchronization.
type configProbeCache map[string]*SchemabotConfig

// probeConfigDir fetches the config for dir, consulting and populating the
// cache. It returns (nil, nil) when the directory has no schemabot.yaml.
// Transient errors are returned without being cached so a retry can succeed.
func (ic *InstallationClient) probeConfigDir(ctx context.Context, repo, ref, dir string, probes configProbeCache) (*SchemabotConfig, error) {
	if config, ok := probes[dir]; ok {
		return config, nil
	}
	config, err := ic.FetchConfig(ctx, repo, configPathForDir(dir), ref)
	if err != nil {
		if errors.Is(err, ErrNoConfig) {
			probes[dir] = nil
			return nil, nil
		}
		return nil, err
	}
	probes[dir] = config
	return config, nil
}

func (ic *InstallationClient) findNearestConfig(ctx context.Context, repo, ref, filePath string, probes configProbeCache) (*SchemabotConfig, string, error) {
	dir := path.Dir(filePath)

	for {
		config, err := ic.probeConfigDir(ctx, repo, ref, dir, probes)
		if err != nil {
			return nil, "", err
		}
		if config != nil {
			return config, dir, nil
		}

		if dir == "." || dir == "" {
			break
		}
		parentDir := path.Dir(dir)
		if parentDir == dir {
			break
		}
		dir = parentDir
	}

	return nil, "", nil
}

func configPathForDir(dir string) string {
	if dir == "." {
		return ConfigFileName
	}
	return dir + "/" + ConfigFileName
}

func newDiscoveredConfig(config *SchemabotConfig, dir string) DiscoveredConfig {
	configPath := dir + "/" + ConfigFileName
	if dir == "." {
		configPath = ConfigFileName
	}
	return DiscoveredConfig{
		Config:    config,
		Path:      configPath,
		SchemaDir: dir,
	}
}

func (ic *InstallationClient) findConfigsChangedInPR(ctx context.Context, repo string, pr int, ref string) ([]DiscoveredConfig, error) {
	files, err := ic.FetchPRFiles(ctx, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("fetch PR files: %w", err)
	}

	configsByPath := make(map[string]DiscoveredConfig)
	for _, file := range files {
		if !isConfigFile(file.Filename) {
			continue
		}
		if isRemovedPRFile(file.Status) {
			continue
		}
		configDir := path.Dir(file.Filename)
		config, err := ic.FetchConfig(ctx, repo, file.Filename, ref)
		if err != nil {
			return nil, fmt.Errorf("fetch changed config %s: %w", file.Filename, err)
		}
		configsByPath[configDir] = newDiscoveredConfig(config, configDir)
	}

	return sortedConfigs(configsByPath), nil
}

func selectConfigByDatabaseName(databaseName string, configs []DiscoveredConfig) (*SchemabotConfig, string, bool, error) {
	var matches []DiscoveredConfig
	for _, dc := range configs {
		if strings.EqualFold(dc.Config.Database, databaseName) {
			matches = append(matches, dc)
		}
	}
	if len(matches) == 0 {
		return nil, "", false, nil
	}
	if len(matches) > 1 {
		var paths []string
		for _, m := range matches {
			paths = append(paths, m.SchemaDir)
		}
		return nil, "", false, fmt.Errorf("ambiguous: database '%s' matches multiple configs at: %s",
			databaseName, strings.Join(paths, ", "))
	}

	match := matches[0]
	return match.Config, match.SchemaDir, true, nil
}

func sortedConfigs(configsByPath map[string]DiscoveredConfig) []DiscoveredConfig {
	configs := make([]DiscoveredConfig, 0, len(configsByPath))
	for _, dc := range configsByPath {
		configs = append(configs, dc)
	}
	sort.Slice(configs, func(i, j int) bool {
		return configs[i].Config.Database < configs[j].Config.Database
	})
	return configs
}
