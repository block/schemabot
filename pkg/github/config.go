package github

import (
	"context"
	"fmt"
	"path"
	"slices"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// DatabaseType represents the type of database backend.
type DatabaseType string

const (
	DatabaseTypeVitess DatabaseType = "vitess"
	DatabaseTypeMySQL  DatabaseType = "mysql"
)

// EnvironmentEntry represents a configured environment with optional target.
type EnvironmentEntry struct {
	Name   string
	Target string
}

// EnvironmentList supports both list-of-strings and map forms in YAML:
//
//	# Simple form — target defaults to database field
//	environments:
//	  - staging
//	  - production
//
//	# Map form — explicit target per environment
//	environments:
//	  staging:
//	    target: cluster-staging-001
//	  production:
//	    target: cluster-production-001
type EnvironmentList []EnvironmentEntry

// UnmarshalYAML handles both list-of-strings and map-of-objects forms.
func (e *EnvironmentList) UnmarshalYAML(value *yaml.Node) error {
	*e = nil // Reset to avoid accumulating entries on re-unmarshal
	switch value.Kind {
	case yaml.SequenceNode:
		// List form: ["staging", "production"]
		var names []string
		if err := value.Decode(&names); err != nil {
			return err
		}
		for _, name := range names {
			*e = append(*e, EnvironmentEntry{Name: name})
		}
		return nil
	case yaml.MappingNode:
		// Map form: staging: {target: "..."}
		// Iterate yaml.Node children to preserve YAML declaration order.
		for i := 0; i < len(value.Content)-1; i += 2 {
			keyNode := value.Content[i]
			valNode := value.Content[i+1]
			name := keyNode.Value
			var entry struct {
				Target string `yaml:"target"`
			}
			if err := valNode.Decode(&entry); err != nil {
				return fmt.Errorf("environment %q: %w", name, err)
			}
			*e = append(*e, EnvironmentEntry{Name: name, Target: entry.Target})
		}
		return nil
	default:
		return fmt.Errorf("environments must be a list or map, got %v", value.Kind)
	}
}

// SchemabotConfig represents the schemabot.yaml configuration file.
// The presence of this file in a directory indicates that directory contains schema files.
type SchemabotConfig struct {
	Database     string          `yaml:"database" json:"database"`
	Name         string          `yaml:"name" json:"name"`
	Type         DatabaseType    `yaml:"type,omitempty" json:"type,omitempty"`
	Environments EnvironmentList `yaml:"environments,omitempty" json:"environments,omitempty"`
}

// GetType returns the database type. Type is always set — FetchConfig rejects empty values.
func (c *SchemabotConfig) GetType() DatabaseType {
	return c.Type
}

// GetEnvironments returns the configured environment names, defaulting to ["staging"].
func (c *SchemabotConfig) GetEnvironments() []string {
	if len(c.Environments) == 0 {
		return []string{"staging"}
	}
	names := make([]string, len(c.Environments))
	for i, e := range c.Environments {
		names[i] = e.Name
	}
	return names
}

// HasEnvironment returns true if the specified environment is configured.
func (c *SchemabotConfig) HasEnvironment(env string) bool {
	return slices.Contains(c.GetEnvironments(), env)
}

// GetTarget returns the target for the given environment.
// If the environment has no explicit target, falls back to the Database field.
func (c *SchemabotConfig) GetTarget(env string) string {
	for _, e := range c.Environments {
		if e.Name == env && e.Target != "" {
			return e.Target
		}
	}
	return c.Database
}

// DiscoveredConfig represents a schemabot.yaml config found via Tree API search.
type DiscoveredConfig struct {
	Config       *SchemabotConfig
	Path         string   // Full path to schemabot.yaml file
	SchemaDir    string   // Directory containing schemabot.yaml
	Environments []string // From config.GetEnvironments()
}

// ConfigFileName is the name of the schemabot config file.
const ConfigFileName = "schemabot.yaml"

// Config discovery errors.
var (
	ErrNoConfig        = fmt.Errorf("no schemabot.yaml config found")
	ErrInvalidConfig   = fmt.Errorf("invalid schemabot.yaml config found")
	ErrMultipleConfigs = fmt.Errorf("multiple schemabot.yaml configs found - use -d flag to specify database")
)

// DatabaseNotFoundError indicates the specified database was not found in any config.
type DatabaseNotFoundError struct {
	DatabaseName       string
	AvailableDatabases []string
}

func (e *DatabaseNotFoundError) Error() string {
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
	if err := yaml.Unmarshal([]byte(content), &config); err != nil {
		return nil, fmt.Errorf("invalid schemabot.yaml at %s: %w", configPath, err)
	}

	if config.Database == "" {
		return nil, fmt.Errorf("invalid schemabot.yaml at %s: database is required", configPath)
	}
	if config.Type == "" {
		return nil, fmt.Errorf("invalid schemabot.yaml at %s: type is required (must be 'vitess' or 'mysql')", configPath)
	}
	if config.Type != DatabaseTypeVitess && config.Type != DatabaseTypeMySQL {
		return nil, fmt.Errorf("invalid schemabot.yaml at %s: type must be 'vitess' or 'mysql', got '%s'", configPath, config.Type)
	}

	validEnvs := map[string]bool{"staging": true, "production": true}
	for _, env := range config.Environments {
		if !validEnvs[env.Name] {
			return nil, fmt.Errorf("invalid schemabot.yaml at %s: environment must be 'staging' or 'production', got '%s'", configPath, env.Name)
		}
	}

	return &config, nil
}

// FindAllConfigsResult contains both valid and invalid config information.
type FindAllConfigsResult struct {
	ValidConfigs   []DiscoveredConfig
	InvalidConfigs []InvalidConfigInfo
}

// FindAllConfigs uses the Tree API to discover all schemabot.yaml config files in the repository.
func (ic *InstallationClient) FindAllConfigs(ctx context.Context, repo, ref string) (*FindAllConfigsResult, error) {
	entries, _, err := ic.FetchGitTree(ctx, repo, ref)
	if err != nil {
		return nil, fmt.Errorf("fetch git tree: %w", err)
	}

	var configPaths []string
	for _, entry := range entries {
		if entry.Type == "blob" && strings.HasSuffix(entry.Path, ConfigFileName) {
			configPaths = append(configPaths, entry.Path)
		}
	}

	result := &FindAllConfigsResult{}
	if len(configPaths) == 0 {
		return result, nil
	}

	for _, configPath := range configPaths {
		config, err := ic.FetchConfig(ctx, repo, configPath, ref)
		if err != nil {
			ic.logger.Warn("failed to parse config", "path", configPath, "error", err)
			result.InvalidConfigs = append(result.InvalidConfigs, InvalidConfigInfo{
				Path:  configPath,
				Error: err.Error(),
			})
			continue
		}

		schemaDir := path.Dir(configPath)
		result.ValidConfigs = append(result.ValidConfigs, DiscoveredConfig{
			Config:       config,
			Path:         configPath,
			SchemaDir:    schemaDir,
			Environments: config.GetEnvironments(),
		})
	}

	ic.logger.Debug("config discovery complete",
		"valid", len(result.ValidConfigs),
		"invalid", len(result.InvalidConfigs),
		"repo", repo,
	)
	return result, nil
}

// FindConfigByDatabaseName finds a schemabot.yaml config by database name.
func (ic *InstallationClient) FindConfigByDatabaseName(ctx context.Context, repo string, pr int, databaseName string) (*SchemabotConfig, string, error) {
	prInfo, err := ic.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		return nil, "", fmt.Errorf("fetch PR info: %w", err)
	}

	result, err := ic.FindAllConfigs(ctx, repo, prInfo.HeadSHA)
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

	if len(result.ValidConfigs) == 0 {
		return nil, "", ErrNoConfig
	}

	var matches []DiscoveredConfig
	for _, dc := range result.ValidConfigs {
		if strings.EqualFold(dc.Config.Database, databaseName) {
			matches = append(matches, dc)
		}
	}

	if len(matches) == 0 {
		var available []string
		for _, dc := range result.ValidConfigs {
			available = append(available, dc.Config.Database)
		}
		return nil, "", &DatabaseNotFoundError{DatabaseName: databaseName, AvailableDatabases: available}
	}

	if len(matches) > 1 {
		var paths []string
		for _, m := range matches {
			paths = append(paths, m.SchemaDir)
		}
		return nil, "", fmt.Errorf("ambiguous: database '%s' matches multiple configs at: %s",
			databaseName, strings.Join(paths, ", "))
	}

	match := matches[0]
	ic.logger.Debug("found config for database", "database", databaseName, "path", match.SchemaDir)
	return match.Config, match.SchemaDir, nil
}

// FindConfigForPR finds the schemabot.yaml config by searching directories of changed schema files.
func (ic *InstallationClient) FindConfigForPR(ctx context.Context, repo string, pr int) (*SchemabotConfig, string, error) {
	prInfo, err := ic.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		return nil, "", fmt.Errorf("fetch PR info: %w", err)
	}

	files, err := ic.FetchPRFiles(ctx, repo, pr)
	if err != nil {
		return nil, "", fmt.Errorf("fetch PR files: %w", err)
	}

	var schemaFiles []string
	for _, file := range files {
		if isSchemaFile(file.Filename) {
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
			continue
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

	var filenames []string
	for _, f := range files {
		filenames = append(filenames, f.Filename)
	}
	schemaFiles := filterSchemaFiles(filenames)
	if len(schemaFiles) == 0 {
		return nil, nil
	}

	configsByPath := make(map[string]DiscoveredConfig)
	for _, schemaFile := range schemaFiles {
		config, configDir := ic.findNearestConfig(ctx, repo, prInfo.HeadSHA, schemaFile)
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

func isSchemaFile(filename string) bool {
	return strings.HasSuffix(filename, ".sql") || strings.HasSuffix(filename, "vschema.json")
}

func filterSchemaFiles(files []string) []string {
	var result []string
	for _, file := range files {
		if isSchemaFile(file) {
			result = append(result, file)
		}
	}
	return result
}

func (ic *InstallationClient) findNearestConfig(ctx context.Context, repo, ref, filePath string) (*SchemabotConfig, string) {
	dir := path.Dir(filePath)

	// Check same directory (MySQL structure)
	if config, err := ic.FetchConfig(ctx, repo, configPathForDir(dir), ref); err == nil {
		return config, dir
	}

	// Check parent directory (Vitess structure with keyspace subdirectories)
	parentDir := path.Dir(dir)
	if parentDir != "" && parentDir != dir {
		if config, err := ic.FetchConfig(ctx, repo, configPathForDir(parentDir), ref); err == nil {
			return config, parentDir
		}
	}

	return nil, ""
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
		Config:       config,
		Path:         configPath,
		SchemaDir:    dir,
		Environments: config.GetEnvironments(),
	}
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
