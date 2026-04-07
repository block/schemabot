package github

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
)

// SchemaRequestResult contains everything needed to execute a plan from a PR.
type SchemaRequestResult struct {
	Database     string
	Environments []string // All configured environments in order (e.g., ["staging", "production"])
	Type         string   // "mysql" or "vitess"
	SchemaFiles  map[string]*ternv1.SchemaFiles
	Repository   string
	PullRequest  int
	SchemaPath   string
	Target       string // Opaque endpoint discovery identifier from config
}

// CreateSchemaRequestFromPR discovers config, fetches schema files, and builds a plan request.
func (ic *InstallationClient) CreateSchemaRequestFromPR(ctx context.Context, repo string, pr int, environment, databaseName string) (*SchemaRequestResult, error) {
	var config *SchemabotConfig
	var configDir string
	var err error

	if databaseName != "" {
		config, configDir, err = ic.FindConfigByDatabaseName(ctx, repo, pr, databaseName)
	} else {
		config, configDir, err = ic.FindConfigForPR(ctx, repo, pr)
		if errors.Is(err, ErrNoConfig) {
			config, configDir, _, err = ic.FindConfigInRepo(ctx, repo, pr)
		}
	}
	if err != nil {
		return nil, err
	}

	if environment != "" && !config.HasEnvironment(environment) {
		return nil, fmt.Errorf("environment %q is not configured for this database (configured: %v)", environment, config.GetEnvironments())
	}

	// Get PR info for head SHA
	prInfo, err := ic.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("fetch PR info: %w", err)
	}

	// Fetch schema files using optimized Tree API + parallel fetching
	files, err := ic.FetchSchemaFilesOptimized(ctx, repo, prInfo.HeadSHA, configDir, string(config.GetType()))
	if err != nil {
		return nil, fmt.Errorf("fetch schema files from %s: %w", configDir, err)
	}

	// Group files by keyspace/namespace
	schemaFiles, err := groupFilesByNamespace(files, configDir)
	if err != nil {
		return nil, err
	}

	return &SchemaRequestResult{
		Database:     config.Database,
		Environments: config.GetEnvironments(),
		Type:         string(config.GetType()),
		SchemaFiles:  schemaFiles,
		Repository:   repo,
		PullRequest:  pr,
		SchemaPath:   configDir,
		Target:       config.GetTarget(environment),
	}, nil
}

// groupFilesByNamespace groups fetched files into the proto SchemaFiles format.
// Files in namespace subdirectories (schema/namespace/table.sql) use the
// subdirectory name as the namespace. Flat files (schema/table.sql) use the
// schema directory name as the namespace (the MySQL database name).
func groupFilesByNamespace(files []GitHubFile, schemaPath string) (map[string]*ternv1.SchemaFiles, error) {
	// Build relativePath → content map for the shared helper.
	// file.Path is the full path (e.g., "schema/payments/transactions.sql"),
	// so trimming schemaPath+"/" gives the relative path ("payments/transactions.sql"
	// or "transactions.sql" for flat files).
	rawFiles := make(map[string]string, len(files))
	prefix := schemaPath + "/"
	for _, file := range files {
		relPath, ok := strings.CutPrefix(file.Path, prefix)
		if !ok {
			return nil, fmt.Errorf("file path %q does not start with schema path %q", file.Path, prefix)
		}
		rawFiles[relPath] = file.Content
	}

	grouped, err := schema.GroupFilesByNamespace(rawFiles, path.Base(schemaPath))
	if err != nil {
		return nil, err
	}

	// Convert schema.SchemaFiles → ternv1.SchemaFiles
	result := make(map[string]*ternv1.SchemaFiles, len(grouped))
	for ns, nsFiles := range grouped {
		result[ns] = &ternv1.SchemaFiles{Files: nsFiles.Files}
	}
	return result, nil
}
