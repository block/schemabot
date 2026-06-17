package github

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	gh "github.com/google/go-github/v86/github"
)

// SchemaRequestResult contains everything needed to execute a plan from a PR.
type SchemaRequestResult struct {
	Database     string
	Environments []string // Server-owned environments for this database, populated by webhook handlers.
	Type         string   // "mysql" or "vitess"
	SchemaFiles  map[string]*ternv1.SchemaFiles
	Repository   string
	PullRequest  int
	SchemaPath   string
	HeadSHA      string // Commit SHA used to fetch schema files
}

// EnvironmentValidator verifies that a discovered database can be used with
// the requested environment before the environment is used to resolve schema
// files in the repository.
type EnvironmentValidator func(database, environment string) error

// CreateSchemaRequestFromPR discovers config, fetches schema files, and builds a plan request.
func (ic *InstallationClient) CreateSchemaRequestFromPR(ctx context.Context, repo string, pr int, environment, databaseName string, validateEnvironment EnvironmentValidator) (*SchemaRequestResult, error) {
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
	if validateEnvironment != nil {
		if err := validateEnvironment(config.Database, environment); err != nil {
			return nil, err
		}
	}

	// Get PR info for head SHA
	prInfo, err := ic.FetchPullRequest(ctx, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("fetch PR info: %w", err)
	}
	schemaRoot, err := ic.resolveSchemaRootForEnvironment(ctx, repo, prInfo.HeadSHA, configDir, environment)
	if err != nil {
		return nil, err
	}

	// Fetch schema files using optimized Tree API + parallel fetching
	files, err := ic.FetchSchemaFilesOptimized(ctx, repo, prInfo.HeadSHA, schemaRoot, string(config.GetType()))
	if err != nil {
		return nil, fmt.Errorf("fetch schema files from %s: %w", schemaRoot, err)
	}

	// Group files by keyspace/namespace
	schemaFiles, err := groupFilesByNamespace(files, schemaRoot, environment)
	if err != nil {
		return nil, err
	}
	if len(schemaFiles) == 0 {
		return nil, fmt.Errorf("no schema files found under %s for environment %q", schemaRoot, environment)
	}

	return &SchemaRequestResult{
		Database:    config.Database,
		Type:        string(config.GetType()),
		SchemaFiles: schemaFiles,
		Repository:  repo,
		PullRequest: pr,
		SchemaPath:  schemaRoot,
		HeadSHA:     prInfo.HeadSHA,
	}, nil
}

func (ic *InstallationClient) resolveSchemaRootForEnvironment(ctx context.Context, repo, ref, configDir, environment string) (string, error) {
	if environment == "" {
		return configDir, nil
	}
	if err := validateEnvironmentPathSegment(environment); err != nil {
		return "", err
	}

	candidate := path.Join(configDir, environment)
	content, directoryContent, err := ic.fetchRepositoryContents(ctx, repo, candidate, ref)
	if err != nil {
		if IsNotFoundError(err) {
			return configDir, nil
		}
		return "", fmt.Errorf("resolve schema root for environment %s at %s: %w", environment, candidate, err)
	}
	if directoryContent != nil {
		return candidate, nil
	}
	if content == nil || content.GetType() != "symlink" {
		return "", fmt.Errorf("environment schema root %s in repo %s ref %s is not a directory or symlink", candidate, repo, ref)
	}

	resolved, err := resolveSchemaRootSymlinkTarget(configDir, candidate, content.GetTarget())
	if err != nil {
		return "", err
	}
	resolvedContent, resolvedDirectoryContent, err := ic.fetchRepositoryContents(ctx, repo, resolved, ref)
	if err != nil {
		return "", fmt.Errorf("resolve environment schema root symlink %s target %s: %w", candidate, resolved, err)
	}
	if resolvedContent != nil || resolvedDirectoryContent == nil {
		return "", fmt.Errorf("environment schema root symlink %s points to non-directory path %s", candidate, resolved)
	}
	return resolved, nil
}

func validateEnvironmentPathSegment(environment string) error {
	if environment == "." || environment == ".." || strings.ContainsAny(environment, `/\`) {
		return fmt.Errorf("environment %q must be a single path segment", environment)
	}
	return nil
}

func (ic *InstallationClient) fetchRepositoryContents(ctx context.Context, repo, filePath, ref string) (*gh.RepositoryContent, []*gh.RepositoryContent, error) {
	owner, repoName := splitRepo(repo)
	opts := &gh.RepositoryContentGetOptions{Ref: ref}
	readResult, err := retryGitHubUnavailableRead(ctx, ic.logger, "fetch repository content", []any{"repo", repo, "path", filePath, "ref", ref}, func(ctx context.Context) (struct {
		fileContent      *gh.RepositoryContent
		directoryContent []*gh.RepositoryContent
	}, error) {
		fileContent, directoryContent, _, err := ic.client.Repositories.GetContents(ctx, owner, repoName, filePath, opts)
		if err != nil {
			return struct {
				fileContent      *gh.RepositoryContent
				directoryContent []*gh.RepositoryContent
			}{}, fmt.Errorf("fetch repository content at %s: %w", filePath, classifyGitHubAPIError(err))
		}
		return struct {
			fileContent      *gh.RepositoryContent
			directoryContent []*gh.RepositoryContent
		}{fileContent: fileContent, directoryContent: directoryContent}, nil
	})
	if err != nil {
		return nil, nil, err
	}
	return readResult.fileContent, readResult.directoryContent, nil
}

func resolveSchemaRootSymlinkTarget(configDir, symlinkPath, target string) (string, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return "", fmt.Errorf("environment schema root symlink %s has empty target", symlinkPath)
	}
	if strings.HasPrefix(target, "/") {
		return "", fmt.Errorf("environment schema root symlink %s points to absolute path %s", symlinkPath, target)
	}

	resolved := path.Clean(path.Join(path.Dir(symlinkPath), target))
	cleanConfigDir := path.Clean(configDir)
	if !schemaPathWithinDirectory(cleanConfigDir, resolved) {
		return "", fmt.Errorf("environment schema root symlink %s points outside schema directory: %s", symlinkPath, target)
	}
	return resolved, nil
}

func schemaPathWithinDirectory(directory, candidate string) bool {
	directory = path.Clean(directory)
	candidate = path.Clean(candidate)
	if directory == "." {
		return candidate == "." || candidate != ".." && !strings.HasPrefix(candidate, "../") && !strings.HasPrefix(candidate, "/")
	}
	return candidate == directory || strings.HasPrefix(candidate, directory+"/")
}

// groupFilesByNamespace groups fetched files into the proto SchemaFiles format.
// Files in namespace subdirectories (schema/namespace/table.sql) use the
// subdirectory name as the namespace. Flat files (schema/table.sql) use the
// schema directory name as the namespace (the MySQL database name).
func groupFilesByNamespace(files []GitHubFile, schemaPath, environment string) (map[string]*ternv1.SchemaFiles, error) {
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

	grouped, err := schema.GroupFilesByNamespace(rawFiles, path.Base(schemaPath), environment)
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
