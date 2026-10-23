package github

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
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
	// SchemaLinkPath is the logical environment path when SchemaPath was
	// resolved through a symlink. Base-freshness checks compare both the link
	// object and its target so retargeting the environment cannot go unnoticed.
	SchemaLinkPath string
	// ConfigPath is the repo-relative path of the schemabot.yaml the database
	// was discovered from. It lies outside SchemaPath when the schema root is
	// an environment subdirectory or a symlink target.
	ConfigPath string
	HeadSHA    string // Commit SHA used to fetch schema files
	// IgnoredNamespaces lists the namespaces that were actually removed from
	// SchemaFiles by the config's ignore_namespaces, resolved for the
	// environment and sorted. Surfaces the exclusion on the PR (plan comment)
	// and travels with the plan request so the data plane can refuse engine
	// shapes that cannot honor it.
	IgnoredNamespaces []string
	// IgnoreTables is the config's ignore_tables as written. Unlike ignored
	// namespaces it cannot be resolved here: the exclusion names live tables on
	// the target, which only the data plane can see, so the entries travel with
	// the plan request and the response reports which of them withheld
	// anything.
	IgnoreTables []string
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
	return ic.CreateSchemaRequestForConfig(ctx, repo, pr, environment, config, configDir, validateEnvironment)
}

// CreateSchemaRequestForConfig fetches schema files and builds a plan request
// for a config the caller has already discovered and selected — for example
// after filtering a multi-config discovery result down to the one config this
// deployment manages.
func (ic *InstallationClient) CreateSchemaRequestForConfig(ctx context.Context, repo string, pr int, environment string, config *SchemabotConfig, configDir string, validateEnvironment EnvironmentValidator) (*SchemaRequestResult, error) {
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
	schemaRoot, schemaLinkPath, err := ic.resolveSchemaRootForEnvironment(ctx, repo, prInfo.HeadSHA, configDir, environment)
	if err != nil {
		return nil, err
	}

	// Fetch schema files using optimized Tree API + parallel fetching
	files, err := ic.FetchSchemaFilesOptimized(ctx, repo, prInfo.HeadSHA, schemaRoot, string(config.GetType()))
	if err != nil {
		return nil, fmt.Errorf("fetch schema files from %s: %w", schemaRoot, err)
	}

	// Group files by keyspace/namespace
	schemaFiles, ignoredNamespaces, err := groupFilesByNamespace(files, schemaRoot, environment, config.IgnoreNamespaces)
	if err != nil {
		return nil, err
	}
	prFiles, err := ic.FetchPRFiles(ctx, repo, pr)
	if err != nil {
		return nil, fmt.Errorf("fetch changed files for %s#%d: %w", repo, pr, err)
	}
	// A file the pull request inherited from history its base has not caught up
	// with — a stacked pull request on a stale base carries every deletion
	// already merged ahead of it — is not one it proposes, and must not plan a
	// namespace it never touched. Narrowing at the pinned head also ties the
	// deletion evidence to the commit the declaration was read from: a vacated
	// path survives only when that commit lacks it and the default branch holds
	// it.
	vacated, _, err := ic.PRFilesProposedAgainstDefaultBranch(ctx, repo, prInfo.HeadSHA, vacatedSchemaFiles(prFiles, schemaRoot))
	if err != nil {
		return nil, fmt.Errorf("scope the schema files %s#%d removes to what it proposes: %w", repo, pr, err)
	}
	if len(schemaFiles) == 0 {
		// An empty schema root is indistinguishable from one that moved, so
		// discovery fails closed rather than planning every table in the
		// database as a drop. When the pull request itself emptied the root,
		// say so: the operator needs to know the removal has not taken effect
		// and what completes it.
		if len(vacated) > 0 {
			return nil, fmt.Errorf("no schema files found under %s for environment %q: this pull request removes the last schema files under it, so the tables those files declared stay live and the removal does not take effect; drop them through a reviewed schema change before deleting the files", schemaRoot, environment)
		}
		if len(ignoredNamespaces) > 0 {
			return nil, fmt.Errorf("no schema files found under %s for environment %q after excluding ignored namespaces %v", schemaRoot, environment, ignoredNamespaces)
		}
		return nil, fmt.Errorf("no schema files found under %s for environment %q", schemaRoot, environment)
	}
	emptied, vacatedIgnored, err := emptiedPRNamespaces(schemaFiles, config.IgnoreNamespaces, vacated, schemaRoot, environment)
	if err != nil {
		return nil, fmt.Errorf("find the namespaces %s#%d empties: %w", repo, pr, err)
	}
	// An ignored namespace whose last file this pull request removes is absent
	// from the head tree, so grouping never saw it. It is still withheld: its
	// tables stay live, and the target has to know it holds tables no file
	// declares rather than read them as deleted.
	ignoredNamespaces = mergeNamespaces(ignoredNamespaces, vacatedIgnored)
	if len(ignoredNamespaces) > 0 {
		ic.logger.Info("excluding ignored namespaces from schema request",
			"repo", repo, "pr", pr, "database", config.Database, "environment", environment,
			"schema_root", schemaRoot, "ignored_namespaces", ignoredNamespaces)
	}
	// A configured entry that removed nothing is a typo, a case mismatch, or a
	// stale entry for a deleted directory — the namespace it names is being
	// fully reconciled, so surface it rather than letting the config imply an
	// exclusion that is not happening.
	unmatched, err := schema.UnmatchedIgnoreEntries(config.IgnoreNamespaces, environment, ignoredNamespaces)
	if err != nil {
		return nil, fmt.Errorf("check ignore_namespaces entries for %s: %w", environment, err)
	}
	if len(unmatched) > 0 {
		ic.logger.Warn("ignore_namespaces entries matched no namespace and excluded nothing",
			"repo", repo, "pr", pr, "database", config.Database, "environment", environment,
			"schema_root", schemaRoot, "unmatched_entries", unmatched)
	}
	for _, namespace := range emptied {
		schemaFiles[namespace] = &ternv1.SchemaFiles{Files: map[string]string{}}
		ic.logger.Info("namespace has no remaining schema files in this pull request; the plan will surface every live table it holds as a DROP TABLE change",
			"repo", repo, "pr", pr, "head_sha", prInfo.HeadSHA, "database", config.Database,
			"database_type", config.GetType(), "environment", environment, "schema_root", schemaRoot, "namespace", namespace)
	}

	return &SchemaRequestResult{
		Database:          config.Database,
		Type:              string(config.GetType()),
		SchemaFiles:       schemaFiles,
		Repository:        repo,
		PullRequest:       pr,
		SchemaPath:        schemaRoot,
		SchemaLinkPath:    schemaLinkPath,
		ConfigPath:        path.Join(configDir, ConfigFileName),
		HeadSHA:           prInfo.HeadSHA,
		IgnoredNamespaces: ignoredNamespaces,
		IgnoreTables:      config.IgnoreTables,
	}, nil
}

// vacatedSchemaFiles returns, as deletions, the schema files under schemaRoot
// that the pull request left empty: a deleted file's own path and a renamed
// file's previous one. Each is reported with the removed status so the caller
// can ask the proposed-against-default-branch narrowing the one question that
// matters for it — does the default branch still hold this path.
//
// Only files head discovery reads are kept: those directly under the root or
// one namespace directory below it. A file nested deeper is never declared,
// so removing it declares nothing either.
func vacatedSchemaFiles(prFiles []PRFile, schemaRoot string) []PRFile {
	prefix := path.Clean(schemaRoot) + "/"
	var vacated []PRFile
	for _, file := range prFiles {
		vacatedPath, ok := vacatedSchemaPath(file)
		if !ok {
			continue
		}
		relativePath, ok := strings.CutPrefix(path.Clean(vacatedPath), prefix)
		if !ok || strings.Count(relativePath, "/") > 1 {
			continue
		}
		vacated = append(vacated, PRFile{Filename: path.Clean(vacatedPath), Status: "removed"})
	}
	return vacated
}

// emptiedPRNamespaces returns the namespaces the pull request emptied: those
// whose schema files it vacated (see vacatedSchemaFiles) and which no file at
// the head commit still declares. Absent from the grouped files, such a
// namespace would otherwise vanish from the plan while every table it holds
// stays live. A vacated namespace that ignore_namespaces withholds is returned
// separately, since it is withheld rather than emptied.
func emptiedPRNamespaces(grouped map[string]*ternv1.SchemaFiles, ignored []string, vacated []PRFile, schemaRoot, environment string) (emptied, vacatedIgnored []string, err error) {
	ignore, err := schema.CompileIgnoreNamespaces(ignored, environment)
	if err != nil {
		return nil, nil, err
	}

	prefix := path.Clean(schemaRoot) + "/"
	emptiedSet := make(map[string]bool)
	ignoredSet := make(map[string]bool)
	for _, file := range vacated {
		relativePath, ok := strings.CutPrefix(path.Clean(file.Filename), prefix)
		if !ok {
			continue
		}
		namespace, namespaced := schema.NamespaceForRelativePath(relativePath, path.Base(schemaRoot), environment)
		if !namespaced || grouped[namespace] != nil {
			continue
		}
		if ignore.Ignores(namespace) {
			ignoredSet[namespace] = true
			continue
		}
		emptiedSet[namespace] = true
	}
	return sortedNamespaces(emptiedSet), sortedNamespaces(ignoredSet), nil
}

// sortedNamespaces returns the set's members in order, or nil when it is empty.
func sortedNamespaces(set map[string]bool) []string {
	if len(set) == 0 {
		return nil
	}
	result := make([]string, 0, len(set))
	for namespace := range set {
		result = append(result, namespace)
	}
	sort.Strings(result)
	return result
}

// mergeNamespaces returns the sorted union of two namespace lists.
func mergeNamespaces(a, b []string) []string {
	if len(b) == 0 {
		return a
	}
	set := make(map[string]bool, len(a)+len(b))
	for _, namespace := range a {
		set[namespace] = true
	}
	for _, namespace := range b {
		set[namespace] = true
	}
	return sortedNamespaces(set)
}

// vacatedSchemaPath reports the schema file path a pull request file left
// empty: the path of a deleted file, or the previous path of a renamed one.
func vacatedSchemaPath(file PRFile) (string, bool) {
	var vacated string
	switch {
	case isRemovedPRFile(file.Status):
		vacated = file.Filename
	case isRenamedPRFile(file.Status):
		vacated = file.PreviousFilename
	default:
		return "", false
	}
	if !IsSchemaFile(vacated) {
		return "", false
	}
	return vacated, true
}

// resolveSchemaRootForEnvironment returns the directory holding environment's
// schema files at ref, by the rules of schema.ResolveEnvironmentSchemaRoot.
func (ic *InstallationClient) resolveSchemaRootForEnvironment(ctx context.Context, repo, ref, configDir, environment string) (schemaRoot, schemaLinkPath string, err error) {
	schemaRoot, schemaLinkPath, err = schema.ResolveEnvironmentSchemaRoot(configDir, environment, func(entryPath string) (schema.Entry, error) {
		return ic.repositoryEntry(ctx, repo, ref, entryPath)
	})
	if err != nil {
		return "", "", fmt.Errorf("repo %s ref %s: %w", repo, ref, err)
	}
	return schemaRoot, schemaLinkPath, nil
}

// repositoryEntry reports what the repository holds at entryPath on ref,
// without following a symlink there: the Contents API returns a symlink's own
// object, with its target, when the symlink path is fetched directly.
func (ic *InstallationClient) repositoryEntry(ctx context.Context, repo, ref, entryPath string) (schema.Entry, error) {
	content, directoryContent, err := ic.fetchRepositoryContents(ctx, repo, entryPath, ref)
	if err != nil {
		if IsNotFoundError(err) {
			return schema.Entry{Kind: schema.EntryMissing}, nil
		}
		return schema.Entry{}, err
	}
	if directoryContent != nil {
		return schema.Entry{Kind: schema.EntryDirectory}, nil
	}
	if content != nil && content.GetType() == "symlink" {
		return schema.Entry{Kind: schema.EntrySymlink, SymlinkTarget: content.GetTarget()}, nil
	}
	return schema.Entry{Kind: schema.EntryOther}, nil
}

func (ic *InstallationClient) fetchRepositoryContents(ctx context.Context, repo, filePath, ref string) (*gh.RepositoryContent, []*gh.RepositoryContent, error) {
	result, err := ic.getRepositoryContents(ctx, repo, filePath, ref, "fetch repository content")
	if err != nil {
		return nil, nil, err
	}
	return result.fileContent, result.directoryContent, nil
}

// groupFilesByNamespace groups fetched files into the proto SchemaFiles format.
// Files in namespace subdirectories (schema/namespace/table.sql) use the
// subdirectory name as the namespace. Flat files (schema/table.sql) use the
// schema directory name as the namespace (the MySQL database name).
// Namespaces listed in ignoreNamespaces are excluded from the result; the
// second return value lists the namespace keys actually removed, sorted.
func groupFilesByNamespace(files []GitHubFile, schemaPath, environment string, ignoreNamespaces []string) (map[string]*ternv1.SchemaFiles, []string, error) {
	// Build relativePath → content map for the shared helper.
	// file.Path is the full path (e.g., "schema/payments/transactions.sql"),
	// so trimming schemaPath+"/" gives the relative path ("payments/transactions.sql"
	// or "transactions.sql" for flat files).
	rawFiles := make(map[string]string, len(files))
	prefix := schemaPath + "/"
	for _, file := range files {
		relPath, ok := strings.CutPrefix(file.Path, prefix)
		if !ok {
			return nil, nil, fmt.Errorf("file path %q does not start with schema path %q", file.Path, prefix)
		}
		rawFiles[relPath] = file.Content
	}

	grouped, removed, err := schema.GroupFilesByNamespace(rawFiles, path.Base(schemaPath), environment, ignoreNamespaces)
	if err != nil {
		return nil, nil, err
	}

	// Convert schema.SchemaFiles → ternv1.SchemaFiles
	result := make(map[string]*ternv1.SchemaFiles, len(grouped))
	for ns, nsFiles := range grouped {
		result[ns] = &ternv1.SchemaFiles{Files: nsFiles.Files}
	}
	return result, removed, nil
}
