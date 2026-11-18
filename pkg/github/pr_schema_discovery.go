package github

import (
	"context"
	"fmt"
	"maps"
	"path"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/schema"
	"golang.org/x/sync/errgroup"
)

// PRSchema is a logical database and its optional namespace. A flat layout
// has no logical namespace suffix even if its physical root varies by environment.
type PRSchema struct {
	Database  string
	Namespace string
}

type prSchemaConfigImpact struct {
	config  DiscoveredConfig
	schemas map[PRSchema]struct{}
	empty   bool
}

// DiscoverPRSchemas maps changed Git paths to the schemas that consume them.
// The first ref is the proposed revision. Additional refs are comparison
// bases, so deleted inputs and old rename paths resolve from their declarations.
// Returned configs come from the proposed revision and include consumers of
// shared inputs even when their paths are outside the changed-file list.
// The supplied path prefixes must cover every eligible configuration when
// nonempty. Environment selection comes from the server's repo-wide registry.
func (ic *InstallationClient) DiscoverPRSchemas(ctx context.Context, repo string, refs, configDirs []string, files []PRFile, environments func(DiscoveredConfig) []string) ([]PRSchema, []DiscoveredConfig, error) {
	changed := make(map[string]bool)
	for _, file := range files {
		if isDiscoveryInputFile(file.Filename) || file.Symlink {
			changed[file.Filename] = true
		}
		if previous, ok := renamedAwayDiscoveryInputPath(file); ok {
			changed[previous] = true
		}
	}
	if len(changed) == 0 || len(refs) == 0 {
		return nil, nil, nil
	}

	g, ctx := errgroup.WithContext(ctx)
	results := make([][]prSchemaConfigImpact, len(refs))
	for i, ref := range refs {
		g.Go(func() error {
			schemas, err := ic.discoverPRSchemasAtRef(ctx, repo, ref, configDirs, changed, environments)
			results[i] = schemas
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}
	combined := make(map[PRSchema]struct{})
	affectedPaths := make(map[string]bool)
	emptyDatabases := make(map[string]bool)
	for _, snapshot := range results {
		for _, impact := range snapshot {
			maps.Copy(combined, impact.schemas)
			if len(impact.schemas) > 0 || impact.empty {
				affectedPaths[impact.config.Path] = true
			}
			if impact.empty {
				emptyDatabases[impact.config.Config.Database] = true
			}
		}
	}
	for id := range combined {
		delete(emptyDatabases, id.Database)
	}
	for database := range emptyDatabases {
		combined[PRSchema{Database: database}] = struct{}{}
	}
	var configs []DiscoveredConfig
	for _, impact := range results[0] {
		if affectedPaths[impact.config.Path] {
			configs = append(configs, impact.config)
		}
	}
	slices.SortFunc(configs, func(a, b DiscoveredConfig) int { return strings.Compare(a.Path, b.Path) })
	schemas := slices.SortedFunc(maps.Keys(combined), func(a, b PRSchema) int {
		if order := strings.Compare(a.Database, b.Database); order != 0 {
			return order
		}
		return strings.Compare(a.Namespace, b.Namespace)
	})
	return schemas, configs, nil
}

func (ic *InstallationClient) discoverPRSchemasAtRef(ctx context.Context, repo, ref string, dirs []string, changed map[string]bool, environments func(DiscoveredConfig) []string) ([]prSchemaConfigImpact, error) {
	var configs *FindAllConfigsResult
	var err error
	if len(dirs) > 0 {
		configs, err = ic.scanHintDirsForConfigs(ctx, repo, ref, dirs)
	} else {
		// The ordinary fallback knows only the local deployment's hints.
		// Admission needs a complete listing across all control planes.
		var entries []TreeEntry
		var truncated bool
		entries, truncated, err = ic.FetchGitTree(ctx, repo, ref)
		if err == nil && truncated {
			err = ErrConfigDiscoveryTruncated
		}
		if err == nil {
			configs = ic.collectConfigsFromTree(ctx, repo, ref, entries)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("discover PR schema registrations at %s: %w", ref, err)
	}
	if len(configs.InvalidConfigs) > 0 {
		return nil, fmt.Errorf("discover PR schemas at %s: cannot read config %s: %s",
			ref, configs.InvalidConfigs[0].Path, configs.InvalidConfigs[0].Error)
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(schemaDiscoveryConcurrency)
	results := make([]prSchemaConfigImpact, len(configs.ValidConfigs))
	for i, cfg := range configs.ValidConfigs {
		envs := environments(cfg)
		if len(envs) == 0 {
			continue
		}
		g.Go(func() error {
			metadata := schemaRevisionTree{client: ic, repo: repo, ref: ref, levels: make(map[string][]TreeEntry)}
			impact := prSchemaConfigImpact{config: cfg, schemas: make(map[PRSchema]struct{})}
			var hasSchema bool
			locatorsByRoot := make(map[string][]schemaFileLocator)
			for _, environment := range envs {
				root, link, err := schema.ResolveEnvironmentSchemaRoot(cfg.SchemaDir, environment, func(name string) (schema.Entry, error) {
					return metadata.entry(ctx, name)
				})
				if err != nil {
					return err
				}
				locators, ok := locatorsByRoot[root]
				if !ok {
					locators, err = metadata.locators(ctx, root)
					if err != nil {
						return fmt.Errorf("list PR schema inputs at %s:%s: %w", ref, root, err)
					}
					locatorsByRoot[root] = locators
				}
				present := addChangedPRSchemas(impact.schemas, cfg, environment, root, link, locators, changed)
				hasSchema = hasSchema || present
				impact.empty = impact.empty || changed[cfg.Path] || changed[link]
			}
			impact.empty = impact.empty && !hasSchema
			results[i] = impact
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return slices.DeleteFunc(results, func(impact prSchemaConfigImpact) bool { return impact.config.Config == nil }), nil
}

func addChangedPRSchemas(matches map[PRSchema]struct{}, cfg DiscoveredConfig, env, root, link string, locators []schemaFileLocator, changed map[string]bool) bool {
	sharedChange := changed[cfg.Path] || changed[link]
	ignored := schema.ResolveIgnoreNamespaces(cfg.Config.IgnoreNamespaces, env)
	var hasSchema bool
	for _, locator := range locators {
		if !isManagedSchemaFile(locator.displayPath) {
			continue
		}
		hasSchema = true
		if !sharedChange && !changed[locator.fetchPath] && !changed[locator.displayPath] && !changed[path.Dir(locator.displayPath)] {
			continue
		}
		relative := strings.TrimPrefix(locator.displayPath, strings.TrimPrefix(root+"/", "./"))
		logical, named := schema.NamespaceForRelativePath(relative, path.Base(root), "")
		if slices.Contains(ignored, schema.ResolveNamespaceEnvironment(logical, env)) {
			continue
		}
		id := PRSchema{Database: cfg.Config.Database}
		if named {
			id.Namespace = strings.ReplaceAll(logical, "$ENV", "{env}")
		}
		matches[id] = struct{}{}
	}
	return hasSchema
}
