package github

import (
	"context"
	"errors"
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
	config        DiscoveredConfig
	schemas       map[PRSchema]struct{}
	countDatabase bool
}

// DiscoverPRSchemas counts changes under their nearest SchemaBot config on each
// revision. Comparison refs supply the owners of removed or renamed inputs.
func (ic *InstallationClient) DiscoverPRSchemas(ctx context.Context, repo string, refs []string, files []PRFile, environments func(DiscoveredConfig) []string) ([]PRSchema, error) {
	changed := make(map[string]bool)
	modifiedConfigs := make(map[string]bool)
	for _, file := range files {
		if isConfigFile(file.Filename) && file.Status == "modified" {
			modifiedConfigs[file.Filename] = true
		}
		if isDiscoveryInputFile(file.Filename) || file.Symlink {
			changed[file.Filename] = true
		}
		if previous, ok := renamedAwayDiscoveryInputPath(file); ok {
			changed[previous] = true
		}
	}
	if len(changed) == 0 || len(refs) == 0 {
		return nil, nil
	}

	g, ctx := errgroup.WithContext(ctx)
	results := make([][]prSchemaConfigImpact, len(refs))
	for i, ref := range refs {
		g.Go(func() error {
			schemas, err := ic.discoverPRSchemasAtRef(ctx, repo, ref, refs[0], changed, modifiedConfigs, environments)
			results[i] = schemas
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	combined := make(map[PRSchema]struct{})
	databaseFallbacks := make(map[string]bool)
	for _, snapshot := range results {
		for _, impact := range snapshot {
			maps.Copy(combined, impact.schemas)
			if impact.countDatabase {
				databaseFallbacks[impact.config.Config.Database] = true
			}
		}
	}
	for id := range combined {
		delete(databaseFallbacks, id.Database)
	}
	for database := range databaseFallbacks {
		combined[PRSchema{Database: database}] = struct{}{}
	}
	schemas := slices.SortedFunc(maps.Keys(combined), func(a, b PRSchema) int {
		if order := strings.Compare(a.Database, b.Database); order != 0 {
			return order
		}
		return strings.Compare(a.Namespace, b.Namespace)
	})
	return schemas, nil
}

func (ic *InstallationClient) discoverPRSchemasAtRef(ctx context.Context, repo, ref, head string, changed, modifiedConfigs map[string]bool, environments func(DiscoveredConfig) []string) ([]prSchemaConfigImpact, error) {
	type owner struct {
		config DiscoveredConfig
		paths  map[string]bool
	}
	owners := make(map[string]*owner)
	probes := make(configProbeCache)
	headProbes := make(configProbeCache)
	for name := range changed {
		// Configs own themselves. A config added in the PR need not exist
		// on the base, and a removed config need not exist on the head.
		dir := path.Dir(name)
		var config *SchemabotConfig
		var err error
		if isConfigFile(name) {
			config, err = ic.probeConfigDir(ctx, repo, ref, dir, probes)
		} else {
			config, dir, err = ic.findNearestConfig(ctx, repo, ref, name, probes)
		}
		if err != nil {
			// A malformed comparison config has no usable historical owner.
			// Its repaired, registered copy at the same path is counted at head.
			if invalid, ok := errors.AsType[*invalidSchemaConfigError](err); ok && ref != head && modifiedConfigs[invalid.path] {
				configDir := path.Dir(invalid.path)
				current, readErr := ic.probeConfigDir(ctx, repo, head, configDir, headProbes)
				if readErr != nil {
					return nil, readErr
				}
				if current != nil && len(environments(newDiscoveredConfig(current, configDir))) > 0 {
					continue
				}
			}
			return nil, fmt.Errorf("find schema config for %s at %s: %w", name, ref, err)
		}
		if config == nil {
			continue
		}
		if owners[dir] == nil {
			owners[dir] = &owner{config: newDiscoveredConfig(config, dir), paths: make(map[string]bool)}
		}
		owners[dir].paths[name] = true
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(schemaDiscoveryConcurrency)
	results := make([]prSchemaConfigImpact, len(owners))
	for i, dir := range slices.Sorted(maps.Keys(owners)) {
		cfg, changed := owners[dir].config, owners[dir].paths
		envs := environments(cfg)
		if len(envs) == 0 {
			continue
		}
		g.Go(func() error {
			metadata := schemaRevisionTree{client: ic, repo: repo, ref: ref, levels: make(map[string][]TreeEntry)}
			impact := prSchemaConfigImpact{config: cfg, schemas: make(map[PRSchema]struct{})}
			var hasSchema bool
			var roots []string
			for _, environment := range envs {
				root, link, err := schema.ResolveEnvironmentSchemaRoot(cfg.SchemaDir, environment, func(name string) (schema.Entry, error) {
					return metadata.entry(ctx, name)
				})
				if err != nil {
					return err
				}
				roots = append(roots, root)
				// Removing an environment link selects the fallback root on this side.
				rootChanged := changed[cfg.Path] || changed[path.Join(cfg.SchemaDir, environment)] || changed[link]
				locators, err := metadata.schemaPaths(ctx, root, changed, rootChanged)
				if err != nil {
					return fmt.Errorf("list PR schema inputs at %s:%s: %w", ref, root, err)
				}
				present := addChangedPRSchemas(impact.schemas, cfg, environment, root, rootChanged, locators, changed)
				hasSchema = hasSchema || present
				impact.countDatabase = impact.countDatabase || rootChanged
			}
			impact.countDatabase = impact.countDatabase && !hasSchema
			// A backing file may have no direct logical schema path. Ordinary
			// discovery identifies its database; count that owner once when no
			// namespace was identified, without looking for incoming links.
			for name := range changed {
				if isManagedSchemaFile(name) && !slices.ContainsFunc(roots, func(root string) bool {
					_, direct := schemaRootEntry(root, name)
					return direct
				}) {
					entry, found, err := ic.resolveGitTreeEntry(ctx, repo, ref, name, metadata.levels)
					if err != nil {
						return err
					}
					if found && entry.Type == "blob" {
						impact.countDatabase = true
						break
					}
				}
			}
			results[i] = impact
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return slices.DeleteFunc(results, func(impact prSchemaConfigImpact) bool { return impact.config.Config == nil }), nil
}

func addChangedPRSchemas(matches map[PRSchema]struct{}, cfg DiscoveredConfig, env, root string, wholeRoot bool, paths []string, changed map[string]bool) bool {
	ignored := schema.ResolveIgnoreNamespaces(cfg.Config.IgnoreNamespaces, env)
	var hasSchema bool
	for _, name := range paths {
		if !isManagedSchemaFile(name) {
			continue
		}
		hasSchema = true
		if !wholeRoot && !changed[name] && !changed[path.Dir(name)] {
			continue
		}
		relative := strings.TrimPrefix(name, strings.TrimPrefix(root+"/", "./"))
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
