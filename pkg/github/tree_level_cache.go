package github

import (
	"regexp"
	"slices"
	"sync"
)

// maxCachedTreeEntries bounds the tree entries a treeLevelCache holds. A
// cache that would grow past it starts over empty.
const maxCachedTreeEntries = 100_000

// fullObjectSHA matches a full hex object name. Only a level read by object
// name is immutable: a branch name or short SHA can resolve to another tree
// later.
var fullObjectSHA = regexp.MustCompile(`^([0-9a-f]{40}|[0-9a-f]{64})$`)

// treeLevelCache holds immutable tree listings and parsed config blobs. A Git
// object never changes, so a result read once is valid for every later call on
// the same installation. A nil cache holds nothing.
type treeLevelCache struct {
	mu      sync.Mutex
	levels  map[treeLevelKey][]TreeEntry
	entries int
	configs map[treeLevelKey]SchemabotConfig
}

type treeLevelKey struct {
	repo, sha string
	recursive bool
}

func newTreeLevelCache() *treeLevelCache {
	return &treeLevelCache{levels: make(map[treeLevelKey][]TreeEntry)}
}

func (c *treeLevelCache) get(key treeLevelKey) ([]TreeEntry, bool) {
	if c == nil || !fullObjectSHA.MatchString(key.sha) {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entries, ok := c.levels[key]
	return entries, ok
}

func (c *treeLevelCache) put(key treeLevelKey, entries []TreeEntry) {
	if c == nil || !fullObjectSHA.MatchString(key.sha) || len(entries) > maxCachedTreeEntries {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.levels[key]; ok {
		return
	}
	if c.entries+len(entries) > maxCachedTreeEntries {
		c.levels = make(map[treeLevelKey][]TreeEntry)
		c.entries = 0
	}
	c.levels[key] = entries
	c.entries += len(entries)
}

const maxCachedSchemaConfigs = 4096

func (c *treeLevelCache) getConfig(key treeLevelKey) *SchemabotConfig {
	if c == nil || !fullObjectSHA.MatchString(key.sha) {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	config, ok := c.configs[key]
	if !ok {
		return nil
	}
	return cloneSchemaConfig(config)
}

func (c *treeLevelCache) putConfig(key treeLevelKey, config *SchemabotConfig) {
	if c == nil || !fullObjectSHA.MatchString(key.sha) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.configs == nil || len(c.configs) >= maxCachedSchemaConfigs {
		c.configs = make(map[treeLevelKey]SchemabotConfig)
	}
	c.configs[key] = *cloneSchemaConfig(*config)
}

func cloneSchemaConfig(config SchemabotConfig) *SchemabotConfig {
	config.IgnoreNamespaces = slices.Clone(config.IgnoreNamespaces)
	config.IgnoreTables = slices.Clone(config.IgnoreTables)
	return &config
}
