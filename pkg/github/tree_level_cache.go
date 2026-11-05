package github

import (
	"regexp"
	"sync"
)

// maxCachedTreeEntries bounds the tree entries a treeLevelCache holds. A
// cache that would grow past it starts over empty.
const maxCachedTreeEntries = 100_000

// fullObjectSHA matches a full hex object name. Only a level read by object
// name is immutable: a branch name or short SHA can resolve to another tree
// later.
var fullObjectSHA = regexp.MustCompile(`^([0-9a-f]{40}|[0-9a-f]{64})$`)

// treeLevelCache holds shallow tree levels by repository and tree SHA. A tree
// object never changes, so a level read once is valid for every later call on
// the same installation. A nil cache holds nothing.
type treeLevelCache struct {
	mu      sync.Mutex
	levels  map[treeLevelKey][]TreeEntry
	entries int
}

type treeLevelKey struct {
	repo, sha string
}

func newTreeLevelCache() *treeLevelCache {
	return &treeLevelCache{levels: make(map[treeLevelKey][]TreeEntry)}
}

func (c *treeLevelCache) get(repo, sha string) ([]TreeEntry, bool) {
	if c == nil || !fullObjectSHA.MatchString(sha) {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entries, ok := c.levels[treeLevelKey{repo, sha}]
	return entries, ok
}

func (c *treeLevelCache) put(repo, sha string, entries []TreeEntry) {
	if c == nil || !fullObjectSHA.MatchString(sha) || len(entries) > maxCachedTreeEntries {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := treeLevelKey{repo, sha}
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
