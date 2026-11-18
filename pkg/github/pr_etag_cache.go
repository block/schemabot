package github

import "sync"

// maxCachedPRETags bounds the pull requests a prETagCache remembers. A cache
// that would grow past it starts over empty, which costs one full read per
// pull request and never a wrong answer.
const maxCachedPRETags = 10_000

// prETagCache remembers, per pull request, the ETag GitHub last returned for
// it together with the PullRequestInfo parsed from that response. A later read
// sends the ETag as If-None-Match. GitHub answers 304 Not Modified only when
// the pull request's current representation still carries that ETag, so the
// stored info is exactly what a full read would return, and a 304 does not
// count against the installation's primary rate limit.
//
// The cache never answers a read on its own: every read still asks GitHub,
// so a new commit, a base change, a merge, or a close is seen on the very
// next read. A nil cache holds nothing.
type prETagCache struct {
	mu      sync.Mutex
	entries map[prETagKey]prETagEntry
}

type prETagKey struct {
	repo string
	pr   int
}

type prETagEntry struct {
	etag string
	info PullRequestInfo
}

func newPRETagCache() *prETagCache {
	return &prETagCache{entries: make(map[prETagKey]prETagEntry)}
}

// get returns the ETag and info stored for (repo, pr).
func (c *prETagCache) get(repo string, pr int) (prETagEntry, bool) {
	if c == nil {
		return prETagEntry{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[prETagKey{repo, pr}]
	return entry, ok
}

// put stores the ETag GitHub returned with info. Concurrent reads of the same
// pull request may store out of order; every stored pair is still a matching
// ETag and body, so an older pair only costs a full read on the next call.
func (c *prETagCache) put(repo string, pr int, etag string, info PullRequestInfo) {
	if c == nil || etag == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := prETagKey{repo, pr}
	if _, ok := c.entries[key]; !ok && len(c.entries) >= maxCachedPRETags {
		c.entries = make(map[prETagKey]prETagEntry)
	}
	c.entries[key] = prETagEntry{etag: etag, info: info}
}
