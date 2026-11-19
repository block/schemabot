package github

import "sync"

// maxCachedBlobBytes bounds the file content a blobContentCache holds. A
// cache that would grow past it starts over empty.
const maxCachedBlobBytes = 64 << 20

// blobContentCache holds file content by repository and blob SHA. A blob
// object never changes, so content read once is valid for every later read of
// the same blob on the same installation, whichever ref, path, or environment
// reached it. A nil cache holds nothing.
type blobContentCache struct {
	mu    sync.Mutex
	blobs map[blobContentKey]string
	bytes int
}

type blobContentKey struct {
	repo, sha string
}

func newBlobContentCache() *blobContentCache {
	return &blobContentCache{blobs: make(map[blobContentKey]string)}
}

// cachedBlobContent returns the content of blob sha in repo from the
// installation's blob cache, calling fetch and caching its result on a miss.
// fetch must return the content of that exact blob.
func (ic *InstallationClient) cachedBlobContent(repo, sha string, fetch func() (string, error)) (string, error) {
	if content, ok := ic.blobs.get(repo, sha); ok {
		return content, nil
	}
	content, err := fetch()
	if err != nil {
		return "", err
	}
	ic.blobs.put(repo, sha, content)
	return content, nil
}

func (c *blobContentCache) get(repo, sha string) (string, bool) {
	if c == nil || !fullObjectSHA.MatchString(sha) {
		return "", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	content, ok := c.blobs[blobContentKey{repo, sha}]
	return content, ok
}

func (c *blobContentCache) put(repo, sha, content string) {
	if c == nil || !fullObjectSHA.MatchString(sha) || len(content) > maxCachedBlobBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := blobContentKey{repo, sha}
	if _, ok := c.blobs[key]; ok {
		return
	}
	if c.bytes+len(content) > maxCachedBlobBytes {
		c.blobs = make(map[blobContentKey]string)
		c.bytes = 0
	}
	c.blobs[key] = content
	c.bytes += len(content)
}
