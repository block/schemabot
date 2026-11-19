package github

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"strconv"
	"sync"
)

// maxCachedBlobBytes bounds the memory a blobContentCache holds: the content
// of its entries, their keys, and an allowance for each entry's bookkeeping.
// A cache that would grow past it starts over empty.
const maxCachedBlobBytes = 64 << 20

// blobEntryOverheadBytes is the allowance counted for one entry beyond its
// content and key strings, so a cache of many small files stays near its
// bound instead of several times over it.
const blobEntryOverheadBytes = 256

// blobContentCache holds file content by repository and blob SHA. A blob
// object never changes, so content read once is valid for every later read of
// the same blob on the same installation, whichever ref, path, or environment
// reached it. It stores content only under the SHA that content hashes to, so
// an entry is the blob its key names whatever the read that produced it
// returned. A nil cache holds nothing.
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
// installation's blob cache, calling fetch on a miss. What fetch returns is
// returned to the caller either way, and is cached only when it is the content
// of that exact blob. A directory listing reports a symbolic link to a file as
// a file under the link's own SHA while a read of its path returns the target
// file's content, so such a file is read every time.
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
	size := len(repo) + len(sha) + len(content) + blobEntryOverheadBytes
	if c == nil || size > maxCachedBlobBytes || !isBlobContent(sha, content) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := blobContentKey{repo, sha}
	if _, ok := c.blobs[key]; ok {
		return
	}
	if c.bytes+size > maxCachedBlobBytes {
		c.blobs = make(map[blobContentKey]string)
		c.bytes = 0
	}
	c.blobs[key] = content
	c.bytes += size
}

// isBlobContent reports whether content is what the git blob named sha holds.
// Git names a blob by the hash of "blob <length>\x00" followed by its content:
// SHA-1 for a 40-character name and SHA-256 for a 64-character one. Any other
// name, a short or branch-like one included, names no content.
func isBlobContent(sha, content string) bool {
	var h hash.Hash
	switch len(sha) {
	case hex.EncodedLen(sha1.Size):
		h = sha1.New()
	case hex.EncodedLen(sha256.Size):
		h = sha256.New()
	default:
		return false
	}
	h.Write([]byte("blob " + strconv.Itoa(len(content)) + "\x00"))
	h.Write([]byte(content))
	return hex.EncodeToString(h.Sum(nil)) == sha
}
