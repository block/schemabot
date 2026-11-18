package github

import (
	"container/list"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"strconv"
	"sync"

	"golang.org/x/sync/singleflight"
)

// maxCachedBlobBytes bounds the content a blobCache holds. Schema files are
// small, so this keeps the files of many large repositories while bounding
// the memory a repository of unusually large files can take.
const maxCachedBlobBytes = 64 << 20

// blobCache holds file content by repository and git object name. A blob's
// content never changes for its object name, so content read once is valid
// for every later read at any commit, in any environment, on any delivery:
// renaming a file, or a push that leaves a file alone, reads nothing new.
//
// Only content that hashes to its object name is held, and a failed read is
// never held, so a later read retries it. Concurrent reads of the same blob
// share one GitHub request. The least recently read content is dropped first
// once the held content would exceed maxBytes. A nil cache holds nothing and
// reads every blob from GitHub.
type blobCache struct {
	maxBytes int

	reads singleflight.Group

	mu      sync.Mutex
	entries map[blobKey]*list.Element
	// order lists the held entries, most recently read first.
	order *list.List
	bytes int
}

type blobKey struct {
	repo, sha string
}

type blobEntry struct {
	key     blobKey
	content string
}

func newBlobCache(maxBytes int) *blobCache {
	return &blobCache{maxBytes: maxBytes, entries: make(map[blobKey]*list.Element), order: list.New()}
}

// read returns the content of the blob sha in repo, from the cache when it
// holds it and otherwise through fetch. Content fetch returns is returned and
// held only once it hashes to sha; content that does not is an error. A sha
// that is not a full object name cannot be verified, so its content is
// fetched every time and never held.
//
// fetch runs once for all concurrent readers of the same blob, under a context
// that keeps ctx's values and deadline but not its cancellation, so one
// reader giving up does not fail the others. Each reader still stops waiting
// when its own ctx is done.
func (c *blobCache) read(ctx context.Context, repo, sha string, fetch func(context.Context) (string, error)) (string, error) {
	if !fullObjectSHA.MatchString(sha) {
		return fetch(ctx)
	}
	if c == nil {
		return fetchVerifiedBlob(ctx, repo, sha, fetch)
	}
	key := blobKey{repo: repo, sha: sha}
	if content, ok := c.get(key); ok {
		return content, nil
	}
	shared := c.reads.DoChan(repo+"\x00"+sha, func() (any, error) {
		if content, ok := c.get(key); ok {
			return content, nil
		}
		fetchCtx, cancel := detachedReadContext(ctx)
		defer cancel()
		content, err := fetchVerifiedBlob(fetchCtx, repo, sha, fetch)
		if err != nil {
			return "", err
		}
		c.put(key, content)
		return content, nil
	})
	select {
	case <-ctx.Done():
		return "", fmt.Errorf("read blob %s in %s: %w", sha, repo, ctx.Err())
	case res := <-shared:
		if res.Err != nil {
			return "", res.Err
		}
		return res.Val.(string), nil
	}
}

// fetchVerifiedBlob returns the content fetch reads for the blob sha in repo,
// once it hashes to sha.
func fetchVerifiedBlob(ctx context.Context, repo, sha string, fetch func(context.Context) (string, error)) (string, error) {
	content, err := fetch(ctx)
	if err != nil {
		return "", err
	}
	if err := verifyBlobContent(sha, content); err != nil {
		return "", fmt.Errorf("read blob %s in %s: %w", sha, repo, err)
	}
	return content, nil
}

// detachedReadContext returns a context with ctx's values and deadline that
// is not cancelled when ctx is.
func detachedReadContext(ctx context.Context) (context.Context, context.CancelFunc) {
	detached := context.WithoutCancel(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		return context.WithDeadline(detached, deadline)
	}
	return context.WithCancel(detached)
}

func (c *blobCache) get(key blobKey) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.entries[key]
	if !ok {
		return "", false
	}
	c.order.MoveToFront(element)
	return element.Value.(*blobEntry).content, true
}

func (c *blobCache) put(key blobKey, content string) {
	size := len(content)
	if size > c.maxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.entries[key]; ok {
		c.order.MoveToFront(element)
		return
	}
	for c.bytes+size > c.maxBytes {
		oldest := c.order.Back()
		evicted := c.order.Remove(oldest).(*blobEntry)
		delete(c.entries, evicted.key)
		c.bytes -= len(evicted.content)
	}
	c.entries[key] = c.order.PushFront(&blobEntry{key: key, content: content})
	c.bytes += size
}

// verifyBlobContent checks that content is the blob git names sha: the hash
// of "blob <length>\0" followed by the content, with SHA-1 for a 40-digit
// name and SHA-256 for a 64-digit one. A sha of any other form is not an
// object name and is reported as an error.
func verifyBlobContent(sha, content string) error {
	var h hash.Hash
	switch len(sha) {
	case sha1.Size * 2:
		h = sha1.New()
	case sha256.Size * 2:
		h = sha256.New()
	default:
		return fmt.Errorf("blob name %q is not a git object name", sha)
	}
	h.Write([]byte("blob " + strconv.Itoa(len(content)) + "\x00"))
	h.Write([]byte(content))
	if got := hex.EncodeToString(h.Sum(nil)); got != sha {
		return fmt.Errorf("blob content hashes to %s, not to the blob's name %s", got, sha)
	}
	return nil
}
