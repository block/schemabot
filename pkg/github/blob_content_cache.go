package github

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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

// sharedBlobRead is the result of one read shared between the callers that
// missed the cache on the same blob at the same time. isBlob reports whether
// the content hashes to the blob SHA the read was keyed by.
type sharedBlobRead struct {
	content string
	isBlob  bool
}

// cachedBlobContent returns the content of blob sha in repo from the
// installation's blob cache, calling fetch on a miss. What fetch returns is
// returned to the caller either way, and is cached only when it is the content
// of that exact blob. A directory listing reports a symbolic link to a file as
// a file under the link's own SHA while a read of its path returns the target
// file's content, so such a file is read every time.
//
// A caller whose context has already ended starts no read. Callers that miss
// on the same blob at the same time share one read; each returns its own
// context's error as soon as that context ends, and the read continues for
// the others. The shared content is given to the other callers only when it
// is the blob the key names: a symbolic link read at different commits shares
// a key but not content, so each other caller then reads the file for itself.
// A read by path can also fail for the reading caller's commit alone, so a
// caller that gets another caller's not-found error reads for itself too.
// Every other error is shared, so an outage costs one read, not one per caller.
func (ic *InstallationClient) cachedBlobContent(ctx context.Context, repo, sha string, fetch func(context.Context) (string, error)) (string, error) {
	if content, ok := ic.blobs.get(repo, sha); ok {
		return content, nil
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if !fullObjectSHA.MatchString(sha) {
		// Without a full blob SHA there is no blob to share a read of.
		return fetch(ctx)
	}

	// readHere is set only by the flight this caller started, and is read
	// after the flight's result arrives, which happens after it is set.
	readHere := false
	flight := ic.blobReads.DoChan(repo+"\x00"+sha, func() (any, error) {
		readHere = true
		// A read may have landed between the first cache miss and the start
		// of this flight.
		if content, ok := ic.blobs.get(repo, sha); ok {
			return sharedBlobRead{content: content, isBlob: true}, nil
		}
		// The read is detached from the caller that started it, so its giving
		// up does not fail the others. It is capped at sharedFetchTimeout even
		// when that caller is the only one and would have waited longer, and
		// the client's retries for an unavailable GitHub run inside that cap.
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sharedFetchTimeout)
		defer cancel()
		content, err := fetch(fetchCtx)
		if err != nil {
			return nil, sharedReadError(fetchCtx, err)
		}
		ic.blobs.put(repo, sha, content)
		return sharedBlobRead{content: content, isBlob: isBlobContent(sha, content)}, nil
	})
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case result := <-flight:
		if result.Err != nil {
			if readHere || !IsNotFoundError(result.Err) {
				return "", result.Err
			}
			// Another caller's read found nothing, which may hold only at
			// its commit.
			ic.logger.Debug("shared blob read found no file at another caller's commit; reading the file for this caller",
				"repo", repo, "blob_sha", sha, "error", result.Err)
			return ic.readBlobForCaller(ctx, repo, sha, fetch)
		}
		read := result.Val.(sharedBlobRead)
		if read.isBlob || readHere {
			return read.content, nil
		}
		// The content came from another caller's read and is not the blob
		// the key names, so it may be another commit's content.
		ic.logger.Debug("shared blob read returned content of another blob; reading the file for this caller",
			"repo", repo, "blob_sha", sha)
		return ic.readBlobForCaller(ctx, repo, sha, fetch)
	}
}

// readBlobForCaller reads a blob on the caller's own context, outside any
// shared read, for a caller that cannot use what a shared read returned.
func (ic *InstallationClient) readBlobForCaller(ctx context.Context, repo, sha string, fetch func(context.Context) (string, error)) (string, error) {
	content, err := fetch(ctx)
	if err != nil {
		return "", err
	}
	ic.blobs.put(repo, sha, content)
	return content, nil
}

// sharedReadError classifies a shared read cut by its own sharedFetchTimeout
// as GitHub being unavailable. The cap can land mid-request, where the error
// is already classified that way, or while the client waits to retry an
// unavailable read, where it is a bare deadline error; both mean the same
// thing to the caller. Any other error is returned as it is.
func sharedReadError(fetchCtx context.Context, err error) error {
	if !errors.Is(fetchCtx.Err(), context.DeadlineExceeded) || !errors.Is(err, context.DeadlineExceeded) || IsUnavailableError(err) {
		return err
	}
	return &githubUnavailableError{err: fmt.Errorf("shared read stopped at its %s limit: %w", sharedFetchTimeout, err)}
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
