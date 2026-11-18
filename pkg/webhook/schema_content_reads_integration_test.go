//go:build integration

package webhook

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"

	"path"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A PR adds a database's schema files, planned for staging and production.
// Each delivery reads every distinct schema file content from GitHub at most
// once, however many environments it plans, and a later delivery reads only
// content no earlier one has read:
//
//   - the PR opens: each file's content is read once, not once per environment;
//   - a push renames every file without changing what it says: nothing is read;
//   - a plan command re-plans the same head: nothing is read;
//   - a push edits one file: only that file's new content is read.
//
// Each delivery ends with a plan comment for the head it was sent for, so every
// count is of a delivery that planned both environments.
func TestE2ESchemaContentIsReadOncePerBlob(t *testing.T) {
	const dbName = "webhook_blob_reads"
	svc := setupE2EServiceMultiEnv(t, dbName)

	const tables = 10
	configPath := "schema/schemabot.yaml"
	config := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	opened := map[string]string{configPath: config}
	renamed := map[string]string{configPath: config}
	schemaSQL := map[string]string{}
	for i := range tables {
		name := fmt.Sprintf("t%02d", i)
		content := fmt.Sprintf("CREATE TABLE `%s` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;", name)
		schemaSQL[name+".sql"] = content
		opened[schemaFixturePath(dbName, name+".sql")] = content
		renamed[schemaFixturePath(dbName, "table_"+name+".sql")] = content
	}
	edited := maps.Clone(renamed)
	edited[schemaFixturePath(dbName, "table_t00.sql")] = "CREATE TABLE `t00` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

	contents := newSchemaContentsFake(map[string]map[string]string{
		"abc123":    opened,
		"renamed22": renamed,
		"edited333": edited,
	})
	mux := http.NewServeMux()
	server := httptest.NewServer(contents.wrap(mux))
	t.Cleanup(server.Close)
	client := gh.NewClient(nil)
	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client.BaseURL = baseURL

	result := setupFakeGitHubForPlanWithConfigs(t, mux, schemaSQL, map[string]string{configPath: config}, dbName, nil)
	registerFreshBaseComparison(t, mux, "renamed22")
	registerFreshBaseComparison(t, mux, "edited333")
	h := newE2EHandler(t, svc, client)
	drainWebhookWorkOnCleanup(t, h)

	comments := consumeComments(t, result)
	deliver := func(t *testing.T, headSHA string, req *http.Request) schemaContentReads {
		t.Helper()
		result.MovedHeadSHA.Store(&headSHA)
		before := contents.reads()
		posted := comments.len()
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		require.Equal(t, http.StatusOK, rr.Code)
		comments.awaitPlan(t, posted, headSHA)
		ctx, cancel := context.WithTimeout(t.Context(), webhookIntegrationCheckRunDeadline)
		defer cancel()
		h.DrainInProcessWebhookWork(ctx)
		require.NoError(t, ctx.Err(), "plan work was still running when the reads were counted")
		return contents.reads().since(before)
	}
	push := func(headSHA string) *http.Request {
		return buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "synchronize", headSHA: headSHA, headRef: "feature-branch"}, nil)
	}

	t.Run("the PR opens", func(t *testing.T) {
		reads := deliver(t, "abc123", buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "opened"}, nil))
		t.Logf("schema content reads planning %d files for 2 environments: %+v", tables, reads)
		assert.Equal(t, schemaContentReads{blobs: tables}, reads,
			"each distinct schema file content is read once across both environments")
	})
	t.Run("a push renames every file", func(t *testing.T) {
		reads := deliver(t, "renamed22", push("renamed22"))
		assert.Equal(t, schemaContentReads{}, reads, "renamed files whose content is unchanged are not read again")
	})
	t.Run("a plan command re-plans the head", func(t *testing.T) {
		reads := deliver(t, "renamed22", buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot plan", isPR: true}, nil))
		assert.Equal(t, schemaContentReads{}, reads, "a re-plan of an unchanged head reads no schema content")
	})
	t.Run("a push edits one file", func(t *testing.T) {
		reads := deliver(t, "edited333", push("edited333"))
		assert.Equal(t, schemaContentReads{blobs: 1}, reads, "only the edited file's new content is read")
	})
}

// observedComments records every comment a fake GitHub accepts, so a test can
// wait for the one a delivery posts without the fake's channel filling.
type observedComments struct {
	mu     sync.Mutex
	bodies []string
}

// consumeComments drains result's comments and Check Run writes for the rest
// of the test, recording the comments.
func consumeComments(t *testing.T, result *planFlowResult) *observedComments {
	t.Helper()
	observed := &observedComments{}
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go func() {
		for {
			select {
			case body := <-result.comments:
				observed.mu.Lock()
				observed.bodies = append(observed.bodies, body)
				observed.mu.Unlock()
			case <-result.checkRuns:
			case <-done:
				return
			}
		}
	}()
	return observed
}

func (o *observedComments) len() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.bodies)
}

// awaitPlan waits for a plan comment, after the first skip comments, that
// planned headSHA.
func (o *observedComments) awaitPlan(t *testing.T, skip int, headSHA string) {
	t.Helper()
	planned := "](https://github.com/octocat/hello-world/commit/" + headSHA + ")"
	require.Eventually(t, func() bool {
		o.mu.Lock()
		defer o.mu.Unlock()
		for _, body := range o.bodies[skip:] {
			if strings.Contains(body, "## Schema Change Plan") && strings.Contains(body, "planned from [`") && strings.Contains(body, planned) {
				return true
			}
		}
		return false
	}, webhookIntegrationCheckRunDeadline, 10*time.Millisecond, "want a plan comment planned from %s", headSHA)
}

// schemaContentReads counts the reads of schema file content a fake GitHub
// served: by path through the Contents API, and by object name as a blob.
type schemaContentReads struct {
	byPath int
	blobs  int
}

func (r schemaContentReads) since(before schemaContentReads) schemaContentReads {
	return schemaContentReads{byPath: r.byPath - before.byPath, blobs: r.blobs - before.blobs}
}

// schemaContentsFake serves the Contents API and git blobs of a repository's
// schema directory the way GitHub does, at each head a test pushes: a
// directory listing names each entry's object, a file read by path returns its
// content, and a blob read by object name returns the content that hashes to
// it. Every other request goes to the wrapped handler.
type schemaContentsFake struct {
	// layouts maps a commit to the repository paths it holds and their content.
	layouts map[string]map[string]string
	// blobs maps the git object name of every content to the content.
	blobs map[string]string

	mu      sync.Mutex
	counted schemaContentReads
}

func newSchemaContentsFake(layouts map[string]map[string]string) *schemaContentsFake {
	f := &schemaContentsFake{layouts: layouts, blobs: map[string]string{}}
	for _, layout := range layouts {
		for _, content := range layout {
			f.blobs[gitBlobSHA(content)] = content
		}
	}
	return f
}

// gitBlobSHA is the git object name of a blob holding content.
func gitBlobSHA(content string) string {
	sum := sha1.Sum(fmt.Appendf(nil, "blob %d\x00%s", len(content), content))
	return hex.EncodeToString(sum[:])
}

func (f *schemaContentsFake) reads() schemaContentReads {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counted
}

func (f *schemaContentsFake) count(byPath, blobs int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.counted.byPath += byPath
	f.counted.blobs += blobs
}

func (f *schemaContentsFake) wrap(next http.Handler) http.Handler {
	const contentsPrefix = "/repos/octocat/hello-world/contents/"
	const blobsPrefix = "/repos/octocat/hello-world/git/blobs/"
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			next.ServeHTTP(w, r)
			return
		}
		if sha, ok := strings.CutPrefix(r.URL.Path, blobsPrefix); ok {
			content, known := f.blobs[sha]
			if !known {
				next.ServeHTTP(w, r)
				return
			}
			f.count(0, 1)
			writeContentsJSON(w, map[string]any{
				"sha": sha, "size": len(content), "encoding": "base64",
				"content": base64.StdEncoding.EncodeToString([]byte(content)),
			})
			return
		}
		repoPath, ok := strings.CutPrefix(r.URL.Path, contentsPrefix)
		layout, known := f.layouts[r.URL.Query().Get("ref")]
		if !ok || !known {
			next.ServeHTTP(w, r)
			return
		}
		if content, isFile := layout[repoPath]; isFile {
			if path.Ext(repoPath) == ".sql" {
				f.count(1, 0)
			}
			writeContentsJSON(w, contentsEntry(repoPath, "file", content, true))
			return
		}
		listing := directoryListing(layout, repoPath)
		if listing == nil {
			http.NotFound(w, r)
			return
		}
		writeContentsJSON(w, listing)
	})
}

// directoryListing lists the entries directly under dir as the Contents API
// lists a directory, or returns nil when layout holds nothing under dir.
func directoryListing(layout map[string]string, dir string) []map[string]any {
	seen := map[string]bool{}
	var listing []map[string]any
	for _, repoPath := range slices.Sorted(maps.Keys(layout)) {
		rest, under := strings.CutPrefix(repoPath, dir+"/")
		if !under {
			continue
		}
		name, _, isDir := strings.Cut(rest, "/")
		if seen[name] {
			continue
		}
		seen[name] = true
		if isDir {
			listing = append(listing, contentsEntry(dir+"/"+name, "dir", "", false))
			continue
		}
		listing = append(listing, contentsEntry(repoPath, "file", layout[repoPath], false))
	}
	return listing
}

// contentsEntry is one Contents API entry. A file names the blob holding its
// content, and carries the content itself only when read by path.
func contentsEntry(repoPath, entryType, content string, withContent bool) map[string]any {
	entry := map[string]any{"type": entryType, "name": path.Base(repoPath), "path": repoPath}
	if entryType == "dir" {
		entry["sha"] = gitBlobSHA("tree " + repoPath)
		return entry
	}
	entry["sha"] = gitBlobSHA(content)
	entry["size"] = len(content)
	if withContent {
		entry["encoding"] = "base64"
		entry["content"] = base64.StdEncoding.EncodeToString([]byte(content))
	}
	return entry
}

func writeContentsJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
