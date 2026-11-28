package webhook

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
)

// A command on a pull request in a listen-only repository is ignored before
// SchemaBot reacts to it or replies, so the pull request shows nothing from
// SchemaBot and no plan or apply starts.
func TestWebhookIssueCommentIgnoresCommandOnListenOnlyRepository(t *testing.T) {
	client, mux := setupGitHubServer(t)
	requests := make(chan string, 10)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Method + " " + r.URL.Path
		w.WriteHeader(http.StatusCreated)
	})

	service := api.New(nil, &api.ServerConfig{
		Repos: map[string]api.RepoConfig{"octocat/hello-world": {ListenOnly: true}},
	}, nil, testLogger())
	h := &Handler{
		insecureSkipSignatureVerification: true,
		service:                           service,
		ghClients:                         ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:                            testLogger(),
	}
	drainWebhookWorkOnCleanup(t, h)

	for _, command := range []string{"schemabot help", "schemabot plan", "schemabot apply -e staging"} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: command, isPR: true}, nil))

		require.Equal(t, http.StatusOK, rr.Code, command)
		assert.JSONEq(t, `{"message":"repository is listen-only"}`, rr.Body.String(), command)
	}

	select {
	case req := <-requests:
		require.FailNow(t, fmt.Sprintf("unexpected GitHub request for a command on a listen-only repository: %s", req))
	case <-time.After(100 * time.Millisecond):
	}
}

// listenOnlyCheckTestHandler serves a listen-only repository in a deployment
// that gates staging and production. It records each Check Run lookup and each
// Check Run write the handler sends to GitHub.
func listenOnlyCheckTestHandler(t *testing.T, headSHA string) (*Handler, chan string, chan string) {
	t.Helper()
	client, mux := setupGitHubServer(t)
	lookups := make(chan string, 10)
	writes := make(chan string, 10)
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/"+headSHA+"/check-runs", func(w http.ResponseWriter, r *http.Request) {
		lookups <- r.URL.Query().Get("check_name")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"total_count":0,"check_runs":[]}`))
	})
	mux.HandleFunc("/repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		writes <- r.Method + " " + r.URL.Path
		w.WriteHeader(http.StatusCreated)
	})

	service := api.New(nil, &api.ServerConfig{
		AllowedEnvironments: []string{"staging", "production"},
		Repos:               map[string]api.RepoConfig{"octocat/hello-world": {ListenOnly: true}},
	}, nil, testLogger())
	h := &Handler{
		insecureSkipSignatureVerification: true,
		service:                           service,
		ghClients:                         ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")}),
		logger:                            testLogger(),
	}
	return h, lookups, writes
}

// receivedNames drains every name already sent on ch. The handlers under test
// run synchronously, so everything they sent is buffered by the time they
// return.
func receivedNames(ch chan string) []string {
	var names []string
	for {
		select {
		case name := <-ch:
			names = append(names, name)
		default:
			return names
		}
	}
}

// A push to the default branch of a listen-only repository looks up the
// checks it would post, as for any repository, but posts none of them.
func TestWebhookPushPostsNoChecksOnListenOnlyRepository(t *testing.T) {
	h, lookups, writes := listenOnlyCheckTestHandler(t, "pushsha123")

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildPushWebhookRequest(t, "refs/heads/main", "pushsha123", false))

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "default-branch checks skipped (listen-only repository)")
	assert.ElementsMatch(t, []string{"SchemaBot (staging)", "SchemaBot (production)"}, receivedNames(lookups),
		"the Check Run lookups still run, so their cost is measured")
	assert.Empty(t, receivedNames(writes), "no Check Run should be written to a listen-only repository")
}

// A merge queue run in a listen-only repository looks up the checks it would
// post, as for any repository, but gets no passing check from SchemaBot.
func TestWebhookMergeGroupPostsNoChecksOnListenOnlyRepository(t *testing.T) {
	h, lookups, writes := listenOnlyCheckTestHandler(t, "mergesha123")

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildMergeGroupWebhookRequest(t, "checks_requested", "mergesha123", nil))

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "merge_group checks skipped (listen-only repository)")
	assert.ElementsMatch(t, []string{"SchemaBot (staging)", "SchemaBot (production)"}, receivedNames(lookups),
		"the Check Run lookups still run, so their cost is measured")
	assert.Empty(t, receivedNames(writes), "no Check Run should be written to a listen-only repository")
}

// A listen-only repository publishes no aggregate check, so after config
// discovery there is no stored block to release. Its database is registered
// by another deployment, not this one, and the release must not warn that
// merge stays blocked. It still reads the PR head once, as a deployment that
// serves the repository does, so the listener measures that read.
// listenOnlyAggregateClear runs the aggregate block release on a listen-only
// repository whose PR read answers with status, and returns the number of PR
// reads and the log entries. The context carries a request-scoped PR-info cache
// already holding the PR, as an auto-plan delivery's context does after its
// first read, so the count shows whether the release reads around that cache.
func listenOnlyAggregateClear(t *testing.T, status int) (int32, []map[string]any) {
	t.Helper()
	client, mux := setupGitHubServer(t)
	var prReads atomic.Int32
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		prReads.Add(1)
		if status != http.StatusOK {
			http.Error(w, `{"message":"Not Found"}`, status)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(gh.PullRequest{
			State: new("open"),
			Head:  &gh.PullRequestBranch{Ref: new("feature-branch"), SHA: new("abc123")},
			Base:  &gh.PullRequestBranch{Ref: new("main"), SHA: new("def456")},
		}))
	})

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	// No storage: a release that went on to read stored blocks would panic.
	service := api.New(nil, &api.ServerConfig{
		Repos: map[string]api.RepoConfig{"octocat/hello-world": {ListenOnly: true}},
	}, nil, logger)
	h := &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, logger)}),
		logger:    logger,
	}

	ctx := ghclient.WithPRInfo(t.Context(), "octocat/hello-world", 1, &ghclient.PullRequestInfo{HeadSHA: "abc123", State: "open"})
	configs := []ghclient.DiscoveredConfig{{Config: &ghclient.SchemabotConfig{Database: "orders", Type: "mysql"}}}
	h.clearAggregateBlocksForVerifiedPR(ctx, ghclient.NewInstallationClient(client, logger), "octocat/hello-world", 1, "abc123", configs)

	var entries []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry), line)
		entries = append(entries, entry)
	}
	return prReads.Load(), entries
}

func TestClearAggregateBlocksReadsHeadAndWarnsNothingOnListenOnlyRepository(t *testing.T) {
	reads, entries := listenOnlyAggregateClear(t, http.StatusOK)

	assert.Equal(t, int32(1), reads, "the PR head is read once from GitHub, past the warm request cache, as a serving deployment reads it")
	for _, entry := range entries {
		assert.NotContains(t, []any{"WARN", "ERROR"}, entry["level"], "nothing blocks a listen-only repository's merge: %v", entry["msg"])
	}
}

func TestClearAggregateBlocksNamesTheMeasurementWhenListenOnlyHeadReadFails(t *testing.T) {
	reads, entries := listenOnlyAggregateClear(t, http.StatusNotFound)

	assert.Equal(t, int32(1), reads)
	var warned bool
	for _, entry := range entries {
		msg, _ := entry["msg"].(string)
		assert.NotEqual(t, "ERROR", entry["level"], "a failed listen-only read is not a failed check write: %s", msg)
		assert.NotContains(t, msg, "status check", "a listen-only repository has no status check to update")
		if entry["level"] == "WARN" && strings.HasPrefix(msg, "listen-only PR head read failed") {
			warned = true
			assert.Equal(t, true, entry["listen_only"])
		}
	}
	assert.True(t, warned, "the failed read logs a warning that names the measurement")
}

// refusingTransport answers every request with err, the way the GitHub
// client's listen-only guard refuses a write without sending it.
type refusingTransport struct{ err error }

func (t refusingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, t.err
}

// A best-effort PR comment that the listen-only guard refuses is the guard
// working as designed, so it is logged at info, like SchemaBot's other
// listen-only decisions. Any other failure to post is
// still logged as an error, so an operator sees comments that should have
// landed and did not.
func TestPostCommentLogsListenOnlyRefusalAtInfo(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		wantMsg   string
		wantLevel string
	}{
		{
			name:      "listen-only refusal",
			err:       fmt.Errorf("POST create_issue_comment for octocat/hello-world: %w", ghclient.ErrListenOnlyRepository),
			wantMsg:   "comment not posted: repository is listen-only",
			wantLevel: "INFO",
		},
		{
			name:      "GitHub unreachable",
			err:       fmt.Errorf("dial tcp: connection refused"),
			wantMsg:   "failed to post comment",
			wantLevel: "ERROR",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := gh.NewClient(&http.Client{Transport: refusingTransport{err: tc.err}})
			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			h := &Handler{
				ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, logger)}),
				logger:    logger,
			}

			h.postComment("octocat/hello-world", 42, 1, "body")

			var entries []map[string]any
			for line := range strings.SplitSeq(strings.TrimSpace(logs.String()), "\n") {
				var entry map[string]any
				require.NoError(t, json.Unmarshal([]byte(line), &entry), line)
				entries = append(entries, entry)
			}
			var matched []map[string]any
			for _, entry := range entries {
				if entry["msg"] == tc.wantMsg {
					matched = append(matched, entry)
				}
				if tc.wantLevel == "INFO" {
					assert.NotEqual(t, "ERROR", entry["level"], "no error log for a refused write: %v", entry)
				}
			}
			require.Len(t, matched, 1, "logs:\n%s", logs.String())
			assert.Equal(t, tc.wantLevel, matched[0]["level"])
			assert.Equal(t, "octocat/hello-world", matched[0]["repo"])
			assert.Equal(t, float64(42), matched[0]["pr"])
		})
	}
}
