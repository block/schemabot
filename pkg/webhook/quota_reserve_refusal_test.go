package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// A discovery the quota reserve refused fails the aggregate closed with its
// own block reason. The check names the reset time in UTC and what happens
// then: SchemaBot retries when the delivery that ran the discovery is retried
// at the reset, and otherwise the author re-runs the check. The installation's
// remaining quota and reserve stay out of the check.
func TestConfigDiscoveryRefusedByQuotaReserveNamesTheReset(t *testing.T) {
	resetAt := time.Date(2026, 11, 22, 15, 4, 30, 0, time.FixedZone("AEDT", 11*60*60))
	for _, tc := range []struct {
		name           string
		retriedAtReset bool
		summary        string
	}{
		{
			name:           "retried at the reset",
			retriedAtReset: true,
			summary:        "SchemaBot is holding back its remaining GitHub API quota for check runs, so it did not read this PR's schema files. It will retry when the quota resets at 04:05 UTC.",
		},
		{
			name:           "not retried",
			retriedAtReset: false,
			summary:        "SchemaBot is holding back its remaining GitHub API quota for check runs, so it did not read this PR's schema files. Re-run this check after the quota resets at 04:05 UTC.",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &foldCheckStore{}
			h, mux, client := newFoldHandler(t, nonAggregateConfig(), store)
			serveHeadSHA(t, mux, "abc123")
			type checkRun struct {
				Conclusion string `json:"conclusion"`
				Output     struct {
					Summary string `json:"summary"`
				} `json:"output"`
			}
			checkRuns := make(chan checkRun, 1)
			mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
				var posted checkRun
				assert.NoError(t, json.NewDecoder(r.Body).Decode(&posted))
				checkRuns <- posted
				w.WriteHeader(http.StatusCreated)
				_, _ = fmt.Fprint(w, `{"id":39}`)
			})

			h.postConfigDiscoveryFailure(t.Context(), client, "octocat/hello-world", 1, "abc123", quotaReserveRefusalError(resetAt), tc.retriedAtReset)

			require.Len(t, checkRuns, 1)
			posted := <-checkRuns
			assert.Equal(t, checkConclusionFailure, posted.Conclusion)
			assert.Equal(t, tc.summary, posted.Output.Summary)
			assert.NotContains(t, posted.Output.Summary, "requests remain")
			assert.NotContains(t, posted.Output.Summary, "reserve of")
			require.Len(t, store.upserted, 1)
			assert.Equal(t, githubQuotaReservedBlock.blockingReason, store.upserted[0].BlockingReason)
			assert.Equal(t, tc.summary, store.upserted[0].ErrorMessage)
		})
	}
}

// The durable driver marks a claim whose quota reserve refusal it would
// reschedule for the reset, so the discovery inside it can say SchemaBot
// retries. A claim at its last attempt past the uncharged window ends instead,
// so it is not marked, and neither is work outside the durable driver.
func TestDurableDriverMarksClaimsItRetriesAtTheQuotaReset(t *testing.T) {
	for _, tc := range []struct {
		name     string
		attempts int
		received time.Duration
		want     bool
	}{
		{name: "first claim", attempts: 0, received: 0, want: true},
		{name: "last attempt inside the uncharged window", attempts: maxDurableWebhookAttempts - 1, received: 0, want: true},
		{name: "attempts left past the uncharged window", attempts: 0, received: quotaReserveUnchargedWindow + time.Minute, want: true},
		{name: "last attempt past the uncharged window", attempts: maxDurableWebhookAttempts - 1, received: quotaReserveUnchargedWindow + time.Minute, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := durablePullRequestEvent(t)
			event.Attempts = tc.attempts
			event.ReceivedAt = time.Now().Add(-tc.received)
			store := newScriptedWebhookEventStore(event)
			h := newDurableDriverHandler(t, store, nil, nil)
			var marked atomic.Bool
			h.durableWebhookProcessOverride = func(ctx context.Context, _ *storage.WebhookEvent) (bool, error) {
				marked.Store(durableRetryAtQuotaReset(ctx))
				return false, nil
			}

			require.True(t, h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0"))

			assert.Equal(t, tc.want, marked.Load())
		})
	}
	assert.False(t, durableRetryAtQuotaReset(t.Context()), "work outside the durable driver is never retried at the reset")
}

// A stored aggregate blocked by the quota reserve keeps blocking: a fold over
// per-database rows that would pass on their own publishes nothing over it,
// so only the auto-plan that re-verifies the PR after the reset lifts it.
func TestAggregateFoldKeepsQuotaReserveBlock(t *testing.T) {
	block := githubQuotaReservedBlockFor(time.Now().Add(30*time.Minute), true)
	store := &foldCheckStore{
		get: &storage.Check{
			Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: "abc123",
			Environment: aggregateSentinel, DatabaseType: aggregateSentinel, DatabaseName: aggregateSentinel,
			CheckRunID: 200, Status: checkStatusCompleted, Conclusion: checkConclusionFailure,
			BlockingReason: block.blockingReason, ErrorMessage: block.message,
		},
		byPR: []*storage.Check{{
			Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: "abc123",
			Environment: "staging", DatabaseType: "mysql", DatabaseName: "orders",
			Status: checkStatusCompleted, Conclusion: checkConclusionSuccess,
		}},
	}
	h, mux, client := newFoldHandler(t, nonAggregateConfig(), store)
	serveHeadSHA(t, mux, "abc123")
	var checkRunWrites atomic.Int64
	mux.HandleFunc("/repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		checkRunWrites.Add(1)
		w.WriteHeader(http.StatusCreated)
	})
	mux.HandleFunc("/repos/octocat/hello-world/check-runs/{id}", func(w http.ResponseWriter, _ *http.Request) {
		checkRunWrites.Add(1)
		w.WriteHeader(http.StatusOK)
	})

	_, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")

	require.NoError(t, err)
	assert.Zero(t, checkRunWrites.Load(), "no Check Run is published over the quota reserve block")
	assert.Empty(t, store.upserted, "the stored block is not replaced")
}

// newQuotaReserveReviewGateHandler is setupReviewGateHandler with GitHub
// reached through a transport that refuses schema file, blob and tree reads
// the way the quota reserve does.
func newQuotaReserveReviewGateHandler(t *testing.T, config *api.ServerConfig) (*Handler, *http.ServeMux, *refuseSchemaReadsOutsidePublication) {
	t.Helper()
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	reserve := &refuseSchemaReadsOutsidePublication{}
	client := gh.NewClient(&http.Client{Transport: reserve})
	var err error
	client.BaseURL, err = url.Parse(server.URL + "/")
	require.NoError(t, err)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := api.New(&emptyStorage{}, config, nil, logger)
	h := NewHandler(svc, &fakeClientFactory{client: ghclient.NewInstallationClient(client, logger)}, nil, logger)
	return h, mux, reserve
}

// An approval on an earlier commit whose comparison with the head needs reads
// the quota reserve refused is not dropped: the gate returns an evaluation
// error the durable command delivery retries, not a block that asks for an
// approval of the latest commit. After the reset the comparison can still
// prove the approval covers the head.
func TestCheckReviewGateRetriesEarlierApprovalRefusedByQuotaReserve(t *testing.T) {
	h, mux, reserve := newQuotaReserveReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob"}
		cfg.Databases["orders"] = db
	}))
	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: time.Now()}, CommitID: new(reviewGateApproved)},
	})
	files := map[string]string{"schema/testdb/orders/orders.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"}
	registerReviewGateBaseBranch(t, mux, map[string]string{reviewGateApproved: reviewGateOldBase, reviewGateTestHeadSHA: reviewGateOldBase})
	registerReviewGateGitObjects(t, mux, map[string]map[string]string{reviewGateOldBase: files, reviewGateApproved: files, reviewGateTestHeadSHA: files})
	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, "staging", reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))

	require.ErrorIs(t, err, ghclient.ErrGitHubQuotaReserved)
	require.NotErrorIs(t, err, errApprovalNotComparable)
	assert.Nil(t, result, "no block is returned, so no comment asks for an approval of the latest commit")
	assert.Positive(t, reserve.refused.Load(), "the comparison reached a read the quota reserve refused")
}

// A command whose retries run out while the quota reserve refuses its reads
// gets a fixed answer naming the reset time. The refusal's own text, with the
// installation's remaining quota and reserve, stays in the delivery's stored
// error and the server log.
func TestDurableWebhookDriverAnswersQuotaReserveRefusalAtAttemptCapWithFixedLine(t *testing.T) {
	event := durableIssueCommentEvent("schemabot apply -e production")
	event.Attempts = maxDurableWebhookAttempts - 1
	event.ReceivedAt = time.Now().Add(-quotaReserveUnchargedWindow - time.Minute)
	store := newScriptedWebhookEventStore(event)
	client, mux := setupGitHubServer(t)
	comments := make(chan string, 1)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/7/comments", commentRecorder(t, comments))
	h := newDurableDriverHandler(t, store, nil, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())})
	resetAt := time.Date(2026, 11, 22, 15, 4, 30, 0, time.UTC)
	h.durableWebhookProcessOverride = func(context.Context, *storage.WebhookEvent) (bool, error) {
		return true, quotaReserveRefusalError(resetAt)
	}

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	failure := <-store.failed
	require.Nil(t, failure.retryAfter, "the delivery is terminal")
	assert.Contains(t, failure.errMsg, "40 requests remain against a reserve of 100")
	body := requireComment(t, comments, "terminal command answer")
	assert.Contains(t, body, "SchemaBot could not complete this command because it is holding back its remaining GitHub API quota for check runs. Comment the command again after the quota resets at 15:05 UTC.")
	assert.NotContains(t, body, "40 requests")
	assert.NotContains(t, body, "requests remain")
	assert.NotContains(t, body, "reserve of 100")
	assert.NotContains(t, body, "fetch_file_content")
}
