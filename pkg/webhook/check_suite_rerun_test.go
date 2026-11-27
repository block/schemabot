package webhook

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/storage"
)

// durableCheckSuiteRerunEvent is a claimed Re-run all checks row as the
// ingress path stores it.
func durableCheckSuiteRerunEvent(t *testing.T, event *storage.WebhookEvent) *storage.WebhookEvent {
	t.Helper()
	var payload map[string]any
	require.NoError(t, json.Unmarshal(event.Payload, &payload))
	payload["action"] = checkSuiteRerunAction
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	event.Action = checkSuiteRerunAction
	event.Payload = body
	return event
}

// serveRerunPR answers the live PR read a re-run makes and counts the file
// listings that start config discovery, which only a re-plan reaches.
func serveRerunPR(t *testing.T, mux *http.ServeMux, number int, state, headSHA string) *atomic.Int64 {
	t.Helper()
	var discovery atomic.Int64
	prPath := "/repos/octocat/hello-world/pulls/" + strconv.Itoa(number)
	mux.HandleFunc("GET "+prPath, func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"number": number,
			"state":  state,
			"head":   map[string]any{"sha": headSHA, "ref": "feature"},
			"base":   map[string]any{"sha": "base-sha", "ref": "main", "repo": map[string]any{"default_branch": "main"}},
			"user":   map[string]any{"login": "octocat"},
		}))
	})
	mux.HandleFunc("GET "+prPath+"/files", func(w http.ResponseWriter, _ *http.Request) {
		discovery.Add(1)
		require.NoError(t, json.NewEncoder(w).Encode([]map[string]any{
			{"filename": "README.md", "status": "modified", "additions": 1, "deletions": 1},
		}))
	})
	return &discovery
}

// serveChangedFileModes answers the reads that find a PR's changed file
// modes: the base branch tip, the merge base, and the compare diff, which
// shows README.md as a regular file.
func serveChangedFileModes(t *testing.T, mux *http.ServeMux) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/git/ref/heads/main", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"object": map[string]string{"sha": "base-sha"}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/{range}", func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.Header.Get("Accept"), "diff") {
			writeRegularFilesDiff(t, w, []string{"README.md"})
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"merge_base_commit": map[string]string{"sha": "merge-base"}}))
	})
}

// An operator presses Re-run all checks. The delivery is queued at once, not
// held back like recovery, and it is queued even with the recovery kill
// switch off: it is a direct request that nothing else will answer.
func TestCheckSuiteRerunWebhookQueuesImmediately(t *testing.T) {
	store := newRecordingWebhookEventStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: store}, &api.ServerConfig{Repos: map[string]api.RepoConfig{"octocat/hello-world": {}}}, nil, logger)
	h := NewHandler(service, &fakeClientFactory{}, nil, logger, WithDurableWebhookDispatch(), WithInsecureSkipSignatureVerification())

	req := buildCheckSuiteWebhookRequest(t, checkSuiteRerunAction, "suite-sha", "feature", 7)
	req.Header.Set(headerDeliveryID, "delivery-rerun-1")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `{"message":"check_suite re-run queued"}`, rr.Body.String())
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, "delivery-rerun-1")
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, "check_suite", row.Event)
	require.Equal(t, checkSuiteRerunAction, row.Action)
	require.Equal(t, "suite-sha", row.HeadSHA)
	require.Nil(t, row.RetryAfter, "a re-run is claimable at once")
}

// Re-run all checks on a same-repository commit with no open PR has nothing
// to plan, so it is acknowledged without an inbox row.
func TestCheckSuiteRerunWebhookIgnoresHeadWithoutOpenPR(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h := newCheckSuiteIngressHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})

	req := buildCheckSuiteWebhookRequest(t, checkSuiteRerunAction, "suite-sha", "main")
	req.Header.Set(headerDeliveryID, "delivery-rerun-1")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `{"message":"check_suite re-run ignored without an open PR"}`, rr.Body.String())
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, "delivery-rerun-1")
	require.NoError(t, err)
	require.Nil(t, row)
}

// Re-run all checks on a commit three PRs point at. The open PR still at
// that commit is planned again. The closed PR is not: it is read-only
// history whose check state close cleanup removed. The PR that moved to a
// newer commit is not either, since its newer push planned it already.
func TestDurableCheckSuiteRerunReplansOnlyOpenPRsAtTheHead(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h, mux := newCheckSuiteProcessHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})
	serveChangedFileModes(t, mux)
	open := serveRerunPR(t, mux, 7, "open", "suite-sha")
	closed := serveRerunPR(t, mux, 8, "closed", "suite-sha")
	moved := serveRerunPR(t, mux, 9, "open", "newer-sha")

	retry, err := h.processDurableCheckSuite(t.Context(), durableCheckSuiteRerunEvent(t, durableCheckSuiteEvent(t, "suite-sha", 7, 8, 9)))

	require.NoError(t, err)
	require.False(t, retry)
	require.Equal(t, int64(1), open.Load(), "the open PR at the suite head is planned again")
	require.Equal(t, int64(0), closed.Load(), "a closed PR is not planned again")
	require.Equal(t, int64(0), moved.Load(), "a PR whose head moved is not planned at the old head")
}

// A fork head cannot name its PRs in the payload, so the re-run finds them
// by walking the open PRs for the suite head.
func TestDurableCheckSuiteRerunResolvesForkPRs(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h, mux := newCheckSuiteProcessHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls", func(w http.ResponseWriter, _ *http.Request) {
		writeOpenPRs(t, w,
			openPR(7, "suite-sha", time.Now().Add(-time.Hour)),
			openPR(8, "other-sha", time.Now().Add(-time.Hour)),
		)
	})
	serveChangedFileModes(t, mux)
	fork := serveRerunPR(t, mux, 7, "open", "suite-sha")

	retry, err := h.processDurableCheckSuite(t.Context(), durableCheckSuiteRerunEvent(t, durableCheckSuiteForkEvent(t, "suite-sha")))

	require.NoError(t, err)
	require.False(t, retry)
	require.Equal(t, int64(1), fork.Load(), "the fork PR at the suite head is planned again")
}

// A GitHub failure reading the PR is uncertainty, so the re-run stays
// retryable instead of completing without a plan.
func TestDurableCheckSuiteRerunRetriesGitHubFailure(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h, mux := newCheckSuiteProcessHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/7", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})

	retry, err := h.processDurableCheckSuite(t.Context(), durableCheckSuiteRerunEvent(t, durableCheckSuiteEvent(t, "suite-sha", 7)))

	require.Error(t, err)
	require.Contains(t, err.Error(), "verify current head for durable check_suite rerequest")
	require.True(t, retry)
}

// The repository can be removed from config between enqueue and claim, and a
// re-run must not plan for it then.
func TestDurableCheckSuiteRerunRevalidatesAllowlist(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h, mux := newCheckSuiteProcessHandler(t, store, map[string]api.RepoConfig{"octocat/other": {}})
	discovery := serveRerunPR(t, mux, 7, "open", "suite-sha")

	retry, err := h.processDurableCheckSuite(t.Context(), durableCheckSuiteRerunEvent(t, durableCheckSuiteEvent(t, "suite-sha", 7)))

	require.NoError(t, err)
	require.False(t, retry)
	require.Equal(t, int64(0), discovery.Load())
}
