package webhook

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
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
		t.Fatalf("unexpected GitHub request for a command on a listen-only repository: %s", req)
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
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")}),
		logger:    testLogger(),
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
