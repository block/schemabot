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

// A push to the default branch of a listen-only repository resolves which
// checks it would post, as for any repository, but posts none of them.
func TestWebhookPushPostsNoChecksOnListenOnlyRepository(t *testing.T) {
	h, created := mergeGroupTestHandler(t,
		[]string{"staging", "production"},
		map[string]api.RepoConfig{"octocat/hello-world": {ListenOnly: true}},
	)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildPushWebhookRequest(t, "refs/heads/main", "pushsha123", false))

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Empty(t, created, "no check run should be created on a listen-only repository")
}

// A merge queue run in a listen-only repository gets no passing check from
// SchemaBot.
func TestWebhookMergeGroupPostsNoChecksOnListenOnlyRepository(t *testing.T) {
	h, created := mergeGroupTestHandler(t,
		[]string{"staging", "production"},
		map[string]api.RepoConfig{"octocat/hello-world": {ListenOnly: true}},
	)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildMergeGroupWebhookRequest(t, "checks_requested", "mergesha123", nil))

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Empty(t, created, "no check run should be created on a listen-only repository")
}
