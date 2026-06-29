package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
)

// buildMergeGroupWebhookRequest constructs a merge_group webhook POST request.
func buildMergeGroupWebhookRequest(t *testing.T, action, headSHA string, secret []byte) *http.Request {
	t.Helper()
	if action == "" {
		action = "checks_requested"
	}
	if headSHA == "" {
		headSHA = "mergesha123"
	}

	payload := map[string]any{
		"action": action,
		"merge_group": map[string]any{
			"head_sha": headSHA,
		},
		"repository": map[string]any{
			"full_name": "octocat/hello-world",
		},
		"installation": map[string]any{
			"id": 12345,
		},
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-GitHub-Event", "merge_group")

	if len(secret) > 0 {
		mac := hmac.New(sha256.New, secret)
		mac.Write(body)
		req.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}

	return req
}

type createdCheckRun struct {
	Name       string `json:"name"`
	HeadSHA    string `json:"head_sha"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
}

func mergeGroupTestHandler(t *testing.T, allowedEnvironments []string, repos map[string]api.RepoConfig) (*Handler, chan createdCheckRun) {
	t.Helper()
	client, mux := setupGitHubServer(t)
	created := make(chan createdCheckRun, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var c createdCheckRun
		require.NoError(t, json.NewDecoder(r.Body).Decode(&c))
		created <- c
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 555})
	})

	installClient := ghclient.NewInstallationClient(client, testLogger())
	service := api.New(nil, &api.ServerConfig{
		AllowedEnvironments: allowedEnvironments,
		Repos:               repos,
	}, nil, testLogger())

	h := &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
		logger:    testLogger(),
	}
	return h, created
}

// A merge queue evaluates the same required checks against the queue's
// synthetic merge-group head commit, not the PR head. Because SchemaBot applies
// and gates schema changes before a PR can enter the queue, it posts a passing
// aggregate check on the merge-group head SHA — one per gated environment, with
// the same names as the PR-head aggregates — so a required SchemaBot check does
// not block the merge queue forever.
func TestWebhookMergeGroupPostsPassingChecks(t *testing.T) {
	h, created := mergeGroupTestHandler(t,
		[]string{"staging", "production"},
		map[string]api.RepoConfig{"octocat/hello-world": {}},
	)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildMergeGroupWebhookRequest(t, "checks_requested", "mergesha123", nil))

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "merge_group checks posted")

	got := map[string]createdCheckRun{}
	for range []int{0, 1} {
		select {
		case c := <-created:
			got[c.Name] = c
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for merge_group check run")
		}
	}

	require.Len(t, got, 2)
	for _, name := range []string{"SchemaBot (staging)", "SchemaBot (production)"} {
		c, ok := got[name]
		require.True(t, ok, "expected a check run named %q", name)
		assert.Equal(t, "mergesha123", c.HeadSHA)
		assert.Equal(t, "completed", c.Status)
		assert.Equal(t, "success", c.Conclusion)
	}
}

// GitHub fires merge_group with "destroyed" when a PR leaves the queue. That
// action needs no check run on any commit, so SchemaBot ignores it.
func TestWebhookMergeGroupIgnoresNonChecksRequested(t *testing.T) {
	h, created := mergeGroupTestHandler(t, nil, map[string]api.RepoConfig{"octocat/hello-world": {}})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildMergeGroupWebhookRequest(t, "destroyed", "mergesha123", nil))

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "merge_group action ignored")

	select {
	case c := <-created:
		t.Fatalf("unexpected check run created for destroyed action: %q", c.Name)
	case <-time.After(100 * time.Millisecond):
	}
}

// A merge_group event for a repository SchemaBot does not manage gets no check:
// SchemaBot's check is not required on that repo, so there is nothing to unblock.
func TestWebhookMergeGroupRejectsUnregisteredRepo(t *testing.T) {
	h, created := mergeGroupTestHandler(t, nil, map[string]api.RepoConfig{"org/allowed-repo": {}})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildMergeGroupWebhookRequest(t, "checks_requested", "mergesha123", nil))

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "repository not registered")

	select {
	case c := <-created:
		t.Fatalf("unexpected check run created for unregistered repo: %q", c.Name)
	case <-time.After(100 * time.Millisecond):
	}
}

// A webhook redelivery for the same merge group must not create a duplicate
// Check Run. When SchemaBot's App slug is known it finds the run it already
// published on the merge-group head SHA and updates it in place.
func TestWebhookMergeGroupUpdatesExistingCheck(t *testing.T) {
	client, mux := setupGitHubServer(t)
	updated := make(chan int64, 4)
	created := make(chan string, 4)
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/mergesha123/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"total_count": 1,
			"check_runs": []map[string]any{
				{"id": 7, "name": "SchemaBot (production)", "status": "completed", "conclusion": "success", "app": map[string]any{"slug": "schemabot"}},
			},
		})
	})
	mux.HandleFunc("PATCH /repos/octocat/hello-world/check-runs/7", func(w http.ResponseWriter, _ *http.Request) {
		updated <- 7
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 7})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var c createdCheckRun
		require.NoError(t, json.NewDecoder(r.Body).Decode(&c))
		created <- c.Name
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 555})
	})

	installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")
	service := api.New(nil, &api.ServerConfig{
		AllowedEnvironments: []string{"production"},
		Repos:               map[string]api.RepoConfig{"octocat/hello-world": {}},
	}, nil, testLogger())
	h := &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
		logger:    testLogger(),
	}

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildMergeGroupWebhookRequest(t, "checks_requested", "mergesha123", nil))
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case id := <-updated:
		assert.Equal(t, int64(7), id)
	case <-time.After(2 * time.Second):
		t.Fatal("expected the existing check run to be updated")
	}

	select {
	case name := <-created:
		t.Fatalf("expected an update, not a new check run: %q", name)
	case <-time.After(100 * time.Millisecond):
	}
}

// With no environment scoping configured, SchemaBot publishes a single
// non-environment-scoped aggregate check on the merge-group head SHA.
func TestWebhookMergeGroupSingleAggregateWhenNoEnvScoping(t *testing.T) {
	h, created := mergeGroupTestHandler(t, nil, map[string]api.RepoConfig{"octocat/hello-world": {}})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildMergeGroupWebhookRequest(t, "checks_requested", "mergesha123", nil))

	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case c := <-created:
		assert.Equal(t, "SchemaBot", c.Name)
		assert.Equal(t, "mergesha123", c.HeadSHA)
		assert.Equal(t, "success", c.Conclusion)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for merge_group check run")
	}

	select {
	case c := <-created:
		t.Fatalf("expected exactly one aggregate check, got extra: %q", c.Name)
	case <-time.After(100 * time.Millisecond):
	}
}
