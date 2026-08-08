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
	"github.com/block/schemabot/pkg/storage"
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
			"head_ref": "refs/heads/gh-readonly-queue/main/pr-1-" + headSHA,
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

func mergeGroupTestHandler(t *testing.T, allowedEnvironments []string, repos map[string]api.RepoConfig, storedChecks ...*storage.Check) (*Handler, chan checkRunCapture, chan string) {
	t.Helper()
	client, mux := setupGitHubServer(t)
	created := make(chan checkRunCapture, 10)
	comments := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var c checkRunCapture
		require.NoError(t, json.NewDecoder(r.Body).Decode(&c))
		created <- c
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 555})
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var c struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&c))
		comments <- c.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 777})
	})

	installClient := ghclient.NewInstallationClient(client, testLogger())
	service := api.New(&foldStorage{checks: &foldCheckStore{byPR: storedChecks}}, &api.ServerConfig{
		AllowedEnvironments: allowedEnvironments,
		Repos:               repos,
	}, nil, testLogger())

	h := &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
		logger:    testLogger(),
	}
	return h, created, comments
}

// A merge queue evaluates the same required checks against the queue's
// synthetic merge-group head commit, not the PR head. Because SchemaBot applies
// and gates schema changes before a PR can enter the queue, it posts a passing
// aggregate check on the merge-group head SHA — one per gated environment, with
// the same names as the PR-head aggregates — so a required SchemaBot check does
// not block the merge queue forever.
func TestWebhookMergeGroupPostsPassingChecks(t *testing.T) {
	h, created, _ := mergeGroupTestHandler(t,
		[]string{"staging", "production"},
		map[string]api.RepoConfig{"octocat/hello-world": {}},
	)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildMergeGroupWebhookRequest(t, "checks_requested", "mergesha123", nil))

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "merge_group checks posted")

	got := map[string]checkRunCapture{}
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
	h, created, _ := mergeGroupTestHandler(t, nil, map[string]api.RepoConfig{"octocat/hello-world": {}})

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
	h, created, _ := mergeGroupTestHandler(t, nil, map[string]api.RepoConfig{"org/allowed-repo": {}})

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
		var c checkRunCapture
		require.NoError(t, json.NewDecoder(r.Body).Decode(&c))
		created <- c.Name
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 555})
	})

	installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")
	service := api.New(&foldStorage{checks: &foldCheckStore{}}, &api.ServerConfig{
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
	h, created, _ := mergeGroupTestHandler(t, nil, map[string]api.RepoConfig{"octocat/hello-world": {}})

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

// An aggregate participant's checks are never required, so it posts nothing on
// a merge-group commit — the leader publishes the required aggregate there.
// Without this silence, every queue entry would re-grow the per-tenant check
// rows the aggregate removes from PR heads.
func TestWebhookMergeGroupParticipantStaysSilent(t *testing.T) {
	h, created, _ := mergeGroupTestHandler(t,
		[]string{"production"},
		map[string]api.RepoConfig{"octocat/hello-world": {
			Aggregate: &api.AggregateConfig{Role: api.AggregateRoleParticipant},
		}},
	)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildMergeGroupWebhookRequest(t, "checks_requested", "mergesha123", nil))

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "aggregate participant, staying silent")

	select {
	case c := <-created:
		t.Fatalf("participant posted a merge_group check run: %q", c.Name)
	case <-time.After(100 * time.Millisecond):
	}
}

// The aggregate leader keeps posting its required checks on merge-group
// commits — silence is participant-only, so the queue never wedges.
func TestWebhookMergeGroupLeaderStillPosts(t *testing.T) {
	h, created, _ := mergeGroupTestHandler(t,
		[]string{"production"},
		map[string]api.RepoConfig{"octocat/hello-world": {
			Aggregate: &api.AggregateConfig{
				Role:            api.AggregateRoleLeader,
				ExpectedTenants: []api.ExpectedTenant{{Tenant: "tenant-b", Paths: []string{"tenant-b/schema"}, CheckName: "SchemaBot Tenant B"}},
			},
		}},
	)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildMergeGroupWebhookRequest(t, "checks_requested", "mergesha123", nil))

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "merge_group checks posted")

	select {
	case c := <-created:
		assert.Equal(t, "SchemaBot (production)", c.Name)
		assert.Equal(t, "success", c.Conclusion)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the leader's merge_group check run")
	}
}

// A queued PR whose stored check state turned blocking after it entered the
// queue — a preflight hold from a sibling change's in-flight apply on a shared
// target — must not merge on the verdict it queued with. The admission check
// re-folds the PR's stored state on the merge-group commit and blocks, so the
// queue removes the PR instead of merging it mid-apply. Because the blocking
// run lives on the synthetic merge-group commit and the queue never re-adds a
// PR by itself, the block also posts a guidance comment on the PR naming the
// blocked database and the re-queue step.
func TestWebhookMergeGroupBlocksWhenStoredCheckHolds(t *testing.T) {
	h, created, comments := mergeGroupTestHandler(t,
		[]string{"production"},
		map[string]api.RepoConfig{"octocat/hello-world": {}},
		&storage.Check{
			Repository:   "octocat/hello-world",
			PullRequest:  1,
			HeadSHA:      "prhead123",
			Environment:  "production",
			DatabaseType: "mysql",
			DatabaseName: "widgets",
			Status:       "completed",
			Conclusion:   "action_required",
		},
	)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildMergeGroupWebhookRequest(t, "checks_requested", "mergesha123", nil))
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case c := <-created:
		assert.Equal(t, "SchemaBot (production)", c.Name)
		assert.Equal(t, "mergesha123", c.HeadSHA)
		assert.Equal(t, "completed", c.Status)
		assert.Equal(t, "action_required", c.Conclusion)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the blocking merge_group check run")
	}

	select {
	case body := <-comments:
		assert.Contains(t, body, "Removed From Merge Queue")
		assert.Contains(t, body, "`widgets` in `production`")
		assert.Contains(t, body, "add it to the merge queue again")
		assert.Contains(t, body, mergeQueueEjectedCommentMarker("mergesha123"))
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the ejection guidance comment")
	}
}

// A redelivered merge_group event finds the ejection comment it already posted
// (identified by the merge-group head SHA marker) and does not post a
// duplicate; the blocking check still reconciles idempotently.
func TestWebhookMergeGroupEjectionCommentDeduplicatesRedelivery(t *testing.T) {
	client, mux := setupGitHubServer(t)
	created := make(chan checkRunCapture, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var c checkRunCapture
		require.NoError(t, json.NewDecoder(r.Body).Decode(&c))
		created <- c
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 555})
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"id": 777, "body": "guidance\n" + mergeQueueEjectedCommentMarker("mergesha123")},
		})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, _ *http.Request) {
		t.Error("redelivery must not post a duplicate ejection comment")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 778})
	})

	installClient := ghclient.NewInstallationClient(client, testLogger())
	service := api.New(&foldStorage{checks: &foldCheckStore{byPR: []*storage.Check{{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "prhead123",
		Environment:  "production",
		DatabaseType: "mysql",
		DatabaseName: "widgets",
		Status:       "completed",
		Conclusion:   "action_required",
	}}}}, &api.ServerConfig{
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
	case c := <-created:
		assert.Equal(t, "action_required", c.Conclusion)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the blocking merge_group check run")
	}
}

// A queued PR with an apply currently running from its own head must not merge
// until the apply settles: an in-progress apply-owned stored row blocks the
// admission fold the same way it blocks the PR-head aggregate.
func TestWebhookMergeGroupBlocksWhenOwnApplyInFlight(t *testing.T) {
	h, created, _ := mergeGroupTestHandler(t,
		[]string{"production"},
		map[string]api.RepoConfig{"octocat/hello-world": {}},
		&storage.Check{
			Repository:   "octocat/hello-world",
			PullRequest:  1,
			HeadSHA:      "prhead123",
			Environment:  "production",
			DatabaseType: "mysql",
			DatabaseName: "widgets",
			Status:       "in_progress",
			ApplyID:      42,
		},
	)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildMergeGroupWebhookRequest(t, "checks_requested", "mergesha123", nil))
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case c := <-created:
		assert.Equal(t, "action_required", c.Conclusion)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the blocking merge_group check run")
	}
}

// A merge group whose ref does not name a pull request cannot have its stored
// check state consulted, so admission fails closed: the check posts blocking
// rather than guessing that nothing blocks.
func TestWebhookMergeGroupFailsClosedOnUnidentifiablePR(t *testing.T) {
	h, created, _ := mergeGroupTestHandler(t,
		[]string{"production"},
		map[string]api.RepoConfig{"octocat/hello-world": {}},
	)

	payload := map[string]any{
		"action": "checks_requested",
		"merge_group": map[string]any{
			"head_sha": "mergesha123",
			"head_ref": "refs/heads/some-unexpected-ref",
		},
		"repository":   map[string]any{"full_name": "octocat/hello-world"},
		"installation": map[string]any{"id": 12345},
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-GitHub-Event", "merge_group")

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case c := <-created:
		assert.Equal(t, "action_required", c.Conclusion)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the fail-closed merge_group check run")
	}
}

// A storage read failure during admission is uncertainty, and uncertainty
// never admits a merge: the handler returns 500 so the delivery is retried
// rather than posting a check computed from unknown state.
func TestWebhookMergeGroupFailsClosedOnStorageError(t *testing.T) {
	client, _ := setupGitHubServer(t)
	installClient := ghclient.NewInstallationClient(client, testLogger())
	service := api.New(&foldStorage{checks: &foldCheckStore{byPRErr: assert.AnError}}, &api.ServerConfig{
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
	require.Equal(t, http.StatusInternalServerError, rr.Code)
}

// mergeGroupPRNumber must identify the queued PR from the merge-queue branch
// ref GitHub generates, including base branches containing slashes, and refuse
// refs that do not name a PR.
func TestMergeGroupPRNumber(t *testing.T) {
	pr, err := mergeGroupPRNumber("refs/heads/gh-readonly-queue/main/pr-123-0123abc")
	require.NoError(t, err)
	assert.Equal(t, 123, pr)

	pr, err = mergeGroupPRNumber("refs/heads/gh-readonly-queue/release/v1/pr-7-deadbeef")
	require.NoError(t, err)
	assert.Equal(t, 7, pr)

	_, err = mergeGroupPRNumber("refs/heads/feature-branch")
	require.Error(t, err)

	_, err = mergeGroupPRNumber("")
	require.Error(t, err)
}
