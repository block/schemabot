package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

func mergeGroupTestHandler(t *testing.T, allowedEnvironments []string, repos map[string]api.RepoConfig) (*Handler, chan checkRunCapture) {
	t.Helper()
	h, created, _ := mergeGroupTestHandlerWithMux(t, allowedEnvironments, repos)
	return h, created
}

// mergeGroupTestHandlerWithMux is mergeGroupTestHandler with the fake GitHub
// server's mux returned, so a test can serve the merge group's existing Check
// Runs. The client knows its App slug, as a production client does, so a
// check-run lookup can verify which App owns a run.
func mergeGroupTestHandlerWithMux(t *testing.T, allowedEnvironments []string, repos map[string]api.RepoConfig) (*Handler, chan checkRunCapture, *http.ServeMux) {
	t.Helper()
	client, mux := setupGitHubServer(t)
	created := make(chan checkRunCapture, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var c checkRunCapture
		require.NoError(t, json.NewDecoder(r.Body).Decode(&c))
		created <- c
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 555})
	})

	installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")
	service := api.New(nil, &api.ServerConfig{
		AllowedEnvironments: allowedEnvironments,
		Repos:               repos,
	}, nil, testLogger())

	h := &Handler{
		insecureSkipSignatureVerification: true,
		service:                           service,
		ghClients:                         ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
		logger:                            testLogger(),
	}
	return h, created, mux
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

	got := map[string]checkRunCapture{}
	for range []int{0, 1} {
		select {
		case c := <-created:
			got[c.Name] = c
		case <-time.After(2 * time.Second):
			require.FailNow(t, "timed out waiting for merge_group check run")
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
		require.FailNow(t, fmt.Sprintf("unexpected check run created for destroyed action: %q", c.Name))
	case <-time.After(100 * time.Millisecond):
	}
}

// A merge_group event for a repository SchemaBot does not manage gets no check:
// SchemaBot's check is not required on that repo, so there is nothing to unblock.
func TestWebhookMergeGroupRejectsUnregisteredRepo(t *testing.T) {
	reader := newDispatchMetricsReader(t)
	h, created := mergeGroupTestHandler(t, nil, map[string]api.RepoConfig{"org/allowed-repo": {}})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildMergeGroupWebhookRequest(t, "checks_requested", "mergesha123", nil))

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "repository not registered")

	select {
	case c := <-created:
		require.FailNow(t, fmt.Sprintf("unexpected check run created for unregistered repo: %q", c.Name))
	case <-time.After(100 * time.Millisecond):
	}
	requireUnregisteredRepositoryCounted(t, reader, "merge_group", "checks_requested")
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
	service := api.New(nil, &api.ServerConfig{
		AllowedEnvironments: []string{"production"},
		Repos:               map[string]api.RepoConfig{"octocat/hello-world": {}},
	}, nil, testLogger())
	h := &Handler{
		insecureSkipSignatureVerification: true,
		service:                           service,
		ghClients:                         ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
		logger:                            testLogger(),
	}

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildMergeGroupWebhookRequest(t, "checks_requested", "mergesha123", nil))
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case id := <-updated:
		assert.Equal(t, int64(7), id)
	case <-time.After(2 * time.Second):
		require.FailNow(t, "expected the existing check run to be updated")
	}

	select {
	case name := <-created:
		require.FailNow(t, fmt.Sprintf("expected an update, not a new check run: %q", name))
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
		require.FailNow(t, "timed out waiting for merge_group check run")
	}

	select {
	case c := <-created:
		require.FailNow(t, fmt.Sprintf("expected exactly one aggregate check, got extra: %q", c.Name))
	case <-time.After(100 * time.Millisecond):
	}
}

// An aggregate participant's checks are never required, so it posts nothing on
// a merge-group commit — the leader publishes the required aggregate there.
// Without this silence, every queue entry would re-grow the per-tenant check
// rows the aggregate removes from PR heads.
func TestWebhookMergeGroupParticipantStaysSilent(t *testing.T) {
	h, created := mergeGroupTestHandler(t,
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
		require.FailNow(t, fmt.Sprintf("participant posted a merge_group check run: %q", c.Name))
	case <-time.After(100 * time.Millisecond):
	}
}

// The aggregate leader keeps posting its required checks on merge-group
// commits — silence is participant-only, so the queue never wedges.
func TestWebhookMergeGroupLeaderStillPosts(t *testing.T) {
	h, created := mergeGroupTestHandler(t,
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
		require.FailNow(t, "timed out waiting for the leader's merge_group check run")
	}
}

// A checks backfill posts the same passing check on a merge group head that
// the merge_group webhook would have, so a queue entry whose delivery was lost
// stops blocking the PRs behind it.
func TestBackfillMergeQueueCheckPostsPassingChecks(t *testing.T) {
	h, created, mux := mergeGroupTestHandlerWithMux(t,
		[]string{"staging", "production"},
		map[string]api.RepoConfig{"octocat/hello-world": {}},
	)
	serveTrustedCheckRuns(t, mux, "octocat/hello-world", "group-412")

	outcome, err := h.BackfillMergeQueueCheck(t.Context(), "octocat/hello-world", 12345, "group-412", api.MergeQueueCheckScope{})
	require.NoError(t, err)
	assert.Equal(t, "posted passing merge queue check", outcome)

	got := map[string]checkRunCapture{}
	for range []int{0, 1} {
		select {
		case c := <-created:
			got[c.Name] = c
		case <-time.After(2 * time.Second):
			require.FailNow(t, "timed out waiting for the backfilled merge queue check run")
		}
	}
	for _, name := range []string{"SchemaBot (staging)", "SchemaBot (production)"} {
		c, ok := got[name]
		require.True(t, ok, "expected a check run named %q", name)
		assert.Equal(t, "group-412", c.HeadSHA)
		assert.Equal(t, "completed", c.Status)
		assert.Equal(t, "success", c.Conclusion)
	}
}

// A backfill skips a merge group for the same reasons the merge_group webhook
// does: an unregistered repository, or an aggregate participant whose leader
// posts the required check. Neither posts anything.
func TestBackfillMergeQueueCheckSkipsWhereTheWebhookWould(t *testing.T) {
	for name, tc := range map[string]struct {
		repos map[string]api.RepoConfig
		want  string
	}{
		"unregistered repository": {
			repos: map[string]api.RepoConfig{"octocat/other-repo": {}},
			want:  mergeGroupSkipUnregistered,
		},
		"aggregate participant": {
			repos: map[string]api.RepoConfig{"octocat/hello-world": {Aggregate: &api.AggregateConfig{Role: api.AggregateRoleParticipant}}},
			want:  mergeGroupSkipParticipant,
		},
	} {
		t.Run(name, func(t *testing.T) {
			h, created := mergeGroupTestHandler(t, []string{"production"}, tc.repos)

			outcome, err := h.BackfillMergeQueueCheck(t.Context(), "octocat/hello-world", 12345, "group-412", api.MergeQueueCheckScope{})
			require.NoError(t, err)
			assert.Equal(t, tc.want, outcome)

			select {
			case c := <-created:
				require.FailNow(t, fmt.Sprintf("backfill posted a merge queue check run it should have skipped: %q", c.Name))
			case <-time.After(100 * time.Millisecond):
			}
		})
	}
}

// A backfill run for one environment posts only that environment's merge queue
// check. A check already on the merge group is left as it is, even one still
// in progress, so the backfill never overwrites a check another path owns.
func TestBackfillMergeQueueCheckPostsOnlyTheMissingChecksInScope(t *testing.T) {
	h, created, mux := mergeGroupTestHandlerWithMux(t,
		[]string{"staging", "production"},
		map[string]api.RepoConfig{"octocat/hello-world": {}},
	)
	serveTrustedCheckRuns(t, mux, "octocat/hello-world", "group-412", ghclient.CheckRunResult{
		ID: 701, Name: "SchemaBot (staging)", Status: "in_progress",
	})
	mux.HandleFunc("PATCH /repos/octocat/hello-world/check-runs/", func(http.ResponseWriter, *http.Request) {
		assert.Fail(t, "backfill updated an existing merge queue check run")
	})

	outcome, err := h.BackfillMergeQueueCheck(t.Context(), "octocat/hello-world", 12345, "group-412",
		api.MergeQueueCheckScope{Environment: "production"})
	require.NoError(t, err)
	assert.Equal(t, "posted passing merge queue check", outcome)
	assertCreatedCheckRuns(t, created, "SchemaBot (production)")

	outcome, err = h.BackfillMergeQueueCheck(t.Context(), "octocat/hello-world", 12345, "group-412",
		api.MergeQueueCheckScope{Environment: "staging"})
	require.NoError(t, err)
	assert.Equal(t, mergeQueueChecksAlreadyPresent, outcome)
	assertCreatedCheckRuns(t, created)
}

// A check name the server does not post on the merge group fails the head
// rather than reporting it handled, so the group stays reported as missing the
// check the operator asked for.
func TestBackfillMergeQueueCheckRefusesAScopeWithNoCheck(t *testing.T) {
	h, created, mux := mergeGroupTestHandlerWithMux(t,
		[]string{"staging", "production"},
		map[string]api.RepoConfig{"octocat/hello-world": {}},
	)
	serveTrustedCheckRuns(t, mux, "octocat/hello-world", "group-412")

	_, err := h.BackfillMergeQueueCheck(t.Context(), "octocat/hello-world", 12345, "group-412",
		api.MergeQueueCheckScope{CheckName: "SchemaBot (sandbox)"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `posts no merge queue check matching environment "" and check name "SchemaBot (sandbox)"`)
	assertCreatedCheckRuns(t, created)
}

// A backfill that cannot read the merge group's existing checks posts nothing,
// because it cannot tell whether it would add a second check beside one that
// is already there.
func TestBackfillMergeQueueCheckPostsNothingWhenTheLookupFails(t *testing.T) {
	h, created, mux := mergeGroupTestHandlerWithMux(t,
		[]string{"production"},
		map[string]api.RepoConfig{"octocat/hello-world": {}},
	)
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/group-412/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
	})

	_, err := h.BackfillMergeQueueCheck(t.Context(), "octocat/hello-world", 12345, "group-412", api.MergeQueueCheckScope{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `look up merge_group_check check "SchemaBot (production)"`)
	assertCreatedCheckRuns(t, created)
}

// assertCreatedCheckRuns asserts the backfill created exactly the named Check
// Runs, each completed and passing.
func assertCreatedCheckRuns(t *testing.T, created chan checkRunCapture, names ...string) {
	t.Helper()
	got := map[string]checkRunCapture{}
	for range names {
		select {
		case c := <-created:
			got[c.Name] = c
		case <-time.After(2 * time.Second):
			require.FailNow(t, fmt.Sprintf("timed out waiting for check runs %v; got %d", names, len(got)))
		}
	}
	for _, name := range names {
		c, ok := got[name]
		if assert.True(t, ok, "expected a check run named %q", name) {
			assert.Equal(t, "completed", c.Status)
			assert.Equal(t, "success", c.Conclusion)
		}
	}
	select {
	case c := <-created:
		require.FailNow(t, fmt.Sprintf("backfill created an unexpected check run: %q", c.Name))
	case <-time.After(100 * time.Millisecond):
	}
}
