package webhook

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
)

// buildPushWebhookRequest constructs a push webhook POST request for
// octocat/hello-world, whose default branch is main.
func buildPushWebhookRequest(t *testing.T, ref, after string, deleted bool) *http.Request {
	t.Helper()

	payload := map[string]any{
		"ref":     ref,
		"after":   after,
		"deleted": deleted,
		"repository": map[string]any{
			"full_name":      "octocat/hello-world",
			"default_branch": "main",
		},
		"installation": map[string]any{
			"id": 12345,
		},
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-GitHub-Event", "push")
	return req
}

// Branch rulesets index required-check sources by the branch a check suite ran
// against, and every other SchemaBot check runs against PR or merge-queue
// heads. Publishing the passing aggregate on default-branch pushes keeps the
// App selectable as a pinned required-check source — one check per gated
// environment, with the same names as the PR-head aggregates.
func TestWebhookPushPostsPassingChecksOnDefaultBranch(t *testing.T) {
	h, created := mergeGroupTestHandler(t,
		[]string{"staging", "production"},
		map[string]api.RepoConfig{"octocat/hello-world": {}},
	)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildPushWebhookRequest(t, "refs/heads/main", "pushsha123", false))

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "default-branch checks posted")

	got := map[string]createdCheckRun{}
	for range []int{0, 1} {
		select {
		case c := <-created:
			got[c.Name] = c
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for default-branch check run")
		}
	}

	require.Len(t, got, 2)
	for _, name := range []string{"SchemaBot (staging)", "SchemaBot (production)"} {
		c, ok := got[name]
		require.True(t, ok, "expected a check run named %q", name)
		assert.Equal(t, "pushsha123", c.HeadSHA)
		assert.Equal(t, "completed", c.Status)
		assert.Equal(t, "success", c.Conclusion)
	}
}

// Pushes to feature branches, merge-queue branches, and tags are covered by
// the PR and merge-queue check paths; only the default branch needs the
// check-source seed.
func TestWebhookPushIgnoresNonDefaultBranch(t *testing.T) {
	h, created := mergeGroupTestHandler(t,
		[]string{"production"},
		map[string]api.RepoConfig{"octocat/hello-world": {}},
	)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildPushWebhookRequest(t, "refs/heads/feature-branch", "pushsha123", false))

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "not the default branch")

	select {
	case c := <-created:
		t.Fatalf("unexpected check run created for non-default branch push: %q", c.Name)
	case <-time.After(100 * time.Millisecond):
	}
}

// A branch deletion push has no commit to publish a check on.
func TestWebhookPushIgnoresBranchDeletion(t *testing.T) {
	h, created := mergeGroupTestHandler(t,
		[]string{"production"},
		map[string]api.RepoConfig{"octocat/hello-world": {}},
	)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildPushWebhookRequest(t, "refs/heads/main", "0000000000000000000000000000000000000000", true))

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "branch deletion")

	select {
	case c := <-created:
		t.Fatalf("unexpected check run created for branch deletion: %q", c.Name)
	case <-time.After(100 * time.Millisecond):
	}
}

// An aggregate participant's checks are never required — the leader owns the
// required aggregate — so a participant stays silent on default-branch pushes
// rather than seeding an informational check on every landed commit.
func TestWebhookPushParticipantStaysSilent(t *testing.T) {
	h, created := mergeGroupTestHandler(t,
		[]string{"production"},
		map[string]api.RepoConfig{"octocat/hello-world": {
			Aggregate: &api.AggregateConfig{Role: api.AggregateRoleParticipant},
		}},
	)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildPushWebhookRequest(t, "refs/heads/main", "pushsha123", false))

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "aggregate participant, staying silent")

	select {
	case c := <-created:
		t.Fatalf("participant posted a default-branch check run: %q", c.Name)
	case <-time.After(100 * time.Millisecond):
	}
}

// The aggregate leader keeps seeding its required check names on
// default-branch pushes — silence is participant-only, so the leader App
// stays selectable as a pinned required-check source.
func TestWebhookPushLeaderStillPosts(t *testing.T) {
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
	h.ServeHTTP(rr, buildPushWebhookRequest(t, "refs/heads/main", "pushsha123", false))

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "default-branch checks posted")

	select {
	case c := <-created:
		assert.Equal(t, "pushsha123", c.HeadSHA)
		assert.Equal(t, "success", c.Conclusion)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the leader's default-branch check run")
	}
}

// A push on a repository SchemaBot does not manage gets no check: SchemaBot's
// check is not required there, so there is no check source to maintain.
func TestWebhookPushRejectsUnregisteredRepo(t *testing.T) {
	h, created := mergeGroupTestHandler(t,
		[]string{"production"},
		map[string]api.RepoConfig{"org/allowed-repo": {}},
	)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildPushWebhookRequest(t, "refs/heads/main", "pushsha123", false))

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "repository not registered")

	select {
	case c := <-created:
		t.Fatalf("unexpected check run created for unregistered repo: %q", c.Name)
	case <-time.After(100 * time.Millisecond):
	}
}
