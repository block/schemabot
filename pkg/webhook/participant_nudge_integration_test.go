//go:build integration

// Participant-comment nudge integration tests. A trusted sibling SchemaBot
// deployment's PR comment is the leader's only timely signal that a
// participant's Check Run changed (check_run events are delivered only to the
// App that created the check), so the leader consumes such comments as
// aggregate re-fold triggers — never parsing the body — and re-reads every
// participant's authoritative Check Run before writing the aggregate.

package webhook

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
)

// nudgeLeaderConfig is leaderRepoConfig with the multi-App shape so the
// deployment carries trusted-check-app-slugs for the led repo.
func nudgeLeaderConfig() *api.ServerConfig {
	cfg := leaderRepoConfig()
	cfg.Apps = map[string]api.GitHubAppConfig{
		"default": {TrustedCheckAppSlugs: []string{trustedTenantAppSlug}},
	}
	repoCfg := cfg.Repos["octocat/hello-world"]
	repoCfg.GitHubApp = "default"
	cfg.Repos["octocat/hello-world"] = repoCfg
	return cfg
}

func buildParticipantBotCommentRequest(t *testing.T, login string) *http.Request {
	t.Helper()
	return buildWebhookRequest(t, webhookPayloadOpts{
		comment:   "## ✅ Schema Change Applied — Production",
		userType:  "Bot",
		userLogin: login,
		isPR:      true,
	}, nil)
}

// A trusted participant's comment re-folds the aggregate: the leader fetches
// the PR head, re-reads the participant's Check Run via the trusted API path,
// and publishes the folded aggregate — twice (immediate and delayed pass), so
// a comment that lands moments before the participant's Check Run update still
// converges.
func TestE2EParticipantCommentNudgeRefoldsAggregate(t *testing.T) {
	svc := setupE2EServiceWithConfig(t, nudgeLeaderConfig())

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	const headSHA = "abc123"
	mockLeaderPRHead(mux, headSHA)
	mockLeaderPRFilesTouchTenantB(mux)
	prodCheckName := aggregateCheckNameForEnv(tenantBCheckName, "production")
	stagingCheckName := aggregateCheckNameForEnv(tenantBCheckName, "staging")
	mockParticipantCheckRuns(mux, map[string]map[string]any{
		prodCheckName: {
			"id": 5001, "name": prodCheckName, "status": "completed", "conclusion": "success",
			"app": map[string]any{"slug": trustedTenantAppSlug},
		},
		stagingCheckName: {
			"id": 5002, "name": stagingCheckName, "status": "completed", "conclusion": "success",
			"app": map[string]any{"slug": trustedTenantAppSlug},
		},
	})
	checkRuns := captureLeaderCheckRuns(mux)

	h := newLeaderHandler(t, svc, client)
	h.participantNudgeRefoldDelay = 10 * time.Millisecond

	req := buildParticipantBotCommentRequest(t, trustedTenantAppSlug+"[bot]")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "participant comment triggered aggregate re-fold")

	cr := collectAggregate(t, checkRuns, aggregateCheckNameForEnv("SchemaBot", "production"))
	assert.Equal(t, headSHA, cr.HeadSHA)
	assert.Equal(t, checkStatusCompleted, cr.Status)
	assert.Equal(t, checkConclusionSuccess, cr.Conclusion,
		"the nudge-triggered fold reads the participant's green check and passes the aggregate")

	// The delayed pass publishes the aggregate a second time, so a comment that
	// lands before the participant's Check Run update still converges.
	second := collectAggregate(t, checkRuns, aggregateCheckNameForEnv("SchemaBot", "production"))
	assert.Equal(t, checkConclusionSuccess, second.Conclusion, "the delayed re-fold publishes again")
}

// A bot that is not a trusted sibling deployment stays ignored — the nudge is
// gated on the same trust set as the fold's Check Run reads, so arbitrary bot
// traffic on a busy repo cannot trigger fold work.
func TestE2EParticipantCommentNudgeIgnoresUntrustedBot(t *testing.T) {
	svc := setupE2EServiceWithConfig(t, nudgeLeaderConfig())

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")
	checkRuns := captureLeaderCheckRuns(mux)

	h := newLeaderHandler(t, svc, client)
	h.participantNudgeRefoldDelay = 10 * time.Millisecond

	req := buildParticipantBotCommentRequest(t, "some-ci-bot[bot]")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "event ignored (comment from bot)")

	select {
	case cr := <-checkRuns:
		t.Fatalf("untrusted bot comment must not trigger a fold, got check run %q", cr.Name)
	case <-time.After(300 * time.Millisecond):
	}
}

// A participant deployment receives the same sibling comments but folds
// nothing — only the leader owns the aggregate — so the nudge is leader-only
// and everyone else keeps the plain bot ignore.
func TestE2EParticipantCommentNudgeLeaderOnly(t *testing.T) {
	cfg := nudgeLeaderConfig()
	repoCfg := cfg.Repos["octocat/hello-world"]
	repoCfg.Aggregate = &api.AggregateConfig{Role: api.AggregateRoleParticipant}
	cfg.Repos["octocat/hello-world"] = repoCfg
	svc := setupE2EServiceWithConfig(t, cfg)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")
	checkRuns := captureLeaderCheckRuns(mux)

	h := newLeaderHandler(t, svc, client)
	h.participantNudgeRefoldDelay = 10 * time.Millisecond

	req := buildParticipantBotCommentRequest(t, trustedTenantAppSlug+"[bot]")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "event ignored (comment from bot)")

	select {
	case cr := <-checkRuns:
		t.Fatalf("a non-leader must not fold on sibling comments, got check run %q", cr.Name)
	case <-time.After(300 * time.Millisecond):
	}
}
