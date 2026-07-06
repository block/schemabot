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

// collectAggregateConclusion drains published aggregates for name until one
// carries the wanted conclusion, tolerating interleaved publishes from the
// immediate and delayed fold passes.
func collectAggregateConclusion(t *testing.T, checkRuns chan checkRunCapture, name, wantConclusion string) checkRunCapture {
	t.Helper()
	deadline := time.After(webhookIntegrationCheckRunDeadline)
	for {
		select {
		case cr := <-checkRuns:
			if cr.Name == name && cr.Conclusion == wantConclusion {
				return cr
			}
		case <-deadline:
			t.Fatalf("timed out waiting for aggregate %q with conclusion %q", name, wantConclusion)
		}
	}
}

// A rollback on a participant must never leave the leader green. The
// participant's check reads action_required after a completed rollback (the
// PR's change is reverted), so a nudge-triggered fold blocks the aggregate —
// and it stays blocked until a fold reads positive evidence again: a trusted,
// completed, successful participant check from a fresh apply. There is no
// timeout or default path back to green.
func TestE2EParticipantCommentNudgeFailClosedThroughRollback(t *testing.T) {
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
	// The participant completed a rollback: its check is action_required.
	participantRuns := map[string]map[string]any{
		prodCheckName: {
			"id": 6001, "name": prodCheckName, "status": "completed", "conclusion": "action_required",
			"app": map[string]any{"slug": trustedTenantAppSlug},
		},
		stagingCheckName: {
			"id": 6002, "name": stagingCheckName, "status": "completed", "conclusion": "action_required",
			"app": map[string]any{"slug": trustedTenantAppSlug},
		},
	}
	mockParticipantCheckRuns(mux, participantRuns)
	checkRuns := captureLeaderCheckRuns(mux)

	h := newLeaderHandler(t, svc, client)
	h.participantNudgeRefoldDelay = 10 * time.Millisecond

	req := buildParticipantBotCommentRequest(t, trustedTenantAppSlug+"[bot]")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// The participant's post-rollback check reads action_required — its change
	// was reverted and a re-apply is pending — so the aggregate blocks as
	// action_required.
	blocked := collectAggregateConclusion(t, checkRuns, aggregateCheckNameForEnv("SchemaBot", "production"), checkConclusionActionRequired)
	assert.Equal(t, headSHA, blocked.HeadSHA)
	assert.Equal(t, checkStatusCompleted, blocked.Status,
		"a rolled-back participant blocks the aggregate")

	// A fresh apply completes on the participant: its check reads success.
	// Only now may a fold emit green.
	participantRuns[prodCheckName]["conclusion"] = "success"
	participantRuns[stagingCheckName]["conclusion"] = "success"

	req = buildParticipantBotCommentRequest(t, trustedTenantAppSlug+"[bot]")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// The green publish edits the existing Check Run in place (update payloads
	// carry no head_sha); reaching this collect at all proves green returned
	// only after the fold read a trusted, completed, successful participant
	// check.
	green := collectAggregateConclusion(t, checkRuns, aggregateCheckNameForEnv("SchemaBot", "production"), checkConclusionSuccess)
	assert.Equal(t, checkStatusCompleted, green.Status)
}

// While a participant's apply or rollback is executing, its check run is
// non-terminal; a nudge-triggered fold maps that to in_progress and the
// aggregate blocks. The leader cannot emit green while work is visibly in
// flight on any expected participant.
func TestE2EParticipantCommentNudgeBlocksOnInProgressParticipant(t *testing.T) {
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
			"id": 6101, "name": prodCheckName, "status": "in_progress",
			"app": map[string]any{"slug": trustedTenantAppSlug},
		},
		stagingCheckName: {
			"id": 6102, "name": stagingCheckName, "status": "in_progress",
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

	deadline := time.After(webhookIntegrationCheckRunDeadline)
	for {
		select {
		case cr := <-checkRuns:
			if cr.Name != aggregateCheckNameForEnv("SchemaBot", "production") {
				continue
			}
			require.NotEqual(t, checkConclusionSuccess, cr.Conclusion,
				"an in-flight participant must never fold green")
			assert.Equal(t, checkStatusInProgress, cr.Status,
				"the aggregate reports the participant's in-flight work")
			return
		case <-deadline:
			t.Fatal("timed out waiting for the folded aggregate")
		}
	}
}
