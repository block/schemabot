//go:build integration

// Aggregate-check leader gate integration tests. These exercise the leader that
// folds participant deployments' Check Runs into its own per-environment
// aggregate check, driving the fold through the check_run "completed" webhook
// path (handleParticipantCheckCompleted -> updateAggregateCheck) and asserting
// the Check Run the leader publishes on the PR head commit. The fail-closed
// safety contract is the focus: only a trusted, completed, successful
// participant check may let the aggregate pass; anything else must block merge.

package webhook

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
)

const (
	leaderOwnAppSlug     = "schemabot-leader"
	trustedTenantAppSlug = "schemabot-tenant-b"
	tenantBCheckName     = "SchemaBot Tenant B"
	tenantBSchemaDir     = "tenant-b/schema"
)

// leaderRepoConfig is the standard leader configuration used across these
// tests: a leader for octocat/hello-world that gates staging and production on
// a single expected participant (tenant-b) whose schema lives under
// tenant-b/schema and which publishes "SchemaBot Tenant B (<env>)" checks.
func leaderRepoConfig() *api.ServerConfig {
	return &api.ServerConfig{
		AllowedEnvironments: []string{"staging", "production"},
		Repos: map[string]api.RepoConfig{
			"octocat/hello-world": {
				Aggregate: &api.AggregateConfig{
					Role: api.AggregateRoleLeader,
					ExpectedTenants: []api.ExpectedTenant{{
						Tenant:    "tenant-b",
						Paths:     []string{tenantBSchemaDir},
						CheckName: tenantBCheckName,
					}},
				},
			},
		},
	}
}

// newLeaderHandler builds a Handler whose GitHub client is constructed with the
// leader's own App slug plus the trusted participant App slug, mirroring how a
// real leader deployment is wired. FindCheckRunByName only trusts runs whose
// app.slug is the leader's own slug or a configured trusted slug, so a
// participant run must carry trustedTenantAppSlug to be folded.
func newLeaderHandler(t *testing.T, svc *api.Service, client *gh.Client) *Handler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClientWithSlug(client, logger, leaderOwnAppSlug, trustedTenantAppSlug)
	factory := &fakeClientFactory{client: installClient}
	h := NewHandler(svc, factory, nil, logger)
	// A fold that leaves participants unresolved arms a self-scheduled re-fold;
	// keep those timers from firing inside the test process (pending timers at
	// process exit are inert). Tests that exercise re-fold convergence lower
	// this after construction.
	h.participantRefoldDelayOverride = time.Hour
	return h
}

// mockLeaderPRHead serves an open PR with the given head SHA so the leader's
// head-SHA freshness check passes for headSHA.
func mockLeaderPRHead(mux *http.ServeMux, headSHA string) {
	mockLeaderPRHeadWithState(mux, headSHA, "open")
}

// mockLeaderPRHeadWithState serves the PR with the given head SHA and
// lifecycle state ("open" or "closed").
func mockLeaderPRHeadWithState(mux *http.ServeMux, headSHA, state string) {
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			State: &state,
			Head:  &gh.PullRequestBranch{Ref: new("feature-branch"), SHA: &headSHA},
			Base:  &gh.PullRequestBranch{Ref: new("main"), SHA: new("def456")},
			User:  &gh.User{Login: new("testuser")},
		})
	})
}

// mockLeaderPRFilesTouchTenantB serves a PR file listing under tenant-b's
// managed directory so the leader computes tenant-b as an expected participant.
func mockLeaderPRFilesTouchTenantB(mux *http.ServeMux) {
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]*gh.CommitFile{
			{Filename: new(tenantBSchemaDir + "/users.sql"), Status: new("added")},
		})
	})
}

// mockParticipantCheckRuns serves the participant Check Run lookup
// (Checks.ListCheckRunsForRef) for the check names in runsByName. Names not in
// the map return an empty result (no run). runsByName values are raw check-run
// JSON objects; set app.slug per scenario to control trust.
func mockParticipantCheckRuns(mux *http.ServeMux, runsByName map[string]map[string]any) {
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/", func(w http.ResponseWriter, r *http.Request) {
		// Only the check-runs sub-endpoint is mocked; 404 anything else so an
		// accidental extra commits API call surfaces as a failure rather than
		// being masked by this handler's check-runs payload.
		if !strings.HasSuffix(r.URL.Path, "/check-runs") {
			http.NotFound(w, r)
			return
		}
		checkName := r.URL.Query().Get("check_name")
		run, ok := runsByName[checkName]
		if !ok {
			_ = json.NewEncoder(w).Encode(map[string]any{"total_count": 0, "check_runs": []any{}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"total_count": 1,
			"check_runs":  []map[string]any{run},
		})
	})
}

// captureLeaderCheckRuns records every aggregate Check Run create/update the
// leader publishes.
func captureLeaderCheckRuns(mux *http.ServeMux) chan checkRunCapture {
	checkRuns := make(chan checkRunCapture, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 999})
	})
	mux.HandleFunc("PATCH /repos/octocat/hello-world/check-runs/", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		checkRuns <- body
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 999})
	})
	return checkRuns
}

// buildParticipantCompletedRequest builds a check_run "completed" webhook for a
// participant's env-scoped check. The leader reacts to this by re-folding the
// aggregate for the PR head, re-reading each participant's state via the
// trusted FindCheckRunByName path.
func buildParticipantCompletedRequest(t *testing.T, checkName, headSHA string) *http.Request {
	t.Helper()
	return buildCheckRunWebhookRequest(t, checkRunWebhookPayloadOpts{
		action:    "completed",
		checkName: checkName,
		headSHA:   headSHA,
	}, nil)
}

// collectAggregate waits for the leader to publish an aggregate check for the
// given env-scoped name, draining any other captured runs (the leader publishes
// one aggregate per allowed environment).
func collectAggregate(t *testing.T, checkRuns chan checkRunCapture, wantName string) checkRunCapture {
	t.Helper()
	deadline := time.After(webhookIntegrationCheckRunDeadline)
	for {
		select {
		case cr := <-checkRuns:
			if cr.Name == wantName {
				return cr
			}
		case <-deadline:
			t.Fatalf("timed out waiting for aggregate check run %q", wantName)
		}
	}
}

// A PR touches only the participant tenant's schema, so the leader owns no
// per-database check of its own and gates purely on the participant. When the
// participant's env-scoped Check Run is trusted, completed, and successful, the
// leader's aggregate for that environment passes.
func TestE2ELeaderPassesOnTrustedSuccessfulParticipant(t *testing.T) {
	svc := setupE2EServiceWithConfig(t, leaderRepoConfig())

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
			"id": 4001, "name": prodCheckName, "status": "completed", "conclusion": "success",
			"app": map[string]any{"slug": trustedTenantAppSlug},
		},
		stagingCheckName: {
			"id": 4002, "name": stagingCheckName, "status": "completed", "conclusion": "success",
			"app": map[string]any{"slug": trustedTenantAppSlug},
		},
	})
	checkRuns := captureLeaderCheckRuns(mux)

	h := newLeaderHandler(t, svc, client)
	req := buildParticipantCompletedRequest(t, prodCheckName, headSHA)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "aggregate re-folded on participant check completion")

	cr := collectAggregate(t, checkRuns, aggregateCheckNameForEnv("SchemaBot", "production"))
	assert.Equal(t, headSHA, cr.HeadSHA)
	assert.Equal(t, checkStatusCompleted, cr.Status)
	assert.Equal(t, checkConclusionSuccess, cr.Conclusion)

	// The stored aggregate mirrors the passing Check Run.
	stored, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1,
		"production", aggregateSentinel, aggregateSentinel)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, checkConclusionSuccess, stored.Conclusion)
	assert.Equal(t, headSHA, stored.HeadSHA)
}

// When an expected participant has not reported its Check Run on the head
// commit, the leader has no evidence the participant's schema is safe. It fails
// closed: the aggregate blocks as in_progress (a self-scheduled re-fold will
// re-read the participant shortly) rather than passing on the participant's
// silence.
func TestE2ELeaderBlocksOnMissingParticipantCheck(t *testing.T) {
	svc := setupE2EServiceWithConfig(t, leaderRepoConfig())

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	const headSHA = "abc123"
	mockLeaderPRHead(mux, headSHA)
	mockLeaderPRFilesTouchTenantB(mux)

	// No participant runs registered — the lookup returns total_count 0.
	mockParticipantCheckRuns(mux, nil)
	checkRuns := captureLeaderCheckRuns(mux)

	h := newLeaderHandler(t, svc, client)
	prodCheckName := aggregateCheckNameForEnv(tenantBCheckName, "production")
	req := buildParticipantCompletedRequest(t, prodCheckName, headSHA)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	cr := collectAggregate(t, checkRuns, aggregateCheckNameForEnv("SchemaBot", "production"))
	assert.Equal(t, headSHA, cr.HeadSHA)
	assert.NotEqual(t, checkConclusionSuccess, cr.Conclusion, "missing participant must not pass the aggregate")
	assert.Equal(t, checkStatusInProgress, cr.Status)
	assert.Empty(t, cr.Conclusion, "the wait for a participant re-read blocks without a conclusion")
}

// While the participant's Check Run is still in progress, the leader's
// aggregate must stay in_progress (no conclusion), which blocks merge until the
// participant reaches a terminal state.
func TestE2ELeaderBlocksOnInProgressParticipant(t *testing.T) {
	svc := setupE2EServiceWithConfig(t, leaderRepoConfig())

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
			"id": 5001, "name": prodCheckName, "status": "in_progress",
			"app": map[string]any{"slug": trustedTenantAppSlug},
		},
		stagingCheckName: {
			"id": 5002, "name": stagingCheckName, "status": "in_progress",
			"app": map[string]any{"slug": trustedTenantAppSlug},
		},
	})
	checkRuns := captureLeaderCheckRuns(mux)

	h := newLeaderHandler(t, svc, client)
	req := buildParticipantCompletedRequest(t, prodCheckName, headSHA)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	cr := collectAggregate(t, checkRuns, aggregateCheckNameForEnv("SchemaBot", "production"))
	assert.Equal(t, headSHA, cr.HeadSHA)
	assert.Equal(t, checkStatusInProgress, cr.Status)
	assert.Empty(t, cr.Conclusion, "in-progress participant leaves the aggregate without a conclusion")
}

// A same-named Check Run published only by an untrusted GitHub App (for example
// a GitHub Actions job configured with a matching name) can never satisfy the
// gate. The leader fails closed: the aggregate blocks rather than trusting a
// run whose App is not the participant's.
func TestE2ELeaderBlocksOnUntrustedParticipantApp(t *testing.T) {
	svc := setupE2EServiceWithConfig(t, leaderRepoConfig())

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
	// The runs are completed+success but published by an untrusted App slug.
	mockParticipantCheckRuns(mux, map[string]map[string]any{
		prodCheckName: {
			"id": 6001, "name": prodCheckName, "status": "completed", "conclusion": "success",
			"app": map[string]any{"slug": "some-other-app"},
		},
		stagingCheckName: {
			"id": 6002, "name": stagingCheckName, "status": "completed", "conclusion": "success",
			"app": map[string]any{"slug": "some-other-app"},
		},
	})
	checkRuns := captureLeaderCheckRuns(mux)

	h := newLeaderHandler(t, svc, client)
	req := buildParticipantCompletedRequest(t, prodCheckName, headSHA)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	cr := collectAggregate(t, checkRuns, aggregateCheckNameForEnv("SchemaBot", "production"))
	assert.Equal(t, headSHA, cr.HeadSHA)
	assert.NotEqual(t, checkConclusionSuccess, cr.Conclusion, "untrusted-app check must not pass the aggregate")
	assert.Equal(t, checkStatusCompleted, cr.Status)
	assert.Equal(t, checkConclusionActionRequired, cr.Conclusion)
}

// A check_run completion on a repo this deployment does not lead must not
// trigger a re-fold: a participant deployment reacting to completions would
// double-write the leader's check. The handler returns the non-leader ignored
// response and posts no aggregate Check Run.
func TestE2ELeaderRefoldSkippedForNonLeaderRepo(t *testing.T) {
	svc := setupE2EServiceWithConfig(t, &api.ServerConfig{
		AllowedEnvironments: []string{"staging", "production"},
		Repos: map[string]api.RepoConfig{
			"octocat/hello-world": {Aggregate: &api.AggregateConfig{Role: api.AggregateRoleParticipant}},
		},
	})

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// Any check-run POST here is a failure — a non-leader must not fold.
	checkRuns := captureLeaderCheckRuns(mux)

	h := newLeaderHandler(t, svc, client)
	// The completed check is a participant-style check name, not this
	// deployment's own aggregate.
	req := buildParticipantCompletedRequest(t, aggregateCheckNameForEnv(tenantBCheckName, "production"), "abc123")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "check_run completion ignored for non-leader repo")

	select {
	case cr := <-checkRuns:
		t.Fatalf("non-leader repo published an aggregate check on participant completion: %+v", cr)
	case <-time.After(time.Second):
	}
}

// A leader must not re-fold in response to its own aggregate Check Run
// completing, or it would loop on its own writes. When the completed check name
// is the leader's own aggregate, the handler returns the own-aggregate ignored
// response and posts no aggregate.
func TestE2ELeaderRefoldSkippedForOwnAggregateCheck(t *testing.T) {
	svc := setupE2EServiceWithConfig(t, leaderRepoConfig())

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	checkRuns := captureLeaderCheckRuns(mux)

	h := newLeaderHandler(t, svc, client)
	// The completed check is the leader's own env-scoped aggregate.
	req := buildParticipantCompletedRequest(t, aggregateCheckNameForEnv("SchemaBot", "production"), "abc123")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "check_run completion ignored for own aggregate check")

	select {
	case cr := <-checkRuns:
		t.Fatalf("leader re-folded on its own aggregate completion: %+v", cr)
	case <-time.After(time.Second):
	}
}

// mockLeaderEmptyTree serves an empty git tree for headSHA so config discovery
// resolves no schemabot.yaml — the leader manages none of the PR's files.
func mockLeaderEmptyTree(mux *http.ServeMux, headSHA string) {
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/"+headSHA, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.Tree{SHA: new(headSHA), Entries: []*gh.TreeEntry{}, Truncated: new(false)})
	})
}

// A PR that touches only an expected participant's schema carries schema work
// the leader must gate on, even though the leader itself manages none of the
// changed files. The pull_request event routes through the aggregate fold, so
// the required aggregate blocks (fail-closed) until the participant reports —
// it must not pass as "no managed schema changes" while the participant's
// schema change is unplanned, in flight, or its deployment is down.
func TestE2ELeaderNonSchemaPRTouchingParticipantPathBlocks(t *testing.T) {
	svc := setupE2EServiceWithConfig(t, leaderRepoConfig())

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	const headSHA = "abc123"
	mockLeaderPRHead(mux, headSHA)
	mockLeaderPRFilesTouchTenantB(mux)
	mockLeaderEmptyTree(mux, headSHA)
	// The participant has posted no Check Run at all on the head commit.
	mockParticipantCheckRuns(mux, nil)
	checkRuns := captureLeaderCheckRuns(mux)

	h := newLeaderHandler(t, svc, client)
	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: headSHA,
		headRef: "feature-branch",
	}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "auto-plan started")

	for _, env := range []string{"staging", "production"} {
		cr := collectAggregate(t, checkRuns, aggregateCheckNameForEnv("SchemaBot", env))
		assert.Equal(t, headSHA, cr.HeadSHA)
		assert.Equal(t, checkStatusInProgress, cr.Status,
			"aggregate for %s must block while the expected participant is silent", env)
		assert.Empty(t, cr.Conclusion)
	}
}

// A participant with no schema work publishes its Check Run moments after the
// leader's first fold and never comments, so no nudge will ever arrive. The
// leader must converge on its own: the first fold blocks fail-closed on the
// unreported participant and arms a delayed re-fold, and the re-fold reads the
// now-present successful participant check and publishes a passing aggregate.
func TestE2ELeaderRefoldsWhenParticipantReportsAfterFirstFold(t *testing.T) {
	svc := setupE2EServiceWithConfig(t, leaderRepoConfig())

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	const headSHA = "abc123"
	mockLeaderPRHead(mux, headSHA)
	mockLeaderPRFilesTouchTenantB(mux)
	mockLeaderEmptyTree(mux, headSHA)

	// The first fold reads one participant check per environment and sees
	// nothing — the participant has not published yet. Every later read finds
	// the completed, successful, trusted run, exactly like a participant that
	// publishes its check just after the leader's first fold.
	prodCheckName := aggregateCheckNameForEnv(tenantBCheckName, "production")
	stagingCheckName := aggregateCheckNameForEnv(tenantBCheckName, "staging")
	runsByName := map[string]map[string]any{
		prodCheckName: {
			"id": 7001, "name": prodCheckName, "status": "completed", "conclusion": "success",
			"app": map[string]any{"slug": trustedTenantAppSlug},
		},
		stagingCheckName: {
			"id": 7002, "name": stagingCheckName, "status": "completed", "conclusion": "success",
			"app": map[string]any{"slug": trustedTenantAppSlug},
		},
	}
	var checkRunReads atomic.Int64
	firstFoldReads := int64(len(leaderRepoConfig().AllowedEnvironments))
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/", func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/check-runs") {
			http.NotFound(w, r)
			return
		}
		if checkRunReads.Add(1) <= firstFoldReads {
			_ = json.NewEncoder(w).Encode(map[string]any{"total_count": 0, "check_runs": []any{}})
			return
		}
		run, ok := runsByName[r.URL.Query().Get("check_name")]
		if !ok {
			_ = json.NewEncoder(w).Encode(map[string]any{"total_count": 0, "check_runs": []any{}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"total_count": 1, "check_runs": []map[string]any{run}})
	})
	checkRuns := captureLeaderCheckRuns(mux)

	h := newLeaderHandler(t, svc, client)
	h.participantRefoldDelayOverride = 10 * time.Millisecond
	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: headSHA,
		headRef: "feature-branch",
	}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// The first fold blocks fail-closed, then the self-scheduled re-fold
	// converges to success without any comment, commit, or webhook.
	sawBlocked := false
	deadline := time.After(webhookIntegrationCheckRunDeadline)
	for {
		var cr checkRunCapture
		select {
		case cr = <-checkRuns:
		case <-deadline:
			t.Fatalf("timed out waiting for the production aggregate to converge (saw blocked first: %v)", sawBlocked)
		}
		if cr.Name != aggregateCheckNameForEnv("SchemaBot", "production") {
			continue
		}
		if cr.Status == checkStatusInProgress {
			// The blocked aggregate is created fresh on the head commit; the
			// converged pass below updates that run in place, so head_sha only
			// appears on this create.
			assert.Equal(t, headSHA, cr.HeadSHA)
			assert.Empty(t, cr.Conclusion, "the wait for the participant re-read blocks without a conclusion")
			sawBlocked = true
			continue
		}
		assert.True(t, sawBlocked, "the first fold must block before the re-fold converges")
		assert.Equal(t, checkStatusCompleted, cr.Status)
		assert.Equal(t, checkConclusionSuccess, cr.Conclusion)
		break
	}

	// The converged fold resets the re-fold budget for the PR.
	require.Eventually(t, func() bool {
		h.participantRefoldMu.Lock()
		defer h.participantRefoldMu.Unlock()
		_, pending := h.participantRefoldAttempts[participantRefoldKey("octocat/hello-world", 1)]
		return !pending
	}, webhookIntegrationCheckRunDeadline, 10*time.Millisecond, "a fold that resolves every participant must clear the re-fold budget")
}

// The expected-tenant set is path-filtered: a leader's non-schema PR touching
// none of the participants' paths keeps the plain passing aggregate, so
// unrelated PRs are unaffected by participant gating.
func TestE2ELeaderNonSchemaPROutsideParticipantPathsPasses(t *testing.T) {
	svc := setupE2EServiceWithConfig(t, leaderRepoConfig())

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	const headSHA = "abc123"
	mockLeaderPRHead(mux, headSHA)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]*gh.CommitFile{
			{Filename: new("README.md"), Status: new("modified")},
		})
	})
	mockLeaderEmptyTree(mux, headSHA)
	checkRuns := captureLeaderCheckRuns(mux)

	h := newLeaderHandler(t, svc, client)
	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: headSHA,
		headRef: "feature-branch",
	}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "auto-plan started")
	assert.NotContains(t, rr.Body.String(), "aggregate folds expected participants")

	for _, env := range []string{"staging", "production"} {
		cr := collectAggregate(t, checkRuns, aggregateCheckNameForEnv("SchemaBot", env))
		assert.Equal(t, headSHA, cr.HeadSHA)
		assert.Equal(t, checkStatusCompleted, cr.Status)
		assert.Equal(t, checkConclusionSuccess, cr.Conclusion,
			"aggregate for %s keeps passing on a PR outside all participant paths", env)
	}
}

// participantRefoldAttemptCount reads the leader's re-fold budget entry for the
// PR, reporting the consumed attempts and whether an entry exists at all.
func participantRefoldAttemptCount(h *Handler, repo string, pr int) (int, bool) {
	h.participantRefoldMu.Lock()
	defer h.participantRefoldMu.Unlock()
	n, ok := h.participantRefoldAttempts[participantRefoldKey(repo, pr)]
	return n, ok
}

// A re-fold pass that cannot read the PR head must not end the retry chain:
// the aggregate is rendering unresolved participants as in_progress on the
// promise of a coming re-read, so the leader arms another bounded attempt
// instead of stranding the wait until an external event happens to re-fold.
func TestE2ELeaderBrokenRefoldArmsAnotherAttempt(t *testing.T) {
	svc := setupE2EServiceWithConfig(t, leaderRepoConfig())

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// No PR endpoint is registered, so the re-fold's head fetch fails.
	checkRuns := captureLeaderCheckRuns(mux)

	h := newLeaderHandler(t, svc, client)
	h.refoldAggregateForPR("octocat/hello-world", 1, 12345, "unresolved participant delayed re-fold")

	attempts, armed := participantRefoldAttemptCount(h, "octocat/hello-world", 1)
	assert.True(t, armed, "a broken re-fold pass must arm another bounded attempt")
	assert.Equal(t, 1, attempts)

	select {
	case cr := <-checkRuns:
		t.Fatalf("a re-fold that cannot read the PR head must not publish an aggregate: %+v", cr)
	default:
	}
}

// An armed re-fold can fire after its PR closes. The leader publishes nothing
// on the closed PR and arms no further attempts; it drops the PR's re-fold
// budget entry so the in-memory map does not retain closed PRs.
func TestE2ELeaderRefoldSkipsClosedPR(t *testing.T) {
	svc := setupE2EServiceWithConfig(t, leaderRepoConfig())

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	mockLeaderPRHeadWithState(mux, "abc123", "closed")
	checkRuns := captureLeaderCheckRuns(mux)

	h := newLeaderHandler(t, svc, client)
	// The chain was armed while the PR was open, so a budget entry exists when
	// the timer fires.
	h.scheduleParticipantRefold(t.Context(), "octocat/hello-world", 1, 12345)
	_, armed := participantRefoldAttemptCount(h, "octocat/hello-world", 1)
	require.True(t, armed)

	h.refoldAggregateForPR("octocat/hello-world", 1, 12345, "unresolved participant delayed re-fold")

	_, armed = participantRefoldAttemptCount(h, "octocat/hello-world", 1)
	assert.False(t, armed, "a closed PR's re-fold budget entry must be dropped")

	select {
	case cr := <-checkRuns:
		t.Fatalf("a re-fold must not publish an aggregate on a closed PR: %+v", cr)
	default:
	}
}
