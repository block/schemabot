package webhook

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
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

// buildCheckSuiteWebhookRequest constructs a check_suite webhook POST request
// for octocat/hello-world, whose default branch is main.
func buildCheckSuiteWebhookRequest(t *testing.T, action, headSHA, headBranch string, prNumbers ...int) *http.Request {
	t.Helper()

	prs := make([]map[string]any, 0, len(prNumbers))
	for _, n := range prNumbers {
		prs = append(prs, map[string]any{"number": n})
	}
	payload := map[string]any{
		"action": action,
		"check_suite": map[string]any{
			"head_sha":      headSHA,
			"head_branch":   headBranch,
			"pull_requests": prs,
		},
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
	req.Header.Set("X-GitHub-Event", "check_suite")
	return req
}

// newCheckSuiteIngressHandler builds a recovery-enabled handler with no
// GitHub server: the ingress path must never touch the GitHub API.
func newCheckSuiteIngressHandler(t *testing.T, store storage.WebhookEventStore, repos map[string]api.RepoConfig, opts ...HandlerOption) *Handler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: store}, &api.ServerConfig{Repos: repos}, nil, logger)
	factory := &fakeClientFactory{}
	return NewHandler(service, factory, nil, logger,
		append([]HandlerOption{WithDurableWebhookDispatch(), WithCheckSuiteRecovery()}, opts...)...)
}

// newCheckSuiteProcessHandler builds a recovery-enabled handler whose GitHub
// client talks to a fake server; register PR responses on the returned mux.
func newCheckSuiteProcessHandler(t *testing.T, store storage.WebhookEventStore, repos map[string]api.RepoConfig, opts ...HandlerOption) (*Handler, *http.ServeMux) {
	t.Helper()
	ghc, mux := setupGitHubServer(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: store}, &api.ServerConfig{Repos: repos}, nil, logger)
	factory := &fakeClientFactory{client: ghclient.NewInstallationClient(ghc, logger)}
	h := NewHandler(service, factory, nil, logger,
		append([]HandlerOption{WithDurableWebhookDispatch(), WithCheckSuiteRecovery()}, opts...)...)
	return h, mux
}

// durableCheckSuiteEvent builds a claimed inbox row as the ingress path
// stores it: a check_suite delivery for octocat/hello-world with the given
// payload PR snapshot.
func durableCheckSuiteEvent(t *testing.T, headSHA string, prNumbers ...int) *storage.WebhookEvent {
	t.Helper()

	prs := make([]map[string]any, 0, len(prNumbers))
	for _, n := range prNumbers {
		prs = append(prs, map[string]any{"number": n})
	}
	payload, err := json.Marshal(map[string]any{
		"action": "requested",
		"check_suite": map[string]any{
			"head_sha":      headSHA,
			"head_branch":   "feature",
			"pull_requests": prs,
		},
		"repository": map[string]any{
			"full_name":      "octocat/hello-world",
			"default_branch": "main",
		},
		"installation": map[string]any{"id": 12345},
	})
	require.NoError(t, err)
	return &storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: "delivery-check-suite-1",
		Event:      "check_suite",
		Action:     "requested",
		Repository: "octocat/hello-world",
		HeadSHA:    headSHA,
		TenantID:   "12345",
		Payload:    payload,
	}
}

// durableCheckSuiteForkEvent builds a claimed inbox row for a fork-headed
// suite: head_branch is null in the payload and GitHub cannot name the PRs,
// so processing must resolve them via the open-PR listing.
func durableCheckSuiteForkEvent(t *testing.T, headSHA string) *storage.WebhookEvent {
	t.Helper()

	payload, err := json.Marshal(map[string]any{
		"action": "requested",
		"check_suite": map[string]any{
			"head_sha":      headSHA,
			"head_branch":   "",
			"pull_requests": []map[string]any{},
		},
		"repository": map[string]any{
			"full_name": "octocat/hello-world",
		},
		"installation": map[string]any{"id": 12345},
	})
	require.NoError(t, err)
	return &storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: "delivery-check-suite-1",
		Event:      "check_suite",
		Action:     "requested",
		Repository: "octocat/hello-world",
		HeadSHA:    headSHA,
		TenantID:   "12345",
		Payload:    payload,
	}
}

// writeSinglePR responds to a GET /repos/{repo}/pulls/{n} fetch with a PR in
// the given lifecycle state at the given current head.
func writeSinglePR(t *testing.T, w http.ResponseWriter, number int, state, headSHA string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
		"number": number,
		"state":  state,
		"merged": state == "closed",
		"head":   map[string]any{"sha": headSHA, "ref": "feature"},
		"base":   map[string]any{"ref": "main"},
		"user":   map[string]any{"login": "octocat"},
	}))
}

// coveringAutoPlanRow is an organic pull_request delivery that already plans
// the head, so recovery must no-op on it.
func coveringAutoPlanRow(t *testing.T, store storage.WebhookEventStore, pr int, headSHA string) {
	t.Helper()
	_, err := store.Create(t.Context(), &storage.WebhookEvent{
		Provider:    storage.WebhookProviderGitHub,
		DeliveryID:  "delivery-organic-1",
		Event:       "pull_request",
		Action:      "synchronize",
		Repository:  "octocat/hello-world",
		PullRequest: pr,
		HeadSHA:     headSHA,
		Payload:     []byte(`{}`),
	})
	require.NoError(t, err)
}

// A check_suite.requested delivery is persisted with a not-before time (the
// recovery grace) and ACKed fast — no GitHub call happens on the request
// path, and the row only becomes claimable after the organic pull_request
// delivery has had time to win.
func TestCheckSuiteWebhookQueuesWithGrace(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h := newCheckSuiteIngressHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})

	req := buildCheckSuiteWebhookRequest(t, "requested", "suite-sha", "feature", 7)
	req.Header.Set(headerDeliveryID, "delivery-cs-1")
	rr := httptest.NewRecorder()
	before := time.Now()

	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `{"message":"check_suite recovery queued"}`, rr.Body.String())
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, "delivery-cs-1")
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, "check_suite", row.Event)
	assert.Equal(t, "requested", row.Action)
	assert.Equal(t, "octocat/hello-world", row.Repository)
	assert.Equal(t, "suite-sha", row.HeadSHA)
	assert.Equal(t, "12345", row.TenantID)
	require.NotNil(t, row.RetryAfter, "check_suite rows must carry the recovery grace as a not-before time")
	assert.WithinDuration(t, before.Add(defaultCheckSuiteRecoveryGrace), *row.RetryAfter, 10*time.Second)
}

// Only "requested" carries recovery work: "rerequested" re-plans through
// check_run.rerequested and "completed" is pure noise, so neither may occupy
// an inbox row.
func TestCheckSuiteWebhookIgnoresNonRequestedActions(t *testing.T) {
	for _, action := range []string{"rerequested", "completed"} {
		t.Run(action, func(t *testing.T) {
			store := newRecordingWebhookEventStore()
			h := newCheckSuiteIngressHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})

			req := buildCheckSuiteWebhookRequest(t, action, "suite-sha", "feature", 7)
			req.Header.Set(headerDeliveryID, "delivery-cs-1")
			rr := httptest.NewRecorder()

			h.ServeHTTP(rr, req)

			require.Equal(t, http.StatusOK, rr.Code)
			require.JSONEq(t, `{"message":"check_suite action ignored"}`, rr.Body.String())
			row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, "delivery-cs-1")
			require.NoError(t, err)
			require.Nil(t, row)
		})
	}
}

// Without the recovery option the delivery is acknowledged and dropped — the
// kill switch must silence the feature without failing GitHub's delivery.
func TestCheckSuiteWebhookIgnoredWhenRecoveryDisabled(t *testing.T) {
	store := newRecordingWebhookEventStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: store}, &api.ServerConfig{
		Repos: map[string]api.RepoConfig{"octocat/hello-world": {}},
	}, nil, logger)
	h := NewHandler(service, &fakeClientFactory{}, nil, logger, WithDurableWebhookDispatch())

	req := buildCheckSuiteWebhookRequest(t, "requested", "suite-sha", "feature", 7)
	req.Header.Set(headerDeliveryID, "delivery-cs-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `{"message":"check_suite recovery disabled"}`, rr.Body.String())
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, "delivery-cs-1")
	require.NoError(t, err)
	require.Nil(t, row)
}

// Recovery rides the durable inbox, so with dispatch disabled the delivery is
// acknowledged and dropped — and the response names the dispatch gate, not
// recovery, so an operator triages the right kill switch.
func TestCheckSuiteWebhookIgnoredWhenDurableDispatchDisabled(t *testing.T) {
	store := newRecordingWebhookEventStore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: store}, &api.ServerConfig{
		Repos: map[string]api.RepoConfig{"octocat/hello-world": {}},
	}, nil, logger)
	h := NewHandler(service, &fakeClientFactory{}, nil, logger, WithCheckSuiteRecovery())

	req := buildCheckSuiteWebhookRequest(t, "requested", "suite-sha", "feature", 7)
	req.Header.Set(headerDeliveryID, "delivery-cs-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `{"message":"durable webhook dispatch disabled"}`, rr.Body.String())
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, "delivery-cs-1")
	require.NoError(t, err)
	require.Nil(t, row)
}

// A delivery from a repository outside the allowlist is acknowledged without
// occupying an inbox row.
func TestCheckSuiteWebhookRejectsUnregisteredRepo(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h := newCheckSuiteIngressHandler(t, store, map[string]api.RepoConfig{"octocat/other-repo": {}})

	req := buildCheckSuiteWebhookRequest(t, "requested", "suite-sha", "feature", 7)
	req.Header.Set(headerDeliveryID, "delivery-cs-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, `{"message":"repository not registered"}`, rr.Body.String())
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, "delivery-cs-1")
	require.NoError(t, err)
	require.Nil(t, row)
}

// check_suite.requested fires for every push to every branch, but GitHub
// names the open PRs at a same-repository head in the payload — so a
// non-fork suite with an empty list had no open PR at delivery time and is
// dropped before it occupies an inbox row, regardless of branch. A suite
// that does name a PR is queued even when its head is the default branch (a
// promotion/backport PR), and fork heads — empty head_branch, whose payload
// cannot name PRs — pass through for API resolution.
func TestCheckSuiteWebhookSkipsHeadsWithoutOpenPRs(t *testing.T) {
	tests := []struct {
		name        string
		headBranch  string
		prNumbers   []int
		wantMessage string
		wantQueued  bool
	}{
		{name: "branch push without a PR", headBranch: "feature",
			wantMessage: `{"message":"check_suite without an open PR skipped"}`},
		{name: "default-branch push without a PR", headBranch: "main",
			wantMessage: `{"message":"check_suite without an open PR skipped"}`},
		{name: "default-branch head with an open PR", headBranch: "main", prNumbers: []int{7},
			wantMessage: `{"message":"check_suite recovery queued"}`, wantQueued: true},
		{name: "fork head", headBranch: "",
			wantMessage: `{"message":"check_suite recovery queued"}`, wantQueued: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newRecordingWebhookEventStore()
			h := newCheckSuiteIngressHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})

			req := buildCheckSuiteWebhookRequest(t, "requested", "suite-sha", tt.headBranch, tt.prNumbers...)
			req.Header.Set(headerDeliveryID, "delivery-cs-1")
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)

			require.Equal(t, http.StatusOK, rr.Code)
			require.JSONEq(t, tt.wantMessage, rr.Body.String())
			row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, "delivery-cs-1")
			require.NoError(t, err)
			if tt.wantQueued {
				require.NotNil(t, row)
			} else {
				require.Nil(t, row)
			}
		})
	}
}

// A payload missing its repository is malformed and must be rejected with a
// 400 — not misfiled under the unregistered-repository metric with an empty
// repo attribute.
func TestCheckSuiteWebhookRejectsPayloadMissingRepo(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h := newCheckSuiteIngressHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})

	body, err := json.Marshal(map[string]any{
		"action":      "requested",
		"check_suite": map[string]any{"head_sha": "suite-sha", "head_branch": "feature"},
	})
	require.NoError(t, err)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/webhook", strings.NewReader(string(body)))
	req.Header.Set("X-GitHub-Event", "check_suite")
	req.Header.Set(headerDeliveryID, "delivery-cs-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusBadRequest, rr.Code)
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, "delivery-cs-1")
	require.NoError(t, err)
	require.Nil(t, row)
}

// GitHub redeliveries reuse the delivery GUID; the second arrival dedupes
// against the live row instead of inserting a duplicate.
func TestCheckSuiteWebhookDedupesRedelivery(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h := newCheckSuiteIngressHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})

	for i, want := range []string{
		`{"message":"check_suite recovery queued"}`,
		`{"message":"check_suite recovery already queued"}`,
	} {
		req := buildCheckSuiteWebhookRequest(t, "requested", "suite-sha", "feature", 7)
		req.Header.Set(headerDeliveryID, "delivery-cs-1")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		require.Equal(t, http.StatusOK, rr.Code, "request %d", i)
		require.JSONEq(t, want, rr.Body.String(), "request %d", i)
	}
}

// The steady state: the organic pull_request delivery arrived during the
// grace and planned the head, so processing finds coverage and synthesizes
// nothing.
func TestDurableCheckSuiteCoveredHeadNoOps(t *testing.T) {
	store := newRecordingWebhookEventStore()
	coveringAutoPlanRow(t, store, 7, "suite-sha")
	h, mux := newCheckSuiteProcessHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})
	mux.HandleFunc("/repos/octocat/hello-world/pulls/7", func(w http.ResponseWriter, _ *http.Request) {
		writeSinglePR(t, w, 7, "open", "suite-sha")
	})

	retry, err := h.processDurableCheckSuite(t.Context(), durableCheckSuiteEvent(t, "suite-sha", 7))

	require.NoError(t, err)
	require.False(t, retry)
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, synthesizedDeliveryGUID("octocat/hello-world", 7, "suite-sha"))
	require.NoError(t, err)
	require.Nil(t, row, "a covered head must not get a recovery row")
}

// The recovery case: an open PR still at the suite head has no auto-plan
// coverage in the inbox, so processing synthesizes the deterministic
// recovery delivery the dispatcher will plan.
func TestDurableCheckSuiteSynthesizesMissingCoverage(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h, mux := newCheckSuiteProcessHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})
	mux.HandleFunc("/repos/octocat/hello-world/pulls/7", func(w http.ResponseWriter, _ *http.Request) {
		writeSinglePR(t, w, 7, "open", "suite-sha")
	})

	retry, err := h.processDurableCheckSuite(t.Context(), durableCheckSuiteEvent(t, "suite-sha", 7))

	require.NoError(t, err)
	require.False(t, retry)
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, synthesizedDeliveryGUID("octocat/hello-world", 7, "suite-sha"))
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, "pull_request", row.Event)
	assert.Equal(t, 7, row.PullRequest)
	assert.Equal(t, "suite-sha", row.HeadSHA)
	assert.Equal(t, "12345", row.TenantID)
}

// A durable row is normalized before config and GitHub routing so replayed
// rows from any producer use the same repository identity as ingress rows.
func TestDurableCheckSuiteCanonicalizesStoredRepository(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h, mux := newCheckSuiteProcessHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})
	mux.HandleFunc("/repos/octocat/hello-world/pulls/7", func(w http.ResponseWriter, _ *http.Request) {
		writeSinglePR(t, w, 7, "open", "suite-sha")
	})
	event := durableCheckSuiteEvent(t, "suite-sha", 7)
	event.Repository = "OctoCat/Hello-World"

	retry, err := h.processDurableCheckSuite(t.Context(), event)

	require.NoError(t, err)
	require.False(t, retry)
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, synthesizedDeliveryGUID("octocat/hello-world", 7, "suite-sha"))
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, "octocat/hello-world", row.Repository)
}

// Every payload PR still open at the suite head gets its own PR-scoped
// recovery row — multiple PRs can share one head SHA.
func TestDurableCheckSuiteSynthesizesPerPR(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h, mux := newCheckSuiteProcessHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})
	for _, pr := range []string{"7", "8"} {
		mux.HandleFunc("/repos/octocat/hello-world/pulls/"+pr, func(w http.ResponseWriter, r *http.Request) {
			num := 7
			if strings.HasSuffix(r.URL.Path, "/8") {
				num = 8
			}
			writeSinglePR(t, w, num, "open", "suite-sha")
		})
	}

	retry, err := h.processDurableCheckSuite(t.Context(), durableCheckSuiteEvent(t, "suite-sha", 7, 8))

	require.NoError(t, err)
	require.False(t, retry)
	for _, pr := range []int{7, 8} {
		row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, synthesizedDeliveryGUID("octocat/hello-world", pr, "suite-sha"))
		require.NoError(t, err)
		require.NotNil(t, row, "PR %d must get its own recovery row", pr)
	}
}

// The payload PR list is a point-in-time snapshot and the grace means
// minutes have passed: a PR that closed or moved to a newer head is skipped,
// because a fresher signal owns its new state.
func TestDurableCheckSuiteSkipsClosedAndMovedPRs(t *testing.T) {
	tests := []struct {
		name  string
		state string
		head  string
	}{
		{name: "closed PR", state: "closed", head: "suite-sha"},
		{name: "moved head", state: "open", head: "newer-sha"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newRecordingWebhookEventStore()
			h, mux := newCheckSuiteProcessHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})
			mux.HandleFunc("/repos/octocat/hello-world/pulls/7", func(w http.ResponseWriter, _ *http.Request) {
				writeSinglePR(t, w, 7, tt.state, tt.head)
			})

			retry, err := h.processDurableCheckSuite(t.Context(), durableCheckSuiteEvent(t, "suite-sha", 7))

			require.NoError(t, err)
			require.False(t, retry)
			row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, synthesizedDeliveryGUID("octocat/hello-world", 7, "suite-sha"))
			require.NoError(t, err)
			require.Nil(t, row)
		})
	}
}

// Fork heads arrive with an empty payload PR list and a null head_branch, so
// processing falls back to walking the repository's open PRs and matching by
// current head SHA.
func TestDurableCheckSuiteEmptyPayloadFallsBackToOpenPRScan(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h, mux := newCheckSuiteProcessHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})
	mux.HandleFunc("/repos/octocat/hello-world/pulls", func(w http.ResponseWriter, _ *http.Request) {
		writeOpenPRs(t, w,
			openPR(7, "suite-sha", time.Now().Add(-time.Hour)),
			openPR(8, "other-sha", time.Now().Add(-time.Hour)),
		)
	})

	retry, err := h.processDurableCheckSuite(t.Context(), durableCheckSuiteForkEvent(t, "suite-sha"))

	require.NoError(t, err)
	require.False(t, retry)
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, synthesizedDeliveryGUID("octocat/hello-world", 7, "suite-sha"))
	require.NoError(t, err)
	require.NotNil(t, row, "the open PR at the suite head must be recovered")
	other, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, synthesizedDeliveryGUID("octocat/hello-world", 8, "other-sha"))
	require.NoError(t, err)
	require.Nil(t, other, "PRs at other heads must be untouched")
}

// A fork suite head that matches no open PR is a clean no-op after the
// listing walk comes back empty.
func TestDurableCheckSuiteNoOpenPRMatchNoOps(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h, mux := newCheckSuiteProcessHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})
	mux.HandleFunc("/repos/octocat/hello-world/pulls", func(w http.ResponseWriter, _ *http.Request) {
		writeOpenPRs(t, w)
	})

	retry, err := h.processDurableCheckSuite(t.Context(), durableCheckSuiteForkEvent(t, "suite-sha"))

	require.NoError(t, err)
	require.False(t, retry)
}

// A fork-head walk that exhausts its page budget before the listing does has
// incomplete PR coverage, not a genuine no-match: a PR matched on a visited
// page is still recovered and the delivery completes without retrying — a
// match beyond the budget is the reconciler's missing-head scan to backstop.
func TestDurableCheckSuiteTruncatedOpenPRWalkStillRecoversVisitedMatches(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h, mux := newCheckSuiteProcessHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})
	h.webhookReconcileMaxPages = 1
	mux.HandleFunc("/repos/octocat/hello-world/pulls", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", fmt.Sprintf(`<%s?page=2>; rel="next"`, r.URL.Path))
		writeOpenPRs(t, w, openPR(7, "suite-sha", time.Now().Add(-time.Hour)))
	})

	retry, err := h.processDurableCheckSuite(t.Context(), durableCheckSuiteForkEvent(t, "suite-sha"))

	require.NoError(t, err)
	require.False(t, retry)
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, synthesizedDeliveryGUID("octocat/hello-world", 7, "suite-sha"))
	require.NoError(t, err)
	require.NotNil(t, row, "a PR matched on a visited page must still be recovered")
}

// Exhausting the page budget while the listing still has pages is truncation,
// not a no-match: the resolver reports the two outcomes distinctly so an
// incomplete walk is never counted as a benign no-open-PR result.
func TestResolveCheckSuitePRsDistinguishesTruncationFromNoMatch(t *testing.T) {
	tests := []struct {
		name          string
		morePages     bool
		wantTruncated bool
	}{
		{name: "budget exhausted with pages remaining", morePages: true, wantTruncated: true},
		{name: "listing exhausted within budget", morePages: false, wantTruncated: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newCheckSuiteProcessHandler(t, newRecordingWebhookEventStore(), map[string]api.RepoConfig{"octocat/hello-world": {}})
			h.webhookReconcileMaxPages = 1
			ghc, clientMux := setupGitHubServer(t)
			client := ghclient.NewInstallationClient(ghc, testLogger())
			clientMux.HandleFunc("/repos/octocat/hello-world/pulls", func(w http.ResponseWriter, r *http.Request) {
				if tt.morePages {
					w.Header().Set("Link", fmt.Sprintf(`<%s?page=2>; rel="next"`, r.URL.Path))
				}
				writeOpenPRs(t, w, openPR(8, "other-sha", time.Now().Add(-time.Hour)))
			})

			var payload checkSuitePayload // a fork head: empty head_branch, no payload PRs
			prs, truncated, retry, err := h.resolveCheckSuitePRs(t.Context(), client, "octocat/hello-world", "suite-sha", payload)

			require.NoError(t, err)
			require.False(t, retry)
			require.Empty(t, prs, "no open PR sits at the suite head on the visited page")
			require.Equal(t, tt.wantTruncated, truncated)
		})
	}
}

// A same-repository suite (non-empty head_branch) whose payload names no PRs
// had no open PR at delivery time, so processing resolves nothing without
// touching the open-PR listing — no route is registered on the fake server,
// so any listing call would surface as an error.
func TestDurableCheckSuiteSameRepoEmptyPRListSkipsOpenPRWalk(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h, _ := newCheckSuiteProcessHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})

	retry, err := h.processDurableCheckSuite(t.Context(), durableCheckSuiteEvent(t, "suite-sha"))

	require.NoError(t, err)
	require.False(t, retry)
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, synthesizedDeliveryGUID("octocat/hello-world", 7, "suite-sha"))
	require.NoError(t, err)
	require.Nil(t, row)
}

// One delivery can cover several PRs at the same head, and each PR's outcome
// is independent: a PR whose auto-plan coverage already exists no-ops while
// an uncovered sibling in the same pass still gets its recovery row.
func TestDurableCheckSuiteMixedCoverageSynthesizesOnlyUncovered(t *testing.T) {
	store := newRecordingWebhookEventStore()
	coveringAutoPlanRow(t, store, 7, "suite-sha")
	h, mux := newCheckSuiteProcessHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})
	for _, pr := range []string{"7", "8"} {
		mux.HandleFunc("/repos/octocat/hello-world/pulls/"+pr, func(w http.ResponseWriter, r *http.Request) {
			num := 7
			if strings.HasSuffix(r.URL.Path, "/8") {
				num = 8
			}
			writeSinglePR(t, w, num, "open", "suite-sha")
		})
	}

	retry, err := h.processDurableCheckSuite(t.Context(), durableCheckSuiteEvent(t, "suite-sha", 7, 8))

	require.NoError(t, err)
	require.False(t, retry)
	covered, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, synthesizedDeliveryGUID("octocat/hello-world", 7, "suite-sha"))
	require.NoError(t, err)
	require.Nil(t, covered, "the covered PR must not get a recovery row")
	uncovered, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, synthesizedDeliveryGUID("octocat/hello-world", 8, "suite-sha"))
	require.NoError(t, err)
	require.NotNil(t, uncovered, "the uncovered sibling must still be recovered")
}

// A malformed check_suite payload cannot be decoded, so the driver
// dead-letters it (no retry) rather than crash-looping the fleet on a
// poison row.
func TestDurableCheckSuiteDriverFailsMalformedTerminally(t *testing.T) {
	store := newScriptedWebhookEventStore(&storage.WebhookEvent{
		Provider:   storage.WebhookProviderGitHub,
		DeliveryID: "delivery-check-suite-malformed",
		Event:      "check_suite",
		Payload:    []byte(`{not json`),
	})
	h := newDurableDriverHandler(t, store, nil, nil)

	h.driveNextDurableWebhook(t.Context(), 0, "test-host/1/webhook-driver-0")

	select {
	case failure := <-store.failedPermanent:
		require.Contains(t, failure.errMsg, "decode durable check_suite delivery")
	default:
		t.Fatal("expected malformed check_suite delivery to be dead-lettered")
	}
	require.Empty(t, store.failed, "a malformed payload is deterministic and must not burn retry budget")
	require.Empty(t, store.completed)
}

// A terminally failed synthesized row does not cover its head; the
// check_suite signal reopens it as a fresh pending delivery so the head gets
// another recovery attempt.
func TestDurableCheckSuiteResynthesizesFailedRecoveryRow(t *testing.T) {
	store := newRecordingWebhookEventStore()
	guid := synthesizedDeliveryGUID("octocat/hello-world", 7, "suite-sha")
	_, err := store.Create(t.Context(), &storage.WebhookEvent{
		Provider:    storage.WebhookProviderGitHub,
		DeliveryID:  guid,
		Event:       "pull_request",
		Action:      webhookReconcileSynthesizedAction,
		Repository:  "octocat/hello-world",
		PullRequest: 7,
		HeadSHA:     "suite-sha",
		State:       storage.WebhookEventFailed,
		Payload:     []byte(`{}`),
	})
	require.NoError(t, err)
	h, mux := newCheckSuiteProcessHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})
	mux.HandleFunc("/repos/octocat/hello-world/pulls/7", func(w http.ResponseWriter, _ *http.Request) {
		writeSinglePR(t, w, 7, "open", "suite-sha")
	})

	retry, err := h.processDurableCheckSuite(t.Context(), durableCheckSuiteEvent(t, "suite-sha", 7))

	require.NoError(t, err)
	require.False(t, retry)
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, guid)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, storage.WebhookEventPending, row.State, "the failed recovery row must be reopened")
}

// GitHub API failures while resolving the suite head are transient: the
// delivery must be retried under its attempt budget, not dropped.
func TestDurableCheckSuiteGitHubFailureRetries(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h, mux := newCheckSuiteProcessHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}})
	mux.HandleFunc("/repos/octocat/hello-world/pulls/7", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})

	retry, err := h.processDurableCheckSuite(t.Context(), durableCheckSuiteEvent(t, "suite-sha", 7))

	require.Error(t, err)
	require.True(t, retry)
}

// Config can change between enqueue and claim: a repository deregistered in
// the interim must not have work synthesized for it.
func TestDurableCheckSuiteRevalidatesAllowlist(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h, _ := newCheckSuiteProcessHandler(t, store, map[string]api.RepoConfig{"octocat/other-repo": {}})

	retry, err := h.processDurableCheckSuite(t.Context(), durableCheckSuiteEvent(t, "suite-sha", 7))

	require.NoError(t, err)
	require.False(t, retry)
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, synthesizedDeliveryGUID("octocat/hello-world", 7, "suite-sha"))
	require.NoError(t, err)
	require.Nil(t, row)
}

// The kill switch must stop synthesis with a restart, including rows already
// sitting in the inbox when the operator flipped it.
func TestDurableCheckSuiteHonorsKillSwitchForQueuedRows(t *testing.T) {
	store := newRecordingWebhookEventStore()
	ghc, _ := setupGitHubServer(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := api.New(&durableWebhookTestStorage{webhookEvents: store}, &api.ServerConfig{
		Repos: map[string]api.RepoConfig{"octocat/hello-world": {}},
	}, nil, logger)
	factory := &fakeClientFactory{client: ghclient.NewInstallationClient(ghc, logger)}
	h := NewHandler(service, factory, nil, logger, WithDurableWebhookDispatch())

	retry, err := h.processDurableCheckSuite(t.Context(), durableCheckSuiteEvent(t, "suite-sha", 7))

	require.NoError(t, err)
	require.False(t, retry)
	row, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, synthesizedDeliveryGUID("octocat/hello-world", 7, "suite-sha"))
	require.NoError(t, err)
	require.Nil(t, row)
}
