package webhook

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
)

func serveUnrequiredTarget(t *testing.T, mux *http.ServeMux) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/rules/branches/", func(w http.ResponseWriter, _ *http.Request) {
		_, err := fmt.Fprint(w, `[]`)
		require.NoError(t, err)
	})
	mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, r *http.Request) {
		var query struct {
			Variables map[string]string `json:"variables"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&query))
		branch := query.Variables["ref"][len("refs/heads/"):]
		_, err := fmt.Fprintf(w, `{"data":{"repository":{"ref":{"name":%q,"branchProtectionRule":null}}}}`, branch)
		require.NoError(t, err)
	})
}

func serveMutablePRTarget(t *testing.T, mux *http.ServeMux, base *atomic.Value) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"head":  map[string]string{"sha": "same-head", "ref": "topic"},
			"base":  map[string]any{"ref": base.Load().(string), "repo": map[string]string{"default_branch": "trunk"}},
			"state": "open",
		}))
	})
}

// All discovery and publication entry points stay silent on an excluded PR.
// Prior passing results survive, and no exclusion success or pending reset is written.
func TestExcludedPRSkipsDiscoveryAndPublication(t *testing.T) {
	for _, role := range []string{"", api.AggregateRoleLeader, api.AggregateRoleParticipant} {
		t.Run("role="+role, func(t *testing.T) {
			checks := &foldCheckStore{byPR: []*storage.Check{{HeadSHA: "same-head", Conclusion: checkConclusionSuccess, CheckRunID: 123}}}
			cfg := &api.ServerConfig{Repos: map[string]api.RepoConfig{"octocat/hello-world": {Aggregate: &api.AggregateConfig{Role: role}}}}
			h, mux, client := newFoldHandler(t, cfg, checks)
			var base atomic.Value
			base.Store("parent")
			serveMutablePRTarget(t, mux, &base)
			serveUnrequiredTarget(t, mux)
			var discovery, writes atomic.Int32
			mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
				discovery.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
			})
			mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, _ *http.Request) {
				writes.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
			})
			for _, source := range []string{"pull_request", "check_run.rerequested", "checks.backfill"} {
				message, err := h.runAutoPlanForPR(t.Context(), client, "octocat/hello-world", 1, "same-head", 12345, source, "edited", "", "")
				require.NoError(t, err)
				assert.Equal(t, "skipped: PR target is excluded", message)
			}
			followUp, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "same-head")
			require.NoError(t, err)
			assert.Equal(t, aggregateFoldNoFollowUp, followUp)
			h.postPassingAggregates(t.Context(), client, "octocat/hello-world", 1, "same-head", nil)
			assert.False(t, h.postFailingAggregatesWithBlock(t.Context(), client, "octocat/hello-world", 1, "same-head", map[string]string{"staging": "failed"}, checkBlockReason{}))
			assert.Equal(t, int32(0), discovery.Load())
			assert.Equal(t, int32(0), writes.Load())
			assert.Equal(t, 0, checks.upsertCalls)
			assert.Equal(t, checkConclusionSuccess, checks.byPR[0].Conclusion)
		})
	}
}

// Commands queued before retargeting are processed against the current base.
// Exclusion rejects plan/apply/apply-confirm with an explanation before discovery.
func TestExcludedPRCommandsRejectBeforeDiscovery(t *testing.T) {
	for _, command := range []string{action.Plan, "plan-all", action.Apply, action.ApplyConfirm} {
		t.Run(command, func(t *testing.T) {
			h, mux, _ := newFoldHandler(t, &api.ServerConfig{}, &foldCheckStore{})
			var base atomic.Value
			base.Store("parent")
			serveMutablePRTarget(t, mux, &base)
			serveUnrequiredTarget(t, mux)
			comments := make(chan string, 2)
			mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))
			switch command {
			case action.Plan:
				h.handlePlanCommand(httptest.NewRecorder(), "octocat/hello-world", 1, "staging", "", "", "", 12345, "", "alice", 0)
			case "plan-all":
				h.handleMultiEnvPlan("octocat/hello-world", 1, "", "", 12345, "alice", false, 1, true, 0, nil)
			case action.Apply:
				retry, err := h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "alice", CommandResult{SuppressRetryComments: true})
				require.NoError(t, err)
				assert.False(t, retry)
			case action.ApplyConfirm:
				retry, err := h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", "", 12345, "alice", CommandResult{SuppressRetryComments: true})
				require.NoError(t, err)
				assert.False(t, retry)
			}
			body := requireComment(t, comments, "excluded target rejection")
			assert.Contains(t, body, "This PR targets `parent`")
			assert.Contains(t, body, "Retarget it to `trunk`")
		})
	}
}

// Publication and the final acceptance gate bypass an earlier cached eligible
// snapshot. A same-SHA return becomes eligible through the normal gate again.
func TestPRTargetRechecksUncachedAndAllowsSameSHAReturn(t *testing.T) {
	h, mux, client := newFoldHandler(t, &api.ServerConfig{}, &foldCheckStore{})
	var base atomic.Value
	base.Store("trunk")
	serveMutablePRTarget(t, mux, &base)
	serveUnrequiredTarget(t, mux)
	comments := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))
	ctx := ghclient.WithPRInfoCache(t.Context())
	_, err := client.FetchPullRequest(ctx, "octocat/hello-world", 1)
	require.NoError(t, err)
	base.Store("parent")
	blocked, err := h.enforcePRTarget(ctx, client, "octocat/hello-world", 1, 12345, action.ApplyConfirm, "staging", "alice", false, false)
	require.NoError(t, err)
	assert.True(t, blocked)
	current, err := h.verifyHeadSHACurrency(ctx, client, "octocat/hello-world", 1, "same-head", "test")
	require.ErrorIs(t, err, errPRTargetExcluded)
	assert.False(t, current)
	base.Store("trunk")
	blocked, err = h.enforcePRTarget(ctx, client, "octocat/hello-world", 1, 12345, action.ApplyConfirm, "staging", "alice", false, false)
	require.NoError(t, err)
	assert.False(t, blocked)
	current, err = h.verifyHeadSHACurrency(ctx, client, "octocat/hello-world", 1, "same-head", "test")
	require.NoError(t, err)
	assert.True(t, current)
}

// An old excluded-head delivery cannot hide a lost retarget notification. Native
// stack target changes and protection-policy changes also change recovery identity.
func TestRecoveryReevaluatesTargetAtSameHead(t *testing.T) {
	for _, change := range []string{"ordinary retarget", "native stack", "release requirement"} {
		t.Run(change, func(t *testing.T) {
			store := newRecordingWebhookEventStore()
			h, mux := newReconcileTestHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}}, WithWebhookReconcileSynthesis())
			serveUnrequiredTarget(t, mux)
			prior := &ghclient.PullRequestInfo{HeadSHA: "same-head", BaseRef: "parent", DefaultBranch: "main"}
			oldDecision := ghclient.PRTargetEligibility{Target: "parent"}
			if change == "native stack" {
				prior.NativeStack = true
				prior.StackBaseRef = "feature"
			}
			if change == "release requirement" {
				prior.BaseRef = "release"
				oldDecision.Target = "release"
			}
			require.NoError(t, h.recordPRTargetObservation(t.Context(), "octocat/hello-world", 7, prior, oldDecision))
			_, err := store.Create(t.Context(), &storage.WebhookEvent{Provider: storage.WebhookProviderGitHub, DeliveryID: "old", Event: "pull_request", Action: "opened", Repository: "octocat/hello-world", PullRequest: 7, HeadSHA: "same-head", Payload: []byte(`{}`)})
			require.NoError(t, err)
			listing := openPR(7, "same-head", time.Now().Add(-time.Hour))
			if change == "release requirement" {
				listing["base"] = map[string]any{"ref": "release", "repo": map[string]string{"default_branch": "main"}}
				mux.HandleFunc("GET /repos/octocat/hello-world/rules/branches/release", func(w http.ResponseWriter, _ *http.Request) {
					_, err := fmt.Fprint(w, `[{"type":"required_status_checks","parameters":{"required_status_checks":[{"context":"SchemaBot (production)"}]}}]`)
					require.NoError(t, err)
				})
			}
			if change == "native stack" {
				listing["base"] = map[string]any{"ref": "parent", "repo": map[string]string{"default_branch": "main"}}
				listing["stack"] = map[string]any{"base": map[string]string{"ref": "main"}}
			}
			mux.HandleFunc("GET /repos/octocat/hello-world/pulls", func(w http.ResponseWriter, _ *http.Request) { writeOpenPRs(t, w, listing) })
			scanned, missing, synthesized := h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")
			assert.Equal(t, 1, scanned)
			assert.Equal(t, 1, missing)
			assert.Equal(t, 1, synthesized)
		})
	}
}

// Native stack edits reevaluate eligibility even when the direct base and head
// remain unchanged. A title-only edit does not trigger schema discovery.
func TestNativeStackEditTriggersEvaluation(t *testing.T) {
	var payload pullRequestPayload
	require.NoError(t, json.Unmarshal([]byte(`{"action":"edited","changes":{"stack":{"base":{"ref":{"from":"parent"}}}}}`), &payload))
	assert.True(t, isAutoPlannablePullRequest(payload))
	// Decode into a new payload: omitted JSON fields must not retain the prior event.
	var titleOnly pullRequestPayload
	require.NoError(t, json.Unmarshal([]byte(`{"action":"edited","changes":{"title":{"from":"old title"}}}`), &titleOnly))
	assert.False(t, isAutoPlannablePullRequest(titleOnly))
}

// A retarget between discovery and publication discards the older scope even
// when the new branch is also eligible at the same head SHA.
func TestPublicationRejectsMovedEligibleTarget(t *testing.T) {
	h, mux, client := newFoldHandler(t, &api.ServerConfig{}, &foldCheckStore{})
	var base atomic.Value
	base.Store("trunk")
	serveMutablePRTarget(t, mux, &base)
	original := &ghclient.PullRequestInfo{HeadSHA: "same-head", BaseRef: "release", DefaultBranch: "trunk"}
	ctx := withExpectedPRTarget(t.Context(), "octocat/hello-world", 1, original)
	current, err := h.verifyHeadSHACurrency(ctx, client, "octocat/hello-world", 1, "same-head", "test")
	require.NoError(t, err)
	assert.False(t, current)
}

// A completed organic delivery for an earlier target cannot cover a later
// target's exhausted synthesized delivery. Recovery reopens the failed row.
func TestTargetRecoveryRetriesDespiteEarlierOrganicHeadCoverage(t *testing.T) {
	store := newRecordingWebhookEventStore()
	h, mux := newReconcileTestHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}}, WithWebhookReconcileSynthesis())
	_, err := store.Create(t.Context(), &storage.WebhookEvent{Provider: storage.WebhookProviderGitHub, DeliveryID: "old-target", Event: "pull_request", Action: "opened", Repository: "octocat/hello-world", PullRequest: 7, HeadSHA: "same-head", State: storage.WebhookEventCompleted, Payload: []byte(`{}`)})
	require.NoError(t, err)
	info := &ghclient.PullRequestInfo{HeadSHA: "same-head", BaseRef: "main", DefaultBranch: "main"}
	decision := ghclient.PRTargetEligibility{Target: "main", Eligible: true}
	guid := synthesizedDeliveryGUID("octocat/hello-world", 7, "same-head", prTargetObservation(info, decision))
	_, err = store.Create(t.Context(), &storage.WebhookEvent{Provider: storage.WebhookProviderGitHub, DeliveryID: guid, Event: "pull_request", Action: "synchronize", Repository: "octocat/hello-world", PullRequest: 7, HeadSHA: "same-head", State: storage.WebhookEventFailed, Payload: []byte(`{}`)})
	require.NoError(t, err)
	require.NoError(t, h.recordPRTargetObservation(t.Context(), "octocat/hello-world", 7, info, decision, guid))
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls", func(w http.ResponseWriter, _ *http.Request) {
		writeOpenPRs(t, w, openPR(7, "same-head", time.Now().Add(-time.Hour)))
	})
	scanned, missing, synthesized := h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")
	assert.Equal(t, 1, scanned)
	assert.Equal(t, 1, missing)
	assert.Equal(t, 1, synthesized)
	reopened, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, guid)
	require.NoError(t, err)
	require.NotNil(t, reopened)
	assert.Equal(t, storage.WebhookEventPending, reopened.State)
}

// A completed or permanently failed recovery from an earlier enforced-target
// visit cannot hide a lost return notification at the same SHA. Its old outcome
// is preserved while a fresh recovery is queued for the new visit.
func TestRecoveryReevaluatesSameSHARoundTrip(t *testing.T) {
	for _, previousState := range []string{storage.WebhookEventCompleted, storage.WebhookEventFailedPermanent} {
		t.Run(previousState, func(t *testing.T) {
			store := newRecordingWebhookEventStore()
			h, mux := newReconcileTestHandler(t, store, map[string]api.RepoConfig{"octocat/hello-world": {}}, WithWebhookReconcileSynthesis())
			main := &ghclient.PullRequestInfo{HeadSHA: "same-head", BaseRef: "main", DefaultBranch: "main"}
			mainDecision := ghclient.PRTargetEligibility{Target: "main", Eligible: true}
			oldGUID := synthesizedDeliveryGUID("octocat/hello-world", 7, "same-head", prTargetObservation(main, mainDecision))
			_, err := store.Create(t.Context(), &storage.WebhookEvent{Provider: storage.WebhookProviderGitHub, DeliveryID: oldGUID, Event: "pull_request", Action: "synchronize", Repository: "octocat/hello-world", PullRequest: 7, HeadSHA: "same-head", State: previousState, Payload: []byte(`{}`)})
			require.NoError(t, err)
			require.NoError(t, h.recordPRTargetObservation(t.Context(), "octocat/hello-world", 7, main, mainDecision, oldGUID))
			excluded := &ghclient.PullRequestInfo{HeadSHA: "same-head", BaseRef: "parent", DefaultBranch: "main"}
			require.NoError(t, h.recordPRTargetObservation(t.Context(), "octocat/hello-world", 7, excluded, ghclient.PRTargetEligibility{Target: "parent"}, "excluded-delivery"))
			mux.HandleFunc("GET /repos/octocat/hello-world/pulls", func(w http.ResponseWriter, _ *http.Request) {
				writeOpenPRs(t, w, openPR(7, "same-head", time.Now().Add(-time.Hour)))
			})
			scanned, missing, synthesized := h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")
			assert.Equal(t, 1, scanned)
			assert.Equal(t, 1, missing)
			assert.Equal(t, 1, synthesized, "the return must get a fresh evaluation context")
			old, err := store.GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub, oldGUID)
			require.NoError(t, err)
			assert.Equal(t, previousState, old.State, "retargeting preserves the earlier outcome")
			_, missing, synthesized = h.reconcileRepoWebhookInbox(t.Context(), store, "octocat/hello-world")
			assert.Zero(t, missing, "the queued recovery covers the new visit")
			assert.Zero(t, synthesized)
		})
	}
}
