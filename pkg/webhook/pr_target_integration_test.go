//go:build integration

package webhook

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
)

// A command that observes an excluded target stops before changing an existing
// confirmation. Normal freshness and replanning rules still apply on return.
func TestExcludedPRPreservesConfirmation(t *testing.T) {
	database := "webhook_target_confirmation"
	svc := setupE2EService(t, database)
	svc.Config().EnforcePRTargets = true
	client, mux := setupGitHubServer(t)
	var base atomic.Value
	base.Store("parent")
	serveMutablePRTarget(t, mux, &base)
	serveUnrequiredTarget(t, mux)
	comments := make(chan string, 4)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, comments))
	h := NewHandler(svc, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}, nil, testLogger())
	plan := &storage.Plan{PlanIdentifier: "plan-preserved-target", CreatedAt: time.Now().UTC(), Database: database, DatabaseType: "mysql", Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: "same-head", Environment: "staging"}
	_, err := svc.Storage().Plans().Create(t.Context(), plan)
	require.NoError(t, err)
	require.NoError(t, svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{DatabaseName: database, DatabaseType: "mysql", Owner: "octocat/hello-world#1", Repository: "octocat/hello-world", PullRequest: 1, PendingPlanID: plan.PlanIdentifier}))
	for _, confirm := range []bool{false, true} {
		var retry bool
		if confirm {
			retry, err = h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", database, 12345, "alice", CommandResult{})
		} else {
			retry, err = h.applyCommandCore(t.Context(), "octocat/hello-world", 1, "staging", database, 12345, "alice", CommandResult{})
		}
		require.NoError(t, err)
		assert.False(t, retry)
	}
	lock, err := svc.Storage().Locks().Get(t.Context(), database, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, plan.PlanIdentifier, lock.PendingPlanID)
	assert.Equal(t, "octocat/hello-world#1", lock.Owner)
	preserved, err := svc.Storage().Plans().Get(t.Context(), plan.PlanIdentifier)
	require.NoError(t, err)
	require.NotNil(t, preserved)
	assert.Equal(t, "same-head", preserved.HeadSHA)
	applies, err := svc.Storage().Applies().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	assert.Empty(t, applies, "excluded commands durably accept no applies")
}

// An accepted apply records its terminal outcome during exclusion, while its
// callback leaves the GitHub Check Run unchanged. Retargeting does not cancel
// pending accepted work or discard an outcome needed by later reconciliation.
func TestExcludedPRRecordsAcceptedApplyOutcomeWithoutPublishing(t *testing.T) {
	database := "webhook_target_outcome"
	svc := setupE2EService(t, database)
	svc.Config().EnforcePRTargets = true
	client, mux := setupGitHubServer(t)
	var base atomic.Value
	base.Store("parent")
	serveMutablePRTarget(t, mux, &base)
	serveUnrequiredTarget(t, mux)
	var writes atomic.Int32
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		writes.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	mux.HandleFunc("PATCH /repos/octocat/hello-world/check-runs/123", func(w http.ResponseWriter, _ *http.Request) {
		writes.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	h := NewHandler(svc, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}, nil, testLogger())
	apply := &storage.Apply{ApplyIdentifier: "apply-preserved-target", Database: database, DatabaseType: "mysql", Environment: "staging", Repository: "octocat/hello-world", PullRequest: 1, InstallationID: 12345, State: state.Apply.Pending, Engine: storage.EngineSpirit}
	id, err := svc.Storage().Applies().Create(t.Context(), apply)
	require.NoError(t, err)
	apply.ID = id
	require.NoError(t, svc.Storage().Checks().Upsert(t.Context(), &storage.Check{Repository: apply.Repository, PullRequest: 1, HeadSHA: "same-head", Environment: "staging", DatabaseType: "mysql", DatabaseName: database, CheckRunID: 123, ApplyID: id, Status: checkStatusInProgress, HasChanges: true}))
	message, err := h.runAutoPlanForPR(t.Context(), ghclient.NewInstallationClient(client, testLogger()), apply.Repository, 1, "same-head", 12345, "pull_request", "edited", "", "")
	require.NoError(t, err)
	assert.Equal(t, "skipped: PR target is excluded", message)
	accepted, err := svc.Storage().Applies().Get(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Pending, accepted.State, "accepted work stays queued")
	apply.State = state.Apply.Completed
	require.NoError(t, svc.Storage().Applies().Update(t.Context(), apply))
	h.refreshChecksForTerminalApply(t.Context(), apply, "target test")
	result, err := svc.Storage().Checks().Get(t.Context(), apply.Repository, 1, "staging", "mysql", database)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, checkStatusCompleted, result.Status)
	assert.Equal(t, checkConclusionSuccess, result.Conclusion, "terminal truth is recorded independently of GitHub publication")
	assert.Equal(t, int32(0), writes.Load())
}

// A target can become excluded after the first command gate. Both fresh PR
// reads must reject it before freshness handling or a no-change re-plan can
// release the pending confirmation (MG-13).
func TestRetargetedCommandPreservesConfirmationBeforePlanResponses(t *testing.T) {
	for _, phase := range []string{"confirmation freshness", "apply replan"} {
		t.Run(phase, func(t *testing.T) {
			database := "webhook_target_response_" + strings.ReplaceAll(phase, " ", "_")
			svc := setupE2EService(t, database)
			ddl := "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
			seedTargetTable(t, database, ddl)
			planID := "plan-pending-" + database
			seedPendingConfirmation(t, svc, database, planID, "staging")

			mux := http.NewServeMux()
			var fixture *planFlowResult
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mux.ServeHTTP(w, r)
				if phase == "confirmation freshness" && strings.Contains(r.URL.Path, "/git/blobs/") {
					// Discovery started on main; its next fresh PR read sees exclusion.
					fixture.BaseRef.Store(new("parent"))
				}
			}))
			t.Cleanup(server.Close)
			client := gh.NewClient(nil)
			var err error
			client.BaseURL, err = url.Parse(server.URL + "/")
			require.NoError(t, err)
			fixture = setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": ddl + ";"}, fmt.Sprintf("database: %s\ntype: mysql\n", database), database)
			serveUnrequiredTarget(t, mux)
			var baseReads atomic.Int32
			mux.HandleFunc("GET /repos/octocat/hello-world/git/ref/heads/parent", func(w http.ResponseWriter, _ *http.Request) {
				baseReads.Add(1)
				_, err := fmt.Fprint(w, `{"ref":"refs/heads/parent","object":{"sha":"def456"}}`)
				require.NoError(t, err)
			})
			ic := ghclient.NewInstallationClient(client, testLogger())
			h := NewHandler(svc, &fakeClientFactory{client: ic}, nil, testLogger())
			if phase == "confirmation freshness" {
				retry, err := h.applyConfirmCommandCore(t.Context(), "octocat/hello-world", 1, "staging", database, 12345, "alice", CommandResult{})
				require.NoError(t, err)
				assert.False(t, retry)
			} else {
				schema, err := h.createManagedSchemaRequestFromPR(t.Context(), ic, "octocat/hello-world", 1, "staging", database, action.ApplyConfirm)
				require.NoError(t, err)
				require.NoError(t, h.attachServerEnvironments(schema, "staging"))
				disclosed, err := svc.Storage().Plans().Get(t.Context(), planID)
				require.NoError(t, err)
				fixture.BaseRef.Store(new("parent"))
				h.executeApply(t.Context(), ic, "octocat/hello-world", 1, schema, "staging", 12345, "alice", CommandResult{}, nil, disclosed, planID, nil)
			}

			body := requireComment(t, fixture.comments, "excluded target rejection")
			assert.Contains(t, body, "This PR targets `parent`")
			assert.Zero(t, baseReads.Load(), "eligibility must precede base freshness handling")
			lock, err := svc.Storage().Locks().Get(t.Context(), database, "mysql")
			require.NoError(t, err)
			require.NotNil(t, lock)
			assert.Equal(t, planID, lock.PendingPlanID)
			applies, err := svc.Storage().Applies().GetByPR(t.Context(), "octocat/hello-world", 1)
			require.NoError(t, err)
			assert.Empty(t, applies)
		})
	}
}

// The branch and commit can stay the same while protection changes. Earlier
// gates use a valid cached policy, and the durable gate must get its own answer.
func TestApplyAcceptanceRechecksCachedBranchPolicy(t *testing.T) {
	for _, tc := range []struct {
		name         string
		liveResponse string
		refusal      string
	}{
		{name: "required check removed", liveResponse: `{"data":{"repository":{"ref":{"name":"release","branchProtectionRule":null}}}}`, refusal: "This PR targets `release`"},
		{name: "rules lookup failed", liveResponse: `{"errors":[{"message":"protection denied"}]}`, refusal: "Could not verify the PR target"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database := "webhook_target_policy_apply"
			svc := setupE2EServiceOpts(t, database, e2eServiceOpts{skipOperator: true})
			client, mux := setupGitHubServer(t)
			fixture := setupFakeGitHubForPlan(t, mux, map[string]string{
				"users.sql": "CREATE TABLE `users` (`id` bigint unsigned NOT NULL PRIMARY KEY) ENGINE=InnoDB;",
			}, fmt.Sprintf("database: %s\ntype: mysql\n", database), database)
			fixture.BaseRef.Store(new("release"))
			var rulesReads, classicReads atomic.Int32
			mux.HandleFunc("GET /repos/octocat/hello-world/git/ref/heads/release", func(w http.ResponseWriter, _ *http.Request) {
				_, err := fmt.Fprint(w, `{"ref":"refs/heads/release","object":{"sha":"def456"}}`)
				require.NoError(t, err)
			})
			mux.HandleFunc("GET /repos/octocat/hello-world/rules/branches/release", func(w http.ResponseWriter, _ *http.Request) {
				rulesReads.Add(1)
				_, err := fmt.Fprint(w, "[]")
				require.NoError(t, err)
			})
			mux.HandleFunc("POST /graphql", func(w http.ResponseWriter, _ *http.Request) {
				response := tc.liveResponse
				if classicReads.Add(1) == 1 {
					response = `{"data":{"repository":{"ref":{"name":"release","branchProtectionRule":{"requiresStatusChecks":true,"requiredStatusCheckContexts":["SchemaBot"]}}}}}`
				}
				_, err := fmt.Fprint(w, response)
				require.NoError(t, err)
			})
			ic := ghclient.NewInstallationClient(client, testLogger())
			h := NewHandler(svc, &fakeClientFactory{client: ic}, nil, testLogger())

			ctx := ghclient.WithPRInfoCache(t.Context())
			info, err := ic.FetchPullRequestNoCache(ctx, "octocat/hello-world", 1)
			require.NoError(t, err)
			decision, err := h.evaluatePRTarget(ctx, ic, "octocat/hello-world", info)
			require.NoError(t, err)
			require.True(t, decision.Eligible)

			retry, err := h.applyCommandCore(ctx, "octocat/hello-world", 1, "staging", database, 12345, "alice", CommandResult{})
			require.NoError(t, err)
			assert.False(t, retry)
			prepared := requireComment(t, fixture.comments, "automatic apply plan")
			require.Contains(t, prepared, "Applying automatically")
			refusal := requireComment(t, fixture.comments, "live branch policy refusal")
			assert.Contains(t, refusal, tc.refusal)
			assert.Equal(t, int32(2), rulesReads.Load())
			assert.Equal(t, int32(2), classicReads.Load())
			applies, err := svc.Storage().Applies().GetByPR(ctx, "octocat/hello-world", 1)
			require.NoError(t, err)
			assert.Empty(t, applies, "cached eligibility must never authorize a durable apply")
		})
	}
}
