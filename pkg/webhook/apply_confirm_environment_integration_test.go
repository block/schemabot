//go:build integration

package webhook

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/storage"
)

const confirmEnvironmentUsersSQL = "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

// seedPendingConfirmation records what a prior `schemabot apply -e <env>`
// leaves behind for this PR: the lock pinned to the plan it posted, and that
// plan rendered for the given environment at the fake PR HEAD.
func seedPendingConfirmation(t *testing.T, svc *api.Service, dbName, planID, environment string) {
	t.Helper()
	require.NoError(t, svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName:  dbName,
		DatabaseType:  "mysql",
		Repository:    "octocat/hello-world",
		PullRequest:   1,
		Owner:         "octocat/hello-world#1",
		PendingPlanID: planID,
	}))
	t.Cleanup(func() {
		// The apply under test may already have released the lock.
		if err := svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql"); !errors.Is(err, storage.ErrLockNotFound) {
			assert.NoError(t, err, "release the pending confirmation lock for %s", dbName)
		}
	})
	seedConfirmationPlan(t, svc, dbName, planID, environment)
}

// seedConfirmationPlan stores the plan a pending confirmation pins: the comment
// the operator reviewed, rendered for the given environment at the fake PR
// HEAD, so apply-confirm can verify which environment it authorizes.
func seedConfirmationPlan(t *testing.T, svc *api.Service, dbName, planID, environment string) {
	t.Helper()
	_, err := svc.Storage().Plans().Create(t.Context(), &storage.Plan{
		PlanIdentifier: planID,
		Database:       dbName,
		DatabaseType:   "mysql",
		Deployment:     dbName,
		Target:         dbName,
		Repository:     "octocat/hello-world",
		PullRequest:    1,
		Environment:    environment,
		HeadSHA:        "abc123",
		CreatedAt:      time.Now(),
	})
	require.NoError(t, err)
}

// sendApplyConfirm posts an apply-confirm comment for this PR against a fake
// GitHub serving the users schema, and returns the fake so the test can read
// the comments the handler posts.
func sendApplyConfirm(t *testing.T, svc *api.Service, dbName, comment string) *planFlowResult {
	t.Helper()
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client := gh.NewClient(nil)
	client.BaseURL = baseURL

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": confirmEnvironmentUsersSQL}, schemabotConfig, dbName)
	h := newE2EHandler(t, svc, client)

	req := buildWebhookRequest(t, webhookPayloadOpts{comment: comment, isPR: true}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	return result
}

// appliesForDatabase returns the applies this PR started against dbName.
func appliesForDatabase(t *testing.T, svc *api.Service, dbName string) []*storage.Apply {
	t.Helper()
	applies, err := svc.Storage().Applies().GetByPR(t.Context(), "octocat/hello-world", 1)
	require.NoError(t, err)
	var matched []*storage.Apply
	for _, a := range applies {
		if a.Database == dbName {
			matched = append(matched, a)
		}
	}
	return matched
}

// `schemabot apply -e staging` left a pending confirmation pinned to the
// staging plan, and the operator then comments
// `schemabot apply-confirm -e production`. The confirmation authorizes the
// staging plan only, so production must not be applied in one step with no
// production plan comment: the command is rejected, nothing is dispatched, and
// the staging confirmation stays pinned so it can still be confirmed.
func TestE2EApplyConfirmRejectsPendingConfirmationForOtherEnvironment(t *testing.T) {
	dbName := "webhook_confirm_other_env"
	svc := setupE2EService(t, dbName)
	configureE2EServiceEnvironments(t, svc, dbName, "production")
	// Staging is clean, so the environment ordering gate alone would let
	// production through; the rejection has to come from the pinned plan.
	seedCheck(t, svc, dbName, "staging", "success")

	stagingPlanID := dbName + "_staging_plan"
	seedPendingConfirmation(t, svc, dbName, stagingPlanID, "staging")

	result := sendApplyConfirm(t, svc, dbName, "schemabot apply-confirm -e production -d "+dbName)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Apply-confirm")
		assert.Contains(t, body, "planned for `staging`, not `production`")
		assert.Contains(t, body, "Nothing was applied")
		assert.Contains(t, body, "schemabot apply-confirm -e staging -d "+dbName)
		assert.Contains(t, body, "schemabot apply -e production -d "+dbName)
		assert.NotContains(t, body, "Schema Change Status", "apply must not have started")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for environment mismatch rejection")
	}

	assert.Empty(t, appliesForDatabase(t, svc, dbName), "no apply may be dispatched for another environment's confirmation")

	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "the staging confirmation must stay pinned")
	assert.Equal(t, stagingPlanID, lock.PendingPlanID)
	assert.Equal(t, "octocat/hello-world#1", lock.Owner)
}

// `schemabot apply -e production` left a pending confirmation pinned to the
// production plan while staging was clean, and the operator comments
// `schemabot apply-confirm -e production`. The environments match and the
// ordering gate passes, so the apply runs against production.
func TestE2EApplyConfirmExecutesPendingConfirmationForSameEnvironment(t *testing.T) {
	dbName := "webhook_confirm_same_env"
	svc := setupE2EService(t, dbName)
	configureE2EServiceEnvironments(t, svc, dbName, "production")
	seedCheck(t, svc, dbName, "staging", "success")
	seedCheck(t, svc, dbName, "production", "action_required")
	seedPendingConfirmation(t, svc, dbName, dbName+"_production_plan", "production")

	result := sendApplyConfirm(t, svc, dbName, "schemabot apply-confirm -e production")

	select {
	case body := <-result.comments:
		hasProgress := strings.Contains(body, "Schema Change Status")
		hasSummary := strings.Contains(body, "Schema Change Applied") || strings.Contains(body, "Schema Change Failed")
		require.True(t, hasProgress || hasSummary, "expected progress or summary comment, got: %s", body[:min(len(body), 200)])
		if hasProgress {
			select {
			case summary := <-result.comments:
				assert.Contains(t, summary, "Schema Change Applied")
			case <-time.After(webhookIntegrationPollDeadline):
				t.Fatal("timed out waiting for summary comment")
			}
		}
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for apply comment")
	}

	applies := appliesForDatabase(t, svc, dbName)
	require.Len(t, applies, 1, "the matching confirmation must dispatch exactly one apply")
	assert.Equal(t, "production", applies[0].Environment)
	assert.Equal(t, "mysql", applies[0].DatabaseType)

	require.Eventually(t, func() bool {
		check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "production", "mysql", dbName)
		return err == nil && check != nil && check.Conclusion == "success"
	}, webhookIntegrationPollDeadline, 200*time.Millisecond, "the production check must pass once the confirmed apply completes")
}

// `schemabot apply -e production` left a pending confirmation pinned to the
// production plan, but staging has pending changes again by the time the
// operator comments `schemabot apply-confirm -e production` (the PR HEAD did
// not move). The confirm re-runs the environment ordering gate, so production
// is blocked until staging is applied, and the production confirmation stays
// pinned.
func TestE2EApplyConfirmBlockedWhenPriorEnvironmentNoLongerClean(t *testing.T) {
	dbName := "webhook_confirm_prior_env"
	svc := setupE2EService(t, dbName)
	configureE2EServiceEnvironments(t, svc, dbName, "production")
	seedCheck(t, svc, dbName, "staging", "action_required")

	productionPlanID := dbName + "_production_plan"
	seedPendingConfirmation(t, svc, dbName, productionPlanID, "production")

	result := sendApplyConfirm(t, svc, dbName, "schemabot apply-confirm -e production")

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Apply Blocked")
		assert.Contains(t, body, "Staging")
		assert.Contains(t, body, "schemabot apply -e staging")
		assert.NotContains(t, body, "Schema Change Status", "apply must not have started")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for environment ordering block")
	}

	assert.Empty(t, appliesForDatabase(t, svc, dbName), "production must not be applied before staging is clean")

	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "the production confirmation must stay pinned")
	assert.Equal(t, productionPlanID, lock.PendingPlanID)
}
