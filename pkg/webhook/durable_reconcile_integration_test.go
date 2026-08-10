//go:build integration

package webhook

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// setupSynthesizedDispatchTest builds the fake-GitHub plan harness and a
// durable-dispatch handler, then enqueues a synthesized recovery delivery for
// PR 1 at headSHA exactly as the reconciler does and returns the stored row
// ready to hand to the dispatcher. Each test uses its own head SHA because
// the synthesized delivery GUID is deterministic per (repo, PR, head) and the
// integration storage is shared across tests; the GUID embeds only a
// truncated head prefix, so the SHAs must differ near their start to mint
// distinct GUIDs.
func setupSynthesizedDispatchTest(t *testing.T, dbName, headSHA string) (*Handler, *planFlowResult, *storage.WebhookEvent) {
	t.Helper()
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}
	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)
	result.HeadSHAs = []string{headSHA}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	h := NewHandler(svc, &fakeClientFactory{client: installClient}, nil, logger,
		WithDurableWebhookDispatch(), WithWebhookReconciler(), WithWebhookReconcileSynthesis())

	inserted, resynthesized, err := h.synthesizeMissingHeadDelivery(t.Context(), "octocat/hello-world", 1, headSHA, 12345)
	require.NoError(t, err)
	require.True(t, inserted)
	require.False(t, resynthesized, "first synthesis for a head must not be labeled a resynthesis")

	row, err := svc.Storage().WebhookEvents().GetByDeliveryID(t.Context(), storage.WebhookProviderGitHub,
		synthesizedDeliveryGUID("octocat/hello-world", 1, headSHA))
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, storage.WebhookEventPending, row.State)
	return h, result, row
}

// TestE2EDurableDispatchPlansSynthesizedRecoveryDelivery drives a synthesized
// recovery delivery through the durable dispatcher: for an open PR head that
// was never planned, the minimal pull_request row the reconciler enqueues must
// route through the ordinary auto-plan flow and post the plan comment the lost
// organic delivery would have produced.
func TestE2EDurableDispatchPlansSynthesizedRecoveryDelivery(t *testing.T) {
	dbName := "webhook_synth_dispatch_plan"
	h, result, row := setupSynthesizedDispatchTest(t, dbName, "one-recovered-head-sha")

	retry, err := h.processDurableWebhookEvent(t.Context(), row)
	require.NoError(t, err)
	require.False(t, retry)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "## Schema Change Plan")
		assert.Contains(t, body, dbName)
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the synthesized recovery delivery's plan comment")
	}
	select {
	case cr := <-result.checkRuns:
		assert.Contains(t, cr.Name, "SchemaBot")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the synthesized recovery delivery's check run")
	}
}

// TestE2EDurableDispatchSynthesizedDeliverySkipsCommentWhenPlanCurrent covers
// the head that was planned while its inbox delivery went missing: a tracked
// plan comment already rendered at the same head SHA proves the visible plan
// current, so the recovery dispatch refreshes checks without posting a
// duplicate plan comment.
func TestE2EDurableDispatchSynthesizedDeliverySkipsCommentWhenPlanCurrent(t *testing.T) {
	dbName := "webhook_synth_dispatch_current"
	h, result, row := setupSynthesizedDispatchTest(t, dbName, "two-recovered-head-sha")
	require.NoError(t, h.service.Storage().PlanComments().Insert(t.Context(), &storage.PlanComment{
		Repository:       "octocat/hello-world",
		PullRequest:      1,
		DatabaseName:     dbName,
		DatabaseType:     "mysql",
		EnvironmentScope: "staging",
		HeadSHA:          "two-recovered-head-sha",
		GitHubCommentID:  98,
		GitHubNodeID:     "IC_98",
	}))

	retry, err := h.processDurableWebhookEvent(t.Context(), row)
	require.NoError(t, err)
	require.False(t, retry)

	select {
	case cr := <-result.checkRuns:
		assert.Contains(t, cr.Name, "SchemaBot")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the recovery delivery's refreshed check run")
	}
	select {
	case body := <-result.comments:
		t.Fatalf("expected no duplicate plan comment for an already-planned head, got: %s", body)
	case <-time.After(3 * time.Second):
	}
}
