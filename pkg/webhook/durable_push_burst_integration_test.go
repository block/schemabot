//go:build integration

package webhook

import (
	"database/sql"
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

// A rebase force-pushes a PR twice within a few seconds. Both synchronize
// deliveries wait out the quiet period, so when the first is claimed the
// second is already in the inbox naming the first push's head as the head it
// replaced. The first delivery is superseded without a single GitHub request,
// and only the newest head is planned: one plan comment and one Check Run on
// that head.
func TestE2EDurableDispatchPlansOnlyTheNewestHeadOfAPushBurst(t *testing.T) {
	const (
		dbName      = "webhook_push_burst"
		quietPeriod = 2 * time.Second
		firstHead   = "burst-head-1"
		newestHead  = "burst-head-2"
	)
	ctx := t.Context()
	svc := setupE2EService(t, dbName)

	// The inbox is shared across this package's tests, so leftover rows from
	// earlier tests would otherwise be claimed ahead of this test's pushes.
	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = schemabotDB.Close() })
	_, err = schemabotDB.ExecContext(ctx, "DELETE FROM webhook_events")
	require.NoError(t, err)

	mux := http.NewServeMux()
	requests := &githubRequestCounter{counts: map[string]int{}}
	server := httptest.NewServer(requests.wrap(mux))
	t.Cleanup(server.Close)
	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}
	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)
	result.HeadSHAs = []string{newestHead}
	registerFreshBaseComparison(t, mux, newestHead)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	h := NewHandler(svc, &fakeClientFactory{client: installClient}, nil, logger,
		WithDurableWebhookDispatch(), WithSynchronizeQuietPeriod(quietPeriod))

	for _, push := range []struct{ deliveryID, before, head string }{
		{"burst-push-1", "abc123", firstHead},
		{"burst-push-2", firstHead, newestHead},
	} {
		req := buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "synchronize", beforeSHA: push.before, headSHA: push.head}, nil)
		req.Header.Set(headerDeliveryID, push.deliveryID)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		require.Equal(t, http.StatusOK, rr.Code)
	}

	store := svc.Storage().WebhookEvents()
	const owner = "burst-test/webhook-driver-0"
	claimed, err := store.FindNext(ctx, owner, time.Minute)
	require.NoError(t, err)
	require.Nil(t, claimed, "both pushes must wait out the quiet period before a driver can claim them")

	claimNext := func(wantDeliveryID string) *storage.WebhookEvent {
		t.Helper()
		deadline := time.Now().Add(webhookIntegrationPollDeadline)
		for time.Now().Before(deadline) {
			claimed, err := store.FindNext(ctx, owner, time.Minute)
			require.NoError(t, err)
			if claimed != nil {
				require.Equal(t, wantDeliveryID, claimed.DeliveryID)
				return claimed
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s to become claimable", wantDeliveryID)
		return nil
	}

	first := claimNext("burst-push-1")
	before := requests.snapshot()
	h.driveClaimedDurableWebhook(ctx, 0, store, first)
	assert.Equal(t, before, requests.snapshot(), "the superseded push must not cost a GitHub request")
	row, err := store.GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "burst-push-1")
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, storage.WebhookEventSuperseded, row.State)

	newest := claimNext("burst-push-2")
	h.driveClaimedDurableWebhook(ctx, 0, store, newest)
	row, err = store.GetByDeliveryID(ctx, storage.WebhookProviderGitHub, "burst-push-2")
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, storage.WebhookEventCompleted, row.State)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "## Schema Change Plan")
		assert.Contains(t, body, dbName)
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the newest head's plan comment")
	}
	select {
	case cr := <-result.checkRuns:
		assert.Contains(t, cr.Name, "SchemaBot")
		assert.Equal(t, newestHead, cr.HeadSHA, "the Check Run must be published on the newest head")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the newest head's Check Run")
	}
}
