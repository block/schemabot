//go:build integration

package webhook

import (
	"context"
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
)

// TestE2EApplyDowngradesWhenBaseSchemaChanged exercises the base-branch drift
// gate: when the schema directory changed on the base branch since the PR
// diverged, automatic apply is downgraded to manual confirmation — the plan
// comment names the changed base files and tells the operator to update the
// branch or run apply-confirm, and nothing is applied automatically.
func TestE2EApplyDowngradesWhenBaseSchemaChanged(t *testing.T) {
	dbName := "webhook_apply_base_drift"
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
	// Base advanced past the PR's fork point (distinct merge base) and the
	// change touched the resolved schema directory: the merge base serves a
	// different users.sql than the base tip, so the content-addressed schema
	// subtree SHA genuinely differs and the gate detects drift.
	result.MergeBaseSHA = "mergebase111"
	result.TreeContentByCommit = map[string]map[string]string{
		"mergebase111": {
			"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
		},
	}
	result.BaseDriftChangedFiles = []string{"schema/orders.sql"}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// The lock is acquired during the downgrade path; release it so it doesn't
	// leak to other tests sharing the same PR.
	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Automatic apply paused")
		assert.Contains(t, body, "schema files changed on `main`")
		assert.Contains(t, body, "`schema/orders.sql`")
		assert.NotContains(t, body, "Applying automatically")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for base-drift downgrade comment")
	}

	// The apply plan check is recorded as action_required, like other downgrades.
	select {
	case cr := <-result.checkRuns:
		assert.Contains(t, cr.Name, "SchemaBot")
		assert.Equal(t, "completed", cr.Status)
		assert.Equal(t, "action_required", cr.Conclusion)
	case <-time.After(webhookIntegrationCheckRunDeadline):
		t.Fatal("timed out waiting for check run")
	}
}

// TestE2EApplyProceedsWhenBaseAdvancedButSchemaUnchanged exercises the
// path-scoped nature of the base-branch drift gate: when the base branch
// advanced past the PR's fork point (a distinct merge base and base tip) but the
// resolved schema directory is byte-identical at the merge base and the base
// tip, automatic apply proceeds. The gate must not downgrade merely because the
// branch is behind base on unrelated files.
func TestE2EApplyProceedsWhenBaseAdvancedButSchemaUnchanged(t *testing.T) {
	dbName := "webhook_apply_base_no_drift"
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
	// Base advanced (distinct merge base and base tip) but no per-commit content
	// override, so the merge base and base tip serve byte-identical schema
	// content: the content-addressed schema subtree SHAs match and the gate
	// reports no drift.
	result.MergeBaseSHA = "mergebase222"
	result.BaseTipSHA = "basetip333"

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	t.Cleanup(func() {
		_ = svc.Storage().Locks().ForceRelease(context.WithoutCancel(t.Context()), dbName, "mysql")
	})

	select {
	case body := <-result.comments:
		assert.NotContains(t, body, "Automatic apply paused",
			"base advanced on unrelated files must not downgrade automatic apply")
		assert.Contains(t, body, "Applying automatically")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for automatic apply plan comment")
	}
}
