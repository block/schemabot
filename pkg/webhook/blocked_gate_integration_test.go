//go:build integration

package webhook

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pkSwapSchema is a schema file declaring a composite primary key on a table
// the target holds with a single-column key, so the plan diff is exactly the
// primary-key reshape the schema-change engine refuses.
const pkSwapSchema = "CREATE TABLE `users` (\n" +
	"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
	"  `tenant_id` bigint unsigned NOT NULL,\n" +
	"  PRIMARY KEY (`id`,`tenant_id`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

// seedPKSwapTargetTable creates the pre-change `users` table on the app
// database so the plan produces the refused primary-key reshape.
func seedPKSwapTargetTable(t *testing.T, dbName string) {
	t.Helper()
	db, err := sql.Open("mysql", driftDSN(t, dbName))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	_, err = db.ExecContext(t.Context(), "CREATE TABLE `users` (\n"+
		"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n"+
		"  `tenant_id` bigint unsigned NOT NULL,\n"+
		"  PRIMARY KEY (`id`)\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err, "seed pre-change users table")
}

// An apply on a plan containing an engine-blocked change is rejected before
// any lock is taken: the PR gets a ⛔ rejection comment naming the table and
// the refusal, with no retry or --allow-unsafe coaching, and the database
// lock stays free.
func TestE2EApplyRejectedOnBlockedPlan(t *testing.T) {
	dbName := "webhook_blocked_gate"
	svc := setupE2EService(t, dbName)
	seedPKSwapTargetTable(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": pkSwapSchema}, schemabotConfig, dbName)

	h := newE2EHandler(t, svc, client)
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "⛔ Apply rejected")
		assert.Contains(t, body, "not supported by the schema-change engine")
		assert.Contains(t, body, "`users`")
		assert.NotContains(t, body, "--allow-unsafe", "a guaranteed failure must not coach an unsafe override")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the apply rejection comment")
	}

	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	assert.Nil(t, lock, "a rejected apply must not leave the database locked")
}

// An apply on a plan that routes a change to direct execution is rejected the
// same way as a blocked plan: running native DDL blocks writes to the table
// and is not revertible, so it requires operator consent the PR apply command
// does not collect. The rejection comment names the table and the policy's
// verdict, no unsafe-override coaching appears, and the database lock stays
// free.
func TestE2EApplyRejectedOnDirectPlan(t *testing.T) {
	dbName := "webhook_direct_gate"
	svc := setupE2EServiceOpts(t, dbName, e2eServiceOpts{
		engineMetadata: map[string]string{
			"direct_execution":                "true",
			"direct_execution_max_table_rows": "1000",
		},
	})
	seedPKSwapTargetTable(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": pkSwapSchema}, schemabotConfig, dbName)

	h := newE2EHandler(t, svc, client)
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot apply -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-result.comments:
		assert.Contains(t, body, "⛔ Apply rejected")
		assert.Contains(t, body, "`users`")
		assert.Contains(t, body, "runs as native MySQL DDL")
		assert.NotContains(t, body, "--allow-unsafe", "a consent-gated change must not coach an unsafe override")
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for the apply rejection comment")
	}

	lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
	require.NoError(t, err)
	assert.Nil(t, lock, "a rejected apply must not leave the database locked")
}
