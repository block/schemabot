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

	"github.com/block/spirit/pkg/utils"
	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
)

// TestE2EFirstApplyToBlankDatabaseThroughNamedDatabase covers the first apply
// to a database whose declarative schema root is already merged: the live
// database holds no tables, the schema files sit on the default branch, and the
// PR touches nothing under the schema root. The PR diff is only what triggers
// an unscoped plan, so that plan reports no managed schema changes and tells
// the user how to ask for the root anyway. Naming the database is that ask:
// discovery finds the config in the repository, the desired schema is the whole
// root at the PR head, and the apply creates the tables on the blank database.
func TestE2EFirstApplyToBlankDatabaseThroughNamedDatabase(t *testing.T) {
	dbName := "webhook_first_apply_named_db"
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
	// The PR changes a file outside the schema root; the root itself is
	// already on the default branch at the same content the head carries.
	prFiles := []*gh.CommitFile{{Filename: new("README.md"), Status: new("modified")}}
	result := setupFakeGitHubForPlanWithPRFiles(t, mux, schemaFiles, schemabotConfig, dbName, prFiles)
	result.baseHolds(schemaFixturePath(dbName, "users.sql"))

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}
	h := NewHandler(svc, factory, nil, logger)

	requireNoTable(t, dbName, "users")

	t.Run("unscoped plan reports no managed schema changes and names the way in", func(t *testing.T) {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot plan -e staging", isPR: true}, nil))
		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "no managed schema changes handled")

		body := requireNextComment(t, result, "no managed schema changes comment")
		assert.Contains(t, body, "No Managed Schema Changes")
		assert.Contains(t, body, "refreshed as passing")
		assert.Contains(t, body, "already merged")
		assert.Contains(t, body, "schemabot plan -d <database>")
	})

	t.Run("plan naming the database plans the whole schema root", func(t *testing.T) {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot plan -d " + dbName, isPR: true}, nil))
		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "multi-env plan started")

		body := requireNextComment(t, result, "plan comment for the named database")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, "`users`")
		assert.Contains(t, body, dbName)
		assert.NotContains(t, body, "No Managed Schema Changes")
		assert.NotContains(t, body, "Database Not Found")
	})

	t.Run("apply naming the database creates the tables on the blank database", func(t *testing.T) {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot apply -e staging -d " + dbName, isPR: true}, nil))
		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "apply started")

		body := requireNextComment(t, result, "apply comment for the named database")
		assert.Contains(t, body, "## Schema Change Apply")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, dbName)

		requireTableEventually(t, dbName, "users")
	})
}

// requireNextComment returns the next comment the fake GitHub received, failing
// the test when none lands within the integration polling deadline.
func requireNextComment(t *testing.T, result *planFlowResult, what string) string {
	t.Helper()
	select {
	case body := <-result.comments:
		return body
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatalf("timed out waiting for %s", what)
		return ""
	}
}

func countTable(t *testing.T, dbName, table string) int {
	t.Helper()
	db, err := sql.Open("block-mysql", driftDSN(t, dbName))
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(t.Context()), "connect to target")
	var count int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		dbName, table).Scan(&count))
	return count
}

func requireNoTable(t *testing.T, dbName, table string) {
	t.Helper()
	require.Zero(t, countTable(t, dbName, table), "the database starts without %s", table)
}

// requireTableEventually polls the live database until the apply has created
// the table, failing at the integration polling deadline.
func requireTableEventually(t *testing.T, dbName, table string) {
	t.Helper()
	deadline := time.Now().Add(webhookIntegrationPollDeadline)
	for time.Now().Before(deadline) {
		if countTable(t, dbName, table) == 1 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for the apply to create %s.%s", dbName, table)
}
