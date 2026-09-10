//go:build integration

package webhook

import (
	"context"
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

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
)

// TestE2EFirstApplyToBlankDatabaseThroughNonceEdit covers the first apply to
// a database whose declarative schema directory is already merged: the live
// database holds no tables and the schema files sit on the default branch. A PR
// that changes nothing under the schema directory gets no plan, only a comment
// that says nothing was compared and how to get a plan. A nonce edit to the
// database's schemabot.yaml is that way: the PR now touches the schema
// directory, so the plan covers the whole directory against the live schema,
// shows the CREATE TABLE for a file the PR did not touch, and the apply creates
// that table on the blank database.
func TestE2EFirstApplyToBlankDatabaseThroughNonceEdit(t *testing.T) {
	dbName := "webhook_first_apply_nonce_db"
	svc := setupE2EService(t, dbName)

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	requireNoTable(t, dbName, "users")

	t.Run("a PR outside the schema directory gets no plan and the nonce hint", func(t *testing.T) {
		prFiles := []*gh.CommitFile{{Filename: new("README.md"), Status: new("modified")}}
		h, result := newFirstApplyRepo(t, svc, dbName, schemaFiles, schemabotConfig, prFiles)

		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot plan -e staging", isPR: true}, nil))
		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "no managed schema changes handled")

		body := requireNextComment(t, result, "no schema files changed comment")
		assert.Contains(t, body, "No Schema Files Changed")
		assert.Contains(t, body, "refreshed as passing")
		assert.Contains(t, body, "no database was compared against its schema directory")
		assert.Contains(t, body, "`# nonce`")
		assert.NotContains(t, body, "CREATE TABLE")
		requireNoTable(t, dbName, "users")
	})

	t.Run("a nonce edit to schemabot.yaml plans and applies the whole directory", func(t *testing.T) {
		nonceConfig := schemabotConfig + "# nonce\n"
		prFiles := []*gh.CommitFile{{Filename: new("schema/" + ghclient.ConfigFileName), Status: new("modified")}}
		h, result := newFirstApplyRepo(t, svc, dbName, schemaFiles, nonceConfig, prFiles)

		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot plan -e staging", isPR: true}, nil))
		require.Equal(t, http.StatusOK, rr.Code)

		body := requireNextComment(t, result, "plan comment for the nonce PR")
		assert.Contains(t, body, "## Schema Change Plan")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, "`users`")
		assert.Contains(t, body, dbName)
		assert.NotContains(t, body, "No Schema Files Changed")

		rr = httptest.NewRecorder()
		h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot apply -e staging", isPR: true}, nil))
		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "apply started")

		body = requireNextComment(t, result, "apply comment for the nonce PR")
		assert.Contains(t, body, "## Schema Change Apply")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, dbName)

		requireTableEventually(t, dbName, "users")
	})
}

// newFirstApplyRepo serves a fake GitHub repository whose schema directory
// holds the given files and config at both the PR head and the base branch,
// with prFiles as the PR's changed files, and returns a handler wired to it.
func newFirstApplyRepo(t *testing.T, svc *api.Service, dbName string, schemaFiles map[string]string, schemabotConfig string, prFiles []*gh.CommitFile) (*Handler, *planFlowResult) {
	t.Helper()
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	result := setupFakeGitHubForPlanWithPRFiles(t, mux, schemaFiles, schemabotConfig, dbName, prFiles)
	for name := range schemaFiles {
		result.baseHolds(schemaFixturePath(dbName, name))
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}
	return NewHandler(svc, factory, nil, logger), result
}

// requireNextComment returns the next comment the fake GitHub received, failing
// the test when none lands within the integration polling deadline.
func requireNextComment(t *testing.T, result *planFlowResult, what string) string {
	t.Helper()
	select {
	case body := <-result.comments:
		return body
	case <-time.After(webhookIntegrationPollDeadline):
		require.FailNow(t, "timed out waiting for "+what)
		return ""
	}
}

// Every database call in the polling helpers gets its own short deadline so an
// unreachable MySQL fails the attempt instead of holding the test past the
// polling deadline.
const targetQueryTimeout = 5 * time.Second

// countTable reports how many tables named table exist in dbName. It returns
// errors instead of asserting so it can run inside an Eventually condition,
// which testify executes off the test goroutine.
func countTable(t *testing.T, dbName, table string) (int, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), targetQueryTimeout)
	defer cancel()
	db, err := sql.Open("block-mysql", driftDSN(t, dbName))
	if err != nil {
		return 0, fmt.Errorf("open target %s: %w", dbName, err)
	}
	defer utils.CloseAndLog(db)
	if err := db.PingContext(ctx); err != nil {
		return 0, fmt.Errorf("connect to target %s: %w", dbName, err)
	}
	var count int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		dbName, table).Scan(&count); err != nil {
		return 0, fmt.Errorf("count table %s.%s: %w", dbName, table, err)
	}
	return count, nil
}

func requireNoTable(t *testing.T, dbName, table string) {
	t.Helper()
	count, err := countTable(t, dbName, table)
	require.NoError(t, err)
	require.Zero(t, count, "the database has no %s table", table)
}

// requireTableEventually polls the live database until the apply has created
// the table. A probe error fails the test at once rather than being retried
// until the polling deadline.
func requireTableEventually(t *testing.T, dbName, table string) {
	t.Helper()
	var probeErr error
	require.Eventually(t, func() bool {
		count, err := countTable(t, dbName, table)
		if err != nil {
			probeErr = err
			return true
		}
		return count == 1
	}, webhookIntegrationPollDeadline, 200*time.Millisecond, "the apply creates %s.%s", dbName, table)
	require.NoError(t, probeErr, "probe the target while waiting for %s.%s", dbName, table)
}
