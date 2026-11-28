//go:build integration

package webhook

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
)

// A PR author who is in neither the deployment's admin team nor the
// database's operator team opens a PR that changes `orders`, on a deployment
// that turns on PR command authorization. The auto-plan comment tells them,
// right under its header and before the DDL, that they cannot run the apply
// and which teams can, and its footer does not repeat it. Finding that out
// costs one membership request per configured team, whatever the team's
// size. When GitHub cannot answer the membership question, the plan still
// posts and says only who can run the apply, which stays true.
func TestE2EAutoPlanTellsUnauthorizedAuthorWhoCanApply(t *testing.T) {
	const author = "testuser"
	teams := []string{"schema-admins", "orders-operators"}

	autoPlanComment := func(t *testing.T, dbName string, serveTeams func(mux *http.ServeMux)) string {
		t.Helper()
		svc := setupE2EService(t, dbName)
		config := svc.Config()
		config.PRCommandAuthorization.Enabled = true
		config.PRCommandAuthorization.AdminTeams = []string{"octocat/schema-admins"}
		db := config.Databases[dbName]
		db.OperatorTeams = []string{"octocat/orders-operators"}
		config.Databases[dbName] = db

		mux := http.NewServeMux()
		server := httptest.NewServer(mux)
		t.Cleanup(server.Close)
		client := gh.NewClient(nil)
		client.BaseURL, _ = url.Parse(server.URL + "/")

		result := setupFakeGitHubForPlan(t, mux, map[string]string{
			"orders.sql": "CREATE TABLE `orders` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `total_cents` bigint NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
		}, fmt.Sprintf("database: %s\ntype: mysql\n", dbName), dbName)
		serveTeams(mux)

		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		installClient := ghclient.NewInstallationClient(client, logger)
		h := NewHandler(svc, &fakeClientFactory{client: installClient}, nil, logger, WithInsecureSkipSignatureVerification())

		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "opened", headSHA: "abc123", headRef: "feature-branch"}, nil))
		require.Equal(t, http.StatusOK, rr.Code)

		select {
		case body := <-result.comments:
			return body
		case <-time.After(webhookIntegrationPollDeadline):
			require.FailNow(t, "timed out waiting for the auto-plan comment")
			return ""
		}
	}

	assertNoticeLeadsThePlan := func(t *testing.T, body, notice string) {
		t.Helper()
		assert.Equal(t, 1, strings.Count(body, "🔐"), body)
		header, rest, found := strings.Cut(body, notice)
		require.True(t, found, body)
		assert.True(t, strings.HasPrefix(header, "## Schema Change Plan — Staging\n"), body)
		assert.NotContains(t, header, "CREATE TABLE", "the notice comes before the DDL")
		assert.Contains(t, rest, "CREATE TABLE `orders`", body)
		_, footer, _ := strings.Cut(rest, "\n---\n")
		assert.Equal(t, "\n▶️ **To apply**, comment:\n```\nschemabot apply -e staging\n```\n", footer, "the footer keeps only the apply command")
	}

	t.Run("author outside every team", func(t *testing.T) {
		var reads *teamReadCounts
		body := autoPlanComment(t, "webhook_who_can_apply_denied", func(mux *http.ServeMux) {
			reads = countTeamReads(t, mux, map[string][]string{
				"schema-admins":    {"octo-admin"},
				"orders-operators": {"mona"},
			})
		})

		assertNoticeLeadsThePlan(t, body, "🔐 **`"+author+"` cannot run `schemabot apply` for `webhook_who_can_apply_denied`.** Only these teams can. Ask a member of one of them to run it:\n"+
			"- Operators: `octocat/orders-operators`\n"+
			"- Admins: `octocat/schema-admins`\n")

		counts := reads.snapshot()
		for _, team := range teams {
			assert.Equal(t, 1, counts["/orgs/octocat/teams/"+team+"/memberships/"+author], "one membership request for %s", team)
		}
		membershipReads := 0
		for path, n := range counts {
			if strings.Contains(path, "/memberships/") {
				membershipReads += n
			}
			assert.False(t, strings.HasSuffix(path, "/members"), "no team's member list is read: %s", path)
		}
		assert.Equal(t, len(teams), membershipReads, "one membership request per configured team: %v", counts)
	})

	t.Run("membership cannot be read", func(t *testing.T) {
		body := autoPlanComment(t, "webhook_who_can_apply_unreadable", func(mux *http.ServeMux) {
			for _, team := range teams {
				handleTeamMembers(t, mux, "octocat", team, http.StatusForbidden)
			}
		})

		assertNoticeLeadsThePlan(t, body, "🔐 Only these teams can run `schemabot apply` for `webhook_who_can_apply_unreadable`. Ask a member of one of them to run it:\n"+
			"- Operators: `octocat/orders-operators`\n"+
			"- Admins: `octocat/schema-admins`\n")
		assert.NotContains(t, body, "cannot run", "a reader whose authorization was never checked is not told they cannot apply")
	})
}
