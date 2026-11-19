//go:build integration

package webhook

import (
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
)

// A pull request changes one schema file in each of several databases. Its
// automatic plan reads each database's schemabot.yaml once, however many
// databases and environments the plan covers. Planning a database selects its config from
// the auto-plan's discovery instead of discovering every config again, and
// each environment's plan reuses the config its database resolved, so the
// GitHub requests a delivery spends on configs grow with the number of
// databases rather than with its square (AV-15).
func TestE2EAutoPlanReadsEachConfigOnce(t *testing.T) {
	for _, deployment := range []struct {
		name  string
		setup func(t *testing.T, dbName string) *api.Service
	}{
		{name: "one environment", setup: setupE2EService},
		{name: "staging and production", setup: setupE2EServiceMultiEnv},
	} {
		t.Run(deployment.name, func(t *testing.T) {
			dbName := "webhook_config_reads"
			one := configReadsToAutoPlan(t, deployment.setup(t, dbName), dbName, nil)
			assert.Equal(t, map[string]int{"schema/" + dbName + "/schemabot.yaml": 1}, one,
				"reads of each schemabot.yaml while planning one database")

			others := []string{"config_reads_ledger", "config_reads_billing"}
			three := configReadsToAutoPlan(t, deployment.setup(t, dbName), dbName, others)
			assert.Equal(t, map[string]int{
				"schema/" + dbName + "/schemabot.yaml":       1,
				"schema/config_reads_ledger/schemabot.yaml":  1,
				"schema/config_reads_billing/schemabot.yaml": 1,
			}, three, "reads of each schemabot.yaml while planning three databases")
		})
	}
}

// configReadsToAutoPlan registers others alongside dbName on svc, opens a pull
// request that changes one schema file in each of those databases, waits for
// every plan comment, and returns how many times each schemabot.yaml was read.
func configReadsToAutoPlan(t *testing.T, svc *api.Service, dbName string, others []string) map[string]int {
	t.Helper()
	serverConfig := svc.Config()
	for _, other := range others {
		serverConfig.Databases[other] = serverConfig.Databases[dbName]
	}

	mux := http.NewServeMux()
	requests := &githubRequestCounter{counts: make(map[string]int)}
	server := httptest.NewServer(requests.wrap(mux))
	t.Cleanup(server.Close)
	client := gh.NewClient(nil)
	var err error
	client.BaseURL, err = url.Parse(server.URL + "/")
	require.NoError(t, err)

	databases := append([]string{dbName}, others...)
	configs := make(map[string]string)
	schemaFiles := make(map[string]string)
	for _, database := range databases {
		configs["schema/"+database+"/schemabot.yaml"] = fmt.Sprintf("database: %s\ntype: mysql\n", database)
		schemaFiles[database+"/users.sql"] = "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	}
	result := setupFakeGitHubForPlanWithConfigs(t, mux, schemaFiles, configs, dbName, nil)
	h := newE2EHandler(t, svc, client)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "opened", headSHA: "abc123", headRef: "feature-branch"}, nil))
	require.Equal(t, http.StatusOK, rr.Code)
	for _, database := range databases {
		select {
		case body := <-result.comments:
			require.Contains(t, body, "Schema Change Plan", "plan comment while waiting for %s", database)
		case <-time.After(webhookIntegrationPollDeadline):
			t.Fatalf("timed out waiting for %d plan comments", len(databases))
		}
	}
	h.DrainInProcessWebhookWork(t.Context())

	reads := make(map[string]int)
	for key, n := range requests.snapshot() {
		if configPath, ok := strings.CutPrefix(key, "GET /repos/octocat/hello-world/contents/"); ok && strings.HasSuffix(configPath, "/schemabot.yaml") {
			reads[configPath] = n
		}
	}
	return reads
}
