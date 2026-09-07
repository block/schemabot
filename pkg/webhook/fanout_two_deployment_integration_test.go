//go:build integration

// Two-deployment fan-out integration tests. Every deployment installed on a
// shared repo receives its own copy of the same PR comment webhook, so these
// tests feed one delivery to two handlers — an owner whose storage holds the
// target work and a sibling tenant whose storage does not — and verify the PR
// gets exactly one answer, from the owner.

package webhook

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
)

// participantFanOutConfig marks the test repo as an aggregate participant on a
// tenant deployment, the shape under which unscoped commands fan out.
func participantFanOutConfig(cfg *api.ServerConfig, tenant string) {
	cfg.Tenant = tenant
	if cfg.Repos == nil {
		cfg.Repos = map[string]api.RepoConfig{}
	}
	cfg.Repos["octocat/hello-world"] = api.RepoConfig{
		Aggregate: &api.AggregateConfig{Role: api.AggregateRoleParticipant},
	}
}

// siblingFanOutHandler builds a tenant participant deployment whose storage
// holds none of the PR's work — the deployment the fan-out contract requires
// to stay silent.
func siblingFanOutHandler(t *testing.T) (*Handler, chan string) {
	t.Helper()
	cfg := aggregateParticipantConfig()
	cfg.Tenant = "tenant-b"
	h, _, comments := newFanOutSkipHandler(t, cfg)
	return h, comments
}

// requireNoFanOutComment asserts the sibling deployment posted nothing for the
// delivery. The grace interval bounds the sibling's async command goroutine,
// whose empty-storage lookup completes well inside it.
func requireNoFanOutComment(t *testing.T, comments chan string) {
	t.Helper()
	select {
	case body := <-comments:
		t.Fatalf("sibling deployment must stay silent on a fan-out command it does not own, posted: %s", body)
	case <-time.After(500 * time.Millisecond):
	}
}

// An unscoped `rollback <apply-id> -e <env>` on an aggregate repo is delivered
// to every deployment. The apply lives in exactly one tenant's storage: that
// owner produces the rollback plan and confirmation prompt, while a sibling
// tenant deployment stays silent — one operator command yields one PR answer.
func TestE2EFanOutRollbackOwnerActsSiblingSilent(t *testing.T) {
	dbName := "webhook_fanout_rb"
	svc := setupE2EService(t, dbName)
	participantFanOutConfig(svc.Config(), "tenant-a")
	ctx := t.Context()

	appDSN := strings.Replace(e2eTargetDSN, "/target_test", "/"+dbName, 1) + "&multiStatements=true"
	db, err := sql.Open("block-mysql", appDSN)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	utils.CloseAndLog(db)

	schemaWithIndex := "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `idx_name` (`name`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	prNumber := int32(1)
	planResp, err := svc.ExecutePlan(ctx, api.PlanRequest{
		Database:    dbName,
		Environment: "staging",
		Type:        "mysql",
		Repository:  "octocat/hello-world",
		PullRequest: &prNumber,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			dbName: {Files: map[string]string{"users.sql": schemaWithIndex}},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, planResp.Changes, "expected DDL changes")

	applyResp, applyID, err := svc.ExecuteApply(ctx, api.ApplyRequest{
		PlanID:      planResp.PlanID,
		Environment: "staging",
		Options:     map[string]string{"allow_unsafe": "true"},
	})
	require.NoError(t, err)
	require.True(t, applyResp.Accepted)
	require.Greater(t, applyID, int64(0))

	require.Eventually(t, func() bool {
		apply, err := svc.Storage().Applies().Get(ctx, applyID)
		if err != nil || apply == nil {
			return false
		}
		return state.IsState(apply.State, state.Apply.Completed)
	}, 30*time.Second, 500*time.Millisecond, "apply should complete")

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	ownerGitHub := setupFakeGitHubForPlan(t, mux, map[string]string{
		"users.sql": schemaWithIndex,
	}, schemabotConfig, dbName)

	installClient := ghclient.NewInstallationClient(client, testLogger())
	owner := NewHandler(svc, &fakeClientFactory{client: installClient}, nil, testLogger())
	sibling, siblingComments := siblingFanOutHandler(t)

	storedApply, err := svc.Storage().Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, storedApply)

	comment := fmt.Sprintf("schemabot rollback %s -e staging", storedApply.ApplyIdentifier)
	for _, h := range []*Handler{owner, sibling} {
		req := buildWebhookRequest(t, webhookPayloadOpts{comment: comment, isPR: true}, nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		require.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, rr.Body.String(), "rollback started")
	}

	select {
	case body := <-ownerGitHub.comments:
		assert.Contains(t, body, "## Schema Rollback Plan")
		assert.Contains(t, body, "DROP INDEX", "rollback should drop the index the apply added")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the owner's rollback plan comment")
	}

	requireNoFanOutComment(t, siblingComments)
}

// An unscoped lifecycle control command (`stop <apply-id> -e <env>`) on an
// aggregate repo is delivered to every deployment. Only the tenant whose
// storage holds the running apply records the durable stop request and posts
// the acceptance comment; a sibling tenant deployment stays silent.
func TestE2EFanOutStopOwnerActsSiblingSilent(t *testing.T) {
	ctx := t.Context()
	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	require.NoError(t, schemabotDB.PingContext(ctx))

	store := mysqlstore.New(schemabotDB)
	applyIdentifier := "apply_fa0142ce"
	database := "fanout_stop_db"
	cleanupStopCommandTestRows(t, schemabotDB, applyIdentifier, database)
	t.Cleanup(func() {
		cleanupStopCommandTestRows(t, schemabotDB, applyIdentifier, database)
		utils.CloseAndLog(schemabotDB)
	})
	createStopCommandApply(t, store, applyIdentifier, database)

	client, mux := setupGitHubServer(t)
	ownerComments := make(chan string, 10)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", commentRecorder(t, ownerComments))
	registerReactionRecorder(t, mux)

	service := apiServiceForStopCommandTest(t, store, database)
	participantFanOutConfig(service.Config(), "tenant-a")
	service.RegisterTernClient(database, "staging", &stopCommandTernClient{remote: true})
	owner := &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: ghclient.NewInstallationClient(client, testLogger())}),
		logger:    testLogger(),
	}
	sibling, siblingComments := siblingFanOutHandler(t)

	postStopCommand(t, owner, applyIdentifier, "alice")
	postStopCommand(t, sibling, applyIdentifier, "alice")

	ownerReply := readComment(t, ownerComments)
	assert.Contains(t, ownerReply, "Stop Request Accepted")
	assert.Contains(t, ownerReply, "`"+applyIdentifier+"`")

	requireNoFanOutComment(t, siblingComments)
}
