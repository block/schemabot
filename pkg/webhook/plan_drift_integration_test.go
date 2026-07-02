//go:build integration

// Review-time deployment drift webhook integration tests. These exercise the
// full plan flow against real MySQL deployments: a database that fans out to
// several deployments is planned once against the primary, every deployment's
// live schema is diffed against that reviewed plan, and any deployment whose
// live schema diverges — or that cannot be diffed — fails the plan check closed
// before an apply is ever attempted.

package webhook

import (
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

const driftEnv = "production"

// usersBaseSchema is the live schema on a deployment that has not yet taken the
// reviewed change: id + name only.
const usersBaseSchema = "CREATE TABLE `users` (\n" +
	"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
	"  `name` varchar(255) NOT NULL,\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

// usersWithEmailSchema is the reviewed desired schema (and the drifted live
// schema of a deployment that already carries email): id + name + email.
const usersWithEmailSchema = "CREATE TABLE `users` (\n" +
	"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
	"  `name` varchar(255) NOT NULL,\n" +
	"  `email` varchar(255) DEFAULT NULL,\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

// deploymentSpec describes one deployment of a fan-out database: its key, the
// opaque routing target, and the live schema to seed its physical database with.
// A liveSchema of "" leaves the physical database empty (no users table); a
// dropDatabase deployment has its physical database removed after seeding so the
// plan diff against it fails, simulating an unreachable/undiffable deployment.
type deploymentSpec struct {
	name         string
	liveSchema   string
	dropDatabase bool
}

// setupE2EReviewDriftService stands up a single logical database that fans out
// to the given deployments under one environment, each backed by its own
// physical MySQL database (seeded with that deployment's live schema) and its
// own registered tern LocalClient. The first spec is the primary (rollout index
// 0). Returns the service; physical databases are dropped on cleanup.
func setupE2EReviewDriftService(t *testing.T, dbName string, specs []deploymentSpec) *api.Service {
	t.Helper()
	ctx := t.Context()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = schemabotDB.Close() })
	st := mysqlstore.New(schemabotDB)

	// Clean up any stale state for this PR/database from prior runs.
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM checks WHERE database_name = ?", dbName)
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM checks WHERE repository = 'octocat/hello-world' AND pull_request = 1")
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM locks WHERE repository = 'octocat/hello-world' AND pull_request = 1")
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM plans WHERE database_name = ?", dbName)

	deployments := make(map[string]api.DeploymentTarget, len(specs))
	order := make([]string, 0, len(specs))
	ternClients := make(map[string]tern.Client, len(specs))

	for _, spec := range specs {
		physicalDB := dbName + "_" + spec.name
		physicalDSN := strings.Replace(e2eTargetDSN, "/target_test", "/"+physicalDB, 1)

		// Create and seed the deployment's physical database.
		adminDB, err := sql.Open("mysql", e2eTargetDSN+"&multiStatements=true")
		require.NoError(t, err)
		_, err = adminDB.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+physicalDB+"`")
		require.NoError(t, err)
		_, err = adminDB.ExecContext(ctx, "CREATE DATABASE `"+physicalDB+"`")
		require.NoError(t, err)
		_ = adminDB.Close()

		if spec.liveSchema != "" {
			seedDB, err := sql.Open("mysql", physicalDSN+"&multiStatements=true")
			require.NoError(t, err)
			_, err = seedDB.ExecContext(ctx, spec.liveSchema)
			require.NoError(t, err)
			_ = seedDB.Close()
		}

		physicalDBName := physicalDB
		t.Cleanup(func() {
			db, err := sql.Open("mysql", e2eTargetDSN+"&multiStatements=true")
			if err == nil {
				_, _ = db.ExecContext(t.Context(), "DROP DATABASE IF EXISTS `"+physicalDBName+"`")
				_ = db.Close()
			}
		})

		// An unreachable/undiffable deployment: drop its physical database after
		// seeding so the plan diff against it fails and the rollup blocks closed.
		if spec.dropDatabase {
			dropDB, err := sql.Open("mysql", e2eTargetDSN+"&multiStatements=true")
			require.NoError(t, err)
			_, err = dropDB.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+physicalDB+"`")
			require.NoError(t, err)
			_ = dropDB.Close()
		}

		client, err := tern.NewLocalClient(tern.LocalConfig{
			Database:  dbName,
			Type:      "mysql",
			TargetDSN: physicalDSN,
		}, st, logger)
		require.NoError(t, err)
		t.Cleanup(func() { _ = client.Close() })

		deployments[spec.name] = api.DeploymentTarget{Target: dbName + "-" + spec.name + "-target"}
		order = append(order, spec.name)
		ternClients[spec.name+"/"+driftEnv] = client
	}

	serverConfig := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			dbName: {
				Type: "mysql",
				Environments: map[string]api.EnvironmentConfig{
					driftEnv: {
						Deployments:     deployments,
						DeploymentOrder: order,
					},
				},
			},
		},
		Repos: map[string]api.RepoConfig{
			"octocat/hello-world": {},
		},
	}

	svc := api.New(st, serverConfig, ternClients, logger)
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

// runDriftPlan drives one `schemabot plan -e production` webhook for the review
// drift fixtures and returns the service so the caller can assert stored state.
func runDriftPlan(t *testing.T, svc *api.Service, dbName string) {
	t.Helper()

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{"users.sql": usersWithEmailSchema}
	setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}
	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e " + driftEnv,
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
}

// A non-primary deployment whose live schema already carries the reviewed change
// diffs to a no-op while the primary plans the change, so the deployment
// diverges from the reviewed plan and the plan check fails closed with a
// review-time deployment drift block.
func TestE2EReviewDriftBlocksWhenDeploymentDiverges(t *testing.T) {
	dbName := "webhook_drift_diverge"
	svc := setupE2EReviewDriftService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersBaseSchema},      // primary: will plan ADD email
		{name: "au", liveSchema: usersBaseSchema},      // matches the reviewed plan
		{name: "us", liveSchema: usersWithEmailSchema}, // drifted: already has email
	})

	runDriftPlan(t, svc, dbName)

	check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, driftEnv, "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check, "expected a stored check record")
	assert.Equal(t, "completed", check.Status)
	assert.Equal(t, "failure", check.Conclusion, "a diverged deployment must fail the plan check closed")
	assert.Equal(t, storage.ReviewTimeDeploymentDriftBlockingReason, check.BlockingReason,
		"the check must carry a durable review-time deployment drift block")
}

// A deployment that cannot be diffed (its physical database is gone) cannot be
// confirmed to match the reviewed plan, so it is treated as blocking rather than
// agreement and the plan check fails closed.
func TestE2EReviewDriftBlocksWhenDeploymentUnreachable(t *testing.T) {
	dbName := "webhook_drift_unreachable"
	svc := setupE2EReviewDriftService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersBaseSchema},                     // primary: will plan ADD email
		{name: "au", liveSchema: usersBaseSchema},                     // matches the reviewed plan
		{name: "us", liveSchema: usersBaseSchema, dropDatabase: true}, // undiffable
	})

	runDriftPlan(t, svc, dbName)

	check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, driftEnv, "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check, "expected a stored check record")
	assert.Equal(t, "completed", check.Status)
	assert.Equal(t, "failure", check.Conclusion, "an undiffable deployment must fail the plan check closed")
	assert.Equal(t, storage.ReviewTimeDeploymentDriftBlockingReason, check.BlockingReason,
		"the check must carry a durable review-time deployment drift block")
}

// When every deployment's live schema matches the reviewed plan, the rollup is
// clean: the plan check reflects the reviewed change (action_required, changes
// pending) with no drift block, so drift never spuriously blocks a uniform
// rollout.
func TestE2EReviewDriftCleanWhenAllDeploymentsMatch(t *testing.T) {
	dbName := "webhook_drift_clean"
	svc := setupE2EReviewDriftService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersBaseSchema},
		{name: "au", liveSchema: usersBaseSchema},
		{name: "us", liveSchema: usersBaseSchema},
	})

	runDriftPlan(t, svc, dbName)

	check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, driftEnv, "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check, "expected a stored check record")
	assert.Equal(t, "completed", check.Status)
	assert.Equal(t, "action_required", check.Conclusion,
		"a uniform rollout with changes pending must be action_required, not drift-blocked")
	assert.True(t, check.HasChanges, "the reviewed change adds a column, so the check has changes")
	assert.Empty(t, check.BlockingReason, "a clean rollup must not record a drift block")
}
