//go:build integration

// Review-time deployment drift webhook integration tests. These exercise the
// full plan flow against real MySQL deployments: a database that fans out to
// several deployments is planned once against the primary, every deployment's
// live schema is diffed against that reviewed plan, and any deployment whose
// live schema diverges — or that cannot be diffed — fails the plan check closed
// before an apply is ever attempted.

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

	mysql "github.com/go-sql-driver/mysql"
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

// openDriftDB opens a MySQL connection for the drift fixtures, verifies it with
// a ping (sql.Open is lazy, so a bad DSN otherwise surfaces later in less
// obvious places), and registers a close on cleanup so a failed setup step never
// leaks the handle.
func openDriftDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.PingContext(t.Context()))
	return db
}

// driftDSN returns the e2e target DSN pointed at dbName (an empty dbName keeps
// the DSN's own database) with multi-statement execution enabled. It is built
// via ParseDSN/FormatDSN so it survives passwords or query params that naive
// string replacement would corrupt.
func driftDSN(t *testing.T, dbName string) string {
	t.Helper()
	cfg, err := mysql.ParseDSN(e2eTargetDSN)
	require.NoError(t, err)
	if dbName != "" {
		cfg.DBName = dbName
	}
	cfg.MultiStatements = true
	return cfg.FormatDSN()
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

	schemabotDB := openDriftDB(t, e2eSchemabotDSN)
	st := mysqlstore.New(schemabotDB)

	// Clean up any stale state for this PR/database from prior runs. Assert the
	// deletes succeed: a silent isolation failure leaves stale rows in the shared
	// schemabot_test database and surfaces as confusing cross-test flakes.
	_, err := schemabotDB.ExecContext(ctx, "DELETE FROM checks WHERE database_name = ?", dbName)
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "DELETE FROM checks WHERE repository = 'octocat/hello-world' AND pull_request = 1")
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "DELETE FROM locks WHERE repository = 'octocat/hello-world' AND pull_request = 1")
	require.NoError(t, err)
	_, err = schemabotDB.ExecContext(ctx, "DELETE FROM plans WHERE database_name = ?", dbName)
	require.NoError(t, err)

	deployments := make(map[string]api.DeploymentTarget, len(specs))
	order := make([]string, 0, len(specs))
	ternClients := make(map[string]tern.Client, len(specs))

	adminDSN := driftDSN(t, "")
	for _, spec := range specs {
		physicalDB := dbName + "_" + spec.name
		physicalDSN := driftDSN(t, physicalDB)

		// Create and seed the deployment's physical database.
		adminDB := openDriftDB(t, adminDSN)
		_, err := adminDB.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+physicalDB+"`")
		require.NoError(t, err)
		_, err = adminDB.ExecContext(ctx, "CREATE DATABASE `"+physicalDB+"`")
		require.NoError(t, err)

		if spec.liveSchema != "" {
			seedDB := openDriftDB(t, physicalDSN)
			_, err = seedDB.ExecContext(ctx, spec.liveSchema)
			require.NoError(t, err)
		}

		// Drop the physical database on cleanup. Detach from the test's context so
		// the drop survives the test finishing — t.Context() is already cancelled
		// by the time cleanup runs, which would otherwise leave databases behind.
		t.Cleanup(func() {
			dropCtx := context.WithoutCancel(t.Context())
			db, err := sql.Open("mysql", adminDSN)
			if err != nil {
				t.Logf("drift cleanup: open admin db to drop %s: %v", physicalDB, err)
				return
			}
			defer func() { _ = db.Close() }()
			_, err = db.ExecContext(dropCtx, "DROP DATABASE IF EXISTS `"+physicalDB+"`")
			assert.NoError(t, err, "drift cleanup: drop physical database %s", physicalDB)
		})

		// An unreachable/undiffable deployment: drop its physical database after
		// seeding so the plan diff against it fails and the rollup blocks closed.
		if spec.dropDatabase {
			dropDB := openDriftDB(t, adminDSN)
			_, err = dropDB.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+physicalDB+"`")
			require.NoError(t, err)
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
	baseURL, err := url.Parse(server.URL + "/")
	require.NoError(t, err)
	client.BaseURL = baseURL

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

// The durable drift block's contract is that only a fresh clean rollup clears
// it. A drifted deployment blocks the plan check closed; once that deployment's
// schema is reconciled back to the reviewed pre-state and the PR is re-planned,
// the now-uniform rollup must clear the block so the PR can proceed. If clearing
// ever regressed, drift-blocked PRs would stick blocked permanently.
func TestE2EReviewDriftClearsBlockAfterDeploymentReconciled(t *testing.T) {
	dbName := "webhook_drift_unblock"
	svc := setupE2EReviewDriftService(t, dbName, []deploymentSpec{
		{name: "eu", liveSchema: usersBaseSchema},      // primary: will plan ADD email
		{name: "au", liveSchema: usersBaseSchema},      // matches the reviewed plan
		{name: "us", liveSchema: usersWithEmailSchema}, // drifted: already has email
	})

	// The drifted deployment first blocks the plan check closed.
	runDriftPlan(t, svc, dbName)
	check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, driftEnv, "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check, "expected a stored check record")
	require.Equal(t, storage.ReviewTimeDeploymentDriftBlockingReason, check.BlockingReason,
		"the drifted deployment must first block the plan check closed")

	// Reconcile the drifted deployment back to the reviewed pre-state so every
	// deployment now diffs to the same reviewed change.
	reconcileDB := openDriftDB(t, driftDSN(t, dbName+"_us"))
	_, err = reconcileDB.ExecContext(t.Context(), "ALTER TABLE `users` DROP COLUMN `email`")
	require.NoError(t, err)

	// Re-planning against the now-uniform rollout clears the durable block.
	runDriftPlan(t, svc, dbName)
	check, err = svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, driftEnv, "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check, "expected a stored check record")
	assert.Equal(t, "completed", check.Status)
	assert.Equal(t, "action_required", check.Conclusion,
		"a reconciled rollout with changes pending must be action_required, not drift-blocked")
	assert.True(t, check.HasChanges, "the reviewed change adds a column, so the check still has changes")
	assert.Empty(t, check.BlockingReason, "reconciling the drifted deployment must clear the durable drift block")
}
