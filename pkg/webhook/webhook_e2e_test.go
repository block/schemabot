//go:build integration

// Webhook E2E tests exercise the full plan flow end-to-end.
//
// Architecture:
//
//   ┌─────────────────────────────────────────────────────┐
//   │                  Test Harness                        │
//   │                                                     │
//   │  Webhook POST    buildWebhookRequest()              │
//   │  (issue_comment)       │                            │
//   │                        ▼                            │
//   │                  ┌───────────┐  ForInstallation()   │
//   │                  │  Handler  │────────────┐         │
//   │                  └─────┬─────┘            │         │
//   │                        │                  ▼         │
//   │            handlePlanCommand()     ┌────────────┐   │
//   │                        │          │  httptest   │   │
//   │                        ▼          │  GitHub API │   │
//   │                  ┌───────────┐    │            │   │
//   │                  │  GitHub   │◄──►│ GET /pulls │   │
//   │                  │  Client   │    │ GET /trees │   │
//   │                  │  (real    │    │ GET /blobs │   │
//   │                  │  go-github)    │ GET /files │   │
//   │                  └─────┬─────┘    └─────┬──────┘   │
//   │                        │           captures│        │
//   │                        ▼                  ▼        │
//   │                  ┌───────────┐    ┌─────────────┐  │
//   │                  │api.Service│    │ chan string  │  │
//   │                  │.ExecutePlan    │ (comments,  │  │
//   │                  └─────┬─────┘    │  reactions, │  │
//   │                        │          │  check runs)│  │
//   │                        ▼          └─────────────┘  │
//   │                  ┌───────────┐                      │
//   │                  │tern.Local │ Spirit DDL diff      │
//   │                  │ Client    │──────────┐           │
//   │                  └───────────┘          ▼           │
//   │                                ┌──────────────┐    │
//   │                                │ testcontainer │    │
//   │  ┌──────────────┐              │ MySQL (target)│    │
//   │  │ testcontainer │              └──────────────┘    │
//   │  │ MySQL         │                                  │
//   │  │ (schemabot    │◄── plans, checks stored          │
//   │  │  storage)     │                                  │
//   │  └──────────────┘                                  │
//   └─────────────────────────────────────────────────────┘
//
// Two MySQL testcontainers:
//   - Target DB: the application database that Spirit diffs against
//   - SchemaBot storage: persists plans, checks, applies, tasks
//
// The httptest server simulates all GitHub API endpoints needed for
// a plan flow: PR info, changed files, git tree, blob content,
// schemabot.yaml config. It also captures outgoing POST requests
// (comments, reactions, check runs) via buffered channels.

package webhook

import (
	"context"
	"database/sql"
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	gh "github.com/google/go-github/v68/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/testutil"
	"github.com/block/spirit/pkg/statement"
)

var (
	e2eTargetDSN    string
	e2eSchemabotDSN string
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	targetContainer, err := startE2EMySQLContainer(ctx, "webhook-target-mysql", "target_test", nil)
	if err != nil {
		log.Fatalf("Failed to start target MySQL: %v", err)
	}

	host, err := testutil.ContainerHost(ctx, targetContainer)
	if err != nil {
		log.Fatalf("Failed to get target host: %v", err)
	}
	port, err := testutil.ContainerPort(ctx, targetContainer, "3306")
	if err != nil {
		log.Fatalf("Failed to get target port: %v", err)
	}
	e2eTargetDSN = fmt.Sprintf("root:testpassword@tcp(%s:%d)/target_test?parseTime=true", host, port)

	sbContainer, err := startE2EMySQLContainer(ctx, "webhook-schemabot-mysql", "schemabot_test", &schema.MySQLFS)
	if err != nil {
		log.Fatalf("Failed to start SchemaBot MySQL: %v", err)
	}

	sbHost, err := testutil.ContainerHost(ctx, sbContainer)
	if err != nil {
		log.Fatalf("Failed to get schemabot host: %v", err)
	}
	sbPort, err := testutil.ContainerPort(ctx, sbContainer, "3306")
	if err != nil {
		log.Fatalf("Failed to get schemabot port: %v", err)
	}
	e2eSchemabotDSN = fmt.Sprintf("root:testpassword@tcp(%s:%d)/schemabot_test?parseTime=true", sbHost, sbPort)

	code := m.Run()

	if os.Getenv("DEBUG") == "" {
		_ = targetContainer.Terminate(ctx)
		_ = sbContainer.Terminate(ctx)
	}

	os.Exit(code)
}

// setupE2EService creates a real api.Service with a LocalClient for the given database.
func setupE2EService(t *testing.T, appDBName string) *api.Service {
	t.Helper()
	ctx := t.Context()

	// Create the app database on the target
	targetDB, err := sql.Open("mysql", e2eTargetDSN+"&multiStatements=true")
	require.NoError(t, err)
	_, err = targetDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+appDBName+"`")
	require.NoError(t, err)
	_ = targetDB.Close()

	t.Cleanup(func() {
		db, err := sql.Open("mysql", e2eTargetDSN+"&multiStatements=true")
		if err == nil {
			_, _ = db.ExecContext(t.Context(), "DROP DATABASE IF EXISTS `"+appDBName+"`")
			_ = db.Close()
		}
	})

	appDSN := strings.Replace(e2eTargetDSN, "/target_test", "/"+appDBName, 1)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = schemabotDB.Close() })

	st := mysqlstore.New(schemabotDB)

	// Clean up any stale data from previous test runs (shared storage DB)
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM checks WHERE database_name = ?", appDBName)
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM checks WHERE repository = 'octocat/hello-world' AND pull_request = 1")
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM plans WHERE database_name = ?", appDBName)

	localClient, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  appDBName,
		Type:      "mysql",
		TargetDSN: appDSN,
	}, st, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = localClient.Close() })

	serverConfig := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			appDBName: {
				Type: "mysql",
				Environments: map[string]api.EnvironmentConfig{
					"staging": {DSN: appDSN},
				},
			},
		},
	}

	svc := api.New(st, serverConfig, map[string]tern.Client{
		appDBName + "/staging": localClient,
	}, logger)
	t.Cleanup(func() { _ = svc.Close() })

	return svc
}

// seedCheck creates a check record in storage with common defaults.
// Use conclusion "action_required" for pending changes, "success" for applied.
func seedCheck(t *testing.T, svc *api.Service, dbName, env, conclusion string) {
	t.Helper()
	check := &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "abc123",
		Environment:  env,
		DatabaseType: "mysql",
		DatabaseName: dbName,
		CheckRunID:   1,
		HasChanges:   conclusion != "success",
		Status:       "completed",
		Conclusion:   conclusion,
	}
	err := svc.Storage().Checks().Upsert(t.Context(), check)
	require.NoError(t, err)
}

// newTestHandler creates a Handler wired to the given service and GitHub client,
// with an error-level logger to reduce test noise.
func newE2EHandler(t *testing.T, svc *api.Service, client *gh.Client) *Handler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}
	return NewHandler(svc, factory, nil, logger)
}

// fakeGitHubForPlan sets up httptest handlers that simulate the GitHub API for a plan flow.
// Returns the captured comment bodies and check run payloads.
type planFlowResult struct {
	comments  chan string
	reactions chan string
	checkRuns chan checkRunCapture
}

type checkRunCapture struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
}

// setupFakeGitHubForPlan sets up a fake GitHub server for plan flows.
// schemaSQL maps filename -> content. Files are placed under schema/{namespace}/.
// namespace is the MySQL schema name (required).
func setupFakeGitHubForPlan(t *testing.T, mux *http.ServeMux, schemaSQL map[string]string, schemabotConfig, ns string) *planFlowResult {
	t.Helper()

	result := &planFlowResult{
		comments:  make(chan string, 10),
		reactions: make(chan string, 10),
		checkRuns: make(chan checkRunCapture, 10),
	}

	// PR info
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{
				Ref: new("feature-branch"),
				SHA: new("abc123"),
			},
			Base: &gh.PullRequestBranch{
				Ref: new("main"),
				SHA: new("def456"),
			},
			User: &gh.User{Login: new("testuser")},
		})
	})

	// PR changed files — report schema files changed (in namespace subdir)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, r *http.Request) {
		var files []*gh.CommitFile
		for name := range schemaSQL {
			files = append(files, &gh.CommitFile{
				Filename: new("schema/" + ns + "/" + name),
				Status:   new("added"),
			})
		}
		_ = json.NewEncoder(w).Encode(files)
	})

	// Build tree entries for all schema files + schemabot.yaml config
	var treeEntries []*gh.TreeEntry
	blobIndex := 0
	blobContents := make(map[string]string) // sha -> content

	// schemabot.yaml config
	if schemabotConfig != "" {
		configSHA := "configsha001"
		blobContents[configSHA] = schemabotConfig
		treeEntries = append(treeEntries, &gh.TreeEntry{
			Path: new("schema/schemabot.yaml"),
			Mode: new("100644"),
			Type: new("blob"),
			SHA:  new(configSHA),
			Size: new(len(schemabotConfig)),
		})
	}

	for name, content := range schemaSQL {
		sha := fmt.Sprintf("blobsha%03d", blobIndex)
		blobIndex++
		blobContents[sha] = content
		treeEntries = append(treeEntries, &gh.TreeEntry{
			Path: new("schema/" + ns + "/" + name),
			Mode: new("100644"),
			Type: new("blob"),
			SHA:  new(sha),
			Size: new(len(content)),
		})
	}

	// Git tree (recursive)
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/abc123", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.Tree{
			SHA:       new("abc123"),
			Entries:   treeEntries,
			Truncated: new(false),
		})
	})

	// Blob content
	mux.HandleFunc("GET /repos/octocat/hello-world/git/blobs/", func(w http.ResponseWriter, r *http.Request) {
		sha := r.URL.Path[len("/repos/octocat/hello-world/git/blobs/"):]
		if _, ok := blobContents[sha]; !ok {
			http.NotFound(w, r)
			return
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(blobContents[sha]))
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"sha":"%s","content":"%s","encoding":"base64","size":%d}`, sha, encoded, len(blobContents[sha]))
	})

	// Contents API (used by FetchConfig -> FetchFileContent)
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/", func(w http.ResponseWriter, r *http.Request) {
		filePath := r.URL.Path[len("/repos/octocat/hello-world/contents/"):]
		if filePath == "schema/schemabot.yaml" && schemabotConfig != "" {
			_ = json.NewEncoder(w).Encode(gh.RepositoryContent{
				Name:     new("schemabot.yaml"),
				Path:     new("schema/schemabot.yaml"),
				Content:  new(base64.StdEncoding.EncodeToString([]byte(schemabotConfig))),
				Encoding: new("base64"),
			})
			return
		}
		http.NotFound(w, r)
	})

	// Capture comments
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		result.comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})

	// Capture reactions
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Content string `json:"content"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		result.reactions <- body.Content
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	// Capture check run creates (incrementing IDs so aggregate can distinguish create vs update)
	var checkRunIDCounter atomic.Int64
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		result.checkRuns <- body
		id := checkRunIDCounter.Add(1)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id})
	})

	// Capture check run updates (PATCH)
	mux.HandleFunc("PATCH /repos/octocat/hello-world/check-runs/", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		result.checkRuns <- body
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	return result
}

func TestE2EPlanWithChanges(t *testing.T) {
	dbName := "webhook_plan_changes"
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

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "plan generated successfully")

	// Verify plan comment was posted
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "## MySQL Schema Change Plan")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, dbName)
		assert.Contains(t, body, "staging")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for plan comment")
	}

	// Verify check run was created (per-database + aggregate)
	select {
	case cr := <-result.checkRuns:
		assert.Contains(t, cr.Name, "SchemaBot")
		assert.Equal(t, "completed", cr.Status)
		assert.Equal(t, "action_required", cr.Conclusion)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for check run")
	}

	// Verify plan was persisted to SchemaBot storage
	ctx := t.Context()
	plans, err := svc.Storage().Plans().GetByPR(ctx, "octocat/hello-world", 1)
	require.NoError(t, err)
	require.NotEmpty(t, plans, "expected at least one plan record")
	// Find the plan for this database (shared storage may have data from prior tests)
	var plan *storage.Plan
	for _, p := range plans {
		if p.Database == dbName {
			plan = p
			break
		}
	}
	require.NotNil(t, plan, "expected a plan record for database %s", dbName)
	assert.Equal(t, dbName, plan.Database)
	assert.Equal(t, "mysql", plan.DatabaseType)
	assert.Equal(t, "staging", plan.Environment)
	assert.Equal(t, "octocat/hello-world", plan.Repository)
	assert.Equal(t, 1, plan.PullRequest)
	assert.NotEmpty(t, plan.PlanIdentifier, "plan should have an identifier")
	assert.NotNil(t, plan.Namespaces, "plan should have namespace data")
	assert.NotEmpty(t, plan.Namespaces[dbName].Tables, "plan should have DDL changes")

	// Verify check record was persisted to SchemaBot storage
	check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check, "expected a check record")
	assert.Equal(t, "octocat/hello-world", check.Repository)
	assert.Equal(t, 1, check.PullRequest)
	assert.Equal(t, "abc123", check.HeadSHA)
	assert.Equal(t, "staging", check.Environment)
	assert.Equal(t, "mysql", check.DatabaseType)
	assert.Equal(t, dbName, check.DatabaseName)
	assert.True(t, check.HasChanges, "check should indicate changes detected")
	assert.Equal(t, "completed", check.Status)
	assert.Equal(t, "action_required", check.Conclusion)
}

func TestE2EPlanNoChanges(t *testing.T) {
	dbName := "webhook_plan_nochanges"
	svc := setupE2EService(t, dbName)

	// Create the table in the target DB first so the plan finds no changes
	ctx := t.Context()
	appDSN := strings.Replace(e2eTargetDSN, "/target_test", "/"+dbName, 1) + "&multiStatements=true"
	db, err := sql.Open("mysql", appDSN)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	_ = db.Close()

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

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "plan generated successfully")

	// Verify plan comment — should say no changes
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "No schema changes detected")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for plan comment")
	}

	// Verify check run — should be success
	select {
	case cr := <-result.checkRuns:
		assert.Equal(t, "completed", cr.Status)
		assert.Equal(t, "success", cr.Conclusion)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for check run")
	}

	// Verify plan was persisted to SchemaBot storage
	plans, err := svc.Storage().Plans().GetByPR(ctx, "octocat/hello-world", 1)
	require.NoError(t, err)
	require.NotEmpty(t, plans, "expected at least one plan record")
	var noChangesPlan *storage.Plan
	for _, p := range plans {
		if p.Database == dbName {
			noChangesPlan = p
			break
		}
	}
	require.NotNil(t, noChangesPlan, "expected a plan record for database %s", dbName)
	assert.Equal(t, dbName, noChangesPlan.Database)
	assert.Equal(t, "staging", noChangesPlan.Environment)

	// Verify check record was persisted — no changes, so conclusion is "success"
	check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check, "expected a check record")
	assert.False(t, check.HasChanges, "check should indicate no changes")
	assert.Equal(t, "completed", check.Status)
	assert.Equal(t, "success", check.Conclusion)
}

func TestE2EPlanConfigNotFound(t *testing.T) {
	dbName := "webhook_plan_noconfig"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// No schemabot.yaml config — empty schema files, no config
	result := setupFakeGitHubForPlan(t, mux, map[string]string{}, "", dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "schema request error handled")

	// Verify error comment about no config
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "No SchemaBot Configuration Found")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for error comment")
	}
}

func TestE2EMultiEnvPlan(t *testing.T) {
	dbName := "webhook_multi_env"
	svc := setupE2EServiceMultiEnv(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\nenvironments:\n  - staging\n  - production\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	// "schemabot plan" without -e → multi-env plan
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "multi-env plan started")

	// Multi-env plan runs as a background goroutine — wait for the single combined comment
	select {
	case body := <-result.comments:
		// Should be a combined comment (not separate per env)
		assert.Contains(t, body, "## MySQL Schema Change Plan")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, dbName)

		// Both envs have identical changes (empty target DBs), so should be deduplicated
		assert.Contains(t, body, "Staging & Production",
			"identical plans should have combined environment header")
		assert.NotContains(t, body, "### Staging\n",
			"should not have separate Staging section when plans are identical")

		// Footer should suggest staging first
		assert.Contains(t, body, "schemabot apply -e staging")
		assert.Contains(t, body, "schemabot apply -e production")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for multi-env plan comment")
	}

	// Should get check runs for both environments (plus aggregate updates on the channel)
	checkRunNames := make(map[string]bool)
	for i := range 2 {
		select {
		case cr := <-result.checkRuns:
			assert.Equal(t, "completed", cr.Status)
			assert.Equal(t, "action_required", cr.Conclusion)
			checkRunNames[cr.Name] = true
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out waiting for check run %d/2", i+1)
		}
	}
	assert.Len(t, checkRunNames, 2, "expected 2 distinct check runs (one per environment)")
}

func TestE2EMultiEnvPlanDifferentChanges(t *testing.T) {
	dbName := "webhook_multi_env_diff"
	svc := setupE2EServiceMultiEnv(t, dbName)

	// Pre-create the table in staging so staging has no changes, but production still does
	ctx := t.Context()
	appDSNStaging := strings.Replace(e2eTargetDSN, "/target_test", "/"+dbName+"_staging", 1) + "&multiStatements=true"
	db, err := sql.Open("mysql", appDSNStaging)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	_ = db.Close()

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\nenvironments:\n  - staging\n  - production\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "multi-env plan started")

	// Wait for the combined comment
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "## MySQL Schema Change Plan")

		// Plans differ: staging has no changes, production has changes
		// Should NOT be deduplicated — should show separate sections
		assert.Contains(t, body, "### Staging")
		assert.Contains(t, body, "### Production")
		assert.Contains(t, body, "No schema changes detected")
		assert.Contains(t, body, "CREATE TABLE")

		// Footer should only suggest production
		assert.Contains(t, body, "schemabot apply -e production")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for multi-env plan comment")
	}
}

func TestE2EAutoPlan(t *testing.T) {
	dbName := "webhook_autoplan"
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

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	// Send a pull_request "opened" webhook instead of an issue_comment
	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "auto-plan started")

	// Auto-plan runs in a background goroutine — wait for the plan comment
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Schema Change Plan")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, dbName)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for auto-plan comment")
	}

	// Verify check run was created
	select {
	case cr := <-result.checkRuns:
		assert.Contains(t, cr.Name, "SchemaBot")
		assert.Equal(t, "completed", cr.Status)
		assert.Equal(t, "action_required", cr.Conclusion)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for check run")
	}
}

func TestE2EAutoPlanWithLintViolations(t *testing.T) {
	dbName := "webhook_autoplan_lint"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	// Use tinyint PK (triggers primary_key linter — tinyint is not in allowed types)
	schemaFiles := map[string]string{
		"bad_table.sql": "CREATE TABLE `bad_table` (\n  `id` tinyint NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	// Send a pull_request "opened" webhook to trigger auto-plan
	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "auto-plan started")

	// Wait for the plan comment — should include lint violations from LintSchema
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "Schema Change Plan")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, "Lint Warnings", "plan comment should include lint violations section")
		assert.Contains(t, body, "bad_table", "lint warning should reference the table name")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for auto-plan comment with lint violations")
	}

	// Verify check run was created
	select {
	case cr := <-result.checkRuns:
		assert.Contains(t, cr.Name, "SchemaBot")
		assert.Equal(t, "completed", cr.Status)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for check run")
	}
}

func TestE2EAutoPlanNoChangesSkipsComment(t *testing.T) {
	dbName := "webhook_autoplan_nochange"
	svc := setupE2EService(t, dbName)

	// Pre-create the table so there are no changes
	ctx := t.Context()
	appDSN := strings.Replace(e2eTargetDSN, "/target_test", "/"+dbName, 1) + "&multiStatements=true"
	db, err := sql.Open("mysql", appDSN)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	_ = db.Close()

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

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "synchronize",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "auto-plan started")

	// Check run should still be created (for PR status)
	select {
	case cr := <-result.checkRuns:
		assert.Equal(t, "completed", cr.Status)
		assert.Equal(t, "success", cr.Conclusion)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for check run")
	}

	// No comment should be posted — give it a moment to confirm nothing arrives
	select {
	case body := <-result.comments:
		t.Fatalf("expected no comment for auto-plan with no changes, but got: %s", body)
	case <-time.After(3 * time.Second):
		// expected: no comment posted
	}
}

func TestE2EAutoPlanNoSchemaFiles(t *testing.T) {
	dbName := "webhook_autoplan_noschema"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// PR info
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{
				Ref: new("feature-branch"),
				SHA: new("abc123"),
			},
			Base: &gh.PullRequestBranch{
				Ref: new("main"),
				SHA: new("def456"),
			},
			User: &gh.User{Login: new("testuser")},
		})
	})

	// PR changed files — only non-schema files
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, r *http.Request) {
		files := []*gh.CommitFile{
			{Filename: new("README.md"), Status: new("modified")},
			{Filename: new("main.go"), Status: new("modified")},
		}
		_ = json.NewEncoder(w).Encode(files)
	})

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "no schema files in PR")
}

// setupE2EServiceMultiEnv creates a real api.Service with staging and production environments.
// Each environment gets its own database on the target container.
func setupE2EServiceMultiEnv(t *testing.T, appDBName string) *api.Service {
	t.Helper()
	ctx := t.Context()

	targetDB, err := sql.Open("mysql", e2eTargetDSN+"&multiStatements=true")
	require.NoError(t, err)

	stagingDB := appDBName + "_staging"
	productionDB := appDBName + "_production"

	_, err = targetDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+stagingDB+"`")
	require.NoError(t, err)
	_, err = targetDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+productionDB+"`")
	require.NoError(t, err)
	_ = targetDB.Close()

	t.Cleanup(func() {
		db, err := sql.Open("mysql", e2eTargetDSN+"&multiStatements=true")
		if err == nil {
			_, _ = db.ExecContext(t.Context(), "DROP DATABASE IF EXISTS `"+stagingDB+"`")
			_, _ = db.ExecContext(t.Context(), "DROP DATABASE IF EXISTS `"+productionDB+"`")
			_ = db.Close()
		}
	})

	stagingDSN := strings.Replace(e2eTargetDSN, "/target_test", "/"+stagingDB, 1)
	productionDSN := strings.Replace(e2eTargetDSN, "/target_test", "/"+productionDB, 1)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = schemabotDB.Close() })

	st := mysqlstore.New(schemabotDB)

	// Clean up stale data
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM checks WHERE database_name = ?", appDBName)
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM checks WHERE repository = 'octocat/hello-world' AND pull_request = 1")
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM plans WHERE database_name = ?", appDBName)

	stagingClient, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  appDBName,
		Type:      "mysql",
		TargetDSN: stagingDSN,
	}, st, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stagingClient.Close() })

	productionClient, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  appDBName,
		Type:      "mysql",
		TargetDSN: productionDSN,
	}, st, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = productionClient.Close() })

	serverConfig := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			appDBName: {
				Type: "mysql",
				Environments: map[string]api.EnvironmentConfig{
					"staging":    {DSN: stagingDSN},
					"production": {DSN: productionDSN},
				},
			},
		},
	}

	svc := api.New(st, serverConfig, map[string]tern.Client{
		appDBName + "/staging":    stagingClient,
		appDBName + "/production": productionClient,
	}, logger)
	t.Cleanup(func() { _ = svc.Close() })

	return svc
}

// TestE2EAggregateCheck verifies that a multi-env plan creates a single aggregate
// "SchemaBot" check run that rolls up per-database checks, and that the aggregate
// record is persisted in storage with the correct conclusion.
func TestE2EAggregateCheck(t *testing.T) {
	dbName := "webhook_aggregate_check"
	svc := setupE2EServiceMultiEnv(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\nenvironments:\n  - staging\n  - production\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	result := setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)
	h := newE2EHandler(t, svc, client)

	// Trigger multi-env plan
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// Drain the comment
	select {
	case <-result.comments:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for plan comment")
	}

	// Collect all check runs: 2 per-database + 1 aggregate = 3 total
	checkRuns := make(map[string]checkRunCapture)
	for range 3 {
		select {
		case cr := <-result.checkRuns:
			checkRuns[cr.Name] = cr
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out waiting for check runs, got %d so far: %v", len(checkRuns), checkRuns)
		}
	}

	// Per-database checks exist
	assert.Contains(t, checkRuns, "SchemaBot: staging/mysql/"+dbName)
	assert.Contains(t, checkRuns, "SchemaBot: production/mysql/"+dbName)

	// Aggregate check exists with correct conclusion
	aggCR, ok := checkRuns["SchemaBot"]
	require.True(t, ok, "expected aggregate check run named 'SchemaBot'")
	assert.Equal(t, "completed", aggCR.Status)
	assert.Equal(t, "action_required", aggCR.Conclusion)

	// Verify aggregate check record persisted in storage
	ctx := t.Context()
	aggCheck, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1,
		aggregateSentinel, aggregateSentinel, aggregateSentinel)
	require.NoError(t, err)
	require.NotNil(t, aggCheck, "expected aggregate check record in storage")
	assert.Equal(t, "action_required", aggCheck.Conclusion)
	assert.Equal(t, "completed", aggCheck.Status)
	assert.True(t, aggCheck.HasChanges)
	assert.Equal(t, "abc123", aggCheck.HeadSHA)
}

// TestE2EAggregateCheckStaleCleanup verifies that when a new commit removes all schema
// changes from a PR, the stale per-database checks and aggregate check are re-created
// on the new HEAD SHA with "success" conclusion. This reproduces the scenario where a
// user pushes a commit that reverts their schema change.
func TestE2EAggregateCheckStaleCleanup(t *testing.T) {
	dbName := "webhook_aggregate_stale"
	svc := setupE2EServiceMultiEnv(t, dbName)
	ctx := t.Context()

	// Seed per-database checks and aggregate as if a plan already ran on the first commit.
	for _, env := range []string{"staging", "production"} {
		check := &storage.Check{
			Repository:   "octocat/hello-world",
			PullRequest:  1,
			HeadSHA:      "oldsha111",
			Environment:  env,
			DatabaseType: "mysql",
			DatabaseName: dbName,
			CheckRunID:   100,
			HasChanges:   true,
			Status:       checkStatusCompleted,
			Conclusion:   checkConclusionActionRequired,
		}
		require.NoError(t, svc.Storage().Checks().Upsert(ctx, check))
	}
	aggCheck := &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "oldsha111",
		Environment:  aggregateSentinel,
		DatabaseType: aggregateSentinel,
		DatabaseName: aggregateSentinel,
		CheckRunID:   200,
		HasChanges:   true,
		Status:       checkStatusCompleted,
		Conclusion:   checkConclusionActionRequired,
	}
	require.NoError(t, svc.Storage().Checks().Upsert(ctx, aggCheck))

	// Set up fake GitHub server that returns NO changed files (simulating revert commit)
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// PR info with new HEAD SHA
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{
				Ref: new("feature-branch"),
				SHA: new("newsha222"),
			},
			Base: &gh.PullRequestBranch{
				Ref: new("main"),
				SHA: new("def456"),
			},
			User: &gh.User{Login: new("testuser")},
		})
	})

	// Empty PR files — the revert commit means no schema files changed
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]*gh.CommitFile{})
	})

	// Capture check run creates on the new SHA
	checkRuns := make(chan checkRunCapture, 10)
	var checkRunIDCounter atomic.Int64
	checkRunIDCounter.Store(300)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		checkRuns <- body
		id := checkRunIDCounter.Add(1)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id})
	})

	h := newE2EHandler(t, svc, client)

	// Send synchronize event with new HEAD SHA
	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "synchronize",
		headSHA: "newsha222",
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// cleanupStaleChecks runs async via goSafe — collect the 3 new check runs
	// (2 per-database + 1 aggregate, all created on the new SHA with success)
	newCheckRuns := make(map[string]checkRunCapture)
	for range 3 {
		select {
		case cr := <-checkRuns:
			newCheckRuns[cr.Name] = cr
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for stale cleanup check runs, got %d: %v", len(newCheckRuns), newCheckRuns)
		}
	}

	// Per-database checks re-created as success
	for _, name := range []string{
		"SchemaBot: staging/mysql/" + dbName,
		"SchemaBot: production/mysql/" + dbName,
	} {
		cr, ok := newCheckRuns[name]
		require.True(t, ok, "expected check run %s", name)
		assert.Equal(t, checkStatusCompleted, cr.Status)
		assert.Equal(t, checkConclusionSuccess, cr.Conclusion)
	}

	// Aggregate check re-created as success
	aggCR, ok := newCheckRuns[aggregateCheckName]
	require.True(t, ok, "expected aggregate check run")
	assert.Equal(t, checkStatusCompleted, aggCR.Status)
	assert.Equal(t, checkConclusionSuccess, aggCR.Conclusion)

	// Verify storage records updated with new SHA
	for _, env := range []string{"staging", "production"} {
		check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, env, "mysql", dbName)
		require.NoError(t, err)
		require.NotNil(t, check)
		assert.Equal(t, "newsha222", check.HeadSHA, "check should have new HEAD SHA")
		assert.Equal(t, checkConclusionSuccess, check.Conclusion)
		assert.False(t, check.HasChanges)
	}

	// Poll for aggregate storage update — the Upsert runs after the GitHub API call
	// that sends the check run to the channel, so there's a brief race.
	var storedAgg *storage.Check
	var aggErr error
	deadline2 := time.After(5 * time.Second)
	for {
		storedAgg, aggErr = svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1,
			aggregateSentinel, aggregateSentinel, aggregateSentinel)
		if aggErr == nil && storedAgg != nil && storedAgg.HeadSHA == "newsha222" {
			break
		}
		select {
		case <-deadline2:
			t.Fatal("timed out waiting for aggregate storage update")
		case <-time.After(100 * time.Millisecond):
		}
	}
	assert.Equal(t, checkConclusionSuccess, storedAgg.Conclusion)
	assert.False(t, storedAgg.HasChanges)
}

// TestE2EPlanUsesRepoDeployment verifies that the webhook plan handler routes
// to the correct Tern deployment based on the repo's default_tern_deployment config.
func TestE2EPlanUsesRepoDeployment(t *testing.T) {
	dbName := "webhook_repo_deployment"
	ctx := t.Context()

	// Create the app database on the target
	targetDB, err := sql.Open("mysql", e2eTargetDSN+"&multiStatements=true")
	require.NoError(t, err)
	_, err = targetDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+dbName+"`")
	require.NoError(t, err)
	_ = targetDB.Close()

	t.Cleanup(func() {
		db, err := sql.Open("mysql", e2eTargetDSN+"&multiStatements=true")
		if err == nil {
			_, _ = db.ExecContext(t.Context(), "DROP DATABASE IF EXISTS `"+dbName+"`")
			_ = db.Close()
		}
	})

	appDSN := strings.Replace(e2eTargetDSN, "/target_test", "/"+dbName, 1)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = schemabotDB.Close() })

	st := mysqlstore.New(schemabotDB)

	// Clean up stale data
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM checks WHERE database_name = ?", dbName)
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM checks WHERE repository = 'octocat/hello-world' AND pull_request = 1")
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM plans WHERE database_name = ?", dbName)

	localClient, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  dbName,
		Type:      "mysql",
		TargetDSN: appDSN,
	}, st, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = localClient.Close() })

	// Configure service in gRPC mode: no Databases, only TernDeployments + Repos.
	// The tern client is registered under "team-a/staging" — NOT "default/staging".
	// If the webhook handler doesn't set Deployment from the Repos config,
	// resolveDeployment falls back to "default" and TernClient lookup fails.
	serverConfig := &api.ServerConfig{
		TernDeployments: api.TernConfig{
			"team-a": api.TernEndpoints{
				"staging": "localhost:9999", // address not dialed; pre-injected client is used instead
			},
		},
		Repos: map[string]api.RepoConfig{
			"octocat/hello-world": {
				DefaultTernDeployment: "team-a",
			},
		},
	}

	svc := api.New(st, serverConfig, map[string]tern.Client{
		"team-a/staging": localClient,
	}, logger)
	t.Cleanup(func() { _ = svc.Close() })

	// Set up fake GitHub API
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

	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}
	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot plan -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "plan generated successfully")

	// Verify plan comment was posted with the expected DDL
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "## MySQL Schema Change Plan")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, dbName)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for plan comment")
	}
}

// --- Container helpers (matches e2e/setup_test.go patterns) ---

func startE2EMySQLContainer(ctx context.Context, baseName, dbName string, schemaFS *embed.FS) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Name:         e2eContainerName(baseName),
		Image:        "mysql:8.0",
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "testpassword",
			"MYSQL_DATABASE":      dbName,
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("ready for connections").WithOccurrence(2).WithStartupTimeout(60*time.Second),
			wait.ForListeningPort("3306/tcp"),
		),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            os.Getenv("DEBUG") != "",
	})
	if err != nil {
		return nil, err
	}

	if schemaFS != nil {
		host, err := testutil.ContainerHost(ctx, container)
		if err != nil {
			_ = container.Terminate(ctx)
			return nil, fmt.Errorf("get container host: %w", err)
		}
		port, err := testutil.ContainerPort(ctx, container, "3306")
		if err != nil {
			_ = container.Terminate(ctx)
			return nil, fmt.Errorf("get container port: %w", err)
		}
		dsn := fmt.Sprintf("root:testpassword@tcp(%s:%d)/%s?parseTime=true&multiStatements=true", host, port, dbName)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			_ = container.Terminate(ctx)
			return nil, fmt.Errorf("open db: %w", err)
		}
		defer func() { _ = db.Close() }()

		// Wait for MySQL to be ready to accept connections
		var pingErr error
		for range 30 {
			if pingErr = db.PingContext(ctx); pingErr == nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if pingErr != nil {
			_ = container.Terminate(ctx)
			return nil, fmt.Errorf("MySQL not ready after 15s: %w", pingErr)
		}

		if err := applyEmbeddedSchema(db, *schemaFS); err != nil {
			_ = container.Terminate(ctx)
			return nil, fmt.Errorf("apply schema: %w", err)
		}
	}

	return container, nil
}

func applyEmbeddedSchema(db *sql.DB, schemaFS embed.FS) error {
	entries, err := schemaFS.ReadDir("mysql")
	if err != nil {
		return fmt.Errorf("read schema directory: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		content, err := schemaFS.ReadFile("mysql/" + entry.Name())
		if err != nil {
			return fmt.Errorf("read schema file %s: %w", entry.Name(), err)
		}
		contentStr := string(content)
		if ct, err := statement.ParseCreateTable(contentStr); err == nil {
			if _, err := db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS `%s`", ct.TableName)); err != nil {
				return fmt.Errorf("drop table %s: %w", ct.TableName, err)
			}
		}
		if _, err := db.ExecContext(context.Background(), contentStr); err != nil {
			return fmt.Errorf("execute schema %s: %w", entry.Name(), err)
		}
	}
	return nil
}

// TestE2ERollbackPlanViaWebhook tests the full rollback flow:
// 1. Plan + apply a schema change via the service (simulating a prior apply)
// 2. Run "schemabot rollback -e staging" via webhook
// 3. Verify the rollback plan comment is posted with reverse DDL
func TestE2ERollbackPlanViaWebhook(t *testing.T) {
	dbName := "webhook_rollback"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	// Step 1: Create an initial table in the target DB (the "before" state)
	appDSN := strings.Replace(e2eTargetDSN, "/target_test", "/"+dbName, 1) + "&multiStatements=true"
	db, err := sql.Open("mysql", appDSN)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	_ = db.Close()

	// Step 2: Plan + apply adding an index (this stores OriginalSchema for rollback)
	schemaWithIndex := "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `idx_name` (`name`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	planReq := api.PlanRequest{
		Database:    dbName,
		Environment: "staging",
		Type:        "mysql",
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			dbName: {Files: map[string]string{"users.sql": schemaWithIndex}},
		},
	}
	planResp, err := svc.ExecutePlan(ctx, planReq)
	require.NoError(t, err)
	require.NotEmpty(t, planResp.Changes, "expected DDL changes")

	applyReq := api.ApplyRequest{
		PlanID:      planResp.PlanID,
		Environment: "staging",
		Options:     map[string]string{"allow_unsafe": "true"},
	}
	applyResp, applyID, err := svc.ExecuteApply(ctx, applyReq)
	require.NoError(t, err)
	require.True(t, applyResp.Accepted)
	require.Greater(t, applyID, int64(0))

	// Wait for apply to complete
	require.Eventually(t, func() bool {
		apply, err := svc.Storage().Applies().Get(ctx, applyID)
		if err != nil || apply == nil {
			return false
		}
		return apply.State == "completed"
	}, 30*time.Second, 500*time.Millisecond, "apply should complete")

	// Step 3: Set up fake GitHub and webhook handler
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	// Schema files still have the index (current desired state)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{
		"users.sql": schemaWithIndex,
	}, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	// Get the apply identifier for the rollback command
	storedApply, err := svc.Storage().Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, storedApply)

	// Step 4: Send rollback command with the apply ID
	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: fmt.Sprintf("schemabot rollback %s", storedApply.ApplyIdentifier),
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "rollback started")

	// Step 5: Verify rollback plan comment was posted
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "## MySQL Schema Rollback Plan")
		assert.Contains(t, body, "DROP INDEX", "rollback should drop the index we added")
		assert.Contains(t, body, "schemabot rollback-confirm -e staging")
		assert.Contains(t, body, "schemabot unlock")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for rollback plan comment")
	}

	// Step 6: Verify lock was acquired
	lock, err := svc.Storage().Locks().Get(ctx, dbName, "mysql")
	require.NoError(t, err)
	require.NotNil(t, lock, "lock should be held after rollback command")
	assert.Equal(t, "octocat/hello-world#1", lock.Owner)
}

// TestE2ERollbackApplyNotFound tests rollback with a nonexistent apply ID.
func TestE2ERollbackApplyNotFound(t *testing.T) {
	dbName := "webhook_rollback_none"
	svc := setupE2EService(t, dbName)

	h, comments, _ := newTestHandler(t)
	h.service = svc

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback apply_deadbeef0000",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	select {
	case body := <-comments:
		assert.Contains(t, body, "Apply Not Found")
		assert.Contains(t, body, "apply_deadbeef0000")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for comment")
	}
}

// TestE2ERollbackConfirmNoLock tests rollback-confirm when no lock is held.
func TestE2ERollbackConfirmNoLock(t *testing.T) {
	dbName := "webhook_rbconfirm_nolock"
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}, schemabotConfig, dbName)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)

	req := buildWebhookRequest(t, webhookPayloadOpts{
		comment: "schemabot rollback-confirm -e staging",
		isPR:    true,
	}, nil)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// Should post a "no lock found" comment
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "No Lock Found")
		assert.Contains(t, body, "schemabot rollback -e staging")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for no-lock comment")
	}
}

// TestE2EPRCloseCleanup verifies that closing a PR releases locks and deletes checks.
func TestE2EPRCloseCleanup(t *testing.T) {
	dbName := "webhook_pr_close"
	svc := setupE2EService(t, dbName)

	// Seed a lock and check record for this PR
	err := svc.Storage().Locks().Acquire(t.Context(), &storage.Lock{
		DatabaseName: dbName,
		DatabaseType: "mysql",
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	})
	require.NoError(t, err)

	err = svc.Storage().Checks().Upsert(t.Context(), &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "abc123",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: dbName,
		CheckRunID:   42,
		HasChanges:   true,
		Status:       "completed",
		Conclusion:   "action_required",
	})
	require.NoError(t, err)

	h := NewHandler(svc, nil, nil, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))

	// Send PR closed webhook
	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "closed"}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "PR close cleanup started")

	// Poll until lock is released (cleanup runs async)
	require.Eventually(t, func() bool {
		lock, err := svc.Storage().Locks().Get(t.Context(), dbName, "mysql")
		return err == nil && lock == nil
	}, 5*time.Second, 100*time.Millisecond, "lock should be released on PR close")

	// Poll until check is deleted
	require.Eventually(t, func() bool {
		check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
		return err == nil && check == nil
	}, 5*time.Second, 100*time.Millisecond, "check should be deleted on PR close")
}

// TestE2EStaleCheckCleanup verifies that checks for databases no longer in the PR
// are marked as success on synchronize.
func TestE2EStaleCheckCleanup(t *testing.T) {
	dbName := "webhook_stale_check"
	svc := setupE2EService(t, dbName)

	// Seed a check for a database that WON'T be in the next auto-plan
	err := svc.Storage().Checks().Upsert(t.Context(), &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "old-sha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: "removed_database",
		CheckRunID:   42,
		HasChanges:   true,
		Status:       "completed",
		Conclusion:   "action_required",
	})
	require.NoError(t, err)

	// Also seed a check for the database that WILL be in the auto-plan
	err = svc.Storage().Checks().Upsert(t.Context(), &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      "old-sha",
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: dbName,
		CheckRunID:   43,
		HasChanges:   true,
		Status:       "completed",
		Conclusion:   "action_required",
	})
	require.NoError(t, err)

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}

	setupFakeGitHubForPlan(t, mux, schemaFiles, schemabotConfig, dbName)

	h := newE2EHandler(t, svc, client)

	// Send synchronize event (simulates new commits pushed)
	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "synchronize"}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	// The stale check (removed_database) should be updated to success
	require.Eventually(t, func() bool {
		check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", "removed_database")
		if err != nil || check == nil {
			return false
		}
		return check.Conclusion == "success"
	}, 10*time.Second, 200*time.Millisecond, "stale check should be updated to success")

	// The active check (dbName) should still exist (may be updated by auto-plan)
	check, err := svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check, "active check should still exist")
}

// TestE2EMultiAppAutoPlan simulates a monorepo with multiple apps, each with their
// own schema directory and database. Verifies that a PR touching one app only creates
// checks for that app's database, not for others.
func TestE2EMultiAppAutoPlan(t *testing.T) {
	// Create two databases simulating two apps in a monorepo
	paymentsSvc := setupE2EService(t, "payments")
	ordersSvc := setupE2EService(t, "orders")
	_ = ordersSvc // orders is configured but not touched by this PR

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	client.BaseURL, _ = url.Parse(server.URL + "/")

	// Simulate a monorepo with two apps, each with their own schema dir:
	//   payments-service/mysql/schema/schemabot.yaml  → database: payments
	//   orders-service/mysql/schema/schemabot.yaml    → database: orders
	// The PR only changes payments — orders should NOT be planned.
	paymentsConfig := "database: payments\ntype: mysql\n"
	ordersConfig := "database: orders\ntype: mysql\n"
	transactionsSQL := "CREATE TABLE `transactions` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `amount_cents` bigint NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	auditLogSQL := "CREATE TABLE `audit_log` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `action` varchar(50) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

	result := &planFlowResult{
		comments:  make(chan string, 10),
		reactions: make(chan string, 10),
		checkRuns: make(chan checkRunCapture, 10),
	}

	// PR info
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{Ref: new("feature-branch"), SHA: new("abc123")},
			Base: &gh.PullRequestBranch{Ref: new("main"), SHA: new("def456")},
			User: &gh.User{Login: new("testuser")},
		})
	})

	// PR changed files in payments-service only (two namespaces: payments + payments_audit)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]*gh.CommitFile{
			{Filename: new("payments-service/mysql/schema/payments/transactions.sql"), Status: new("added")},
			{Filename: new("payments-service/mysql/schema/payments_audit/audit_log.sql"), Status: new("added")},
		})
	})

	// Git tree contains BOTH apps' schema files (full repo tree)
	treeEntries := []*gh.TreeEntry{
		// payments app (two namespaces)
		{Path: new("payments-service/mysql/schema/schemabot.yaml"), Mode: new("100644"), Type: new("blob"), SHA: new("configsha_payments"), Size: new(len(paymentsConfig))},
		{Path: new("payments-service/mysql/schema/payments/transactions.sql"), Mode: new("100644"), Type: new("blob"), SHA: new("blobsha_transactions"), Size: new(len(transactionsSQL))},
		{Path: new("payments-service/mysql/schema/payments_audit/audit_log.sql"), Mode: new("100644"), Type: new("blob"), SHA: new("blobsha_audit"), Size: new(len(auditLogSQL))},
		// orders app (not in changed files)
		{Path: new("orders-service/mysql/schema/schemabot.yaml"), Mode: new("100644"), Type: new("blob"), SHA: new("configsha_orders"), Size: new(len(ordersConfig))},
	}

	blobContents := map[string]string{
		"configsha_payments":   paymentsConfig,
		"blobsha_transactions": transactionsSQL,
		"blobsha_audit":        auditLogSQL,
		"configsha_orders":     ordersConfig,
	}

	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/abc123", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.Tree{SHA: new("abc123"), Entries: treeEntries, Truncated: new(false)})
	})

	mux.HandleFunc("GET /repos/octocat/hello-world/git/blobs/", func(w http.ResponseWriter, r *http.Request) {
		sha := r.URL.Path[len("/repos/octocat/hello-world/git/blobs/"):]
		if _, ok := blobContents[sha]; !ok {
			http.NotFound(w, r)
			return
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(blobContents[sha]))
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"sha":"%s","content":"%s","encoding":"base64","size":%d}`, sha, encoded, len(blobContents[sha]))
	})

	mux.HandleFunc("GET /repos/octocat/hello-world/contents/", func(w http.ResponseWriter, r *http.Request) {
		filePath := r.URL.Path[len("/repos/octocat/hello-world/contents/"):]
		if filePath == "payments-service/mysql/schema/schemabot.yaml" {
			_ = json.NewEncoder(w).Encode(gh.RepositoryContent{
				Name:     new("schemabot.yaml"),
				Path:     new("payments-service/mysql/schema/schemabot.yaml"),
				Content:  new(base64.StdEncoding.EncodeToString([]byte(paymentsConfig))),
				Encoding: new("base64"),
			})
			return
		}
		http.NotFound(w, r)
	})

	// Capture comments, reactions, check runs
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		result.comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		result.checkRuns <- body
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})
	mux.HandleFunc("PATCH /repos/octocat/hello-world/check-runs/", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		result.checkRuns <- body
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	// Wire up BOTH databases in the service
	h := newE2EHandler(t, paymentsSvc, client)

	// Send PR opened webhook (triggers auto-plan)
	req := buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "opened"}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	// Should get a plan comment for payments only, showing both namespaces
	select {
	case body := <-result.comments:
		assert.Contains(t, body, "payments", "plan comment should be for payments database")
		assert.NotContains(t, body, "orders", "should NOT plan orders database")
		assert.Contains(t, body, "CREATE TABLE")
		assert.Contains(t, body, "transactions", "should include payments namespace table")
		assert.Contains(t, body, "audit_log", "should include payments_audit namespace table")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for plan comment")
	}

	// Check runs should be for payments only (1 per-database + 1 aggregate).
	// This test uses a single-env setup, so expect exactly 1 per-database check.
	var perDBCheckNames []string
	hasAggregate := false
	deadline := time.After(5 * time.Second)
	for {
		select {
		case cr := <-result.checkRuns:
			if cr.Name == aggregateCheckName {
				hasAggregate = true
			} else {
				perDBCheckNames = append(perDBCheckNames, cr.Name)
			}
			if len(perDBCheckNames) >= 1 && hasAggregate {
				goto checksDone
			}
		case <-deadline:
			goto checksDone
		}
	}
checksDone:
	assert.True(t, hasAggregate, "expected aggregate check run")
	require.NotEmpty(t, perDBCheckNames, "expected at least one per-database check run")
	for _, name := range perDBCheckNames {
		assert.Contains(t, name, "payments", "check run should be for payments: %s", name)
		assert.NotContains(t, name, "orders", "check run should NOT be for orders: %s", name)
	}
}

func e2eContainerName(base string) string {
	out, err := exec.CommandContext(context.Background(), "git", "rev-parse", "--abbrev-ref", "HEAD").Output()
	if err != nil {
		return base
	}
	branch := strings.TrimSpace(string(out))
	if branch == "" || branch == "HEAD" {
		return base
	}
	sanitized := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '-'
	}, branch)
	return base + "-" + sanitized
}
