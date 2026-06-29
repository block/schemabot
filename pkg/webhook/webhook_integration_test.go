//go:build integration

// Webhook integration tests exercise the full plan flow end to end, against
// in-process services and MySQL testcontainers (not the e2e/ Docker stack).
// Shared setup, fake-GitHub wiring, and testcontainer helpers live here; the
// tests themselves are grouped by theme into sibling *_integration_test.go files.
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
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
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

const (
	webhookIntegrationPollDeadline     = 30 * time.Second
	webhookIntegrationCheckRunDeadline = 10 * time.Second
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
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM locks WHERE repository = 'octocat/hello-world' AND pull_request = 1")
	// Delete child apply_operations rows before their parent applies rows so the
	// operator claim loop cannot re-claim orphan operations whose parent lookup
	// returns nil.
	_, _ = schemabotDB.ExecContext(ctx, "DELETE ao FROM apply_operations ao JOIN applies a ON a.id = ao.apply_id WHERE a.repository = 'octocat/hello-world' AND a.pull_request = 1")
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM applies WHERE repository = 'octocat/hello-world' AND pull_request = 1")
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM plans WHERE database_name = ?", appDBName)

	localClient, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  appDBName,
		Type:      "mysql",
		TargetDSN: appDSN,
	}, st, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = localClient.Close() })

	serverConfig := &api.ServerConfig{
		Drivers: 1,
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
	require.NoError(t, svc.SetOperatorPollInterval(100*time.Millisecond))
	// Webhook E2E helpers use the real service lifecycle. Local applies are
	// durably queued by ExecuteApply, then dispatched by operator drivers.
	svc.StartOperator(ctx)
	t.Cleanup(func() { _ = svc.Close() })

	return svc
}

func configureE2EServiceEnvironments(t *testing.T, svc *api.Service, dbName string, environments ...string) {
	t.Helper()
	config := svc.Config()
	if config.Databases == nil {
		config.Databases = make(map[string]api.DatabaseConfig)
	}
	dbConfig := config.Databases[dbName]
	if dbConfig.Type == "" {
		dbConfig.Type = "mysql"
	}
	if dbConfig.Environments == nil {
		dbConfig.Environments = make(map[string]api.EnvironmentConfig)
	}
	stagingConfig := dbConfig.Environments["staging"]
	for _, environment := range environments {
		if _, ok := dbConfig.Environments[environment]; ok {
			continue
		}
		dbConfig.Environments[environment] = stagingConfig
	}
	config.Databases[dbName] = dbConfig
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

	// HeadSHAs drives the SHA returned by /pulls/1 on each successive call.
	// Empty (the default) means the handler always returns "abc123" — the
	// historical behavior preserved for all existing tests. Tests that need
	// to simulate the PR HEAD advancing between the cached FetchPullRequest
	// call and a later FetchPullRequestNoCache call populate this slice
	// (e.g. {"abc123", "newsha456"}). Out-of-range calls return the last
	// element.
	HeadSHAs    []string
	headSHACall atomic.Int64

	// CheckStatusNodes optionally overrides the default passing-checks REST
	// responses installed by setupFakeGitHubForPlan. Tests that need to drive
	// different check-gate responses across consecutive calls (e.g. PASS at the
	// early gate, FAIL at the fresh-HEAD re-gate) install their own provider here
	// before issuing the webhook request. Nil preserves the default
	// "no checks → passing" behavior.
	CheckStatusNodes atomic.Pointer[func() []checkStatusNode]
}

func (p *planFlowResult) nextHeadSHA() string {
	if len(p.HeadSHAs) == 0 {
		return "abc123"
	}
	idx := int(p.headSHACall.Add(1) - 1)
	if idx >= len(p.HeadSHAs) {
		idx = len(p.HeadSHAs) - 1
	}
	return p.HeadSHAs[idx]
}

type checkRunCapture struct {
	Name       string                   `json:"name"`
	HeadSHA    string                   `json:"head_sha"`
	Status     string                   `json:"status"`
	Conclusion string                   `json:"conclusion"`
	Output     *ghclient.CheckRunOutput `json:"output"`
}

// registerPassingChecks adds mock REST endpoints for PR check statuses that
// return no checks. This prevents enforcePassingChecks from blocking apply
// commands in e2e tests.
func registerPassingChecks(mux *http.ServeMux) {
	registerCheckStatusRESTHandlers(mux, nil)
}

func registerCheckStatusRESTHandlersForAnyRef(mux *http.ServeMux, nodes func() []checkStatusNode) {
	prefix := "/repos/octocat/hello-world/commits/"
	mux.HandleFunc("GET "+prefix, func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, prefix)
		switch {
		case strings.HasSuffix(path, "/status"):
			writeCommitStatusResponse(w, nodes())
		case strings.HasSuffix(path, "/check-runs"):
			writeCheckRunsResponse(w, nodes())
		default:
			http.NotFound(w, r)
		}
	})
}

// setupFakeGitHubForPlan sets up a fake GitHub server for plan flows.
// schemaSQL maps filename -> content. Files are placed under schema/{namespace}/.
// namespace is the MySQL schema name (required).
func setupFakeGitHubForPlan(t *testing.T, mux *http.ServeMux, schemaSQL map[string]string, schemabotConfig, ns string) *planFlowResult {
	return setupFakeGitHubForPlanWithPRFiles(t, mux, schemaSQL, schemabotConfig, ns, nil)
}

func setupFakeGitHubForPlanWithPRFiles(t *testing.T, mux *http.ServeMux, schemaSQL map[string]string, schemabotConfig, ns string, prFiles []*gh.CommitFile) *planFlowResult {
	t.Helper()

	result := &planFlowResult{
		comments:  make(chan string, 10),
		reactions: make(chan string, 10),
		checkRuns: make(chan checkRunCapture, 10),
	}

	// PR info — head SHA can shift across calls via result.HeadSHAs (default
	// preserves the historical "abc123" for every existing test).
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, r *http.Request) {
		sha := result.nextHeadSHA()
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			Head: &gh.PullRequestBranch{
				Ref: new("feature-branch"),
				SHA: &sha,
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
		if prFiles != nil {
			_ = json.NewEncoder(w).Encode(prFiles)
			return
		}
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

	// Git tree (recursive). The exact-SHA handler preserves the historical
	// "abc123" behavior for tests that only exercise one head; the prefix
	// fallback serves the same tree for any other SHA so tests that simulate
	// HEAD advancing (e.g. the cross-delivery freshness checks) don't need a
	// custom fixture per SHA. Go 1.22 ServeMux gives precedence to the more
	// specific exact path, so existing tests are unaffected.
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/abc123", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(gh.Tree{
			SHA:       new("abc123"),
			Entries:   treeEntries,
			Truncated: new(false),
		})
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/", func(w http.ResponseWriter, r *http.Request) {
		sha := r.URL.Path[len("/repos/octocat/hello-world/git/trees/"):]
		_ = json.NewEncoder(w).Encode(gh.Tree{
			SHA:       &sha,
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

	// PR check statuses for enforcePassingChecks. Defaults to all passing
	// (empty REST responses) so existing tests are unaffected. Tests that need to
	// drive different responses across calls install a provider in
	// result.CheckStatusNodes before issuing the webhook request.
	registerCheckStatusRESTHandlersForAnyRef(mux, func() []checkStatusNode {
		if provider := result.CheckStatusNodes.Load(); provider != nil {
			return (*provider)()
		}
		return nil
	})

	return result
}

func registerCompareFiles(t *testing.T, mux *http.ServeMux, baseSHA, headSHA string, files []*gh.CommitFile) {
	t.Helper()

	mux.HandleFunc("GET /repos/octocat/hello-world/compare/"+baseSHA+"..."+headSHA, func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.CommitsComparison{Files: files}))
	})
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
	_, _ = schemabotDB.ExecContext(ctx, "DELETE FROM locks WHERE repository = 'octocat/hello-world' AND pull_request = 1")
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

// setupE2EServiceWithAllowedEnvs creates a service with AllowedEnvironments set.
// Uses the shared SchemaBot storage DB but no target databases (for testing check run
// posting without needing to plan).
func setupE2EServiceWithAllowedEnvs(t *testing.T, allowedEnvs []string) *api.Service {
	t.Helper()

	schemabotDB, err := sql.Open("mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = schemabotDB.Close() })

	st := mysqlstore.New(schemabotDB)

	// Clean up stale data
	_, _ = schemabotDB.ExecContext(t.Context(),
		"DELETE FROM checks WHERE repository = 'octocat/hello-world' AND pull_request = 1")
	_, _ = schemabotDB.ExecContext(t.Context(),
		"DELETE FROM locks WHERE repository = 'octocat/hello-world' AND pull_request = 1")

	serverConfig := &api.ServerConfig{
		AllowedEnvironments: allowedEnvs,
	}

	svc := api.New(st, serverConfig, nil, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	t.Cleanup(func() { _ = svc.Close() })

	return svc
}
