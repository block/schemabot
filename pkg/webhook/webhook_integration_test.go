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
	"maps"
	"net/http"
	"os"
	"os/exec"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mysql "github.com/block/mysql"
	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/testutil"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/utils"
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

	e2eTargetDSN, err = testutil.MySQLDSN(ctx, targetContainer, "target_test", "parseTime=true")
	if err != nil {
		log.Fatalf("Failed to build target DSN: %v", err)
	}

	sbContainer, err := startE2EMySQLContainer(ctx, "webhook-schemabot-mysql", "schemabot_test", &schema.MySQLFS)
	if err != nil {
		log.Fatalf("Failed to start SchemaBot MySQL: %v", err)
	}

	e2eSchemabotDSN, err = testutil.MySQLDSN(ctx, sbContainer, "schemabot_test", "parseTime=true")
	if err != nil {
		log.Fatalf("Failed to build SchemaBot storage DSN: %v", err)
	}

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
	return setupE2EServiceWithStorage(t, appDBName, nil)
}

// setupE2EServiceWithStorage is setupE2EService with a storage decorator hook.
// A non-nil wrapStorage wraps the service's storage so a test can observe every
// storage write the handler and observers perform (e.g. record stored check
// state transitions).
func setupE2EServiceWithStorage(t *testing.T, appDBName string, wrapStorage func(storage.Storage) storage.Storage) *api.Service {
	t.Helper()
	return setupE2EServiceOpts(t, appDBName, e2eServiceOpts{wrapStorage: wrapStorage})
}

// e2eServiceOpts customizes setupE2EServiceOpts beyond the defaults.
type e2eServiceOpts struct {
	// wrapStorage wraps the service's storage so a test can observe every
	// storage write the handler and observers perform.
	wrapStorage func(storage.Storage) storage.Storage
	// engineMetadata is forwarded to the LocalClient, enabling per-database
	// engine policies (e.g. direct execution) for the test's plans and
	// applies.
	engineMetadata map[string]string
	// skipOperator leaves the operator claim loop stopped so a test can
	// observe the initial durable state of queued work (e.g. a pending
	// apply_operations row) without racing an operator claim.
	skipOperator bool
	// databaseType and targetDSN let integration scenarios exercise another
	// built-in engine while retaining the same API, storage, and operator path.
	// For MySQL, a non-empty targetDSN overrides the default database-scoped
	// DSN — e.g. a namespace-free DSN for scenarios where the namespace
	// selects the database.
	databaseType string
	targetDSN    string
	// preserveDurableState skips the stale-data cleanup so a second service
	// instance can adopt durable work queued by a previous instance — the
	// restart fixtures' crash/recovery seam. Only the first instance of a
	// test may clean; a recovering instance must see the queued rows.
	preserveDurableState bool
}

// namespaceFreeTargetDSN returns the shared MySQL target's DSN with no
// database selected — the shape where each namespace's directory name selects
// the database it is planned against.
func namespaceFreeTargetDSN(t *testing.T) string {
	t.Helper()
	cfg, err := mysql.ParseDSN(e2eTargetDSN)
	require.NoError(t, err)
	cfg.DBName = ""
	return cfg.FormatDSN()
}

// setupE2EServiceOpts creates a real api.Service with a LocalClient for the
// given database, customized by opts.
func setupE2EServiceOpts(t *testing.T, appDBName string, opts e2eServiceOpts) *api.Service {
	t.Helper()
	ctx := t.Context()
	databaseType := opts.databaseType
	if databaseType == "" {
		databaseType = storage.DatabaseTypeMySQL
	}
	appDSN := opts.targetDSN

	if databaseType == storage.DatabaseTypeMySQL {
		// Create the app database on the target.
		targetDB, err := sql.Open("block-mysql", e2eTargetDSN+"&multiStatements=true")
		require.NoError(t, err)
		_, err = targetDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+appDBName+"`")
		require.NoError(t, err)
		utils.CloseAndLog(targetDB)

		t.Cleanup(func() {
			db, err := sql.Open("block-mysql", e2eTargetDSN+"&multiStatements=true")
			if err == nil {
				_, _ = db.ExecContext(t.Context(), "DROP DATABASE IF EXISTS `"+appDBName+"`")
				utils.CloseAndLog(db)
			}
		})

		if appDSN == "" {
			cfg, err := mysql.ParseDSN(e2eTargetDSN)
			require.NoError(t, err)
			cfg.DBName = appDBName
			appDSN = cfg.FormatDSN()
		}
	}
	require.NotEmpty(t, appDSN, "target DSN is required for database type %s", databaseType)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(schemabotDB) })

	st := storage.Storage(mysqlstore.New(schemabotDB))
	if opts.wrapStorage != nil {
		st = opts.wrapStorage(st)
	}

	if !opts.preserveDurableState {
		// Clean up any stale data from previous test runs (shared storage DB).
		// A failed delete must stop the test here: leftover rows surface later
		// as misleading assertion failures. Child apply_operations rows are
		// deleted before their parent applies rows so the operator claim loop
		// cannot re-claim orphan operations whose parent lookup returns nil.
		for _, cleanup := range []struct {
			query string
			args  []any
		}{
			{"DELETE FROM checks WHERE database_name = ?", []any{appDBName}},
			{"DELETE FROM checks WHERE repository = 'octocat/hello-world' AND pull_request = 1", nil},
			{"DELETE FROM locks WHERE repository = 'octocat/hello-world' AND pull_request = 1", nil},
			{"DELETE ao FROM apply_operations ao JOIN applies a ON a.id = ao.apply_id WHERE a.repository = 'octocat/hello-world' AND a.pull_request = 1", nil},
			{"DELETE FROM applies WHERE repository = 'octocat/hello-world' AND pull_request = 1", nil},
			{"DELETE FROM plans WHERE database_name = ?", []any{appDBName}},
		} {
			_, err := schemabotDB.ExecContext(ctx, cleanup.query, cleanup.args...)
			require.NoError(t, err, "stale-data cleanup: %s", cleanup.query)
		}
	}

	localClient, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  appDBName,
		Type:      databaseType,
		TargetDSN: appDSN,
		Metadata:  opts.engineMetadata,
	}, st, logger)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(localClient) })

	serverConfig := &api.ServerConfig{
		Drivers: 1,
		Databases: map[string]api.DatabaseConfig{
			appDBName: {
				Type: databaseType,
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
	// Tests that assert on pre-claim durable state opt out via skipOperator.
	if !opts.skipOperator {
		svc.StartOperator(ctx)
	}
	t.Cleanup(func() { utils.CloseAndLog(svc) })

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
	installClient := ghclient.NewInstallationClientWithSlug(client, logger, "schemabot")
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

	// PRState overrides the lifecycle state served by /pulls/1. Empty (the
	// default) serves "open"; tests exercising the closed-PR command gate set
	// "closed" before issuing the webhook request.
	PRState atomic.Pointer[string]

	// FailCommentPost makes every comment POST fail, so tests can exercise the
	// paths that must not record durable state describing a comment the
	// operator never received.
	FailCommentPost atomic.Bool

	// baseFiles is what the default branch holds, as repository path to blob
	// SHA. It starts as the schema directory carrying the config at a blob of
	// its own, so resolving a PR's schema file at the default branch tip walks
	// the same directory levels a real repository has, and none of the PR's own
	// files are found there. Tests add to it through baseHolds to model a
	// changed-file list that carries a file the PR does not propose.
	baseFiles atomic.Pointer[map[string]string]

	// headTreeEntries is the repository at head, as the fixture described it.
	headTreeEntries []*gh.TreeEntry

	// BaseRef overrides the base branch served by /pulls/1. Nil (the default)
	// serves "main". Tests that model a PR being retargeted install the new ref,
	// optionally partway through a flow so the base a plan was computed against
	// differs from the base it would be published on.
	BaseRef atomic.Pointer[string]

	// PRMerged marks the PR served by /pulls/1 as merged. GitHub reports a
	// merged PR with state "closed" and merged true, so tests exercising the
	// merged-PR rejection copy set this together with PRState "closed".
	PRMerged atomic.Bool

	// CheckStatusNodes optionally overrides the default passing-checks REST
	// responses installed by setupFakeGitHubForPlan. Tests that need to drive
	// different check-gate responses across consecutive calls (e.g. PASS at the
	// early gate, FAIL at the fresh-HEAD re-gate) install their own provider here
	// before issuing the webhook request. Nil preserves the default
	// "no checks → passing" behavior.
	CheckStatusNodes atomic.Pointer[func() []checkStatusNode]

	// FailFirstPRRead makes the first /pulls/1 request answer with a 403 and
	// serves every later one. Auto-plan reads the PR to snapshot its base
	// branch before discovery, so this models a failure to establish which
	// base a plan would be computed against, while leaving the reads that post
	// the resulting check run working. The status has to be one the client
	// treats as a semantic answer rather than an availability failure: a 5xx
	// is retried, and the retry would serve the request and hide the failure
	// the test is asking for.
	FailFirstPRRead atomic.Bool

	// FailPRReadsAfterFirstFetch makes /pulls/1 serve only its first request and
	// answer every later one with a 403. Auto-plan snapshots the PR once at the
	// top of discovery and re-reads it uncached before publishing, so this models
	// SchemaBot losing access to the PR in between — the publish-time
	// re-verification cannot be answered and the plan must not be published. The
	// status is the non-retryable kind on purpose: the client already retries an
	// availability failure, so what reaches that re-verification is a failure
	// retrying does not fix.
	FailPRReadsAfterFirstFetch atomic.Bool

	prReadRequests atomic.Int64

	// FailPRFilesAfterFirstFetch makes /pulls/1/files serve only its first
	// request and answer every later one with a 503. Config discovery fetches
	// the changed files exactly once, so this simulates GitHub becoming
	// unavailable between a successful discovery and the dispatched command's
	// own re-fetch. False (the default) always serves the files.
	FailPRFilesAfterFirstFetch atomic.Bool
	prFilesRequests            atomic.Int64

	// BaseTip overrides the commit SHA that /git/ref/heads/main resolves to.
	// Nil (the default) serves "def456" — the same commit /pulls/1 reports as
	// the PR base, so the base branch appears unmoved and the base-schema
	// freshness gate lets applies proceed. Tests that simulate the base
	// branch advancing after the PR diverged install a provider returning
	// the moved tip and register comparison fixtures for it.
	BaseTip atomic.Pointer[func() string]
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

// registerPassingChecks adds mock REST endpoints for PR check statuses that
// return no checks. This prevents enforcePassingChecks from blocking apply
// commands in e2e tests.
func registerPassingChecks(mux *http.ServeMux) {
	registerCheckStatusRESTHandlers(mux, nil)
}

// fakeCheckRunStore records every Check Run a fake GitHub server accepts so
// the commit check-runs list can serve the same runs the real API would: each
// run's latest state, attributed to the trusted "schemabot" App. The
// aggregate fold resolves its reuse/rewind decision from that list, so a
// harness whose handler can fold the same head more than once must list the
// runs its own create and update calls produced.
type fakeCheckRunStore struct {
	mu   sync.Mutex
	next int64
	runs []*fakeCheckRunRecord
}

type fakeCheckRunRecord struct {
	id         int64
	name       string
	headSHA    string
	status     string
	conclusion string
}

// create records a created Check Run and returns its assigned ID.
func (s *fakeCheckRunStore) create(body checkRunCapture) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	s.runs = append(s.runs, &fakeCheckRunRecord{
		id:         s.next,
		name:       body.Name,
		headSHA:    body.HeadSHA,
		status:     body.Status,
		conclusion: body.Conclusion,
	})
	return s.next
}

// update records an update to an existing run. Update payloads carry no head
// SHA, so an update to an ID the store never saw created (for example a
// seeded stored CheckRunID) is recorded without one and stays invisible to
// every commit's list — like a run that lives on some other commit.
func (s *fakeCheckRunStore) update(id int64, body checkRunCapture) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, run := range s.runs {
		if run.id != id {
			continue
		}
		run.status = body.Status
		run.conclusion = body.Conclusion
		return
	}
	s.runs = append(s.runs, &fakeCheckRunRecord{
		id:         id,
		name:       body.Name,
		status:     body.Status,
		conclusion: body.Conclusion,
	})
}

// serveList writes GitHub's list-check-runs response for the recorded runs on
// headSHA whose name matches checkName, each attributed to the trusted
// "schemabot" App.
func (s *fakeCheckRunStore) serveList(w http.ResponseWriter, headSHA, checkName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]map[string]any, 0, len(s.runs))
	for _, run := range s.runs {
		if run.headSHA != headSHA || run.name != checkName {
			continue
		}
		out = append(out, map[string]any{
			"id":         run.id,
			"name":       run.name,
			"status":     run.status,
			"conclusion": run.conclusion,
			"app":        map[string]any{"slug": "schemabot"},
		})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"total_count": len(out), "check_runs": out})
}

func registerCheckStatusRESTHandlersForAnyRef(mux *http.ServeMux, nodes func() []checkStatusNode, checkRuns *fakeCheckRunStore) {
	prefix := "/repos/octocat/hello-world/commits/"
	mux.HandleFunc("GET "+prefix, func(w http.ResponseWriter, r *http.Request) {
		reqPath := strings.TrimPrefix(r.URL.Path, prefix)
		switch {
		case strings.HasSuffix(reqPath, "/status"):
			writeCommitStatusResponse(w, nodes())
		case strings.HasSuffix(reqPath, "/check-runs"):
			// A name-filtered list is the aggregate fold resolving the newest
			// run for its check name; the unfiltered list is the apply gate
			// reading the PR's checks.
			if name := r.URL.Query().Get("check_name"); name != "" && checkRuns != nil {
				checkRuns.serveList(w, strings.TrimSuffix(reqPath, "/check-runs"), name)
				return
			}
			writeCheckRunsResponse(w, nodes())
		default:
			http.NotFound(w, r)
		}
	})
}

// setupFakeGitHubForPlan sets up a fake GitHub server for plan flows.
// schemaSQL maps filename -> content. Plain filenames are placed under
// schema/{namespace}/; filenames containing a "/" are placed under schema/
// as-is, so a test can lay out multiple namespace subdirectories.
// namespace is the MySQL schema name (required).
func setupFakeGitHubForPlan(t *testing.T, mux *http.ServeMux, schemaSQL map[string]string, schemabotConfig, ns string) *planFlowResult {
	return setupFakeGitHubForPlanWithPRFiles(t, mux, schemaSQL, schemabotConfig, ns, nil)
}

// baseHolds adds the given repository paths to what the default branch already
// carries: each at the blob head has for it when head has it too — a file the
// PR's changed-file list carries but that merging it would not change — and at
// a blob of its own otherwise, which is what a file the PR deletes looks like.
func (r *planFlowResult) baseHolds(paths ...string) {
	held := make(map[string]string, len(paths))
	if existing := r.baseFiles.Load(); existing != nil {
		maps.Copy(held, *existing)
	}
	for _, path := range paths {
		held[path] = "basesha~" + path
		for _, entry := range r.headTreeEntries {
			if entry.GetPath() == path {
				held[path] = entry.GetSHA()
			}
		}
	}
	r.baseFiles.Store(&held)
}

// baseTreeEntries is the repository as the default branch holds it.
func (r *planFlowResult) baseTreeEntries() []*gh.TreeEntry {
	held := r.baseFiles.Load()
	if held == nil {
		return nil
	}
	entries := make([]*gh.TreeEntry, 0, len(*held))
	for path, blobSHA := range *held {
		entries = append(entries, &gh.TreeEntry{Path: new(path), Type: new("blob"), SHA: new(blobSHA)})
	}
	slices.SortFunc(entries, func(a, b *gh.TreeEntry) int { return strings.Compare(a.GetPath(), b.GetPath()) })
	return entries
}

// Synthesized tree identifiers for directories below a root level, one prefix
// per branch so the head and the default branch resolve through distinct nodes.
// Directory separators are encoded so an identifier stays one URL path segment,
// as a real tree SHA is.
const (
	headSubtreeSHAPrefix = "subtree~"
	baseSubtreeSHAPrefix = "basetree~"
)

// repositoryTreeFixture describes a repository to the tree endpoints at the two
// commits a plan flow reads: the PR head, and the default branch tip that
// discovery scopes the PR's changed files against. Both are given as the flat
// path lists a recursive tree read returns.
type repositoryTreeFixture struct {
	headSHA     string
	headEntries []*gh.TreeEntry
	// baseEntries is what the default branch holds. Nil means it holds none of
	// the head's files, so every changed file is the PR's own proposal.
	baseEntries func() []*gh.TreeEntry
	// defaultTip is the commit the default branch resolves to. Nil serves the
	// PR's base commit, so the base branch appears unmoved.
	defaultTip func() string
}

func (f repositoryTreeFixture) base() []*gh.TreeEntry {
	if f.baseEntries == nil {
		return nil
	}
	return f.baseEntries()
}

func (f repositoryTreeFixture) tip() string {
	if f.defaultTip == nil {
		return "def456"
	}
	return f.defaultTip()
}

// shallowTreeLevel serves one directory level. treeSHA is either a commit (a
// root level) or a synthesized subtree identifier, and which commit it belongs
// to decides which entry list the level is built from. A commit the fixture
// does not name as the default branch tip is a head commit, so a test that
// advances the PR head needs no fixture of its own.
func (f repositoryTreeFixture) shallowTreeLevel(treeSHA string) []*gh.TreeEntry {
	if encoded, isBase := strings.CutPrefix(treeSHA, baseSubtreeSHAPrefix); isBase {
		return treeLevelFrom(f.base(), strings.ReplaceAll(encoded, "~", "/"), baseSubtreeSHAPrefix)
	}
	if encoded, isHead := strings.CutPrefix(treeSHA, headSubtreeSHAPrefix); isHead {
		return treeLevelFrom(f.headEntries, strings.ReplaceAll(encoded, "~", "/"), headSubtreeSHAPrefix)
	}
	if treeSHA == f.tip() {
		return treeLevelFrom(f.base(), "", baseSubtreeSHAPrefix)
	}
	return treeLevelFrom(f.headEntries, "", headSubtreeSHAPrefix)
}

// registerRepositoryTrees serves the repository reads a plan flow makes: the
// default branch and its tip, and the git trees at head and at that tip. A
// shallow read walks one directory level at a time; a recursive read wants the
// whole tree flat. Both hit the same path, so serve what was asked for: the
// recursive form is what config discovery reads, and the level form is what the
// path resolvers walk.
func registerRepositoryTrees(t *testing.T, mux *http.ServeMux, fixture repositoryTreeFixture) {
	t.Helper()

	mux.HandleFunc("GET /repos/octocat/hello-world", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.Repository{DefaultBranch: new("main")}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/ref/heads/main", func(w http.ResponseWriter, _ *http.Request) {
		tip := fixture.tip()
		require.NoError(t, json.NewEncoder(w).Encode(gh.Reference{
			Ref:    new("refs/heads/main"),
			Object: &gh.GitObject{Type: new("commit"), SHA: &tip},
		}))
	})

	serveTree := func(w http.ResponseWriter, r *http.Request, sha string) {
		entries := fixture.headEntries
		if r.URL.Query().Get("recursive") == "" {
			entries = fixture.shallowTreeLevel(sha)
		}
		require.NoError(t, json.NewEncoder(w).Encode(gh.Tree{
			SHA:       &sha,
			Entries:   entries,
			Truncated: new(false),
		}))
	}
	// Go 1.22 ServeMux gives precedence to the more specific exact path, so the
	// head commit keeps its own route and every other SHA falls through to the
	// prefix — a test that advances the head needs no fixture per commit.
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/"+fixture.headSHA, func(w http.ResponseWriter, r *http.Request) {
		serveTree(w, r, fixture.headSHA)
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/", func(w http.ResponseWriter, r *http.Request) {
		serveTree(w, r, r.URL.Path[len("/repos/octocat/hello-world/git/trees/"):])
	})
}

// treeLevelFrom returns the entries directly under dir, minting subtree
// identifiers for deeper directories with subtreePrefix.
func treeLevelFrom(entries []*gh.TreeEntry, dir, subtreePrefix string) []*gh.TreeEntry {
	pathPrefix := ""
	if dir != "" {
		pathPrefix = dir + "/"
	}
	var level []*gh.TreeEntry
	seen := map[string]bool{}
	for _, entry := range entries {
		rest, under := strings.CutPrefix(entry.GetPath(), pathPrefix)
		if !under || rest == "" {
			continue
		}
		segment, deeper, isDir := strings.Cut(rest, "/")
		if seen[segment] {
			continue
		}
		seen[segment] = true
		if isDir && deeper != "" {
			level = append(level, &gh.TreeEntry{
				Path: new(segment),
				Type: new("tree"),
				SHA:  new(subtreePrefix + strings.ReplaceAll(pathPrefix+segment, "/", "~")),
			})
			continue
		}
		level = append(level, &gh.TreeEntry{Path: new(segment), Type: new("blob"), SHA: new(entry.GetSHA())})
	}
	return level
}

// schemaFixturePath resolves a schemaSQL key to its repository path: plain
// filenames live under the namespace subdirectory, keys with a "/" already
// name their namespace subdirectory.
func schemaFixturePath(ns, name string) string {
	if strings.Contains(name, "/") {
		return "schema/" + name
	}
	return "schema/" + ns + "/" + name
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
		read := result.prReadRequests.Add(1)
		if result.FailFirstPRRead.Load() && read == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"message":"Resource not accessible by integration"}`))
			return
		}
		if result.FailPRReadsAfterFirstFetch.Load() && read > 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"message":"Resource not accessible by integration"}`))
			return
		}
		sha := result.nextHeadSHA()
		prState := "open"
		if override := result.PRState.Load(); override != nil {
			prState = *override
		}
		merged := result.PRMerged.Load()
		baseRef := "main"
		if override := result.BaseRef.Load(); override != nil {
			baseRef = *override
		}
		_ = json.NewEncoder(w).Encode(gh.PullRequest{
			State:  &prState,
			Merged: &merged,
			Head: &gh.PullRequestBranch{
				Ref: new("feature-branch"),
				SHA: &sha,
			},
			Base: &gh.PullRequestBranch{
				Ref: &baseRef,
				SHA: new("def456"),
			},
			User: &gh.User{Login: new("testuser")},
		})
	})

	registerFreshBaseComparison(t, mux, "abc123")

	// PR changed files — report schema files changed (in namespace subdir)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, r *http.Request) {
		if result.FailPRFilesAfterFirstFetch.Load() && result.prFilesRequests.Add(1) > 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if prFiles != nil {
			_ = json.NewEncoder(w).Encode(prFiles)
			return
		}
		var files []*gh.CommitFile
		for name := range schemaSQL {
			files = append(files, &gh.CommitFile{
				Filename: new(schemaFixturePath(ns, name)),
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
			Path: new(schemaFixturePath(ns, name)),
			Mode: new("100644"),
			Type: new("blob"),
			SHA:  new(sha),
			Size: new(len(content)),
		})
	}
	result.headTreeEntries = treeEntries

	// The default branch already has the schema directory and the config in it,
	// at a blob of its own. Resolving one of the PR's schema files at the default
	// branch tip then descends the directory levels a real repository has instead
	// of stopping at an empty root, and still finds none of the PR's own files
	// there — so every changed file remains the PR's own proposal.
	if schemabotConfig != "" {
		result.baseFiles.Store(&map[string]string{"schema/schemabot.yaml": "baseconfigsha001"})
	}

	// The repository at both commits a plan flow reads. By default the default
	// branch tip is the PR's own base commit, so the base branch appears
	// unmoved — the schema tree cannot differ from itself, and the base-schema
	// freshness gate lets applies proceed.
	registerRepositoryTrees(t, mux, repositoryTreeFixture{
		headSHA:     "abc123",
		headEntries: treeEntries,
		baseEntries: result.baseTreeEntries,
		defaultTip: func() string {
			if provider := result.BaseTip.Load(); provider != nil {
				return (*provider)()
			}
			return "def456"
		},
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
		if result.FailCommentPost.Load() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"message":"Server Error"}`))
			return
		}
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

	// Capture check run creates and updates. Every write is also recorded in
	// checkRunState (with incrementing IDs) so the commit's check-runs list
	// serves the runs this server actually accepted — the aggregate fold
	// resolves its reuse/rewind decision from that list.
	checkRunState := &fakeCheckRunStore{}
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		result.checkRuns <- body
		id := checkRunState.create(body)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id})
	})

	mux.HandleFunc("PATCH /repos/octocat/hello-world/check-runs/", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		_ = json.NewDecoder(r.Body).Decode(&body)
		result.checkRuns <- body
		id, err := strconv.ParseInt(path.Base(r.URL.Path), 10, 64)
		if err != nil {
			http.Error(w, "invalid check run id", http.StatusBadRequest)
			return
		}
		checkRunState.update(id, body)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id})
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
	}, checkRunState)

	return result
}

// registerFreshBaseComparison serves the merge-base comparison for a head
// whose merge base is the unmoved base tip "def456", so the base-schema
// freshness gate sees no base-branch schema change for that head. Tests that
// serve a head other than the default "abc123" register their head here.
func registerFreshBaseComparison(t *testing.T, mux *http.ServeMux, headSHA string) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/def456..."+headSHA, func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.CommitsComparison{
			MergeBaseCommit: &gh.RepositoryCommit{SHA: new("def456")},
		}))
	})
}

func registerCompareFiles(t *testing.T, mux *http.ServeMux, baseSHA, headSHA string, files []*gh.CommitFile) {
	t.Helper()

	mux.HandleFunc("GET /repos/octocat/hello-world/compare/"+baseSHA+"..."+headSHA, func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(gh.CommitsComparison{Status: new("ahead"), Files: files}))
	})
}

// setupE2EServiceMultiEnv creates a real api.Service with staging and production environments.
// Each environment gets its own database on the target container.
func setupE2EServiceMultiEnv(t *testing.T, appDBName string) *api.Service {
	t.Helper()
	ctx := t.Context()

	targetDB, err := sql.Open("block-mysql", e2eTargetDSN+"&multiStatements=true")
	require.NoError(t, err)

	stagingDB := appDBName + "_staging"
	productionDB := appDBName + "_production"

	_, err = targetDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+stagingDB+"`")
	require.NoError(t, err)
	_, err = targetDB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+productionDB+"`")
	require.NoError(t, err)
	utils.CloseAndLog(targetDB)

	t.Cleanup(func() {
		db, err := sql.Open("block-mysql", e2eTargetDSN+"&multiStatements=true")
		if err == nil {
			_, _ = db.ExecContext(t.Context(), "DROP DATABASE IF EXISTS `"+stagingDB+"`")
			_, _ = db.ExecContext(t.Context(), "DROP DATABASE IF EXISTS `"+productionDB+"`")
			utils.CloseAndLog(db)
		}
	})

	stagingDSN := strings.Replace(e2eTargetDSN, "/target_test", "/"+stagingDB, 1)
	productionDSN := strings.Replace(e2eTargetDSN, "/target_test", "/"+productionDB, 1)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(schemabotDB) })

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
	t.Cleanup(func() { utils.CloseAndLog(stagingClient) })

	productionClient, err := tern.NewLocalClient(tern.LocalConfig{
		Database:  appDBName,
		Type:      "mysql",
		TargetDSN: productionDSN,
	}, st, logger)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(productionClient) })

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
	t.Cleanup(func() { utils.CloseAndLog(svc) })

	return svc
}

func startE2EMySQLContainer(ctx context.Context, baseName, dbName string, schemaFS *embed.FS) (testcontainers.Container, error) {
	req := testutil.MySQLContainerRequest("mysql:8.0", dbName)
	req.Name = e2eContainerName(baseName)

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            os.Getenv("DEBUG") != "",
	})
	if err != nil {
		return nil, err
	}

	if schemaFS != nil {
		dsn, err := testutil.MySQLDSN(ctx, container, dbName, "parseTime=true", "multiStatements=true")
		if err != nil {
			_ = container.Terminate(ctx)
			return nil, fmt.Errorf("build mysql dsn: %w", err)
		}
		db, err := sql.Open("block-mysql", dsn)
		if err != nil {
			_ = container.Terminate(ctx)
			return nil, fmt.Errorf("open db: %w", err)
		}
		defer utils.CloseAndLog(db)

		if err := testutil.PingMySQL(ctx, db); err != nil {
			_ = container.Terminate(ctx)
			return nil, err
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
	return setupE2EServiceWithConfig(t, &api.ServerConfig{AllowedEnvironments: allowedEnvs})
}

// setupE2EServiceWithConfig builds an E2E service against the shared schemabot
// test database using the given server config, so tests can exercise
// repo-scoped behavior (e.g. aggregate roles) beyond just allowed environments.
func setupE2EServiceWithConfig(t *testing.T, serverConfig *api.ServerConfig) *api.Service {
	t.Helper()

	schemabotDB, err := sql.Open("block-mysql", e2eSchemabotDSN)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(schemabotDB) })

	st := mysqlstore.New(schemabotDB)

	// Clean up stale data
	_, _ = schemabotDB.ExecContext(t.Context(),
		"DELETE FROM checks WHERE repository = 'octocat/hello-world' AND pull_request = 1")
	_, _ = schemabotDB.ExecContext(t.Context(),
		"DELETE FROM locks WHERE repository = 'octocat/hello-world' AND pull_request = 1")

	svc := api.New(st, serverConfig, nil, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	t.Cleanup(func() { utils.CloseAndLog(svc) })

	return svc
}
