//go:build integration

package webhook

import (
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/checkstate"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// oversizedPRFixture is a pull request that changes one schema file in each of
// several registered databases, served by a fake GitHub that counts every
// request it receives.
type oversizedPRFixture struct {
	svc      *api.Service
	h        *Handler
	client   *ghclient.InstallationClient
	mux      *http.ServeMux
	result   *planFlowResult
	requests *githubRequestCounter
}

// setupOversizedPR registers dbName plus each of otherDatabases with the
// service, lays out one schemabot.yaml and one changed schema file per
// database, and applies limits.
func setupOversizedPR(t *testing.T, dbName string, otherDatabases []string, limits api.AutoPlanLimitsConfig) *oversizedPRFixture {
	t.Helper()
	svc := setupE2EService(t, dbName)
	serverConfig := svc.Config()
	serverConfig.AutoPlanLimits = limits
	for _, other := range otherDatabases {
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

	configs := make(map[string]string)
	schemaFiles := make(map[string]string)
	for _, database := range append([]string{dbName}, otherDatabases...) {
		configs["schema/"+database+"/schemabot.yaml"] = fmt.Sprintf("database: %s\ntype: mysql\n", database)
		schemaFiles[database+"/users.sql"] = "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
	}
	result := setupFakeGitHubForPlanWithConfigs(t, mux, schemaFiles, configs, dbName, nil)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	installClient := ghclient.NewInstallationClientWithSlug(client, logger, "schemabot")
	return &oversizedPRFixture{
		svc:      svc,
		h:        NewHandler(svc, &fakeClientFactory{client: installClient}, nil, logger),
		client:   installClient,
		mux:      mux,
		result:   result,
		requests: requests,
	}
}

// deliver sends req to the handler and waits for the comment it posts.
func (f *oversizedPRFixture) deliver(t *testing.T, req *http.Request) string {
	t.Helper()
	rr := httptest.NewRecorder()
	f.h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	select {
	case body := <-f.result.comments:
		return body
	case <-time.After(webhookIntegrationPollDeadline):
		t.Fatal("timed out waiting for a PR comment")
		return ""
	}
}

// schemaReads lists the requests that read schema file content, whether by
// blob or through the contents API.
func (f *oversizedPRFixture) schemaReads(t *testing.T) []string {
	t.Helper()
	f.h.DrainInProcessWebhookWork(t.Context())
	var reads []string
	for _, key := range slices.Sorted(maps.Keys(f.requests.snapshot())) {
		if strings.Contains(key, "/git/blobs/") || (strings.Contains(key, "/contents/") && strings.HasSuffix(key, ".sql")) {
			reads = append(reads, key)
		}
	}
	return reads
}

// waitForFailingAggregate waits for the aggregate Check Run to be written as
// a failure and returns it.
func (f *oversizedPRFixture) waitForFailingAggregate(t *testing.T) checkRunCapture {
	t.Helper()
	deadline := time.After(webhookIntegrationCheckRunDeadline)
	for {
		select {
		case run := <-f.result.checkRuns:
			if run.Conclusion == "failure" {
				return run
			}
		case <-deadline:
			t.Fatal("timed out waiting for a failing aggregate Check Run")
		}
	}
}

// autoPlan runs one automatic plan of the PR at headSHA directly, so the test
// sees the error a durable delivery would retry on, then waits for the work it
// started to finish.
func (f *oversizedPRFixture) autoPlan(t *testing.T, action, beforeSHA, headSHA string) (string, error) {
	t.Helper()
	outcome, err := f.h.runAutoPlanForPR(t.Context(), f.client, "octocat/hello-world", 1, headSHA, 0,
		"pull_request."+action, action, beforeSHA, "")
	f.h.DrainInProcessWebhookWork(t.Context())
	return outcome, err
}

// storedAggregate reads the PR's stored aggregate check state.
func (f *oversizedPRFixture) storedAggregate(t *testing.T) *storage.Check {
	t.Helper()
	stored, err := f.svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, aggregateSentinel, aggregateSentinel, aggregateSentinel)
	require.NoError(t, err)
	return stored
}

// postedComments drains the comments posted so far.
func (f *oversizedPRFixture) postedComments() []string {
	var comments []string
	for {
		select {
		case body := <-f.result.comments:
			comments = append(comments, body)
		default:
			return comments
		}
	}
}

// TestE2EAutoPlanRefusesPRLargerThanOneAutomaticPlan covers a PR that changes
// schema in three databases on a deployment that plans at most two at once.
// The automatic plan reads no schema file: it fails the aggregate check closed
// with a stored block, and posts one comment naming the counts, the limit, and
// a command that plans one database. While the block stands, a command across
// every database asks which one without reading schema, `schemabot apply -d`
// is refused, and `schemabot plan -d` still plans the one database it names
// without clearing the block.
func TestE2EAutoPlanRefusesPRLargerThanOneAutomaticPlan(t *testing.T) {
	dbName := "webhook_autoplan_size_limit"
	f := setupOversizedPR(t, dbName, []string{"size_limit_ledger", "size_limit_billing"},
		api.AutoPlanLimitsConfig{MaxDatabases: 2})

	body := f.deliver(t, buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil))
	assert.Contains(t, body, "This PR is too large to plan automatically")
	assert.Contains(t, body, "It changes schema in 3 databases, and SchemaBot plans at most 2 databases in one PR.")
	assert.Contains(t, body, "schemabot plan -d size_limit_billing")
	assert.Contains(t, body, "`size_limit_billing`, `size_limit_ledger`, `"+dbName+"`")

	run := f.waitForFailingAggregate(t)
	require.NotNil(t, run.Output)
	assert.Equal(t, "Plan failed", run.Output.Title)
	assert.Contains(t, run.Output.Summary, "It changes schema in 3 databases, and SchemaBot plans at most 2 databases in one PR.")
	assert.Empty(t, f.schemaReads(t), "an automatic plan over the limit reads no schema file")

	stored, err := f.svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, aggregateSentinel, aggregateSentinel, aggregateSentinel)
	require.NoError(t, err)
	require.NotNil(t, stored, "the aggregate check state is stored")
	assert.Equal(t, checkstate.BlockAutoPlanSizeLimit, stored.BlockingReason)
	assert.Equal(t, "abc123", stored.HeadSHA)

	body = f.deliver(t, buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot apply -e staging", isPR: true}, nil))
	assert.Contains(t, body, "Multiple Databases Detected", "an apply across several databases asks which one")
	body = f.deliver(t, buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot plan", isPR: true}, nil))
	assert.Contains(t, body, "Multiple Databases Detected", "a plan across several databases asks which one")
	assert.Empty(t, f.schemaReads(t), "a command across several databases reads no schema file")

	body = f.deliver(t, buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot apply -e staging -d " + dbName, isPR: true}, nil))
	assert.Contains(t, body, "This PR is too large to plan automatically, so SchemaBot will not apply it.")
	assert.NotContains(t, body, "Schema Change Plan")
	body = f.deliver(t, buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot apply-confirm -e staging -d " + dbName, isPR: true}, nil))
	assert.Contains(t, body, "This PR is too large to plan automatically, so SchemaBot will not apply it.", "a confirmation cannot run an apply the block refuses")

	body = f.deliver(t, buildWebhookRequest(t, webhookPayloadOpts{comment: "schemabot plan -d " + dbName + " -e staging", isPR: true}, nil))
	assert.Contains(t, body, "Schema Change Plan")
	assert.Contains(t, body, "`users`")
	assert.NotEmpty(t, f.schemaReads(t), "an explicit plan of one database reads its schema")

	stored, err = f.svc.Storage().Checks().Get(t.Context(), "octocat/hello-world", 1, aggregateSentinel, aggregateSentinel, aggregateSentinel)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, checkstate.BlockAutoPlanSizeLimit, stored.BlockingReason, "planning one database leaves the PR blocked")
}

// TestE2EAutoPlanRefusesPRWithTooManySchemaFiles covers a PR across two
// databases that changes more schema files than one automatic plan covers:
// the comment names the file count and the file limit.
func TestE2EAutoPlanRefusesPRWithTooManySchemaFiles(t *testing.T) {
	dbName := "webhook_autoplan_file_limit"
	f := setupOversizedPR(t, dbName, []string{"file_limit_ledger"},
		api.AutoPlanLimitsConfig{MaxSchemaFiles: 1})

	body := f.deliver(t, buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil))
	assert.Contains(t, body, "It changes 2 schema files across 2 databases, and SchemaBot plans at most 1 schema files in one PR that changes more than one database.")
	f.waitForFailingAggregate(t)
	assert.Empty(t, f.schemaReads(t), "an automatic plan over the limit reads no schema file")
}

// TestE2EAutoPlanPlansPRAtTheLimit covers a PR that changes schema in exactly
// as many databases as one automatic plan covers: it plans as usual.
func TestE2EAutoPlanPlansPRAtTheLimit(t *testing.T) {
	dbName := "webhook_autoplan_at_limit"
	f := setupOversizedPR(t, dbName, nil, api.AutoPlanLimitsConfig{MaxDatabases: 1, MaxSchemaFiles: 1})

	body := f.deliver(t, buildPRWebhookRequest(t, prWebhookPayloadOpts{
		action:  "opened",
		headSHA: "abc123",
		headRef: "feature-branch",
	}, nil))
	assert.Contains(t, body, "Schema Change Plan")
	assert.NotContains(t, body, "too large to plan automatically")
	assert.NotEmpty(t, f.schemaReads(t))
}

// TestE2EAutoPlanSizeRefusalRetriesWhenItsBlockIsNotStored covers a refusal
// whose failing aggregate Check Run GitHub rejects. The block is not stored,
// so the run returns an error a durable delivery retries on, and posts no
// comment claiming a block that does not exist. The retry stores the block and
// posts the comment.
func TestE2EAutoPlanSizeRefusalRetriesWhenItsBlockIsNotStored(t *testing.T) {
	dbName := "webhook_autoplan_size_retry"
	f := setupOversizedPR(t, dbName, []string{"size_retry_ledger"}, api.AutoPlanLimitsConfig{MaxDatabases: 1})

	f.result.FailCheckRunCreate.Store(true)
	_, err := f.autoPlan(t, "opened", "", "abc123")
	require.Error(t, err, "a refusal that stored no block is retried")
	assert.Nil(t, f.storedAggregate(t), "no block is stored")
	assert.Empty(t, f.postedComments(), "no comment claims a block that was not stored")

	f.result.FailCheckRunCreate.Store(false)
	_, err = f.autoPlan(t, "opened", "", "abc123")
	require.NoError(t, err)
	stored := f.storedAggregate(t)
	require.NotNil(t, stored)
	assert.Equal(t, checkstate.BlockAutoPlanSizeLimit, stored.BlockingReason)
	assert.Equal(t, "abc123", stored.HeadSHA)
	comments := f.postedComments()
	require.Len(t, comments, 1)
	assert.Contains(t, comments[0], "This PR is too large to plan automatically")
}

// TestE2EAutoPlanSizeRefusalDiscardedWhenHeadMoved covers a refusal computed
// for a head the PR has already moved past. It stores no block and posts no
// comment, since the push that moved the head plans the PR again.
func TestE2EAutoPlanSizeRefusalDiscardedWhenHeadMoved(t *testing.T) {
	dbName := "webhook_autoplan_size_stale"
	f := setupOversizedPR(t, dbName, []string{"size_stale_ledger"}, api.AutoPlanLimitsConfig{MaxDatabases: 1})

	newer := "newer456"
	f.result.MovedHeadSHA.Store(&newer)
	outcome, err := f.autoPlan(t, "opened", "", "abc123")
	require.NoError(t, err)
	assert.Equal(t, "auto-plan discarded because the PR moved during discovery", outcome)
	assert.Nil(t, f.storedAggregate(t), "a stale head stores no block")
	assert.Empty(t, f.postedComments(), "a stale head posts no comment")
}

// TestE2EAutoPlanSizeRefusalCommentsOncePerSchemaChange covers pushes to a PR
// that stays over the limit. A push that changes no schema inputs keeps the
// block on the new head without another comment. A push that changes a schema
// file posts the comment again, as a plan comment would be.
func TestE2EAutoPlanSizeRefusalCommentsOncePerSchemaChange(t *testing.T) {
	dbName := "webhook_autoplan_size_repeat"
	f := setupOversizedPR(t, dbName, []string{"size_repeat_ledger"}, api.AutoPlanLimitsConfig{MaxDatabases: 1})

	_, err := f.autoPlan(t, "opened", "", "abc123")
	require.NoError(t, err)
	require.Len(t, f.postedComments(), 1)

	readmeHead := "readme789"
	f.result.MovedHeadSHA.Store(&readmeHead)
	registerFreshBaseComparison(t, f.mux, readmeHead)
	registerCompareFiles(t, f.mux, "abc123", readmeHead, []*gh.CommitFile{{Filename: new("README.md"), Status: new("modified")}})
	_, err = f.autoPlan(t, "synchronize", "abc123", readmeHead)
	require.NoError(t, err)
	assert.Empty(t, f.postedComments(), "a push that changes no schema inputs posts no second comment")
	stored := f.storedAggregate(t)
	require.NotNil(t, stored)
	assert.Equal(t, checkstate.BlockAutoPlanSizeLimit, stored.BlockingReason)
	assert.Equal(t, readmeHead, stored.HeadSHA, "the block moves to the new head")

	schemaHead := "schema012"
	f.result.MovedHeadSHA.Store(&schemaHead)
	registerFreshBaseComparison(t, f.mux, schemaHead)
	registerCompareFiles(t, f.mux, readmeHead, schemaHead, []*gh.CommitFile{{Filename: new(schemaFixturePath(dbName, dbName+"/users.sql")), Status: new("modified")}})
	_, err = f.autoPlan(t, "synchronize", readmeHead, schemaHead)
	require.NoError(t, err)
	comments := f.postedComments()
	require.Len(t, comments, 1, "a push that changes a schema file posts the comment again")
	assert.Contains(t, comments[0], "This PR is too large to plan automatically")
}

// TestE2EAutoPlanSizeRefusalWithChecksDisabled covers a repository whose
// status checks are disabled. Nothing gates its merge, so the refusal stores
// no block and its comment asks for the split without claiming a blocked
// check or a refused apply.
func TestE2EAutoPlanSizeRefusalWithChecksDisabled(t *testing.T) {
	dbName := "webhook_autoplan_size_nochecks"
	f := setupOversizedPR(t, dbName, []string{"size_nochecks_ledger"}, api.AutoPlanLimitsConfig{MaxDatabases: 1})
	disabled := false
	f.svc.Config().Repos = map[string]api.RepoConfig{"octocat/hello-world": {EnableChecks: &disabled}}

	_, err := f.autoPlan(t, "opened", "", "abc123")
	require.NoError(t, err)
	comments := f.postedComments()
	require.Len(t, comments, 1)
	assert.Contains(t, comments[0], "**What to do next:** split this PR into smaller PRs.\n")
	assert.NotContains(t, comments[0], "check stays blocked")
	assert.Nil(t, f.storedAggregate(t), "no block is stored where nothing gates merge")
}
