//go:build integration

package webhook

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
)

// A pull request in a large repository edits hundreds of files outside any
// schema directory, each by a single line. Every way SchemaBot plans that PR
// makes exactly the GitHub requests it makes when the PR edits one such file,
// so a monorepo's routine edits cannot multiply the requests every delivery
// makes and exhaust the App's rate limit for every repository it serves
// (AV-15). The paths covered are an auto-plan when the PR opens and when a push
// lands, a plan command, and a plan command in a repository whose tree GitHub
// truncates, where config discovery falls back to the configured schema
// directories. In a repository whose aggregate leader gates on participant
// deployments, they also cover the leader's fold after an auto-plan and the
// check refresh a plan command makes when the PR changes only participants'
// schema inputs: both compare what the PR proposes against the default branch
// and read the participants' Check Runs.
func TestE2EGitHubRequestsDoNotGrowWithNonSchemaFiles(t *testing.T) {
	const dbName = "webhook_github_cost"
	for _, delivery := range githubCostDeliveries(dbName) {
		t.Run(delivery.name, func(t *testing.T) {
			var few, many map[string]int
			t.Run("one non-schema file", func(t *testing.T) {
				few = githubRequestsToPlan(t, dbName, delivery, 1)
			})
			t.Run("many non-schema files", func(t *testing.T) {
				// Below GitHub's 300-file cap on a commit comparison, so a
				// push is read through the comparison in both runs rather
				// than giving up on it only in this one.
				many = githubRequestsToPlan(t, dbName, delivery, 250)
			})
			require.NotEmpty(t, few, "the plan flow made GitHub requests to count")
			if delivery.leader != nil {
				assert.Equal(t, delivery.leader.participantReads, participantCheckRunReads(few),
					"the leader folded its participants' Check Runs on the head")
			}
			// The fake lists every changed file in one page, so the listing
			// request counts the same for one file and for many. Any
			// difference, the listing included, is a request made per file.
			assert.Equal(t, few, many, "GitHub requests by method and path, for one non-schema file vs many")
		})
	}
}

// githubCostDelivery is one way a webhook delivery makes SchemaBot plan a PR.
type githubCostDelivery struct {
	name string
	// monorepo serves a repository too large for GitHub to list recursively.
	// The PR then changes no schema file, so the plan command has to find the
	// database's config through the server-configured schema directories.
	monorepo bool
	// leader, when set, makes this deployment the repository's aggregate
	// leader and has the PR change participants' schema inputs as well.
	leader *githubCostLeader
	// request builds the delivery once the fake GitHub serves the PR's files.
	request func(t *testing.T, mux *http.ServeMux, result *planFlowResult, prFiles []*gh.CommitFile) *http.Request
}

// githubCostLeader is an unscoped aggregate leader gating on two participants,
// tenant-b and tenant-c, and the PR's changes to their schema inputs. The PR
// proposes tenant-b's change, so the leader folds tenant-b's Check Run. It only
// inherited tenant-c's from the default branch, so the leader reads tenant-c's
// Check Run on the head to learn whether tenant-c still blocks.
type githubCostLeader struct {
	// ownSchemaUnchanged leaves the leader's own schema file out of the PR, so
	// a plan command refreshes the checks instead of planning.
	ownSchemaUnchanged bool
	// files are the PR's changes to the participants' schema inputs.
	files []*gh.CommitFile
	// atHead are the participants' paths the head holds.
	atHead []string
	// atDefault are the participants' paths the default branch holds, at the
	// head's blob where the head holds one too.
	atDefault []string
	// done is a phrase in the comment that ends the delivery.
	done string
	// participantReads is how many times the delivery reads each
	// participant's Check Run on the head, by check name.
	participantReads map[string]int
}

const (
	tenantCSchemaDir = "tenant-c/schema"
	tenantCCheckName = "SchemaBot Tenant C"
)

// participantCheckRunReads picks the reads of the head's Check Runs by a
// participant's check name out of counts.
func participantCheckRunReads(counts map[string]int) map[string]int {
	reads := make(map[string]int)
	for _, name := range []string{tenantBCheckName, tenantCCheckName} {
		if n := counts["GET /repos/octocat/hello-world/commits/abc123/check-runs?check_name="+name]; n > 0 {
			reads[name] = n
		}
	}
	return reads
}

// githubCostLeaderConfig is the aggregate configuration of a githubCostLeader.
func githubCostLeaderConfig() api.RepoConfig {
	return api.RepoConfig{Aggregate: &api.AggregateConfig{
		Role: api.AggregateRoleLeader,
		ExpectedTenants: []api.ExpectedTenant{
			{Tenant: "tenant-b", Paths: []string{tenantBSchemaDir}, CheckName: tenantBCheckName},
			{Tenant: "tenant-c", Paths: []string{tenantCSchemaDir}, CheckName: tenantCCheckName},
		},
	}}
}

func githubCostDeliveries(dbName string) []githubCostDelivery {
	planCommand := func(comment string) func(*testing.T, *http.ServeMux, *planFlowResult, []*gh.CommitFile) *http.Request {
		return func(t *testing.T, _ *http.ServeMux, _ *planFlowResult, _ []*gh.CommitFile) *http.Request {
			return buildWebhookRequest(t, webhookPayloadOpts{comment: comment, isPR: true}, nil)
		}
	}
	proposedSchema, inheritedSchema := tenantBSchemaDir+"/orders.sql", tenantCSchemaDir+"/orders.sql"
	proposedDeletion, inheritedDeletion := tenantBSchemaDir+"/schemabot.yaml", tenantCSchemaDir+"/schemabot.yaml"
	return []githubCostDelivery{
		{
			name: "auto-plan on open",
			request: func(t *testing.T, _ *http.ServeMux, _ *planFlowResult, _ []*gh.CommitFile) *http.Request {
				return buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "opened"}, nil)
			},
		},
		{
			name: "auto-plan on push",
			request: func(t *testing.T, mux *http.ServeMux, result *planFlowResult, prFiles []*gh.CommitFile) *http.Request {
				result.HeadSHAs = []string{"newsha"}
				registerCompareFiles(t, mux, "oldsha", "newsha", prFiles)
				registerFreshBaseComparison(t, mux, "newsha")
				return buildPRWebhookRequest(t, prWebhookPayloadOpts{
					action:    "synchronize",
					beforeSHA: "oldsha",
					headSHA:   "newsha",
					headRef:   "feature-branch",
				}, nil)
			},
		},
		{
			name:    "plan command",
			request: planCommand("schemabot plan -e staging"),
		},
		{
			name:     "plan command in a monorepo",
			monorepo: true,
			request:  planCommand(fmt.Sprintf("schemabot plan -e staging -d %s", dbName)),
		},
		{
			name: "leader fold after auto-plan on open",
			leader: &githubCostLeader{
				files: []*gh.CommitFile{
					{Filename: new(proposedSchema), Status: new("modified")},
					{Filename: new(inheritedSchema), Status: new("modified")},
				},
				atHead:    []string{proposedSchema, inheritedSchema},
				atDefault: []string{inheritedSchema},
				done:      "CREATE TABLE",
				// The fold after the plan reads each participant once.
				participantReads: map[string]int{tenantBCheckName: 1, tenantCCheckName: 1},
			},
			request: func(t *testing.T, _ *http.ServeMux, _ *planFlowResult, _ []*gh.CommitFile) *http.Request {
				return buildPRWebhookRequest(t, prWebhookPayloadOpts{action: "opened"}, nil)
			},
		},
		{
			name: "leader check refresh on a plan command",
			leader: &githubCostLeader{
				ownSchemaUnchanged: true,
				files: []*gh.CommitFile{
					{Filename: new(proposedDeletion), Status: new("removed")},
					{Filename: new(inheritedDeletion), Status: new("removed")},
				},
				atDefault: []string{proposedDeletion},
				done:      "touches schema paths owned by tenant deployments",
				// The refresh reads tenant-c to decide whether to fold, then
				// the fold reads both participants.
				participantReads: map[string]int{tenantBCheckName: 1, tenantCCheckName: 2},
			},
			request: planCommand("schemabot plan -e staging"),
		},
	}
}

// githubRequestsToPlan plans a PR that edits nonSchemaFiles files outside the
// schema directory by one line each through delivery, and returns every GitHub
// request the delivery made, counted by method and path. Outside a monorepo the
// PR also adds the database's one schema file, unless a leader delivery leaves
// it out, and a leader delivery's PR changes its participants' schema inputs.
func githubRequestsToPlan(t *testing.T, dbName string, delivery githubCostDelivery, nonSchemaFiles int) map[string]int {
	t.Helper()
	svc := setupE2EService(t, dbName)

	mux := http.NewServeMux()
	requests := &githubRequestCounter{counts: make(map[string]int)}
	server := httptest.NewServer(requests.wrap(mux))
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	var err error
	client.BaseURL, err = url.Parse(server.URL + "/")
	require.NoError(t, err)

	schemaFiles := map[string]string{
		"users.sql": "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
	}
	leader := delivery.leader
	var prFiles []*gh.CommitFile
	if !delivery.monorepo && (leader == nil || !leader.ownSchemaUnchanged) {
		prFiles = append(prFiles, &gh.CommitFile{
			Filename: new(schemaFixturePath(dbName, "users.sql")),
			Status:   new("added"),
		})
	}
	var headFiles []string
	if leader != nil {
		prFiles = append(prFiles, leader.files...)
		headFiles = append(headFiles, leader.atHead...)
	}
	var otherFiles []string
	for i := range nonSchemaFiles {
		// A one-line edit several directories deep is the most common change
		// in a large repository, and the one a per-file lookup costs most for.
		path := fmt.Sprintf("services/app/src/main/java/com/example/module%03d/internal/Handler.java", i)
		otherFiles = append(otherFiles, path)
		prFiles = append(prFiles, &gh.CommitFile{
			Filename:  new(path),
			Status:    new("modified"),
			Additions: new(1),
			Deletions: new(1),
			Changes:   new(2),
			Patch:     new("@@ -1 +1 @@\n-old\n+new"),
		})
	}
	configs := map[string]string{"schema/schemabot.yaml": fmt.Sprintf("database: %s\ntype: mysql\n", dbName)}
	headFiles = append(headFiles, otherFiles...)
	result := setupFakeGitHubForPlanInRepository(t, mux, schemaFiles, configs, dbName, prFiles, headFiles)

	var h *Handler
	switch {
	case leader != nil:
		result.baseHolds(leader.atDefault...)
		svc.Config().Repos = map[string]api.RepoConfig{"octocat/hello-world": githubCostLeaderConfig()}
		h = newE2EHandler(t, svc, client)
		// A participant that has not reported arms a re-fold; keep it from
		// firing while the requests are counted, so each run counts one fold.
		h.participantRefoldDelayOverride = time.Hour
	case delivery.monorepo:
		result.TruncateRecursiveTrees.Store(true)
		serverConfig := svc.Config()
		dbConfig := serverConfig.Databases[dbName]
		dbConfig.AllowedDirs = []string{"schema"}
		serverConfig.Databases[dbName] = dbConfig
		h = newE2EHandlerWithConfigDirHints(t, svc, client)
	default:
		h = newE2EHandler(t, svc, client)
	}

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, delivery.request(t, mux, result, prFiles))
	require.Equal(t, http.StatusOK, rr.Code)

	done := "CREATE TABLE"
	if leader != nil {
		done = leader.done
	}
	deadline := time.After(webhookIntegrationCheckRunDeadline)
	for finished := false; !finished; {
		select {
		case body := <-result.comments:
			finished = strings.Contains(body, done)
		case <-deadline:
			require.Fail(t, "timed out waiting for the comment that ends the delivery", "want a comment containing %q", done)
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), webhookIntegrationCheckRunDeadline)
	defer cancel()
	h.DrainInProcessWebhookWork(ctx)
	require.NoError(t, ctx.Err(), "plan work was still running when the requests were counted")
	return requests.snapshot()
}

// githubRequestCounter counts the requests a fake GitHub server receives, by
// method and path, and by check name for a read of a commit's Check Runs that
// names one, so each participant's Check Run reads count on their own.
type githubRequestCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

func (c *githubRequestCounter) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Method + " " + r.URL.Path
		if name := r.URL.Query().Get("check_name"); name != "" {
			key += "?check_name=" + name
		}
		c.mu.Lock()
		c.counts[key]++
		c.mu.Unlock()
		next.ServeHTTP(w, r)
	})
}

func (c *githubRequestCounter) snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.counts)
}
