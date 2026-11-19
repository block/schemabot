package webhook

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const schemaCapTestRepo = "octocat/hello-world"
const schemaCapTestLabel = "schemabot: allow-large-plan"

type schemaCapTestFixture struct {
	*Handler
	client     *ghclient.InstallationClient
	mux        *http.ServeMux
	settings   *memorySettingsStore
	plans      *schemaCapTestPlanStore
	head       atomic.Value
	base       atomic.Value
	defaultTip atomic.Value
	truncated  atomic.Bool
	labels     atomic.Bool
	draft      atomic.Bool
	fileReads  atomic.Int64
}

type schemaCapTestStore struct {
	durableWebhookTestStorage
	plans *schemaCapTestPlanStore
}

func (s *schemaCapTestStore) Plans() storage.PlanStore { return s.plans }

type schemaCapTestPlanStore struct {
	noopPlanCreateStore
	plan *storage.Plan
}

func (s *schemaCapTestPlanStore) Get(context.Context, string) (*storage.Plan, error) {
	return s.plan, nil
}

func newSchemaCapTestFixture(t *testing.T, configs, local int, policy api.PRSchemaCapConfig, planner tern.Client) *schemaCapTestFixture {
	t.Helper()
	gh, mux := setupGitHubServer(t)
	logger := testLogger()
	client := ghclient.NewInstallationClient(gh, logger)
	settings := newMemorySettingsStore()
	plans := &schemaCapTestPlanStore{}
	cfg := &api.ServerConfig{
		Repos:           map[string]api.RepoConfig{schemaCapTestRepo: {PRSchemaCap: &policy, EnableChecks: new(false)}},
		Databases:       make(map[string]api.DatabaseConfig),
		TernDeployments: api.TernConfig{"remote": {"staging": "remote:9090"}},
	}
	for i := range local {
		cfg.Databases[fmt.Sprintf("db%d", i)] = api.DatabaseConfig{Type: "mysql",
			Environments: map[string]api.EnvironmentConfig{"staging": {Deployment: "remote", Target: fmt.Sprintf("target%d", i)}}}
	}
	store := &schemaCapTestStore{durableWebhookTestStorage: durableWebhookTestStorage{settings: settings}, plans: plans}
	service := api.New(store, cfg, map[string]tern.Client{"remote/staging": planner}, logger)
	h := NewHandler(service, &fakeClientFactory{client: client}, nil, logger)
	f := &schemaCapTestFixture{Handler: h, client: client, mux: mux, settings: settings, plans: plans}
	f.head.Store("head")
	f.base.Store("main")
	f.defaultTip.Store("base")
	h.transientPlanRetryDelay = time.Millisecond
	drainWebhookWorkOnCleanup(t, h)

	repoPath := "/repos/" + schemaCapTestRepo
	mux.HandleFunc("GET "+repoPath, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"default_branch":"main"}`)
	})
	mux.HandleFunc("GET "+repoPath+"/git/ref/heads/main", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"object":{"sha":%q}}`, f.defaultTip.Load())
	})
	mux.HandleFunc("GET "+repoPath+"/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		labels := "[]"
		if f.labels.Load() {
			labels = fmt.Sprintf(`[{"name":%q}]`, schemaCapTestLabel)
		}
		_, _ = fmt.Fprintf(w, `{"state":"open","draft":%t,"user":{"login":"author"},"head":{"sha":%q},"base":{"ref":%q,"sha":"base","repo":{"default_branch":"main"}},"labels":%s}`,
			f.draft.Load(), f.head.Load(), f.base.Load(), labels)
	})
	mux.HandleFunc("GET "+repoPath+"/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		f.fileReads.Add(1)
		if f.truncated.Load() {
			files := make([]map[string]string, 3000)
			for i := range files {
				files[i] = map[string]string{"filename": fmt.Sprintf("changed/%d.txt", i), "status": "modified"}
			}
			assert.NoError(t, json.NewEncoder(w).Encode(files))
			return
		}
		files := make([]map[string]string, 0, configs)
		for i := range configs {
			files = append(files, map[string]string{"filename": fmt.Sprintf("db%d/schemabot.yaml", i), "status": "added"})
		}
		assert.NoError(t, json.NewEncoder(w).Encode(files))
	})
	mux.HandleFunc("GET "+repoPath+"/git/trees/{sha}", func(w http.ResponseWriter, r *http.Request) {
		entries := make([]map[string]string, 0, configs)
		switch sha := r.PathValue("sha"); {
		case strings.HasPrefix(sha, "base"):
		case strings.HasPrefix(sha, "tree-db"):
			entries = append(entries, map[string]string{"path": "schemabot.yaml", "mode": "100644", "type": "blob", "sha": "blob-" + sha})
		default:
			for i := range configs {
				entries = append(entries, map[string]string{"path": fmt.Sprintf("db%d", i), "mode": "040000", "type": "tree", "sha": fmt.Sprintf("tree-db%d", i)})
			}
		}
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tree": entries}))
	})
	mux.HandleFunc("GET "+repoPath+"/contents/{db}/schemabot.yaml", func(w http.ResponseWriter, r *http.Request) {
		content := "database: " + r.PathValue("db") + "\ntype: mysql\n"
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]string{"type": "file", "encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(content))}))
	})
	mux.HandleFunc("POST "+repoPath+"/issues/1/comments", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprint(w, `{"id":5}`)
	})
	mux.HandleFunc("PATCH "+repoPath+"/issues/comments/5", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"id":5}`)
	})
	return f
}

// A cold narrowed command counts the whole PR. The warm auto/recovery and
// apply paths reuse discovery and stop before queueing any local database work.
func TestPRSchemaCapStopsPlanAndCachedApply(t *testing.T) {
	planner := &flakyPlanTernClient{}
	f := newSchemaCapTestFixture(t, 9, 6, api.PRSchemaCapConfig{Label: schemaCapTestLabel}, planner)
	w := httptest.NewRecorder()
	f.handlePlanCommand(w, schemaCapTestRepo, 1, "staging", "db0", "one-target", "", 42, "delivery", "author", 0)
	require.Contains(t, w.Body.String(), errPRSchemaCapHeld.Error())
	require.EqualValues(t, 1, f.fileReads.Load())

	for _, source := range []string{"pull_request", "check_run.rerequested", "checks.backfill"} {
		message, err := f.runAutoPlanForPR(t.Context(), f.client, schemaCapTestRepo, 1, "head", 42, source, "opened", "", "event")
		require.NoError(t, err)
		require.Equal(t, errPRSchemaCapHeld.Error(), message)
	}
	f.inProcessWebhookMu.Lock()
	tasks := f.inProcessWebhookCount
	f.inProcessWebhookMu.Unlock()
	require.Zero(t, tasks)
	f.plans.plan = &storage.Plan{Database: "db0", Environment: "staging", Deployment: "remote", Target: "target0",
		HeadSHA: "head", Repository: schemaCapTestRepo, PullRequest: 1}
	_, id, err := f.service.ExecuteApply(t.Context(), api.ApplyRequest{PlanID: "cached", Environment: "staging"})
	require.ErrorIs(t, err, errPRSchemaCapHeld)
	require.Zero(t, id)
	require.Zero(t, planner.calls())
	require.EqualValues(t, 1, f.fileReads.Load())
}

func TestPRSchemaCapCountsLocalLogicalDatabases(t *testing.T) {
	for _, sizes := range []struct{ repo, local int }{{5, 5}, {6, 6}, {6, 3}, {9, 3}, {9, 6}} {
		t.Run(fmt.Sprintf("%d-of-%d", sizes.local, sizes.repo), func(t *testing.T) {
			f := newSchemaCapTestFixture(t, sizes.repo, sizes.local, api.PRSchemaCapConfig{Label: schemaCapTestLabel}, nil)
			got, err := f.admitPRSchemaCommand(t.Context(), f.client, schemaCapTestRepo, 1, prPlanDiscoveryKey{})
			if sizes.local > 5 {
				require.ErrorIs(t, err, errPRSchemaCapHeld)
				return
			}
			require.NoError(t, err)
			require.Len(t, f.prPlanDiscovery.get(got.key).localDatabases, sizes.local)
			dispatch, err := f.dispatchAutoPlanForPR(t.Context(), f.client, schemaCapTestRepo, 1, "head", 42,
				"check-recovery", "recovery", "", "delivery", autoPlanOptions{deferSchemaPlans: true})
			require.NoError(t, err)
			require.Len(t, dispatch.deferredDatabases, sizes.local)
		})
	}
	f := newSchemaCapTestFixture(t, 0, 1, api.PRSchemaCapConfig{Label: schemaCapTestLabel}, nil)
	db := f.config().Databases["db0"]
	db.Environments["production"] = api.EnvironmentConfig{Deployment: "other", Target: "physical-copy"}
	var entries []ghclient.DiscoveredConfig
	for shard := range 6 {
		entries = append(entries, ghclient.DiscoveredConfig{Config: &ghclient.SchemabotConfig{Database: "db0"}, SchemaDir: fmt.Sprint(shard)})
	}
	require.Equal(t, []string{"db0"}, managedDatabaseNames(f.localPRPlanConfigs(schemaCapTestRepo, 1, entries)))
}

func TestPRSchemaCapRejectsMovedQueuedRevision(t *testing.T) {
	f := newSchemaCapTestFixture(t, 5, 5, api.PRSchemaCapConfig{Label: schemaCapTestLabel}, nil)
	_, err := f.admitPRSchemaCommand(t.Context(), f.client, schemaCapTestRepo, 1, prPlanDiscoveryKey{})
	require.NoError(t, err)
	info, err := f.client.FetchPullRequest(t.Context(), schemaCapTestRepo, 1)
	require.NoError(t, err)
	expected := f.prPlanDiscoveryKey(schemaCapTestRepo, 1, info)
	f.base.Store("release")
	_, err = f.admitPRSchemaCommand(t.Context(), f.client, schemaCapTestRepo, 1, expected)
	require.ErrorIs(t, err, errPRSchemaMoved)
	require.EqualValues(t, 1, f.fileReads.Load())
}

type revokingSchemaCapPlanner struct {
	tern.Client
	revoke func()
	calls  int
}

func (c *revokingSchemaCapPlanner) IsRemote() bool   { return true }
func (c *revokingSchemaCapPlanner) Endpoint() string { return "remote:9090" }

func (c *revokingSchemaCapPlanner) Plan(context.Context, *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	c.calls++
	c.revoke()
	return nil, status.Error(codes.Unavailable, "retryable")
}

// Removing the exception after the first Plan RPC prevents its transient retry.
func TestPRSchemaCapStopsRetryAfterLabelRemoval(t *testing.T) {
	planner := &revokingSchemaCapPlanner{}
	f := newSchemaCapTestFixture(t, 6, 6, api.PRSchemaCapConfig{Label: schemaCapTestLabel}, planner)
	planner.revoke = func() { f.labels.Store(false) }
	f.labels.Store(true)
	now := time.Date(2026, 11, 18, 21, 0, 0, 0, time.UTC)
	registerSchemaCapTimeline(f.mux, fmt.Sprintf(`[{"id":13,"event":"labeled","label":{"name":%q},"actor":{"login":"alice"},"created_at":%q}]`, schemaCapTestLabel, now.Format(time.RFC3339)))
	info, err := f.client.FetchPullRequest(t.Context(), schemaCapTestRepo, 1)
	require.NoError(t, err)
	payload := schemaCapLabelPayload(now)
	accepted, err := f.recordPRSchemaLabel(t.Context(), f.client, schemaCapTestRepo, 1, info, schemaCapTestLabel, payload, now.Add(time.Second))
	require.NoError(t, err)
	require.True(t, accepted)
	admission, err := f.admitPRSchemaCommand(t.Context(), f.client, schemaCapTestRepo, 1, prPlanDiscoveryKey{})
	require.NoError(t, err)
	req := planRetryTestRequest()
	req.Database, req.Repository = "db0", schemaCapTestRepo
	req.HeadSHA, req.PullRequest = new("head"), new(int32(1))
	_, _, err = f.executePlanProtoWithTransientRetry(withPRSchemaAdmission(t.Context(), admission), req, schemaCapTestRepo, 1)
	require.ErrorIs(t, err, errPRSchemaCapHeld)
	require.Equal(t, 1, planner.calls)
}

func schemaCapLabelPayload(now time.Time) pullRequestPayload {
	var p pullRequestPayload
	p.Action, p.Label.Name, p.Sender.Login = "labeled", schemaCapTestLabel, "alice"
	p.Repository.FullName, p.PullRequest.Number = schemaCapTestRepo, 1
	p.PullRequest.Head.SHA, p.PullRequest.Base.SHA, p.PullRequest.Base.Ref = "head", "base", "main"
	p.PullRequest.UpdatedAt = now
	return p
}

func registerSchemaCapTimeline(mux *http.ServeMux, body string) {
	mux.HandleFunc("GET /repos/"+schemaCapTestRepo+"/issues/1/timeline", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, body)
	})
}

func TestPRSchemaCapClaimsAnExceptedDatabaseOnce(t *testing.T) {
	f := newSchemaCapTestFixture(t, 0, 0, api.PRSchemaCapConfig{Label: schemaCapTestLabel}, nil)
	admission := prSchemaAdmission{key: prPlanDiscoveryKey{repo: schemaCapTestRepo, pr: 1}, exception: "label:13"}
	claims := make(chan *storage.Setting, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			claim, claimed, err := f.claimPRSchemaPlan(t.Context(), admission, "db0")
			assert.True(t, err == nil || errors.Is(err, errPRSchemaPlanClaimActive))
			if claimed {
				claims <- claim
			}
		})
	}
	wg.Wait()
	require.Len(t, claims, 1)
	f.finishPRSchemaPlan(<-claims, false)
	retry, claimed, err := f.claimPRSchemaPlan(t.Context(), admission, "db0")
	require.NoError(t, err)
	require.True(t, claimed)
	f.finishPRSchemaPlan(retry, true)
	_, claimed, err = f.claimPRSchemaPlan(t.Context(), admission, "db0")
	require.NoError(t, err)
	require.False(t, claimed)
	_, claimed, err = f.claimPRSchemaPlan(t.Context(), admission, "db1")
	require.NoError(t, err)
	require.True(t, claimed)
}

// The original label delivery cannot revive a removed label or acknowledge a
// later label on a different base, even when the head SHA stayed the same.
func TestPRSchemaCapLabelReorderingAndRetarget(t *testing.T) {
	f := newSchemaCapTestFixture(t, 6, 6, api.PRSchemaCapConfig{Label: schemaCapTestLabel}, nil)
	f.labels.Store(true)
	old := time.Date(2026, 11, 18, 21, 0, 0, 0, time.UTC)
	var event atomic.Value
	event.Store(fmt.Sprintf(`[{"id":13,"event":"labeled","label":{"name":%q},"actor":{"login":"alice"},"created_at":%q}]`, schemaCapTestLabel, old.Format(time.RFC3339)))
	path := "/repos/" + schemaCapTestRepo + "/issues/1"
	f.mux.HandleFunc("GET "+path+"/timeline", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, event.Load())
	})
	var removals atomic.Int64
	f.mux.HandleFunc("DELETE "+path+"/labels/{label}", func(w http.ResponseWriter, _ *http.Request) {
		removals.Add(1)
		f.labels.Store(false)
		w.WriteHeader(http.StatusNoContent)
	})
	info, err := f.client.FetchPullRequest(t.Context(), schemaCapTestRepo, 1)
	require.NoError(t, err)
	signed := schemaCapLabelPayload(old)
	ok, err := f.recordPRSchemaLabel(t.Context(), f.client, schemaCapTestRepo, 1, info, schemaCapTestLabel, signed, old.Add(time.Second))
	require.NoError(t, err)
	require.True(t, ok)

	f.base.Store("release")
	fresh, err := f.client.FetchPullRequestNoCache(t.Context(), schemaCapTestRepo, 1)
	require.NoError(t, err)
	admission, err := f.evaluatePRSchemaLabel(t.Context(), f.client, schemaCapTestRepo, 1, fresh, schemaCapTestLabel)
	require.NoError(t, err)
	require.Empty(t, admission)
	require.EqualValues(t, 1, removals.Load())

	f.labels.Store(true)
	readded := old.Add(10 * time.Second)
	event.Store(fmt.Sprintf(`[{"id":15,"event":"base_ref_changed","created_at":%q},{"id":16,"event":"labeled","label":{"name":%q},"actor":{"login":"alice"},"created_at":%q}]`,
		old.Add(5*time.Second).Format(time.RFC3339), schemaCapTestLabel, readded.Format(time.RFC3339)))
	fresh, err = f.client.FetchPullRequestNoCache(t.Context(), schemaCapTestRepo, 1)
	require.NoError(t, err)
	ok, err = f.recordPRSchemaLabel(t.Context(), f.client, schemaCapTestRepo, 1, fresh, schemaCapTestLabel, signed, old.Add(time.Second))
	require.NoError(t, err)
	require.False(t, ok)
	signed.PullRequest.Base.Ref = "release"
	signed.PullRequest.UpdatedAt = readded
	ok, err = f.recordPRSchemaLabel(t.Context(), f.client, schemaCapTestRepo, 1, fresh, schemaCapTestLabel, signed, readded.Add(time.Second))
	require.NoError(t, err)
	require.True(t, ok)
	admission, err = f.evaluatePRSchemaLabel(t.Context(), f.client, schemaCapTestRepo, 1, fresh, schemaCapTestLabel)
	require.NoError(t, err)
	require.Equal(t, "label:16", admission)
}

func TestPRSchemaCapRequiresCurrentEffectiveTeamApproval(t *testing.T) {
	const date = "2026-11-18T21:00:00Z"
	for _, tc := range []struct {
		name, reviewUser, state, commit, timeline string
		membership                                int
		admitted, verificationError               bool
	}{
		{name: "current team approval", reviewUser: "alice", state: "APPROVED", commit: "head", admitted: true, membership: 200},
		{name: "nonmember", reviewUser: "alice", state: "APPROVED", commit: "head", membership: 404},
		{name: "unreadable membership", reviewUser: "alice", state: "APPROVED", commit: "head", membership: 403, verificationError: true},
		{name: "author", reviewUser: "author", state: "APPROVED", commit: "head"},
		{name: "prior commit", reviewUser: "alice", state: "APPROVED", commit: "previous"},
		{name: "dismissed", reviewUser: "alice", state: "DISMISSED", commit: "head"},
		{name: "retargeted", reviewUser: "alice", state: "APPROVED", commit: "head", timeline: `[{"id":44,"event":"base_ref_changed","created_at":"2026-11-18T22:00:00Z"}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSchemaCapTestFixture(t, 0, 0, api.PRSchemaCapConfig{ReviewTeam: "octocat/database-reviewers"}, nil)
			f.mux.HandleFunc("GET /repos/"+schemaCapTestRepo+"/pulls/1/reviews", func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprintf(w, `[{"id":17,"user":{"login":%q},"state":%q,"commit_id":%q,"submitted_at":%q}]`, tc.reviewUser, tc.state, tc.commit, date)
			})
			timeline := tc.timeline
			if timeline == "" {
				timeline = "[]"
			}
			registerSchemaCapTimeline(f.mux, timeline)
			f.mux.HandleFunc("GET /orgs/octocat/teams/database-reviewers/memberships/alice", func(w http.ResponseWriter, _ *http.Request) {
				assert.NotZero(t, tc.membership)
				w.WriteHeader(tc.membership)
				_, _ = fmt.Fprint(w, `{"state":"active"}`)
			})
			f.mux.HandleFunc("GET /orgs/octocat/teams/database-reviewers", func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprint(w, `{"slug":"database-reviewers"}`)
			})
			info, err := f.client.FetchPullRequest(t.Context(), schemaCapTestRepo, 1)
			require.NoError(t, err)
			token, _, err := f.evaluatePRSchemaReview(t.Context(), f.client, schemaCapTestRepo, 1, info, f.config().PRSchemaCapForRepo(schemaCapTestRepo), false)
			if tc.verificationError {
				require.ErrorIs(t, err, ghclient.ErrTeamMembershipUnreadable)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.admitted, token != "")
		})
	}
}

func TestPRSchemaCapRequestsReviewOnceAfterDraft(t *testing.T) {
	f := newSchemaCapTestFixture(t, 0, 0, api.PRSchemaCapConfig{ReviewTeam: "octocat/database-reviewers", RequestTeamReview: true}, nil)
	repoPath := "/repos/" + schemaCapTestRepo
	f.mux.HandleFunc("GET "+repoPath+"/pulls/1/reviews", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "[]")
	})
	registerSchemaCapTimeline(f.mux, "[]")
	var requests atomic.Int64
	f.mux.HandleFunc("POST "+repoPath+"/pulls/1/requested_reviewers", func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			TeamReviewers []string `json:"team_reviewers"`
		}
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&payload))
		assert.Equal(t, []string{"database-reviewers"}, payload.TeamReviewers)
		requests.Add(1)
		w.WriteHeader(http.StatusForbidden)
	})
	f.draft.Store(true)
	policy := f.config().PRSchemaCapForRepo(schemaCapTestRepo)
	fresh, err := f.client.FetchPullRequestNoCache(t.Context(), schemaCapTestRepo, 1)
	require.NoError(t, err)
	_, notice, err := f.evaluatePRSchemaReview(t.Context(), f.client, schemaCapTestRepo, 1, fresh, policy, true)
	require.NoError(t, err)
	require.Empty(t, notice)
	require.Zero(t, requests.Load())

	f.draft.Store(false)
	fresh, err = f.client.FetchPullRequestNoCache(t.Context(), schemaCapTestRepo, 1)
	require.NoError(t, err)
	for range 2 {
		_, notice, err := f.evaluatePRSchemaReview(t.Context(), f.client, schemaCapTestRepo, 1, fresh, policy, true)
		require.NoError(t, err)
		require.Contains(t, notice, "HTTP 403")
	}
	require.EqualValues(t, 1, requests.Load())
}

func TestPRSchemaCapPublishesActionRequiredAndAffectedDatabases(t *testing.T) {
	f := newSchemaCapTestFixture(t, 6, 6, api.PRSchemaCapConfig{Label: schemaCapTestLabel, ControlPlane: "east"}, nil)
	cfg := f.config()
	policy := cfg.Repos[schemaCapTestRepo]
	policy.EnableChecks = new(true)
	cfg.Repos[schemaCapTestRepo] = policy
	store := &schemaCapCheckStorage{
		schemaCapTestStore: f.service.Storage().(*schemaCapTestStore),
		checks:             &foldCheckStore{},
	}
	f.service = api.New(store, cfg, nil, testLogger())
	type checkRunOutput struct {
		Conclusion string
		Output     struct {
			Title, Summary string
		}
	}
	checkRuns := make(chan checkRunOutput, 1)
	f.mux.HandleFunc("POST /repos/"+schemaCapTestRepo+"/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var posted checkRunOutput
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&posted))
		checkRuns <- posted
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprint(w, `{"id":39}`)
	})
	message, err := f.runAutoPlanForPR(t.Context(), f.client, schemaCapTestRepo, 1, "head", 42, "pull_request", "opened", "", "hold")
	require.NoError(t, err)
	require.Equal(t, errPRSchemaCapHeld.Error(), message)
	require.Len(t, checkRuns, 1)
	posted := <-checkRuns
	require.Equal(t, "action_required", posted.Conclusion)
	require.Equal(t, prSchemaCapTitle, posted.Output.Title)
	require.Contains(t, posted.Output.Summary, "Planning is paused on `east`")
	require.Contains(t, posted.Output.Summary, "6 databases")
	require.Contains(t, posted.Output.Summary, schemaCapTestLabel)
	require.Contains(t, posted.Output.Summary, "`db5`")
	require.Len(t, store.checks.upserted, 1)
	require.Equal(t, prSchemaCapBlock.blockingReason, store.checks.upserted[0].BlockingReason)
}

type schemaCapCheckStorage struct {
	*schemaCapTestStore
	checks *foldCheckStore
}

func (s *schemaCapCheckStorage) Checks() storage.CheckStore { return s.checks }

func TestPRSchemaCapTeamGrantChangesOnlyWhenRevoked(t *testing.T) {
	f := newSchemaCapTestFixture(t, 0, 0, api.PRSchemaCapConfig{ReviewTeam: "octocat/database-reviewers"}, nil)
	var seen []uint64
	for _, granted := range []bool{false, true, true, false, false, true} {
		cycle, err := f.updatePRSchemaReviewCycle(t.Context(), "review-cycle", granted)
		require.NoError(t, err)
		seen = append(seen, cycle)
	}
	require.Equal(t, []uint64{1, 1, 1, 2, 2, 2}, seen)
}

func TestPRSchemaCapRejectsIncompleteChangedFiles(t *testing.T) {
	planner := &flakyPlanTernClient{}
	f := newSchemaCapTestFixture(t, 0, 0, api.PRSchemaCapConfig{Label: schemaCapTestLabel}, planner)
	f.truncated.Store(true)
	_, err := f.runAutoPlanForPR(t.Context(), f.client, schemaCapTestRepo, 1, "head", 42, "pull_request", "opened", "", "truncated")
	require.ErrorIs(t, err, ghclient.ErrPRFilesIncomplete)
	require.Zero(t, planner.calls())
}

func TestPRSchemaCapRefreshesDiscoveryWhenComparisonTipMoves(t *testing.T) {
	f := newSchemaCapTestFixture(t, 5, 5, api.PRSchemaCapConfig{Label: schemaCapTestLabel}, nil)
	first, err := f.admitPRSchemaCommand(t.Context(), f.client, schemaCapTestRepo, 1, prPlanDiscoveryKey{})
	require.NoError(t, err)
	require.EqualValues(t, 1, f.fileReads.Load())

	f.defaultTip.Store("base-new")
	_, err = f.admitPRSchemaCommand(t.Context(), f.client, schemaCapTestRepo, 1, first.key)
	require.NoError(t, err)
	require.EqualValues(t, 2, f.fileReads.Load())
}
