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
	h := NewHandler(service, &fakeClientFactory{client: client}, nil, logger, WithInsecureSkipSignatureVerification())
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
		_, _ = fmt.Fprintf(w, `{"state":"open","user":{"login":"author"},"head":{"sha":%q},"base":{"ref":%q,"sha":"base","repo":{"default_branch":"main"}},"labels":%s}`,
			f.head.Load(), f.base.Load(), labels)
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
			files = append(files,
				map[string]string{"filename": fmt.Sprintf("db%d/schemabot.yaml", i), "status": "added"},
				map[string]string{"filename": fmt.Sprintf("db%d/users.sql", i), "status": "modified"},
			)
		}
		assert.NoError(t, json.NewEncoder(w).Encode(files))
	})
	mux.HandleFunc("GET "+repoPath+"/git/trees/{sha}", func(w http.ResponseWriter, r *http.Request) {
		entries := make([]map[string]string, 0, configs)
		switch sha := r.PathValue("sha"); {
		case strings.HasPrefix(sha, "base"):
		case strings.HasPrefix(sha, "tree-db"):
			entries = append(entries, map[string]string{"path": "users.sql", "mode": "100644", "type": "blob", "sha": "sql-" + sha})
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
			// Deferred work includes unknown databases so planning can report registration errors.
			require.Len(t, dispatch.deferredDatabases, sizes.repo)
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

// The database limit re-reads the PR just before it publishes. A push seen
// there means the delivery's head was never planned, so the auto-plan reports
// the head moved and its delivery ends superseded; a retarget seen there
// leaves the delivery's head current, so its delivery still completes.
func TestPRSchemaCapLateReadTellsAPushFromARetarget(t *testing.T) {
	for _, tt := range []struct {
		name      string
		move      func(*schemaCapTestFixture)
		headMoved bool
	}{
		{name: "push", move: func(f *schemaCapTestFixture) { f.head.Store("pushed") }, headMoved: true},
		{name: "retarget", move: func(f *schemaCapTestFixture) { f.base.Store("release") }, headMoved: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newSchemaCapTestFixture(t, 1, 1, api.PRSchemaCapConfig{Label: schemaCapTestLabel}, nil)
			info, err := f.client.FetchPullRequestNoCache(t.Context(), schemaCapTestRepo, 1)
			require.NoError(t, err)
			key := f.prPlanDiscoveryKey(schemaCapTestRepo, 1, info)
			tt.move(f)

			_, err = f.verifyPRSchemaRevision(t.Context(), f.client, key, info.State)
			require.ErrorIs(t, err, errPRSchemaMoved, "every move refuses the publish")
			assert.Equal(t, tt.headMoved, errors.Is(err, errPRSchemaHeadMoved))
		})
	}
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

func TestPRSchemaCapApplyRefusalReleasesOnlyAutomaticIntent(t *testing.T) {
	for _, tt := range []struct {
		name         string
		storedPlan   *storage.Plan
		wantReleased []string
	}{
		{"automatic", &storage.Plan{}, []string{"original-plan"}},
		{"manual confirmation", nil, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			planner := &flakyPlanTernClient{}
			f := newSchemaCapTestFixture(t, 6, 6, api.PRSchemaCapConfig{Label: schemaCapTestLabel}, planner)
			locks := &actorAuthLockStore{}
			f.service.Storage().(*schemaCapTestStore).locks = locks
			var admissions int
			f.service.OnPRSchemaAdmission = func(context.Context, string, int, string) error {
				admissions++
				return errPRSchemaCapHeld
			}
			schema := &ghclient.SchemaRequestResult{Database: "db0", Type: "mysql", SchemaPath: "db0",
				HeadSHA: "head", SchemaFiles: planRetryTestRequest().SchemaFiles}
			f.executeApply(t.Context(), f.client, schemaCapTestRepo, 1, schema, nil, "staging", 42, "author", CommandResult{},
				tt.storedPlan, nil, "original-plan", nil, nil)
			require.Equal(t, 1, admissions)
			require.Equal(t, tt.wantReleased, locks.releasedIfPending)
			require.Empty(t, locks.released)
			require.Zero(t, planner.calls())
		})
	}
}

func TestPRSchemaCapLabelTimelineBudget(t *testing.T) {
	f := newSchemaCapTestFixture(t, 2, 2, api.PRSchemaCapConfig{Limit: 1, Label: schemaCapTestLabel}, nil)
	f.labels.Store(true)
	now := time.Now().UTC().Truncate(time.Second)
	var reads atomic.Int64
	var more atomic.Bool
	f.mux.HandleFunc("GET /repos/"+schemaCapTestRepo+"/issues/1/timeline", func(w http.ResponseWriter, r *http.Request) {
		reads.Add(1)
		assert.Equal(t, "100", r.URL.Query().Get("per_page"))
		if r.URL.Query().Get("page") == "2" {
			assert.Fail(t, "requested a second timeline page")
			http.Error(w, "unexpected page", http.StatusBadRequest)
			return
		}
		if more.Load() {
			w.Header().Set("Link", `<https://api.github.com/repos/octocat/hello-world/issues/1/timeline?page=2>; rel="next"`)
		}
		_, _ = fmt.Fprintf(w, `[{"id":13,"event":"labeled","label":{"name":%q},"actor":{"login":"alice"},"created_at":%q}]`, schemaCapTestLabel, now.Format(time.RFC3339))
	})
	ctx := t.Context()
	info, err := f.client.FetchPullRequest(ctx, schemaCapTestRepo, 1)
	require.NoError(t, err)
	policy := f.config().PRSchemaCapForRepo(schemaCapTestRepo)
	for _, databases := range [][]string{nil, {"db0"}} {
		_, err := f.evaluatePRSchemaCap(ctx, f.client, schemaCapTestRepo, 1, info, policy, databases, schemaCapAutoPlan)
		require.NoError(t, err)
	}
	require.Zero(t, reads.Load())

	payload := schemaCapLabelPayload(now)
	adopted, err := f.recordPRSchemaLabel(ctx, f.client, schemaCapTestRepo, 1, info, schemaCapTestLabel, payload, now)
	require.NoError(t, err)
	require.True(t, adopted)
	dbs := []string{"db0", "db1"}
	admission, err := f.evaluatePRSchemaCap(ctx, f.client, schemaCapTestRepo, 1, info, policy, dbs, schemaCapVerify)
	require.NoError(t, err)
	require.Equal(t, "label:13", admission.exception)
	require.EqualValues(t, 2, reads.Load())

	// Page one still contains the acknowledged action, but a later page
	// could contain a removal, reapplication, or revision change.
	more.Store(true)
	_, err = f.evaluatePRSchemaCap(ctx, f.client, schemaCapTestRepo, 1, info, policy, dbs, schemaCapReport)
	require.ErrorIs(t, err, errPRSchemaCapHeld)
	require.EqualValues(t, 3, reads.Load())
	adopted, err = f.recordPRSchemaLabel(ctx, f.client, schemaCapTestRepo, 1, info, schemaCapTestLabel, payload, now)
	require.ErrorIs(t, err, ghclient.ErrPRSchemaTimelineIncomplete)
	require.False(t, adopted)
	require.EqualValues(t, 4, reads.Load())
}

func TestPRSchemaCapRetriesLabelVerification(t *testing.T) {
	f := newSchemaCapTestFixture(t, 2, 2, api.PRSchemaCapConfig{Limit: 1, Label: schemaCapTestLabel}, &flakyPlanTernClient{})
	f.labels.Store(true)
	now := time.Now().UTC().Truncate(time.Second)
	var unavailable atomic.Bool
	f.mux.HandleFunc("GET /repos/"+schemaCapTestRepo+"/issues/1/timeline", func(w http.ResponseWriter, _ *http.Request) {
		if unavailable.Load() {
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprintf(w, `[{"id":13,"event":"labeled","label":{"name":%q},"actor":{"login":"alice"},"created_at":%q}]`, schemaCapTestLabel, now.Format(time.RFC3339))
	})
	ctx := t.Context()
	info, err := f.client.FetchPullRequest(ctx, schemaCapTestRepo, 1)
	require.NoError(t, err)
	adopted, err := f.recordPRSchemaLabel(ctx, f.client, schemaCapTestRepo, 1, info, schemaCapTestLabel, schemaCapLabelPayload(now), now)
	require.NoError(t, err)
	require.True(t, adopted)

	run := func() (string, error) {
		return f.runAutoPlanForPR(ctx, f.client, schemaCapTestRepo, 1, "head", 42, "pull_request", "opened", "", "same-delivery")
	}
	unavailable.Store(true)
	_, err = run()
	require.ErrorContains(t, err, "503")
	require.NotErrorIs(t, err, errPRSchemaCapHeld)
	require.False(t, autoPlanFailureIsPermanent(err))
	unavailable.Store(false)
	message, err := run()
	require.NoError(t, err)
	require.Equal(t, "auto-plan started", message)
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

func TestPRSchemaCapReportsUnregisteredDatabase(t *testing.T) {
	f := newSchemaCapTestFixture(t, 1, 0, api.PRSchemaCapConfig{Label: schemaCapTestLabel}, nil)
	cfg := f.config()
	repo := cfg.Repos[schemaCapTestRepo]
	repo.EnableChecks = new(true)
	cfg.Repos[schemaCapTestRepo] = repo
	store := &schemaCapCheckStorage{schemaCapTestStore: f.service.Storage().(*schemaCapTestStore), checks: &foldCheckStore{}}
	f.service = api.New(store, cfg, nil, testLogger())
	type checkRun struct {
		Name, Conclusion string
		Output           struct{ Summary string }
	}
	checkRuns := make(chan checkRun, 1)
	f.mux.HandleFunc("POST /repos/"+schemaCapTestRepo+"/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var posted checkRun
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&posted))
		checkRuns <- posted
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprint(w, `{"id":39}`)
	})
	_, err := f.runAutoPlanForPR(t.Context(), f.client, schemaCapTestRepo, 1, "head", 42, "pull_request", "opened", "", "unknown-db")
	require.NoError(t, err)
	f.DrainInProcessWebhookWork(t.Context())
	require.Len(t, checkRuns, 1)
	posted := <-checkRuns
	require.Equal(t, "SchemaBot", posted.Name)
	require.Equal(t, "failure", posted.Conclusion)
	require.Contains(t, posted.Output.Summary, "db0/schemabot.yaml")
	require.Equal(t, configDiscoveryFailedBlock.blockingReason, store.checks.upserted[0].BlockingReason)
	require.Contains(t, posted.Output.Summary, "not configured")
}

func TestPRSchemaCapMixedConfigsBlockBeforeValidPlanStarts(t *testing.T) {
	f := newSchemaCapTestFixture(t, 2, 1, api.PRSchemaCapConfig{Label: schemaCapTestLabel}, nil)
	cfg := f.config()
	repo := cfg.Repos[schemaCapTestRepo]
	repo.EnableChecks = new(true)
	cfg.Repos[schemaCapTestRepo] = repo
	store := &schemaCapCheckStorage{schemaCapTestStore: f.service.Storage().(*schemaCapTestStore), checks: &foldCheckStore{}}
	f.service = api.New(store, cfg, nil, testLogger())
	f.mux.HandleFunc("POST /repos/"+schemaCapTestRepo+"/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprint(w, `{"id":39}`)
	})
	ctx := t.Context()
	info, err := f.client.FetchPullRequest(ctx, schemaCapTestRepo, 1)
	require.NoError(t, err)
	discovery, err := f.readPRPlanDiscovery(ctx, f.client, schemaCapTestRepo, 1, info, "pull_request", "mixed")
	require.NoError(t, err)
	ready, err := f.registeredAutoPlanConfigs(ctx, f.client, schemaCapTestRepo, 1, info.HeadSHA, 42, discovery.managed)
	require.NoError(t, err)
	require.Equal(t, []string{"db0"}, managedDatabaseNames(ready))
	require.Len(t, store.checks.upserted, 1)
	block := store.checks.upserted[0]
	require.Equal(t, configDiscoveryFailedBlock.blockingReason, block.BlockingReason)
	require.Equal(t, "failure", block.Conclusion)
	require.Contains(t, block.ErrorMessage, "db1/schemabot.yaml")
	require.Contains(t, block.ErrorMessage, "not configured")
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

// A plan stopped before it dispatches tells the PR why. A refusal SchemaBot
// wrote shows its own words, even when a caller wrapped it with more text, and
// any other failure shows a fixed line and keeps its text out of the comment
// and the response.
func TestPlanRefusalCommentShowsOnlyAuthoredText(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "a refusal wrapped with more text shows only its own words",
			err:  fmt.Errorf("admit plan: %w: %s", errPRSchemaMoved, rawInfrastructureError),
			want: errPRSchemaMoved.Error(),
		},
		{
			name: "any other failure shows the fixed line",
			err:  errors.New(rawInfrastructureError),
			want: "Could not verify the PR database limit. Retry the plan.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux := setupGitHubServer(t)
			comments := recordComments(t, mux)
			st := &unlockTestStorage{locks: &unlockTestLockStore{}, applies: &noActiveAppliesStore{}}
			h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))
			w := httptest.NewRecorder()

			h.answerPRSchemaPlanRefusal(w, schemaCapTestRepo, 1, 12345, "staging", "testuser", tt.err)

			body := requireComment(t, comments, "plan refusal comment")
			assert.Contains(t, body, tt.want)
			requireNoInfrastructureDetail(t, body)
			assert.NotContains(t, body, "admit plan")
			assert.Contains(t, w.Body.String(), tt.want)
			requireNoInfrastructureDetail(t, w.Body.String())
		})
	}
}
