package webhook

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

const capTestRepo = "octocat/hello-world"
const capTestLabel = "schemabot-lsc"

var capTestRevision = prSchemaRevision{Head: "head", Base: "base", BaseRef: "main"}

type prSchemaCapFixture struct {
	*Handler
	client   *ghclient.InstallationClient
	settings *memorySettingsStore
	hasLabel atomic.Bool
}

func newPRSchemaCapFixture(t *testing.T, schemas int) *prSchemaCapFixture {
	t.Helper()
	github, mux := setupGitHubServer(t)
	settings := newMemorySettingsStore()
	publish := false
	cfg := &api.ServerConfig{
		PRSchemaCap: api.PRSchemaCapPolicy{Limit: 5, Label: capTestLabel},
		Repos: map[string]api.RepoConfig{capTestRepo: {
			EnableChecks: &publish,
			PRSchemaCap:  api.PRSchemaCapOverride{PublishComment: &publish},
		}},
	}
	logger := testLogger()
	client := ghclient.NewInstallationClient(github, logger)
	factory := &fakeClientFactory{client: client}
	service := api.New(&durableWebhookTestStorage{settings: settings}, cfg, nil, logger)
	fixture := &prSchemaCapFixture{Handler: NewHandler(service, factory, nil, logger), client: client, settings: settings}
	mux.HandleFunc("/repos/"+capTestRepo, func(w http.ResponseWriter, _ *http.Request) {
		_, err := fmt.Fprint(w, `{"default_branch":"main"}`)
		assert.NoError(t, err)
	})
	mux.HandleFunc("/repos/"+capTestRepo+"/git/ref/heads/main", func(w http.ResponseWriter, _ *http.Request) {
		_, err := fmt.Fprint(w, `{"object":{"sha":"base"}}`)
		assert.NoError(t, err)
	})
	mux.HandleFunc("/repos/"+capTestRepo+"/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		labels := "[]"
		if fixture.hasLabel.Load() {
			labels = `[{"name":"schemabot-lsc"}]`
		}
		_, err := fmt.Fprintf(w, `{"state":"open","head":{"sha":"head"},"base":{"sha":"base","ref":"main"},"labels":%s}`, labels)
		assert.NoError(t, err)
	})
	mux.HandleFunc("/repos/"+capTestRepo+"/issues/1/timeline", func(w http.ResponseWriter, _ *http.Request) {
		_, err := fmt.Fprint(w, `[{"id":13,"event":"labeled","label":{"name":"schemabot-lsc"},"actor":{"login":"mona"},"created_at":"2026-11-18T21:00:00Z"}]`)
		assert.NoError(t, err)
	})
	discovery := &prSchemaDiscovery{}
	for i := range schemas {
		database := "local"
		if i >= 3 {
			database = "other-plane"
		}
		discovery.schemas = append(discovery.schemas, ghclient.PRSchema{Database: database, Namespace: fmt.Sprintf("ns%d", i)})
	}
	registrations, err := cfg.PRSchemaCapRegistrations(capTestRepo)
	require.NoError(t, err)
	registry, err := json.Marshal(registrations)
	require.NoError(t, err)
	fixture.prSchemaDiscovery.put(prSchemaDiscoveryKey{
		Repo: capTestRepo, PR: 1, Revision: capTestRevision, DefaultTip: "base", BaseTip: "base", Registrations: sha256.Sum256(registry),
	}, discovery)
	return fixture
}

func TestPRSchemaCapAdmitsFiveAndHoldsSixAcrossPlanes(t *testing.T) {
	for _, size := range []int{5, 6} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			fixture := newPRSchemaCapFixture(t, size)
			revision, err := fixture.admitPRSchemaWork(t.Context(), fixture.client, capTestRepo, 1, prSchemaRevision{})
			require.Equal(t, capTestRevision, revision)
			if size == 5 {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, errPRSchemaCapHeld)
		})
	}
}

// The hold precedes database dispatch even when a command names only one
// database or target. A cached apply cannot get as far as loading its plan.
func TestPRSchemaCapStopsAutomaticManualAndApplyDispatch(t *testing.T) {
	fixture := newPRSchemaCapFixture(t, 6)
	ctx := t.Context()
	result, err := fixture.runAutoPlanForPR(ctx, fixture.client, capTestRepo, 1, "head", 42, "pull_request", "opened", "", "delivery")
	require.NoError(t, err)
	require.Equal(t, errPRSchemaCapHeld.Error(), result)

	w := httptest.NewRecorder()
	fixture.handlePlanCommand(w, capTestRepo, 1, "staging", "local", "one-shard", "", 42, "delivery", "mona", 0)
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), errPRSchemaCapHeld.Error())

	_, err = fixture.admitPRSchemaBeforeQueue(ctx, capTestRepo, 1, 42, capTestRevision)
	require.ErrorIs(t, err, errPRSchemaCapHeld)
	for _, invoke := range []func(context.Context, string, int, string, string, int64, string, CommandResult) (bool, error){
		fixture.applyCommandCore, fixture.applyConfirmCommandCore,
	} {
		retry, err := invoke(ctx, capTestRepo, 1, "staging", "local", 42, "mona", CommandResult{})
		require.NoError(t, err)
		require.False(t, retry)
	}
}

type schemaCapPlanClient struct {
	tern.Client
	onPlan func()
	calls  int
}

func (c *schemaCapPlanClient) Plan(context.Context, *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	c.calls++
	c.onPlan()
	return nil, grpcstatus.Error(grpccodes.Unavailable, "transient remote failure")
}

func (c *schemaCapPlanClient) IsRemote() bool   { return true }
func (c *schemaCapPlanClient) Endpoint() string { return "remote:9090" }

type schemaCapRetryStorage struct {
	planRetryStorage
	settings storage.SettingsStore
}

func (s schemaCapRetryStorage) Settings() storage.SettingsStore { return s.settings }

// A fresh exception allows a plan attempt. Removing the label before its
// transient retry stops the second data-plane call.
func TestPRSchemaCapRechecksRevocationOnRetry(t *testing.T) {
	fixture := newPRSchemaCapFixture(t, 6)
	fixture.hasLabel.Store(true)
	ctx := withPRSchemaAdmission(t.Context(), fixture.client, capTestRevision)
	policy := fixture.prSchemaCap(capTestRepo)
	require.NoError(t, fixture.recordPRSchemaLabel(ctx, capTestRepo, 1, policy, capTestRevision, ghclient.PRLabelState{EventID: 13}))
	planClient := &schemaCapPlanClient{onPlan: func() { fixture.hasLabel.Store(false) }}
	retryHandler := newPlanRetryTestHandler(planClient, time.Millisecond)
	cfg := retryHandler.config()
	cfg.Repos = fixture.config().Repos
	cfg.PRSchemaCap = fixture.config().PRSchemaCap
	fixture.service = api.New(&schemaCapRetryStorage{settings: fixture.settings}, cfg, map[string]tern.Client{"remote/staging": planClient}, testLogger())
	fixture.service.OnPRSchemaAdmission = fixture.admitDispatchPRSchemaWork
	fixture.transientPlanRetryDelay = time.Millisecond

	// The test uses the same precomputed PR-wide discovery after replacing
	// the empty registry with the test planner's registered database.
	discovery := fixture.prSchemaDiscovery.get(prSchemaDiscoveryKey{
		Repo: capTestRepo, PR: 1, Revision: capTestRevision, DefaultTip: "base", BaseTip: "base", Registrations: sha256.Sum256([]byte("{}")),
	})
	registrations, err := cfg.PRSchemaCapRegistrations(capTestRepo)
	require.NoError(t, err)
	registry, err := json.Marshal(registrations)
	require.NoError(t, err)
	fixture.prSchemaDiscovery.put(prSchemaDiscoveryKey{
		Repo: capTestRepo, PR: 1, Revision: capTestRevision, DefaultTip: "base", BaseTip: "base", Registrations: sha256.Sum256(registry),
	}, discovery)

	request := planRetryTestRequest()
	request.Repository = capTestRepo
	request.PullRequest = new(int32(1))
	request.HeadSHA = new(capTestRevision.Head)
	_, _, err = fixture.executePlanProtoWithTransientRetry(ctx, request, capTestRepo, 1)
	require.ErrorIs(t, err, errPRSchemaCapHeld)
	require.Equal(t, 1, planClient.calls)
}

func TestPRSchemaLabelAcknowledgmentIsRevocableAndClaimedOnce(t *testing.T) {
	fixture := newPRSchemaCapFixture(t, 6)
	ctx := t.Context()
	policy := fixture.prSchemaCap(capTestRepo)
	state := ghclient.PRLabelState{EventID: 13, RevisionID: 7, Present: true}
	require.NoError(t, fixture.recordPRSchemaLabel(ctx, capTestRepo, 1, policy, capTestRevision, state))
	_, ack, err := readPRSchemaAcknowledgment(ctx, fixture.settings, capTestRepo, 1)
	require.NoError(t, err)
	require.True(t, ack.authorizes(capTestRevision, state, capTestLabel))
	require.False(t, ack.authorizes(prSchemaRevision{Head: "new-head", BaseRef: "main", Base: "base"}, state, capTestLabel))
	require.False(t, ack.authorizes(prSchemaRevision{Head: "head", BaseRef: "release", Base: "base"}, state, capTestLabel))
	require.False(t, ack.authorizes(capTestRevision, ghclient.PRLabelState{EventID: 13, RevisionID: 15, Present: true}, capTestLabel))
	require.False(t, ack.authorizes(capTestRevision, ghclient.PRLabelState{EventID: 14}, capTestLabel))
	require.False(t, ack.authorizes(capTestRevision, ghclient.PRLabelState{EventID: 17, RevisionID: 7, Present: true}, capTestLabel))

	claims := make(chan *storage.Setting, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			claim, _ := fixture.claimPRSchemaAutoPlan(ctx, capTestRepo, 1, "local", ack)
			if claim != nil {
				claims <- claim
			}
		})
	}
	wg.Wait()
	require.Len(t, claims, 1)
	first := <-claims
	require.NoError(t, fixture.finishPRSchemaAutoPlan(ctx, first, ack.EventID, true))

	retry, err := fixture.claimPRSchemaAutoPlan(ctx, capTestRepo, 1, "local", ack)
	require.NoError(t, err)
	require.NotNil(t, retry, "a failed database is claimable again")
	require.NoError(t, fixture.finishPRSchemaAutoPlan(ctx, retry, ack.EventID, false))
	complete, err := fixture.claimPRSchemaAutoPlan(ctx, capTestRepo, 1, "local", ack)
	require.NoError(t, err)
	require.Nil(t, complete, "a completed database is not scheduled again")
}

func TestPRSchemaLabelDeliveryCannotUseLaterTimelineEvent(t *testing.T) {
	updated := time.Date(2026, 11, 18, 21, 0, 0, 0, time.UTC)
	var payload pullRequestPayload
	payload.Sender.Login = "mona"
	payload.PullRequest.UpdatedAt = updated
	state := ghclient.PRLabelState{EventID: 13, Present: true, Actor: "mona", ChangedAt: updated.Add(2 * time.Second)}
	require.True(t, matchesPRSchemaLabelAddition(payload, state, updated.Add(3*time.Second)))
	require.False(t, matchesPRSchemaLabelAddition(payload, state, updated.Add(time.Second)))
	payload.PullRequest.UpdatedAt = updated.Add(3 * time.Second)
	require.False(t, matchesPRSchemaLabelAddition(payload, state, updated.Add(4*time.Second)))
}
