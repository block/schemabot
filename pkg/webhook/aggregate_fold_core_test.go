package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// foldCheckStore is a configurable CheckStore for exercising the follow-up /
// error contract of updateAggregateCheckOnce in isolation. Each hook controls
// exactly one branch the fold core takes, and upsertCalls records how many
// per-environment writes were attempted so a test can prove later environments
// are still attempted after an earlier write fails.
type foldCheckStore struct {
	storage.CheckStore
	byPR        []*storage.Check
	byPRErr     error
	get         *storage.Check
	getErr      error
	upsertErr   error
	upsertCalls int
	upserted    []*storage.Check
}

func (s *foldCheckStore) GetByPR(_ context.Context, _ string, _ int) ([]*storage.Check, error) {
	return s.byPR, s.byPRErr
}

func (s *foldCheckStore) Get(_ context.Context, _ string, _ int, _, _, _ string) (*storage.Check, error) {
	return s.get, s.getErr
}

func (s *foldCheckStore) Upsert(_ context.Context, check *storage.Check) error {
	s.upsertCalls++
	s.upserted = append(s.upserted, check)
	return s.upsertErr
}

// foldStorage wires a foldCheckStore into the empty storage used elsewhere in
// the package so only the check store's behavior varies per test.
type foldStorage struct {
	emptyStorage
	checks storage.CheckStore
}

func (s *foldStorage) Checks() storage.CheckStore {
	return s.checks
}

// newFoldHandler builds a handler backed by the given config and check store
// and a fake GitHub server, returning the handler, the GitHub mux for route
// registration, and the installation client to pass to updateAggregateCheckOnce.
func newFoldHandler(t *testing.T, cfg *api.ServerConfig, checks storage.CheckStore) (*Handler, *http.ServeMux, *ghclient.InstallationClient) {
	t.Helper()
	client, mux := setupGitHubServer(t)
	// Resolve the App slug so a participant Check Run read that finds no run
	// classifies as a clean "not reported" (retriable, no error) rather than an
	// ownership-unverifiable read error — letting a test exercise the genuine
	// pending-convergence path distinctly from a participant read failure.
	installClient := ghclient.NewInstallationClientWithSlug(client, testLogger(), "schemabot")
	h := &Handler{
		service:   api.New(&foldStorage{checks: checks}, cfg, nil, testLogger()),
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: installClient}),
		logger:    testLogger(),
	}
	return h, mux, installClient
}

// serveHeadSHA registers the PR lookup FetchPullRequest performs, reporting the
// given commit as the PR's current head.
func serveHeadSHA(t *testing.T, mux *http.ServeMux, sha string) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"head": map[string]any{"sha": sha, "ref": "feature-branch"},
			"base": map[string]any{"sha": "def456", "ref": "main"},
			"user": map[string]any{"login": "testuser"},
		}))
	})
}

// serveLeaderPRFiles registers the changed-files lookup the aggregate leader
// performs to resolve which participants a PR requires, reporting the given path
// as modified so the participant covering it becomes expected.
func serveLeaderPRFiles(t *testing.T, mux *http.ServeMux, path string) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]map[string]string{{
			"filename": path,
			"status":   "modified",
		}}))
	})
}

// serveParticipantCheckRunsEmpty registers the commit check-runs lookup
// FindCheckRunByName performs, reporting no runs on the commit so an expected
// participant reads as not-yet-reported (a retriable, still-pending outcome).
func serveParticipantCheckRunsEmpty(t *testing.T, mux *http.ServeMux, sha string) {
	t.Helper()
	mux.HandleFunc("GET /repos/octocat/hello-world/commits/"+sha+"/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"total_count": 0,
			"check_runs":  []any{},
		}))
	})
}

// serveCheckRunCreate registers the check-run create endpoint so the fold's
// upsert reaches the storage write.
func serveCheckRunCreate(t *testing.T, mux *http.ServeMux) {
	t.Helper()
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 555}))
	})
}

// TestUpdateAggregateCheckOnceFollowUpContract pins the follow-up disposition
// and error semantics of the one-shot fold core so the durable dispatch path
// (which acts on both) can rely on them. The legacy fire-and-forget wrapper
// discards the error but applies the same follow-up, so these mappings also
// characterize its unchanged behavior.
func TestUpdateAggregateCheckOnceFollowUpContract(t *testing.T) {
	t.Run("checks disabled yields no follow-up and no error", func(t *testing.T) {
		checksOff := false
		cfg := &api.ServerConfig{Repos: map[string]api.RepoConfig{
			"octocat/hello-world": {EnableChecks: &checksOff},
		}}
		h, _, client := newFoldHandler(t, cfg, &foldCheckStore{})

		followUp, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")

		assert.Equal(t, aggregateFoldNoFollowUp, followUp)
		assert.NoError(t, err)
	})

	t.Run("empty head SHA schedules a leader re-fold and returns an error", func(t *testing.T) {
		h, _, client := newFoldHandler(t, nonAggregateConfig(), &foldCheckStore{})

		followUp, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "")

		assert.Equal(t, aggregateFoldScheduleLeaderRefold, followUp)
		assert.Error(t, err)
	})

	t.Run("superseded head schedules a leader re-fold with no error", func(t *testing.T) {
		h, mux, client := newFoldHandler(t, nonAggregateConfig(), &foldCheckStore{})
		serveHeadSHA(t, mux, "current999")

		followUp, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "stale111")

		assert.Equal(t, aggregateFoldScheduleLeaderRefold, followUp)
		assert.NoError(t, err, "a genuinely newer head is not an operational failure")
	})

	t.Run("PR fetch failure schedules a leader re-fold and returns an error", func(t *testing.T) {
		h, mux, client := newFoldHandler(t, nonAggregateConfig(), &foldCheckStore{})
		mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		})

		followUp, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")

		assert.Equal(t, aggregateFoldScheduleLeaderRefold, followUp)
		assert.Error(t, err)
	})

	t.Run("GetByPR failure schedules a leader re-fold and returns an error", func(t *testing.T) {
		store := &foldCheckStore{byPRErr: assert.AnError}
		h, mux, client := newFoldHandler(t, nonAggregateConfig(), store)
		serveHeadSHA(t, mux, "abc123")

		followUp, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")

		assert.Equal(t, aggregateFoldScheduleLeaderRefold, followUp)
		assert.Error(t, err)
	})

	t.Run("no checks and no participants yields no follow-up and no error", func(t *testing.T) {
		h, mux, client := newFoldHandler(t, nonAggregateConfig(), &foldCheckStore{})
		serveHeadSHA(t, mux, "abc123")

		followUp, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")

		assert.Equal(t, aggregateFoldNoFollowUp, followUp)
		assert.NoError(t, err)
	})

	t.Run("leader that cannot fetch PR files schedules a participant re-fold and returns an error", func(t *testing.T) {
		h, mux, client := newFoldHandler(t, aggregateLeaderConfig(), &foldCheckStore{})
		serveHeadSHA(t, mux, "abc123")
		mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		})

		followUp, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")

		assert.Equal(t, aggregateFoldScheduleParticipantRefold, followUp)
		assert.Error(t, err)
	})

	t.Run("participant that has not reported schedules a participant re-fold with no error", func(t *testing.T) {
		h, mux, client := newFoldHandler(t, aggregateLeaderConfig(), &foldCheckStore{})
		serveHeadSHA(t, mux, "abc123")
		serveLeaderPRFiles(t, mux, "tenant-b/schema/orders.sql")
		serveParticipantCheckRunsEmpty(t, mux, "abc123")
		serveCheckRunCreate(t, mux)

		followUp, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")

		assert.Equal(t, aggregateFoldScheduleParticipantRefold, followUp)
		assert.NoError(t, err)
	})

	t.Run("participant check read failure schedules a participant re-fold with no error", func(t *testing.T) {
		// A failed participant Check Run read is deliberately conveyed only
		// through the retriable disposition, not the error return: participant
		// convergence is owned by the re-fold budget, so folding the read error
		// in would make a retry-owning caller and the re-fold timer double-retry
		// the same condition. The fold reports the aggregate as
		// published-but-unconverged with a nil error.
		h, mux, client := newFoldHandler(t, aggregateLeaderConfig(), &foldCheckStore{})
		serveHeadSHA(t, mux, "abc123")
		serveLeaderPRFiles(t, mux, "tenant-b/schema/orders.sql")
		mux.HandleFunc("GET /repos/octocat/hello-world/commits/abc123/check-runs", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		})
		serveCheckRunCreate(t, mux)

		followUp, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")

		assert.Equal(t, aggregateFoldScheduleParticipantRefold, followUp)
		assert.NoError(t, err)
	})

	t.Run("successful fold clears the participant re-fold budget with no error", func(t *testing.T) {
		store := &foldCheckStore{byPR: []*storage.Check{{
			Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: "abc123",
			Environment: "staging", DatabaseType: "mysql", DatabaseName: "orders",
			Status: "completed", Conclusion: "success",
		}}}
		h, mux, client := newFoldHandler(t, nonAggregateConfig(), store)
		serveHeadSHA(t, mux, "abc123")
		serveCheckRunCreate(t, mux)

		followUp, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")

		assert.Equal(t, aggregateFoldClearParticipantRefoldBudget, followUp)
		assert.NoError(t, err)
		assert.Equal(t, 1, store.upsertCalls)
	})

	t.Run("a per-environment upsert failure still attempts every environment, joins the errors, and schedules a re-fold", func(t *testing.T) {
		cfg := &api.ServerConfig{
			AllowedEnvironments: []string{"staging", "production"},
			Repos:               map[string]api.RepoConfig{"octocat/hello-world": {}},
		}
		store := &foldCheckStore{
			byPR: []*storage.Check{
				{Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: "abc123", Environment: "staging", DatabaseType: "mysql", DatabaseName: "orders", Status: "completed", Conclusion: "success"},
				{Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: "abc123", Environment: "production", DatabaseType: "mysql", DatabaseName: "orders", Status: "completed", Conclusion: "success"},
			},
			upsertErr: assert.AnError,
		}
		h, mux, client := newFoldHandler(t, cfg, store)
		serveHeadSHA(t, mux, "abc123")
		serveCheckRunCreate(t, mux)

		followUp, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")

		assert.Equal(t, aggregateFoldScheduleParticipantRefold, followUp,
			"a failed aggregate publish must arm the bounded re-fold, not clear the budget: the fire-and-forget callers have no retry of their own")
		assert.Error(t, err)
		assert.Equal(t, 2, store.upsertCalls, "a failed environment write must not skip the remaining environments")
	})

	t.Run("a failed Check Run create schedules a re-fold and returns an error", func(t *testing.T) {
		store := &foldCheckStore{
			byPR: []*storage.Check{
				{Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: "abc123", Environment: "staging", DatabaseType: "mysql", DatabaseName: "orders", Status: checkStatusInProgress},
			},
		}
		h, mux, client := newFoldHandler(t, nonAggregateConfig(), store)
		serveHeadSHA(t, mux, "abc123")
		mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		})

		followUp, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")

		assert.Equal(t, aggregateFoldScheduleParticipantRefold, followUp,
			"a failed Check Run write must arm the bounded re-fold so the gate converges without waiting for an external event")
		assert.Error(t, err)
		assert.Empty(t, store.upserted, "no stored state may record a Check Run that was never created")
	})
}
