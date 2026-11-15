package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
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
// as modified so the participant covering it becomes expected. The file exists
// at every commit but the default branch tip, so the leader's comparison against
// the default branch reads it as the PR's own proposal.
func serveLeaderPRFiles(t *testing.T, mux *http.ServeMux, path string) {
	t.Helper()
	const defaultTip, subtreePrefix = "default-tip", "tree~"
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]map[string]string{{
			"filename": path,
			"status":   "modified",
		}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]string{"default_branch": "main"}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/ref/heads/main", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"object": map[string]string{"sha": defaultTip}}))
	})
	// Reading the changed files' modes finds the merge base, then reads the
	// compare diff, which shows the path as a regular file.
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/{range}", func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.Header.Get("Accept"), "diff") {
			writeRegularFilesDiff(t, w, []string{path})
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"merge_base_commit": map[string]string{"sha": "merge-base"}}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/", func(w http.ResponseWriter, req *http.Request) {
		treeSHA := strings.TrimPrefix(req.URL.Path, "/repos/octocat/hello-world/git/trees/")
		entries := []map[string]string{}
		if treeSHA != defaultTip {
			// A commit SHA serves the root level; a subtree SHA names its directory.
			dir, rest := "", path
			if subtree, isSubtree := strings.CutPrefix(treeSHA, subtreePrefix); isSubtree {
				dir = strings.ReplaceAll(subtree, "~", "/")
				rest = strings.TrimPrefix(path, dir+"/")
			}
			if segment, _, isDir := strings.Cut(rest, "/"); isDir {
				entries = append(entries, map[string]string{"path": segment, "type": "tree", "sha": subtreePrefix + strings.ReplaceAll(strings.TrimPrefix(dir+"/"+segment, "/"), "/", "~")})
			} else {
				entries = append(entries, map[string]string{"path": segment, "type": "blob", "sha": "head-file"})
			}
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tree": entries, "truncated": false}))
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

		assert.Equal(t, aggregateFoldScheduleRefold, followUp)
		assert.Error(t, err)
	})

	t.Run("superseded head schedules a leader re-fold with no error", func(t *testing.T) {
		h, mux, client := newFoldHandler(t, nonAggregateConfig(), &foldCheckStore{})
		serveHeadSHA(t, mux, "current999")

		followUp, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "stale111")

		assert.Equal(t, aggregateFoldScheduleRefold, followUp)
		assert.NoError(t, err, "a genuinely newer head is not an operational failure")
	})

	t.Run("PR fetch failure schedules a leader re-fold and returns an error", func(t *testing.T) {
		h, mux, client := newFoldHandler(t, nonAggregateConfig(), &foldCheckStore{})
		mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		})

		followUp, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")

		assert.Equal(t, aggregateFoldScheduleRefold, followUp)
		assert.Error(t, err)
	})

	t.Run("GetByPR failure schedules a leader re-fold and returns an error", func(t *testing.T) {
		store := &foldCheckStore{byPRErr: assert.AnError}
		h, mux, client := newFoldHandler(t, nonAggregateConfig(), store)
		serveHeadSHA(t, mux, "abc123")

		followUp, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")

		assert.Equal(t, aggregateFoldScheduleRefold, followUp)
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

	t.Run("leader that cannot compare PR files with the default branch publishes nothing and schedules a participant re-fold", func(t *testing.T) {
		store := &foldCheckStore{}
		h, mux, client := newFoldHandler(t, aggregateLeaderConfig(), store)
		serveHeadSHA(t, mux, "abc123")
		mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
			require.NoError(t, json.NewEncoder(w).Encode([]map[string]string{{
				"filename": "tenant-b/schema/orders.sql",
				"status":   "modified",
			}}))
		})
		mux.HandleFunc("GET /repos/octocat/hello-world", func(w http.ResponseWriter, _ *http.Request) {
			require.NoError(t, json.NewEncoder(w).Encode(map[string]string{"default_branch": "main"}))
		})
		mux.HandleFunc("GET /repos/octocat/hello-world/git/ref/heads/main", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		})
		// tenant-b has not reported, so nothing but the failed comparison
		// keeps the leader from treating the PR as proposing no work.
		serveParticipantCheckRunsEmpty(t, mux, "abc123")
		var checkRunWrites atomic.Int64
		mux.HandleFunc("/repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, _ *http.Request) {
			checkRunWrites.Add(1)
			w.WriteHeader(http.StatusCreated)
		})

		followUp, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")

		assert.Equal(t, aggregateFoldScheduleParticipantRefold, followUp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "to what it proposes")
		assert.Zero(t, checkRunWrites.Load(), "no Check Run is published while the leader cannot tell which participants the PR requires")
		assert.Empty(t, store.upserted, "no stored aggregate is written")
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
		serveLeaderPRFiles(t, mux, "README.md")
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

// TestLeaderSettleWithNoProposedWorkGuards covers which stored aggregate an
// unscoped leader settles when the PR proposes nothing for any participant or
// for the leader itself. Only the leader's own unscoped aggregate on the head
// that is not already passing is settled; an aggregate on another commit, one
// that already passes, or an aggregate row of a named environment stays as it
// is. A settle whose publish fails arms the participant re-fold so the gate
// does not stay on its previous state.
func TestLeaderSettleWithNoProposedWorkGuards(t *testing.T) {
	aggregateRow := func(env, headSHA, status, conclusion string) *storage.Check {
		return &storage.Check{
			Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: headSHA,
			Environment: env, DatabaseType: aggregateSentinel, DatabaseName: aggregateSentinel,
			Status: status, Conclusion: conclusion, CheckRunID: 42,
		}
	}
	tests := []struct {
		name           string
		stored         *storage.Check
		publishStatus  int
		wantFollowUp   aggregateFoldFollowUp
		wantError      bool
		wantPublishing bool
	}{
		{
			name:           "waiting aggregate on the head settles",
			stored:         aggregateRow(aggregateSentinel, "abc123", checkStatusInProgress, ""),
			publishStatus:  http.StatusCreated,
			wantFollowUp:   aggregateFoldClearParticipantRefoldBudget,
			wantPublishing: true,
		},
		{
			name:         "aggregate on another commit stays",
			stored:       aggregateRow(aggregateSentinel, "older111", checkStatusInProgress, ""),
			wantFollowUp: aggregateFoldNoFollowUp,
		},
		{
			name:         "aggregate already passing on the head stays",
			stored:       aggregateRow(aggregateSentinel, "abc123", checkStatusCompleted, checkConclusionSuccess),
			wantFollowUp: aggregateFoldNoFollowUp,
		},
		{
			name:         "aggregate row of a named environment is not the unscoped aggregate",
			stored:       aggregateRow("staging", "abc123", checkStatusInProgress, ""),
			wantFollowUp: aggregateFoldNoFollowUp,
		},
		{
			name:           "failed publish schedules a participant re-fold",
			stored:         aggregateRow(aggregateSentinel, "abc123", checkStatusInProgress, ""),
			publishStatus:  http.StatusInternalServerError,
			wantFollowUp:   aggregateFoldScheduleParticipantRefold,
			wantError:      true,
			wantPublishing: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &foldCheckStore{byPR: []*storage.Check{tt.stored}}
			h, mux, client := newFoldHandler(t, aggregateLeaderConfig(), store)
			serveHeadSHA(t, mux, "abc123")
			// A README change proposes nothing for any participant or for the
			// leader, so the fold reaches the settle.
			serveLeaderPRFiles(t, mux, "README.md")
			var checkRunWrites atomic.Int64
			mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, _ *http.Request) {
				checkRunWrites.Add(1)
				w.WriteHeader(tt.publishStatus)
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 555}))
			})

			followUp, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")

			assert.Equal(t, tt.wantFollowUp, followUp)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			if tt.wantPublishing {
				assert.Equal(t, int64(1), checkRunWrites.Load(), "the settle publishes the verified no-work aggregate")
			} else {
				assert.Zero(t, checkRunWrites.Load(), "no Check Run replaces the stored aggregate")
				assert.Empty(t, store.upserted, "no stored aggregate is written")
			}
		})
	}
}
