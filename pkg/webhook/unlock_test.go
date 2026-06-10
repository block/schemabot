package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
)

// unlockTestStorage serves a fixed set of locks and records release calls so
// tests can verify whether the unlock guard permitted a release.
type unlockTestStorage struct {
	emptyStorage
	locks   *unlockTestLockStore
	applies storage.ApplyStore
}

func (s *unlockTestStorage) Locks() storage.LockStore { return s.locks }

func (s *unlockTestStorage) Applies() storage.ApplyStore { return s.applies }

type unlockTestLockStore struct {
	storage.LockStore
	locks             []*storage.Lock
	releaseCalls      int
	forceReleaseCalls int
}

func (s *unlockTestLockStore) GetByPR(_ context.Context, _ string, _ int) ([]*storage.Lock, error) {
	return s.locks, nil
}

func (s *unlockTestLockStore) Release(_ context.Context, _, _, _ string) error {
	s.releaseCalls++
	return nil
}

func (s *unlockTestLockStore) ForceRelease(_ context.Context, _, _ string) error {
	s.forceReleaseCalls++
	return nil
}

type failingApplyLookupStore struct {
	storage.ApplyStore
}

func (s *failingApplyLookupStore) GetByDatabase(_ context.Context, _, _, _ string) ([]*storage.Apply, error) {
	return nil, errors.New("storage read failed")
}

type noActiveAppliesStore struct {
	storage.ApplyStore
}

func (s *noActiveAppliesStore) GetByDatabase(_ context.Context, _, _, _ string) ([]*storage.Apply, error) {
	return nil, nil
}

func unlockTestHandler(t *testing.T, st storage.Storage, client *ghclient.InstallationClient) *Handler {
	t.Helper()
	service := api.New(st, &api.ServerConfig{}, nil, testLogger())
	return &Handler{
		service:   service,
		ghClients: ghclient.NewSingleClientSet(defaultAppName, &fakeClientFactory{client: client}),
		logger:    testLogger(),
	}
}

func recordComments(t *testing.T, mux *http.ServeMux) chan string {
	t.Helper()
	comments := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Body string `json:"body"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		comments <- body.Body
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 99}))
	})
	return comments
}

// TestUnlockRefusedWhenActiveApplyLookupFails verifies that the unlock guard
// fails closed: when SchemaBot cannot read apply state for a locked database,
// no lock is released and the PR receives an error comment. A lock released
// under storage uncertainty could let another PR start a concurrent apply
// against a database with an apply still in flight.
func TestUnlockRefusedWhenActiveApplyLookupFails(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := recordComments(t, mux)

	lockStore := &unlockTestLockStore{locks: []*storage.Lock{{
		DatabaseName: "orders",
		DatabaseType: storage.DatabaseTypeMySQL,
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	}}}
	st := &unlockTestStorage{locks: lockStore, applies: &failingApplyLookupStore{}}
	h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

	h.handleUnlockCommand("octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

	assert.Equal(t, 0, lockStore.releaseCalls, "unlock must not release any lock when apply state is unknown")
	assert.Equal(t, 0, lockStore.forceReleaseCalls, "unlock must not force-release any lock when apply state is unknown")

	select {
	case body := <-comments:
		assert.Contains(t, body, "Failed to verify active applies for database `orders`")
		assert.Contains(t, body, "storage read failed")
		assert.Contains(t, body, "No locks were released")
	case <-time.After(2 * time.Second):
		require.FailNow(t, "timed out waiting for unlock error comment")
	}
}

// TestUnlockReleasesLockWhenNoActiveApplies verifies the unlock happy path:
// when the active-apply check confirms no non-terminal apply exists for the
// locked database, the PR-owned lock is released, a success comment is
// posted, and the apply check run is set to neutral.
func TestUnlockReleasesLockWhenNoActiveApplies(t *testing.T) {
	client, mux := setupGitHubServer(t)
	comments := recordComments(t, mux)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"head": map[string]any{"ref": "feature-branch", "sha": "abc123"},
			"base": map[string]any{"ref": "main", "sha": "def456"},
			"user": map[string]any{"login": "testuser"},
		}))
	})
	checkRuns := make(chan string, 2)
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Conclusion string `json:"conclusion"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		checkRuns <- body.Conclusion
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 1}))
	})

	lockStore := &unlockTestLockStore{locks: []*storage.Lock{{
		DatabaseName: "orders",
		DatabaseType: storage.DatabaseTypeMySQL,
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		Owner:        "octocat/hello-world#1",
	}}}
	st := &unlockTestStorage{locks: lockStore, applies: &noActiveAppliesStore{}}
	h := unlockTestHandler(t, st, ghclient.NewInstallationClient(client, testLogger()))

	h.handleUnlockCommand("octocat/hello-world", 1, 12345, "testuser", CommandResult{Action: action.Unlock})

	assert.Equal(t, 1, lockStore.releaseCalls, "exactly one owned-lock release expected")
	assert.Equal(t, 0, lockStore.forceReleaseCalls, "non-force unlock must not force-release")

	select {
	case body := <-comments:
		assert.Contains(t, body, "Lock Released")
		assert.Contains(t, body, "orders")
		assert.Contains(t, body, "@testuser")
	case <-time.After(2 * time.Second):
		require.FailNow(t, "timed out waiting for unlock success comment")
	}

	select {
	case conclusion := <-checkRuns:
		assert.Equal(t, "neutral", conclusion)
	case <-time.After(2 * time.Second):
		require.FailNow(t, "timed out waiting for check run update after unlock")
	}
}
