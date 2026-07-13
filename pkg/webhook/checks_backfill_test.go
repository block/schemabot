package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A Check Run backfill on a closed PR is refused: close-time cleanup owns a
// closed PR's stored check state, so recreating Check Runs there would
// resurrect what cleanup settled.
func TestBackfillPRCheckRunsSkipsClosedPR(t *testing.T) {
	client, mux := setupGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"state": "closed",
			"head":  map[string]any{"sha": "abc123", "ref": "feature-branch"},
			"base":  map[string]any{"sha": "def456", "ref": "main"},
			"user":  map[string]any{"login": "testuser"},
		}))
	})
	h := actorAuthStorageTestHandler(actorAuthTestConfig(false), &emptyStorage{}, ghclient.NewInstallationClient(client, testLogger()))

	outcome, err := h.BackfillPRCheckRuns(t.Context(), "octocat/hello-world", 1, 12345)

	require.NoError(t, err)
	assert.Equal(t, "skipped: PR is closed", outcome)
}

// appliesByStateStorage serves a fixed set of applies for any PR, so a
// backfill test can stage in-flight or terminal apply state.
type appliesByStateStorage struct {
	emptyStorage
	applies []*storage.Apply
	err     error
}

func (s *appliesByStateStorage) Applies() storage.ApplyStore {
	return &appliesByStateStore{applies: s.applies, err: s.err}
}

type appliesByStateStore struct {
	storage.ApplyStore
	applies []*storage.Apply
	err     error
}

func (s *appliesByStateStore) GetByPR(ctx context.Context, repo string, pr int) ([]*storage.Apply, error) {
	return s.applies, s.err
}

// openPRGitHubServer stages the one GitHub route a backfill guard test needs:
// an open PR whose head the backfill would re-plan.
func openPRGitHubServer(t *testing.T) *ghclient.InstallationClient {
	t.Helper()
	client, mux := setupGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"state": "open",
			"head":  map[string]any{"sha": "abc123", "ref": "feature-branch"},
			"base":  map[string]any{"sha": "def456", "ref": "main"},
			"user":  map[string]any{"login": "testuser"},
		}))
	})
	return ghclient.NewInstallationClient(client, testLogger())
}

// A backfill on a PR with a non-terminal apply is refused: the started apply
// remains authoritative for the PR's check state, and replaying the auto-plan
// flow over it could replace an apply-owned merge block with a fresh passing
// plan.
func TestBackfillPRCheckRunsRefusesNonTerminalApply(t *testing.T) {
	store := &appliesByStateStorage{applies: []*storage.Apply{
		{ApplyIdentifier: "apply-completed", State: state.Apply.Completed, Repository: "octocat/hello-world", PullRequest: 1},
		{ApplyIdentifier: "apply-running", State: state.Apply.Running, Repository: "octocat/hello-world", PullRequest: 1},
	}}
	h := actorAuthStorageTestHandler(actorAuthTestConfig(false), store, openPRGitHubServer(t))

	outcome, err := h.BackfillPRCheckRuns(t.Context(), "octocat/hello-world", 1, 12345)

	require.NoError(t, err)
	assert.Equal(t, "skipped: apply apply-running is running; backfill will not re-plan over a started apply", outcome)
}

// When the apply rows cannot be read, the backfill fails instead of
// proceeding: without them there is no proof the PR is safe to re-plan.
func TestBackfillPRCheckRunsFailsClosedOnApplyLookupError(t *testing.T) {
	store := &appliesByStateStorage{err: errors.New("storage read failed")}
	h := actorAuthStorageTestHandler(actorAuthTestConfig(false), store, openPRGitHubServer(t))

	_, err := h.BackfillPRCheckRuns(t.Context(), "octocat/hello-world", 1, 12345)

	require.ErrorContains(t, err, "look up applies before backfilling Check Runs for octocat/hello-world#1")
	require.ErrorContains(t, err, "storage read failed")
}

// A backfill on an open PR replays the auto-plan flow against the PR's
// current head — for a PR with no managed schema changes, that is the
// passing-aggregate path a real check-creating delivery would have taken.
// Terminal applies do not block it: only a non-terminal apply is
// authoritative for the PR's check state.
func TestBackfillPRCheckRunsReplaysAutoPlanFlow(t *testing.T) {
	client, mux := setupGitHubServer(t)
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"state": "open",
			"head":  map[string]any{"sha": "abc123", "ref": "feature-branch"},
			"base":  map[string]any{"sha": "def456", "ref": "main"},
			"user":  map[string]any{"login": "testuser"},
		}))
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/files", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode([]map[string]string{{
			"filename": "docs/readme.md",
			"status":   "modified",
		}}))
	})
	store := &appliesByStateStorage{applies: []*storage.Apply{
		{ApplyIdentifier: "apply-completed", State: state.Apply.Completed, Repository: "octocat/hello-world", PullRequest: 1},
	}}
	h := actorAuthStorageTestHandler(actorAuthTestConfig(false), store, ghclient.NewInstallationClient(client, testLogger()))

	outcome, err := h.BackfillPRCheckRuns(t.Context(), "octocat/hello-world", 1, 12345)

	require.NoError(t, err)
	assert.Equal(t, "no schema files in PR", outcome)
}
