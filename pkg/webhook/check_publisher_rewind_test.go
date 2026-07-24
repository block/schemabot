package webhook

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// TestAggregateRewindFromConcludedCreatesFreshCheckRun exercises the merge
// gate when an apply starts on a PR whose aggregate Check Run already
// concluded — the rollback-confirm flow, where the original apply completed
// the aggregate as success before the rollback drive begins. GitHub does not
// move a concluded Check Run back out of "completed" (an update lands the new
// output but keeps the stored conclusion), so reusing the concluded run would
// leave a passing check on the PR while the rollback is still driving. The
// fold must publish the rewind as a fresh in-progress Check Run and store the
// new run's ID so the PR stays blocked until the apply reaches a terminal
// state.
func TestAggregateRewindFromConcludedCreatesFreshCheckRun(t *testing.T) {
	store := &foldCheckStore{
		byPR: []*storage.Check{{
			Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: "abc123",
			Environment: "staging", DatabaseType: "mysql", DatabaseName: "orders",
			ApplyID: 42, HasChanges: true,
			Status: checkStatusInProgress, Conclusion: "",
		}},
		get: &storage.Check{
			Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: "abc123",
			Environment: aggregateSentinel, DatabaseType: aggregateSentinel, DatabaseName: aggregateSentinel,
			CheckRunID: 555,
			Status:     checkStatusCompleted, Conclusion: checkConclusionSuccess,
		},
	}
	h, mux, client := newFoldHandler(t, nonAggregateConfig(), store)
	serveHeadSHA(t, mux, "abc123")

	var created []checkRunCapture
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		created = append(created, body)
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 999}))
	})
	var patched bool
	mux.HandleFunc("PATCH /repos/octocat/hello-world/check-runs/", func(w http.ResponseWriter, _ *http.Request) {
		patched = true
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 555}))
	})

	_, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")
	require.NoError(t, err)

	assert.False(t, patched, "the concluded Check Run must not be reused: GitHub keeps its conclusion, leaving a passing check over a running apply")
	require.Len(t, created, 1, "the rewind must publish exactly one fresh Check Run")
	assert.Equal(t, aggregateCheckName, created[0].Name)
	assert.Equal(t, checkStatusInProgress, created[0].Status)
	assert.Empty(t, created[0].Conclusion)

	require.Len(t, store.upserted, 1, "the fold must store the published aggregate state")
	assert.Equal(t, int64(999), store.upserted[0].CheckRunID, "stored aggregate state must track the fresh Check Run, not the concluded one")
	assert.Equal(t, checkStatusInProgress, store.upserted[0].Status)
	assert.Empty(t, store.upserted[0].Conclusion)
}

// TestAggregateRecomputeOnConcludedRunReusesItWhenStillCompleted pins the
// counterpart: a recompute that stays "completed" (for example a second
// database's result landing after the aggregate already concluded) updates the
// existing Check Run in place rather than creating a duplicate — re-concluding
// an already-concluded run is a transition GitHub allows.
func TestAggregateRecomputeOnConcludedRunReusesItWhenStillCompleted(t *testing.T) {
	store := &foldCheckStore{
		byPR: []*storage.Check{{
			Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: "abc123",
			Environment: "staging", DatabaseType: "mysql", DatabaseName: "orders",
			Status: checkStatusCompleted, Conclusion: checkConclusionSuccess,
		}},
		get: &storage.Check{
			Repository: "octocat/hello-world", PullRequest: 1, HeadSHA: "abc123",
			Environment: aggregateSentinel, DatabaseType: aggregateSentinel, DatabaseName: aggregateSentinel,
			CheckRunID: 555,
			Status:     checkStatusCompleted, Conclusion: checkConclusionActionRequired,
		},
	}
	h, mux, client := newFoldHandler(t, nonAggregateConfig(), store)
	serveHeadSHA(t, mux, "abc123")

	var created bool
	mux.HandleFunc("POST /repos/octocat/hello-world/check-runs", func(w http.ResponseWriter, _ *http.Request) {
		created = true
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 999}))
	})
	var patchedBodies []checkRunCapture
	mux.HandleFunc("PATCH /repos/octocat/hello-world/check-runs/", func(w http.ResponseWriter, r *http.Request) {
		var body checkRunCapture
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		patchedBodies = append(patchedBodies, body)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 555}))
	})

	_, err := h.updateAggregateCheckOnce(t.Context(), client, "octocat/hello-world", 1, "abc123")
	require.NoError(t, err)

	assert.False(t, created, "a completed-to-completed recompute must reuse the existing Check Run")
	require.Len(t, patchedBodies, 1)
	assert.Equal(t, checkStatusCompleted, patchedBodies[0].Status)
	assert.Equal(t, checkConclusionSuccess, patchedBodies[0].Conclusion)

	require.Len(t, store.upserted, 1)
	assert.Equal(t, int64(555), store.upserted[0].CheckRunID)
}
