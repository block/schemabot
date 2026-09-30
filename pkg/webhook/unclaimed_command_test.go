package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/webhook/action"
)

const unclaimedCommandCommentID = 42

// unclaimedStopResult is a stop command carrying an apply identifier no
// deployment stores, arriving unscoped (no -t) so every deployment on the repo
// receives it.
func unclaimedStopResult() CommandResult {
	return CommandResult{
		Action:      action.Stop,
		ApplyID:     "apply_a1b2c3",
		Environment: "staging",
		CommentID:   unclaimedCommandCommentID,
	}
}

// reactionRoutes serves the acknowledgment-reaction endpoints for the command
// comment: reads return whatever claimed reports, and each write is counted.
// Reads and writes are independent so a test can hold the comment unclaimed
// across the leader's own write and observe that write separately.
func reactionRoutes(t *testing.T, mux *http.ServeMux, claimed bool) *atomic.Int64 {
	t.Helper()
	var added atomic.Int64
	mux.HandleFunc("GET /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, _ *http.Request) {
		payload := []map[string]any{}
		if claimed {
			payload = append(payload, map[string]any{"id": 1, "content": commandAcknowledgmentReaction})
		}
		require.NoError(t, json.NewEncoder(w).Encode(payload))
	})
	mux.HandleFunc("POST /repos/octocat/hello-world/issues/comments/42/reactions", func(w http.ResponseWriter, _ *http.Request) {
		added.Add(1)
		w.WriteHeader(http.StatusCreated)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 2, "content": commandAcknowledgmentReaction}))
	})
	return &added
}

// An apply-scoped control command naming an apply no deployment stores would
// otherwise produce nothing at all on a repository several deployments serve:
// each one defers to the owner, and there is no owner. The aggregate leader
// waits out the grace period, reads the acknowledgment reaction to confirm no
// sibling claimed the comment, and answers once — so a mistyped or foreign
// apply identifier gets a reply naming what to do next instead of silence.
func TestUnclaimedControlCommandAnsweredByLeader(t *testing.T) {
	t.Run("leader answers a command nothing claimed", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())
		added := reactionRoutes(t, mux, false)
		h.unclaimedCommandGraceOverride = 10 * time.Millisecond

		h.handleStopCommand("octocat/hello-world", 1, 12345, "hubot", unclaimedStopResult())

		body := requireComment(t, comments, "unclaimed control command reply")
		assert.Contains(t, body, "No Schema Change Matched This Command")
		assert.Contains(t, body, "apply_a1b2c3")
		assert.Contains(t, body, "stop")
		assert.Equal(t, int64(1), added.Load(),
			"the leader marks the comment as it answers, so a redelivery sees the claim and stays quiet")
	})

	t.Run("a command a sibling deployment claimed stays unanswered", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())
		added := reactionRoutes(t, mux, true)
		h.unclaimedCommandGraceOverride = 10 * time.Millisecond

		h.handleStopCommand("octocat/hello-world", 1, 12345, "hubot", unclaimedStopResult())

		requireNoComment(t, comments, "a claimed command is the owning deployment's to answer")
		assert.Equal(t, int64(0), added.Load())
	})

	t.Run("participants leave the answer to the leader", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateParticipantConfig())
		added := reactionRoutes(t, mux, false)
		h.unclaimedCommandGraceOverride = 10 * time.Millisecond

		h.handleStopCommand("octocat/hello-world", 1, 12345, "hubot", unclaimedStopResult())

		requireNoComment(t, comments, "several participants answering is the duplicate noise fan-out exists to avoid")
		assert.Equal(t, int64(0), added.Load())
	})

	t.Run("the grace does not hold a shutdown drain open", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, aggregateLeaderConfig())
		reactionRoutes(t, mux, false)
		grace := 200 * time.Millisecond
		h.unclaimedCommandGraceOverride = grace

		h.handleStopCommand("octocat/hello-world", 1, 12345, "hubot", unclaimedStopResult())

		started := time.Now()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		h.DrainInProcessWebhookWork(ctx)
		assert.Less(t, time.Since(started), grace,
			"the grace is a scheduled wait, not tracked work, so a deploy drain must not sit through it")

		requireComment(t, comments, "unclaimed control command reply")
	})

	t.Run("a repo one deployment serves answers directly", func(t *testing.T) {
		h, mux, comments := newFanOutSkipHandler(t, nonAggregateConfig())
		added := reactionRoutes(t, mux, false)
		h.unclaimedCommandGraceOverride = 10 * time.Millisecond

		h.handleStopCommand("octocat/hello-world", 1, 12345, "hubot", unclaimedStopResult())

		body := requireComment(t, comments, "apply-not-found control comment")
		assert.Contains(t, body, "Apply not found")
		assert.Equal(t, int64(0), added.Load(),
			"the sole addressee answers on the spot, with no reaction round trip")
	})
}

// requireNoComment fails the test if any PR comment arrives, allowing enough
// time for an answering goroutine that should not have started to post one.
func requireNoComment(t *testing.T, comments <-chan string, failureContext string) {
	t.Helper()
	select {
	case body := <-comments:
		require.FailNow(t, "unexpected comment: "+body, failureContext)
	case <-time.After(300 * time.Millisecond):
	}
}
