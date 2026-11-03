package storagetest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestApplyComments runs the behavioral parity suite for
// storage.ApplyCommentStore: the tracked-comment slot per
// (apply_id, comment_state), the supersede/reactivate lifecycle, the
// pending-freeze marker, the exactly-once summary-claim machinery, and the
// apply-lease guard on every lease-guarded mutation.
//
// Claim behaviors that can only be proven against an aged row stay in the
// dialect suites, which can backdate timestamps directly: the stale-window
// takeover in ReclaimStaleSummaryClaim, its refusal to reclaim a marker
// recording a posted comment, the assertion that every mutation renews
// updated_at (the claim machinery's freshness signal), and the stale-heartbeat
// takeover in ClaimProgressCommentAuthority. The parity suite covers the
// claim decisions that do not require aging a row.
func TestApplyComments(t *testing.T, h Harness) {
	// Upsert_And_Get verifies the tracked-comment round trip: an insert
	// stores the posted control phase, a conflicting upsert for the same
	// (apply_id, comment_state) slot rotates the recorded GitHub comment in
	// place, and a summary comment — which carries no phase — reads back with
	// nil pointers.
	t.Run("Upsert_And_Get", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "comment_upsert_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_comment_upsert", 700)

		noPhase := ""
		comment := &storage.ApplyComment{
			ApplyID:         apply.ID,
			CommentState:    state.Comment.Progress,
			GitHubCommentID: 111222333,
			PostedPhase:     &noPhase,
		}
		require.NoError(t, store.ApplyComments().Upsert(ctx, comment))

		retrieved, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		assert.Equal(t, apply.ID, retrieved.ApplyID)
		assert.Equal(t, state.Comment.Progress, retrieved.CommentState)
		assert.Equal(t, int64(111222333), retrieved.GitHubCommentID)
		require.NotNil(t, retrieved.PostedPhase)
		assert.Empty(t, *retrieved.PostedPhase)
		assert.Nil(t, retrieved.PostedCutoverOperationID)
		assert.NotZero(t, retrieved.ID)
		assert.NotZero(t, retrieved.CreatedAt)
		assert.NotZero(t, retrieved.UpdatedAt)

		// A rotation to a fresh comment upserts the same slot with the new
		// comment ID, control phase, and the rollout member whose cutover it
		// tracks.
		comment.GitHubCommentID = 444555666
		reverting := state.Apply.Reverting
		comment.PostedPhase = &reverting
		cutoverOperationID := int64(4242)
		comment.PostedCutoverOperationID = &cutoverOperationID
		require.NoError(t, store.ApplyComments().Upsert(ctx, comment))

		retrieved, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		assert.Equal(t, int64(444555666), retrieved.GitHubCommentID)
		require.NotNil(t, retrieved.PostedPhase)
		assert.Equal(t, state.Apply.Reverting, *retrieved.PostedPhase)
		require.NotNil(t, retrieved.PostedCutoverOperationID)
		assert.Equal(t, int64(4242), *retrieved.PostedCutoverOperationID)

		// A summary comment carries no control phase; the column stays NULL
		// and reads back nil.
		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID:         apply.ID,
			CommentState:    state.Comment.Summary,
			GitHubCommentID: 777888999,
		}))
		retrieved, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		assert.Nil(t, retrieved.PostedPhase)
		assert.Nil(t, retrieved.PostedCutoverOperationID)

		// A missing slot reads back nil, not an error.
		missing, err := store.ApplyComments().Get(ctx, apply.ID+1000, state.Comment.Progress)
		require.NoError(t, err)
		assert.Nil(t, missing)
	})

	// ListByApply verifies the per-apply read: an apply with no comments
	// returns an empty result, all tracked states come back ordered by id
	// ascending, and comments belonging to another apply are excluded.
	t.Run("ListByApply", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "comment_list_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_comment_list", 701)
		other := CreateApplyWithStateAndEnv(t, store, lock, "apply_comment_list_other", 702, state.Apply.Completed, "production")

		comments, err := store.ApplyComments().ListByApply(ctx, apply.ID)
		require.NoError(t, err)
		require.Empty(t, comments)

		for _, c := range []struct {
			commentState string
			githubID     int64
		}{
			{state.Comment.Progress, 100},
			{state.Comment.Cutover, 200},
			{state.Comment.Summary, 300},
		} {
			require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
				ApplyID: apply.ID, CommentState: c.commentState, GitHubCommentID: c.githubID,
			}))
		}
		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: other.ID, CommentState: state.Comment.Progress, GitHubCommentID: 900,
		}))

		comments, err = store.ApplyComments().ListByApply(ctx, apply.ID)
		require.NoError(t, err)
		require.Len(t, comments, 3)
		states := make([]string, len(comments))
		for i, c := range comments {
			states[i] = c.CommentState
		}
		assert.Equal(t, []string{state.Comment.Progress, state.Comment.Cutover, state.Comment.Summary}, states)
	})

	// UniqueSlot verifies each (apply_id, comment_state) pair is a single
	// tracked slot: upserting the same slot again updates the recorded GitHub
	// comment instead of accumulating rows.
	t.Run("UniqueSlot", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "comment_unique_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_comment_unique", 703)

		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: apply.ID, CommentState: state.Comment.Progress, GitHubCommentID: 100,
		}))
		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 200,
		}))
		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: apply.ID, CommentState: state.Comment.Progress, GitHubCommentID: 999,
		}))

		comments, err := store.ApplyComments().ListByApply(ctx, apply.ID)
		require.NoError(t, err)
		require.Len(t, comments, 2)

		progress, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
		require.NoError(t, err)
		require.NotNil(t, progress)
		assert.Equal(t, int64(999), progress.GitHubCommentID)
	})

	// DeleteByApply verifies the per-apply cleanup: all of one apply's
	// comment records are removed, another apply's records survive, and
	// deleting for an apply with no records is a no-op, not an error.
	// DeleteByApply is deliberately lease-agnostic — it serves teardown of
	// the apply itself — so no lease fixture is involved here.
	t.Run("DeleteByApply", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "comment_delete_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_comment_delete", 704)
		other := CreateApplyWithStateAndEnv(t, store, lock, "apply_comment_delete_other", 705, state.Apply.Completed, "production")

		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: apply.ID, CommentState: state.Comment.Progress, GitHubCommentID: 100,
		}))
		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 101,
		}))
		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: other.ID, CommentState: state.Comment.Progress, GitHubCommentID: 200,
		}))

		require.NoError(t, store.ApplyComments().DeleteByApply(ctx, apply.ID))

		comments, err := store.ApplyComments().ListByApply(ctx, apply.ID)
		require.NoError(t, err)
		require.Empty(t, comments)

		comments, err = store.ApplyComments().ListByApply(ctx, other.ID)
		require.NoError(t, err)
		require.Len(t, comments, 1)

		require.NoError(t, store.ApplyComments().DeleteByApply(ctx, apply.ID+1000))
	})

	// Supersede verifies the retire-in-place lifecycle: the superseded row
	// and its GitHub comment ID are kept but stamped superseded_at, sibling
	// states are untouched, a later upsert for the same state reactivates the
	// row, and superseding a missing or already-superseded state is a no-op.
	t.Run("Supersede", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "comment_supersede_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_comment_supersede", 706)

		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: apply.ID, CommentState: state.Comment.Progress, GitHubCommentID: 100,
		}))
		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 101,
		}))

		require.NoError(t, store.ApplyComments().Supersede(ctx, apply.ID, state.Comment.Summary))

		summary, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
		require.NoError(t, err)
		require.NotNil(t, summary, "the superseded row is kept, not deleted")
		assert.NotNil(t, summary.SupersededAt, "the row is marked superseded")
		assert.Equal(t, int64(101), summary.GitHubCommentID, "the GitHub comment id is preserved")

		progress, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
		require.NoError(t, err)
		require.NotNil(t, progress)
		assert.Nil(t, progress.SupersededAt)
		assert.Equal(t, int64(100), progress.GitHubCommentID)

		// Re-posting a summary (e.g. on a later stop) reactivates the row.
		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 102,
		}))
		summary, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
		require.NoError(t, err)
		require.NotNil(t, summary)
		assert.Nil(t, summary.SupersededAt, "a re-posted summary is active again")
		assert.Equal(t, int64(102), summary.GitHubCommentID)

		// Superseding a missing or already-superseded state is a no-op.
		require.NoError(t, store.ApplyComments().Supersede(ctx, apply.ID, state.Comment.Cutover))
		require.NoError(t, store.ApplyComments().Supersede(ctx, apply.ID+1000, state.Comment.Progress))
	})

	// PendingFreeze verifies the rotation-freeze marker: a rotation records
	// the freeze owed to the superseded comment in the same write that tracks
	// its successor, ClearPendingFreeze removes only the marker once the
	// frozen rendering lands, clearing an already-clear marker or a missing
	// row is a no-op, and a post that supersedes nothing leaves the marker
	// unset.
	t.Run("PendingFreeze", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "comment_freeze_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_comment_freeze", 707)

		postedPhase := state.Apply.Reverting
		supersededID := int64(100)
		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID:                apply.ID,
			CommentState:           state.Comment.Progress,
			GitHubCommentID:        200,
			PostedPhase:            &postedPhase,
			PendingFreezeCommentID: &supersededID,
		}))

		retrieved, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		assert.Equal(t, int64(200), retrieved.GitHubCommentID)
		require.NotNil(t, retrieved.PendingFreezeCommentID)
		assert.Equal(t, supersededID, *retrieved.PendingFreezeCommentID)

		require.NoError(t, store.ApplyComments().ClearPendingFreeze(ctx, apply.ID, state.Comment.Progress))
		retrieved, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		assert.Nil(t, retrieved.PendingFreezeCommentID)
		assert.Equal(t, int64(200), retrieved.GitHubCommentID)
		require.NotNil(t, retrieved.PostedPhase)
		assert.Equal(t, state.Apply.Reverting, *retrieved.PostedPhase)

		require.NoError(t, store.ApplyComments().ClearPendingFreeze(ctx, apply.ID, state.Comment.Progress))
		require.NoError(t, store.ApplyComments().ClearPendingFreeze(ctx, apply.ID+1000, state.Comment.Progress))

		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 300,
		}))
		summary, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
		require.NoError(t, err)
		require.NotNil(t, summary)
		assert.Nil(t, summary.PendingFreezeCommentID)
	})

	// ClaimSummaryComment verifies the atomic summary-marker claim is
	// first-writer-wins: the first claim inserts the sentinel
	// (github_comment_id = 0) and wins, every later claim for the same apply
	// loses, and a summary marker that already records a real comment also
	// blocks the claim. A superseded marker — a stop's summary consumed by a
	// resume rotation — does not block: it converts back into a claim
	// sentinel exactly once, so the apply's next terminal state still gets
	// its summary. A superseded claim sentinel is never reclaimable through
	// the claim itself. Claims for different applies are independent.
	t.Run("ClaimSummaryComment", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "comment_claim_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_comment_claim", 708)
		otherLock := CreateLock(t, store, "comment_claim_other_db", storage.DatabaseTypeMySQL)
		other := CreateApply(t, store, otherLock, "apply_comment_claim_other", 709)

		won, err := store.ApplyComments().ClaimSummaryComment(ctx, apply.ID, state.Apply.Stopped)
		require.NoError(t, err)
		assert.True(t, won, "first claim must win")

		claimed, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
		require.NoError(t, err)
		require.NotNil(t, claimed)
		assert.Equal(t, int64(0), claimed.GitHubCommentID, "won claim is the sentinel form of the marker")

		won, err = store.ApplyComments().ClaimSummaryComment(ctx, apply.ID, state.Apply.Stopped)
		require.NoError(t, err)
		assert.False(t, won, "second claim for the same apply must lose")

		won, err = store.ApplyComments().ClaimSummaryComment(ctx, other.ID, state.Apply.Stopped)
		require.NoError(t, err)
		assert.True(t, won, "claims for different applies are independent")

		// A recorded real comment blocks the claim the same way a sentinel
		// does.
		require.NoError(t, store.ApplyComments().ReleaseSummaryClaim(ctx, apply.ID))
		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 9001,
		}))
		won, err = store.ApplyComments().ClaimSummaryComment(ctx, apply.ID, state.Apply.Stopped)
		require.NoError(t, err)
		assert.False(t, won, "a posted summary must block the claim")

		// A superseded summary — a stop's summary marker consumed by a resume
		// rotation — no longer describes the current terminal state, so it
		// must not block the claim: the row converts back into a claim
		// sentinel.
		require.NoError(t, store.ApplyComments().Supersede(ctx, apply.ID, state.Comment.Summary))
		won, err = store.ApplyComments().ClaimSummaryComment(ctx, apply.ID, state.Apply.Stopped)
		require.NoError(t, err)
		assert.True(t, won, "a superseded summary marker must be reclaimable")

		claimed, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
		require.NoError(t, err)
		require.NotNil(t, claimed)
		assert.Equal(t, int64(0), claimed.GitHubCommentID, "reclaimed marker is the sentinel form")
		assert.Nil(t, claimed.SupersededAt, "reclaimed marker is active again")

		won, err = store.ApplyComments().ClaimSummaryComment(ctx, apply.ID, state.Apply.Stopped)
		require.NoError(t, err)
		assert.False(t, won, "exactly one claimant reclaims a superseded summary")

		// A superseded claim sentinel belongs to a publish that is still in
		// flight or crashed mid-publish — converting it back into an active
		// sentinel would hand two writers the same claim. It is recovered by
		// the stale-claim machinery (ReclaimStaleSummaryClaim), never by a
		// competing claim.
		require.NoError(t, store.ApplyComments().Supersede(ctx, apply.ID, state.Comment.Summary))
		won, err = store.ApplyComments().ClaimSummaryComment(ctx, apply.ID, state.Apply.Stopped)
		require.NoError(t, err)
		assert.False(t, won, "a superseded claim sentinel is not reclaimable")
	})

	// ClaimSummaryCommentForAnotherTerminalState verifies a posted summary
	// marker blocks only claims for the terminal state it describes. A
	// stopped apply that is started and completes before a resume rotation
	// consumes the stop's summary is owed the completion summary: the
	// completion claim converts the stop's marker back into a claim sentinel
	// exactly once. A claim for the state the marker already describes loses,
	// a marker for a settled state blocks every claim because a settled
	// outcome is final, and a marker recorded without its state blocks every
	// claim, as it did before the state was tracked.
	t.Run("ClaimSummaryCommentForAnotherTerminalState", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)
		stopped := state.Apply.Stopped

		lock := CreateLock(t, store, "comment_claim_state_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_comment_claim_state", 715)

		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 9101, SummarizedState: &stopped,
		}))
		posted, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
		require.NoError(t, err)
		require.NotNil(t, posted)
		require.NotNil(t, posted.SummarizedState, "Upsert must record the summarized state")
		assert.Equal(t, state.Apply.Stopped, *posted.SummarizedState)

		won, err := store.ApplyComments().ClaimSummaryComment(ctx, apply.ID, state.Apply.Stopped)
		require.NoError(t, err)
		assert.False(t, won, "a summary of the same terminal state must block the claim")

		won, err = store.ApplyComments().ClaimSummaryComment(ctx, apply.ID, state.Apply.Completed)
		require.NoError(t, err)
		assert.True(t, won, "a summary of an earlier terminal state must not block the claim")

		claimed, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
		require.NoError(t, err)
		require.NotNil(t, claimed)
		assert.Equal(t, int64(0), claimed.GitHubCommentID, "reclaimed marker is the sentinel form")
		assert.Nil(t, claimed.SummarizedState, "a claim sentinel describes no posted summary")
		assert.Nil(t, claimed.SupersededAt)

		won, err = store.ApplyComments().ClaimSummaryComment(ctx, apply.ID, state.Apply.Completed)
		require.NoError(t, err)
		assert.False(t, won, "exactly one claimant reclaims a summary of an earlier terminal state")

		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 9102,
		}))
		won, err = store.ApplyComments().ClaimSummaryComment(ctx, apply.ID, state.Apply.Completed)
		require.NoError(t, err)
		assert.False(t, won, "a summary recorded without its state must block the claim")

		for i, settled := range state.SettledApplyStates {
			require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
				ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: int64(9110 + i), SummarizedState: &settled,
			}))
			won, err = store.ApplyComments().ClaimSummaryComment(ctx, apply.ID, state.Apply.Stopped)
			require.NoError(t, err)
			assert.False(t, won, "a %s summary must block a claim from a stale stopped read, since a settled outcome is final", settled)
		}
	})

	// SupersedeSummaryForResume verifies a resume rotation consumes only the
	// summary it rotated for, and only while the apply is active: a marker
	// that records another comment or has become a claim sentinel belongs to a
	// later terminal publish, and an apply that is terminal again keeps the
	// marker for its next resume. The rotation marks that marker rotated past
	// instead, which keeps it active but lets the terminal state's own summary
	// claim it. Both writes are fenced on the drive's lease.
	t.Run("SupersedeSummaryForResume", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "comment_resume_supersede_db", storage.DatabaseTypeMySQL)
		apply := CreateClaimedApply(t, store, lock, "apply_comment_resume_supersede", 716, "current-driver")
		ownedCtx := storage.WithApplyLease(ctx, storage.ApplyLease{
			ApplyID: apply.ID, Owner: apply.LeaseOwner, Token: apply.LeaseToken,
		})
		staleCtx := storage.WithApplyLease(ctx, storage.ApplyLease{
			ApplyID: apply.ID, Owner: "old-driver", Token: "stale-token",
		})

		_, err := store.ApplyComments().SupersedeSummaryForResume(ctx, apply.ID, 0)
		require.Error(t, err, "a claim sentinel is never consumed by a resume")

		consumed, err := store.ApplyComments().SupersedeSummaryForResume(ctx, apply.ID, 9201)
		require.NoError(t, err)
		assert.False(t, consumed, "no summary marker means nothing to consume")

		// A claim sentinel belongs to a terminal publish in flight.
		won, err := store.ApplyComments().ClaimSummaryComment(ctx, apply.ID, state.Apply.Stopped)
		require.NoError(t, err)
		require.True(t, won)
		consumed, err = store.ApplyComments().SupersedeSummaryForResume(ctx, apply.ID, 9201)
		require.NoError(t, err)
		assert.False(t, consumed, "a claim sentinel must be left to its publisher")

		// A marker recording another comment is a later terminal summary.
		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 9202,
		}))
		consumed, err = store.ApplyComments().SupersedeSummaryForResume(ctx, apply.ID, 9201)
		require.NoError(t, err)
		assert.False(t, consumed, "a marker recording another summary must be left in place")

		summary, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
		require.NoError(t, err)
		require.NotNil(t, summary)
		assert.Nil(t, summary.SupersededAt)

		_, err = store.ApplyComments().SupersedeSummaryForResume(staleCtx, apply.ID, 9202)
		require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
		summary, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
		require.NoError(t, err)
		require.NotNil(t, summary)
		assert.Nil(t, summary.SupersededAt, "a stale lease must not consume the summary")

		consumed, err = store.ApplyComments().SupersedeSummaryForResume(ownedCtx, apply.ID, 9202)
		require.NoError(t, err)
		assert.True(t, consumed, "the rotated-for summary of an active apply is consumed")
		summary, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
		require.NoError(t, err)
		require.NotNil(t, summary)
		assert.NotNil(t, summary.SupersededAt)

		consumed, err = store.ApplyComments().SupersedeSummaryForResume(ownedCtx, apply.ID, 9202)
		require.NoError(t, err)
		assert.False(t, consumed, "an already consumed summary is not consumed again")

		// An apply that is terminal again keeps its summary marker.
		terminalLock := CreateLock(t, store, "comment_resume_terminal_db", storage.DatabaseTypeMySQL)
		terminal := CreateApplyWithStateAndEnv(t, store, terminalLock, "apply_comment_resume_terminal", 717, state.Apply.Stopped, "staging")
		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: terminal.ID, CommentState: state.Comment.Summary, GitHubCommentID: 9203,
		}))
		consumed, err = store.ApplyComments().SupersedeSummaryForResume(ctx, terminal.ID, 9203)
		require.NoError(t, err)
		assert.False(t, consumed, "a terminal apply's summary must be left in place")
		summary, err = store.ApplyComments().Get(ctx, terminal.ID, state.Comment.Summary)
		require.NoError(t, err)
		require.NotNil(t, summary)
		assert.Nil(t, summary.SupersededAt)

		// Marking rotated past applies only to a terminal apply's marker that
		// still records the rotated-for summary.
		_, err = store.ApplyComments().MarkSummaryRotatedPast(ctx, terminal.ID, 0)
		require.Error(t, err, "a claim sentinel is never the rotation's marker")
		marked, err := store.ApplyComments().MarkSummaryRotatedPast(ctx, apply.ID, 9202)
		require.NoError(t, err)
		assert.False(t, marked, "a consumed marker is not marked rotated past")
		marked, err = store.ApplyComments().MarkSummaryRotatedPast(ctx, terminal.ID, 9299)
		require.NoError(t, err)
		assert.False(t, marked, "a marker recording another summary is not marked rotated past")

		marked, err = store.ApplyComments().MarkSummaryRotatedPast(ctx, terminal.ID, 9203)
		require.NoError(t, err)
		assert.True(t, marked)
		summary, err = store.ApplyComments().Get(ctx, terminal.ID, state.Comment.Summary)
		require.NoError(t, err)
		require.NotNil(t, summary)
		assert.Nil(t, summary.SupersededAt, "a marker rotated past stays active for the next resume")
		assert.Equal(t, int64(9203), summary.GitHubCommentID)
		require.NotNil(t, summary.SummarizedState)
		assert.Equal(t, storage.SummaryRotatedPast, *summary.SummarizedState)

		won, err = store.ApplyComments().ClaimSummaryComment(ctx, terminal.ID, state.Apply.Stopped)
		require.NoError(t, err)
		assert.True(t, won, "a marker rotated past must not block the summary of the state the apply reached")

		// Marking an active apply's marker is refused: its rotation consumes it.
		activeLock := CreateLock(t, store, "comment_resume_active_db", storage.DatabaseTypeMySQL)
		active := CreateApplyWithStateAndEnv(t, store, activeLock, "apply_comment_resume_active", 718, state.Apply.Running, "staging")
		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: active.ID, CommentState: state.Comment.Summary, GitHubCommentID: 9204,
		}))
		marked, err = store.ApplyComments().MarkSummaryRotatedPast(ctx, active.ID, 9204)
		require.NoError(t, err)
		assert.False(t, marked, "an active apply's marker is consumed, never marked rotated past")
	})

	// ClaimProgressCommentAuthority verifies the durable progress-comment edit
	// authority's fresh-row decisions on every dialect: an apply with no
	// tracked progress comment row has nothing to claim, the first claim on an
	// unowned row wins and records its owner, the holder renews its own
	// authority (on MySQL an identical-value renewal reports zero changed rows
	// and must still be recognized as held), a second owner loses while the
	// holder's heartbeat is fresh, and claims for different applies are
	// independent. The stale-heartbeat takeover needs an aged row and lives in
	// the dialect suites.
	t.Run("ClaimProgressCommentAuthority", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "comment_authority_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_comment_authority", 713)
		otherLock := CreateLock(t, store, "comment_authority_other_db", storage.DatabaseTypeMySQL)
		other := CreateApply(t, store, otherLock, "apply_comment_authority_other", 714)

		held, err := store.ApplyComments().ClaimProgressCommentAuthority(ctx, apply.ID, "pod-a/1/comment-observer")
		require.NoError(t, err)
		assert.False(t, held, "no tracked progress comment row means nothing to claim")

		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: apply.ID, CommentState: state.Comment.Progress, GitHubCommentID: 555,
		}))
		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: other.ID, CommentState: state.Comment.Progress, GitHubCommentID: 556,
		}))

		held, err = store.ApplyComments().ClaimProgressCommentAuthority(ctx, apply.ID, "pod-a/1/comment-observer")
		require.NoError(t, err)
		assert.True(t, held, "first claim on an unowned progress comment must win")

		held, err = store.ApplyComments().ClaimProgressCommentAuthority(ctx, apply.ID, "pod-a/1/comment-observer")
		require.NoError(t, err)
		assert.True(t, held, "the holder renews its own authority, including an identical-value renewal")

		held, err = store.ApplyComments().ClaimProgressCommentAuthority(ctx, apply.ID, "pod-b/2/comment-observer")
		require.NoError(t, err)
		assert.False(t, held, "a second owner must lose while the holder's heartbeat is fresh")

		held, err = store.ApplyComments().ClaimProgressCommentAuthority(ctx, other.ID, "pod-b/2/comment-observer")
		require.NoError(t, err)
		assert.True(t, held, "authorities for different applies are independent")
	})

	// ReclaimStaleSummaryClaim_RequiresStaleSentinel verifies the reclaim
	// refusals that do not depend on aging a row: a missing marker is not
	// reclaimable, and a fresh sentinel is an in-flight publish and stays
	// with its holder. The stale-window takeover and the refusal to reclaim
	// a marker recording a real posted comment can only be proven against an
	// aged row, so both require backdating updated_at and are exercised by
	// the dialect suites.
	t.Run("ReclaimStaleSummaryClaim_RequiresStaleSentinel", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "comment_reclaim_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_comment_reclaim", 710)

		reclaimed, err := store.ApplyComments().ReclaimStaleSummaryClaim(ctx, apply.ID)
		require.NoError(t, err)
		assert.False(t, reclaimed, "missing marker is not reclaimable")

		won, err := store.ApplyComments().ClaimSummaryComment(ctx, apply.ID, state.Apply.Stopped)
		require.NoError(t, err)
		require.True(t, won)

		reclaimed, err = store.ApplyComments().ReclaimStaleSummaryClaim(ctx, apply.ID)
		require.NoError(t, err)
		assert.False(t, reclaimed, "fresh sentinel is an in-flight publish, not reclaimable")
	})

	// ReleaseSummaryClaim verifies release deletes only the sentinel form of
	// the summary marker: a released claim can be re-won, a missing marker
	// releases without error, and a marker recording a real posted comment
	// survives release untouched.
	t.Run("ReleaseSummaryClaim", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "comment_release_db", storage.DatabaseTypeMySQL)
		apply := CreateApply(t, store, lock, "apply_comment_release", 711)

		require.NoError(t, store.ApplyComments().ReleaseSummaryClaim(ctx, apply.ID), "releasing a missing claim is not an error")

		won, err := store.ApplyComments().ClaimSummaryComment(ctx, apply.ID, state.Apply.Stopped)
		require.NoError(t, err)
		require.True(t, won)
		require.NoError(t, store.ApplyComments().ReleaseSummaryClaim(ctx, apply.ID))

		won, err = store.ApplyComments().ClaimSummaryComment(ctx, apply.ID, state.Apply.Stopped)
		require.NoError(t, err)
		assert.True(t, won, "a released claim must be re-winnable")

		// Convert the claim to a posted summary; release must not delete it.
		require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
			ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 9001,
		}))
		require.NoError(t, store.ApplyComments().ReleaseSummaryClaim(ctx, apply.ID))
		posted, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Summary)
		require.NoError(t, err)
		require.NotNil(t, posted, "a recorded posted summary must survive release")
		assert.Equal(t, int64(9001), posted.GitHubCommentID)
	})

	// LeaseGuardsWrites verifies every lease-guarded comment mutation —
	// Upsert, IncrementEditCount, ClearPendingFreeze, and Supersede: a caller
	// holding a stale lease fails closed with ErrApplyLeaseLost and leaves
	// the row untouched, the current lease holder's writes land, and a caller
	// with no lease in context writes unguarded. The lease is established
	// through the driver claim path, so the guard is exercised exactly as a
	// drive exercises it.
	t.Run("LeaseGuardsWrites", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "comment_lease_db", storage.DatabaseTypeMySQL)
		apply := CreateClaimedApply(t, store, lock, "apply_comment_lease", 712, "current-driver")

		ownedCtx := storage.WithApplyLease(ctx, storage.ApplyLease{
			ApplyID: apply.ID, Owner: apply.LeaseOwner, Token: apply.LeaseToken,
		})
		staleCtx := storage.WithApplyLease(ctx, storage.ApplyLease{
			ApplyID: apply.ID, Owner: "old-driver", Token: "stale-token",
		})

		comment := &storage.ApplyComment{
			ApplyID:         apply.ID,
			CommentState:    state.Comment.Progress,
			GitHubCommentID: 100,
		}
		require.ErrorIs(t, store.ApplyComments().Upsert(staleCtx, comment), storage.ErrApplyLeaseLost)

		missing, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
		require.NoError(t, err)
		assert.Nil(t, missing, "a stale lease must not create the comment record")

		require.NoError(t, store.ApplyComments().Upsert(ownedCtx, comment))
		require.ErrorIs(t, store.ApplyComments().IncrementEditCount(staleCtx, apply.ID, state.Comment.Progress), storage.ErrApplyLeaseLost)

		retrieved, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		assert.Equal(t, int64(100), retrieved.GitHubCommentID)
		assert.Equal(t, 0, retrieved.EditCount)

		require.NoError(t, store.ApplyComments().IncrementEditCount(ownedCtx, apply.ID, state.Comment.Progress))
		retrieved, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		assert.Equal(t, 1, retrieved.EditCount)
		require.NotNil(t, retrieved.LastEditedAt, "IncrementEditCount must stamp last_edited_at")

		// A caller with no lease in context takes the unguarded path: the
		// increment lands without a lease fence.
		require.NoError(t, store.ApplyComments().IncrementEditCount(ctx, apply.ID, state.Comment.Progress))
		retrieved, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		assert.Equal(t, 2, retrieved.EditCount)

		// ClearPendingFreeze is a lease-guarded write like the others: a
		// stale lease fails closed, the current lease clears the marker.
		pendingFreezeID := int64(50)
		comment.PendingFreezeCommentID = &pendingFreezeID
		require.NoError(t, store.ApplyComments().Upsert(ownedCtx, comment))
		require.ErrorIs(t, store.ApplyComments().ClearPendingFreeze(staleCtx, apply.ID, state.Comment.Progress), storage.ErrApplyLeaseLost)

		retrieved, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		require.NotNil(t, retrieved.PendingFreezeCommentID, "a stale lease must not clear the marker")

		require.NoError(t, store.ApplyComments().ClearPendingFreeze(ownedCtx, apply.ID, state.Comment.Progress))
		retrieved, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		assert.Nil(t, retrieved.PendingFreezeCommentID)

		// Supersede is lease-guarded like the other mutations: a stale lease
		// cannot retire the tracked comment, the current lease holder's
		// supersede lands.
		require.ErrorIs(t, store.ApplyComments().Supersede(staleCtx, apply.ID, state.Comment.Progress), storage.ErrApplyLeaseLost)

		retrieved, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		assert.Nil(t, retrieved.SupersededAt, "a stale lease must not retire the comment")

		require.NoError(t, store.ApplyComments().Supersede(ownedCtx, apply.ID, state.Comment.Progress))
		retrieved, err = store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		assert.NotNil(t, retrieved.SupersededAt)
	})

	t.Run("Upsert_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		err := store.ApplyComments().Upsert(t.Context(), &storage.ApplyComment{
			ApplyID: 1, CommentState: state.Comment.Progress, GitHubCommentID: 100,
		})
		require.Error(t, err)
	})

	t.Run("Get_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.ApplyComments().Get(t.Context(), 1, state.Comment.Progress)
		require.Error(t, err)
	})

	t.Run("ListByApply_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.ApplyComments().ListByApply(t.Context(), 1)
		require.Error(t, err)
	})

	t.Run("IncrementEditCount_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		require.Error(t, store.ApplyComments().IncrementEditCount(t.Context(), 1, state.Comment.Progress))
	})

	t.Run("DeleteByApply_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		require.Error(t, store.ApplyComments().DeleteByApply(t.Context(), 1))
	})

	t.Run("Supersede_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		require.Error(t, store.ApplyComments().Supersede(t.Context(), 1, state.Comment.Progress))
	})

	t.Run("ClearPendingFreeze_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		require.Error(t, store.ApplyComments().ClearPendingFreeze(t.Context(), 1, state.Comment.Progress))
	})

	t.Run("ClaimSummaryComment_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.ApplyComments().ClaimSummaryComment(t.Context(), 1, state.Apply.Stopped)
		require.Error(t, err)
	})

	t.Run("ReclaimStaleSummaryClaim_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.ApplyComments().ReclaimStaleSummaryClaim(t.Context(), 1)
		require.Error(t, err)
	})

	t.Run("ClaimProgressCommentAuthority_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.ApplyComments().ClaimProgressCommentAuthority(t.Context(), 1, "pod-a/1/comment-observer")
		require.Error(t, err)
	})

	t.Run("ReleaseSummaryClaim_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		require.Error(t, store.ApplyComments().ReleaseSummaryClaim(t.Context(), 1))
	})
}
