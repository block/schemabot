//go:build integration

package sqlstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// The behavioral suite for ApplyCommentStore lives in
// pkg/storage/storagetest and runs against every dialect via parity_test.go.
// The tests here cover only the timestamp-driven claim behaviors that can be
// proven solely against an aged row, which requires backdating updated_at
// with raw SQL — something the storage interface cannot express.

// TestApplyCommentStore_ReclaimStaleSummaryClaim verifies crashed-publisher
// takeover: a claim sentinel older than the stale window transfers to the
// reclaimer (bumping updated_at so a second reclaimer loses), while a fresh
// sentinel, a missing marker, and a recorded real comment are all not
// reclaimable.
func TestApplyCommentStore_ReclaimStaleSummaryClaim(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
	apply := createTestApply(t, store, lock, "apply_comment_reclaim", 1)

	reclaimed, err := store.ApplyComments().ReclaimStaleSummaryClaim(ctx, apply.ID)
	require.NoError(t, err)
	assert.False(t, reclaimed, "missing marker is not reclaimable")

	won, err := store.ApplyComments().ClaimSummaryComment(ctx, apply.ID)
	require.NoError(t, err)
	require.True(t, won)

	reclaimed, err = store.ApplyComments().ReclaimStaleSummaryClaim(ctx, apply.ID)
	require.NoError(t, err)
	assert.False(t, reclaimed, "fresh sentinel is an in-flight publish, not reclaimable")

	// Backdate the sentinel past the stale window to simulate a publisher that
	// crashed between claiming and posting.
	backdateSummaryClaim(t, apply.ID)

	reclaimed, err = store.ApplyComments().ReclaimStaleSummaryClaim(ctx, apply.ID)
	require.NoError(t, err)
	assert.True(t, reclaimed, "stale sentinel transfers to the reclaimer")

	reclaimed, err = store.ApplyComments().ReclaimStaleSummaryClaim(ctx, apply.ID)
	require.NoError(t, err)
	assert.False(t, reclaimed, "a just-reclaimed sentinel is fresh again; a second reclaimer loses")

	// A recorded real comment is never reclaimable, however old.
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 9001,
	}))
	backdateSummaryClaim(t, apply.ID)
	reclaimed, err = store.ApplyComments().ReclaimStaleSummaryClaim(ctx, apply.ID)
	require.NoError(t, err)
	assert.False(t, reclaimed, "a posted summary must never be reclaimed")
}

// TestApplyCommentStore_ReleaseSummaryClaim verifies release deletes only the
// sentinel form of the summary marker: a released claim can be re-won, a
// missing marker releases without error, and a marker recording a real posted
// comment survives release untouched.
func TestApplyCommentStore_ReleaseSummaryClaim(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
	apply := createTestApply(t, store, lock, "apply_comment_release", 1)

	require.NoError(t, store.ApplyComments().ReleaseSummaryClaim(ctx, apply.ID), "releasing a missing claim is not an error")

	won, err := store.ApplyComments().ClaimSummaryComment(ctx, apply.ID)
	require.NoError(t, err)
	require.True(t, won)
	require.NoError(t, store.ApplyComments().ReleaseSummaryClaim(ctx, apply.ID))

	won, err = store.ApplyComments().ClaimSummaryComment(ctx, apply.ID)
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
}

// TestApplyCommentStore_ClaimProgressCommentAuthority verifies the
// crashed-holder handover of the progress-comment authority: a recorded owner
// whose heartbeat is older than the stale window transfers to the next
// claimant, exactly once — the takeover stamps a fresh heartbeat, so a third
// claimant loses again. Aging the heartbeat requires backdating
// observer_heartbeat_at with raw SQL, which the storage interface cannot
// express, so the scenario lives in each dialect suite; the fresh-row claim
// decisions run on both dialects through the parity suite.
func TestApplyCommentStore_ClaimProgressCommentAuthority(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
	apply := createTestApply(t, store, lock, "apply_comment_authority", 1)

	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: apply.ID, CommentState: state.Comment.Progress, GitHubCommentID: 555,
	}))

	held, err := store.ApplyComments().ClaimProgressCommentAuthority(ctx, apply.ID, "pod-a/1/comment-observer")
	require.NoError(t, err)
	require.True(t, held, "first claim on an unowned progress comment must win")

	held, err = store.ApplyComments().ClaimProgressCommentAuthority(ctx, apply.ID, "pod-b/2/comment-observer")
	require.NoError(t, err)
	require.False(t, held, "a second owner must lose while the holder's heartbeat is fresh")

	// A crashed holder hands over only after its heartbeat goes stale, and
	// exactly one successor wins the handover.
	backdateProgressObserverHeartbeat(t, apply.ID)
	held, err = store.ApplyComments().ClaimProgressCommentAuthority(ctx, apply.ID, "pod-b/2/comment-observer")
	require.NoError(t, err)
	assert.True(t, held, "a stale authority transfers to the next claimant")

	held, err = store.ApplyComments().ClaimProgressCommentAuthority(ctx, apply.ID, "pod-c/3/comment-observer")
	require.NoError(t, err)
	assert.False(t, held, "a just-transferred authority is fresh again; a third owner loses")
}

// backdateProgressObserverHeartbeat pushes an apply's progress-comment
// authority heartbeat past the stale window, simulating an observer that
// stopped renewing (crashed pod or cleared observer).
func backdateProgressObserverHeartbeat(t *testing.T, applyID int64) {
	t.Helper()
	_, err := testDB.ExecContext(t.Context(), `
		UPDATE apply_comments SET observer_heartbeat_at = NOW() - INTERVAL ? SECOND
		WHERE apply_id = ? AND comment_state = ?
	`, int64(storage.ProgressCommentAuthorityStaleAfter.Seconds())+1, applyID, state.Comment.Progress)
	require.NoError(t, err)
}

// backdateSummaryClaim pushes an apply's summary marker updated_at past the
// stale-claim window, simulating a publisher that crashed after claiming.
func backdateSummaryClaim(t *testing.T, applyID int64) {
	t.Helper()
	_, err := testDB.ExecContext(t.Context(), `
		UPDATE apply_comments SET updated_at = NOW() - INTERVAL ? SECOND
		WHERE apply_id = ? AND comment_state = ?
	`, int64(storage.SummaryClaimStaleAfter.Seconds())+1, applyID, state.Comment.Summary)
	require.NoError(t, err)
}

// TestApplyCommentStore_MutationsStampUpdatedAt verifies that every comment
// mutation renews the row's updated_at. The summary-claim machinery reads the
// column as its freshness signal, and stamping it is the application's
// responsibility on every dialect. An upsert replay that writes identical
// values still counts as publisher activity and renews the timestamp.
func TestApplyCommentStore_MutationsStampUpdatedAt(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
	apply := createTestApply(t, store, lock, "apply_comment_updated_at_stamp", 1)

	frozen := int64(900)
	comment := &storage.ApplyComment{
		ApplyID:                apply.ID,
		CommentState:           state.Comment.Progress,
		GitHubCommentID:        1001,
		PendingFreezeCommentID: &frozen,
	}
	require.NoError(t, store.ApplyComments().Upsert(ctx, comment))

	backdate := func(t *testing.T) time.Time {
		t.Helper()
		_, err := testDB.ExecContext(t.Context(), `
			UPDATE apply_comments SET updated_at = NOW() - INTERVAL 1 HOUR
			WHERE apply_id = ? AND comment_state = ?
		`, apply.ID, state.Comment.Progress)
		require.NoError(t, err)
		stale, err := store.ApplyComments().Get(t.Context(), apply.ID, state.Comment.Progress)
		require.NoError(t, err)
		require.NotNil(t, stale)
		return stale.UpdatedAt
	}

	// The subtests mutate one shared comment row and depend on this order:
	// "clear pending freeze" only matches because the preceding upserts re-set
	// the freeze marker, and "supersede" must run last because it retires the
	// row from further mutation.
	mutations := []struct {
		name   string
		mutate func(t *testing.T)
	}{
		{"upsert conflict update", func(t *testing.T) {
			comment.GitHubCommentID = 1002
			require.NoError(t, store.ApplyComments().Upsert(ctx, comment))
		}},
		{"identical-value upsert replay", func(t *testing.T) {
			require.NoError(t, store.ApplyComments().Upsert(ctx, comment))
		}},
		{"increment edit count", func(t *testing.T) {
			require.NoError(t, store.ApplyComments().IncrementEditCount(ctx, apply.ID, state.Comment.Progress))
		}},
		{"clear pending freeze", func(t *testing.T) {
			require.NoError(t, store.ApplyComments().ClearPendingFreeze(ctx, apply.ID, state.Comment.Progress))
		}},
		{"supersede", func(t *testing.T) {
			require.NoError(t, store.ApplyComments().Supersede(ctx, apply.ID, state.Comment.Progress))
		}},
	}
	for _, m := range mutations {
		t.Run(m.name, func(t *testing.T) {
			stale := backdate(t)
			m.mutate(t)
			got, err := store.ApplyComments().Get(ctx, apply.ID, state.Comment.Progress)
			require.NoError(t, err)
			require.NotNil(t, got)
			assert.True(t, got.UpdatedAt.After(stale),
				"mutation must renew updated_at (stale=%s got=%s)", stale, got.UpdatedAt)
		})
	}
}

// TestApplyCommentStore_ClaimConversionRestartsStaleWindow verifies that
// converting a superseded posted summary marker back into a claim sentinel
// restarts the stale-claim window. The conversion is a brand-new claim, so
// ReclaimStaleSummaryClaim must not immediately hand the same claim to a
// second publisher.
func TestApplyCommentStore_ClaimConversionRestartsStaleWindow(t *testing.T) {
	clearTables(t)
	ctx := t.Context()
	store := NewMySQL(testDB)

	lock := createTestLock(t, store, "testdb", "mysql")
	apply := createTestApply(t, store, lock, "apply_comment_claim_fresh", 1)

	// A stop's summary was posted and later consumed by a resume rotation,
	// then the row sat idle long past the stale window.
	require.NoError(t, store.ApplyComments().Upsert(ctx, &storage.ApplyComment{
		ApplyID: apply.ID, CommentState: state.Comment.Summary, GitHubCommentID: 9001,
	}))
	require.NoError(t, store.ApplyComments().Supersede(ctx, apply.ID, state.Comment.Summary))
	backdateSummaryClaim(t, apply.ID)

	won, err := store.ApplyComments().ClaimSummaryComment(ctx, apply.ID)
	require.NoError(t, err)
	require.True(t, won, "the superseded posted marker converts back into a claim sentinel")

	reclaimed, err := store.ApplyComments().ReclaimStaleSummaryClaim(ctx, apply.ID)
	require.NoError(t, err)
	assert.False(t, reclaimed, "a just-converted sentinel is a fresh in-flight publish, not reclaimable")
}
