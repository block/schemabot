//go:build integration

package tern

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// attachStampStorage stands in for storage whose handoff stamp cannot be
// written: the attach that carries a takeover fails once, and a separate
// post-attach stamp always fails. Everything else is the real store, so what a
// retry leaves behind is what production would.
type attachStampStorage struct {
	storage.Storage
	applies *attachStampApplies
}

func (s *attachStampStorage) Applies() storage.ApplyStore { return s.applies }

type attachStampApplies struct {
	storage.ApplyStore
	attachFailed bool
}

func (s *attachStampApplies) AttachOperationWithTasks(ctx context.Context, apply *storage.Apply, operation *storage.ApplyOperation, tasks []*storage.Task) error {
	if len(apply.TakesOverApplyIDs) > 0 && !s.attachFailed {
		s.attachFailed = true
		return errStampWrite
	}
	return s.ApplyStore.AttachOperationWithTasks(ctx, apply, operation, tasks)
}

func (s *attachStampApplies) MarkSuperseded(context.Context, int64, string) error {
	return errStampWrite
}

// A shard dispatch for the keyed apply B finds a stopped apply A resting on the
// same table of the other shard, and attaching its operation to B takes A's work
// over. If the handoff cannot be recorded, the dispatch must fail rather than
// run B's operation beside a stopped apply that a later start would replay: the
// operation is not attached, A stays unmarked, and the same dispatch retried
// records the handoff together with the attached operation, after which a start
// on A is refused.
func TestApplyAttachFailsWhenTheTakeoverCannotBeRecorded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	stor, client, planID := setupAttachDispatchClient(t)
	ctx := t.Context()

	stoppedResp, err := client.Apply(ctx, shardDispatchRequest(planID, "schemabot:v1:attach-stamp-stopped", "80-"))
	require.NoError(t, err)
	require.True(t, stoppedResp.Accepted, "the dispatch that will stop must be accepted: %s", stoppedResp.ErrorMessage)
	holder, err := stor.Applies().GetByApplyIdentifier(ctx, stoppedResp.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, holder)
	holder.State = state.Apply.Stopped
	require.NoError(t, stor.Applies().Update(ctx, holder))
	holderTasks, err := stor.Tasks().GetByApplyID(ctx, holder.ID)
	require.NoError(t, err)
	require.Len(t, holderTasks, 1)
	holderTasks[0].State = state.Task.Stopped
	require.NoError(t, stor.Tasks().Update(ctx, holderTasks[0]))

	const key = "schemabot:v1:attach-stamp-keyed"
	first, err := client.Apply(ctx, shardDispatchRequest(planID, key, "-80"))
	require.NoError(t, err)
	require.True(t, first.Accepted, "the other shard's dispatch does not meet the stopped apply: %s", first.ErrorMessage)
	keyed, err := stor.Applies().GetByApplyIdentifier(ctx, first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, keyed)
	marked, err := stor.Applies().Get(ctx, holder.ID)
	require.NoError(t, err)
	require.NotNil(t, marked)
	require.Empty(t, marked.SupersededBy, "the keyed apply's own create took nothing over")

	client.storage = &attachStampStorage{Storage: stor, applies: &attachStampApplies{ApplyStore: stor.Applies()}}
	sibling := shardDispatchRequest(planID, key, "80-")

	resp, err := client.Apply(ctx, sibling)
	require.Error(t, err, "a dispatch whose takeover cannot be recorded must not succeed silently")
	assert.Nil(t, resp)
	assert.ErrorIs(t, err, errStampWrite)

	ops, err := stor.ApplyOperations().ListByApply(ctx, keyed.ID)
	require.NoError(t, err)
	assert.Len(t, ops, 1, "the failed dispatch attached no operation beside the stopped apply")
	unmarked, err := stor.Applies().Get(ctx, holder.ID)
	require.NoError(t, err)
	require.NotNil(t, unmarked)
	assert.Empty(t, unmarked.SupersededBy, "nothing took the stopped apply's work over")

	retried, err := client.Apply(ctx, sibling)
	require.NoError(t, err)
	require.True(t, retried.Accepted, "the retried dispatch must be accepted: %s", retried.ErrorMessage)
	assert.Equal(t, first.ApplyId, retried.ApplyId, "the sibling attaches to the keyed apply")

	ops, err = stor.ApplyOperations().ListByApply(ctx, keyed.ID)
	require.NoError(t, err)
	assert.Len(t, ops, 2, "the retried dispatch attached its operation")
	stamped, err := stor.Applies().Get(ctx, holder.ID)
	require.NoError(t, err)
	require.NotNil(t, stamped)
	assert.Equal(t, first.ApplyId, stamped.SupersededBy,
		"the attach recorded the handoff, with no separate write left to fail")
}
