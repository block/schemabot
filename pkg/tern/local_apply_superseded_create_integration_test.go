//go:build integration

package tern

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// takeoverStampStorage stands in for storage whose handoff stamp cannot be
// written: the create that carries a takeover fails once, and a separate
// post-create stamp always fails. Everything else is the real store, so what a
// retry leaves behind is what production would.
type takeoverStampStorage struct {
	storage.Storage
	applies *takeoverStampApplies
}

func (s *takeoverStampStorage) Applies() storage.ApplyStore { return s.applies }

type takeoverStampApplies struct {
	storage.ApplyStore
	createFailed bool
}

var errStampWrite = errors.New("simulated stamp write failure")

func (s *takeoverStampApplies) CreateWithTasksAndOperations(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, operations []*storage.ApplyOperation) (int64, error) {
	if len(apply.TakesOverApplyIDs) > 0 && !s.createFailed {
		s.createFailed = true
		return 0, errStampWrite
	}
	return s.ApplyStore.CreateWithTasksAndOperations(ctx, apply, tasks, operations)
}

func (s *takeoverStampApplies) MarkSuperseded(context.Context, int64, string) error {
	return errStampWrite
}

// A stopped apply is resting on the target with its change unfinished, and a new
// dispatch for the same table takes its work over. If the handoff cannot be
// recorded, the dispatch must fail rather than start a successor beside a stopped
// apply that a later start would replay: the successor is not created, the
// stopped apply stays unmarked, and the same dispatch retried records the handoff
// together with the successor, after which a start on the stopped apply is
// refused.
func TestApplyFailsWhenTheTakeoverCannotBeRecorded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newAdoptTestFixture(t, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})

	first, holder := f.dispatchStopped(t, "schemabot:v1:takeover-stamp-first")

	f.client.storage = &takeoverStampStorage{Storage: f.stor, applies: &takeoverStampApplies{ApplyStore: f.stor.Applies()}}
	req := &ternv1.ApplyRequest{
		PlanId:         f.planID,
		Environment:    localClientTestEnvironment,
		Database:       "testdb",
		Type:           storage.DatabaseTypeMySQL,
		IdempotencyKey: "schemabot:v1:takeover-stamp-second",
	}

	resp, err := f.client.Apply(t.Context(), req)
	require.Error(t, err, "a dispatch whose takeover cannot be recorded must not succeed silently")
	assert.Nil(t, resp)
	assert.ErrorIs(t, err, errStampWrite)

	var applies int
	require.NoError(t, f.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM applies").Scan(&applies))
	assert.Equal(t, 1, applies, "the failed dispatch created no successor beside the stopped apply")
	unmarked, err := f.stor.Applies().Get(t.Context(), holder.ID)
	require.NoError(t, err)
	require.NotNil(t, unmarked)
	assert.Empty(t, unmarked.SupersededBy, "nothing took the stopped apply's work over")
	assert.Empty(t, supersededEvents(t, f.stor, holder.ID),
		"the stopped apply's timeline names no successor while none exists")

	retried, err := f.client.Apply(t.Context(), req)
	require.NoError(t, err)
	require.True(t, retried.Accepted, "the retried dispatch must be accepted: %s", retried.ErrorMessage)
	assert.NotEqual(t, first.ApplyId, retried.ApplyId)

	marked, err := f.stor.Applies().Get(t.Context(), holder.ID)
	require.NoError(t, err)
	require.NotNil(t, marked)
	assert.Equal(t, retried.ApplyId, marked.SupersededBy,
		"the successor's create recorded the handoff, with no separate write left to fail")

	events := supersededEvents(t, f.stor, holder.ID)
	require.Len(t, events, 1, "the stopped apply's timeline records the handoff once")
	assert.Contains(t, events[0], "Superseded by "+retried.ApplyId,
		"the timeline names the apply that took the work over")
}

// A stopped apply whose work an earlier apply already took over is met again by
// a new dispatch for the same table. The earlier successor keeps the marker, the
// new dispatch still goes ahead, and the stopped apply's timeline gains no entry
// claiming the new dispatch took its work over.
func TestTakeoverOfAnAlreadyTakenOverApplyLeavesItsTimelineAlone(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newAdoptTestFixture(t, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})

	_, holder := f.dispatchStopped(t, "schemabot:v1:takeover-earlier-first")
	const earlier = "apply_earlier_successor"
	require.NoError(t, f.stor.Applies().MarkSuperseded(t.Context(), holder.ID, earlier))

	second := f.dispatch(t, "schemabot:v1:takeover-earlier-second")
	require.True(t, second.Accepted, "the new dispatch must be accepted: %s", second.ErrorMessage)

	marked, err := f.stor.Applies().Get(t.Context(), holder.ID)
	require.NoError(t, err)
	require.NotNil(t, marked)
	assert.Equal(t, earlier, marked.SupersededBy, "the first successor keeps the marker")
	assert.Empty(t, supersededEvents(t, f.stor, holder.ID),
		"the timeline must not name a dispatch that did not take the work over")
}

// dispatchStopped dispatches the fixture's plan and rests the resulting apply
// and its tasks in stopped, as an operator's stop leaves them, so a later
// dispatch for the same table takes its work over.
func (f *adoptTestFixture) dispatchStopped(t *testing.T, key string) (*ternv1.ApplyResponse, *storage.Apply) {
	t.Helper()
	resp := f.dispatch(t, key)
	require.True(t, resp.Accepted, "dispatch must be accepted: %s", resp.ErrorMessage)
	apply, err := f.stor.Applies().GetByApplyIdentifier(t.Context(), resp.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	_, err = f.db.ExecContext(t.Context(), "UPDATE applies SET state = ? WHERE id = ?", state.Apply.Stopped, apply.ID)
	require.NoError(t, err)
	_, err = f.db.ExecContext(t.Context(), "UPDATE tasks SET state = ? WHERE apply_id = ?", state.Task.Stopped, apply.ID)
	require.NoError(t, err)
	return resp, apply
}

// supersededEvents is the messages on an apply's timeline that name a successor.
func supersededEvents(t *testing.T, stor storage.Storage, applyID int64) []string {
	t.Helper()
	logs, err := stor.ApplyLogs().GetByApply(t.Context(), applyID)
	require.NoError(t, err)
	var events []string
	for _, l := range logs {
		if strings.HasPrefix(l.Message, "Superseded by ") {
			events = append(events, l.Message)
		}
	}
	return events
}
