package tern

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// supersedeCall is one recorded handoff write, so a test can assert both which
// apply was marked and which successor it names.
type supersedeCall struct {
	applyID   int64
	successor string
}

// recordingApplyStore records every handoff write and answers each with a
// scripted result, standing in for a store that already carries a marker or
// cannot be written to.
type recordingApplyStore struct {
	storage.ApplyStore
	calls []supersedeCall
	err   error
}

func (s *recordingApplyStore) MarkSuperseded(_ context.Context, applyID int64, successor string) error {
	s.calls = append(s.calls, supersedeCall{applyID: applyID, successor: successor})
	return s.err
}

// supersedeClient builds a client whose handoff writes land in the returned
// store, with a MySQL database so a change that names no namespace normalizes
// to it the way a real dispatch's does. Its logs are captured so a test can
// tell a benign outcome from one an operator has to act on.
func supersedeClient(applies *recordingApplyStore, logs *mockApplyLogStore) (*LocalClient, *bytes.Buffer) {
	captured := &bytes.Buffer{}
	return &LocalClient{
		config: LocalConfig{Database: "testdb", Type: storage.DatabaseTypeMySQL},
		storage: &exactProgressStorage{
			applies: applies,
			logs:    logs,
		},
		logger: slog.New(slog.NewTextHandler(captured, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}, captured
}

func supersedeHolder() supersededHolder {
	return supersededHolder{
		applyID:         7,
		applyIdentifier: "apply-stopped-holder",
		namespace:       "testdb",
		table:           "users",
	}
}

func supersedeSuccessor() *storage.Apply {
	return &storage.Apply{
		ID:              8,
		ApplyIdentifier: "apply-successor",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
	}
}

// A dispatch that changes the same table as a released stopped apply meets the
// copy that apply left resting on the target, and the engine decides on its own
// whether to resume or replace it. The stopped apply's work is no longer its
// own to continue, so the successor is recorded on it and its timeline says who
// took the work over.
func TestMarkSupersededHoldersRecordsTheTakeover(t *testing.T) {
	applies := &recordingApplyStore{}
	logs := &mockApplyLogStore{}
	client, _ := supersedeClient(applies, logs)
	successor := supersedeSuccessor()

	client.markSupersededHolders(t.Context(), successor, []supersededHolder{supersedeHolder()},
		[]storage.TableChange{{Namespace: "testdb", Table: "users", Operation: "alter"}})

	require.Len(t, applies.calls, 1, "the holder whose table the dispatch changes is marked")
	assert.Equal(t, int64(7), applies.calls[0].applyID)
	assert.Equal(t, "apply-successor", applies.calls[0].successor)

	require.Len(t, logs.logs, 1, "the holder's timeline records the handoff")
	assert.Equal(t, int64(7), logs.logs[0].ApplyID, "the event belongs to the superseded apply, not the successor")
	assert.Contains(t, logs.logs[0].Message, "apply-successor", "the timeline names the apply that took over")
	assert.Contains(t, logs.logs[0].Message, "users", "the timeline names the table whose work was taken over")
	assert.Contains(t, logs.logs[0].Message, "can no longer be started",
		"the timeline states the consequence the marker enforces")
	assert.Contains(t, logs.logs[0].Message, "fresh apply",
		"the timeline says where work the successor did not take over goes")
}

// The marker and the refusal it backs are apply-granular: several released
// tasks of one apply met by one dispatch are a single handoff, recorded with
// one mark and one timeline event naming every table taken over — not one
// event per task, which would read as several takeovers of the same apply.
func TestMarkSupersededHoldersRecordsOneHandoffPerApply(t *testing.T) {
	applies := &recordingApplyStore{}
	logs := &mockApplyLogStore{}
	client, _ := supersedeClient(applies, logs)

	usersShardA := supersedeHolder()
	usersShardB := supersedeHolder()
	accounts := supersedeHolder()
	accounts.table = "accounts"

	client.markSupersededHolders(t.Context(), supersedeSuccessor(),
		[]supersededHolder{usersShardA, usersShardB, accounts},
		[]storage.TableChange{
			{Namespace: "testdb", Table: "users", Operation: "alter"},
			{Namespace: "testdb", Table: "accounts", Operation: "alter"},
		})

	require.Len(t, applies.calls, 1, "one apply's released tasks are one handoff, marked once")
	assert.Equal(t, int64(7), applies.calls[0].applyID)

	require.Len(t, logs.logs, 1, "one timeline event records the whole handoff")
	assert.Contains(t, logs.logs[0].Message, "users, accounts",
		"the single event names every table taken over, each once")
}

// Releasing a stopped apply's hold frees its whole database, but only a
// dispatch that touches the same table meets the copy it left behind. A
// dispatch that changes a different table takes over nothing, so the holder
// keeps its own work and stays startable.
func TestMarkSupersededHoldersLeavesAHolderOfAnUntouchedTable(t *testing.T) {
	applies := &recordingApplyStore{}
	logs := &mockApplyLogStore{}
	client, _ := supersedeClient(applies, logs)

	client.markSupersededHolders(t.Context(), supersedeSuccessor(), []supersededHolder{supersedeHolder()},
		[]storage.TableChange{{Namespace: "testdb", Table: "orders", Operation: "alter"}})

	assert.Empty(t, applies.calls, "a dispatch that changes another table takes over no work")
	assert.Empty(t, logs.logs, "a holder that keeps its work gets no handoff event")
}

// A holder whose task names no table is a multi-table atomic change: its
// resting copies are real, just not keyed by table. Any table change the
// dispatch makes in that namespace may meet one of them, and the ambiguity
// resolves toward marking — an unmarked holder whose copy was met can be
// started into replaying work the successor now owns, while a marked holder's
// remaining work still reaches the database through a fresh apply.
func TestMarkSupersededHoldersMarksATablelessHolderOnItsNamespace(t *testing.T) {
	applies := &recordingApplyStore{}
	logs := &mockApplyLogStore{}
	client, _ := supersedeClient(applies, logs)

	holder := supersedeHolder()
	holder.table = ""

	client.markSupersededHolders(t.Context(), supersedeSuccessor(), []supersededHolder{holder},
		[]storage.TableChange{{Namespace: "testdb", Table: "users", Operation: "alter"}})

	require.Len(t, applies.calls, 1, "a table change in the holder's namespace meets its unkeyed copies")
	assert.Equal(t, int64(7), applies.calls[0].applyID)

	require.Len(t, logs.logs, 1)
	assert.Contains(t, logs.logs[0].Message, "this change's unfinished work",
		"a holder with no table to name has its work named as a whole")
}

// A tableless holder is met by table changes in its namespace, not by
// namespace-level work: a dispatch of VSchema-only changes leaves no copy on
// the target and meets none, and a table change in another namespace says
// nothing about this holder's copies. Either way the holder keeps its work.
func TestMarkSupersededHoldersLeavesATablelessHolderUnmet(t *testing.T) {
	tests := []struct {
		name    string
		changes []storage.TableChange
	}{
		{
			name:    "dispatch carries only namespace-level work",
			changes: []storage.TableChange{{Namespace: "testdb", Operation: "alter"}},
		},
		{
			name:    "dispatch changes tables in another namespace only",
			changes: []storage.TableChange{{Namespace: "otherdb", Table: "users", Operation: "alter"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			applies := &recordingApplyStore{}
			logs := &mockApplyLogStore{}
			client, _ := supersedeClient(applies, logs)

			holder := supersedeHolder()
			holder.table = ""

			client.markSupersededHolders(t.Context(), supersedeSuccessor(), []supersededHolder{holder}, tt.changes)

			assert.Empty(t, applies.calls, "no table change meets the holder's namespace, so nothing is taken over")
			assert.Empty(t, logs.logs)
		})
	}
}

// A namespace left absent means the dispatch's own database, on the change and
// on the task row alike. Both sides must be read that way, or a MySQL dispatch
// misses the holder whose copy it is about to meet.
func TestMarkSupersededHoldersMatchesAnAbsentNamespaceAgainstTheDatabase(t *testing.T) {
	tests := []struct {
		name            string
		holderNamespace string
		changeNamespace string
	}{
		{name: "change names no namespace", holderNamespace: "testdb"},
		{name: "task named no namespace", changeNamespace: "testdb"},
		{name: "neither names one"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			applies := &recordingApplyStore{}
			client, _ := supersedeClient(applies, &mockApplyLogStore{})

			holder := supersedeHolder()
			holder.namespace = tt.holderNamespace

			client.markSupersededHolders(t.Context(), supersedeSuccessor(), []supersededHolder{holder},
				[]storage.TableChange{{Namespace: tt.changeNamespace, Table: "users", Operation: "alter"}})

			require.Len(t, applies.calls, 1, "an absent namespace is the dispatch's own database")
			assert.Equal(t, int64(7), applies.calls[0].applyID)
		})
	}
}

// The first successor of a stopped apply owns its marker. A later dispatch that
// meets the same released holder changes nothing rather than reassigning the
// handoff, and says so instead of reporting a failure.
func TestMarkSupersededHoldersToleratesAnEarlierHandoff(t *testing.T) {
	applies := &recordingApplyStore{err: storage.ErrApplyAlreadySuperseded}
	logs := &mockApplyLogStore{}
	client, logged := supersedeClient(applies, logs)

	client.markSupersededHolders(t.Context(), supersedeSuccessor(), []supersededHolder{supersedeHolder()},
		[]storage.TableChange{{Namespace: "testdb", Table: "users", Operation: "alter"}})

	require.Len(t, applies.calls, 1)
	assert.Empty(t, logs.logs, "the earlier successor already recorded the handoff, so this one adds no event")
	assert.NotContains(t, logged.String(), "level=ERROR",
		"a handoff another dispatch already recorded is a benign race, not something an operator has to act on")
}

// The successor is already dispatched and taking the work over by the time the
// marker is written, so a failed write cannot un-take it. The dispatch carries
// on, and every remaining holder is still attempted.
func TestMarkSupersededHoldersContinuesPastAFailedMark(t *testing.T) {
	applies := &recordingApplyStore{err: errors.New("storage down")}
	logs := &mockApplyLogStore{}
	client, logged := supersedeClient(applies, logs)

	second := supersedeHolder()
	second.applyID = 9
	second.applyIdentifier = "apply-second-holder"
	second.table = "accounts"

	client.markSupersededHolders(t.Context(), supersedeSuccessor(),
		[]supersededHolder{supersedeHolder(), second},
		[]storage.TableChange{
			{Namespace: "testdb", Table: "users", Operation: "alter"},
			{Namespace: "testdb", Table: "accounts", Operation: "alter"},
		})

	require.Len(t, applies.calls, 2, "a failed mark does not abandon the remaining holders")
	assert.Equal(t, int64(9), applies.calls[1].applyID)
	assert.Empty(t, logs.logs, "a handoff that was not recorded must not claim one on the timeline")

	failures := logged.String()
	assert.Contains(t, failures, "level=ERROR",
		"a marker that was not written leaves a start unrefused, which an operator has to act on")
	assert.Contains(t, failures, "apply-stopped-holder", "the log names each holder left without a marker")
	assert.Contains(t, failures, "apply-second-holder")
}
