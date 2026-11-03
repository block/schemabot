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

// markerStore stands in for storage whose stopped applies carry the markers the
// test scripts: a successor that already committed the handoff, an earlier
// successor that owned it first, or a marker that cannot be read back.
type markerStore struct {
	storage.ApplyStore
	markers map[int64]string
	errs    map[int64]error
}

func (s *markerStore) GetSupersededBy(_ context.Context, applyID int64) (string, error) {
	if err := s.errs[applyID]; err != nil {
		return "", err
	}
	return s.markers[applyID], nil
}

// supersedeClient builds a client over the returned store, with a MySQL
// database so a change that names no namespace normalizes to it the way a real
// dispatch's does. Its logs are captured so a test can tell a benign outcome
// from one an operator has to act on.
func supersedeClient(applies *markerStore, logs *mockApplyLogStore) (*LocalClient, *bytes.Buffer) {
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

// takeoverIDs is the stopped applies a dispatch takes over, as the create or
// attach records them.
func takeoverIDs(client *LocalClient, holders []supersededHolder, changes []storage.TableChange) []int64 {
	return takeoverApplyIDs(client.supersededTakeovers(supersedeSuccessor(), holders, changes))
}

// A dispatch that changes the same table as a released stopped apply meets the
// copy that apply left resting on the target, and the engine decides on its own
// whether to resume or replace it. The stopped apply's work is no longer its
// own to continue, so the create or attach names it as taken over, and once that
// commits its timeline says who took the work over.
func TestSupersededTakeoversRecordTheTakeover(t *testing.T) {
	applies := &markerStore{markers: map[int64]string{7: "apply-successor"}}
	logs := &mockApplyLogStore{}
	client, _ := supersedeClient(applies, logs)
	successor := supersedeSuccessor()

	takeovers := client.supersededTakeovers(successor, []supersededHolder{supersedeHolder()},
		[]storage.TableChange{{Namespace: "testdb", Table: "users", Operation: "alter"}})
	assert.Equal(t, []int64{7}, takeoverApplyIDs(takeovers), "the holder whose table the dispatch changes is taken over")

	client.announceSupersededTakeovers(t.Context(), successor, takeovers)

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
// tasks of one apply met by one dispatch are a single handoff, named once and
// announced with one timeline event naming every table taken over — not one
// event per task, which would read as several takeovers of the same apply.
func TestSupersededTakeoversRecordOneHandoffPerApply(t *testing.T) {
	applies := &markerStore{markers: map[int64]string{7: "apply-successor"}}
	logs := &mockApplyLogStore{}
	client, _ := supersedeClient(applies, logs)
	successor := supersedeSuccessor()

	usersShardA := supersedeHolder()
	usersShardB := supersedeHolder()
	accounts := supersedeHolder()
	accounts.table = "accounts"

	takeovers := client.supersededTakeovers(successor,
		[]supersededHolder{usersShardA, usersShardB, accounts},
		[]storage.TableChange{
			{Namespace: "testdb", Table: "users", Operation: "alter"},
			{Namespace: "testdb", Table: "accounts", Operation: "alter"},
		})
	assert.Equal(t, []int64{7}, takeoverApplyIDs(takeovers), "one apply's released tasks are one handoff")

	client.announceSupersededTakeovers(t.Context(), successor, takeovers)

	require.Len(t, logs.logs, 1, "one timeline event records the whole handoff")
	assert.Contains(t, logs.logs[0].Message, "users, accounts",
		"the single event names every table taken over, each once")
}

// Releasing a stopped apply's hold frees its whole database, but only a
// dispatch that touches the same table meets the copy it left behind. A
// dispatch that changes a different table takes over nothing, so the holder
// keeps its own work and stays startable.
func TestSupersededTakeoversLeaveAHolderOfAnUntouchedTable(t *testing.T) {
	client, _ := supersedeClient(&markerStore{}, &mockApplyLogStore{})

	ids := takeoverIDs(client, []supersededHolder{supersedeHolder()},
		[]storage.TableChange{{Namespace: "testdb", Table: "orders", Operation: "alter"}})

	assert.Empty(t, ids, "a dispatch that changes another table takes over no work")
}

// A holder whose task names no table is a multi-table atomic change: its
// resting copies are real, just not keyed by table. Any table change the
// dispatch makes in that namespace may meet one of them, and the ambiguity
// resolves toward taking the work over — an unmarked holder whose copy was met
// can be started into replaying work the successor now owns, while a marked
// holder's remaining work still reaches the database through a fresh apply.
func TestSupersededTakeoversTakeOverATablelessHolderOnItsNamespace(t *testing.T) {
	applies := &markerStore{markers: map[int64]string{7: "apply-successor"}}
	logs := &mockApplyLogStore{}
	client, _ := supersedeClient(applies, logs)
	successor := supersedeSuccessor()

	holder := supersedeHolder()
	holder.table = ""

	takeovers := client.supersededTakeovers(successor, []supersededHolder{holder},
		[]storage.TableChange{{Namespace: "testdb", Table: "users", Operation: "alter"}})
	assert.Equal(t, []int64{7}, takeoverApplyIDs(takeovers), "a table change in the holder's namespace meets its unkeyed copies")

	client.announceSupersededTakeovers(t.Context(), successor, takeovers)

	require.Len(t, logs.logs, 1)
	assert.Contains(t, logs.logs[0].Message, "this change's unfinished work",
		"a holder with no table to name has its work named as a whole")
}

// A tableless holder is met by table changes in its namespace, not by
// namespace-level work: a dispatch of VSchema-only changes leaves no copy on
// the target and meets none, and a table change in another namespace says
// nothing about this holder's copies. Either way the holder keeps its work.
func TestSupersededTakeoversLeaveATablelessHolderUnmet(t *testing.T) {
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
			client, _ := supersedeClient(&markerStore{}, &mockApplyLogStore{})

			holder := supersedeHolder()
			holder.table = ""

			ids := takeoverIDs(client, []supersededHolder{holder}, tt.changes)

			assert.Empty(t, ids, "no table change meets the holder's namespace, so nothing is taken over")
		})
	}
}

// A namespace left absent means the dispatch's own database, on the change and
// on the task row alike. Both sides must be read that way, or a MySQL dispatch
// misses the holder whose copy it is about to meet.
func TestSupersededTakeoversMatchAnAbsentNamespaceAgainstTheDatabase(t *testing.T) {
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
			client, _ := supersedeClient(&markerStore{}, &mockApplyLogStore{})

			holder := supersedeHolder()
			holder.namespace = tt.holderNamespace

			ids := takeoverIDs(client, []supersededHolder{holder},
				[]storage.TableChange{{Namespace: tt.changeNamespace, Table: "users", Operation: "alter"}})

			assert.Equal(t, []int64{7}, ids, "an absent namespace is the dispatch's own database")
		})
	}
}

// The first successor of a stopped apply owns its marker. A later dispatch that
// meets the same released holder leaves the handoff as it is, and says so
// instead of claiming a takeover on the stopped apply's timeline or reporting a
// failure.
func TestAnnounceSupersededTakeoversToleratesAnEarlierHandoff(t *testing.T) {
	applies := &markerStore{markers: map[int64]string{7: "apply-earlier-successor"}}
	logs := &mockApplyLogStore{}
	client, logged := supersedeClient(applies, logs)

	client.announceSupersededTakeovers(t.Context(), supersedeSuccessor(),
		[]*supersededTakeover{{applyID: 7, applyIdentifier: "apply-stopped-holder", tables: []string{"users"}}})

	assert.Empty(t, logs.logs, "the earlier successor already recorded the handoff, so this one adds no event")
	assert.NotContains(t, logged.String(), "level=ERROR",
		"a handoff another dispatch already recorded is a benign race, not something an operator has to act on")
}

// The handoff is already committed with the successor's work by the time it is
// announced, so a marker that cannot be read back cannot un-take it. The
// dispatch carries on, and every remaining holder is still announced.
func TestAnnounceSupersededTakeoversContinuesPastAnUnreadableMarker(t *testing.T) {
	applies := &markerStore{
		markers: map[int64]string{9: "apply-successor"},
		errs:    map[int64]error{7: errors.New("storage down")},
	}
	logs := &mockApplyLogStore{}
	client, logged := supersedeClient(applies, logs)

	client.announceSupersededTakeovers(t.Context(), supersedeSuccessor(), []*supersededTakeover{
		{applyID: 7, applyIdentifier: "apply-stopped-holder", tables: []string{"users"}},
		{applyID: 9, applyIdentifier: "apply-second-holder", tables: []string{"accounts"}},
	})

	require.Len(t, logs.logs, 1, "an unreadable marker does not abandon the remaining holders")
	assert.Equal(t, int64(9), logs.logs[0].ApplyID)

	failures := logged.String()
	assert.Contains(t, failures, "level=ERROR", "a timeline that cannot name the successor is reported")
	assert.Contains(t, failures, "apply-stopped-holder", "the log names the holder whose timeline is missing the event")
}
