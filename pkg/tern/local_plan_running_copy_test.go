package tern

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func adoptCopy(namespace string, tables ...string) *engine.ExistingCopy {
	return &engine.ExistingCopy{
		Namespace:   namespace,
		Disposition: engine.CopyAdopt,
		Tables:      tables,
		Age:         4 * time.Second,
	}
}

// A plan marks a copy running from the database's non-terminal tasks alone. A
// task copying `line_items` on the planned target marks it, and a row that only
// reads as terminal once normalized marks nothing.
func TestRunningCopyTablesReadsOnlyTheNonTerminalTasks(t *testing.T) {
	operationID := int64(5)
	copying := &storage.Task{
		ID: 1, TaskIdentifier: "task-copying", Database: "orders", DatabaseType: storage.DatabaseTypeMySQL,
		Environment: "staging", TableName: "line_items", State: state.Task.Running, ApplyOperationID: &operationID,
	}
	aliased := &storage.Task{
		ID: 2, TaskIdentifier: "task-aliased", Database: "orders", DatabaseType: storage.DatabaseTypeMySQL,
		Environment: "staging", TableName: "shipments", State: "STATE_COMPLETED", ApplyOperationID: &operationID,
	}
	client := &LocalClient{
		config: LocalConfig{Database: "orders", Type: storage.DatabaseTypeMySQL},
		storage: &exactProgressStorage{
			tasks:           rawNonTerminalStore{rows: []*storage.Task{aliased, copying}},
			applyOperations: &exactProgressApplyOperationStore{ops: []*storage.ApplyOperation{{ID: operationID, Target: "orders-target"}}},
		},
		logger: slog.Default(),
	}

	running, err := client.runningCopyTables(t.Context(), "orders", "staging", "orders-target")

	require.NoError(t, err)
	assert.Equal(t, map[runningCopyKey]bool{{"orders", "line_items"}: true}, running)
}

// A copy the deployment is still making is disclosed as running, so the surface
// rendering it can say the apply joins work in flight rather than picking up
// work that stopped.
func TestExistingCopyIsMarkedRunningWhileItsTableIsBeingCopied(t *testing.T) {
	client := newMultisetClient("orders", storage.DatabaseTypeMySQL)

	copies := client.protoExistingCopies(
		&engine.PlanResult{ExistingCopies: []*engine.ExistingCopy{adoptCopy("orders", "line_items")}},
		map[runningCopyKey]bool{{"orders", "line_items"}: true},
	)

	require.Len(t, copies, 1)
	assert.True(t, copies[0].Running)
}

// A copy no in-flight task covers is work that stopped. It stays unmarked, so
// the disclosure keeps describing it as work applying picks back up.
func TestExistingCopyIsNotMarkedRunningWithoutAnInFlightTable(t *testing.T) {
	client := newMultisetClient("orders", storage.DatabaseTypeMySQL)

	copies := client.protoExistingCopies(
		&engine.PlanResult{ExistingCopies: []*engine.ExistingCopy{adoptCopy("orders", "line_items")}},
		map[runningCopyKey]bool{{"orders", "shipments"}: true},
	)

	require.Len(t, copies, 1)
	assert.False(t, copies[0].Running)
}

// A disclosure is one unit of work to the operator reading it, so one table
// still being copied makes the whole entry running: the sentence it renders —
// applying joins the copy rather than restarting it — is true as soon as any
// of its tables is live.
func TestExistingCopyIsMarkedRunningWhenAnyTableIsBeingCopied(t *testing.T) {
	client := newMultisetClient("orders", storage.DatabaseTypeMySQL)

	copies := client.protoExistingCopies(
		&engine.PlanResult{ExistingCopies: []*engine.ExistingCopy{adoptCopy("orders", "line_items", "shipments")}},
		map[runningCopyKey]bool{{"orders", "shipments"}: true},
	)

	require.Len(t, copies, 1)
	assert.True(t, copies[0].Running)
}

// Each namespace's disclosure is marked from its own tables, so a copy running
// in one namespace never vouches for a copy that stopped in another.
func TestExistingCopiesAreMarkedRunningPerNamespace(t *testing.T) {
	client := newMultisetClient("orders", storage.DatabaseTypeMySQL)

	copies := client.protoExistingCopies(
		&engine.PlanResult{ExistingCopies: []*engine.ExistingCopy{
			adoptCopy("orders", "line_items"),
			adoptCopy("payments", "charges"),
		}},
		map[runningCopyKey]bool{{"orders", "line_items"}: true},
	)

	require.Len(t, copies, 2)
	assert.True(t, copies[0].Running, "the namespace holding the in-flight table is running")
	assert.False(t, copies[1].Running, "a table of the same name elsewhere is a different copy")
}

// The namespace a task records and the one a disclosure names are normalized
// the same way, so a MySQL deployment's unnamed namespace matches its database
// rather than reading as a namespace of its own.
func TestExistingCopyRunningMarkNormalizesNamespace(t *testing.T) {
	client := newMultisetClient("orders", storage.DatabaseTypeMySQL)

	copies := client.protoExistingCopies(
		&engine.PlanResult{ExistingCopies: []*engine.ExistingCopy{adoptCopy("", "line_items")}},
		map[runningCopyKey]bool{{"orders", "line_items"}: true},
	)

	require.Len(t, copies, 1)
	assert.True(t, copies[0].Running)
}

// Without the deployment's task rows nothing is marked, which leaves every
// disclosure reading as work that stopped — the shape it had before the
// distinction existed, and the safe direction to fail in: an operator is told
// a running copy will be picked up, never that a copy that stopped is running.
func TestExistingCopiesAreUnmarkedWithoutARunningSet(t *testing.T) {
	client := newMultisetClient("orders", storage.DatabaseTypeMySQL)

	copies := client.protoExistingCopies(
		&engine.PlanResult{ExistingCopies: []*engine.ExistingCopy{adoptCopy("orders", "line_items")}},
		nil,
	)

	require.Len(t, copies, 1)
	assert.False(t, copies[0].Running)
	assert.Equal(t, int64(4), copies[0].AgeSeconds, "every other field is unaffected")
}
