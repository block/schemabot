package tern

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// errOtherSchemaChangeRunning is what draining the whole engine reports while
// a schema change on another table, or on a sibling shard, is still running.
var errOtherSchemaChangeRunning = errors.New("another schema change is still running")

// errTableCuttingOver is what draining a table reports while that table's
// previous run is still cutting over.
var errTableCuttingOver = errors.New("the table's previous run is still cutting over")

// perTableDrainEngine runs each table's work on each shard as its own schema
// change, and another one is still running: draining the whole engine cannot
// finish, while draining one table returns tableErr once that table is settled.
type perTableDrainEngine struct {
	*replanTargetEngine
	drainedTargets []engine.DrainTarget
	wholeDrains    int
	tableErr       error
}

func (e *perTableDrainEngine) DrainContext(context.Context) error {
	e.wholeDrains++
	return errOtherSchemaChangeRunning
}

func (e *perTableDrainEngine) DrainTargetContext(_ context.Context, target engine.DrainTarget) error {
	e.drainedTargets = append(e.drainedTargets, target)
	return e.tableErr
}

// A sharded namespace copies orders on every shard at once, and a schema
// change on another table holds at its deferred cutover. A drive resuming the
// orders task on shard -40 waits only for work on that table and shard, so it
// starts while the other schema changes keep running instead of queueing
// behind them.
func TestResumeApplySequential_TaskStartsWhileOtherSchemaChangesRun(t *testing.T) {
	eng := &perTableDrainEngine{replanTargetEngine: &replanTargetEngine{plan: resumeTaskReplan(resumeTaskDDL)}}
	client, apply, task := shardTaskFixture(eng)

	err := client.resumeApplySequential(t.Context(), apply, []*storage.Task{task}, &storage.Plan{ID: 7}, nil)

	require.NoError(t, err)
	assert.Equal(t, []engine.DrainTarget{{Namespace: "appdb_sharded", Shard: "-40", Table: "orders"}}, eng.drainedTargets,
		"the drive waits for its own table's work")
	assert.Zero(t, eng.wholeDrains, "the drive does not wait for other schema changes")
	assert.Equal(t, 1, eng.applies, "the task starts while other schema changes run")
	assert.Equal(t, state.Task.Completed, task.State)
}

// A drive does not start a task while that table's previous run is still
// cutting over: it reports the wait and leaves the task for a later drive.
func TestResumeApplySequential_TaskWaitsForItsOwnTablesWork(t *testing.T) {
	eng := &perTableDrainEngine{
		replanTargetEngine: &replanTargetEngine{plan: resumeTaskReplan(resumeTaskDDL)},
		tableErr:           errTableCuttingOver,
	}
	client, apply, task := shardTaskFixture(eng)

	err := client.resumeApplySequential(t.Context(), apply, []*storage.Task{task}, &storage.Plan{ID: 7}, nil)

	require.ErrorIs(t, err, errTableCuttingOver)
	assert.Contains(t, err.Error(), "appdb_sharded.orders")
	assert.Zero(t, eng.wholeDrains)
	assert.Zero(t, eng.applies, "the task does not start while its table's previous run is cutting over")
	assert.Equal(t, state.Task.Pending, task.State)
}

// wholeDrainEngine drains only as a whole.
type wholeDrainEngine struct {
	*replanTargetEngine
	drains int
}

func (e *wholeDrainEngine) DrainContext(context.Context) error {
	e.drains++
	return nil
}

// An engine that cannot drain one table is drained as a whole, whatever table
// the drive names; a drive that covers the whole apply drains every engine as
// a whole; and an engine with no background work has nothing to drain.
func TestDrainEngineWork_FallsBackToTheWholeEngine(t *testing.T) {
	target := &engine.DrainTarget{Namespace: "appdb_sharded", Shard: "-40", Table: "orders"}

	whole := &wholeDrainEngine{replanTargetEngine: &replanTargetEngine{}}
	require.NoError(t, drainEngineWork(t.Context(), whole, target))
	assert.Equal(t, 1, whole.drains)

	perTable := &perTableDrainEngine{replanTargetEngine: &replanTargetEngine{}}
	require.ErrorIs(t, drainEngineWork(t.Context(), perTable, nil), errOtherSchemaChangeRunning)
	assert.Equal(t, 1, perTable.wholeDrains)
	assert.Empty(t, perTable.drainedTargets)

	require.NoError(t, drainEngineWork(t.Context(), &replanTargetEngine{}, target))
}
