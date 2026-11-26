package tern

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// terminalDeliveryObserver records every terminal delivery with the apply
// state and task states it carried.
type terminalDeliveryObserver struct {
	deliveries [][]string
}

func (o *terminalDeliveryObserver) OnProgress(*storage.Apply, []*storage.Task) {}
func (o *terminalDeliveryObserver) OnTerminal(apply *storage.Apply, tasks []*storage.Task) {
	delivery := []string{apply.State}
	for _, task := range tasks {
		delivery = append(delivery, task.State)
	}
	o.deliveries = append(o.deliveries, delivery)
}

// An operator cancels an apply while its drive is still running in this
// process, and the cancel path, running elsewhere, records the apply and its
// task cancelled. When the drive finalizes, it finds the apply already
// terminal in storage and delivers the observer it still holds once, with
// the state and task rows the settling writer stored rather than its own
// stale running copies, so the PR's terminal summary reports the cancel. A
// second finalization finds no observer left and delivers nothing.
func TestFinalizeSequentialApplyDeliversAnApplyAnotherWriterSettled(t *testing.T) {
	stored := failureLogTestApply(state.Apply.Cancelled, 0)
	storedTask := &storage.Task{ID: 1, TaskIdentifier: "task-1", ApplyID: stored.ID, TableName: "orders", State: state.Task.Cancelled}
	client := &LocalClient{
		config: LocalConfig{Database: "testdb", Type: storage.DatabaseTypePostgres},
		storage: &controlTestStorage{
			applies:         &controlTestApplyStore{apply: stored},
			tasks:           &controlTestTaskStore{tasks: []*storage.Task{storedTask}},
			applyLogs:       &capturingApplyLogStore{},
			controlRequests: &testControlRequestStore{},
		},
		logger: slog.Default(),
	}
	observer := &terminalDeliveryObserver{}
	client.SetObserver(stored.ID, observer)

	staleApply := failureLogTestApply(state.Apply.Running, 0)
	staleTask := &storage.Task{ID: 1, TaskIdentifier: "task-1", ApplyID: stored.ID, TableName: "orders", State: state.Task.Running}
	require.NoError(t, client.finalizeSequentialApply(t.Context(), staleApply, []*storage.Task{staleTask}, nil, false))

	assert.Equal(t, [][]string{{state.Apply.Cancelled, state.Task.Cancelled}}, observer.deliveries,
		"the drive delivers the stored terminal state and task rows once")
	assert.Equal(t, state.Apply.Cancelled, staleApply.State, "the drive adopts the stored terminal apply")

	again := failureLogTestApply(state.Apply.Running, 0)
	require.NoError(t, client.finalizeSequentialApply(t.Context(), again, []*storage.Task{staleTask}, nil, false))
	assert.Len(t, observer.deliveries, 1, "a second finalization must not deliver the terminal summary again")
}
