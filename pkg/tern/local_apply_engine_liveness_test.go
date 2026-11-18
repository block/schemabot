package tern

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// eventEmittingEngine reports a progress event from inside Apply, the way an
// engine does while it waits on a slow external phase, then winds the drive
// down. insideApply is true only while that event is being delivered.
type eventEmittingEngine struct {
	engine.Engine
	cancel      context.CancelFunc
	insideApply bool
}

func (e *eventEmittingEngine) Name() string { return "event-emitting" }

func (e *eventEmittingEngine) Apply(_ context.Context, req *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.insideApply = true
	req.OnEvent(engine.ApplyEvent{Message: "Waiting for branch schemabot-orders-1 to be ready (1m0s elapsed)"})
	e.insideApply = false
	e.cancel()
	return &engine.ApplyResult{Accepted: true}, nil
}

func (e *eventEmittingEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	return &engine.ProgressResult{State: engine.StateRunning}, nil
}

// An engine's Apply call can legitimately run longer than the operator's stall
// window: preparing a branch of a large sharded database takes many minutes,
// and no poll loop writes the tasks until the call returns. The operator reads
// the tasks' write time as the drive's liveness, so each event the engine
// reports from inside the call persists every task row; otherwise a healthy
// drive is cancelled as wedged and resumed, again and again. A write refused
// because the lease moved stops the mirroring at once, since the rows now
// belong to another driver.
func TestExecuteGroupedApply_EngineEventsKeepTheDriveLive(t *testing.T) {
	tests := []struct {
		name       string
		refusal    error
		wantWrites int
	}{
		{name: "every task row is written while the engine call runs", wantWrites: 2},
		{name: "a lost lease stops the mirroring at the first refused write", refusal: fmt.Errorf("apply apply-1 lease taken by another driver: %w", storage.ErrApplyLeaseLost), wantWrites: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			tasks := []*storage.Task{
				{ID: 1, ApplyID: 1, TaskIdentifier: "task-1", Database: "orders", Namespace: "orders", TableName: "line_items", State: state.Task.Pending, DDL: "CREATE TABLE `line_items` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"},
				{ID: 2, ApplyID: 1, TaskIdentifier: "task-2", Database: "orders", Namespace: "orders", TableName: "shipments", State: state.Task.Pending, DDL: "CREATE TABLE `shipments` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"},
			}
			apply := &storage.Apply{
				ID: 1, ApplyIdentifier: "apply-1", Database: "orders",
				Environment: "staging", State: state.Apply.Pending,
			}
			eng := &eventEmittingEngine{cancel: cancel}
			var writesInsideApply []string
			taskStore := &exactProgressTaskStore{
				tasks: tasks,
				updateErr: func(_ context.Context, task *storage.Task) error {
					if !eng.insideApply {
						return nil
					}
					writesInsideApply = append(writesInsideApply, task.TaskIdentifier)
					return tt.refusal
				},
			}
			client := &LocalClient{
				config:            LocalConfig{Database: "orders"},
				customEngine:      eng,
				heartbeatInterval: time.Hour,
				storage: &exactProgressStorage{
					applies:         &snapshotApplyStore{stored: *apply},
					tasks:           taskStore,
					controlRequests: &testControlRequestStore{},
					logs:            &mockApplyLogStore{},
				},
				logger: slog.Default(),
			}
			plan := &storage.Plan{Namespaces: map[string]*storage.NamespacePlanData{"orders": {}}}

			require.NoError(t, client.executeGroupedApply(ctx, apply, tasks, plan, nil, false))

			assert.Len(t, writesInsideApply, tt.wantWrites)
			assert.Equal(t, "task-1", writesInsideApply[0])
		})
	}
}
