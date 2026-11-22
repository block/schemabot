//go:build integration

package api

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/block/schemabot/pkg/applynotifications"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type completionRecorder struct {
	events []applynotifications.Event
	err    error
}

func (r *completionRecorder) Publish(_ context.Context, event applynotifications.Event) error {
	r.events = append(r.events, event)
	return r.err
}

// A PR-associated apply spanning two operations emits only after the parent
// completes. A failed notification still leaves both operations and the parent
// completed, and the existing terminal summary still receives all tasks.
func TestMultiOperationCompletionNotification(t *testing.T) {
	db := openMatrixStorage(t)
	stor := mysqlstore.New(db)
	for _, sendFails := range []bool{false, true} {
		name := "send succeeds"
		if sendFails {
			name = "send fails"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			resetMatrixTables(t, ctx, db)
			seed := seedGroupedApply(t, ctx, stor, multiOpSeed{
				applyIdentifier: "notify-multi-op", parentState: state.Apply.Pending,
				cutoverPolicy: storage.CutoverPolicyRolling, onFailure: storage.OnFailureHalt,
				deployments: []string{"region-a", "region-b"}, opState: state.ApplyOperation.Pending, taskState: state.Task.Pending,
			})
			svc := newMatrixService(t, stor, matrixClients(stor, &driveRecorder{}, map[string]matrixOutcome{
				"region-a": {taskState: state.Task.Completed}, "region-b": {taskState: state.Task.Completed},
			}))
			publisher := &completionRecorder{}
			if sendFails {
				publisher.err = errors.New("queue unavailable")
			}
			svc.OnApplyTerminalSummary = applynotifications.Wrap(svc.OnApplyTerminalSummary, publisher, slog.New(slog.DiscardHandler))
			driveNextOperation(t, ctx, svc, 1)
			assert.Empty(t, publisher.events, "a completed operation is not a completed apply")
			driveNextOperation(t, ctx, svc, 2)
			apply := getApply(t, ctx, stor, seed.applyID)
			assert.Equal(t, state.Apply.Completed, apply.State)
			require.NotNil(t, apply.CompletedAt)
			for _, deployment := range seed.deployments {
				assert.Equal(t, state.ApplyOperation.Completed, opState(t, ctx, stor, seed.opID(deployment)))
			}
			assert.Equal(t, 1, svc.matrixSummary.count())
			assert.Len(t, svc.matrixSummary.lastTasks(), 2)
			require.Len(t, publisher.events, 1)
			assert.Equal(t, applynotifications.Event{
				ApplyIdentifier: "notify-multi-op", Database: "payments", DatabaseType: storage.DatabaseTypeMySQL,
				Environment: "staging", Repository: "octocat/hello-world", PullRequest: 1, CompletedAt: apply.CompletedAt,
			}, publisher.events[0])
		})
	}
}
