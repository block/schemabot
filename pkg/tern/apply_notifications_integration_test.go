//go:build integration

package tern

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/notifications"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

type completionPublisher struct{ events []notifications.Event }

func (p *completionPublisher) Publish(_ context.Context, event notifications.Event) error {
	p.events = append(p.events, event)
	return nil
}

// Exercise each actual completion writer against SQL, then use a fresh catch-up
// runner with no observer/webhook to prove that stored completion is sufficient.
func TestCompletionPathsArePublishedWithoutGitHub(t *testing.T) {
	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	for _, path := range []string{"aggregate_projection", "already_completed_engine_control", "recovery_without_tasks", "recovery_replan_without_work", "taskless_grouped", "sequential"} {
		t.Run(path, func(t *testing.T) {
			cleanupTasks(t, dsn)
			cleanupTestTables(t, dsn)
			store := createStorage(t, dsn)
			defer utils.CloseAndLog(store)
			client, _ := newTasklessControlClient(t, dsn, store)
			var changes []storage.TableChange
			if path == "recovery_replan_without_work" {
				changes = []storage.TableChange{{Namespace: "testdb", Table: "users", DDL: "ALTER TABLE users ADD COLUMN email varchar(255)", Operation: "alter"}}
			}
			pending := dispatchQueuedApply(t, store, client, changes)
			apply, err := store.Applies().ClaimApplyByID(t.Context(), pending.ID, "notification-path-test")
			require.NoError(t, err)
			require.NotNil(t, apply)
			ctx := storage.WithApplyLease(t.Context(), apply.Lease())
			apply.State = state.Apply.Running
			started := time.Now().Add(-time.Minute)
			apply.StartedAt = &started
			require.NoError(t, store.Applies().Update(ctx, apply))
			tasks, err := store.Tasks().GetByApplyID(ctx, apply.ID)
			require.NoError(t, err)
			switch path {
			case "aggregate_projection":
				now := time.Now()
				changed, err := store.Applies().UpdateDerivedState(ctx, apply.ID, apply.State, state.Apply.Completed, "", apply.StartedAt, &now)
				require.NoError(t, err)
				require.True(t, changed)
			case "already_completed_engine_control":
				_, err := client.settleControlForCompletedEngineChange(ctx, "cancel", nil, apply.ID, apply, "test", engine.NewAlreadyCompletedError("already completed"))
				require.NoError(t, err)
			case "recovery_without_tasks":
				require.NoError(t, client.resumeApplyWithTasks(ctx, apply, nil, nil, nil, false, false))
			case "recovery_replan_without_work":
				require.Len(t, tasks, 1)
				tasks[0].State = state.Task.Completed
				now := time.Now()
				tasks[0].CompletedAt = &now
				require.NoError(t, store.Tasks().Update(ctx, tasks[0]))
				require.NoError(t, client.resumeApplyWithTasks(ctx, apply, nil, tasks, nil, false, false))
			case "taskless_grouped":
				require.NoError(t, client.completeTasklessGroupedApply(ctx, apply, ""))
			case "sequential":
				require.NoError(t, client.finalizeSequentialApply(ctx, apply, nil, nil, false))
			}
			completed, err := store.Applies().Get(t.Context(), apply.ID)
			require.NoError(t, err)
			require.Equal(t, state.Apply.Completed, completed.State)
			require.NotNil(t, completed.CompletedAt)
			assert.Empty(t, completed.Repository)
			assert.Zero(t, completed.PullRequest)
			publisher := &completionPublisher{}
			runner, err := notifications.New(store.Notifications(), publisher, notifications.Config{}, slog.Default())
			require.NoError(t, err)
			require.NoError(t, runner.Pass(t.Context()))
			require.Len(t, publisher.events, 1)
			assert.Equal(t, apply.ApplyIdentifier, publisher.events[0].ApplyIdentifier)
			assert.WithinDuration(t, *completed.CompletedAt, publisher.events[0].CompletedAt, time.Second)
			assert.Equal(t, completed.Database, publisher.events[0].Database)
			restarted, err := notifications.New(store.Notifications(), publisher, notifications.Config{}, slog.Default())
			require.NoError(t, err)
			require.NoError(t, restarted.Pass(t.Context()))
			assert.Len(t, publisher.events, 1, "a new process does not resend a marked publication")
		})
	}
}
