package storagetest

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// TestApplyLogs runs the behavioral parity suite for storage.ApplyLogStore.
// The ordering assertions exercise the interface's normative contract:
// created_at ascending with ties broken by ascending id, so rapid same-
// timestamp appends always read back in insertion order.
func TestApplyLogs(t *testing.T, h Harness) {
	// GetByApply verifies the full audit read: every entry for the apply, in
	// insertion order per the ordering contract, and an unknown apply
	// returns no entries.
	t.Run("GetByApply", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "all_logs_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "apply_all_logs", 600, state.Apply.Running, "staging")

		const seeded = 4
		for i := 1; i <= seeded; i++ {
			require.NoError(t, store.ApplyLogs().Append(ctx, &storage.ApplyLog{
				ApplyID:   apply.ID,
				Level:     storage.LogLevelInfo,
				EventType: storage.LogEventInfo,
				Source:    storage.LogSourceSchemaBot,
				Message:   fmt.Sprintf("entry %d", i),
			}))
		}

		all, err := store.ApplyLogs().GetByApply(ctx, apply.ID)
		require.NoError(t, err)
		require.Len(t, all, seeded)
		for i, entry := range all {
			assert.Equal(t, fmt.Sprintf("entry %d", i+1), entry.Message)
		}

		none, err := store.ApplyLogs().GetByApply(ctx, apply.ID+1000)
		require.NoError(t, err)
		assert.Empty(t, none)
	})

	// An apply log line quotes the failure it reports, which has no bound of
	// its own. A line past the column's width is stored cut to fit, so the
	// apply log still carries the failure an operator reads it for.
	t.Run("Append_PastColumnWidth_IsCutToFit", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "long_log_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "apply_long_log", 601, state.Apply.Running, "staging")

		require.NoError(t, store.ApplyLogs().Append(ctx, &storage.ApplyLog{
			ApplyID:   apply.ID,
			Level:     storage.LogLevelError,
			EventType: storage.LogEventError,
			Source:    storage.LogSourceSchemaBot,
			Message:   "Apply failed: " + overLongFailureMessage,
		}), "an apply log line past the column's width is stored")

		logs, err := store.ApplyLogs().GetByApply(ctx, apply.ID)
		require.NoError(t, err)
		require.Len(t, logs, 1)
		// The prefix keeps every two-byte character at an odd offset, so the
		// cut still falls inside one and backs off to its start.
		want := "Apply failed: x" + strings.Repeat("é", 32_758) + "…"
		assertStoredMessage(t, want, logs[0].Message, "the apply log line is cut")
	})

	// GetRecentByApply verifies the bounded tail read used by the
	// failed-summary comment: only the newest limit entries are returned, in
	// insertion order per the ordering contract.
	t.Run("GetRecentByApply", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "recent_logs_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "apply_recent_logs", 601, state.Apply.Running, "staging")

		const seeded = 5
		for i := 1; i <= seeded; i++ {
			require.NoError(t, store.ApplyLogs().Append(ctx, &storage.ApplyLog{
				ApplyID:   apply.ID,
				Level:     storage.LogLevelInfo,
				EventType: storage.LogEventInfo,
				Source:    storage.LogSourceSchemaBot,
				Message:   fmt.Sprintf("entry %d", i),
			}))
		}

		recent, err := store.ApplyLogs().GetRecentByApply(ctx, apply.ID, 3)
		require.NoError(t, err)
		require.Len(t, recent, 3)
		assert.Equal(t, "entry 3", recent[0].Message)
		assert.Equal(t, "entry 4", recent[1].Message)
		assert.Equal(t, "entry 5", recent[2].Message)

		all, err := store.ApplyLogs().GetRecentByApply(ctx, apply.ID, seeded*2)
		require.NoError(t, err)
		require.Len(t, all, seeded)
		for i, entry := range all {
			assert.Equal(t, fmt.Sprintf("entry %d", i+1), entry.Message)
		}

		none, err := store.ApplyLogs().GetRecentByApply(ctx, apply.ID+1000, 3)
		require.NoError(t, err)
		assert.Empty(t, none)
	})

	// List_ReturnsNewestWindow verifies the bounded read behind the logs API:
	// when an apply has more entries than the requested limit, the window is
	// the newest limit entries in chronological order — an operator tailing a
	// busy apply sees the latest activity, not a stale head. Insertion order
	// within the window holds per the ordering contract.
	t.Run("List_ReturnsNewestWindow", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "list_logs_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "apply_list_logs", 602, state.Apply.Running, "staging")

		const seeded = 6
		for i := 1; i <= seeded; i++ {
			level := storage.LogLevelInfo
			if i%2 == 0 {
				level = storage.LogLevelWarn
			}
			require.NoError(t, store.ApplyLogs().Append(ctx, &storage.ApplyLog{
				ApplyID:   apply.ID,
				Level:     level,
				EventType: storage.LogEventInfo,
				Source:    storage.LogSourceSchemaBot,
				Message:   fmt.Sprintf("entry %d", i),
			}))
		}

		window, err := store.ApplyLogs().List(ctx, storage.ApplyLogFilter{ApplyID: apply.ID, Limit: 3})
		require.NoError(t, err)
		require.Len(t, window, 3)
		assert.Equal(t, "entry 4", window[0].Message)
		assert.Equal(t, "entry 5", window[1].Message)
		assert.Equal(t, "entry 6", window[2].Message)

		all, err := store.ApplyLogs().List(ctx, storage.ApplyLogFilter{ApplyID: apply.ID, Limit: seeded * 2})
		require.NoError(t, err)
		require.Len(t, all, seeded)
		for i, entry := range all {
			assert.Equal(t, fmt.Sprintf("entry %d", i+1), entry.Message)
		}

		warns, err := store.ApplyLogs().List(ctx, storage.ApplyLogFilter{ApplyID: apply.ID, Level: storage.LogLevelWarn, Limit: 2})
		require.NoError(t, err)
		require.Len(t, warns, 2)
		assert.Equal(t, "entry 4", warns[0].Message)
		assert.Equal(t, "entry 6", warns[1].Message)
	})

	// ListByOperation verifies that an entry is attributed to the operation
	// whose drive wrote it, read off the operation lease on the context, and
	// that an operation filter returns exactly that operation's entries: the
	// ones its drive wrote, and the ones about its tasks written from outside
	// its drive. Another operation's entries and entries about the whole apply
	// are left out.
	t.Run("ListByOperation", func(t *testing.T) {
		ctx := t.Context()
		store := h.NewStorage(t)

		lock := CreateLock(t, store, "operation_logs_db", storage.DatabaseTypeMySQL)
		apply := CreateApplyWithStateAndEnv(t, store, lock, "apply_operation_logs", 603, state.Apply.Running, "production")
		opIDs := make(map[string]int64, 2)
		taskIDs := make(map[string]int64, 2)
		for _, target := range []string{"shop-001", "shop-002"} {
			opID, err := store.ApplyOperations().Insert(ctx, &storage.ApplyOperation{
				ApplyID: apply.ID, Deployment: "region-a", Target: target,
				OperationKey:  target + "/step-1",
				OperationKind: storage.ApplyOperationKindWork,
			})
			require.NoError(t, err)
			task := newTask(apply, "task_operation_logs_"+target, "orders", time.Now().UTC())
			task.ApplyOperationID = &opID
			taskID, err := store.Tasks().Create(ctx, task)
			require.NoError(t, err)
			opIDs[target] = opID
			taskIDs[target] = taskID
		}
		appendEntry := func(ctx context.Context, message string, taskID *int64) {
			t.Helper()
			require.NoError(t, store.ApplyLogs().Append(ctx, &storage.ApplyLog{
				ApplyID:   apply.ID,
				TaskID:    taskID,
				Level:     storage.LogLevelInfo,
				EventType: storage.LogEventInfo,
				Source:    storage.LogSourceSchemaBot,
				Message:   message,
			}))
		}
		driving := func(target string) context.Context {
			return storage.WithOperationLease(ctx, storage.OperationLease{ApplyID: apply.ID, OperationID: opIDs[target], Owner: "driver", Token: "token-" + target})
		}
		shop002Task := taskIDs["shop-002"]

		appendEntry(driving("shop-001"), "shop-001 drive", nil)
		appendEntry(driving("shop-002"), "shop-002 drive", nil)
		appendEntry(ctx, "whole apply", nil)
		appendEntry(ctx, "shop-002 task", &shop002Task)
		require.NoError(t, store.ApplyLogs().Append(driving("shop-001"), &storage.ApplyLog{
			ApplyID:   apply.ID,
			Level:     storage.LogLevelInfo,
			EventType: storage.LogEventStateTransition,
			Source:    storage.LogSourceSchemaBot,
			Message:   "projected by shop-001",
			ApplyWide: true,
		}))

		all, err := store.ApplyLogs().List(ctx, storage.ApplyLogFilter{ApplyID: apply.ID, Limit: 10})
		require.NoError(t, err)
		require.Len(t, all, 5)
		require.NotNil(t, all[0].ApplyOperationID, "an entry written under an operation lease names its operation")
		assert.Equal(t, opIDs["shop-001"], *all[0].ApplyOperationID)
		assert.Nil(t, all[2].ApplyOperationID, "an entry written outside any operation's drive names no operation")

		messagesFor := func(target string) []string {
			entries, err := store.ApplyLogs().List(ctx, storage.ApplyLogFilter{ApplyID: apply.ID, OperationIDs: []int64{opIDs[target]}, Limit: 10})
			require.NoError(t, err)
			messages := make([]string, 0, len(entries))
			for _, entry := range entries {
				messages = append(messages, entry.Message)
			}
			return messages
		}
		assert.Nil(t, all[4].ApplyOperationID, "an apply-wide entry written under an operation lease names no operation")
		assert.Equal(t, []string{"shop-002 drive", "whole apply", "shop-002 task", "projected by shop-001"}, messagesFor("shop-002"),
			"the parent state one operation projects belongs to every target")
		assert.Equal(t, []string{"shop-001 drive", "whole apply", "projected by shop-001"}, messagesFor("shop-001"), "another operation's drive and task entries stay out; the whole-apply entry belongs to every operation")
	})

	t.Run("Append_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		err := store.ApplyLogs().Append(t.Context(), &storage.ApplyLog{
			ApplyID:   1,
			Level:     storage.LogLevelInfo,
			EventType: storage.LogEventInfo,
			Source:    storage.LogSourceSchemaBot,
			Message:   "entry",
		})
		require.Error(t, err)
	})

	t.Run("GetByApply_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.ApplyLogs().GetByApply(t.Context(), 1)
		require.Error(t, err)
	})

	t.Run("GetRecentByApply_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.ApplyLogs().GetRecentByApply(t.Context(), 1, 3)
		require.Error(t, err)
	})

	t.Run("List_DBError", func(t *testing.T) {
		store := h.NewUnreachableStorage(t)
		_, err := store.ApplyLogs().List(t.Context(), storage.ApplyLogFilter{ApplyID: 1, Limit: 3})
		require.Error(t, err)
	})
}
