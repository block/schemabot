package tern

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// A multi-table apply interleaves Spirit log lines from every table in the
// apply log stream, so each stored line must name its table and be attributed
// to that table's task — otherwise an operator triaging a failure cannot tell
// which table a line (especially a fatal error) belongs to. Lines that carry
// no single table (run-level or spanning several tables) belong to the apply,
// not to an arbitrary task.
func TestSpiritApplyLogFunc_TableContextAndTaskAttribution(t *testing.T) {
	logs := &mockApplyLogStore{}
	client := &LocalClient{
		storage: &mockStorage{logs: logs},
		logger:  slog.Default(),
	}

	apply := &storage.Apply{ID: 9}
	baristas := &storage.Task{ID: 101, TableName: "baristas"}
	customers := &storage.Task{ID: 102, TableName: "customers"}
	logFn := client.spiritApplyLogFunc(t.Context(), apply, []*storage.Task{baristas, customers})

	logFn(slog.LevelInfo, "customers", "copy rows complete")
	logFn(slog.LevelError, "baristas", "fatal error processing GTID rows event: row has 8 values")
	logFn(slog.LevelInfo, "", "starting schema change")

	require.Len(t, logs.logs, 3)

	assert.Equal(t, "[customers] copy rows complete", logs.logs[0].Message)
	assert.Equal(t, storage.LogLevelInfo, logs.logs[0].Level)
	require.NotNil(t, logs.logs[0].TaskID)
	assert.Equal(t, customers.ID, *logs.logs[0].TaskID)

	assert.Equal(t, "[baristas] fatal error processing GTID rows event: row has 8 values", logs.logs[1].Message)
	assert.Equal(t, storage.LogLevelError, logs.logs[1].Level)
	require.NotNil(t, logs.logs[1].TaskID)
	assert.Equal(t, baristas.ID, *logs.logs[1].TaskID)

	assert.Equal(t, "starting schema change", logs.logs[2].Message)
	assert.Nil(t, logs.logs[2].TaskID, "a line with no table belongs to the apply, not an arbitrary task")

	for _, stored := range logs.logs {
		assert.Equal(t, apply.ID, stored.ApplyID)
		assert.Equal(t, storage.LogSourceSpirit, stored.Source)
	}
}

// A table list that matches no single task — a combined statement spanning
// several tables — still names the tables in the stored message but is
// attributed to the apply.
func TestSpiritApplyLogFunc_MultiTableLineAttributedToApply(t *testing.T) {
	logs := &mockApplyLogStore{}
	client := &LocalClient{
		storage: &mockStorage{logs: logs},
		logger:  slog.Default(),
	}

	apply := &storage.Apply{ID: 4}
	tasks := []*storage.Task{{ID: 7, TableName: "drinks"}, {ID: 8, TableName: "orders"}}
	logFn := client.spiritApplyLogFunc(t.Context(), apply, tasks)

	logFn(slog.LevelInfo, "drinks, orders", "apply complete")

	require.Len(t, logs.logs, 1)
	assert.Equal(t, "[drinks, orders] apply complete", logs.logs[0].Message)
	assert.Nil(t, logs.logs[0].TaskID)
}

// The engine's log wiring is bound to the context it was built on. A resume
// that hands its polling to a goroutine outliving the caller therefore rewires
// on that goroutine's own context: a callback still holding the caller's
// cancelled context records nothing, and the engine lines for the rest of the
// schema change — the ones an operator reads when it fails — are exactly the
// ones that would be lost.
func TestSpiritApplyLogFunc_BoundContextDecidesWhatSurvivesTheCaller(t *testing.T) {
	logs := &mockApplyLogStore{}
	client := &LocalClient{
		storage: &mockStorage{logs: logs},
		logger:  slog.Default(),
	}

	apply := &storage.Apply{ID: 3}
	tasks := []*storage.Task{{ID: 11, TableName: "orders"}}

	callerCtx, cancelCaller := context.WithCancel(t.Context())
	boundToCaller := client.spiritApplyLogFunc(callerCtx, apply, tasks)
	boundToDetachedPoll := client.spiritApplyLogFunc(context.WithoutCancel(callerCtx), apply, tasks)

	cancelCaller()

	boundToCaller(slog.LevelInfo, "orders", "copy rows complete")
	assert.Empty(t, logs.logs, "a callback holding the caller's cancelled context records nothing")

	boundToDetachedPoll(slog.LevelError, "orders", "fatal error processing GTID rows event")
	require.Len(t, logs.logs, 1)
	assert.Equal(t, "[orders] fatal error processing GTID rows event", logs.logs[0].Message)
	assert.Equal(t, storage.LogLevelError, logs.logs[0].Level)
	require.NotNil(t, logs.logs[0].TaskID)
	assert.Equal(t, tasks[0].ID, *logs.logs[0].TaskID)
}
