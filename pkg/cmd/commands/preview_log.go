package commands

import (
	"fmt"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/templates"
	"github.com/block/schemabot/pkg/state"
)

// logPreviewTime is a fixed reference time for deterministic log preview output.
var logPreviewTime = time.Date(2026, 1, 15, 14, 30, 0, 0, time.UTC)

// previewLogOutput dispatches log output preview types.
func previewLogOutput(previewType templates.PreviewType) {
	switch previewType {
	case templates.PreviewLogSmall:
		previewLogSmall()
	case templates.PreviewLogLarge:
		previewLogLarge()
	case templates.PreviewLogFailed:
		previewLogFailed()
	case templates.PreviewLogStopped:
		previewLogStopped()
	case templates.PreviewLogMulti:
		previewLogMulti()
	case templates.PreviewLogCutover:
		previewLogCutover()
	case templates.PreviewLogDetailed:
		previewLogDetailed()
	case templates.PreviewLogAll:
		previewLogAll()
	}
}

func previewLogAll() {
	sections := []struct {
		name string
		fn   func()
	}{
		{"SMALL/INSTANT TABLES", previewLogSmall},
		{"LARGE TABLE WITH HEARTBEATS", previewLogLarge},
		{"FAILED APPLY", previewLogFailed},
		{"STOPPED APPLY", previewLogStopped},
		{"MIXED (SMALL + LARGE)", previewLogMulti},
		{"CUTOVER FLOW", previewLogCutover},
		{"DETAILED (WITH TASK IDs)", previewLogDetailed},
	}
	for i, s := range sections {
		if i > 0 {
			fmt.Println()
		}
		fmt.Println("=" + strings.Repeat("=", 70))
		fmt.Println(s.name)
		fmt.Println("=" + strings.Repeat("=", 70))
		s.fn()
	}
}

// previewLogSmall shows log output for small/instant tables — just start + complete, no noise.
func previewLogSmall() {
	e := &logEmitter{nowFunc: func() time.Time { return logPreviewTime }, applyID: "apply-8487c4ad"}

	ts1 := &tableLogState{taskID: "task-a1b2c3d4", startedAt: logPreviewTime}
	ts2 := &tableLogState{taskID: "task-e5f6a7b8", startedAt: logPreviewTime}

	tbl1 := &apitypes.TableProgressResponse{TableName: "config", TaskID: "task-a1b2c3d4"}
	tbl2 := &apitypes.TableProgressResponse{TableName: "users", TaskID: "task-e5f6a7b8"}

	// Start
	e.emit(append(tableKVs("Table started", tbl1, ts1), "ddl", "ALTER TABLE `config` ADD COLUMN flags INT")...)
	e.emit(append(tableKVs("Table started", tbl2, ts2), "ddl", "CREATE TABLE `users` (id BIGINT PRIMARY KEY, email VARCHAR(255))")...)

	// Complete (fast — no heartbeats)
	ts1.startedAt = logPreviewTime.Add(-1 * time.Second)
	ts2.startedAt = logPreviewTime.Add(-2 * time.Second)
	e.emitTableStateChange(tbl1, state.Apply.Completed, ts1)
	e.emitTableStateChange(tbl2, state.Apply.Completed, ts2)

	// Summary
	tableStates := map[string]*tableLogState{
		"config": {status: state.Apply.Completed},
		"users":  {status: state.Apply.Completed},
	}
	e.emitApplySummary("completed", tableStates, logPreviewTime.Add(-2*time.Second), "")
}

// previewLogLarge shows log output for a large table with periodic heartbeats.
func previewLogLarge() {
	e := &logEmitter{nowFunc: func() time.Time { return logPreviewTime }, applyID: "apply-9f3e2d1c"}
	ts := &tableLogState{taskID: "task-orders-01", startedAt: logPreviewTime.Add(-4 * time.Minute)}

	tbl := &apitypes.TableProgressResponse{
		TableName: "orders",
		TaskID:    "task-orders-01",
	}

	// Start
	e.emit(append(tableKVs("Table started", tbl, ts), "ddl", "ALTER TABLE `orders` ADD INDEX idx_status (status)")...)

	// Heartbeats at 30s intervals
	heartbeats := []struct {
		pct    int32
		copied int64
		total  int64
		eta    string
	}{
		{12, 26543, 221193, "99450/221193 12.00% copyRows ETA 4m 10s"},
		{25, 55298, 221193, "99450/221193 25.00% copyRows ETA 3m 30s"},
		{45, 99537, 221193, "99450/221193 45.00% copyRows ETA 2m 15s"},
		{68, 150412, 221193, "150412/221193 68.00% copyRows ETA 1m 10s"},
		{89, 196861, 221193, "196861/221193 89.00% copyRows ETA 25s"},
	}
	for _, hb := range heartbeats {
		tbl.PercentComplete = hb.pct
		tbl.RowsCopied = hb.copied
		tbl.RowsTotal = hb.total
		tbl.ProgressDetail = hb.eta
		e.emitProgressHeartbeat(tbl, ts)
	}

	// Complete
	e.emitTableStateChange(tbl, state.Apply.Completed, ts)

	tableStates := map[string]*tableLogState{
		"orders": {status: state.Apply.Completed},
	}
	e.emitApplySummary("completed", tableStates, logPreviewTime.Add(-4*time.Minute), "")
}

// previewLogFailed shows log output when a table fails.
func previewLogFailed() {
	e := &logEmitter{nowFunc: func() time.Time { return logPreviewTime }, applyID: "apply-fail1234"}

	ts1 := &tableLogState{taskID: "task-users-01", startedAt: logPreviewTime.Add(-30 * time.Second)}
	ts2 := &tableLogState{taskID: "task-orders-01", startedAt: logPreviewTime.Add(-30 * time.Second)}

	tbl1 := &apitypes.TableProgressResponse{TableName: "users", TaskID: "task-users-01"}
	tbl2 := &apitypes.TableProgressResponse{TableName: "orders", TaskID: "task-orders-01"}

	// Both start
	e.emit(append(tableKVs("Table started", tbl1, ts1), "ddl", "ALTER TABLE `users` ADD COLUMN age INT")...)
	e.emit(append(tableKVs("Table started", tbl2, ts2), "ddl", "ALTER TABLE `orders` ADD INDEX idx_created (created_at)")...)

	// Users completes
	e.emitTableStateChange(tbl1, state.Apply.Completed, ts1)

	// Orders fails
	e.emitTableStateChange(tbl2, state.Apply.Failed, ts2)

	tableStates := map[string]*tableLogState{
		"users":  {status: state.Apply.Completed},
		"orders": {status: state.Apply.Failed},
	}
	e.emitApplySummary("failed", tableStates, logPreviewTime.Add(-30*time.Second),
		"schema change failed for orders: duplicate key in index idx_created")
}

// previewLogStopped shows log output when user stops an apply.
func previewLogStopped() {
	e := &logEmitter{nowFunc: func() time.Time { return logPreviewTime }, applyID: "apply-stop5678"}

	ts1 := &tableLogState{taskID: "task-users-01", startedAt: logPreviewTime.Add(-2 * time.Minute)}
	ts2 := &tableLogState{taskID: "task-orders-01", startedAt: logPreviewTime.Add(-2 * time.Minute)}

	tbl1 := &apitypes.TableProgressResponse{TableName: "users", TaskID: "task-users-01"}
	tbl2 := &apitypes.TableProgressResponse{
		TableName:       "orders",
		TaskID:          "task-orders-01",
		PercentComplete: 34,
	}

	e.emit(append(tableKVs("Table started", tbl1, ts1), "ddl", "ALTER TABLE `users` ADD COLUMN bio TEXT")...)
	e.emit(append(tableKVs("Table started", tbl2, ts2), "ddl", "ALTER TABLE `orders` ADD INDEX idx_total (total_cents)")...)

	// Users completes, orders gets stopped mid-copy
	e.emitTableStateChange(tbl1, state.Apply.Completed, ts1)
	e.emitTableStateChange(tbl2, state.Apply.Stopped, ts2)

	tableStates := map[string]*tableLogState{
		"users":  {status: state.Apply.Completed},
		"orders": {status: state.Apply.Stopped},
	}
	e.emitApplySummary("stopped", tableStates, logPreviewTime.Add(-2*time.Minute), "")
}

// previewLogMulti shows a mixed scenario with small instant tables and one large table.
func previewLogMulti() {
	e := &logEmitter{nowFunc: func() time.Time { return logPreviewTime }, applyID: "apply-multi9abc"}

	tsConfig := &tableLogState{taskID: "task-config-01", startedAt: logPreviewTime.Add(-3 * time.Minute)}
	tsUsers := &tableLogState{taskID: "task-users-01", startedAt: logPreviewTime.Add(-3 * time.Minute)}
	tsOrders := &tableLogState{taskID: "task-orders-01", startedAt: logPreviewTime.Add(-3 * time.Minute)}

	tblConfig := &apitypes.TableProgressResponse{TableName: "config", TaskID: "task-config-01"}
	tblUsers := &apitypes.TableProgressResponse{TableName: "users", TaskID: "task-users-01"}
	tblOrders := &apitypes.TableProgressResponse{TableName: "orders", TaskID: "task-orders-01"}

	// All start
	e.emit(append(tableKVs("Table started", tblConfig, tsConfig), "ddl", "ALTER TABLE `config` ADD COLUMN flags INT")...)
	e.emit(append(tableKVs("Table started", tblUsers, tsUsers), "ddl", "ALTER TABLE `users` DROP INDEX idx_old")...)
	e.emit(append(tableKVs("Table started", tblOrders, tsOrders), "ddl", "ALTER TABLE `orders` ADD INDEX idx_status (status)")...)

	// Small tables finish instantly
	tsConfig.startedAt = logPreviewTime.Add(-1 * time.Second)
	e.emitTableStateChange(tblConfig, state.Apply.Completed, tsConfig)
	tsUsers.startedAt = logPreviewTime.Add(-2 * time.Second)
	e.emitTableStateChange(tblUsers, state.Apply.Completed, tsUsers)

	// Large table has heartbeats
	tblOrders.PercentComplete = 30
	tblOrders.RowsCopied = 66357
	tblOrders.RowsTotal = 221193
	tblOrders.ProgressDetail = "66357/221193 30.00% copyRows ETA 2m 30s"
	e.emitProgressHeartbeat(tblOrders, tsOrders)

	tblOrders.PercentComplete = 65
	tblOrders.RowsCopied = 143776
	tblOrders.RowsTotal = 221193
	tblOrders.ProgressDetail = "143776/221193 65.00% copyRows ETA 1m 5s"
	e.emitProgressHeartbeat(tblOrders, tsOrders)

	e.emitTableStateChange(tblOrders, state.Apply.Completed, tsOrders)

	tableStates := map[string]*tableLogState{
		"config": {status: state.Apply.Completed},
		"users":  {status: state.Apply.Completed},
		"orders": {status: state.Apply.Completed},
	}
	e.emitApplySummary("completed", tableStates, logPreviewTime.Add(-3*time.Minute), "")
}

// previewLogCutover shows log output for the cutover flow.
func previewLogCutover() {
	e := &logEmitter{nowFunc: func() time.Time { return logPreviewTime }, applyID: "apply-cut7890"}

	ts := &tableLogState{taskID: "task-orders-01", startedAt: logPreviewTime.Add(-5 * time.Minute)}
	tbl := &apitypes.TableProgressResponse{TableName: "orders", TaskID: "task-orders-01"}

	e.emit(append(tableKVs("Table started", tbl, ts), "ddl", "ALTER TABLE `orders` ADD INDEX idx_status (status)")...)

	// Heartbeat
	tbl.PercentComplete = 50
	tbl.RowsCopied = 110000
	tbl.RowsTotal = 221193
	tbl.ProgressDetail = "110000/221193 50.00% copyRows ETA 2m 30s"
	e.emitProgressHeartbeat(tbl, ts)

	// Waiting for cutover
	e.emitTableStateChange(tbl, state.Apply.WaitingForCutover, ts)
	e.emit("msg", "Waiting for cutover")

	// Cutting over
	e.emitTableStateChange(tbl, state.Apply.CuttingOver, ts)
	e.emit("msg", "Cutting over")

	// Complete
	e.emitTableStateChange(tbl, state.Apply.Completed, ts)
	tableStates := map[string]*tableLogState{
		"orders": {status: state.Apply.Completed},
	}
	e.emitApplySummary("completed", tableStates, logPreviewTime.Add(-5*time.Minute), "")
}

// previewLogDetailed shows all available fields for debugging.
func previewLogDetailed() {
	e := &logEmitter{nowFunc: func() time.Time { return logPreviewTime }, applyID: "apply-detail42"}

	ts := &tableLogState{taskID: "task-abc12345", startedAt: logPreviewTime.Add(-90 * time.Second)}
	tbl := &apitypes.TableProgressResponse{
		TableName:       "orders",
		TaskID:          "task-abc12345",
		PercentComplete: 67,
		RowsCopied:      148102,
		RowsTotal:       221193,
		ETASeconds:      45,
		ProgressDetail:  "148102/221193 67.00% copyRows ETA 45s",
	}

	e.emit(append(tableKVs("Table started", tbl, ts),
		"ddl", "ALTER TABLE `orders` ADD INDEX idx_status (status)")...)
	e.emitProgressHeartbeat(tbl, ts)
	e.emitTableStateChange(tbl, state.Apply.Completed, ts)

	tableStates := map[string]*tableLogState{
		"orders": {status: state.Apply.Completed},
	}
	e.emitApplySummary("completed", tableStates, logPreviewTime.Add(-90*time.Second), "")
}
