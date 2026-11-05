package tern

import (
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

const (
	// defaultProgressWriteCap is the shortest time between two writes of a task
	// row that carry only new progress figures. Every reader of progress serves
	// it from the stored row, and the fastest of them, the CLI watch, refreshes
	// every two seconds. A cap below that refresh, with a poll interval of room
	// to spare, puts newer figures in front of every refresh.
	defaultProgressWriteCap = 1500 * time.Millisecond

	// defaultProgressWriteFloor is the longest a drive leaves a task row
	// unwritten while nothing on it changes. The operator reads tasks.updated_at
	// as the drive's liveness signal, so the floor stays far inside
	// storage.ApplyDriveStallAfter.
	defaultProgressWriteFloor = 30 * time.Second
)

// progressWritePolicy says when a progress poll writes a task row. A change to
// what the task is doing is written at once, because an operator acts on it.
// New progress figures are written at most once per cap, because they change
// on nearly every poll and every driver writes them to one shared storage
// writer. A row with nothing new is still written once per floor, so its drive
// keeps reporting that it is alive.
type progressWritePolicy struct {
	cap   time.Duration
	floor time.Duration
}

// progressWritePolicy returns the policy a drive applies to its task writes.
func (c *LocalClient) progressWritePolicy() progressWritePolicy {
	policy := progressWritePolicy{cap: defaultProgressWriteCap, floor: defaultProgressWriteFloor}
	if c.progressWriteCapOverride > 0 {
		policy.cap = c.progressWriteCapOverride
	}
	if c.progressWriteFloorOverride > 0 {
		policy.floor = c.progressWriteFloorOverride
	}
	return policy
}

// taskStatus is the part of a task row an operator acts on: what the task is
// doing, whether the engine is holding it back, and how it ended.
type taskStatus struct {
	state          string
	throttled      bool
	throttleReason string
	errorMessage   string
	isInstant      bool
	started        bool
	completed      bool
}

// taskFigures is the part of a task row that measures how far it has got.
type taskFigures struct {
	rowsCopied          int64
	rowsTotal           int64
	progressPercent     int
	etaSeconds          int
	checksumRowsChecked int64
	checksumRowsTotal   int64
}

// taskRow is what a write of a task row in the given state would store.
type taskRow struct {
	status  taskStatus
	figures taskFigures
}

// taskRowOf projects task onto the row a write in taskState would store. It
// clears the ETA and throttle state for a task at rest the way
// persistTaskStateTransition does, so a resting task's row compares equal to
// what was stored for it.
func taskRowOf(task *storage.Task, taskState string) taskRow {
	row := taskRow{
		status: taskStatus{
			state:          taskState,
			throttled:      task.Throttled,
			throttleReason: task.ThrottleReason,
			errorMessage:   task.ErrorMessage,
			isInstant:      task.IsInstant,
			started:        task.StartedAt != nil,
			completed:      task.CompletedAt != nil,
		},
		figures: taskFigures{
			rowsCopied:          task.RowsCopied,
			rowsTotal:           task.RowsTotal,
			progressPercent:     task.ProgressPercent,
			etaSeconds:          task.ETASeconds,
			checksumRowsChecked: task.ChecksumRowsChecked,
			checksumRowsTotal:   task.ChecksumRowsTotal,
		},
	}
	if !state.IsInFlightTaskState(taskState) {
		row.status.throttled = false
		row.status.throttleReason = ""
		row.figures.etaSeconds = 0
	}
	return row
}

// taskWriteGate remembers the last row a drive stored for one task, and the
// poll that stored it.
type taskWriteGate struct {
	policy   progressWritePolicy
	stored   taskRow
	storedAt time.Time
	written  bool
}

// due reports whether the poll at now should write next. The time since the
// last write is measured between polls rather than from when the write
// returned, so a cap that is a whole number of poll intervals writes on
// exactly that poll and not the one after.
func (g *taskWriteGate) due(now time.Time, next taskRow) bool {
	if !g.written || next.status != g.stored.status {
		return true
	}
	since := now.Sub(g.storedAt)
	if next.figures != g.stored.figures {
		return since >= g.policy.cap
	}
	return since >= g.policy.floor
}

// landed records a write that storage accepted. A write that was refused is
// not recorded, so the next poll is due again and retries it.
func (g *taskWriteGate) landed(now time.Time, row taskRow) {
	g.stored = row
	g.storedAt = now
	g.written = true
}

// taskWrites holds a write gate for each task a grouped drive polls.
type taskWrites struct {
	policy progressWritePolicy
	byTask map[string]*taskWriteGate
}

func newTaskWrites(policy progressWritePolicy) *taskWrites {
	return &taskWrites{policy: policy, byTask: map[string]*taskWriteGate{}}
}

// gate returns the write gate for task, creating it on the task's first poll.
func (w *taskWrites) gate(task *storage.Task) *taskWriteGate {
	g, ok := w.byTask[task.TaskIdentifier]
	if !ok {
		g = &taskWriteGate{policy: w.policy}
		w.byTask[task.TaskIdentifier] = g
	}
	return g
}
