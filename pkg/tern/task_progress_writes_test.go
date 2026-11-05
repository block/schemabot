package tern

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// pollsThatWrite runs a drive's polls at the default poll interval, calling
// change before each one, and returns the polls whose write was due. Every due
// write lands.
func pollsThatWrite(t *testing.T, polls int, task *storage.Task, change func(poll int, task *storage.Task)) []int {
	t.Helper()
	gate := taskWriteGate{policy: progressWritePolicy{cap: defaultProgressWriteCap, floor: defaultProgressWriteFloor}}
	start := time.Now()
	var wrote []int
	for poll := range polls {
		change(poll, task)
		now := start.Add(time.Duration(poll) * defaultTaskPollInterval)
		row := taskRowOf(task, task.State)
		if gate.due(now, row) {
			gate.landed(now, row)
			wrote = append(wrote, poll)
		}
	}
	return wrote
}

func copyingTask() *storage.Task {
	started := time.Now()
	return &storage.Task{TaskIdentifier: "task-1", State: state.Task.Running, StartedAt: &started, RowsTotal: 1_000_000}
}

// A copy moves its row count on every poll. The row is written on the first
// poll and then on every third, so it is never older than the cap and every
// two-second CLI refresh finds newer figures than the last one showed.
func TestTaskWriteGate_ProgressIsWrittenOncePerCap(t *testing.T) {
	wrote := pollsThatWrite(t, 10, copyingTask(), func(poll int, task *storage.Task) {
		task.RowsCopied = int64(poll+1) * 1000
	})
	assert.Equal(t, []int{0, 3, 6, 9}, wrote)
}

// A change to what the task is doing is written on the poll that sees it,
// however recently its progress was written.
func TestTaskWriteGate_StatusChangesAreWrittenAtOnce(t *testing.T) {
	tests := []struct {
		name   string
		change func(task *storage.Task)
	}{
		{"state", func(task *storage.Task) { task.State = state.Task.WaitingForCutover }},
		{"throttle", func(task *storage.Task) { task.Throttled, task.ThrottleReason = true, "replica-lag 5s >= 2s" }},
		{"error message", func(task *storage.Task) { task.ErrorMessage = "duplicate entry" }},
		{"completion stamp", func(task *storage.Task) { now := time.Now(); task.CompletedAt = &now }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wrote := pollsThatWrite(t, 3, copyingTask(), func(poll int, task *storage.Task) {
				task.RowsCopied = int64(poll+1) * 1000
				if poll == 1 {
					tt.change(task)
				}
			})
			assert.Equal(t, []int{0, 1}, wrote, "the change on poll 1 is written on poll 1")
		})
	}
}

// A task held at its cutover reports the same row on every poll. It is still
// written once per floor, which keeps the drive's liveness signal fresh well
// inside the window the operator cancels a quiet drive at.
func TestTaskWriteGate_UnchangedRowIsWrittenOncePerFloor(t *testing.T) {
	task := copyingTask()
	task.State = state.Task.WaitingForCutover
	task.RowsCopied = task.RowsTotal
	pollsPerFloor := int(defaultProgressWriteFloor / defaultTaskPollInterval)
	wrote := pollsThatWrite(t, 2*pollsPerFloor+1, task, func(int, *storage.Task) {})
	assert.Equal(t, []int{0, pollsPerFloor, 2 * pollsPerFloor}, wrote)
	assert.Less(t, defaultProgressWriteFloor, storage.ApplyDriveStallAfter/5,
		"the floor stays far inside the stall window")
}

// A write that storage refused is not recorded, so the next poll is due and
// retries it rather than waiting out another cap.
func TestTaskWriteGate_RefusedWriteIsRetriedOnTheNextPoll(t *testing.T) {
	gate := taskWriteGate{policy: progressWritePolicy{cap: defaultProgressWriteCap, floor: defaultProgressWriteFloor}}
	task := copyingTask()
	start := time.Now()
	first := taskRowOf(task, task.State)
	gate.landed(start, first)

	task.RowsCopied = 3000
	capReached := start.Add(defaultProgressWriteCap)
	assert.True(t, gate.due(capReached, taskRowOf(task, task.State)), "the poll at the cap is due")
	// That write is refused, so nothing is recorded.
	task.RowsCopied = 4000
	assert.True(t, gate.due(capReached.Add(defaultTaskPollInterval), taskRowOf(task, task.State)),
		"the next poll is due again")
}

// A task at rest has its ETA and throttle state cleared when it is written, so
// a resting row whose engine still reports them compares equal to what was
// stored and is not rewritten on every cap.
func TestTaskRowOf_RestingTaskMatchesItsStoredRow(t *testing.T) {
	task := copyingTask()
	task.State = state.Task.Stopped
	task.ETASeconds = 120
	task.Throttled, task.ThrottleReason = true, "replica-lag 5s >= 2s"
	stored := *task
	stored.ETASeconds, stored.Throttled, stored.ThrottleReason = 0, false, ""
	assert.Equal(t, taskRowOf(&stored, stored.State), taskRowOf(task, task.State))
}

// The cap leaves a poll interval of room under the CLI watch's refresh, so a
// write lands between any two refreshes even when the two clocks drift.
func TestProgressWriteCap_FitsInsideTheWatchRefresh(t *testing.T) {
	const watchRefresh = 2 * time.Second
	assert.LessOrEqual(t, defaultProgressWriteCap+defaultTaskPollInterval, watchRefresh)
	assert.Zero(t, defaultProgressWriteCap%defaultTaskPollInterval,
		"the cap is a whole number of poll intervals, so it names the poll that writes")
}
