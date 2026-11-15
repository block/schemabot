package commands

import (
	"context"
	"reflect"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
)

const exitTestApplyID = "apply-7c2e"

// watchAt is an interactive watch that has polled the apply once and seen it in
// the given state.
func watchAt(t *testing.T, applyState string) WatchModel {
	t.Helper()
	m := NewWatchModel("http://localhost:8080", "", "staging", false)
	updated, _ := m.Update(progressMsg{
		state:   applyState,
		applyID: exitTestApplyID,
		tables:  []templates.TableProgress{{TableName: "orders", Status: state.Task.Running}},
	})
	return updated.(WatchModel)
}

func press(t *testing.T, m WatchModel, key tea.KeyMsg) WatchModel {
	t.Helper()
	updated, _ := m.Update(key)
	return updated.(WatchModel)
}

// terminalApplyStates lists every apply state pkg/state calls terminal, read
// from the registry so a state added there is covered without a change here.
func terminalApplyStates(t *testing.T) []string {
	t.Helper()
	var terminal []string
	for _, field := range reflect.ValueOf(state.Apply).Fields() {
		if s := field.String(); state.IsTerminalApplyState(s) {
			terminal = append(terminal, s)
		}
	}
	require.GreaterOrEqual(t, len(terminal), 5, "completed, failed, stopped, cancelled and reverted are all terminal")
	return terminal
}

// An apply ends in the same state under the interactive watch and under the log
// watch. Both exit 0 for a completed apply and 1 for every other end, so a
// script gating on the exit status sees the same answer whichever watch ran.
func TestWatchExit_EveryEndMatchesTheLogWatcher(t *testing.T) {
	for _, applyState := range terminalApplyStates(t) {
		t.Run(applyState, func(t *testing.T) {
			interactive := watchAt(t, applyState).exitError()
			log := terminalWatchExit(&apitypes.ProgressResponse{ApplyID: exitTestApplyID, Environment: "staging", State: applyState})

			assert.Equal(t, ExitCodeFor(log), ExitCodeFor(interactive))
			assert.Equal(t, log == nil, interactive == nil, "success is the same answer on both watches")
			if applyState == state.Apply.Completed {
				assert.NoError(t, interactive)
			} else {
				assert.Equal(t, 1, ExitCodeFor(interactive))
			}
		})
	}

	t.Run("no active schema change", func(t *testing.T) {
		interactive := watchAt(t, state.NoActiveChange).exitError()
		log := noActiveChangeError(exitTestApplyID, "staging")

		assert.Equal(t, 1, ExitCodeFor(log))
		assert.Equal(t, 1, ExitCodeFor(interactive))
	})
}

// An operator leaves the interactive watch while the apply is still copying.
// That is not an outcome of the apply, so it exits with the status a Ctrl-C ends
// a log watch with: not 0, which a script reads as the schema change being on
// the target, and not 1, which it reads as the apply having failed. A poll that
// failed or a stop the server refused is still on screen when the operator
// leaves, and does not turn the exit into a failure.
func TestWatchExit_LeavingTheWatchExitsAsInterrupted(t *testing.T) {
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	logWatchCtrlC := ExitCodeFor(interrupted(cancelled))
	require.Equal(t, ExitInterrupted, logWatchCtrlC)

	esc := tea.KeyMsg{Type: tea.KeyEsc}
	failedPoll := progressMsg{failed: true, retryable: true, errorMsg: "connection refused"}

	for _, tc := range []struct {
		name  string
		leave func(t *testing.T, running WatchModel) WatchModel
	}{
		{"Esc", func(t *testing.T, m WatchModel) WatchModel { return press(t, m, esc) }},
		{"Ctrl-C", func(t *testing.T, m WatchModel) WatchModel { return press(t, m, tea.KeyMsg{Type: tea.KeyCtrlC}) }},
		{"q", func(t *testing.T, m WatchModel) WatchModel {
			return press(t, m, tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}})
		}},
		{"SIGTERM, which quits the program without a key", func(t *testing.T, m WatchModel) WatchModel { return m }},
		{"Esc after a failed poll", func(t *testing.T, m WatchModel) WatchModel {
			updated, _ := m.Update(failedPoll)
			failing := updated.(WatchModel)
			require.Equal(t, "connection refused", failing.errorMsg)
			return press(t, failing, esc)
		}},
		{"Esc after a refused stop", func(t *testing.T, m WatchModel) WatchModel {
			updated, _ := m.Update(stopResultMsg{err: assert.AnError})
			refused := updated.(WatchModel)
			require.Equal(t, assert.AnError.Error(), refused.errorMsg)
			return press(t, refused, esc)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			left := tc.leave(t, watchAt(t, state.Apply.Running))

			err := left.exitError()
			require.ErrorIs(t, err, ErrSilent, "the detach view already says how to reattach")
			assert.Equal(t, logWatchCtrlC, ExitCodeFor(err))
		})
	}
}

// An operator leaves the interactive watch before its first poll has answered,
// so the watch knows nothing about the apply yet. That is a detach like any
// other, not the apply having failed: the exit is 130, whether the first poll
// is still in flight or has failed and is being retried.
func TestWatchExit_LeavingBeforeTheFirstPollExitsAsInterrupted(t *testing.T) {
	fresh := func() WatchModel {
		m := NewWatchModel("http://localhost:8080", "", "staging", false)
		m.applyID = exitTestApplyID
		return m
	}

	t.Run("before the first poll answers", func(t *testing.T) {
		left := press(t, fresh(), tea.KeyMsg{Type: tea.KeyEsc})

		err := left.exitError()
		require.ErrorIs(t, err, ErrSilent)
		assert.Equal(t, ExitInterrupted, ExitCodeFor(err))
	})

	t.Run("while the first polls fail and are retried", func(t *testing.T) {
		updated, quit := fresh().Update(progressMsg{failed: true, retryable: true, errorMsg: "connection refused"})
		require.Nil(t, quit, "a failed first poll is retried, not fatal")
		left := press(t, updated.(WatchModel), tea.KeyMsg{Type: tea.KeyEsc})

		err := left.exitError()
		require.ErrorIs(t, err, ErrSilent)
		assert.Equal(t, ExitInterrupted, ExitCodeFor(err))
	})

	// An interrupt signal reaches the program as a signal rather than a
	// keystroke when stdin is not a terminal, and ends the run with an error
	// instead of a final model. It is still the operator leaving.
	t.Run("SIGINT delivered as a signal", func(t *testing.T) {
		m := watchAt(t, state.Apply.Running)

		err := watchExit(m, m, tea.ErrInterrupted)
		require.ErrorIs(t, err, ErrSilent)
		assert.Equal(t, ExitInterrupted, ExitCodeFor(err))
	})
}

// A watch that cannot follow the apply fails with status 1: the server refused
// the apply permanently, or polls kept failing until the watch gave up. Neither
// is the operator leaving, and neither says anything about how the apply ended.
func TestWatchExit_FailingToFollowTheApplyExitsOne(t *testing.T) {
	t.Run("permanent error", func(t *testing.T) {
		m := watchAt(t, state.Apply.Running)
		updated, cmd := m.Update(progressMsg{failed: true, errorMsg: "apply not found"})

		require.NotNil(t, cmd)
		assert.IsType(t, tea.QuitMsg{}, cmd())
		err := updated.(WatchModel).exitError()
		require.ErrorIs(t, err, ErrSilent)
		assert.Equal(t, 1, ExitCodeFor(err))
	})

	t.Run("polls kept failing", func(t *testing.T) {
		failedPoll := progressMsg{failed: true, retryable: true, errorMsg: "connection refused"}
		m := watchAt(t, state.Apply.Running)
		for range maxConsecutiveProgressFailures - 1 {
			updated, quit := m.Update(failedPoll)
			m = updated.(WatchModel)
			assert.Nil(t, quit, "the watch keeps polling until it has failed this many times in a row")
		}
		assert.Equal(t, ExitInterrupted, ExitCodeFor(m.exitError()), "still retrying, so leaving now is a detach")

		updated, quit := m.Update(failedPoll)
		require.NotNil(t, quit)
		assert.IsType(t, tea.QuitMsg{}, quit())
		err := updated.(WatchModel).exitError()
		require.ErrorIs(t, err, ErrSilent)
		assert.Equal(t, 1, ExitCodeFor(err))
	})
}
