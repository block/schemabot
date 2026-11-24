package commands

import (
	"testing"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
)

// The watcher offers a PostgreSQL apply only the command its engine accepts:
// c cancels a running concurrent index build that is the apply's only
// operation, and s, which asks for a pause the engine cannot make, never
// cancels it (CO-8). Plain DDL, which commits or fails on its own, offers no
// key and no command after detaching, and so does a rollout, whose apply-wide
// build work speaks for one of its operations only. The engine is named as
// the progress API reports it, since that is what the watcher reads.
func TestTUIPostgresOffersOnlyTheCommandItsEngineAccepts(t *testing.T) {
	build := apitypes.BuildWork{Operation: "concurrent-index-build", ServerPhase: "building index: scanning table"}
	tests := []struct {
		name         string
		work         apitypes.BuildWork
		stepsTotal   int
		operations   []templates.ProgressOperation
		keyAvailable bool
		cancelKey    bool
		footer       string
		detached     []string
		absent       []string
	}{
		{
			name: "index build cancels", work: build, stepsTotal: 1, keyAvailable: true, cancelKey: true,
			footer:   "ESC detach • c cancel",
			detached: []string{"To cancel:   schemabot cancel apply-a1b2c3d4e5f6 -e staging"},
			absent:   []string{"To stop:"},
		},
		{
			name: "plain DDL offers nothing", stepsTotal: 1,
			footer: "ESC detach",
			absent: []string{"To stop:", "To cancel:", "s stop", "c cancel"},
		},
		{
			name: "an index build in a rollout offers nothing", work: build, stepsTotal: 1,
			operations: []templates.ProgressOperation{
				{Deployment: "primary", Target: "orders-001", State: state.Apply.Running},
				{Deployment: "primary", Target: "orders-002", State: state.Apply.Running},
			},
			footer: "ESC detach",
			absent: []string{"To stop:", "To cancel:", "s stop", "c cancel"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := WatchModel{
				state:       state.Apply.Running,
				applyID:     "apply-a1b2c3d4e5f6",
				environment: "staging",
				tables:      []templates.TableProgress{{TableName: "users", Status: state.Task.Running}},
				initialized: true,
				spinner:     spinner.New(),
				engine:      "PostgreSQL",
				buildWork:   tt.work,
				stepsTotal:  tt.stepsTotal,
				operations:  tt.operations,
			}

			assert.Equal(t, tt.keyAvailable, m.stopKeyAvailable())
			assert.Contains(t, m.formatFooter(), tt.footer)
			detached := m.detachedView()
			for _, want := range tt.detached {
				assert.Contains(t, detached, want)
			}
			for _, unwanted := range tt.absent {
				assert.NotContains(t, detached+m.formatFooter(), unwanted)
			}

			afterStop, stopCmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
			assert.False(t, afterStop.(WatchModel).stopTriggered, "s asks for a pause, so it never cancels")
			assert.Nil(t, stopCmd)
			afterCancel, cancelCmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'c'}})
			assert.Equal(t, tt.cancelKey, afterCancel.(WatchModel).stopTriggered)
			assert.Equal(t, tt.cancelKey, cancelCmd != nil)
		})
	}
}
