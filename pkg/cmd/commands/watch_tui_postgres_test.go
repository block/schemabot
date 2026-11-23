package commands

import (
	"testing"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// The watcher offers a PostgreSQL apply only the command its engine accepts:
// c cancels a running concurrent index build, and plain DDL, which commits or
// fails on its own, offers no key and no command after detaching.
func TestTUIPostgresOffersOnlyTheCommandItsEngineAccepts(t *testing.T) {
	build := apitypes.BuildWork{Operation: "concurrent-index-build", ServerPhase: "building index: scanning table"}
	tests := []struct {
		name         string
		work         apitypes.BuildWork
		stepsTotal   int
		keyAvailable bool
		footer       string
		detached     []string
		absent       []string
	}{
		{
			name: "index build cancels", work: build, stepsTotal: 1, keyAvailable: true,
			footer:   "ESC detach • c cancel",
			detached: []string{"To cancel:   schemabot cancel apply-a1b2c3d4e5f6 -e staging"},
			absent:   []string{"To stop:"},
		},
		{
			name: "plain DDL offers nothing", stepsTotal: 1,
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
				engine:      storage.EnginePostgres,
				buildWork:   tt.work,
				stepsTotal:  tt.stepsTotal,
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
		})
	}
}
