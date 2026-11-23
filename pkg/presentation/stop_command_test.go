package presentation

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
)

// Every surface offers the command StopCommand names, so it must name only a
// command the engine accepts for the work in flight: PostgreSQL cancels a
// lone concurrent index build and offers nothing for any other statement.
func TestStopCommand(t *testing.T) {
	build := apitypes.BuildWork{Operation: "concurrent-index-build", ServerPhase: "building index: scanning table"}
	tests := []struct {
		name       string
		engine     string
		work       apitypes.BuildWork
		stepsTotal int
		want       string
	}{
		{name: "spirit stops", engine: "spirit", want: StopCommandStop},
		{name: "an unnamed engine stops", engine: "", want: StopCommandStop},
		{name: "planetscale cancels", engine: storage.EnginePlanetScale, want: StopCommandCancel},
		{name: "planetscale matches case-insensitively", engine: "PlanetScale", want: StopCommandCancel},
		{name: "postgres cancels its only statement's index build", engine: storage.EnginePostgres, work: build, stepsTotal: 1, want: StopCommandCancel},
		{name: "postgres cancels an index build before the step total arrives", engine: storage.EnginePostgres, work: build, want: StopCommandCancel},
		{name: "postgres offers nothing for an index build among several statements", engine: storage.EnginePostgres, work: build, stepsTotal: 3, want: StopCommandNone},
		{name: "postgres offers nothing for plain DDL", engine: storage.EnginePostgres, stepsTotal: 1, want: StopCommandNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, StopCommand(tt.engine, tt.work, tt.stepsTotal))
		})
	}
}
