package presentation

import (
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
)

// The commands that end an apply's in-flight work. StopCommandNone means the
// engine cannot end the work in flight, so surfaces offer no command.
const (
	StopCommandStop   = "stop"
	StopCommandCancel = "cancel"
	StopCommandNone   = ""
)

// StopCommand names the command an operator runs to end an apply's in-flight
// work on its engine. Every surface that offers stop or cancel asks this, so
// the PR comment and the CLI never offer a command the engine refuses.
//
//   - PlanetScale cancels its deploy request; it has no stop.
//   - PostgreSQL has no resumable midpoint, so it never stops. It can cancel
//     a concurrent index build that is the apply's only statement. Any other
//     statement commits or fails on its own, so there is nothing to offer.
//   - Every other engine stops, and the apply can start again from there.
//
// engine is either the name storage records or the name the progress API
// reports, since the PR comment reads the first and the CLI the second.
func StopCommand(engine string, work apitypes.BuildWork, stepsTotal int) string {
	switch {
	case strings.EqualFold(engine, storage.EnginePlanetScale):
		return StopCommandCancel
	case isPostgresEngine(engine):
		if work.IsConcurrentIndexBuild() && stepsTotal <= 1 {
			return StopCommandCancel
		}
		return StopCommandNone
	default:
		return StopCommandStop
	}
}

// progressAPIPostgresEngine is the name the progress API reports for the
// engine storage records as storage.EnginePostgres.
const progressAPIPostgresEngine = "PostgreSQL"

func isPostgresEngine(engine string) bool {
	return strings.EqualFold(engine, storage.EnginePostgres) || strings.EqualFold(engine, progressAPIPostgresEngine)
}
