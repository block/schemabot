package state

import (
	"strings"

	vitessstatus "vitess.io/vitess/go/vt/schema"
)

// Vitess holds Vitess OnlineDDL status constants.
// Values match vitess.io/vitess/go/vt/schema.OnlineDDLStatus.
var Vitess = struct {
	Requested       string
	Cancelled       string
	Queued          string
	Ready           string
	Running         string
	Complete        string
	Failed          string
	ReadyToComplete string
}{
	Requested: string(vitessstatus.OnlineDDLStatusRequested),
	Cancelled: string(vitessstatus.OnlineDDLStatusCancelled),
	Queued:    string(vitessstatus.OnlineDDLStatusQueued),
	Ready:     string(vitessstatus.OnlineDDLStatusReady),
	Running:   string(vitessstatus.OnlineDDLStatusRunning),
	Complete:  string(vitessstatus.OnlineDDLStatusComplete),
	Failed:    string(vitessstatus.OnlineDDLStatusFailed),
	// ReadyToComplete is a derived state, not a Vitess OnlineDDLStatus enum value.
	// SchemaBot synthesizes it when a migration is running with ready_to_complete=1
	// in SHOW VITESS_MIGRATIONS output.
	ReadyToComplete: "ready_to_complete",
}

// IsTerminalVitessState reports whether a Vitess OnlineDDL status is terminal —
// the schema change has reached an end state and will make no further progress.
// Comparison is case-insensitive so it tolerates whatever casing
// SHOW VITESS_MIGRATIONS reports.
func IsTerminalVitessState(s string) bool {
	switch strings.ToLower(s) {
	case Vitess.Complete, Vitess.Failed, Vitess.Cancelled:
		return true
	default:
		return false
	}
}
