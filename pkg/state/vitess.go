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
	// SchemaBot synthesizes it when a schema change in any non-terminal status
	// reports ready_to_complete=1 in SHOW VITESS_MIGRATIONS output — typically
	// running, but also queued/requested/ready for immediate operations
	// (CREATE/DROP TABLE) that set the flag before being scheduled. Terminal
	// statuses (complete/failed/cancelled) are never promoted, even when the
	// flag remains set.
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

// EffectiveVitessState resolves a Vitess status using its authoritative
// ready_to_complete cutover-readiness signal. The flag can be set while the
// status still reads running during a brief race, or queued, requested, or
// ready for immediate operations such as CREATE or DROP TABLE. Terminal
// statuses always win because Vitess can leave the flag set after cancel or
// failure. The returned status is canonicalized to lower case so callers can
// compare it against the Vitess constants regardless of how
// SHOW VITESS_MIGRATIONS cased it.
func EffectiveVitessState(status string, readyToComplete bool) string {
	if readyToComplete && !IsTerminalVitessState(status) {
		return Vitess.ReadyToComplete
	}
	return strings.ToLower(status)
}

// IsKnownVitessState reports whether a status is one of the Vitess OnlineDDL
// statuses SchemaBot recognizes, including the synthesized ready_to_complete
// state. Comparison is case-insensitive so it tolerates whatever casing
// SHOW VITESS_MIGRATIONS reports. Callers use this to surface genuinely
// unrecognized statuses (a new or malformed Vitess value) before resolving
// them through EffectiveVitessState.
func IsKnownVitessState(s string) bool {
	switch strings.ToLower(s) {
	case Vitess.Requested, Vitess.Cancelled, Vitess.Queued, Vitess.Ready,
		Vitess.Running, Vitess.Complete, Vitess.Failed, Vitess.ReadyToComplete:
		return true
	default:
		return false
	}
}
