package state

// ApplyOperation holds the state-machine constants for one child row in the
// apply_operations table — the per-(deployment, target) slice of a
// multi-deployment apply, and the unit of work the operator claims.
//
// The state vocabulary deliberately mirrors Apply exactly. Same canonical
// values, same PlanetScale-specific lifecycle phases. Two reasons:
//
//  1. An operator must be able to cutover, stop, revert, or remediate a
//     single deployment independently — none of which a narrower
//     pending/in_progress/completed/failed enum supports.
//  2. applies.state is derived from the child rows' states via the existing
//     DeriveApplyState() (see apply.go), so identical vocabulary lets that
//     aggregator be reused verbatim, rather than re-implementing it for a
//     second state model.
//
// See DeriveApplyState in apply.go for the priority-based aggregation rules
// that collapse child operation states into the parent apply state.
var ApplyOperation = Apply

// IsApplyOperationTerminal reports whether the given state is terminal
// (no further transitions expected). Delegates to the shared Apply
// classification so terminal-ness stays consistent across both layers.
func IsApplyOperationTerminal(s string) bool {
	return IsTerminalApplyState(s)
}
