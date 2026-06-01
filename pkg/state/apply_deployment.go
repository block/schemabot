package state

// ApplyDeployment holds the state-machine constants for one child row in the
// apply_deployments table — i.e., the per-deployment slice of a multi-deployment
// apply.
//
// The state machine is deliberately narrower than Apply's: a child row tracks
// the lifecycle of a single deployment target (one Tern endpoint, one lock),
// so the engine-specific intermediate states (waiting_for_deploy,
// waiting_for_cutover, etc.) do not apply at this layer. Those still live on
// the apply / task it spawned.
//
//	pending     → row created, scheduler has not picked it up yet
//	in_progress → scheduler has claimed it, engine is executing
//	completed   → terminal: the deployment finished successfully
//	failed      → terminal: the deployment failed (see error_message)
var ApplyDeployment = struct {
	Pending    string
	InProgress string
	Completed  string
	Failed     string
}{
	Pending:    "pending",
	InProgress: "in_progress",
	Completed:  "completed",
	Failed:     "failed",
}

// IsApplyDeploymentTerminal reports whether the given state is terminal
// (no further transitions expected).
func IsApplyDeploymentTerminal(s string) bool {
	return s == ApplyDeployment.Completed || s == ApplyDeployment.Failed
}
