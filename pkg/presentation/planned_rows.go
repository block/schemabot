package presentation

import "github.com/block/schemabot/pkg/state"

// CopyStillAhead reports whether a target whose engine has reported no row
// total will still copy the table, so the plan's row estimate may stand in for
// the rows it is about to report. A target that is queued, waiting for its
// deploy, copying or recovering has not reported its copy yet. A target past
// its copy (catching up, checksumming, waiting for or in its cutover,
// reverting) copied a table that had no rows, so its zero is the engine's
// answer and an estimate would count work that is already done.
func CopyStillAhead(status string) bool {
	return state.IsState(status,
		state.Task.Pending,
		state.Task.WaitingForDeploy,
		state.Task.Running,
		state.Task.Recovering)
}
