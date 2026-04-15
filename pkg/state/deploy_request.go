package state

// DeployRequest holds PlanetScale deploy request state constants.
// These map to the deployment_state values returned by the PlanetScale API.
var DeployRequest = struct {
	Pending                 string
	Ready                   string
	NoChanges               string
	Submitting              string
	Queued                  string
	InProgress              string
	PendingCutover          string
	InProgressCutover       string
	InProgressVSchema       string
	CompletePendingRevert   string
	Complete                string
	CompleteError           string
	InProgressCancel        string
	CompleteCancel          string
	InProgressRevert        string
	InProgressRevertVSchema string
	CompleteRevert          string
	CompleteRevertError     string
	Cancelled               string
	Error                   string
	Failed                  string
}{
	Pending:                 "pending",
	Ready:                   "ready",
	NoChanges:               "no_changes",
	Submitting:              "submitting",
	Queued:                  "queued",
	InProgress:              "in_progress",
	PendingCutover:          "pending_cutover",
	InProgressCutover:       "in_progress_cutover",
	InProgressVSchema:       "in_progress_vschema",
	CompletePendingRevert:   "complete_pending_revert",
	Complete:                "complete",
	CompleteError:           "complete_error",
	InProgressCancel:        "in_progress_cancel",
	CompleteCancel:          "complete_cancel",
	InProgressRevert:        "in_progress_revert",
	InProgressRevertVSchema: "in_progress_revert_vschema",
	CompleteRevert:          "complete_revert",
	CompleteRevertError:     "complete_revert_error",
	Cancelled:               "cancelled",
	Error:                   "error",
	Failed:                  "failed",
}
