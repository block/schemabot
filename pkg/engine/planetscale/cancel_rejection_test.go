package planetscale

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
)

// Every deployment state PlanetScale can report must carry an explicit
// classification for a rejected cancel: reading a rejection as success or as
// an already-completed change in the wrong state would misrepresent what
// happened on the target, and leaving a state unclassified has the drive
// retry the rejection forever. This test walks every state.DeployRequest
// constant, so adding a new constant fails here until a deliberate
// classification is added both to classifyCancelRejection and to this
// expectation table.
func TestClassifyCancelRejection_EveryDeploymentStateIsClassified(t *testing.T) {
	expected := map[string]cancelRejectionClass{
		state.DeployRequest.InProgressCancel: cancelRejectionAlreadyCancelled,
		state.DeployRequest.CompleteCancel:   cancelRejectionAlreadyCancelled,
		state.DeployRequest.Cancelled:        cancelRejectionAlreadyCancelled,

		state.DeployRequest.Complete:  cancelRejectionAlreadyCompleted,
		state.DeployRequest.NoChanges: cancelRejectionAlreadyCompleted,

		// Still live: a rejected cancel here is a backend surprise that must
		// surface as an error naming the state.
		state.DeployRequest.Pending:           cancelRejectionStateError,
		state.DeployRequest.Ready:             cancelRejectionStateError,
		state.DeployRequest.Submitting:        cancelRejectionStateError,
		state.DeployRequest.Queued:            cancelRejectionStateError,
		state.DeployRequest.InProgress:        cancelRejectionStateError,
		state.DeployRequest.PendingCutover:    cancelRejectionStateError,
		state.DeployRequest.InProgressCutover: cancelRejectionStateError,
		state.DeployRequest.InProgressVSchema: cancelRejectionStateError,

		// Revert window and revert flow own their outcome; never read as
		// cancelled or completed here.
		state.DeployRequest.CompletePendingRevert:   cancelRejectionStateError,
		state.DeployRequest.InProgressRevert:        cancelRejectionStateError,
		state.DeployRequest.InProgressRevertVSchema: cancelRejectionStateError,
		state.DeployRequest.CompleteRevert:          cancelRejectionStateError,
		state.DeployRequest.CompleteRevertError:     cancelRejectionStateError,

		// Closed by failing: keep the failed outcome visible.
		state.DeployRequest.CompleteError: cancelRejectionStateError,
		state.DeployRequest.Error:         cancelRejectionStateError,
		state.DeployRequest.Failed:        cancelRejectionStateError,
	}

	deployStates := reflect.ValueOf(state.DeployRequest)
	deployStatesType := deployStates.Type()
	require.Equal(t, len(expected), deployStates.NumField(),
		"the expectation table must cover exactly the state.DeployRequest constants")
	for i := range deployStates.NumField() {
		fieldName := deployStatesType.Field(i).Name
		deploymentState := deployStates.Field(i).String()
		want, ok := expected[deploymentState]
		require.True(t, ok,
			"deployment state %q (state.DeployRequest.%s) has no expected cancel-rejection classification — decide how a rejected cancel observed in this state must be reported and add it to classifyCancelRejection and this table",
			deploymentState, fieldName)

		got, classified := classifyCancelRejection(deploymentState)
		assert.True(t, classified,
			"deployment state %q (state.DeployRequest.%s) must be explicitly classified in classifyCancelRejection, not fall to the unclassified default",
			deploymentState, fieldName)
		assert.Equal(t, want, got, "deployment state %q (state.DeployRequest.%s)", deploymentState, fieldName)
	}
}

// A deployment state this enumeration has never seen fails toward a plain
// error — never toward success or a typed completed rejection — and reports
// itself unclassified so the call site can count it for operators.
func TestClassifyCancelRejection_UnknownStateFailsTowardError(t *testing.T) {
	got, classified := classifyCancelRejection("in_progress_new_planetscale_phase")
	assert.False(t, classified)
	assert.Equal(t, cancelRejectionStateError, got)
}
