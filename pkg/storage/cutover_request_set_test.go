package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A cutover request bound to a set of operations names every one of them, and
// still names its first one under the single-operation key, so a server that
// reads only that key takes the request for that operation alone.
func TestCutoverRequestSetMetadataRoundTrip(t *testing.T) {
	metadata, err := CutoverRequestSetMetadata([]int64{42, 7, 19})
	require.NoError(t, err)
	assert.JSONEq(t, `{"apply_operation_id":7,"apply_operation_ids":[7,19,42]}`, string(metadata))

	req := &ApplyControlRequest{ID: 1, Operation: ControlOperationCutover, Metadata: metadata}
	ids, err := req.CutoverOperationIDs()
	require.NoError(t, err)
	assert.Equal(t, []int64{7, 19, 42}, ids)
	first, err := req.CutoverOperationID()
	require.NoError(t, err)
	assert.Equal(t, int64(7), first)
}

func TestCutoverRequestSetMetadataOfOneMatchesSingleBinding(t *testing.T) {
	metadata, err := CutoverRequestSetMetadata([]int64{42})
	require.NoError(t, err)
	assert.Equal(t, CutoverRequestMetadata(42), metadata)

	ids, err := (&ApplyControlRequest{Metadata: metadata}).CutoverOperationIDs()
	require.NoError(t, err)
	assert.Equal(t, []int64{42}, ids)
}

func TestCutoverRequestSetMetadataRefusesEmptyOrRepeatedSets(t *testing.T) {
	_, err := CutoverRequestSetMetadata(nil)
	require.ErrorContains(t, err, "at least one operation")
	_, err = CutoverRequestSetMetadata([]int64{3, 5, 3})
	require.ErrorContains(t, err, "more than once")
}

func TestCutoverOperationIDsOfUnboundAndUnreadableRequests(t *testing.T) {
	ids, err := (&ApplyControlRequest{}).CutoverOperationIDs()
	require.NoError(t, err)
	assert.Nil(t, ids)

	_, err = (&ApplyControlRequest{ID: 9, Operation: ControlOperationCutover, Metadata: []byte(`{`)}).CutoverOperationIDs()
	require.ErrorContains(t, err, "parse metadata of cutover control request 9")
}

// TestSettledByControlCommand pins which sends of an operator command leave a
// settled request settled: the command it carries, or an earlier command from
// the same origin, which only a delayed re-send can carry. A newer command, a
// command from another origin, or an id without an order re-arms the request.
func TestSettledByControlCommand(t *testing.T) {
	settled := func(t *testing.T, status ControlRequestStatus, recorded string) *ApplyControlRequest {
		t.Helper()
		metadata, err := WithControlCommandID(nil, recorded)
		require.NoError(t, err)
		return &ApplyControlRequest{ID: 9, Operation: ControlOperationCutover, Status: status, Metadata: metadata}
	}

	tests := []struct {
		name     string
		status   ControlRequestStatus
		recorded string
		sent     string
		want     bool
	}{
		{name: "the same command", status: ControlRequestFailed, recorded: NewControlCommandID(9, 1), sent: "9.1", want: true},
		{name: "an earlier command from the same origin", status: ControlRequestFailed, recorded: "9.1", sent: "9.0", want: true},
		{name: "an earlier command after completion", status: ControlRequestCompleted, recorded: "9.3", sent: "9.1", want: true},
		{name: "a newer command", status: ControlRequestFailed, recorded: "9.1", sent: "9.2", want: false},
		{name: "another origin", status: ControlRequestFailed, recorded: "9.1", sent: "8.0", want: false},
		{name: "an unordered id", status: ControlRequestFailed, recorded: "cmd-b", sent: "cmd-a", want: false},
		{name: "an unordered id against an ordered one", status: ControlRequestFailed, recorded: "9.1", sent: "9", want: false},
		{name: "a pending request", status: ControlRequestPending, recorded: "9.1", sent: "9.0", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := settled(t, tc.status, tc.recorded).SettledByControlCommand(tc.sent)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
