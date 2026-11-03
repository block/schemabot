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
