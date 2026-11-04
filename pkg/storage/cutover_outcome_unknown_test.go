package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A cutover request records when each operation's send lost its answer
// separately: stamping one member of a set leaves the other member unstamped,
// a second stamp keeps the first, and the binding the request carries is kept
// through both.
func TestCutoverOutcomeUnknownSinceIsKeptPerOperation(t *testing.T) {
	metadata, err := CutoverRequestSetMetadata([]int64{42, 43})
	require.NoError(t, err)
	req := &ApplyControlRequest{ID: 1, Operation: ControlOperationCutover, Metadata: metadata}

	since, err := req.CutoverOutcomeUnknownSince(42)
	require.NoError(t, err)
	assert.True(t, since.IsZero(), "a request whose sends all answered has no stamp")

	first := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	req.Metadata, err = req.WithCutoverOutcomeUnknownSince(42, first)
	require.NoError(t, err)

	since, err = req.CutoverOutcomeUnknownSince(42)
	require.NoError(t, err)
	assert.True(t, first.Equal(since))
	since, err = req.CutoverOutcomeUnknownSince(43)
	require.NoError(t, err)
	assert.True(t, since.IsZero(), "one member's lost answer does not stamp its sibling")

	second := first.Add(time.Minute)
	req.Metadata, err = req.WithCutoverOutcomeUnknownSince(43, second)
	require.NoError(t, err)

	since, err = req.CutoverOutcomeUnknownSince(42)
	require.NoError(t, err)
	assert.True(t, first.Equal(since), "stamping a sibling keeps the earlier stamp")
	since, err = req.CutoverOutcomeUnknownSince(43)
	require.NoError(t, err)
	assert.True(t, second.Equal(since))

	ids, err := req.CutoverOperationIDs()
	require.NoError(t, err)
	assert.Equal(t, []int64{42, 43}, ids, "the stamps keep the request bound to its operations")
	assert.JSONEq(t, `{
		"apply_operation_id": 42,
		"apply_operation_ids": [42, 43],
		"outcome_unknown_since": {"42": "2026-11-04T10:00:00Z", "43": "2026-11-04T10:01:00Z"}
	}`, string(req.Metadata))
}

// An unbound request stamps under operation 0 and starts from empty metadata.
func TestCutoverOutcomeUnknownSinceOfUnboundRequest(t *testing.T) {
	req := &ApplyControlRequest{ID: 2, Operation: ControlOperationCutover}
	since, err := req.CutoverOutcomeUnknownSince(0)
	require.NoError(t, err)
	assert.True(t, since.IsZero())

	at := time.Date(2026, 11, 4, 10, 0, 0, 0, time.FixedZone("plus-two", 2*60*60))
	req.Metadata, err = req.WithCutoverOutcomeUnknownSince(0, at)
	require.NoError(t, err)
	assert.JSONEq(t, `{"outcome_unknown_since": {"0": "2026-11-04T08:00:00Z"}}`, string(req.Metadata), "stamps are stored in UTC")

	since, err = req.CutoverOutcomeUnknownSince(0)
	require.NoError(t, err)
	assert.True(t, at.Equal(since))
}

func TestCutoverOutcomeUnknownSinceOfUnreadableMetadata(t *testing.T) {
	req := &ApplyControlRequest{ID: 9, Operation: ControlOperationCutover, Metadata: []byte(`{`)}
	_, err := req.CutoverOutcomeUnknownSince(0)
	require.ErrorContains(t, err, "parse metadata of cutover control request 9")
	_, err = req.WithCutoverOutcomeUnknownSince(0, time.Now())
	require.ErrorContains(t, err, "parse metadata of cutover control request 9")

	req.Metadata = []byte(`{"outcome_unknown_since": "2026-11-04T08:00:00Z"}`)
	_, err = req.CutoverOutcomeUnknownSince(0)
	require.ErrorContains(t, err, "parse outcome_unknown_since of cutover control request 9")
}
